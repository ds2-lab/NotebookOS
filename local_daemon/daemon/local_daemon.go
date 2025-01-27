package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-viper/mapstructure/v2"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/petermattis/goid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/consul"
	"github.com/scusemua/distributed-notebook/common/docker_events/notification_manager"
	"github.com/scusemua/distributed-notebook/common/docker_events/observer"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"

	"github.com/scusemua/distributed-notebook/local_daemon/device"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
	"github.com/scusemua/distributed-notebook/local_daemon/invoker"
)

const (
	// DefaultPrometheusPort is the default port on which the Local Daemon will serve Prometheus metrics.
	DefaultPrometheusPort int = 8089
	// DefaultPrometheusInterval is the default interval on which the Local Daemon will push new Prometheus metrics.
	DefaultPrometheusInterval = time.Second * 5
	// DefaultNumResendAttempts is the default number of attempts we'll resend a message before giving up.
	DefaultNumResendAttempts = 3
	// DefaultExecuteRequestQueueSize is the default capacity of outgoing "execute_request" message queues.
	//
	// Important note: if there are more concurrent execute_request messages sent than the capacity of the buffered
	// channel that serves as the message queue, then the FCFS ordering of the messages cannot be guaranteed. Specifically,
	// the order in which the messages are enqueued is non-deterministic. (Once enqueued, the messages will be served in
	// a FCFS manner.)
	DefaultExecuteRequestQueueSize = 128
)

var (
	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute

	ErrExistingReplicaAlreadyRunning = status.Error(codes.Internal, "an existing replica of the target kernel is already running on this node")
	ErrNilArgument                   = status.Error(codes.InvalidArgument, "one or more of the required arguments was nil")

	errConcurrentConnectionAttempt = errors.New("another goroutine is already attempting to connect to the Cluster Gateway")
)

// enqueuedExecOrYieldRequest encapsulates an "execute_request" or "yield_request" *messaging.JupyterMessage and a
// chan interface{} used to notify the caller when the request has been submitted and a result has been returned.
type enqueuedExecOrYieldRequest struct {
	Msg           *messaging.JupyterMessage
	ResultChannel chan interface{}
	Kernel        scheduling.KernelReplica
}

type GRPCServerWrapper struct {
	*grpc.Server
	Id                   string
	PurposefullyShutdown bool
}

// SchedulerDaemonImpl is the daemon that proxy requests to kernel replicas on local-host.
//
// WIP: Replica membership change.
// TODO: Distinguish reachable host list from replica list.
// TODO: Synchronize resource status using replica network (e.g., control socket).
// Synchronization message should load-balance between replicas mapped the same host.
type SchedulerDaemonImpl struct {
	// SchedulerOptions
	id       string
	nodeName string

	// finishedGatewayHandshake is set in setID when the Local Daemon completes to registration
	// procedure with the cluster gateway for the first time.
	finishedGatewayHandshake bool

	tracer       opentracing.Tracer
	consulClient *consul.Client

	// The gRPC server used by the Local Daemon and Cluster Gateway.
	grpcServer *GRPCServerWrapper
	listener   net.Listener

	// connectingToGateway indicates whether the scheduler is actively trying to connect to the Cluster Gateway.
	// If its value is > 0, then it is. If its value is 0, then it is not.
	connectingToGateway atomic.Int32

	// DebugMode is a configuration parameter that, when enabled, causes the RequestTrace to be enabled as well
	// as the request history.
	DebugMode bool

	virtualGpuPluginServer device.VirtualGpuPluginServer

	schedulingPolicy scheduling.Policy

	kernelsStopping hashmap.HashMap[string, chan struct{}]

	proto.UnimplementedLocalGatewayServer
	proto.UnimplementedKernelErrorReporterServer
	router *router.Router

	// SchedulerOptions
	connectionOptions      *jupyter.ConnectionInfo
	schedulerDaemonOptions domain.SchedulerDaemonOptions

	// internalCluster client
	provisioner                     proto.ClusterGatewayClient
	provisionerClientConnectionGRPC *grpc.ClientConn

	// prometheusManager creates and serves Prometheus metrics for the Local Daemon.
	prometheusManager *metrics.LocalDaemonPrometheusManager
	// Indicates that a goroutine has been started to publish metrics to Prometheus.
	servingPrometheus atomic.Int32
	// prometheusStarted is a sync.WaitGroup used to signal to the metric-publishing goroutine
	// that it should start publishing metrics now.
	prometheusStarted sync.WaitGroup
	// prometheusInterval is how often we publish metrics to Prometheus.
	prometheusInterval time.Duration
	// prometheusPort is the port on which this local daemon will serve Prometheus metrics.
	prometheusPort int

	// electionTimeoutSeconds is how long kernel leader elections wait to receive all proposals before electing a leader
	electionTimeoutSeconds int

	// outgoingExecuteRequestQueue is used to send "execute_request" messages one-at-a-time to their target kernels.
	outgoingExecuteRequestQueue hashmap.HashMap[string, chan *enqueuedExecOrYieldRequest]
	// outgoingExecuteRequestQueueMutexes is a map of mutexes. Keys are kernel IDs. Values are the mutex for the
	// associated kernel's outgoing "execute_request" message queue.
	outgoingExecuteRequestQueueMutexes hashmap.HashMap[string, *sync.Mutex]
	// executeRequestQueueStopChans is a map from kernel ID to the channel used to tell the goroutine responsible
	// for forwarding that kernel's "execute_request" messages to stop running (such as when we are stopping
	// the kernel replica).
	executeRequestQueueStopChannels hashmap.HashMap[string, chan interface{}]

	smrPort int

	// members
	transport                    string
	ip                           string
	kernels                      hashmap.HashMap[string, scheduling.KernelReplica]
	kernelClientCreationChannels hashmap.HashMap[string, chan *proto.KernelConnectionInfo]

	log logger.Logger

	// SimulateCheckpointingLatency controls whether the kernels will be configured to simulate the latency of
	// performing checkpointing after a migration (read) and after executing code (write).
	SimulateCheckpointingLatency bool

	// The IOPub socket that the Gateway subscribes to.
	// All pub/sub messages are forwarded from kernels to the gateway (through us, the local daemon) using this socket.
	// We wrap the messages in another message that just has a header that is the kernel ID.
	// This enables the Gateway's SUB sockets to filter messages from each kernel.
	// iopub *jupyter.Socket

	// When deployed in Docker Swarm mode, the dockerEventObserver listens for "container-created"
	// events from the node's Docker daemon socket.
	//
	// This is only used (and is only non-nil) when deployed in Docker Swarm/Compose mode.
	dockerEventObserver *observer.EventObserver

	// containerStartedNotificationManager keeps track of observer.ContainerStartedNotification structs
	// associated with various kernels. Specifically, it maintains the most recent notification associated
	// with a particular kernel.
	//
	// This is only used (and is only non-nil) when deployed in Docker Swarm/Compose mode.
	containerStartedNotificationManager *notification_manager.ContainerStartedNotificationManager

	// There's a simple TCP server that listens for kernel registration notifications on this port.
	kernelRegistryPort int

	// numResendAttempts is the number of times to try resending a message before giving up.
	numResendAttempts int

	// Manages resource allocations on behalf of the Local Daemon.
	resourceManager *resource.AllocationManager

	// Hostname of the remote storage. The SyncLog's remote storage client will connect to this.
	remoteStorageEndpoint string

	// Type of remote storage, 'hdfs' or 'redis'
	remoteStorage string

	// Base directory in which the persistent store data is stored when running in docker mode.
	dockerStorageBase string

	// Kubernetes or Docker.
	deploymentMode types.DeploymentMode

	// When using Docker mode, we assign "debug ports" to kernels so that they can serve a Go HTTP server for debugging.
	kernelDebugPorts hashmap.HashMap[string, int]

	// MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements
	// for the ZMQ messages that we're forwarding back and forth between the Cluster Gateway and kernel replicas.
	//
	// MessageAcknowledgementsEnabled is controlled by the "acks_enabled" field of the configuration file.
	MessageAcknowledgementsEnabled bool

	// If true, then the kernels will be executed within GDB.
	runKernelsInGdb bool

	// kernelErrorReporterServerPort is the port on which the proto.KernelErrorReporterServer gRPC service is listening.
	kernelErrorReporterServerPort int

	// localDaemonOptions is the options struct that the Local Daemon was created with.
	localDaemonOptions *domain.LocalDaemonOptions

	// useRealGpus controls whether we tell the kernels to train using real GPUs and real PyTorch code or not.
	useRealGpus bool

	// bindDebugpyPort specifies whether to bind a port to kernel containers for DebugPy.
	bindDebugpyPort bool

	// If true, rename stopped kernel containers to save/persist them. Enables you to persist their state, logs, etc.
	saveStoppedKernelContainers bool

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

type KernelRegistrationPayload struct {
	Op                 string                  `json:"op"`
	SignatureScheme    string                  `json:"signatureScheme"`
	Key                string                  `json:"key"`
	Kernel             *proto.KernelSpec       `json:"kernel,omitempty"`
	ReplicaId          int32                   `json:"replicaId,omitempty"`
	NumReplicas        int32                   `json:"numReplicas,omitempty"`
	Join               bool                    `json:"join,omitempty"`
	PersistentId       *string                 `json:"persistentId,omitempty"`
	PodOrContainerName string                  `json:"podName,omitempty"`
	NodeName           string                  `json:"nodeName,omitempty"`
	Cpu                int32                   `json:"cpu,omitempty"`
	Memory             int32                   `json:"memory,omitempty"`
	Gpu                int32                   `json:"gpu,omitempty"`
	ConnectionInfo     *jupyter.ConnectionInfo `json:"connection-info,omitempty"`
	WorkloadId         string                  `json:"workload_id"`
}

// KernelRegistrationClient represents an incoming connection from local distributed kernel.
type KernelRegistrationClient struct {
	conn net.Conn
}

func New(connectionOptions *jupyter.ConnectionInfo, localDaemonOptions *domain.LocalDaemonOptions,
	kernelRegistryPort int, kernelErrorReporterServerPort int, virtualGpuPluginServer device.VirtualGpuPluginServer,
	nodeName string, dockerNodeId string, configs ...domain.SchedulerDaemonConfig) *SchedulerDaemonImpl {

	ip := os.Getenv("POD_IP")

	daemon := &SchedulerDaemonImpl{
		connectionOptions:                  connectionOptions,
		transport:                          "tcp",
		ip:                                 ip,
		id:                                 dockerNodeId,
		nodeName:                           nodeName,
		kernelsStopping:                    hashmap.NewCornelkMap[string, chan struct{}](128),
		kernels:                            hashmap.NewCornelkMap[string, scheduling.KernelReplica](128),
		kernelClientCreationChannels:       hashmap.NewCornelkMap[string, chan *proto.KernelConnectionInfo](128),
		kernelDebugPorts:                   hashmap.NewCornelkMap[string, int](256),
		closed:                             make(chan struct{}),
		cleaned:                            make(chan struct{}),
		kernelRegistryPort:                 kernelRegistryPort,
		kernelErrorReporterServerPort:      kernelErrorReporterServerPort,
		localDaemonOptions:                 localDaemonOptions,
		smrPort:                            localDaemonOptions.SMRPort,
		virtualGpuPluginServer:             virtualGpuPluginServer,
		deploymentMode:                     types.DeploymentMode(localDaemonOptions.DeploymentMode),
		remoteStorageEndpoint:              localDaemonOptions.RemoteStorageEndpoint,
		remoteStorage:                      localDaemonOptions.RemoteStorage,
		dockerStorageBase:                  localDaemonOptions.DockerStorageBase,
		DebugMode:                          localDaemonOptions.CommonOptions.DebugMode,
		prometheusInterval:                 time.Second * time.Duration(localDaemonOptions.PrometheusInterval),
		prometheusPort:                     localDaemonOptions.PrometheusPort,
		numResendAttempts:                  localDaemonOptions.NumResendAttempts,
		runKernelsInGdb:                    localDaemonOptions.RunKernelsInGdb,
		outgoingExecuteRequestQueue:        hashmap.NewCornelkMap[string, chan *enqueuedExecOrYieldRequest](128),
		outgoingExecuteRequestQueueMutexes: hashmap.NewCornelkMap[string, *sync.Mutex](128),
		executeRequestQueueStopChannels:    hashmap.NewCornelkMap[string, chan interface{}](128),
		MessageAcknowledgementsEnabled:     localDaemonOptions.MessageAcknowledgementsEnabled,
		SimulateCheckpointingLatency:       localDaemonOptions.SimulateCheckpointingLatency,
		electionTimeoutSeconds:             localDaemonOptions.ElectionTimeoutSeconds,
		useRealGpus:                        localDaemonOptions.UseRealGPUs,
		bindDebugpyPort:                    localDaemonOptions.BindDebugPyPort,
		saveStoppedKernelContainers:        localDaemonOptions.SaveStoppedKernelContainers,
	}

	for _, configFunc := range configs {
		configFunc(daemon)
	}

	config.InitLogger(&daemon.log, daemon)

	if daemon.DebugMode {
		daemon.log.Debug("Running in DebugMode.")
	} else {
		daemon.log.Debug("Not running in DebugMode.")
	}

	if daemon.electionTimeoutSeconds <= 0 {
		daemon.electionTimeoutSeconds = 3
	}

	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon, daemon.MessageAcknowledgementsEnabled,
		fmt.Sprintf("LocalDaemon_%s", nodeName), true, metrics.LocalDaemon, daemon.DebugMode, nil)

	if daemon.numResendAttempts <= 0 {
		daemon.log.Error("Invalid number of message resend attempts specified: %d. Defaulting to %d.",
			daemon.numResendAttempts, DefaultNumResendAttempts)
		daemon.numResendAttempts = DefaultNumResendAttempts
	}

	gpusPerHost := localDaemonOptions.GpusPerHost
	if gpusPerHost == -1 {
		if !localDaemonOptions.UseRealGPUs {
			daemon.log.Error("Invalid number of simulated GPUs specified: %d", gpusPerHost)
			panic(fmt.Sprintf("invalid number of simulated GPUs specified: %d", gpusPerHost))
		}

		daemon.log.Debug("Attempting to look up the number of real/actual GPUs available on this node.")

		var err error
		gpusPerHost, err = utils.GetNumberOfActualGPUs()
		if err != nil {
			daemon.log.Error("Failed to look up the number of real/actual GPUs available on this node: %v", err)
			panic(err)
		}

		daemon.log.Debug("Successfully looked up the number of real/actual GPUs available on this node: %d", gpusPerHost)
	}

	daemon.resourceManager = resource.NewAllocationManager(&types.Float64Spec{
		GPUs:      float64(gpusPerHost),
		VRam:      scheduling.VramPerHostGb,
		Millicpus: scheduling.MillicpusPerHost,
		Memory:    scheduling.MemoryMbPerHost})

	if daemon.prometheusInterval == time.Duration(0) {
		daemon.log.Debug("Using default Prometheus interval: %v.", DefaultPrometheusInterval)
		daemon.prometheusInterval = DefaultPrometheusInterval
	}

	if daemon.prometheusPort <= 0 {
		daemon.log.Debug("Using default Prometheus port: %d.", DefaultPrometheusPort)
		daemon.prometheusPort = DefaultPrometheusPort
	}

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	if len(localDaemonOptions.RemoteStorageEndpoint) == 0 {
		panic("remote storage endpoint is empty.")
	}

	schedulingPolicy, err := scheduler.GetSchedulingPolicy(&localDaemonOptions.SchedulerOptions)
	if err != nil {
		panic(err)
	}
	daemon.schedulingPolicy = schedulingPolicy
	daemon.log.Debug("Scheduling policy: %s", schedulingPolicy.Name())

	switch localDaemonOptions.DeploymentMode {
	case "":
		{
			daemon.log.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			daemon.deploymentMode = types.LocalMode
		}
	case "local":
		daemon.log.Info("Running in LOCAL mode.")
		daemon.deploymentMode = types.LocalMode

		if daemon.id != "" {
			daemon.log.Error("We're running in DefaultSchedulingPolicy mode, yet we already have an ID: \"%s\"", daemon.id)
			panic("We should not yet have an ID as we're running in DefaultSchedulingPolicy mode.")
		}
	case "docker":
		{
			daemon.log.Error("\"docker\" mode is no longer a valid deployment mode")
			daemon.log.Error("The supported deployment modes are: ")
			daemon.log.Error("- \"docker-swarm\"")
			daemon.log.Error("- \"docker-compose\"")
			daemon.log.Error("- \"kubernetes\"")
			daemon.log.Error("- \"local\"")
			os.Exit(1)
		}
	case "docker-compose":
		{
			daemon.log.Info("Running in DOCKER COMPOSE mode.")
			daemon.deploymentMode = types.DockerComposeMode

			daemon.containerStartedNotificationManager = notification_manager.NewContainerStartedNotificationManager()

			daemon.dockerEventObserver = observer.NewEventObserver(localDaemonOptions.DockerAppName, localDaemonOptions.DockerNetworkName)
			daemon.dockerEventObserver.RegisterEventConsumer(uuid.NewString(), daemon)
			daemon.dockerEventObserver.Start()
		}
	case "docker-swarm":
		{
			daemon.log.Info("Running in DOCKER SWARM mode.")
			daemon.deploymentMode = types.DockerSwarmMode

			daemon.containerStartedNotificationManager = notification_manager.NewContainerStartedNotificationManager()

			daemon.dockerEventObserver = observer.NewEventObserver(localDaemonOptions.DockerAppName, localDaemonOptions.DockerNetworkName)
			daemon.dockerEventObserver.RegisterEventConsumer(uuid.NewString(), daemon)
			daemon.dockerEventObserver.Start()
		}
	case "kubernetes":
		{
			daemon.log.Info("Running in KUBERNETES mode.")
			daemon.deploymentMode = types.KubernetesMode

			if daemon.id != "" {
				daemon.log.Error("We're running in Kubernetes mode, yet we already have an ID: \"%s\"", daemon.id)
				panic("We should not yet have an ID as we're running in Kubernetes mode.")
			}
		}
	default:
		{
			daemon.log.Error("Unknown/unsupported deployment mode: \"%s\"", localDaemonOptions.DeploymentMode)
			daemon.log.Error("The supported deployment modes are: ")
			daemon.log.Error("- \"kubernetes\"")
			daemon.log.Error("- \"docker-swarm\"")
			daemon.log.Error("- \"docker-compose\"")
			daemon.log.Error("- \"local\"")
		}
	}

	if daemon.id != "" {
		daemon.log.Debug("Successfully recovered ID of Docker Container: \"%s\"", daemon.id)
	}

	daemon.log.Debug("Connection options: %v", daemon.connectionOptions)

	if !localDaemonOptions.IsLocalMode() && len(nodeName) == 0 {
		panic("Node name is empty.")
	}

	if localDaemonOptions.IsLocalMode() && len(nodeName) == 0 {
		daemon.nodeName = types.LocalNode
	}

	if localDaemonOptions.IsDockerComposeMode() && len(nodeName) == 0 {
		daemon.nodeName = types.VirtualDockerNode
	}

	if localDaemonOptions.IsDockerSwarmMode() && len(nodeName) == 0 {
		// Eventually, Docker Swarm mode will only support "Docker Nodes", which correspond to real machines or VMs.
		// Virtual Docker Nodes will only be used in Docker Compose mode.
		daemon.nodeName = types.VirtualDockerNode // types.DockerNode
	}

	// The goroutine that publishes metrics to Prometheus waits for this WaitGroup to be Done.
	daemon.prometheusStarted.Add(1)

	// We use this WaitGroup to wait for the goroutine that publishes metrics to Prometheus to start.
	var goroutineStarted sync.WaitGroup
	goroutineStarted.Add(1)
	daemon.publishPrometheusMetrics(&goroutineStarted)
	goroutineStarted.Wait() // Wait for goroutine to start.

	return daemon
}

// Provisioner returns the proto.ClusterGatewayClient field of the target SchedulerDaemonImpl struct.
func (d *SchedulerDaemonImpl) Provisioner() proto.ClusterGatewayClient {
	return d.provisioner
}

// SetProvisioner assigns a value to the provisioner and provisionerClientConnectionGRPC
// fields of the target SchedulerDaemonImpl struct.
func (d *SchedulerDaemonImpl) SetProvisioner(provisioner proto.ClusterGatewayClient, grpcClientConn *grpc.ClientConn) {
	if provisioner == nil {
		panic("Cannot set LocalDaemon's provisioner to nil")
	}

	if grpcClientConn == nil {
		panic("Cannot set LocalDaemon's provisioner gRPC connection to nil")
	}

	d.provisioner = provisioner
	d.provisionerClientConnectionGRPC = grpcClientConn
}

// hasId returns true if we already have a valid ID assigned.
// Otherwise, hasId returns false.
func (d *SchedulerDaemonImpl) hasId() bool {
	return d.id != ""
}

func (d *SchedulerDaemonImpl) ConsumeDockerEvent(event map[string]interface{}) {
	m, _ := json.MarshalIndent(event, "", "  ")
	d.log.Debug("Received Docker 'container-created' event:\n%s", m)

	// TODO: Store this event in a way that avoids race between receiving the event and the kernel registering.

	fullContainerId := event["id"].(string)
	shortContainerId := fullContainerId[0:12]
	attributes := event["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})

	var kernelId string
	if val, ok := attributes["kernel_id"]; ok {
		kernelId = val.(string)
	} else {
		d.log.Debug("Docker Container %s related to the distributed cluster has started.", shortContainerId)
		return
	}

	// Generate a more meaningful error message + notify the dashboard UI.
	if d.containerStartedNotificationManager == nil {
		err := fmt.Errorf("containerStartedNotificationManager is nil")

		// This call will panic.
		d.notifyClusterGatewayAndPanic("Cannot Handle Docker Event", err.Error(), err)
	}

	notification := &observer.ContainerStartedNotification{
		FullContainerId:  fullContainerId,
		ShortContainerId: shortContainerId,
		KernelId:         kernelId,
	}
	d.containerStartedNotificationManager.DeliverNotification(notification)
}

// isValidId returns true if the given ID is not the empty string and thus is valid.
func isValidId(id string) bool {
	return id != ""
}

// SetID sets the SchedulerDaemonImpl id by the gateway.
// This also instructs the Local Daemon to create a LocalDaemonPrometheusManager and begin serving metrics.
func (d *SchedulerDaemonImpl) SetID(_ context.Context, in *proto.HostId) (*proto.HostId, error) {
	// If we've already done this once before, then we'll use our existing ID and whatnot.
	if d.finishedGatewayHandshake {
		return &proto.HostId{
			Id:       d.id,
			NodeName: d.nodeName,
			Existing: true,
		}, nil
	}

	// If we don't have an ID yet...
	if !d.hasId() {
		// Make sure we received a valid ID.
		if !isValidId(in.Id) {
			log.Fatalln(utils.RedStyle.Render("Received empty ID, and our current ID is also empty..."))
		}

		d.id = in.Id
		d.log.Debug("Set ID to \"%s\"", d.id)
	} else {
		// We'll use our existing ID, which must be the ID of our Docker container, since we've
		// not already been through the registration process already, so it isn't the case that
		// we just simply have some other ID already assigned (that isn't our container ID).
		d.log.Debug("Refusing to replace existing ID \"%s\" with new ID \"%s\"", d.id, in.Id)
		in.Id = d.id // Pass our existing ID back
	}

	in.NodeName = d.nodeName // We're passing this value back

	// Update the ID field of the router and of any existing kernels.
	d.router.SetComponentId(d.id)
	d.kernels.Range(func(_ string, replicaClient scheduling.KernelReplica) (contd bool) {
		replicaClient.SetComponentId(d.id)
		return true
	})
	d.resourceManager.NodeID = d.id
	d.finishedGatewayHandshake = true

	if d.prometheusManager != nil {
		// We'll just restart the Local Daemon's Prometheus ExecutionManager.
		_ = d.prometheusManager.Stop()
		if err := d.prometheusManager.Start(); err != nil {
			d.log.Error("Failed to start Prometheus ExecutionManager because: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		d.prometheusManager = metrics.NewLocalDaemonPrometheusManager(8089, d.id)
		err := d.prometheusManager.Start()
		if err != nil {
			d.log.Error("Failed to start Prometheus ExecutionManager because: %v", err)
			return in, status.Error(codes.Internal, err.Error())
		}

		// Publish GPU resource metrics.
		d.prometheusManager.IdleGpuGauge.
			Set(d.resourceManager.IdleGPUs().InexactFloat64())
		d.prometheusManager.PendingGpuGauge.
			Set(d.resourceManager.PendingGPUs().InexactFloat64())
		d.prometheusManager.CommittedGpuGauge.
			Set(d.resourceManager.CommittedGPUs().InexactFloat64())
		d.prometheusManager.SpecGpuGauge.
			Set(d.resourceManager.SpecGPUs().InexactFloat64())

		// Publish CPU resource metrics.
		d.prometheusManager.IdleCpuGauge.
			Set(d.resourceManager.IdleCPUs().InexactFloat64())
		d.prometheusManager.PendingCpuGauge.
			Set(d.resourceManager.PendingCPUs().InexactFloat64())
		d.prometheusManager.CommittedCpuGauge.
			Set(d.resourceManager.CommittedCPUs().InexactFloat64())
		d.prometheusManager.SpecCpuGauge.
			Set(d.resourceManager.SpecCPUs().InexactFloat64())

		// Publish memory resource metrics.
		d.prometheusManager.IdleMemoryGauge.
			Set(d.resourceManager.IdleMemoryMB().InexactFloat64())
		d.prometheusManager.PendingMemoryGauge.
			Set(d.resourceManager.PendingMemoryMB().InexactFloat64())
		d.prometheusManager.CommittedMemoryGauge.
			Set(d.resourceManager.CommittedMemoryMB().InexactFloat64())
		d.prometheusManager.SpecMemoryGauge.
			Set(d.resourceManager.SpecMemoryMB().InexactFloat64())

		d.prometheusManager.NumActiveKernelReplicasGauge.
			Set(float64(d.kernels.Len()))

		// We only call Done if we're creating the LocalDaemonPrometheusManager for the first time.
		d.prometheusStarted.Done()

		// Register the Prometheus metrics manager with the ResourceManager and the Local Daemon's Router.
		d.resourceManager.RegisterMetricsManager(d.prometheusManager)
		d.router.AssignPrometheusManager(d.prometheusManager)
	}

	return in, nil
}

// publishPrometheusMetrics creates a goroutine that publishes metrics to prometheus on a configurable interval.
func (d *SchedulerDaemonImpl) publishPrometheusMetrics(wg *sync.WaitGroup) {
	go func() {
		// Claim ownership of publishing metrics.
		if !d.servingPrometheus.CompareAndSwap(0, 1) {
			return
		}

		wg.Done()
		d.prometheusStarted.Wait()

		d.log.Debug("Beginning to publish metrics to Prometheus now. Interval: %v", d.prometheusInterval)

		for {
			time.Sleep(d.prometheusInterval)

			// Publish GPU resource metrics.
			d.prometheusManager.IdleGpuGauge.
				Set(d.resourceManager.IdleGPUs().InexactFloat64())
			d.prometheusManager.PendingGpuGauge.
				Set(d.resourceManager.PendingGPUs().InexactFloat64())
			d.prometheusManager.CommittedGpuGauge.
				Set(d.resourceManager.CommittedGPUs().InexactFloat64())
			d.prometheusManager.SpecGpuGauge.
				Set(d.resourceManager.SpecGPUs().InexactFloat64())

			// Publish CPU resource metrics.
			d.prometheusManager.IdleCpuGauge.
				Set(d.resourceManager.IdleCPUs().InexactFloat64())
			d.prometheusManager.PendingCpuGauge.
				Set(d.resourceManager.PendingCPUs().InexactFloat64())
			d.prometheusManager.CommittedCpuGauge.
				Set(d.resourceManager.CommittedCPUs().InexactFloat64())
			d.prometheusManager.SpecCpuGauge.
				Set(d.resourceManager.SpecCPUs().InexactFloat64())

			// Publish memory resource metrics.
			d.prometheusManager.IdleMemoryGauge.
				Set(d.resourceManager.IdleMemoryMB().InexactFloat64())
			d.prometheusManager.PendingMemoryGauge.
				Set(d.resourceManager.PendingMemoryMB().InexactFloat64())
			d.prometheusManager.CommittedMemoryGauge.
				Set(d.resourceManager.CommittedMemoryMB().InexactFloat64())
			d.prometheusManager.SpecMemoryGauge.
				Set(d.resourceManager.SpecMemoryMB().InexactFloat64())

			// TODO: This is somewhat imprecise insofar if we stop training RIGHT before this goroutine runs again,
			// then we'll not add any of that training time.
			d.kernels.Range(func(_ string, replicaClient scheduling.KernelReplica) (contd bool) {
				if replicaClient.IsTraining() {
					trainingTimeSeconds := time.Since(replicaClient.TrainingStartedAt()).Seconds()

					// If we've been training for at least one interval, then we're safe to just add another interval's
					// worth of seconds to the Prometheus counter.
					if trainingTimeSeconds > d.prometheusInterval.Seconds() {
						d.prometheusManager.TrainingTimeGaugeVec.
							With(prometheus.Labels{
								"workload_id": replicaClient.WorkloadId(),
								"kernel_id":   replicaClient.ID(),
								"node_id":     d.id,
							}).
							Add(d.prometheusInterval.Seconds())
					} else {
						// If we started training within the last interval, then we should only increment the Prometheus
						// counter by the exact amount of time that we've been training.
						d.prometheusManager.TrainingTimeGaugeVec.
							With(prometheus.Labels{"workload_id": replicaClient.WorkloadId(), "kernel_id": replicaClient.ID(), "node_id": d.id}).
							Add(trainingTimeSeconds) // trainingTimeSeconds is < 1.
					}

					replicaClient.SetLastTrainingTimePrometheusUpdate()
				}

				return true
			})
		}
	}()
}

// StartKernel starts a single kernel.
func (d *SchedulerDaemonImpl) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return d.StartKernelReplica(ctx, &proto.KernelReplicaSpec{
		ReplicaId:  1,
		Replicas:   nil,
		Kernel:     in,
		WorkloadId: in.WorkloadId,
	})
}

// initializeConsulAndTracer creates the Consul client and Tracer.
func (d *SchedulerDaemonImpl) initializeConsulAndTracer() {
	tracer, consulClient := CreateConsulAndTracer(d.localDaemonOptions)

	d.tracer = tracer
	d.consulClient = consulClient
}

// connectToGateway connects to the Cluster Gateway.
//
// This is thread-safe. If another thread is already executing connectToGateway when the current thread calls
// connectToGateway, then the current thread will return immediately.
func (d *SchedulerDaemonImpl) connectToGateway(gatewayAddress string, finalize LocalDaemonFinalizer) error {
	// We use this finalize function to ensure we don't needlessly terminate the Local Daemon process/container
	// when we're purposefully reconnecting to the Cluster Gateway (as doing so necessarily requires that we
	// shut down the existing gRPC server and whatnot, and ordinarily that causes use to panic).
	wrappedFinalizeFunction := func(currentRpcServer *GRPCServerWrapper) {
		// If the server was purposefully shutdown, then we shouldn't terminate.
		// That is, if we shut it down because we're attempting to reconnect to the Cluster Gateway,
		// then we shouldn't panic/kill the entire Local Daemon process. We're intentionally shutting
		// down the Provisioner. It's fine.
		//
		// If on the other hand, we did NOT manually/explicitly shut down the provisioner ourselves,
		// then indeed we should probably panic.
		//
		// Alternatively, we could try to initiate a reconnection attempt here, but for now, we'll just
		// call finalize like always.
		shouldTerminate := !currentRpcServer.PurposefullyShutdown

		d.log.Warn("Finalizer for gRPC serving thread called. Something must have happened to the server. "+
			"Purposefully shutdown: %v", currentRpcServer.PurposefullyShutdown)

		finalize(shouldTerminate)

		// Not sure when the arguments are evaluated for a deferred function,
		// so I'm passing a local variable that's already been initialized when
		// the correct gRPC server struct.
	}

	// Make sure nobody else is already trying to connect. Don't want to execute this method non-atomically.
	if !d.connectingToGateway.CompareAndSwap(0, 1) {
		d.log.Warn("Another goroutine is already attempting to connect to the Cluster Gateway.")
		return errConcurrentConnectionAttempt
	}

	// Make sure to swap the connectingToGateway flag back to 0 once we're done, regardless of whether
	// we successfully reconnected to the Cluster Gateway.
	defer func() {
		swapped := d.connectingToGateway.CompareAndSwap(1, 0)
		if !swapped {
			log.Fatalln(utils.RedStyle.Render("Failed to swap `connecting to gateway` flag back after finishing connection attempt..."))
		}
	}()

	d.log.Debug("Connecting to Cluster Gateway at \"%s\"", gatewayAddress)

	if d.tracer == nil {
		d.log.Debug("Initializing ConsulClient and Tracer.")
		d.initializeConsulAndTracer()
	}

	if d.grpcServer != nil {
		d.log.Warn("Found existing gRPC server. Shutting it down.")
		d.grpcServer.PurposefullyShutdown = true
		d.grpcServer.Stop()
		d.grpcServer = nil
	}

	var err error
	if d.provisioner != nil {
		d.log.Warn("Found existing provisioner. Shutting it down.")
		err = d.provisioner.(*Provisioner).Close()
		if err != nil {
			d.log.Error("Error while shutting down existing Provisioner: %v", err)
		}

		d.provisioner = nil
	}

	gOpts := GetGrpcOptions(d.tracer)
	d.grpcServer = &GRPCServerWrapper{
		Server:               grpc.NewServer(gOpts...),
		Id:                   uuid.NewString(),
		PurposefullyShutdown: false,
	}
	proto.RegisterLocalGatewayServer(d.grpcServer, d)
	proto.RegisterKernelErrorReporterServer(d.grpcServer, d)

	// Initialize gRPC listener.
	d.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", d.localDaemonOptions.Port))
	if err != nil {
		d.log.Error("Failed to listen: %v", err)
		return err
	}

	start := time.Now()
	var (
		connectedToProvisioner = false
		numAttempts            = 1
		provConn               net.Conn
	)
	for !connectedToProvisioner && time.Since(start) < (time.Minute*1) {
		globalLogger.Info("Attempt #%d to connect to Provisioner (Gateway) at %s. Connection timeout: %v.",
			numAttempts, gatewayAddress, connectionTimeout)
		provConn, err = net.DialTimeout("tcp", gatewayAddress, connectionTimeout)

		if err != nil {
			globalLogger.Warn("Failed to connect to provisioner at %s on attempt #%d: %v",
				gatewayAddress, numAttempts, err)
			numAttempts += 1
			time.Sleep(time.Second * 3)
		} else {
			connectedToProvisioner = true
		}
	}

	// Initialize connection to the provisioner
	if !connectedToProvisioner {
		return err
	}

	if provConn == nil {
		return fmt.Errorf("provisioner connection is nil")
	}

	// Initialize provisioner and wait for ready
	provisioner, grpcClientConn, err := NewProvisioner(provConn)
	if err != nil {
		d.log.Error("Failed to initialize the provisioner: %v", err)
		return err
	}

	errorChan := make(chan interface{}, 1)

	// Wait for reverse connection
	go func() {
		currentServer := d.grpcServer
		defer wrappedFinalizeFunction(currentServer)

		if err := d.grpcServer.Serve(provisioner); err != nil {
			d.log.Error("Error while serving provisioner gRPC server: %v", err)
			errorChan <- err
		}

		errorChan <- struct{}{}
	}()

	// TODO: Add timeout option here.
	select {
	case <-provisioner.Ready():
		{
			break
		}
	case v := <-errorChan:
		{
			if err, ok := v.(error); ok {
				return err
			}
		}
	}

	if err := provisioner.Validate(); err != nil {
		d.log.Error("Failed to validate the provisioner: %v", err)
		return err
	}
	d.SetProvisioner(provisioner, grpcClientConn)
	d.log.Debug("Scheduler connected to %v", provConn.RemoteAddr())

	// Register services in consulClient
	if d.consulClient != nil {
		err = d.consulClient.Register(ServiceName, uuid.New().String(), "", d.localDaemonOptions.Port)
		if err != nil {
			d.log.Error("Failed to register in consulClient: %v", err)
			return err
		}
		d.log.Debug("Successfully registered in consulClient")
	}

	// Start gRPC server
	go func() {
		// We don't want to panic if we're purposefully shutting down the old gRPC server.
		currentServer := d.grpcServer
		defer wrappedFinalizeFunction(currentServer)

		if err := d.grpcServer.Serve(d.listener); err != nil {
			// This error may be because we're reconnecting to the Cluster Gateway.
			d.log.Error("Error while serving gRPC server: %v", err)
		}
	}()

	return nil
}

// Register a Kernel that has started running on the same node that we are running on.
// This method must be thread-safe.
func (d *SchedulerDaemonImpl) registerKernelReplica(_ context.Context, kernelRegistrationClient *KernelRegistrationClient) {
	registeredAt := time.Now()
	d.log.Debug("Registering Kernel at (remote) address %v", kernelRegistrationClient.conn.RemoteAddr())

	remoteIp, _, err := net.SplitHostPort(kernelRegistrationClient.conn.RemoteAddr().String())
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to extract remote address from kernel registration connection because: %v", err)
		d.log.Error(errorMessage)
		d.log.Error("Cannot register kernel.") // TODO(Ben): Handle this more elegantly.
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Failed to Register Kernel.",
			Message:          errorMessage,
			NotificationType: 0,
			Panicked:         false,
		})
		return
	}

	var registrationPayload KernelRegistrationPayload
	jsonDecoder := json.NewDecoder(kernelRegistrationClient.conn)
	err = jsonDecoder.Decode(&registrationPayload)
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to decode registration payload: %v", err)
		d.log.Error(errorMessage)
		d.log.Error("Cannot register kernel.") // TODO(Ben): Handle this more elegantly.
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Failed to Register Kernel.",
			Message:          errorMessage,
			NotificationType: 0,
			Panicked:         false,
		})
		return
	}

	d.log.Debug("Received registration payload: %v", registrationPayload)

	var connInfo *jupyter.ConnectionInfo
	if d.LocalMode() {
		connInfo = registrationPayload.ConnectionInfo
	} else {
		connInfo = &jupyter.ConnectionInfo{
			IP:              remoteIp,
			Transport:       "tcp",
			ControlPort:     d.connectionOptions.ControlPort,
			ShellPort:       d.connectionOptions.ShellPort,
			StdinPort:       d.connectionOptions.StdinPort,
			HBPort:          d.connectionOptions.HBPort,
			IOSubPort:       d.connectionOptions.IOSubPort,
			IOPubPort:       d.connectionOptions.IOPubPort,
			SignatureScheme: registrationPayload.SignatureScheme,
			Key:             registrationPayload.Key,
		}
	}

	if connInfo == nil {
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Received nil Connection Info from Kernel",
			Message:          fmt.Sprintf("The connection info sent in the registration payload of kernel at address %s is nil.", remoteIp),
			NotificationType: 0,
			Panicked:         true,
		})
		panic(fmt.Sprintf("Connection info sent to us by kernel at %s is nil.", remoteIp))
	}
	d.log.Debug("connInfo: %v", connInfo)

	kernelReplicaSpec := &proto.KernelReplicaSpec{
		Kernel:       registrationPayload.Kernel,
		ReplicaId:    registrationPayload.ReplicaId,   // Can get (from config file).
		NumReplicas:  registrationPayload.NumReplicas, // Can get (from config file).
		Join:         registrationPayload.Join,        // Can get (from config file).
		PersistentId: registrationPayload.PersistentId,
		WorkloadId:   registrationPayload.WorkloadId,
	}

	d.log.Debug("Kernel replica spec: %v", kernelReplicaSpec)

	// If we're running in Kubernetes mode, then we need to create a new kernel client here (as well as a new DockerInvoker).
	// If we're running in Docker mode, then we'll already have created the kernel client for this kernel.
	// We create the kernel client in Docker mode when we launch the kernel (using a DockerInvoker).
	var (
		kernel                      scheduling.KernelReplica
		kernelClientCreationChannel chan *proto.KernelConnectionInfo
		loaded                      bool
		kernelConnectionInfo        *proto.KernelConnectionInfo
	)
	if d.deploymentMode == types.KubernetesMode {
		invokerOpts := &invoker.DockerInvokerOptions{
			RemoteStorageEndpoint:                d.remoteStorageEndpoint,
			RemoteStorage:                        d.remoteStorage,
			KernelDebugPort:                      -1,
			DockerStorageBase:                    d.dockerStorageBase,
			RunKernelsInGdb:                      d.runKernelsInGdb,
			IsInDockerSwarm:                      d.DockerSwarmMode(),
			PrometheusMetricsPort:                d.prometheusPort,
			ElectionTimeoutSeconds:               d.electionTimeoutSeconds,
			SimulateCheckpointingLatency:         d.SimulateCheckpointingLatency,
			SimulateWriteAfterExec:               d.schedulingPolicy.PostExecutionStatePolicy().ShouldPerformWriteOperation(),
			SimulateWriteAfterExecOnCriticalPath: d.schedulingPolicy.PostExecutionStatePolicy().WriteOperationIsOnCriticalPath(),
			SmrEnabled:                           d.schedulingPolicy.SmrEnabled(),
			UseRealGpus:                          d.useRealGpus,
			BindDebugpyPort:                      d.bindDebugpyPort,
			SaveStoppedKernelContainers:          d.saveStoppedKernelContainers,
		}

		dockerInvoker := invoker.NewDockerInvoker(d.connectionOptions, invokerOpts, d.prometheusManager)
		kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, dockerInvoker)
		// We're passing "" for the persistent ID here; we'll re-assign it once we receive the persistent ID from the internalCluster Gateway.
		kernel = client.NewKernelReplicaClient(kernelCtx, kernelReplicaSpec, connInfo, d.id,
			d.numResendAttempts, registrationPayload.PodOrContainerName, registrationPayload.NodeName,
			d.smrReadyCallback, d.smrNodeAddedCallback, d.MessageAcknowledgementsEnabled, "", d.id, nil,
			metrics.LocalDaemon, false, false, d.DebugMode, d.prometheusManager, d.kernelReconnectionFailed,
			d.kernelRequestResubmissionFailedAfterReconnection, nil)

		kernelConnectionInfo, err = d.initializeKernelClient(registrationPayload.Kernel.Id, connInfo, kernel)
		if err != nil {
			errorMessage := fmt.Sprintf("Failed to initialize replica %d of kernel %s because: %v", registrationPayload.ReplicaId, registrationPayload.Kernel.Id, err)
			d.log.Error(errorMessage)
			go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
				Id:               uuid.NewString(),
				Title:            "Failed to Register Kernel.",
				Message:          errorMessage,
				NotificationType: 0,
				Panicked:         false,
			})

			// Write an error back to the kernel that registered with us.
			payload := map[string]interface{}{
				"status":  "error",
				"error":   "Failed to Register",
				"message": fmt.Sprintf("Could not initialize kernel client because: %s", errorMessage),
			}
			payloadJson, _ := json.Marshal(payload)
			_, _ = kernelRegistrationClient.conn.Write(payloadJson)

			// TODO: Handle this more gracefully.
			return
		}

		// Buffered so we don't get stuck sending a "stop" notification to a goroutine that is processed an "execute_request" message.
		executeRequestStopChan := make(chan interface{}, 1)
		executeRequestQueue := make(chan *enqueuedExecOrYieldRequest, DefaultExecuteRequestQueueSize)
		d.outgoingExecuteRequestQueue.Store(kernelReplicaSpec.Kernel.Id, executeRequestQueue)
		d.outgoingExecuteRequestQueueMutexes.Store(kernelReplicaSpec.Kernel.Id, &sync.Mutex{})
		d.executeRequestQueueStopChannels.Store(kernelReplicaSpec.Kernel.Id, executeRequestStopChan)

		go d.executeRequestForwarder(executeRequestQueue, executeRequestStopChan, kernel)
	} else {
		kernelClientCreationChannel, loaded = d.kernelClientCreationChannels.Load(kernelReplicaSpec.Kernel.Id)
		if !loaded {
			err := fmt.Errorf("failed to load 'kernel client creation' channel for kernel \"%s\"", kernelReplicaSpec.Kernel.Id)
			d.notifyClusterGatewayAndPanic("Failed to Load 'Kernel Client Creation' Channel", err.Error(), err)
		}

		d.log.Debug("Waiting for notification that the KernelClient for kernel \"%s\" has been created.", kernelReplicaSpec.Kernel.Id)
		kernelConnectionInfo = <-kernelClientCreationChannel
		d.log.Debug("Received notification that the KernelClient for kernel \"%s\" was created.", kernelReplicaSpec.Kernel.Id)

		kernel, loaded = d.kernels.Load(kernelReplicaSpec.Kernel.Id)

		if !loaded {
			message := fmt.Sprintf("Failed to load kernel client with ID \"%s\", even though one should have already been created...", kernelReplicaSpec.Kernel.Id)
			d.notifyClusterGatewayAndPanic("Failed to Load Kernel Client for New Kernel Replica", message, message)
		}

		createdAt, ok := d.getInvoker(kernel).KernelCreatedAt()
		if !ok {
			message := "Docker Invoker thinks it hasn't created kernel container, but kernel just registered..."
			d.notifyClusterGatewayAndPanic("Docker Invoker Thinks Container Has Not Been Created", message, message)
		}

		timeElapsed := registeredAt.Sub(createdAt)
		d.log.Debug("Kernel %s-%d is registering %v after its Docker container was created.",
			kernelReplicaSpec.Kernel.Id, kernelReplicaSpec.ReplicaId, timeElapsed)
	}

	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager(manager.py)
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	// TODO: Is this any different than the KernelConnectionInfo object that's created while initializing the kernel client?
	// If not, then we should just return the `kernelConnectionInfo` variable directly (that's the one created when initializing the kernel client).
	info := &proto.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(messaging.ControlMessage).Port),
		ShellPort:       kernelConnectionInfo.ShellPort,
		StdinPort:       int32(d.router.Socket(messaging.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(messaging.HBMessage).Port),
		IopubPort:       kernelConnectionInfo.IopubPort, // TODO(Ben): Are these still needed? I think so...
		IosubPort:       kernelConnectionInfo.IosubPort, // TODO(Ben): Are these still needed? I think so...
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}

	kernelRegistrationNotification := &proto.KernelRegistrationNotification{
		ConnectionInfo:     info,
		KernelId:           kernel.ID(),
		HostId:             d.id,
		SessionId:          "N/A",
		ReplicaId:          registrationPayload.ReplicaId,
		KernelIp:           remoteIp,
		PodOrContainerName: registrationPayload.PodOrContainerName,
		NodeName:           d.nodeName,
		NotificationId:     uuid.NewString(),
	}

	var dockerContainerId string
	if d.DockerMode() {
		containerStartedNotification := d.containerStartedNotificationManager.GetAndDeleteNotification(kernel.ID())
		dockerContainerId = containerStartedNotification.FullContainerId

		kernel.SetPodOrContainerName(dockerContainerId)
		kernel.SetNodeName(d.nodeName)

		kernelRegistrationNotification.DockerContainerId = dockerContainerId
		kernelRegistrationNotification.NodeName = d.nodeName
		kernelRegistrationNotification.PodOrContainerName = dockerContainerId
	}

	d.log.Info("Kernel %s registered: %v. Notifying Gateway now.", kernelReplicaSpec.ID(), info)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelPing()

	_, err = d.provisioner.PingGateway(pingCtx, proto.VOID)
	if err != nil {
		d.log.Error("PingGateway RPC failed... we're in trouble. Error was: %v", err)
	}

	numTries := 0
	maxNumTries := 3

	// TODO: Figure out a better way to handle this. As of right now, we really cannot recover from this.
	var response *proto.KernelRegistrationNotificationResponse
	for response == nil && numTries < maxNumTries {
		// NOTE: If you feel inclined to add a context.WithTimeout here -- is it REALLY necessary?
		// Because often, there is either a delay or an error on the Gateway side (e.g., some sort of deadlock),
		// rather than a connectivity problem here, and resubmitting the request won't help.
		response, err = d.provisioner.NotifyKernelRegistered(context.Background(), kernelRegistrationNotification)

		if err != nil {
			d.log.Error("Failed to notify Cluster Gateway that kernel %s has registered on attempt %d/%d: %v",
				kernelReplicaSpec.ID(), numTries+1, maxNumTries, err)

			if errors.Is(err, types.ErrDuplicateRegistrationNotification) {
				// TODO: What to do here? If the Gateway received our request but then there was some sort
				//       of error, then we need a way to get the response. The Gateway could call an RPC to
				//		 this Local Daemon with the response, perhaps. But is it obvious that the Gateway will
				//	     know to do this? Like, will the connection error be visible to the Gateway, or is it just
				// 		 going to return from the NotifyKernelRegistered RPC handler like nothing is wrong?
				//
				//	     In any case, if it becomes a problem, then the Gateway can either send the result proactively
				//		 via some other RPC, or if the Local Daemon gets a types.ErrDuplicateRegistrationNotification
				//	     error, then maybe the Local Daemon can periodically call some other RPC (yet to be implemented)
				//		 that either attempts to receive the result of the registration, OR just blocks until that
				//		 result is available.
				//
				// 		 For now, we panic, as we have no way to handle this.
				notifyCtx, cancelNotify := context.WithTimeout(context.Background(), time.Second*10)
				d.notifyClusterGatewayOfError(notifyCtx, &proto.Notification{
					Id:    uuid.NewString(),
					Title: "Local Daemon Sent Duplicate \"Kernel Registered\" Message",
					Message: fmt.Sprintf("Local Daemon %s (ID=%s) sent a duplicate \"kernel registered\" notification for replica %d of kernel %s.",
						d.nodeName, d.id, registrationPayload.ReplicaId, kernel.ID()),
					NotificationType: 0,
					Panicked:         true,
				})
				cancelNotify()
				log.Fatalf(utils.RedStyle.Render("Sent duplicate a duplicate \"kernel registered\" notification for replica %d of kernel %s.\n"),
					registrationPayload.ReplicaId, kernel.ID())
			}

			// Convert the golang error to a gRPC Status struct for additional information.
			if statusError, ok := status.FromError(err); ok {
				d.log.Error("Received gRPC error with statusError code %d: %s.",
					statusError.Code(), statusError.Message())
				details := statusError.Details()
				if len(details) > 0 {
					d.log.Error("Additional details associated with gRPC error: %v", details)
				}
			}

			// Attempt to re-establish connection with Cluster Gateway.
			grpcConnState := d.provisionerClientConnectionGRPC.GetState()
			d.log.Error("gRPC Client Connection for Provisioner is in state \"%s\"", grpcConnState.String())
			if grpcConnState != connectivity.Ready {
				d.log.Warn("Attempting to re-establish provisioner gRPC connection with Cluster Gateway...")

				// TODO: This will cause SetID to be called. We may need to wait to try again until that process
				// finishes. We also need to implement that process.
				err = d.connectToGateway(d.localDaemonOptions.ProvisionerAddr, func(v bool) {
					d.log.Error("The finalize function passed to connectToGateway when registerKernelReplica fails to notify the provisioner was called with argument %v", v)
				})
				if err != nil {
					log.Fatalf(utils.RedStyle.Render("Failed to re-establish connectivity with the Cluster Gateway: %v"), err)
				}
			}

			numTries += 1

			// If we're not done (i.e., if this loop will execute again because we've not exhausted our attempts),
			// then we'll sleep. If we've already exhausted our attempts, then there's no point in sleeping again.
			if numTries < maxNumTries {
				time.Sleep(time.Millisecond * 100 * time.Duration(numTries))
			}
		} else {
			break
		}
	}

	if response == nil {
		d.log.Error("Failed to notify Gateway of kernel registration after %d attempts.", maxNumTries)
		panic(domain.ErrKernelRegistrationNotificationFailure)
	}

	d.log.Debug("Successfully notified Gateway of kernel registration. Will be assigning replica ID of %d to kernel. Replicas: %v. Response: %v.",
		response.Id, response.Replicas, response.String())

	if response.ResourceSpec == nil {
		errorMessage := fmt.Sprintf("ResourceSpec for kernel %s is nil.", kernel.ID())
		d.log.Error(errorMessage)
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Null Resource Spec",
			Message:          errorMessage,
			NotificationType: 0,
			Panicked:         false,
		})

		// Write an error back to the kernel that registered with us.
		payload := map[string]interface{}{
			"status":  "error",
			"error":   "Null Resource Spec",
			"message": "The ResourceSpec included in the registration payload is nil",
		}
		payloadJson, _ := json.Marshal(payload)
		_, _ = kernelRegistrationClient.conn.Write(payloadJson)

		return
	}

	d.log.Debug("Resource spec for kernel %s: %v", kernel.ID(), response.ResourceSpec)

	kernel.InitializeResourceSpec(response.ResourceSpec)
	kernel.SetReplicaID(response.Id)

	var kernelDebugPort = -1
	kernelDebugPort, _ = d.kernelDebugPorts.Load(kernel.ID())

	payload := map[string]interface{}{
		"smr_node_id":                      response.Id,
		"hostname":                         remoteIp,
		"replicas":                         response.Replicas,
		"debug_port":                       kernelDebugPort,
		"status":                           "ok",
		"grpc_port":                        d.kernelErrorReporterServerPort,
		"message_acknowledgements_enabled": d.MessageAcknowledgementsEnabled,
	}

	if response.PersistentId != nil && response.GetPersistentId() != "" {
		d.log.Debug("Including persistent store ID \"%s\" in notification response to replica %d of kernel %s.", response.GetPersistentId(), response.Id, kernel.ID())
		payload["persistent_id"] = response.GetPersistentId()
		kernel.SetPersistentID(response.GetPersistentId())
	} else {
		d.log.Debug("No persistent ID to include in response.")
	}

	payload["should_read_data_from_remote_storage"] = response.ShouldReadDataFromRemoteStorage

	// if response.DataDirectory != nil && response.GetDataDirectory() != "" {
	// 	d.log.Debug("Including data directory \"%s\" in notification response to replica %d of kernel %s.", response.GetDataDirectory(), response.Id, kernel.ID())
	// 	payload["data_directory"] = response.GetDataDirectory()
	// } else {
	// 	d.log.Debug("No data directory to include in response.")
	// }

	payloadJson, err := json.Marshal(payload)
	if err != nil {
		d.log.Error("Error encountered while marshalling registration response payload to JSON: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		d.notifyClusterGatewayAndPanic("Failed to Marshal Registration Response Payload", err.Error(), err)
	}

	bytesWritten, err := kernelRegistrationClient.conn.Write(payloadJson)
	if err != nil {
		d.log.Error("Error encountered while writing registration response payload back to kernel: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		d.notifyClusterGatewayAndPanic("Failed to Write Registration Response Payload Back to Kernel", err.Error(), err)
	}
	d.log.Debug("Wrote %d bytes back to kernel in response to kernel registration.", bytesWritten)

	// TODO(Ben): Need a better system for this. Basically, give the kernel time to setup its persistent store.
	// TODO: Is this still needed?
	// time.Sleep(time.Second * 1)
}

// ReconnectToGateway is used to force the Local Daemon to reconnect to the Cluster Gateway.
//
// The reconnection procedure is optionally initiated shortly after the ReconnectToGateway gRPC call returns,
// to avoid causing the ReconnectToGateway to encounter an error.
func (d *SchedulerDaemonImpl) ReconnectToGateway(_ context.Context, in *proto.ReconnectToGatewayRequest) (*proto.Void, error) {
	if in.Delay {
		d.log.Warn("We've been instructed to reconnect to the Cluster Gateway. Will initiate procedure after returning from ReconnectToGateway gRPC handler.")
	} else {
		d.log.Warn("We've been instructed to reconnect to the Cluster Gateway immediately.")
	}

	go func() {
		if in.Delay {
			// Sleep for 3 seconds so that the gRPC handler can return.
			for i := 3; i > 0; i-- {
				d.log.Warn("Forcibly terminating and then reestablishing connection with Cluster Gateway in %d seconds...", i)
				time.Sleep(time.Second)
			}
		}

		err := d.connectToGateway(d.localDaemonOptions.ProvisionerAddr, func(v bool) {
			d.log.Error(utils.RedStyle.Render("The finalize function passed to ReconnectToGateway->connectToGateway was called with argument %v"), v)
		})
		if err != nil {
			d.log.Error(utils.RedStyle.Render("Failed to reconnect to Cluster Gateway after explicit instruction to do so: %v"), err)
		}
	}()

	return proto.VOID, nil
}

// notifyClusterGatewayAndPanic attempts to notify the internalCluster Gateway of a fatal error and then panics.
// notifyClusterGatewayAndPanic's attempt to notify the internalCluster Gateway will time out after 30 seconds.
func (d *SchedulerDaemonImpl) notifyClusterGatewayAndPanic(errorTitle string, errorBody string, panicArg interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	d.notifyClusterGatewayOfError(ctx, &proto.Notification{
		Id:               uuid.NewString(),
		Title:            errorTitle,
		Message:          errorBody,
		NotificationType: 0,
		Panicked:         true,
	})

	panic(panicArg)
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (d *SchedulerDaemonImpl) kernelReconnectionFailed(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, reconnectionError error) {
	// var messageType string = "N/A"
	// _, header, _, err := jupyter.HeaderFromMsg(msg)
	// if err != nil {
	// 	d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
	// 	d.log.Error("ZMQ message in question: %v", msg)
	// } else {
	// 	messageType = header.MsgType
	// }

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending \"%s\" message: %v", kernel.ReplicaID(), kernel.ID(), msg.JupyterMessageType(), reconnectionError)
	d.log.Error(errorMessage)

	go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
		Id:               uuid.NewString(),
		Title:            "Connection to Kernel Lost & Reconnection Failed",
		Message:          errorMessage,
		NotificationType: 0,
		Panicked:         false,
	})
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
func (d *SchedulerDaemonImpl) kernelRequestResubmissionFailedAfterReconnection(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, resubmissionError error) {
	// var messageType string = "N/A"
	// _, header, _, err := jupyter.HeaderFromMsg(msg)
	// if err != nil {
	// 	d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
	// 	d.log.Error("ZMQ message in question: %v", msg)
	// } else {
	// 	messageType = header.MsgType
	// }

	errorMessage := fmt.Sprintf("Failed to forward \"'%s'\" request to replica %d of kernel %s following successful connection re-establishment because: %v", msg.JupyterMessageType(), kernel.ReplicaID(), kernel.ID(), resubmissionError)
	d.log.Error(errorMessage)

	go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
		Id:               uuid.NewString(),
		Title:            "Connection to Kernel Lost, Reconnection Succeeded, but Request Resubmission Failed",
		Message:          errorMessage,
		NotificationType: 0,
		Panicked:         false,
	})
}

func (d *SchedulerDaemonImpl) smrReadyCallback(kernelClient scheduling.KernelReplica) {
	d.log.Debug("Replica %d of kernel %s is ready to join its SMR cluster.", kernelClient.ReplicaID(), kernelClient.ID())

	_, err := d.provisioner.SmrReady(context.TODO(), &proto.SmrReadyNotification{
		KernelId:     kernelClient.ID(),
		ReplicaId:    kernelClient.ReplicaID(),
		PersistentId: kernelClient.PersistentID(),
		Address:      kernelClient.Address(),
	})

	if err != nil {
		d.log.Error("Error when informing Gateway of SMR-Ready for replica %d of kernel %s: %v", kernelClient.ReplicaID(), kernelClient.ID(), err)
	}
}

// GetActualGpuInfo returns the "actual" GPU resource information for the node.
//
// Deprecated: this should eventually be merged with the updated/unified ModifyClusterNodes API.
func (d *SchedulerDaemonImpl) GetActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.GpuInfo, error) {
	gpuInfo := &proto.GpuInfo{
		SpecGPUs:              int32(d.resourceManager.SpecGPUs().InexactFloat64()),
		IdleGPUs:              int32(d.resourceManager.IdleGPUs().InexactFloat64()),
		CommittedGPUs:         int32(d.resourceManager.CommittedGPUs().InexactFloat64()),
		PendingGPUs:           int32(d.resourceManager.PendingGPUs().InexactFloat64()),
		NumPendingAllocations: int32(d.resourceManager.NumAllocations()),
		NumAllocations:        int32(d.resourceManager.NumPendingAllocations()),
		GpuSchedulerID:        d.resourceManager.ID,
		LocalDaemonID:         d.id,
	}

	// d.log.Debug("Returning GPU information: %v", gpuInfo)

	return gpuInfo, nil
}

func (d *SchedulerDaemonImpl) PingKernel(_ context.Context, _ *proto.PingInstruction) (*proto.Pong, error) {
	return nil, domain.ErrNotImplemented
}

func (d *SchedulerDaemonImpl) PrepareToMigrate(_ context.Context, req *proto.ReplicaInfo) (*proto.PrepareToMigrateResponse, error) {
	kernelId := req.KernelId
	replicaId := req.ReplicaId
	d.log.Debug("Preparing to migrate replica %d of kernel %s now.", req.ReplicaId, req.KernelId)

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find BasicKernelReplicaClient for kernel %s.", kernelId)
		return nil, domain.ErrInvalidParameter
	}

	frames := messaging.NewJupyterFramesWithHeader(messaging.MessageTypePrepareToMigrateRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&messaging.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: kernel.Address(),
	}); err != nil {
		d.log.Error("Failed to encode JupyterFrames for \"MessageSMRAddOrUpdateReplicaRequest\" message because: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for prepare-to-migrate request to replica %d of kernel %s: %v", replicaId, kernelId, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	d.log.Debug("Sending Jupyter 'prepare-to-migrate' request to replica %d of kernel %s now.", req.ReplicaId, req.KernelId)
	_msg := &zmq4.Msg{Frames: frames.Frames}
	jMsg := messaging.NewJupyterMessage(_msg)
	var requestWG sync.WaitGroup
	requestWG.Add(1)
	// var dataDirectory string

	err := kernel.RequestWithHandler(context.Background(), "Sending", messaging.ControlMessage, jMsg, func(kernel scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		d.log.Debug("Received response from 'prepare-to-migrate' request.")

		for i, frame := range msg.JupyterFrames.Frames {
			d.log.Debug("Frame #%d: %s", i, string(frame))
		}

		var respMessage messaging.MessageDataDirectory
		if err := msg.JupyterFrames.Validate(); err != nil {
			d.log.Error("Failed to validate frames of `MessageDataDirectory` message: %v", err)
			return err
		}
		err := msg.JupyterFrames.DecodeContent(&respMessage)
		if err != nil {
			d.log.Error("Failed to decode Content frame of `MessageDataDirectory` message: %v", err)
			return err
		}

		// It seems like this particular message is sent in the 6th frame, which is the Buffers frame, rather than the Content frame.
		if respMessage.NodeID == 0 {
			err := msg.JupyterFrames.DecodeBuffers(&respMessage)

			if err != nil {
				d.log.Error("Failed to decode Buffer frame of `MessageDataDirectory` message: %v", err)
				return err
			}
		}

		// dataDirectory = respMessage.DataDirectory
		if respMessage.Status == "error" {
			var msgErr messaging.MessageError
			err := msg.JupyterFrames.DecodeBuffers(&msgErr)
			if err != nil {
				d.log.Error("Failed to decode ErrorMessage from JupyterMessage content: %v", err)
				return err
			}

			d.log.Error("Error encountered by kernel %s while it was preparing to migrate: %v", kernel.ID(), msgErr)
			return fmt.Errorf("ErrPrepareToMigrateFailed (%s) -- %s: %s", msgErr.Status, msgErr.ErrName, msgErr.ErrValue)
		} else {
			d.log.Debug("Response from 'prepare-to-migrate' request: %s", respMessage.String())
		}

		return nil
	}, requestWG.Done)
	if err != nil {
		d.log.Error("Error occurred while issuing prepare-to-migrate request to replica %d of kernel %s: %v", replicaId, kernelId, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	requestWG.Wait()
	d.log.Debug("Prepare-to-migrate request to replica %d of kernel %s succeeded.", replicaId, kernelId)

	return &proto.PrepareToMigrateResponse{
		Id:       replicaId,
		KernelId: kernelId,
	}, nil
}

// YieldNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (d *SchedulerDaemonImpl) YieldNextExecution(_ context.Context, in *proto.KernelId) (*proto.Void, error) {
	kernelId := in.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find BasicKernelReplicaClient for kernel %s specified in 'YieldNextExecution' request.", kernelId)
		return proto.VOID, domain.ErrInvalidParameter
	}

	d.log.Debug("Kernel %s will YIELD its next execution request.", in.Id)

	kernel.YieldNextExecutionRequest()
	return proto.VOID, nil
}

func (d *SchedulerDaemonImpl) UpdateReplicaAddr(_ context.Context, req *proto.ReplicaInfoWithAddr) (*proto.Void, error) {
	kernelId := req.KernelId
	hostname := req.Hostname // The new hostname of the replica.
	replicaId := req.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find BasicKernelReplicaClient for kernel %s.", kernelId)
		return proto.VOID, domain.ErrInvalidParameter
	}

	d.log.Debug("Informing replicas of kernel %s to update address of replica %d to %s.", kernelId, replicaId, hostname)
	frames := messaging.NewJupyterFramesWithHeader(messaging.MessageTypeUpdateReplicaRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&messaging.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: hostname,
	}); err != nil {
		d.log.Error("Error occurred while encoding the content frame for update-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for update-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	_msg := &zmq4.Msg{Frames: frames.Frames}
	jMsg := messaging.NewJupyterMessage(_msg)

	var currentNumTries = 0
	var maxNumTries = 3
	var success = false
	for currentNumTries < maxNumTries {
		var wg sync.WaitGroup
		var requestReceived int32 = 0
		wg.Add(1)
		err := kernel.RequestWithHandler(context.Background(), "Sending", messaging.ControlMessage, jMsg, func(kernel scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
			d.log.Debug("Received response from 'update-replica' request.")

			// Record that we've received the response.
			swapped := atomic.CompareAndSwapInt32(&requestReceived, 0, 1)
			if !swapped {
				d.log.Warn("Apparently we've already received a request for prepare-to-migrate request sent to replica %d of kernel %s: %v", replicaId, kernelId)
			}

			return nil
		}, wg.Done)
		if err != nil {
			d.log.Error("Error occurred while issuing update-replica request to kernel %s: %v", kernelId, err)
			return proto.VOID, err
		}
		wg.Wait()

		// Because of how requests are handled under the covers, the value of `requestReceived` will necessarily be 1 at this point
		// if we received a response. This is because the handler is called BEFORE Done() is called on the 'requestWG'.
		if atomic.LoadInt32(&requestReceived) == 0 {
			d.log.Error("TIMED-OUT: 'Update-replica' request to replica %d of kernel %s did not complete in time.", replicaId, kernelId)
			currentNumTries++
			sleepAmount := time.Millisecond * 1500 * time.Duration(currentNumTries)
			d.log.Error("Sleeping for %d ms before trying again.", sleepAmount)
			time.Sleep(sleepAmount)
			wg.Add(1)
		} else {
			success = true
			break
		}
	}

	if !success {
		return proto.VOID, domain.ErrRequestFailed
	}

	return proto.VOID, nil
}

// AddReplica prompts the SchedulerDaemonImpl to send an "add_replica_request" CONTROL message to the specified kernel.
//
// NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called.
func (d *SchedulerDaemonImpl) AddReplica(_ context.Context, req *proto.ReplicaInfoWithAddr) (*proto.Void, error) {
	kernelId := req.KernelId
	hostname := req.Hostname
	replicaId := req.Id

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find BasicKernelReplicaClient for kernel %s.", kernelId)
		return proto.VOID, domain.ErrInvalidParameter
	}

	d.log.Debug("Now that replica %d of kernel %s (host=%s) has been added, notify the existing members.", replicaId, kernelId, hostname)
	frames := messaging.NewJupyterFramesWithHeader(messaging.MessageTypeAddReplicaRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&messaging.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: hostname,
	}); err != nil {
		d.log.Error("Failed to encode content of \"MessageSMRAddOrUpdateReplicaRequest\" message: %v", err)
		return nil, err
	}

	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for add-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	_msg := &zmq4.Msg{Frames: frames.Frames}
	jMsg := messaging.NewJupyterMessage(_msg)

	var wg sync.WaitGroup
	wg.Add(1)

	err := kernel.RequestWithHandler(context.Background(), "Sending", messaging.ControlMessage, jMsg, nil, wg.Done)

	if err != nil {
		d.log.Error("Error occurred while issuing add-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	wg.Wait()

	return proto.VOID, nil
}

// smrNodeAddedCallback is a callback passed to the Kernel of a kernel such that, when the kernel
// client receives a "smr_node_added" IOPub message, it will call the smrNodeAddedCallback method so that
// the Local Daemon can notify the Cluster Gateway.
//
// NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called.
func (d *SchedulerDaemonImpl) smrNodeAddedCallback(readyMessage *messaging.MessageSMRNodeUpdated) {
	if readyMessage.Success {
		d.log.Debug("Replica %d of kernel %s has successfully joined its SMR cluster.", readyMessage.NodeID, readyMessage.KernelId)
	} else {
		d.log.Error("Replica %d of kernel %s has failed to join its SMR cluster.", readyMessage.NodeID, readyMessage.KernelId)

		// TODO(Ben): Handle this somehow.
		return
	}

	_, err := d.provisioner.SmrNodeAdded(context.TODO(), &proto.ReplicaInfo{
		KernelId:     readyMessage.KernelId,
		ReplicaId:    readyMessage.NodeID,
		PersistentId: readyMessage.PersistentID,
	})

	if err != nil {
		// TODO(Ben): Handle this somehow.
		d.log.Error("Error when informing Gateway of SMR Node-Added for replica %d of kernel %s: %v", readyMessage.NodeID, readyMessage.KernelId, err)
	}
}

// DockerComposeMode returns true if we're running in Docker via "docker compose".
// If we're running via "docker swarm", then DockerComposeMode returns false.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, DockerComposeMode also returns false.
func (d *SchedulerDaemonImpl) DockerComposeMode() bool {
	return d.deploymentMode == types.DockerComposeMode
}

// DockerSwarmMode returns true if we're running in Docker via "docker swarm".
// If we're running via "docker compose", then DockerSwarmMode returns false.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, DockerSwarmMode also returns false.
func (d *SchedulerDaemonImpl) DockerSwarmMode() bool {
	return d.deploymentMode == types.DockerSwarmMode
}

// DockerMode returns true if we're running in either "docker swarm" or "docker compose".
// That is, DockerMode turns true if and only if one of DockerSwarmMode or DockerComposeMode return true.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *SchedulerDaemonImpl) DockerMode() bool {
	return d.DockerComposeMode() || d.DockerSwarmMode()
}

// KubernetesMode returns true if we're running in Kubernetes.
func (d *SchedulerDaemonImpl) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// LocalMode returns true if we're running in DefaultSchedulingPolicy mode.
func (d *SchedulerDaemonImpl) LocalMode() bool {
	return d.deploymentMode == types.LocalMode
}

// Initialize a kernel client for a new kernel.
// Initialize shell/IO forwarders, validate the connection with the kernel (which includes connecting to the ZMQ sockets), etc.
// If there's an error at any point during the initialization process, then the kernel connection/client is closed and an error is returned.
func (d *SchedulerDaemonImpl) initializeKernelClient(id string, connInfo *jupyter.ConnectionInfo, kernel scheduling.KernelReplica) (*proto.KernelConnectionInfo, error) {
	shell := d.router.Socket(messaging.ShellMessage)
	if d.schedulerDaemonOptions.DirectServer {
		var err error
		shell, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			d.log.Error("Failed to initialize shell forwarder (ZMQ shell socket) for kernel %s because: %v", id, err)
			d.closeKernel(kernel, "failed initializing shell forwarder")
			return nil, status.Errorf(codes.Internal, err.Error())
		}

		d.log.Debug("Successfully initialized shell forwarder for kernel \"%s\"", id)
	}

	iopub, err := kernel.InitializeIOForwarder()
	if err != nil {
		d.log.Error("Failed to initialize IO forwarder (ZMQ IO Pub socket) for kernel %s because: %v", id, err)
		d.closeKernel(kernel, "failed initializing io forwarder")
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	iosub, err := kernel.InitializeIOSub(nil, "")
	if err != nil {
		d.log.Error("Failed to initialize IO SUB socket. Error: %v", err)
		d.closeKernel(kernel, fmt.Sprintf("Failed to initialize IO SUB socket. Error: %v", err))
		return nil, err
	}

	if err := kernel.Validate(); err != nil {
		d.log.Error("Failed to validate connection with new kernel %s because: %v", id, err)
		d.closeKernel(kernel, "validation error")
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Handle kernel response.
	_ = kernel.AddIOHandler(messaging.MessageTypeSMRLeadTask, d.handleSMRLeadTask)
	_ = kernel.AddIOHandler(messaging.MessageTypeErrorReport, d.handleErrorReport)

	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager (manager.py).
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	info := &proto.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(messaging.ControlMessage).Port),
		ShellPort:       int32(shell.Port),
		StdinPort:       int32(d.router.Socket(messaging.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(messaging.HBMessage).Port),
		IopubPort:       int32(iopub.Port),
		IosubPort:       int32(iosub.Port),
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}

	d.log.Info("Kernel %s started: %v", id, info)

	return info, nil
}

// StartKernelReplica launches a new kernel via Docker.
// This is ONLY used in the Docker-based deployment mode.
func (d *SchedulerDaemonImpl) StartKernelReplica(ctx context.Context, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if in == nil {
		d.log.Error("`gateway.KernelReplicaSpec` argument is nil in call to StartKernelReplica...")
		return nil, ErrNilArgument
	}

	if in.Kernel == nil {
		d.log.Error("The `gateway.KernelSpec` field within the gateway.KernelReplicaSpec argument is nil in call to StartKernelReplica...")
		d.log.Error("gateway.KernelReplicaSpec: %v", in)
		return nil, ErrNilArgument
	}

	kernelId := in.Kernel.Id
	d.log.Debug("SchedulerDaemonImpl::StartKernelReplica(\"%s\"). Spec: %v.", kernelId, in)

	if otherReplica, loaded := d.kernels.Load(kernelId); loaded {
		d.log.Error("We already have a replica of kernel %s running locally (replica %d). Cannot launch new replica on this node.", kernelId, otherReplica.ReplicaID())
		return nil, ErrExistingReplicaAlreadyRunning
	}

	// We always create a pending resource allocation (i.e., for any scheduling policy).
	resourceError := d.resourceManager.KernelReplicaScheduled(in.ReplicaId, in.Kernel.Id, in.Kernel.ResourceSpec)
	if resourceError != nil {
		d.log.Error("Failed to allocate %d pending GPUs for new replica %d of kernel %s because: %v",
			in.Kernel.ResourceSpec.Gpu, in.ReplicaId, in.Kernel.Id, resourceError)
		return nil, status.Error(codes.Internal, resourceError.Error())
	}

	// If we're performing FCFS batch scheduling, then we commit resources right away.
	if d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesWhenContainerScheduled {
		_, resourceError = d.resourceManager.CommitResources(in.ReplicaId, in.Kernel.Id, in.Kernel.ResourceSpec, false)

		if resourceError != nil {
			d.log.Error("Failed to allocate %d committed GPUs for new replica %d of kernel %s because: %v",
				in.Kernel.ResourceSpec.Gpu, in.ReplicaId, in.Kernel.Id, resourceError)
			return nil, status.Error(codes.Internal, resourceError.Error())
		}
	}

	var kernelInvoker invoker.KernelInvoker
	if d.DockerMode() {
		invokerOpts := &invoker.DockerInvokerOptions{
			RemoteStorageEndpoint:                d.remoteStorageEndpoint,
			RemoteStorage:                        d.remoteStorage,
			KernelDebugPort:                      int(in.DockerModeKernelDebugPort),
			DockerStorageBase:                    d.dockerStorageBase,
			RunKernelsInGdb:                      d.runKernelsInGdb,
			SimulateCheckpointingLatency:         d.SimulateCheckpointingLatency,
			IsInDockerSwarm:                      d.DockerSwarmMode(),
			PrometheusMetricsPort:                d.prometheusPort,
			ElectionTimeoutSeconds:               d.electionTimeoutSeconds,
			SimulateWriteAfterExec:               d.schedulingPolicy.PostExecutionStatePolicy().ShouldPerformWriteOperation(),
			SimulateWriteAfterExecOnCriticalPath: d.schedulingPolicy.PostExecutionStatePolicy().WriteOperationIsOnCriticalPath(),
			WorkloadId:                           in.WorkloadId,
			SmrEnabled:                           d.schedulingPolicy.SmrEnabled(),
			UseRealGpus:                          d.useRealGpus,
			BindDebugpyPort:                      d.bindDebugpyPort,
			SaveStoppedKernelContainers:          d.saveStoppedKernelContainers,
		}
		kernelInvoker = invoker.NewDockerInvoker(d.connectionOptions, invokerOpts, d.prometheusManager.GetContainerMetricsProvider())
		// Note that we could pass d.prometheusManager directly in the call above.

		d.kernelDebugPorts.Store(kernelId, int(in.DockerModeKernelDebugPort))
	} else if d.LocalMode() {
		kernelInvoker = invoker.NewLocalInvoker()
	} else {
		message := fmt.Sprintf("Unknown/unsupported deployment mode: \"%s\"", d.deploymentMode)
		d.notifyClusterGatewayAndPanic("Unknown or Unsupported Deployment Mode", message, message)
	}

	// When the kernel registers, we need the kernel client that we create here.
	// We use this channel to notify the goroutine handling the registration that the kernel client is set up and connected.
	kernelClientCreationChannel := make(chan *proto.KernelConnectionInfo)
	d.kernelClientCreationChannels.Store(in.Kernel.Id, kernelClientCreationChannel)

	// Clear any existing "container started" notifications associated with the target kernel.
	if d.containerStartedNotificationManager != nil {
		deleted := d.containerStartedNotificationManager.DeleteNotification(in.Kernel.Id)
		if deleted {
			d.log.Warn("Deleting existing 'Container Started' notification associated with some replica of kernel %s.",
				in.Kernel.Id)
		}
	}

	// We already know for a fact that kernelInvoker cannot be nil here.
	// We'll have either returned from this method or panicked in any of the cases in which
	// kernelInvoker is nil before this line of code is executed.
	// This is just to stop the IDE from complaining, as it (apparently) cannot detect the impossibility here.
	if kernelInvoker == nil {
		errorMessage := fmt.Errorf("invoker is nil when creating replica %d of kernel %s", in.ReplicaId, in.Kernel.Id)
		d.notifyClusterGatewayAndPanic(errorMessage.Error(), errorMessage.Error(), errorMessage)

		// This won't get executed, as the above call will panic.
		// This line is just here so that the IDE won't complain about kernelInvoker possibly being null below.
		return nil, errorMessage
	}

	connInfo, resourceError := kernelInvoker.InvokeWithContext(ctx, in)
	if resourceError != nil {
		go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            fmt.Sprintf("Failed to Create Container for Kernel %s-%d", in.Kernel.Id, in.ReplicaId),
			Message:          resourceError.Error(),
			NotificationType: 0,
			Panicked:         false,
		})
		return nil, status.Errorf(codes.Internal, resourceError.Error())
	}

	// Initialize kernel client with new context.
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, kernelInvoker)

	kernel := client.NewKernelReplicaClient(kernelCtx, in, connInfo, d.id, d.numResendAttempts,
		types.DockerContainerIdTBD, types.DockerNode, d.smrReadyCallback, d.smrNodeAddedCallback,
		d.MessageAcknowledgementsEnabled, "", d.id, nil, metrics.LocalDaemon, false,
		false, d.DebugMode, d.prometheusManager, d.kernelReconnectionFailed,
		d.kernelRequestResubmissionFailedAfterReconnection, nil)

	// Register kernel.
	d.kernels.Store(kernel.ID(), kernel)

	info, resourceError := d.initializeKernelClient(in.Kernel.Id, connInfo, kernel)
	if resourceError != nil {
		d.log.Error("Failed to initialize replica %d of kernel %s.", in.ReplicaId, in.Kernel.Id)
		return nil, resourceError
	}

	// Buffered so we don't get stuck sending a "stop" notification to a goroutine that is processed an "execute_request" message.
	executeRequestStopChan := make(chan interface{}, 1)
	executeRequestQueue := make(chan *enqueuedExecOrYieldRequest, DefaultExecuteRequestQueueSize)
	d.outgoingExecuteRequestQueue.Store(in.Kernel.Id, executeRequestQueue)
	d.outgoingExecuteRequestQueueMutexes.Store(in.Kernel.Id, &sync.Mutex{})
	d.executeRequestQueueStopChannels.Store(in.Kernel.Id, executeRequestStopChan)

	go d.executeRequestForwarder(executeRequestQueue, executeRequestStopChan, kernel)

	// Notify that the kernel client has been set up successfully.
	kernelClientCreationChannel <- info

	d.prometheusManager.TotalNumKernelsCounter.Inc()
	d.prometheusManager.NumActiveKernelReplicasGauge.Add(1)

	return info, nil
}

// GetKernelStatus returns the status of a kernel.
func (d *SchedulerDaemonImpl) GetKernelStatus(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Warn("Kernel %s not found on query status", in.Id)
		return nil, domain.ErrKernelNotFound
	}

	kernelStatus, err := d.getInvoker(kernel).Status()
	return d.statusErrorf(kernel, kernelStatus, err)
}

// KillKernel kills a kernel.
func (d *SchedulerDaemonImpl) KillKernel(_ context.Context, in *proto.KernelId) (*proto.Void, error) {
	d.log.Debug("KillKernel RPC called for kernel %s.", in.Id)

	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", in.Id)
		return nil, domain.ErrKernelNotFound
	}

	if err := d.errorf(d.getInvoker(kernel).Close()); err != nil {
		d.log.Error("Error while killing replica %d of kernel %s: %v", kernel.ReplicaID(), in.Id, err)
		return nil, err
	}

	// Release any resources allocated to the kernel.
	if err := d.resourceManager.ReplicaEvicted(kernel.ReplicaID(), in.Id); err != nil {
		d.log.Error("ResourceManager encountered error while evicting replica %d of kernel %s: %v",
			kernel.ReplicaID(), in.Id, err)
		return nil, err
	}
	//_ = d.resourceManager.ReleaseAllocatedGPUs(kernel.ReplicaID(), in.Id)
	//_ = d.resourceManager.ReleasePendingGPUs(kernel.ReplicaID(), in.Id)

	return proto.VOID, nil
}

// StopKernel stops a kernel.
func (d *SchedulerDaemonImpl) StopKernel(ctx context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	d.log.Debug("StopKernel RPC called for kernel %s.", in.Id)

	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", in.Id)
		return nil, domain.ErrKernelNotFound
	}

	d.log.Debug("Stopping replica %d of kernel %s now.", kernel.ReplicaID(), in.Id)
	err = d.stopKernel(ctx, kernel, false)
	if err != nil {
		return nil, d.errorf(err)
	}

	d.log.Debug("Successfully stopped replica %d of kernel %s.", kernel.ReplicaID(), in.Id)

	// Remove the kernel from our hash map.
	d.kernels.Delete(in.Id)

	if d.prometheusManager != nil {
		d.prometheusManager.NumActiveKernelReplicasGauge.Sub(1)
	}

	// Release any resources allocated to the kernel.
	if err := d.resourceManager.ReplicaEvicted(kernel.ReplicaID(), in.Id); err != nil {
		d.log.Error("ResourceManager encountered error while evicting replica %d of kernel %s: %v",
			kernel.ReplicaID(), in.Id, err)
		return nil, err
	}

	stopChan, loaded := d.executeRequestQueueStopChannels.Load(kernel.ID())
	if !loaded {
		d.log.Warn("Unable to load \"stop channel\" for \"execute_request\" forwarder of replica %d of kernel %s",
			kernel.ReplicaID(), kernel.ID())
	} else {
		// Tell the goroutine to stop.
		stopChan <- struct{}{}
	}

	//_ = d.resourceManager.ReleaseAllocatedGPUs(kernel.ReplicaID(), in.Id)
	//_ = d.resourceManager.ReleasePendingGPUs(kernel.ReplicaID(), in.Id)

	return proto.VOID, nil
}

func (d *SchedulerDaemonImpl) stopKernel(ctx context.Context, kernel scheduling.KernelReplica, ignoreReply bool) (err error) {
	if ignoreReply {
		_ = kernel.AddIOHandler(messaging.IOTopicShutdown, d.handleIgnoreMsg)
	}

	var msg zmq4.Msg
	frames := messaging.NewJupyterFramesWithHeader(messaging.MessageTypeShutdownRequest, kernel.Sessions()[0])

	// Encode the content.
	if err = frames.EncodeContent(&messaging.MessageShutdownRequest{
		Restart: false,
	}); err != nil {
		d.log.Error("Failed to encode content of Jupyter \"shutdown_request\" message because: %v", err)
		return err
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	err = kernel.RequestWithHandler(ctx, "Stopping by", messaging.ControlMessage, messaging.NewJupyterMessage(&msg), nil, wg.Done)
	if err != nil {
		return err
	}

	d.log.Debug("Sent \"%s\" message to replica %d of kernel %s.", messaging.MessageTypeShutdownRequest, kernel.ReplicaID(), kernel.ID())

	wg.Wait()

	if d.DockerMode() {
		d.log.Debug("Stopping container for kernel %s-%d via its invoker now.", kernel.ID(), kernel.ReplicaID())
		if err := d.getInvoker(kernel).Close(); err != nil {
			d.log.Error("Error while closing kernel %s: %v", kernel.String(), err)
		}
	} else {
		d.log.Debug("Skipping invoker::Close step for kernel %s-%d; we're running in \"%v\" mode.", kernel.ID(), kernel.ReplicaID(), d.deploymentMode)
	}
	return nil
}

// WaitKernel waits for a kernel to exit.
func (d *SchedulerDaemonImpl) WaitKernel(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", in.Id)
		return nil, domain.ErrKernelNotFound
	}

	kernelStatus, err := d.getInvoker(kernel).Wait()
	return d.statusErrorf(kernel, kernelStatus, err)
}

func (d *SchedulerDaemonImpl) SetClose(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	_ = d.Close()
	return proto.VOID, nil
}

func (d *SchedulerDaemonImpl) Start() error {
	d.log.Info("Starting router...")

	// Start cleaning routine.
	go d.cleanUp()

	go d.startKernelRegistryService()

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Shutdown helper routines (e.g., cleanUp())
	close(d.closed)
	return err
}

func (d *SchedulerDaemonImpl) startKernelRegistryService() {
	d.log.Debug("Beginning to listen for kernel registrations at %v", fmt.Sprintf(":%d", d.kernelRegistryPort))

	// Initialize the Kernel Registry listener
	registryListener, err := net.Listen("tcp", fmt.Sprintf(":%d", d.kernelRegistryPort))
	if err != nil {
		log.Fatalf("Failed to listen for kernel registry: %v", err)
	}
	defer func() {
		err := registryListener.Close()
		if err != nil {
			d.log.Error("Failed to cleanly shut down kernel registry listener: %v", err)
		}
	}()
	d.log.Info("Scheduler listening for kernel registrations at %v", registryListener.Addr())

	for {
		d.log.Info("Listening for kernel registration connection...")
		conn, err := registryListener.Accept()
		if err != nil {
			d.log.Error("Error encountered while listening for kernel registrations: %v", err)
			continue
		}

		d.log.Info("Accepted kernel registry connection. Local: %s. Remote: %s.", conn.LocalAddr(), conn.RemoteAddr())
		kernelRegistrationClient := &KernelRegistrationClient{conn: conn}
		go d.registerKernelReplica(context.TODO(), kernelRegistrationClient)
	}
}

func (d *SchedulerDaemonImpl) Close() error {
	// Close the router
	_ = d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned
	return nil
}

// Provider implementations.

func (d *SchedulerDaemonImpl) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	// Kernel ID is not available in the control message.
	// _, header, _, err := jupyter.HeaderFromMsg(msg)
	// if err != nil {
	// 	return err
	// }

	kernel, ok := d.kernels.Load(msg.JupyterSession())
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", msg.JupyterSession())
		return domain.ErrKernelNotFound
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	// Extract the workload ID (which may or may not be included in the metadata of the request),
	// and assign it to the kernel ID if it hasn't already been assigned a value for this kernel.
	metadataDict, err := msg.DecodeMetadata()
	if err == nil {
		if workloadId, loaded := metadataDict["workload_id"]; loaded && !kernel.WorkloadIdSet() {
			kernel.SetWorkloadId(workloadId.(string))
		}
	} else {
		d.log.Warn("Could not decode metadata dictionary of %s \"%s\" message %s (JupyterID=\"%s\").",
			messaging.ShellMessage, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
	}

	if err := d.forwardRequest(context.Background(), kernel, messaging.ControlMessage, msg, nil); err != nil {
		return err
	}

	// Handle ShutdownRequest
	if msg.JupyterMessageType() == messaging.ShellShutdownRequest {
		go func() {
			kernelStatus, err := d.getInvoker(kernel).Wait() // Wait() will detect the kernel status and the cleanup() will clean kernel automatically.
			_, _ = d.statusErrorf(kernel, kernelStatus, err)
		}()
	}

	return nil
}

func (d *SchedulerDaemonImpl) kernelShellHandler(info scheduling.KernelInfo, _ messaging.MessageType, msg *messaging.JupyterMessage) error {
	return d.ShellHandler(info, msg)
}

func (d *SchedulerDaemonImpl) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	// d.log.Debug("Received shell message with %d frame(s): %s", len(msg.JupyterFrames), msg)
	// kernelId, header, offset, err := d.headerAndOffsetFromMsg(msg)
	// if err != nil {
	// 	return err
	// }

	session := msg.JupyterSession()
	kernel, ok := d.kernels.Load(session)
	msgType := msg.JupyterMessageType()
	if !ok && (msgType == messaging.KernelInfoRequest || msgType == messaging.ShellExecuteRequest || msgType == messaging.ShellYieldRequest) {
		// Register kernel on KernelInfoRequest
		if msg.DestinationId == "" {
			return domain.ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(msg.DestinationId)
		if !ok {
			d.log.Error("Could not find kernel with ID \"%s\"", msg.DestinationId)
			return domain.ErrKernelNotFound
		}

		d.log.Debug("Binding %v with session %s ", kernel, session)
		d.kernels.Store(session, kernel)
		kernel.BindSession(session)
	}
	if kernel == nil {
		d.log.Error("Could not find kernel with ID \"%s\"", session)
		return domain.ErrKernelNotFound
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return domain.ErrKernelNotReady
	}

	// TODO(Ben): We'll inspect here to determine if the message is an execute_request.
	// If it is, then we'll see if we have enough resources for the kernel to (potentially) execute the code.
	// If not, we'll change the message's header to "yield_request".
	// If the message is an execute_request message, then we have some processing to do on it.
	if msg.JupyterMessageType() == messaging.ShellExecuteRequest || msg.JupyterMessageType() == messaging.ShellYieldRequest {
		resultChan := d.enqueueExecuteRequest(msg, kernel)

		// Wait for the result.
		res := <-resultChan

		// Return the result as an error or nil if there was no error.
		switch res.(type) {
		case error:
			return res.(error)
		case struct{}:
			return nil
		}
	} else { // Print a message about forwarding generic shell message.
		d.log.Debug("Forwarding shell message with %d frames to replica %d of kernel %s: %s",
			msg.JupyterFrames.Len(), kernel.ReplicaID(), kernel.ID(), msg)

		// Extract the workload ID (which may or may not be included in the metadata of the request),
		// and assign it to the kernel ID if it hasn't already been assigned a value for this kernel.
		metadataDict, err := msg.DecodeMetadata()
		if err == nil {
			if workloadId, loaded := metadataDict["workload_id"]; loaded && !kernel.WorkloadIdSet() {
				kernel.SetWorkloadId(workloadId.(string))
			}
		} else {
			d.log.Warn("Could not decode metadata dictionary of %s \"%s\" message %s (JupyterID=\"%s\").",
				messaging.ShellMessage, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		}
	}

	// IMPORTANT NOTE: The code below the if-else-statement above is NOT executed for "execute_request"
	// messages. Those are enqueued and processed separately to avoid resource allocation issues.
	ctx, cancel := context.WithCancel(context.Background())
	if err := kernel.RequestWithHandler(ctx, "Forwarding", messaging.ShellMessage, msg, d.kernelResponseForwarder, func() {
		cancel()
		d.log.Debug("Done() called for shell \"%s\" message targeting replica %d of kernel %s. Cancelling.",
			msg.JupyterMessageType(), kernel.ReplicaID(), kernel.ID())
	}); err != nil {
		return err
	}

	return nil
}

// enqueueExecuteRequest enqueues a given messaging.JupyterMessage (which must be an "execute_request" message) for
// submission to the target kernel. Messages are dequeued and submitted in a FCFS manner by a separate goroutine.
//
// The reason for this is that we need to process "execute_request" messages one-at-a-time to avoid resource allocation
// issues, in which we try to allocate resources to the same kernel multiple times upon the submission of concurrent
// "execute_request" messages, and/or to avoid starving other locally-running kernel replicas by keeping the same
// resources allocated to the same kernel replicas all the time.
//
// This function returns a channel used to notify the caller when the message has been sent.
//
// Important note: if there are more concurrent execute_request messages sent than the capacity of the buffered
// channel that serves as the message queue, then the FCFS ordering of the messages cannot be guaranteed. Specifically,
// the order in which the messages are enqueued is non-deterministic. (Once enqueued, the messages will be served in
// a FCFS manner.)
func (d *SchedulerDaemonImpl) enqueueExecuteRequest(execOrYieldReq *messaging.JupyterMessage, kernel scheduling.KernelReplica) <-chan interface{} {
	gid := goid.Get()
	msgType := execOrYieldReq.JupyterMessageType()
	if msgType != messaging.ShellExecuteRequest && msgType != messaging.ShellYieldRequest {
		log.Fatalf("[gid=%d] Cannot enqueue Jupyter message of type \"%s\" in outgoing \"execute_request\" queue of kernel \"%s\".",
			gid, msgType, kernel.ID())
	}

	mutex, loaded := d.outgoingExecuteRequestQueueMutexes.Load(kernel.ID())
	if !loaded {
		errorMessage := fmt.Sprintf("Could not find \"execute_request\" queue mutex for kernel \"%s\"", kernel.ID())
		d.notifyClusterGatewayAndPanic(
			"Could Not Find \"execute_request\" Queue Mutex", errorMessage, errorMessage)
		return nil // We won't reach this line; we panic in the call to notifyClusterGatewayAndPanic.
	}

	// Begin transaction on the outgoing "execute_request" queue for the target kernel.
	mutex.Lock()
	defer mutex.Unlock()

	queue, loaded := d.outgoingExecuteRequestQueue.Load(kernel.ID())
	if !loaded {
		d.log.Warn("[gid=%d] No \"execute_request\" queue found for replica %d of kernel %s. Creating one now.",
			gid, kernel.ReplicaID(), kernel.ID()) // Should already have been made when kernel replica was first created.
		queue = make(chan *enqueuedExecOrYieldRequest, DefaultExecuteRequestQueueSize)
		d.outgoingExecuteRequestQueue.Store(kernel.ID(), queue)
	}

	resultChan := make(chan interface{})

	queueLength := float64(len(queue))
	warningThreshold := 0.75 * float64(cap(queue))
	if queueLength > warningThreshold {
		// If the queue is quite full, then we'll print a warning.
		// Once the queue is full, the order in which future requests are processed is no longer guaranteed.
		// Specifically, the order in which new items are added to the queue is non-deterministic.
		// (Once in the queue, requests will still be processed in a FCFS manner.)
		d.log.Warn("[gid=%d] Enqueuing outbound \"%s\" %s targeting replica %d of kernel %s. Queue is almost full: %d/%d. IsTraining: %v.",
			gid, execOrYieldReq.JupyterMessageType(), execOrYieldReq.JupyterMessageId(), kernel.ReplicaID(), kernel.ID(), int(queueLength), cap(queue), kernel.IsTraining())
	} else {
		d.log.Debug("[gid=%d] Enqueuing outbound \"%s\" %s targeting replica %d of kernel %s. Queue size: %d/%d. IsTraining: %v.",
			gid, execOrYieldReq.JupyterMessageType(), execOrYieldReq.JupyterMessageId(), kernel.ReplicaID(), kernel.ID(), int(queueLength), cap(queue), kernel.IsTraining())
	}

	// This could conceivably block, which would be fine.
	queue <- &enqueuedExecOrYieldRequest{
		Msg:           execOrYieldReq,
		ResultChannel: resultChan,
		Kernel:        kernel,
	}

	return resultChan
}

// executeRequestForwarder forwards Jupyter "execute_request" messages to a particular kernel replica
// in a FCFS (first come, first served) manner.
//
// executeRequestForwarder is meant to be called in its own goroutine.
//
// Important note: if there are more concurrent execute_request messages sent than the capacity of the buffered
// channel that serves as the message queue, then the FCFS ordering of the messages cannot be guaranteed. Specifically,
// the order in which the messages are enqueued is non-deterministic. (Once enqueued, the messages will be served in
// a FCFS manner.)
func (d *SchedulerDaemonImpl) executeRequestForwarder(queue chan *enqueuedExecOrYieldRequest, stopChan chan interface{}, kernel scheduling.KernelReplica) {
	gid := goid.Get()
	d.log.Debug("[gid=%d] \"execute_request\" forwarder for replica %d of kernel %s has started running.", gid, kernel.ReplicaID(), kernel.ID())
	for {
		select {
		case enqueuedMessage := <-queue:
			{
				// Sanity check.
				// Ensure that the message is meant for the same kernel replica that this thread is responsible for.
				if enqueuedMessage.Kernel.ID() != kernel.ID() {
					errorMessage := fmt.Sprintf("Found enqueued \"%s\" message with mismatched kernel ID. "+
						"Enqueued message kernel ID: \"%s\". Expected kernel ID: \"%s\"",
						enqueuedMessage.Msg.JupyterMessageType(), enqueuedMessage.Kernel.ID(), kernel.ID())

					d.notifyClusterGatewayAndPanic(fmt.Sprintf("Enqueued \"%s\" with Mismatched Kernel ID",
						enqueuedMessage.Msg.JupyterMessageType()), errorMessage, errorMessage)

					return // We'll panic before this line is executed.
				} else if enqueuedMessage.Kernel.ReplicaID() != kernel.ReplicaID() {
					errorMessage := fmt.Sprintf("Found enqueued \"%s\" message targeting kernel \"%s\" with mismatched replica ID. "+
						"Enqueued message replica ID: %d. Expected replica ID: %d", enqueuedMessage.Msg.JupyterMessageType(),
						kernel.ID(), enqueuedMessage.Kernel.ReplicaID(), kernel.ReplicaID())

					d.notifyClusterGatewayAndPanic("Enqueued \"%s\" with Mismatched Replica ID", errorMessage, errorMessage)

					return // We'll panic before this line is executed.
				}
				d.log.Debug("[gid=%d] Dequeued shell \"%s\" message %s (JupyterID=%s) targeting kernel %s.",
					gid, enqueuedMessage.Msg.JupyterMessageType(), enqueuedMessage.Msg.RequestId, enqueuedMessage.Msg.JupyterMessageId(), enqueuedMessage.Kernel.ID())

				// Process the message.
				processedMessage := d.processExecOrYieldRequest(enqueuedMessage.Msg, enqueuedMessage.Kernel) // , header, offset)
				d.log.Debug("[gid=%d] Forwarding shell \"%s\" to replica %d of kernel %s: %s",
					gid, enqueuedMessage.Msg.JupyterMessageType(), enqueuedMessage.Kernel.ReplicaID(), enqueuedMessage.Kernel.ID(), processedMessage)

				// Sanity check.
				if enqueuedMessage.Kernel.IsTraining() {
					log.Fatalf(utils.RedStyle.Render("[gid=%d] Kernel %s is already training, even though we haven't sent the next \"execute_request\"/\"yield_execute\" request yet. Started training at: %v (i.e., %v ago)."),
						gid, enqueuedMessage.Kernel.ID(), enqueuedMessage.Kernel.TrainingStartedAt(), time.Since(enqueuedMessage.Kernel.TrainingStartedAt()))
				}

				// Record that we've sent this (although technically we haven't yet).
				kernel.SentExecuteRequest(processedMessage)

				// Send the message and post the result back to the caller via the channel included within
				// the enqueued "execute_request" message.
				ctx, cancel := context.WithCancel(context.Background())
				if err := enqueuedMessage.Kernel.RequestWithHandler(ctx, "Forwarding", messaging.ShellMessage, processedMessage, d.kernelResponseForwarder, func() {
					d.log.Debug("[gid=%d] Done() called for shell \"%s\" message targeting replica %d of kernel %s. Cancelling (though request may have succeeded already).",
						goid.Get(), processedMessage.JupyterMessageType(), enqueuedMessage.Kernel.ReplicaID(), enqueuedMessage.Kernel.ID())
					cancel()
				}); err != nil {
					// Send the error back to the caller.
					enqueuedMessage.ResultChannel <- err
				} else {
					// General notification that we're done, and there was no error.
					enqueuedMessage.ResultChannel <- struct{}{}
				}
			}
		case <-stopChan:
			{
				d.log.Debug("[gid=%d] \"execute_request\" forwarder for kernel %s has been instructed to stop.", gid, kernel.ID())
				return
			}
		}
	}
}

// processExecuteReply handles the logic of deallocating resources that have been committed to a kernel so that it could execute user-submitted code.
func (d *SchedulerDaemonImpl) processExecuteReply(msg *messaging.JupyterMessage, kernel scheduling.KernelInfo /*, offset int */) error {
	kernelClient := kernel.(scheduling.KernelReplica)
	// Check if we need to release allocated GPUs.
	// We only release allocated GPUs if this kernel replica executed the code.
	// If this replica yielded, then there will be no GPUs to release.
	var msgErr messaging.MessageError
	err := msg.JupyterFrames.DecodeContent(&msgErr)
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to unmarshal shell message received from replica %d of kernel %s because: %v",
			kernelClient.ReplicaID(), kernelClient.ID(), err)
		d.log.Error(errorMessage)

		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Failed to Unmarshal Shell \"execute_reply\" Message",
			Message:          errorMessage,
			NotificationType: 0,
			Panicked:         false,
		})

		// Still need to release the pending message...
		// kernelClient.ReceivedExecuteReply(msg)

		// return err
	}

	var (
		// releaseResourcesMustSucceed indicates whether we know for a fact that the call to the ResourceManager's
		// ReleaseCommittedResources method should succeed or not. We know for a fact that it should succeed if the
		// replica was in fact leading the execution.
		//
		// If the replica proposed 'yield', then we cannot say for sure, as we might've told it to propose 'yield'
		// due to insufficient resources available prior to the replica's leader election.
		releaseResourcesMustSucceed bool

		// shouldCallTrainingStopped tells us whether to call SessionStoppedTraining on the associated KernelClient.
		// We need to call SessionStoppedTraining if the replica was in fact leading its execution and therefore
		// executing user-submitted code. If this wasn't the case, then the status of the message will be
		// a messaging.MessageStatusError status, and the error will be a jupyter.MessageErrYieldExecution error.
		shouldCallTrainingStopped bool
	)
	if msgErr.Status == messaging.MessageStatusOK {
		d.log.Debug("Status of \"execute_reply\" message from replica %d of kernel %s is OK.", kernelClient.ReplicaID(), kernelClient.ID())
		releaseResourcesMustSucceed = true // Replica was leader and is done executing.
		shouldCallTrainingStopped = true
	} else if msgErr.Status == messaging.MessageStatusError {
		d.log.Warn("Status of \"execute_reply\" message from replica %d of kernel %s is \"%s\": %v", kernelClient.ReplicaID(), kernelClient.ID(), msgErr.Status, msgErr.String())

		// We should only call KernelStoppedTraining if the replica was actively training.
		// We can check this by inspecting the type of error encoded in the "execute_reply" message.
		// If it's a jupyter.MessageErrYieldExecution error, then the replica was NOT training,
		// and therefore we should not call KernelStoppedTraining on the associated Kernel.
		shouldCallTrainingStopped = msgErr.ErrName != messaging.MessageErrYieldExecution
	} else {
		// This should never happen. So, if it does, then we'll panic.
		errorMessage := fmt.Sprintf("Unexpected message status in \"execute_reply\" message from replica %d of kernel %s: \"%s\"",
			kernelClient.ReplicaID(), kernelClient.ID(), msgErr.Status)
		d.log.Error(errorMessage)
		d.notifyClusterGatewayAndPanic("Unexpected Message Status in \"execute_reply\" Message", errorMessage, errorMessage)
	}

	// Check if we should be releasing resources at this point or not.
	if d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart {
		// Release any resources committed to the kernel replica, as it is done training and does not need the resources
		// to be actively-bound/committed to it anymore.
		//
		// This may fail, as sometimes, the replica will not have resources allocated to it, such as if it
		// proposed 'YIELD' during its leader election (like if there were not enough resources available on
		// the node for it to train if it were to have won).
		d.log.Debug("Attempting to release committed resources from replica %d of kernel %s.", kernelClient.ReplicaID(), kernel.ID())
		if err = d.resourceManager.ReleaseCommittedResources(kernelClient.ReplicaID(), kernel.ID()); err != nil && releaseResourcesMustSucceed {
			errorMessage := fmt.Sprintf("Failed to release GPUs allocated to leader replica %d of kernel %s because: %v",
				kernelClient.ReplicaID(), kernel.ID(), err)
			d.log.Error(errorMessage)
			go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
				Id:               uuid.NewString(),
				Title:            "Failed to Release Committed Resources",
				Message:          errorMessage,
				NotificationType: 0,
				Panicked:         false,
			})
		} else if err == nil {
			// Again, if the error is non-nil, that doesn't necessarily mean the system is in an error state.
			// There are cases where we'll have told the replica to yield and did not reserve any resources for it.
			// These are described in greater detail in the comment(s) above.
			d.log.Debug("Successfully released committed resources from replica %d of kernel %s.",
				kernelClient.ReplicaID(), kernel.ID())
		}
	}

	if shouldCallTrainingStopped {
		_ = kernelClient.KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.")

		if d.prometheusManager != nil {
			d.prometheusManager.TrainingTimeGaugeVec.
				With(prometheus.Labels{"workload_id": kernelClient.WorkloadId(), "kernel_id": kernelClient.ID(), "node_id": d.id}).
				Add(time.Since(kernelClient.LastTrainingTimePrometheusUpdate()).Seconds())
		}
	}

	kernelClient.ReceivedExecuteReply(msg)

	// Include a snapshot of the current resource quantities on the node within the metadata frame of the message.
	_, _ = d.addResourceSnapshotToJupyterMessage(msg, kernelClient)

	if d.prometheusManager != nil {
		d.prometheusManager.NumTrainingEventsCompletedCounter.Inc()
	}

	return nil /* will be nil on success */
}

// updateKernelResourceSpec attempts to update the resource spec of the specified kernel.
//
// updateKernelResourceSpec will return nil on success. updateKernelResourceSpec will return an error if the kernel
// presently has resources committed to it, and the adjustment cannot occur due to resource contention.
func (d *SchedulerDaemonImpl) updateKernelResourceSpec(kernel scheduling.KernelReplica, newSpec types.Spec) error {
	if newSpec.GPU() < 0 || newSpec.CPU() < 0 || newSpec.VRAM() < 0 || newSpec.MemoryMB() < 0 {
		d.log.Error("Requested updated resource spec for kernel %s is invalid, as one or more quantities are negative: %s",
			kernel.ID(), newSpec.String())
		return fmt.Errorf("%w: %s", client.ErrInvalidResourceSpec, newSpec.String())
	}

	d.log.Debug("Updating pending alloc for kernel %s: from %s to %s.",
		kernel.ID(), kernel.ResourceSpec().String(), newSpec.String())
	err := d.resourceManager.AdjustPendingResources(kernel.ReplicaID(), kernel.ID(), newSpec)
	if err != nil {
		d.log.Error("Error while updating resource spec of kernel \"%s\": %v", kernel.ID(), err)
		return err
	}

	err = kernel.UpdateResourceSpec(newSpec, nil)
	if err != nil {
		panic(err)
	}

	return nil
}

// resourceRequestAdjustmentEnabled returns true if dynamically adjusting resource requests is enabled
// based on the configured scheduling policy used by the cluster.
func (d *SchedulerDaemonImpl) resourceRequestAdjustmentEnabled() bool {
	return d.schedulingPolicy.SupportsDynamicResourceAdjustments()
}

// processExecuteRequestMetadata processes the metadata frame of an "execute_request" message.
//
// Returns the target replica ID, if there is one, or -1 if there is not, along with the decoded metadata dictionary
// if the dictionary was decoded successfully. If the dictionary was not decoded successfully, then an empty map
// will be returned.
func (d *SchedulerDaemonImpl) processExecuteRequestMetadata(msg *messaging.JupyterMessage, kernel scheduling.KernelReplica) (int32, map[string]interface{}, error) {
	// If there is nothing in the message's metadata frame, then we just return immediately.
	if len(*msg.JupyterFrames.MetadataFrame()) == 0 {
		return -1, make(map[string]interface{}), nil
	}

	var metadataDict map[string]interface{}
	if err := msg.JupyterFrames.DecodeMetadata(&metadataDict); err != nil {
		d.log.Error("Failed to decode metadata frame of \"execute_request\" message \"%s\" with JSON: %v", msg.JupyterMessageId(), err)
		return -1, make(map[string]interface{}), err
	}

	var requestMetadata *messaging.ExecuteRequestMetadata
	if err := mapstructure.Decode(metadataDict, &requestMetadata); err != nil {
		d.log.Error("Failed to parse decoded metadata frame of \"execute_request\" message \"%s\" with mapstructure: %v", msg.JupyterMessageId(), err)
		return -1, metadataDict, err
	}

	d.log.Debug("Decoded metadata of \"execute_request\" message \"%s\": %s", msg.JupyterMessageId(), requestMetadata.String())

	var targetReplicaId int32 = -1
	if requestMetadata.TargetReplicaId != nil {
		targetReplicaId = *requestMetadata.TargetReplicaId
	}

	if requestMetadata.ResourceRequest != nil && d.resourceRequestAdjustmentEnabled() {
		d.log.Debug("Found new resource request for kernel \"%s\" in \"execute_request\" message \"%s\": %s",
			kernel.ID(), msg.JupyterMessageId(), requestMetadata.ResourceRequest.String())

		if err := d.updateKernelResourceSpec(kernel, requestMetadata.ResourceRequest); err != nil {
			return targetReplicaId, metadataDict, err
		}
	}

	return targetReplicaId, metadataDict, nil
}

// processExecOrYieldRequest performs some scheduling logic, such as verifying that there are sufficient resources available
// for the locally-running kernel replica to train (if it were to win its leader election).
//
// We also check if this replica has been explicitly instructed to yield, or if there is simply another replica of
// the same kernel that has been explicitly targeted as the winner (in which case the locally-running replica of the
// associated kernel must yield).
//
// TODO: Should we "reserve" resources for the kernel replica before the leader election to ensure that they are
// TODO: | available in the event that the replica wins? Or should we instead require that the winning replica contact
// TODO: | its local daemon upon winning to request the resources (in which case they may be unavailable due to
// TODO: | concurrent code executions running on the same node)?
// TODO: |
// TODO: | For now, we're reserving resources.
func (d *SchedulerDaemonImpl) processExecOrYieldRequest(msg *messaging.JupyterMessage, kernel scheduling.KernelReplica) *messaging.JupyterMessage {
	gid := goid.Get()

	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		errorMessage := fmt.Sprintf("Message of Invalid Type Passed to 'processExecOrYieldRequest': %s", msg.JupyterMessageType())
		d.notifyClusterGatewayAndPanic(errorMessage, "No idea how that happened, bud!", errorMessage)
		return nil // We'll panic before this line is executed.
	}

	// This ensures that we send "execute_request" messages one-at-a-time.
	// We wait until any pending "execute_request" messages receive an "execute_reply"
	// response before we can forward this next "execute_request".
	kernel.WaitForPendingExecuteRequests()

	d.log.Debug("[gid=%d] Processing `execute_request` for idle kernel %s now.", gid, kernel.ID())

	// If there are insufficient GPUs available, then we'll modify the message to be a "yield_request" message.
	// This will force the replica to necessarily yield the execution to the other replicas.
	// If no replicas are able to execute the code due to resource contention, then a new replica will be created dynamically.
	// There may be a particular replica specified to execute the request. We'll extract the ID of that replica to this variable, if it is present.
	targetReplicaId, metadataDict, err := d.processExecuteRequestMetadata(msg, kernel)

	// Will store the return value of `AllocatePendingGPUs`. If it is non-nil, then the allocation failed due to insufficient resources.
	var allocationFailedDueToInsufficientResources bool

	var targetError resource.InsufficientResourcesError
	if err != nil && errors.As(err, &targetError) {
		d.log.Debug("Received InsufficientResourcesError while processing metadata of 'execute_request'. " +
			"Must've tried to update resource request to some invalid value.")
		allocationFailedDueToInsufficientResources = true
	}

	// Extract the workload ID (which may or may not be included in the metadata of the request),
	// and assign it to the kernel ID if it hasn't already been assigned a value for this kernel.
	if workloadId, loaded := metadataDict["workload_id"]; loaded && !kernel.WorkloadIdSet() {
		kernel.SetWorkloadId(workloadId.(string))
	}

	// Check if another replica was specified as the one that should execute the code.
	// If this is true, then we'll yield the execution.
	// Note that we may pass 0 to force the execution to fail, for testing/debugging purposes.
	// No SMR replica can have an ID of 0.
	differentTargetReplicaSpecified := targetReplicaId != int32(-1) && targetReplicaId != kernel.ReplicaID()

	// Create a snapshot of the available idle resources on this node prior to our (potential) attempt
	// to reserve resources for this kernel replica in anticipation of its leader election.
	idleResourcesBeforeReservation := d.resourceManager.IdleResources()
	shouldYield := differentTargetReplicaSpecified || allocationFailedDueToInsufficientResources || kernel.SupposedToYieldNextExecutionRequest() || msg.JupyterMessageType() == messaging.ShellYieldRequest
	if !shouldYield && d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart {
		// We didn't want to bother reserving resources for this kernel replica if its either been explicitly told
		// to yield, or if another replica of the same kernel was explicitly expected to yield. But now that we know
		// that neither of those two things are true, we can go ahead and try to reserve the resources.
		d.log.Debug("[gid=%d] Attempting to reserve the following resources resources for replica %d of kernel %s in anticipation of its leader election: %s",
			gid, kernel.ReplicaID(), kernel.ID(), kernel.ResourceSpec().String())
		gpuDeviceIds, resourceAllocationError := d.resourceManager.CommitResources(kernel.ReplicaID(), kernel.ID(), kernel.ResourceSpec(), true)

		if resourceAllocationError != nil {
			d.log.Warn("[gid=%d] Could not reserve resources for replica %d of kernel %s in anticipation of its leader election because: %v.",
				gid, kernel.ReplicaID(), kernel.ID(), resourceAllocationError.Error())
			d.log.Warn("[gid=%d] Replica %d of kernel %s requires the following resources: %s.",
				gid, kernel.ReplicaID(), kernel.ID(), kernel.ResourceSpec().String())
			d.log.Warn("[gid=%d] The following resources are currently available on the node: %s.",
				gid, idleResourcesBeforeReservation.String())

			// There are other errors that could be returned here aside from "insufficient resources".
			// So, we should only set allocationFailedDueToInsufficientResources to false if the returned error is
			// in fact an "insufficient resources" type of error.
			if errors.As(resourceAllocationError, &resource.InsufficientResourcesError{}) {
				allocationFailedDueToInsufficientResources = true
			}

			shouldYield = true
		} else {
			d.log.Debug("[gid=%d] Successfully reserved the following resources for replica %d of kernel %s in anticipation of its leader election: %s (GPU Device IDs: %v).",
				gid, kernel.ReplicaID(), kernel.ID(), kernel.ResourceSpec().String(), gpuDeviceIds)
			allocationFailedDueToInsufficientResources = false

			metadataDict["gpu_device_ids"] = gpuDeviceIds
		}
	} else if !shouldYield && d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesWhenContainerScheduled {
		gpuDeviceIds, _ := d.resourceManager.GetGpuDeviceIdsAssignedToReplica(kernel.ReplicaID(), kernel.ID())

		if gpuDeviceIds != nil {
			metadataDict["gpu_device_ids"] = gpuDeviceIds
		}
	}

	// Include the quantities of idle GPUs available on the node PRIOR to our attempt to reserve resources for the kernel replica.
	metadataDict["idle-gpus"] = idleResourcesBeforeReservation.GPU()
	metadataDict["idle-millicpus"] = idleResourcesBeforeReservation.CPU()
	metadataDict["idle-memory-mb"] = idleResourcesBeforeReservation.MemoryMB()
	metadataDict["idle-vram-gb"] = idleResourcesBeforeReservation.VRAM()
	metadataDict["required-gpus"] = kernel.ResourceSpec().GPU()
	metadataDict["required-millicpus"] = kernel.ResourceSpec().CPU()
	metadataDict["required-memory-mb"] = kernel.ResourceSpec().MemoryMB()
	metadataDict["required-vram-gb"] = kernel.ResourceSpec().VRAM()

	d.log.Debug("[gid=%d] Including current idle resource counts in request metadata. Idle Millicpus: %s, idle memory (MB): %s, idle GPUs: %s, idle VRAM: %s.",
		gid, idleResourcesBeforeReservation.Millicpus.StringFixed(4), idleResourcesBeforeReservation.MemoryMb.StringFixed(4),
		idleResourcesBeforeReservation.GPUs.StringFixed(1), idleResourcesBeforeReservation.VRam.StringFixed(4))

	// There are several circumstances in which we'll need to tell our replica of the target kernel to yield the execution to one of the other replicas:
	// - If there are insufficient GPUs on this node, then our replica will need to yield.
	// - If one of the other replicas was explicitly specified as the target replica, then our replica will need to yield.
	if shouldYield {
		var reason domain.YieldReason
		// Log message depends on which condition was true (first).
		if differentTargetReplicaSpecified {
			d.log.Debug("[gid=%d] Replica %d of kernel %s is targeted, while we have replica %d running on this node.",
				gid, targetReplicaId, kernel.ID(), kernel.ReplicaID() /* Placeholder */)
			reason = domain.YieldDifferentReplicaTargeted
		} else if kernel.SupposedToYieldNextExecutionRequest() {
			d.log.Debug("[gid=%d] Replica %d of kernel %s has been explicitly instructed to yield its next execution request.",
				gid, kernel.ReplicaID(), kernel.ID())
			reason = domain.YieldExplicitlyInstructed
		} else if msg.JupyterMessageType() == messaging.ShellYieldRequest {
			d.log.Debug("[gid=%d] Replica %d of kernel %s was sent an \"yield_request\" message instead of an \"execute_request\" message, presumably due to insufficient resources.",
				gid, kernel.ReplicaID(), kernel.ID())
			reason = domain.YieldInsufficientResourcesAvailable
		} else if allocationFailedDueToInsufficientResources {
			d.log.Debug("[gid=%d] There are insufficient resources available for replica %d of kernel %s to train. Available: %s. Required: %s.",
				gid, kernel.ReplicaID(), kernel.ID(), idleResourcesBeforeReservation.String(), kernel.ResourceSpec().String())
			reason = domain.YieldInsufficientResourcesAvailable
		} else {
			d.log.Debug("[gid=%d] Replica %d of kernel %s must propose 'yield' for some other unspecified reason.",
				gid, kernel.ReplicaID(), kernel.ID())
			reason = domain.YieldUnspecifiedReason
		}

		metadataDict["yield-reason"] = reason

		// Convert the message to a yield request.
		// We'll return this converted message, and it'll ultimately be forwarded to the kernel replica
		// in place of the original 'execute_request' message.
		msg, err = msg.CreateAndReturnYieldRequestMessage()
		if err != nil {
			d.notifyClusterGatewayAndPanic("Failed to Convert Message of Type \"execute_request\" to a \"yield_request\" Message",
				err.Error(), err)
			return nil // We'll panic before this gets executed.
		}

		// If we were told explicitly to YIELD this execution request via the `YieldNextExecutionRequest` API,
		// then record that we've done this. Even if we're yielding for other reasons too, we should still
		// record that we've done this.
		if kernel.SupposedToYieldNextExecutionRequest() {
			kernel.YieldedNextExecutionRequest()
		}
	}

	// Re-encode the metadata frame. It will have the number of idle GPUs available,
	// as well as the reason that the request was yielded (if it was yielded).
	err = msg.EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("[gid=%d] Failed to encode metadata frame because: %v", gid, err)
		d.notifyClusterGatewayAndPanic("Failed to Encode Metadata Frame", err.Error(), err)
	}

	// Regenerate the signature.
	_, err = msg.JupyterFrames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key))
	if err != nil {
		message := fmt.Sprintf("[gid=%d] Failed to sign updated JupyterFrames for \"%s\" message because: %v",
			gid, msg.JupyterMessageType(), err)
		d.notifyClusterGatewayAndPanic("Failed to Sign JupyterFrames", message, err)
	}

	if verified := messaging.ValidateFrames([]byte(kernel.ConnectionInfo().Key), kernel.ConnectionInfo().SignatureScheme, msg.JupyterFrames); !verified {
		d.log.Error("[gid=%d] Failed to verify modified message with signature scheme '%v' and key '%v'",
			gid, kernel.ConnectionInfo().SignatureScheme, kernel.ConnectionInfo().Key)
		d.log.Error("[gid=%d] This message will likely be rejected by the kernel:\n%v", gid, msg)
	}

	return msg
}

func (d *SchedulerDaemonImpl) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return d.forwardRequest(context.Background(), nil, messaging.StdinMessage, msg, nil)
}

func (d *SchedulerDaemonImpl) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return d.forwardRequest(context.Background(), nil, messaging.HBMessage, msg, nil)
}

// GetVirtualGpuAllocations returns the current vGPU allocations on this node.
func (d *SchedulerDaemonImpl) GetVirtualGpuAllocations(_ context.Context, _ *proto.Void) (*proto.VirtualGpuAllocations, error) {
	allocations := &proto.VirtualGpuAllocations{
		Allocations: d.virtualGpuPluginServer.GetAllocations(),
	}

	d.log.Debug("Returning vGPU allocations: %v", d.virtualGpuPluginServer.GetAllocations())

	return allocations, nil
}

func (d *SchedulerDaemonImpl) GetVirtualGpuInfo(_ context.Context, _ *proto.Void) (*proto.VirtualGpuInfo, error) {
	response := &proto.VirtualGpuInfo{
		TotalVirtualGPUs:     int32(d.virtualGpuPluginServer.NumVirtualGPUs()),
		AllocatedVirtualGPUs: int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()),
		FreeVirtualGPUs:      int32(d.virtualGpuPluginServer.NumFreeVirtualGPUs()),
	}

	d.log.Debug("Returning vGPU information: %v", response)

	return response, nil
}

// SetTotalVirtualGPUs adjusts the total number of virtual GPUs available on this node.
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), a domain.ErrInvalidParameter is returned.
func (d *SchedulerDaemonImpl) SetTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	if d.KubernetesMode() {
		return d.setTotalVirtualGPUsKubernetes(ctx, in)
	} else {
		return d.setTotalVirtualGPUsDocker(in)
	}
}

// setTotalVirtualGPUsKubernetes is used to change the vGPUs available on this node when running in Docker mode.
func (d *SchedulerDaemonImpl) setTotalVirtualGPUsDocker(in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	err := d.resourceManager.AdjustSpecGPUs(float64(in.GetValue()))
	if err != nil {
		response := &proto.VirtualGpuInfo{
			TotalVirtualGPUs:     int32(d.resourceManager.SpecGPUs().InexactFloat64()),
			AllocatedVirtualGPUs: int32(d.resourceManager.CommittedGPUs().InexactFloat64()),
			FreeVirtualGPUs:      int32(d.resourceManager.IdleGPUs().InexactFloat64()),
		}
		return response, status.Error(codes.InvalidArgument, err.Error())
	}

	response := &proto.VirtualGpuInfo{
		TotalVirtualGPUs:     int32(d.resourceManager.SpecGPUs().InexactFloat64()),
		AllocatedVirtualGPUs: int32(d.resourceManager.CommittedGPUs().InexactFloat64()),
		FreeVirtualGPUs:      int32(d.resourceManager.IdleGPUs().InexactFloat64()),
	}
	return response, nil
}

// setTotalVirtualGPUsKubernetes is used to change the vGPUs available on this node when running in Kubernetes mode.
func (d *SchedulerDaemonImpl) setTotalVirtualGPUsKubernetes(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	newNumVirtualGPUs := in.GetValue()
	if newNumVirtualGPUs < int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()) {
		response := &proto.VirtualGpuInfo{
			TotalVirtualGPUs:     int32(d.virtualGpuPluginServer.NumVirtualGPUs()),
			AllocatedVirtualGPUs: int32(d.virtualGpuPluginServer.NumAllocatedVirtualGPUs()),
			FreeVirtualGPUs:      int32(d.virtualGpuPluginServer.NumFreeVirtualGPUs()),
		}

		return response, fmt.Errorf("%w : cannot decrease the total number of vGPUs below the number of allocated vGPUs", domain.ErrInvalidParameter)
	}

	err := d.virtualGpuPluginServer.SetTotalVirtualGPUs(newNumVirtualGPUs)
	if err != nil {
		d.log.Error("Failed to set the total number of virtual GPUs to new value %d: %v", newNumVirtualGPUs, err)
		return nil, err
	}

	response, err := d.GetVirtualGpuInfo(ctx, &proto.Void{})
	if err != nil {
		d.log.Error("Unexpected error while getting the new virtual GPU info: %v", err)
		return nil, err
	}

	return response, nil
}

// ResourcesSnapshot returns a *proto.NodeResourcesSnapshot struct encoding a snapshot of the current resource quantities on the node.
func (d *SchedulerDaemonImpl) ResourcesSnapshot(_ context.Context, _ *proto.Void) (*proto.NodeResourcesSnapshotWithContainers, error) {
	resourceSnapshot := d.resourceManager.ProtoResourcesSnapshot()

	containers := make([]*proto.ReplicaInfo, 0)

	d.kernels.Range(func(s string, replicaClient scheduling.KernelReplica) (contd bool) {
		replicaInfo := &proto.ReplicaInfo{
			ReplicaId:    replicaClient.ReplicaID(),
			KernelId:     replicaClient.ID(),
			PersistentId: replicaClient.PersistentID(), // Probably don't need to include this here.
		}

		containers = append(containers, replicaInfo)

		return true
	})

	snapshotWithContainers := &proto.NodeResourcesSnapshotWithContainers{
		Id:               uuid.NewString(),
		ResourceSnapshot: resourceSnapshot,
		Containers:       containers,
	}

	return snapshotWithContainers, nil
}

func (d *SchedulerDaemonImpl) kernelFromMsg(msg *messaging.JupyterMessage) (kernel scheduling.KernelReplica, err error) {
	kernel, ok := d.kernels.Load(msg.DestinationId)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", msg.DestinationId)
		return nil, domain.ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, domain.ErrKernelNotReady
	}

	return kernel, nil
}

func (d *SchedulerDaemonImpl) forwardRequest(ctx context.Context, kernel scheduling.KernelReplica, typ messaging.MessageType, msg *messaging.JupyterMessage, done func()) (err error) {
	// goroutineId := goid.Get()
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	return kernel.RequestWithHandler(ctx, "Forwarding", typ, msg, d.kernelResponseForwarder, done)
}

func (d *SchedulerDaemonImpl) kernelResponseForwarder(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	var (
		sender         messaging.Sender
		connectionInfo *jupyter.ConnectionInfo
		requiresAck    bool
	)

	kernelClient := from.(scheduling.KernelReplica)
	socket := from.Socket(typ)
	if socket == nil {
		requiresAck = d.router.ShouldAckMessages()

		socket = d.router.Socket(typ)

		// TODO (Ben): If we're forwarding a response back to the internalCluster Gateway here, then the internalCluster Gateway will ACK the response.
		// We need to:
		// (a) Listen for the ACK.
		// (b) Re-forward the kernel's response to the internalCluster Gateway if we don't receive an ACK.
		d.router.RegisterAck(msg)

		// Use the router as the server to forward the kernel's response to the cluster gateway.
		// The socket belongs to the router, hence we use that server.
		sender = d.router
	} else {
		// Use the kernel client as the server to forward the kernel's response to the cluster gateway.
		// The socket belongs to the kernel client's server, hence we use that server.
		sender = kernelClient
		requiresAck = kernelClient.ShouldAckMessages()
	}

	// We always want to use the connection information from the kernel client.
	connectionInfo = kernelClient.ConnectionInfo()

	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if typ == messaging.ShellMessage {
		// _, header, offset, err := jupyter.HeaderFromMsg(msg)
		// if err != nil {
		// 	d.log.Error("Failed to extract header from %v message for kernel %s because: %v", typ, from.ID(), err)
		// } else if header.MsgType == messaging.ShellExecuteReply {
		// 	d.processExecuteReply(msg, from, offset)
		// }

		if msg.JupyterMessageType() == messaging.ShellExecuteReply {
			err := d.processExecuteReply(msg, from)
			if err != nil {
				d.log.Error("Error while processing 'execute_reply' message from %s: %v", from.String(), err)
				return err
			}
		}
	}

	builder := messaging.NewRequestBuilder(context.Background(), from.ID(), from.ID(), connectionInfo).
		WithAckRequired(messaging.ShouldMessageRequireAck(typ) && requiresAck && d.MessageAcknowledgementsEnabled).
		WithMessageType(typ).
		WithBlocking(true).
		WithTimeout(messaging.DefaultRequestTimeout).
		WithDoneCallback(messaging.DefaultDoneHandler).
		WithMessageHandler(messaging.DefaultMessageHandler).
		WithNumAttempts(d.numResendAttempts).
		WithSocketProvider(from).
		WithJMsgPayload(msg)
	request, err := builder.BuildRequest()
	if err != nil {
		d.log.Error(utils.RedStyle.Render("Error while building request: %v"), err)
		return err
	}

	// d.log.Debug("Forwarding %v response from %v via %s: %v", typ, from, socket.Name, msg)
	// We should only use the router here if that's where the socket came from...
	// err := sender.SendRequest(true, socket, "" /* will be auto-resolved */, msg, sender, from.(*client.Kernel), -1 /* will be auto-resolved */)
	// err := socket.Send(*msg)
	err = sender.SendRequest(request, socket)
	if err != nil {
		d.log.Error("Error while forwarding %v response from kernel %s: %s", typ, from.ID(), err.Error())
	}

	return nil // Will be nil on success.
}

func (d *SchedulerDaemonImpl) handleErrorReport(kernel scheduling.KernelReplica, frames *messaging.JupyterFrames, _ *messaging.JupyterMessage) error {
	var errorReport messaging.ErrorReport
	if err := frames.DecodeContent(&errorReport); err != nil {
		d.log.Error("Failed to decode content of 'error report' message: %v", err)
		return err
	}

	d.log.Error("Received '%s' error from kernel %s: %v", errorReport.ErrorTitle, kernel.ID(), errorReport.ErrorMessage)

	if errorReport.KernelId != kernel.ID() {
		d.log.Error("Error report specifies kernel %s, but our records indicate that the report originated from kernel %s...", errorReport.KernelId, kernel.ID())
	}

	go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
		Id:               uuid.NewString(),
		Title:            errorReport.ErrorTitle,
		Message:          errorReport.ErrorMessage,
		NotificationType: 0,
		Panicked:         false,
	})

	return nil
}

// Notify is an implementation of the proto.KernelErrorReporterServer gRPC interface. Specifically, Notify is used
// by Jupyter kernel replicas running on the same scheduling.Host as this Local Daemon to notify when something occurs.
// Typically, this API is used to inform the Local Daemon that an error has occurred, so that the Local Daemon can
// in turn notify the Cluster Gateway, which can then send a notification to the Cluster Dashboard UI to inform the user.
func (d *SchedulerDaemonImpl) Notify(ctx context.Context, notification *proto.KernelNotification) (*proto.Void, error) {
	if notification.NotificationType == 0 {
		d.log.Warn("Received error notification from replica %d of kernel %s. Title: %s. Message: %s.",
			notification.ReplicaId, notification.KernelId, notification.Title, notification.Message)
	} else {
		d.log.Debug("Received notification from replica %d of kernel %s. Title: %s. Message: %s.",
			notification.ReplicaId, notification.KernelId, notification.Title, notification.Message)
	}

	message := fmt.Sprintf("%s [KernelID=%s, ReplicaID=%d]",
		notification.Message, notification.KernelId, notification.ReplicaId)

	return d.provisioner.Notify(ctx, &proto.Notification{
		Id:               uuid.NewString(),
		Title:            notification.Title,
		Message:          message,
		NotificationType: notification.NotificationType,
		Panicked:         false,
	})
}

// notifyClusterGatewayOfError calls the internalCluster Gateway's 'Notify' gRPC method.
// This is used to report errors to the Gateway.
// In general, this is done so that the errors can then be
// pushed to the frontend UI to inform the user.
func (d *SchedulerDaemonImpl) notifyClusterGatewayOfError(ctx context.Context, notification *proto.Notification) {
	_, err := d.provisioner.Notify(ctx, notification)

	if err != nil {
		d.log.Error("Failed to notify internalCluster Gateway of error because: %v", err)
	}
}

func (d *SchedulerDaemonImpl) handleSMRLeadTask(kernel scheduling.KernelReplica, frames *messaging.JupyterFrames, jMsg *messaging.JupyterMessage) error {
	messageType, err := frames.GetMessageType()
	if err != nil {
		d.log.Error("Failed to extract message type from SMR Lead ZMQ message: %v", err)
		return err
	}

	if messageType == messaging.MessageTypeSMRLeadTask {
		var leadMessage messaging.MessageSMRLeadTask
		if err = frames.DecodeContent(&leadMessage); err != nil {
			d.log.Error("Failed to decode content of SMR Lead ZMQ message: %v", err)
			return err
		}

		d.log.Debug("%v leads the task, GPU required (%v), notify the scheduler. Resources required: %v.",
			kernel, leadMessage.GPURequired, kernel.ResourceSpec())

		// If we're supposed to commit the resources when the container is scheduled, then it should already have
		// resources commited to it. If it doesn't, then that's problematic.
		if d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesWhenContainerScheduled &&
			!d.resourceManager.ReplicaHasCommittedResources(kernel.ReplicaID(), kernel.ID()) {

			d.log.Error("Replica %d of kernel %s does not already have resources committed to it.",
				kernel.ReplicaID(), kernel.ID())
			go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
				Id: uuid.NewString(),
				Title: fmt.Sprintf("Replica %d of Kernel %s Does Not Already Have Resources Committed to It",
					kernel.ReplicaID(), kernel.ID()),
				Message:          "Resources should already be committed to the kernel because we're using FCFS batch scheduling.",
				NotificationType: 0,
				Panicked:         true,
			})

			return fmt.Errorf("replica %d of kernel %s does not already have resources committed to it",
				kernel.ReplicaID(), kernel.ID())
		}

		// If we're supposed to bind resources at training start, then we'd better do that now.
		if d.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart {
			d.log.Debug("Promoting resource reservation of replica %d of kernel %s now.",
				kernel.ReplicaID(), kernel.ID())
			err = d.resourceManager.PromoteReservation(kernel.ReplicaID(), kernel.ID())
			if err != nil {
				d.log.Error("Our attempt to promote reserved resources of replica %d of kernel %s failed because: %v.",
					kernel.ReplicaID(), kernel.ID(), err)
				go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
					Id:    uuid.NewString(),
					Title: "Promotion of Resource Reservation Failed",
					Message: fmt.Sprintf("Could not promote resource reservation for replica %d of kernel %s because: %v",
						kernel.ReplicaID(), kernel.ID(), err),
					NotificationType: 0,
					Panicked:         true,
				})
				//panic(err) // TODO(Ben): Handle gracefully.
				return err
			}
		}

		// Include a snapshot of the current resource quantities on the node within the metadata frame of the message.
		_, err = d.addResourceSnapshotToJupyterMessage(jMsg, kernel)
		if err != nil {
			d.log.Warn("Failed to embed resource snapshot in \"%s\" message \"%s\" for kernel \"%s\" because: %v",
				jMsg.JupyterMessageType(), jMsg.JupyterMessageId(), kernel.ID(), err)
		}

		// Note: we don't really need to pass the snapshot here, as it isn't used in the Local Daemon.
		_ = kernel.KernelStartedTraining()

		// Don't return here -- we want this to be forwarded to the internalCluster Gateway.
		// return commonTypes.ErrStopPropagation
	} else if messageType == messaging.MessageTypeLeadAfterYield {
		// TODO(Ben): Need a better way to propagate errors back to the user, either at the Jupyter Notebook or the Workload Driver.
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Kernel Replica Lead Execution After Yielding",
			Message:          fmt.Sprintf("Replica %d of kernel %s was selected to lead an execution after explicitly yielding.", kernel.ReplicaID(), kernel.ID()),
			NotificationType: 0,
			Panicked:         true,
		})
		log.Fatalf("Our replica of kernel %s was selected to lead an execution after explicitly yielding.\n", kernel.ID())
	} else {
		d.log.Error("Received message of unexpected type '%s' for SMR Lead ZMQ message topic.", messageType)
		return domain.ErrUnexpectedZMQMessageType
	}

	return nil
}

// addResourceSnapshotToJupyterMessage decodes the metadata frame of the given messaging.JupyterMessage
// and adds an entry under the scheduling.SnapshotMetadataKey key with the value being a snapshot
// of the current resource quantities of the Local Daemon's ResourceManager.
func (d *SchedulerDaemonImpl) addResourceSnapshotToJupyterMessage(jMsg *messaging.JupyterMessage, kernel scheduling.KernelReplica) (*resource.ManagerSnapshot, error) {
	var snapshot *resource.ManagerSnapshot

	// Include in the message a snapshot of the current resource quantities of the ResourceManager.
	metadata, decodeError := jMsg.DecodeMetadata()
	if decodeError != nil {
		errorMessage := fmt.Sprintf("Failed to decode metadata frame of IOPub \"%s\" Jupyter message: %v",
			messaging.MessageTypeSMRLeadTask, decodeError)
		d.log.Error(errorMessage)
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            "Failed to Decode Metadata Frame of Jupyter Message",
			Message:          errorMessage,
			NotificationType: 0,
			Panicked:         false,
		})

		return nil, decodeError
	} else {
		snapshot = d.resourceManager.ResourcesSnapshot()
		metadata[resource.SnapshotMetadataKey] = snapshot

		// Re-encode the metadata frame. It will have the number of idle GPUs available,
		// as well as the reason that the request was yielded (if it was yielded).
		encodeErr := jMsg.EncodeMetadata(metadata)
		if encodeErr != nil {
			d.log.Error("Failed to encode metadata frame because: %v", encodeErr)
			d.notifyClusterGatewayAndPanic("Failed to Encode Metadata Frame", encodeErr.Error(), encodeErr)
		}

		// Regenerate the signature.
		_, err := jMsg.JupyterFrames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key))
		if err != nil {
			message := fmt.Sprintf("Failed to sign updated JupyterFrames for \"%s\" message because: %v", jMsg.JupyterMessageType(), err)
			d.notifyClusterGatewayAndPanic("Failed to Sign JupyterFrames", message, err)
		}

		if verified := messaging.ValidateFrames([]byte(kernel.ConnectionInfo().Key), kernel.ConnectionInfo().SignatureScheme, jMsg.JupyterFrames); !verified {
			errorMessage := fmt.Sprintf("Failed to verify modified message with signature scheme '%v' and key '%v'",
				kernel.ConnectionInfo().SignatureScheme, kernel.ConnectionInfo().Key)
			d.log.Error(errorMessage)
			go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
				Id:               uuid.NewString(),
				Title:            "Failed to Validate Modified Jupyter Message with Resource Snapshot",
				Message:          errorMessage,
				NotificationType: 0,
				Panicked:         false,
			})
			return nil, encodeErr
		}
	}

	d.log.Debug("Added snapshot to Jupyter \"%s\" message: %s", jMsg.JupyterMessageType(), snapshot.String())
	d.log.Debug("Message frames after adding snapshot: %s", jMsg.JupyterFrames.String())
	return snapshot, nil
}

func (d *SchedulerDaemonImpl) handleIgnoreMsg(kernel scheduling.KernelReplica, _ *messaging.JupyterFrames, raw *messaging.JupyterMessage) error {
	d.log.Debug("%v ignores %v", kernel, raw)
	return types.ErrStopPropagation
}

func (d *SchedulerDaemonImpl) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *SchedulerDaemonImpl) statusErrorf(kernel scheduling.KernelReplica, status jupyter.KernelStatus, err error) (*proto.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}

	if status >= jupyter.KernelStatusExited {
		d.kernels.Delete(kernel.ID())
		for _, session := range kernel.Sessions() {
			d.kernels.Delete(session)
		}
		d.log.Debug("Cleaned kernel %s and associated sessions %v after kernel stopped.", kernel.ID(), kernel.Sessions())
		err := kernel.Close()
		if err != nil {
			d.log.Error("Error while closing kernel %s: %v", kernel.String(), err)
		}
	}
	return &proto.KernelStatus{Status: int32(status)}, nil
}

func (d *SchedulerDaemonImpl) getInvoker(kernel scheduling.KernelReplica) invoker.KernelInvoker {
	return kernel.Context().Value(ctxKernelInvoker).(invoker.KernelInvoker)
}

func (d *SchedulerDaemonImpl) closeKernel(kernel scheduling.KernelReplica, reason string) {
	if err := d.getInvoker(kernel).Close(); err != nil {
		d.log.Warn("Failed to close %v after %s, failure: %v", kernel, reason, err)
	}
	err := kernel.Close()
	if err != nil {
		d.log.Error("Error while closing kernel %s: %v", kernel.String(), err)
	}
}

func (d *SchedulerDaemonImpl) cleanUp() {
	timer := time.NewTimer(cleanUpInterval)

	for {
		select {
		case <-d.closed:
			// Router is closed, clean up all kernels.
			d.kernels.Range(d.clearHandler)
			// Signal that the cleanup is done.
			close(d.cleaned)
			return
		case <-timer.C:
			// Clean up expired kernels.
			d.kernels.Range(d.gcHandler)
			// Reset the timer.
			timer.Reset(cleanUpInterval)
		}
	}
}

func (d *SchedulerDaemonImpl) clearHandler(_ string, kernel scheduling.KernelReplica) (contd bool) {
	err := d.getInvoker(kernel).Close()
	if err != nil {
		d.log.Error("Error while closing kernel %s: %v", kernel.String(), err)
	}
	return true
}

func (d *SchedulerDaemonImpl) gcHandler(kernelId string, kernel scheduling.KernelReplica) (contd bool) {
	if d.getInvoker(kernel).Expired(cleanUpInterval) {
		d.kernels.Delete(kernelId)
		if kernelId == kernel.ID() {
			d.log.Debug("Cleaned kernel %s due to expiration.", kernelId)
		} else {
			d.log.Debug("Cleaned session %s of kernel %s due to expiration.", kernelId, kernel.ID())
		}
	}
	return true
}
