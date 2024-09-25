package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"

	"github.com/zhangjyr/distributed-notebook/local_daemon/device"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
	"github.com/zhangjyr/distributed-notebook/local_daemon/invoker"
)

const (
	// DefaultPrometheusPort is the default port on which the Local Daemon will serve Prometheus metrics.
	DefaultPrometheusPort int = 8089
	// DefaultPrometheusInterval is the default interval on which the Local Daemon will push new Prometheus metrics.
	DefaultPrometheusInterval = time.Second * 5
	// DefaultNumResendAttempts is the default number of attempts we'll resend a message before giving up.
	DefaultNumResendAttempts = 3
)

var (
	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute

	ErrExistingReplicaAlreadyRunning = status.Error(codes.Internal, "an existing replica of the target kernel is already running on this node")
	ErrNilArgument                   = status.Error(codes.InvalidArgument, "one or more of the required arguments was nil")
)

// SchedulerDaemonImpl is the daemon that proxy requests to kernel replicas on local-host.
//
// WIP: Replica membership change.
// TODO: Distinguish reachable host list from replica list.
// TODO: Synchronize resource status using replica network (e.g., control socket). Synchoronization message should load-balance between replicas mapped the same host.
type SchedulerDaemonImpl struct {
	// Options
	id       string
	nodeName string

	virtualGpuPluginServer device.VirtualGpuPluginServer

	schedulingPolicy string
	proto.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions      *jupyter.ConnectionInfo
	schedulerDaemonOptions domain.SchedulerDaemonOptions

	// Cluster client
	provisioner proto.ClusterGatewayClient

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

	smrPort int

	// members
	transport                    string
	ip                           string
	kernels                      hashmap.HashMap[string, *client.KernelReplicaClient]
	kernelClientCreationChannels hashmap.HashMap[string, chan *proto.KernelConnectionInfo]

	log logger.Logger

	// The IOPub socket that the Gateway subscribes to.
	// All pub/sub messages are forwarded from kernels to the gateway (through us, the local daemon) using this socket.
	// We wrap the messages in another message that just has a header that is the kernel ID.
	// This enables the Gateway's SUB sockets to filter messages from each kernel.
	// iopub *jupyter.Socket

	// devicePluginServer deviceplugin.VirtualGpuPluginServer

	// There's a simple TCP server that listens for kernel registration notifications on this port.
	kernelRegistryPort int

	// numResendAttempts is the number of times to try resending a message before giving up.
	numResendAttempts int

	availablePorts *utils.AvailablePorts

	// Manages resource allocations on behalf of the Local Daemon.
	resourceManager *scheduling.ResourceManager

	// Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.
	hdfsNameNodeEndpoint string

	// Base directory in which the persistent store data is stored when running in docker mode.
	dockerStorageBase string

	// Kubernetes or Docker.
	deploymentMode types.DeploymentMode

	// When using Docker mode, we assign "debug ports" to kernels so that they can serve a Go HTTP server for debugging.
	kernelDebugPorts hashmap.HashMap[string, int]

	// Indicates whether we're running within WSL (Windows Subsystem for Linux).
	// If we are, then there is some additional configuration required for the kernel containers in order for
	// them to be able to connect to HDFS running in the host (WSL).
	usingWSL bool

	// If true, then the kernels will be executed within GDB.
	runKernelsInGdb bool

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

type KernelRegistrationPayload struct {
	SignatureScheme string                  `json:"signature_scheme"`
	Key             string                  `json:"key"`
	Kernel          *proto.KernelSpec       `json:"kernel,omitempty"`
	ReplicaId       int32                   `json:"replicaId,omitempty"`
	NumReplicas     int32                   `json:"numReplicas,omitempty"`
	Join            bool                    `json:"join,omitempty"`
	PersistentId    *string                 `json:"persistentId,omitempty"`
	PodName         string                  `json:"podName,omitempty"`
	NodeName        string                  `json:"nodeName,omitempty"`
	Cpu             int32                   `json:"cpu,omitempty"`
	Memory          int32                   `json:"memory,omitempty"`
	Gpu             int32                   `json:"gpu,omitempty"`
	ConnectionInfo  *jupyter.ConnectionInfo `json:"connection-info,omitempty"`
}

// KernelRegistrationClient represents an incoming connection from local distributed kernel.
type KernelRegistrationClient struct {
	conn net.Conn
}

func New(connectionOptions *jupyter.ConnectionInfo, schedulerDaemonOptions *domain.SchedulerDaemonOptions, kernelRegistryPort int, virtualGpuPluginServer device.VirtualGpuPluginServer, nodeName string, configs ...domain.SchedulerDaemonConfig) *SchedulerDaemonImpl {
	ip := os.Getenv("POD_IP")

	daemon := &SchedulerDaemonImpl{
		connectionOptions:            connectionOptions,
		transport:                    "tcp",
		ip:                           ip,
		nodeName:                     nodeName,
		kernels:                      hashmap.NewCornelkMap[string, *client.KernelReplicaClient](1000),
		kernelClientCreationChannels: hashmap.NewCornelkMap[string, chan *proto.KernelConnectionInfo](250),
		kernelDebugPorts:             hashmap.NewCornelkMap[string, int](250),
		availablePorts:               utils.NewAvailablePorts(connectionOptions.StartingResourcePort, connectionOptions.NumResourcePorts, 2),
		closed:                       make(chan struct{}),
		cleaned:                      make(chan struct{}),
		kernelRegistryPort:           kernelRegistryPort,
		smrPort:                      schedulerDaemonOptions.SMRPort,
		virtualGpuPluginServer:       virtualGpuPluginServer,
		deploymentMode:               types.DeploymentMode(schedulerDaemonOptions.DeploymentMode),
		hdfsNameNodeEndpoint:         schedulerDaemonOptions.HdfsNameNodeEndpoint,
		dockerStorageBase:            schedulerDaemonOptions.DockerStorageBase,
		usingWSL:                     schedulerDaemonOptions.UsingWSL,
		prometheusInterval:           time.Second * time.Duration(schedulerDaemonOptions.PrometheusInterval),
		prometheusPort:               schedulerDaemonOptions.PrometheusPort,
		numResendAttempts:            schedulerDaemonOptions.NumResendAttempts,
		runKernelsInGdb:              schedulerDaemonOptions.RunKernelsInGdb,
	}

	for _, configFunc := range configs {
		configFunc(daemon)
	}

	config.InitLogger(&daemon.log, daemon)

	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon,
		fmt.Sprintf("LocalDaemon_%s", nodeName), true, metrics.LocalDaemon)

	if daemon.numResendAttempts <= 0 {
		daemon.log.Error("Invalid number of message resend attempts specified: %d. Defaulting to %d.",
			daemon.numResendAttempts, DefaultNumResendAttempts)
		daemon.numResendAttempts = DefaultNumResendAttempts
	}

	daemon.resourceManager = scheduling.NewResourceManager(&types.Float64Spec{
		GPUs:     types.GPUSpec(schedulerDaemonOptions.NumGPUs),
		CPUs:     scheduling.MillicpusPerHost,
		MemoryMb: scheduling.MemoryMbPerHost})

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

	if len(schedulerDaemonOptions.HdfsNameNodeEndpoint) == 0 {
		panic("HDFS NameNode endpoint is empty.")
	}

	switch schedulerDaemonOptions.SchedulingPolicy {
	case "default":
		{
			daemon.schedulingPolicy = "default"
			daemon.log.Debug("Using the 'DEFAULT' scheduling policy.")
		}
	case "static":
		{
			daemon.schedulingPolicy = "static"
			daemon.log.Debug("Using the 'STATIC' scheduling policy.")
		}
	case "dynamic-v3":
		{
			daemon.schedulingPolicy = "dynamic-v3"
			daemon.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	case "dynamic-v4":
		{
			daemon.schedulingPolicy = "dynamic-v4"
			daemon.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")

			panic("The 'DYNAMIC' scheduling policy is not yet supported.")
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", schedulerDaemonOptions.SchedulingPolicy))
		}
	}

	switch schedulerDaemonOptions.DeploymentMode {
	case "":
		{
			daemon.log.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			daemon.deploymentMode = types.LocalMode
		}
	case "local":
		daemon.log.Info("Running in LOCAL mode.")
		daemon.deploymentMode = types.LocalMode
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
		}
	case "docker-swarm":
		{
			daemon.log.Info("Running in DOCKER SWARM mode.")
			daemon.deploymentMode = types.DockerSwarmMode
		}
	case "kubernetes":
		{
			daemon.log.Info("Running in KUBERNETES mode.")
			daemon.deploymentMode = types.KubernetesMode
		}
	default:
		{
			daemon.log.Error("Unknown/unsupported deployment mode: \"%s\"", schedulerDaemonOptions.DeploymentMode)
			daemon.log.Error("The supported deployment modes are: ")
			daemon.log.Error("- \"kubernetes\"")
			daemon.log.Error("- \"docker-swarm\"")
			daemon.log.Error("- \"docker-compose\"")
			daemon.log.Error("- \"local\"")
		}
	}

	daemon.log.Debug("Connection options: %v", daemon.connectionOptions)

	if !schedulerDaemonOptions.IsLocalMode() && len(nodeName) == 0 {
		panic("Node name is empty.")
	}

	if schedulerDaemonOptions.IsLocalMode() && len(nodeName) == 0 {
		daemon.nodeName = types.LocalNode
	}

	if schedulerDaemonOptions.IsDockerComposeMode() && len(nodeName) == 0 {
		daemon.nodeName = types.VirtualDockerNode
	}

	if schedulerDaemonOptions.IsDockerSwarmMode() && len(nodeName) == 0 {
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

func (d *SchedulerDaemonImpl) Provisioner() proto.ClusterGatewayClient {
	return d.provisioner
}

func (d *SchedulerDaemonImpl) SetProvisioner(provisioner proto.ClusterGatewayClient) {
	d.provisioner = provisioner
}

// SetID sets the SchedulerDaemonImpl id by the gateway.
// This also instructs the Local Daemon to create a LocalDaemonPrometheusManager and begin serving metrics.
func (d *SchedulerDaemonImpl) SetID(_ context.Context, in *proto.HostId) (*proto.HostId, error) {
	// If id has been set(e.g., restored after restart), return the original id.
	if d.id != "" {
		return &proto.HostId{
			Id:       d.id,
			NodeName: d.nodeName,
		}, nil
	}

	d.id = in.Id
	in.NodeName = d.nodeName // We're passing this value back

	d.log.Debug("Set ID to \"%s\"", d.id)

	// Update the ID field of the router and of any existing kernels.
	d.router.SetComponentId(in.Id)
	d.kernels.Range(func(_ string, replicaClient *client.KernelReplicaClient) (contd bool) {
		replicaClient.SetComponentId(in.Id)
		return true
	})

	if d.prometheusManager != nil {
		// We'll just restart the Local Daemon's Prometheus Manager.
		_ = d.prometheusManager.Stop()
		if err := d.prometheusManager.Start(); err != nil {
			d.log.Error("Failed to start Prometheus Manager because: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		d.prometheusManager = metrics.NewLocalDaemonPrometheusManager(8089, in.Id)
		err := d.prometheusManager.Start()
		if err != nil {
			d.log.Error("Failed to start Prometheus Manager because: %v", err)
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
			d.kernels.Range(func(_ string, replicaClient *client.KernelReplicaClient) (contd bool) {
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

// updatePrometheusGpuMetrics updates all the resource-related Prometheus metrics.
// updatePrometheusGpuMetrics is used as a callback by the GPU/Resource Manager.
//
// Deprecated: superseded by updatePrometheusResourceMetrics.
func (d *SchedulerDaemonImpl) updatePrometheusGpuMetrics(idleGpus float64, pendingGpus float64, committedGpus float64) {
	d.prometheusManager.IdleGpuGauge.
		Set(idleGpus)
	d.prometheusManager.PendingGpuGauge.
		Set(pendingGpus)
	d.prometheusManager.CommittedGpuGauge.
		Set(committedGpus)
}

// updatePrometheusResourceMetrics updates all the resource-related Prometheus metrics.
// updatePrometheusResourceMetrics is used as a callback by the GPU/Resource Manager.
func (d *SchedulerDaemonImpl) updatePrometheusResourceMetrics(resources scheduling.ResourceStateWrapper) {
	// CPU resource metrics.
	d.prometheusManager.IdleCpuGauge.
		Set(resources.IdleResources().Millicpus())
	d.prometheusManager.PendingCpuGauge.
		Set(resources.PendingResources().Millicpus())
	d.prometheusManager.CommittedCpuGauge.
		Set(resources.CommittedResources().Millicpus())

	// Memory resource metrics.
	d.prometheusManager.IdleMemoryGauge.
		Set(resources.IdleResources().MemoryMB())
	d.prometheusManager.PendingMemoryGauge.
		Set(resources.PendingResources().MemoryMB())
	d.prometheusManager.CommittedMemoryGauge.
		Set(resources.CommittedResources().MemoryMB())

	// GPU resource metrics.
	d.prometheusManager.IdleGpuGauge.
		Set(resources.IdleResources().GPUs())
	d.prometheusManager.PendingGpuGauge.
		Set(resources.PendingResources().GPUs())
	d.prometheusManager.CommittedGpuGauge.
		Set(resources.CommittedResources().GPUs())
}

// StartKernel starts a single kernel.
func (d *SchedulerDaemonImpl) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return d.StartKernelReplica(ctx, &proto.KernelReplicaSpec{
		ReplicaId: 1,
		Replicas:  nil,
		Kernel:    in,
	})
}

// Register a Kernel that has started running on the same node as we are.
// This method must be thread-safe.
func (d *SchedulerDaemonImpl) registerKernelReplica(ctx context.Context, kernelRegistrationClient *KernelRegistrationClient) {
	registeredAt := time.Now()
	d.log.Debug("Registering Kernel at (remote) address %v", kernelRegistrationClient.conn.RemoteAddr())

	remoteIp, _, err := net.SplitHostPort(kernelRegistrationClient.conn.RemoteAddr().String())
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to extract remote address from kernel registration connection because: %v", err)
		d.log.Error(errorMessage)
		d.log.Error("Cannot register kernel.") // TODO(Ben): Handle this more elegantly.
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
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
		panic(fmt.Sprintf("Connection info sent to us by kernel at %s is nil.", remoteIp))
	}
	d.log.Debug("connInfo: %v", connInfo)

	kernelReplicaSpec := &proto.KernelReplicaSpec{
		Kernel:       registrationPayload.Kernel,
		ReplicaId:    registrationPayload.ReplicaId,   // Can get (from config file).
		NumReplicas:  registrationPayload.NumReplicas, // Can get (from config file).
		Join:         registrationPayload.Join,        // Can get (from config file).
		PersistentId: registrationPayload.PersistentId,
	}

	d.log.Debug("Kernel replica spec: %v", kernelReplicaSpec)

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		panic(err)
	}

	d.log.Debug("Allocating the following \"listen\" ports to replica %d of kernel %s: %v",
		registrationPayload.ReplicaId, registrationPayload.Kernel.Id, listenPorts)

	// If we're running in Kubernetes mode, then we need to create a new kernel client here (as well as a new DockerInvoker).
	// If we're running in Docker mode, then we'll already have created the kernel client for this kernel.
	// We create the kernel client in Docker mode when we launch the kernel (using a DockerInvoker).
	var (
		kernel                      *client.KernelReplicaClient
		kernelClientCreationChannel chan *proto.KernelConnectionInfo
		loaded                      bool
		kernelConnectionInfo        *proto.KernelConnectionInfo
	)
	if d.deploymentMode == types.KubernetesMode {
		invokerOpts := &invoker.DockerInvokerOptions{
			HdfsNameNodeEndpoint: d.hdfsNameNodeEndpoint,
			KernelDebugPort:      -1,
			DockerStorageBase:    d.dockerStorageBase,
			UsingWSL:             d.usingWSL,
			RunKernelsInGdb:      d.runKernelsInGdb,
		}

		dockerInvoker := invoker.NewDockerInvoker(d.connectionOptions, invokerOpts, d.prometheusManager)
		kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, dockerInvoker)
		// We're passing "" for the persistent ID here; we'll re-assign it once we receive the persistent ID from the Cluster Gateway.
		kernel = client.NewKernelReplicaClient(kernelCtx, kernelReplicaSpec, connInfo, d.id, true,
			d.numResendAttempts, listenPorts[0], listenPorts[1], registrationPayload.PodName, registrationPayload.NodeName,
			d.smrReadyCallback, d.smrNodeAddedCallback, "", d.id, nil, metrics.LocalDaemon, false,
			false, d.prometheusManager, d.kernelReconnectionFailed,
			d.kernelRequestResubmissionFailedAfterReconnection)

		kernelConnectionInfo, err = d.initializeKernelClient(registrationPayload.Kernel.Id, connInfo, kernel)
		if err != nil {
			errorMessage := fmt.Sprintf("Failed to initialize replica %d of kernel %s because: %v", registrationPayload.ReplicaId, registrationPayload.Kernel.Id, err)
			d.log.Error(errorMessage)
			go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
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
	} else {
		kernelClientCreationChannel, loaded = d.kernelClientCreationChannels.Load(kernelReplicaSpec.Kernel.Id)
		if !loaded {
			panic(fmt.Sprintf("Failed to load 'kernel client creation' channel for kernel \"%s\".", kernelReplicaSpec.Kernel.Id))
		}

		d.log.Debug("Waiting for notification that the KernelClient for kernel \"%s\" has been created.", kernelReplicaSpec.Kernel.Id)
		kernelConnectionInfo = <-kernelClientCreationChannel
		d.log.Debug("Received notification that the KernelClient for kernel \"%s\" was created.", kernelReplicaSpec.Kernel.Id)

		kernel, loaded = d.kernels.Load(kernelReplicaSpec.Kernel.Id)

		if !loaded {
			panic(fmt.Sprintf("Failed to load kernel client with ID \"%s\", even though one should have already been created...", kernelReplicaSpec.Kernel.Id))
		}

		createdAt, ok := d.getInvoker(kernel).KernelCreatedAt()
		if !ok {
			panic("Docker Invoker thinks it hasn't created kernel container, but kernel just registered...")
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
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       kernelConnectionInfo.ShellPort,
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       kernelConnectionInfo.IopubPort, // TODO(Ben): Are these still needed? I think so...
		IosubPort:       kernelConnectionInfo.IosubPort, // TODO(Ben): Are these still needed? I think so...
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}

	kernelRegistrationNotification := &proto.KernelRegistrationNotification{
		ConnectionInfo: info,
		KernelId:       kernel.ID(),
		SessionId:      "N/A",
		ReplicaId:      registrationPayload.ReplicaId,
		HostId:         d.id,
		KernelIp:       remoteIp,
		PodName:        registrationPayload.PodName,
		NodeName:       registrationPayload.NodeName,
		// ResourceSpec:   registrationPayload.ResourceSpec,
	}

	d.log.Info("Kernel %s registered: %v. Notifying Gateway now.", kernelReplicaSpec.ID(), info)

	numTries := 0
	maxNumTries := 3

	var response *proto.KernelRegistrationNotificationResponse
	// TODO: Figure out a better way to handle this. As of right now, we really cannot recover from this.
	for numTries < maxNumTries {
		response, err = d.provisioner.NotifyKernelRegistered(ctx, kernelRegistrationNotification)
		if err != nil {
			d.log.Error("Error encountered while notifying Gateway of kernel registration (attempt %d/%d): %s", numTries+1, maxNumTries, err.Error())
			numTries += 1

			if numTries < maxNumTries {
				time.Sleep(time.Second * time.Duration(numTries))
			}
			continue
		} else if response == nil {
			d.log.Error("Failed to notify Gateway of kernel registration (attempt %d/%d).", numTries+1, maxNumTries)
			numTries += 1

			if numTries < maxNumTries {
				time.Sleep(time.Second * time.Duration(numTries))
			}
			continue
		}

		break
	}

	if response == nil {
		d.log.Error("Failed to notify Gateway of kernel registration after %d attempts.", maxNumTries)
		panic(domain.ErrKernelRegistrationNotificationFailure)
	}

	d.log.Debug("Successfully notified Gateway of kernel registration. Will be assigning replica ID of %d to kernel. Replicas: %v.", response.Id, response.Replicas)

	if response.ResourceSpec == nil {
		errorMessage := fmt.Sprintf("ResourceSpec for kernel %s is nil.", kernel.ID())
		d.log.Error(errorMessage)
		go d.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
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

	kernel.SetResourceSpec(response.ResourceSpec)
	kernel.SetReplicaID(response.Id)

	var kernelDebugPort = -1
	kernelDebugPort, _ = d.kernelDebugPorts.Load(kernel.ID())

	payload := map[string]interface{}{
		"smr_node_id": response.Id,
		"hostname":    remoteIp,
		"replicas":    response.Replicas,
		"debug_port":  kernelDebugPort,
		"status":      "ok",
	}

	if response.PersistentId != nil && response.GetPersistentId() != "" {
		d.log.Debug("Including persistent store ID \"%s\" in notification response to replica %d of kernel %s.", response.GetPersistentId(), response.Id, kernel.ID())
		payload["persistent_id"] = response.GetPersistentId()
		kernel.SetPersistentID(response.GetPersistentId())
		// hdfsDataDirectoryExpected = true
	} else {
		d.log.Debug("No persistent ID to include in response.")
	}

	payload["should_read_data_from_hdfs"] = response.ShouldReadDataFromHdfs

	// if response.DataDirectory != nil && response.GetDataDirectory() != "" {
	// 	d.log.Debug("Including data directory \"%s\" in notification response to replica %d of kernel %s.", response.GetDataDirectory(), response.Id, kernel.ID())
	// 	payload["data_directory"] = response.GetDataDirectory()
	// } else {
	// 	d.log.Debug("No data directory to include in response.")
	// }

	payloadJson, err := json.Marshal(payload)
	if err != nil {
		d.log.Error("Error encountered while marshalling replica ID to JSON: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		panic(err)
	}

	bytesWritten, err := kernelRegistrationClient.conn.Write(payloadJson)
	if err != nil {
		d.log.Error("Error encountered while writing replica ID back to kernel: %v", err)
		// TODO(Ben): Handle gracefully. For now, panic so we see something bad happened.
		panic(err)
	}
	d.log.Debug("Wrote %d bytes back to kernel in response to kernel registration.", bytesWritten)

	// TODO(Ben): Need a better system for this. Basically, give the kernel time to setup its persistent store.
	// TODO: Is this still needed?
	time.Sleep(time.Second * 1)
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (d *SchedulerDaemonImpl) kernelReconnectionFailed(kernel *client.KernelReplicaClient, msg *jupyter.JupyterMessage, reconnectionError error) {
	// var messageType string = "N/A"
	// _, header, _, err := jupyter.HeaderFromMsg(msg)
	// if err != nil {
	// 	d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
	// 	d.log.Error("ZMQ message in question: %v", msg)
	// } else {
	// 	messageType = header.MsgType
	// }

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending %v \"%s\" message: %v", kernel.ReplicaID(), kernel.ID(), msg.Type, msg.JupyterMessageType(), reconnectionError)
	d.log.Error(errorMessage)

	go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
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
func (d *SchedulerDaemonImpl) kernelRequestResubmissionFailedAfterReconnection(kernel *client.KernelReplicaClient, msg *jupyter.JupyterMessage, resubmissionError error) { /* client client.DistributedKernelClient, */
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
		Title:            "Connection to Kernel Lost, Reconnection Succeeded, but Request Resubmission Failed",
		Message:          errorMessage,
		NotificationType: 0,
		Panicked:         false,
	})
}

func (d *SchedulerDaemonImpl) smrReadyCallback(kernelClient *client.KernelReplicaClient) {
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

	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypePrepareToMigrateRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
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
	_msg := &zmq4.Msg{Frames: frames}
	jMsg := jupyter.NewJupyterMessage(_msg)
	var requestWG sync.WaitGroup
	requestWG.Add(1)
	// var dataDirectory string

	err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, jMsg, func(kernel scheduling.KernelInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
		d.log.Debug("Received response from 'prepare-to-migrate' request.")

		for i, frame := range msg.Frames {
			d.log.Debug("Frame #%d: %s", i, string(frame))
		}

		var frames jupyter.JupyterFrames = msg.Frames
		var respMessage jupyter.MessageDataDirectory
		if err := frames.Validate(); err != nil {
			d.log.Error("Failed to validate frames of `MessageDataDirectory` message: %v", err)
			return err
		}
		err := frames.DecodeContent(&respMessage)
		if err != nil {
			d.log.Error("Failed to decode Content frame of `MessageDataDirectory` message: %v", err)
			return err
		}

		// It seems like this particular message is sent in the 6th frame, which is the Buffers frame, rather than the Content frame.
		if respMessage.NodeID == 0 {
			err := frames.DecodeBuffers(&respMessage)

			if err != nil {
				d.log.Error("Failed to decode Buffer frame of `MessageDataDirectory` message: %v", err)
				return err
			}
		}

		// dataDirectory = respMessage.DataDirectory
		if respMessage.Status == "error" {
			var msgErr jupyter.MessageError
			err := frames.DecodeBuffers(&msgErr)
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
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeUpdateReplicaRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
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

	_msg := &zmq4.Msg{Frames: frames}
	jMsg := jupyter.NewJupyterMessage(_msg)

	var currentNumTries = 0
	var maxNumTries = 3
	var success = false
	for currentNumTries < maxNumTries {
		var wg sync.WaitGroup
		var requestReceived int32 = 0
		wg.Add(1)
		err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, jMsg, func(kernel scheduling.KernelInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
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
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeAddReplicaRequest, kernel.Sessions()[0])
	if err := frames.EncodeContent(&jupyter.MessageSMRAddOrUpdateReplicaRequest{
		NodeID:  replicaId,
		Address: hostname, // s.daemon.getInvoker(kernel).GetReplicaAddress(kernel.KernelSpec(), replicaId),
	}); err != nil {
		d.log.Error("Failed to encode content of \"MessageSMRAddOrUpdateReplicaRequest\" message: %v", err)
		return nil, err
	}

	if _, err := frames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Error occurred while signing frames for add-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	_msg := &zmq4.Msg{Frames: frames}
	jMsg := jupyter.NewJupyterMessage(_msg)

	var wg sync.WaitGroup
	wg.Add(1)

	err := kernel.RequestWithHandler(context.Background(), "Sending", jupyter.ControlMessage, jMsg, nil, wg.Done)

	if err != nil {
		d.log.Error("Error occurred while issuing add-replica request to kernel %s: %v", kernelId, err)
		return proto.VOID, err
	}

	wg.Wait()

	return proto.VOID, nil
}

func (d *SchedulerDaemonImpl) smrNodeAddedCallback(readyMessage *jupyter.MessageSMRNodeUpdated) {
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
	return d.deploymentMode == types.DockerComposeMode
}

// DockerMode returns true if we're running in either "docker swarm" or "docker compose".
// That is, DockerMode turns true if and only if one of DockerSwarmMode or DockerComposeMode return true.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *SchedulerDaemonImpl) DockerMode() bool {
	return d.DockerComposeMode() || d.DockerComposeMode()
}

// KubernetesMode returns true if we're running in Kubernetes.
func (d *SchedulerDaemonImpl) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// LocalMode returns true if we're running in Local mode.
func (d *SchedulerDaemonImpl) LocalMode() bool {
	return d.deploymentMode == types.LocalMode
}

// Initialize a kernel client for a new kernel.
// Initialize shell/IO forwarders, validate the connection with the kernel (which includes connecting to the ZMQ sockets), etc.
// If there's an error at any point during the initialization process, then the kernel connection/client is closed and an error is returned.
func (d *SchedulerDaemonImpl) initializeKernelClient(id string, connInfo *jupyter.ConnectionInfo, kernel *client.KernelReplicaClient) (*proto.KernelConnectionInfo, error) {
	shell := d.router.Socket(jupyter.ShellMessage)
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
	_ = kernel.AddIOHandler(jupyter.MessageTypeSMRLeadTask, d.handleSMRLeadTask)
	_ = kernel.AddIOHandler(jupyter.MessageTypeErrorReport, d.handleErrorReport)

	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager (manager.py).
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	info := &proto.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(shell.Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
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
	d.log.Debug("SchedulerDaemonImpl::StartKernelReplica(\"%s\"). ResourceSpec: %v", kernelId, in.Kernel.ResourceSpec)

	if otherReplica, loaded := d.kernels.Load(kernelId); loaded {
		d.log.Error("We already have a replica of kernel %s running locally (replica %d). Cannot launch new replica on this node.", kernelId, otherReplica.ReplicaID())
		return nil, ErrExistingReplicaAlreadyRunning
	}

	err := d.resourceManager.KernelReplicaScheduled(in.ReplicaId, in.Kernel.Id, in.Kernel.ResourceSpec)
	// err := d.resourceManager.AllocatePendingGPUs(decimal.NewFromFloat(float64(in.Kernel.ResourceSpec.Gpu)), in.ReplicaId, in.Kernel.Id)
	if err != nil {
		d.log.Error("Failed to allocate %d pending GPUs for new replica %d of kernel %s because: %v",
			in.Kernel.ResourceSpec.Gpu, in.ReplicaId, in.Kernel.Id, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	var kernelInvoker invoker.KernelInvoker
	if d.DockerMode() {
		invokerOpts := &invoker.DockerInvokerOptions{
			HdfsNameNodeEndpoint: d.hdfsNameNodeEndpoint,
			KernelDebugPort:      int(in.DockerModeKernelDebugPort),
			DockerStorageBase:    d.dockerStorageBase,
			UsingWSL:             d.usingWSL,
			RunKernelsInGdb:      d.runKernelsInGdb,
		}
		kernelInvoker = invoker.NewDockerInvoker(d.connectionOptions, invokerOpts, d.prometheusManager.GetContainerMetricsProvider())
		// Note that we could pass d.prometheusManager directly in the call above.

		d.kernelDebugPorts.Store(kernelId, int(in.DockerModeKernelDebugPort))
	} else if d.LocalMode() {
		kernelInvoker = invoker.NewLocalInvoker()
	} else {
		panic(fmt.Sprintf("Unknown/unsupported deployment mode: \"%s\"", d.deploymentMode))
	}

	// When the kernel registers, we need the kernel client that we create here.
	// We use this channel to notify the goroutine handling the registration that the kernel client is set up and connected.
	kernelClientCreationChannel := make(chan *proto.KernelConnectionInfo)
	d.kernelClientCreationChannels.Store(in.Kernel.Id, kernelClientCreationChannel)

	connInfo, err := kernelInvoker.InvokeWithContext(ctx, in)
	if err != nil {
		go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
			Title:            fmt.Sprintf("Failed to Create Container for Kernel %s-%d", in.Kernel.Id, in.ReplicaId),
			Message:          err.Error(),
			NotificationType: 0,
			Panicked:         false,
		})
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Initialize kernel client with new context.
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, kernelInvoker)

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		d.log.Error("Failed to request listen ports for new kernel %s because: %v", in.ID(), err)
		return nil, err
	}

	kernel := client.NewKernelReplicaClient(kernelCtx, in, connInfo, d.id, true, d.numResendAttempts,
		listenPorts[0], listenPorts[1], types.DockerContainerIdTBD, types.DockerNode, d.smrReadyCallback, d.smrNodeAddedCallback,
		"", d.id, nil, metrics.LocalDaemon, false, false, d.prometheusManager,
		d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection)

	d.log.Debug("Allocating the following \"listen\" ports to replica %d of kernel %s: %v",
		in.ReplicaId, kernel.ID(), listenPorts)

	// Register kernel.
	d.kernels.Store(kernel.ID(), kernel)

	info, err := d.initializeKernelClient(in.Kernel.Id, connInfo, kernel)
	if err != nil {
		d.log.Error("Failed to initialize replica %d of kernel %s.", in.ReplicaId, in.Kernel.Id)
		return nil, err
	}

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

	listenPorts := []int{kernel.ShellListenPort(), kernel.IOPubListenPort()}
	err = d.availablePorts.ReturnPorts(listenPorts)
	if err != nil {
		return nil, d.errorf(err)
	}

	d.prometheusManager.NumActiveKernelReplicasGauge.Sub(1)

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

func (d *SchedulerDaemonImpl) stopKernel(ctx context.Context, kernel *client.KernelReplicaClient, ignoreReply bool) (err error) {
	if ignoreReply {
		_ = kernel.AddIOHandler(jupyter.IOTopicShutdown, d.handleIgnoreMsg)
	}

	var msg zmq4.Msg
	frames := jupyter.NewJupyterFramesWithHeader(jupyter.MessageTypeShutdownRequest, kernel.Sessions()[0])

	// Encode the content.
	if err = frames.EncodeContent(&jupyter.MessageShutdownRequest{
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
	err = kernel.RequestWithHandler(ctx, "Stopping by", jupyter.ControlMessage, jupyter.NewJupyterMessage(&msg), nil, wg.Done)
	if err != nil {
		return err
	}

	d.log.Debug("Sent \"%s\" message to replica %d of kernel %s.", jupyter.MessageTypeShutdownRequest, kernel.ReplicaID(), kernel.ID())

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
	defer registryListener.Close()
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

// RouterProvider implementations.

func (d *SchedulerDaemonImpl) ControlHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
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
			jupyter.ShellMessage, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
	}

	if err := d.forwardRequest(context.Background(), kernel, jupyter.ControlMessage, msg, nil); err != nil {
		return err
	}

	// Handle ShutdownRequest
	if msg.JupyterMessageType() == domain.ShellShutdownRequest {
		go func() {
			kernelStatus, err := d.getInvoker(kernel).Wait() // Wait() will detect the kernel status and the cleanup() will clean kernel automatically.
			_, _ = d.statusErrorf(kernel, kernelStatus, err)
		}()
	}

	return nil
}

func (d *SchedulerDaemonImpl) kernelShellHandler(info scheduling.KernelInfo, _ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
	return d.ShellHandler(info, msg)
}

func (d *SchedulerDaemonImpl) ShellHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	// d.log.Debug("Received shell message with %d frame(s): %s", len(msg.Frames), msg)
	// kernelId, header, offset, err := d.headerAndOffsetFromMsg(msg)
	// if err != nil {
	// 	return err
	// }

	session := msg.JupyterSession()
	kernel, ok := d.kernels.Load(session)
	msgType := msg.JupyterMessageType()
	if !ok && (msgType == domain.ShellKernelInfoRequest || msgType == domain.ShellExecuteRequest) {
		// Register kernel on ShellKernelInfoRequest
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

	// d.log.Debug("Shell message with %d frame(s) is targeting replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)

	// TODO(Ben): We'll inspect here to determine if the message is an execute_request.
	// If it is, then we'll see if we have enough resources for the kernel to (potentially) execute the code.
	// If not, we'll change the message's header to "yield_execute".
	// If the message is an execute_request message, then we have some processing to do on it.
	if msg.JupyterMessageType() == domain.ShellExecuteRequest {
		msg = d.processExecuteRequest(msg, kernel) // , header, offset)
		d.log.Debug("Forwarding shell 'execution-request' with %d frames to replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)
	} else { // Print a message about forwarding generic shell message.
		d.log.Debug("Forwarding shell message with %d frames to replica %d of kernel %s: %s", len(msg.Frames), kernel.ReplicaID(), kernel.ID(), msg)

		// Extract the workload ID (which may or may not be included in the metadata of the request),
		// and assign it to the kernel ID if it hasn't already been assigned a value for this kernel.
		metadataDict, err := msg.DecodeMetadata()
		if err == nil {
			if workloadId, loaded := metadataDict["workload_id"]; loaded && !kernel.WorkloadIdSet() {
				kernel.SetWorkloadId(workloadId.(string))
			}
		} else {
			d.log.Warn("Could not decode metadata dictionary of %s \"%s\" message %s (JupyterID=\"%s\").",
				jupyter.ShellMessage, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := kernel.RequestWithHandler(ctx, "Forwarding", jupyter.ShellMessage, msg, d.kernelResponseForwarder, func() {
		cancel()
		d.log.Debug("Done() called for shell \"%s\" message targeting replica %d of kernel %s. Cancelling.", msg.JupyterMessageType(), kernel.ReplicaID(), kernel.ID())
	}); err != nil {
		return err
	}

	return nil
}

// Deallocate the GPU resources associated with the kernel.
func (d *SchedulerDaemonImpl) processExecuteReply(msg *jupyter.JupyterMessage, kernel scheduling.KernelInfo /*, offset int */) error {
	kernelClient := kernel.(*client.KernelReplicaClient)
	// Check if we need to release allocated GPUs.
	// We only release allocated GPUs if this kernel replica executed the code.
	// If this replica yielded, then there will be no GPUs to release.
	jFrames := jupyter.JupyterFrames(msg.Frames[msg.Offset:])
	var msgErr jupyter.MessageError
	err := json.Unmarshal(jFrames[jupyter.JupyterFrameContent], &msgErr)
	if err != nil {
		d.log.Error("Failed to unmarshal shell message received from replica %d of kernel %s because: %v", kernelClient.ReplicaID(), kernelClient.ID(), err)
		return err
	}

	// shouldReleaseResources basically indicates whether this kernel replica was leading the execution and thus
	// actively training / running user-submitted code. If it was, then we'll now need to release its resources.
	var shouldReleaseResources bool
	if msgErr.Status == jupyter.MessageStatusOK {
		d.log.Debug("Status of \"execute_reply\" message from replica %d of kernel %s is OK.", kernelClient.ReplicaID(), kernelClient.ID())
		shouldReleaseResources = true // Replica was leader and is done executing.
	} else if msgErr.Status == jupyter.MessageStatusError {
		d.log.Warn("Status of \"execute_reply\" message from replica %d of kernel %s is \"%s\": %v", kernelClient.ReplicaID(), kernelClient.ID(), msgErr.Status, msgErr.String())

		// Since there was an error, we should only release resources if the error occurred while
		// executing the user-submitted Python code.
		//
		// If the returned error is ExecutionYieldError, then the replica did not lead the execution,
		// and therefore it will not have any resources committed to it.
		shouldReleaseResources = msgErr.ErrName != jupyter.MessageErrYieldExecution
	} else {
		d.log.Error("Unexpected message status in \"execute_reply\" message from replica %d of kernel %s: \"%s\"", kernelClient.ReplicaID(), kernelClient.ID(), msgErr.Status)
	}

	//err = d.resourceManager.ReleaseAllocatedGPUs(kernelClient.ReplicaID(), kernel.ID())

	// Release any resources committed to the kernel replica, as it is done training and does not need the resources
	// to be actively-bound/committed to it anymore.
	if shouldReleaseResources {
		if err = d.resourceManager.ReleaseCommittedResources(kernelClient.ReplicaID(), kernel.ID()); err != nil {
			d.log.Error("Failed to release GPUs allocated to replica %d of kernel %s because: %v", kernelClient.ReplicaID(), kernel.ID(), err)
		}

		_ = kernelClient.TrainingStopped()
		d.prometheusManager.TrainingTimeGaugeVec.
			With(prometheus.Labels{"workload_id": kernelClient.WorkloadId(), "kernel_id": kernelClient.ID(), "node_id": d.id}).
			Add(time.Since(kernelClient.LastTrainingTimePrometheusUpdate()).Seconds())
	}

	d.prometheusManager.NumTrainingEventsCompletedCounter.Inc()

	return nil /* will be nil on success */
}

// func (d *SchedulerDaemonImpl) processExecuteRequest(msg *jupyter.JupyterMessage, kernel *client.KernelReplicaClient, header *jupyter.MessageHeader, offset int) *jupyter.JupyterMessage {
func (d *SchedulerDaemonImpl) processExecuteRequest(msg *jupyter.JupyterMessage, kernel *client.KernelReplicaClient) *jupyter.JupyterMessage {
	// There may be a particular replica specified to execute the request. We'll extract the ID of that replica to this variable, if it is present.
	var targetReplicaId int32 = -1

	// TODO(Ben): Check GPU resources. If there are sufficiently-many GPUs available, then leave the message as-is.
	// If there are insufficient GPUs available, then we'll modify the message to be a "yield_execute" message.
	// This will force the replica to necessarily yield the execution to the other replicas.
	// If no replicas are able to execute the code due to resource contention, then a new replica will be created dynamically.
	var frames = jupyter.JupyterFrames(msg.Frames)
	var metadataFrame = frames[msg.Offset:].MetadataFrame()
	var metadataDict map[string]interface{}

	d.log.Debug("Processing `execute_request` for kernel %s now.", kernel.ID())

	// Don't try to unmarshal the metadata frame unless the size of the frame is non-zero.
	if len(metadataFrame.Frame()) > 0 {
		// Unmarshal the frame.
		err := metadataFrame.Decode(&metadataDict)
		if err != nil {
			d.log.Error("Error unmarshalling metadata frame for 'execute_request' message: %v", err)
		} else {
			d.log.Debug("Unmarshalled metadata frame for 'execute_request' message: %v", metadataDict)

			// See if there are any notable custom arguments, such as a target replica.
			if val, ok := metadataDict[domain.TargetReplicaArg]; ok {
				targetReplicaAsFloat64, ok := val.(float64)
				if !ok {
					d.log.Error("Could not parse target replica ID in metadata ('%v') for 'execute_request' message: %v", val, err)
					targetReplicaId = -1
				} else {
					targetReplicaId = int32(targetReplicaAsFloat64)
					d.log.Debug("Found target replica argument for 'execute_request' message. Target replica: %d.", targetReplicaId)
				}
			}
		}
	} else {
		// If we're not retrieving the map from the message, then we'll create a new map here.
		metadataDict = make(map[string]interface{})
	}

	// Extract the workload ID (which may or may not be included in the metadata of the request),
	// and assign it to the kernel ID if it hasn't already been assigned a value for this kernel.
	if workloadId, loaded := metadataDict["workload_id"]; loaded && !kernel.WorkloadIdSet() {
		kernel.SetWorkloadId(workloadId.(string))
	}

	var (
		// Check if another replica was specified as the one that should execute the code.
		// If this is true, then we'll yield the execution.
		// Note that we may pass 0 to force the execution to fail, for testing/debugging purposes.
		// No SMR replica can have an ID of 0.
		differentTargetReplicaSpecified = targetReplicaId != int32(-1) && targetReplicaId != kernel.ReplicaID()

		// Will store the return value of `AllocatePendingGPUs`. If it is non-nil, then the allocation failed due to insufficient resources.
		err error

		// The number of GPUs required by this kernel replica.
		//requiredGPUs decimal.Decimal
	)

	//if kernel.ResourceSpec() == nil {
	//	d.log.Error("Kernel %s (replica %d) does not have a ResourceSpec associated with it...", kernel.ID(), kernel.ReplicaID())
	//	requiredGPUs = ZeroDecimal.Copy()
	//} else {
	//	d.log.Debug("Kernel %s requires %d GPU(s).", kernel.ID(), kernel.ResourceSpec().GPU())
	//	requiredGPUs = decimal.NewFromFloat(kernel.ResourceSpec().GPU())
	//}

	// If the error is non-nil, then there weren't enough idle GPUs available.
	//if !differentTargetReplicaSpecified {
	//	// Only bother trying to allocate GPUs if the request isn't explicitly targeting another replica.
	//	err = d.resourceManager.AllocatePendingGPUs(requiredGPUs, kernel.ReplicaID(), kernel.ID())
	//}

	// Include the current number of idle GPUs available.
	idleGPUs := d.resourceManager.IdleGPUs()
	d.log.Debug("Including idle-gpus (%s) in request metadata.", idleGPUs.StringFixed(0))
	metadataDict["idle-gpus"] = idleGPUs

	// There are several circumstances in which we'll need to tell our replica of the target kernel to yield the execution to one of the other replicas:
	// - If there are insufficient GPUs on this node, then our replica will need to yield.
	// - If one of the other replicas was explicitly specified as the target replica, then our replica will need to yield.
	if differentTargetReplicaSpecified || kernel.SupposedToYieldNextExecutionRequest() { // err != nil ||
		var reason domain.YieldReason
		// Log message depends on which condition was true (first).
		//if err != nil {
		//	d.log.Debug("Insufficient GPUs available (%s) for replica %d of kernel %s to execute code (%v required).", d.resourceManager.IdleGPUs(), kernel.ReplicaID(), kernel.ID(), 0 /* Placeholder */)
		//	reason = domain.YieldInsufficientGPUs
		//} else
		if differentTargetReplicaSpecified {
			d.log.Debug("Replica %d of kernel %s is targeted, while we have replica %d running on this node.", targetReplicaId, kernel.ID(), kernel.ReplicaID() /* Placeholder */)
			reason = domain.YieldDifferentReplicaTargeted
		} else if kernel.SupposedToYieldNextExecutionRequest() {
			d.log.Debug("Replica %d of kernel %s has been explicitly instructed to yield its next execution request.", kernel.ReplicaID(), kernel.ID())
			reason = domain.YieldExplicitlyInstructed
		}
		metadataDict["yield-reason"] = reason
		// Convert the message to a yield request.
		// We'll return this converted message, and it'll ultimately be forwarded to the kernel replica in place of the original 'execute_request' message.
		msg, _ = d.convertExecuteRequestToYieldExecute(msg) // , header, offset)
		frames = msg.Frames

		// If we were told explicitly to YIELD this execution request via the `YieldNextExecutionRequest` API, then record that we've done this.
		// Even if we're yielding for other reasons too, we should still record that we've done this.
		if kernel.SupposedToYieldNextExecutionRequest() {
			kernel.YieldedNextExecutionRequest()
		}
	}

	// Re-encode the metadata frame. It will have the number of idle GPUs available,
	// as well as the reason that the request was yielded (if it was yielded).
	err = frames[msg.Offset:].EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		panic(err)
	}

	// Regenerate the signature.
	framesWithoutIdentities, _ := jupyter.SkipIdentitiesFrame(msg.Frames)
	_, err = framesWithoutIdentities.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key))
	if err != nil {
		d.log.Error("Failed to sign updated JupyterFrames for \"%s\" message because: %v", msg.JupyterMessageType(), err)
		panic(err)
	}

	if verified := jupyter.ValidateFrames([]byte(kernel.ConnectionInfo().Key), kernel.ConnectionInfo().SignatureScheme, msg.Offset, msg.Frames); !verified {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'", kernel.ConnectionInfo().SignatureScheme, kernel.ConnectionInfo().Key)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", msg)
	}

	return msg
}

func (d *SchedulerDaemonImpl) StdinHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(context.Background(), nil, jupyter.StdinMessage, msg, nil)
}

func (d *SchedulerDaemonImpl) HBHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(context.Background(), nil, jupyter.HBMessage, msg, nil)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
// func (d *SchedulerDaemonImpl) kernelIdFromMsg(msg *jupyter.JupyterMessage) (id string, sessId bool, err error) {
// 	kernelId, _, offset := jupyter.ExtractDestFrame(msg.Frames)
// 	if kernelId != "" {
// 		return kernelId, false, nil
// 	}

// 	header, err := d.headerFromFrames(msg.Frames[offset:])
// 	if err != nil {
// 		return "", false, err
// 	}

// 	return header.Session, true, nil
// }

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

		return response, fmt.Errorf("domain.ErrInvalidParameter %w : cannot decrease the total number of vGPUs below the number of allocated vGPUs", domain.ErrInvalidParameter)
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

// convertExecuteRequestToYieldExecute converts the given message to a "yield_execute" message.
//
// This will return a COPY of the original message with the type field modified to contact "yield_execute" instead of "execute_request".
// On success, the returned error will be nil. If an error occurs, then the returned message will be nil, and the error will be non-nil.
//
// PRECONDITION: The given message must be an "execute_request" message.
// This function will NOT check this. It should be checked before calling this function.
func (d *SchedulerDaemonImpl) convertExecuteRequestToYieldExecute(msg *jupyter.JupyterMessage /*, header *jupyter.MessageHeader, offset int*/) (*jupyter.JupyterMessage, error) {
	d.log.Debug("Converting 'execute_request' message to 'yield_request' message.")

	var err error

	// Clone the original message.
	var newMessage = msg.Clone()
	jMsg := jupyter.NewJupyterMessage(&newMessage)

	// Change the message header.
	jMsg.SetMessageType(domain.ShellYieldExecute)

	// Create a JupyterFrames struct by wrapping with the message's frames.
	jFrames := jupyter.JupyterFrames(newMessage.Frames)
	if err = jFrames.Validate(); err != nil {
		d.log.Error("Error encountered while converting 'execute_request' message to 'yield_execute' message, specifically while validating the existing frames: %v", err)
		panic(err) // TODO(Ben): Handle this error more gracefully.
		// return nil, err
	}

	// Replace the header with the new header (that has the 'yield_execute' MsgType).
	header, err := jMsg.GetHeader()
	if err != nil {
		panic(err)
	}

	if jFrames[jupyter.JupyterFrameHeader+jMsg.Offset], err = json.Marshal(header); err != nil {
		d.log.Error("Error encountered while converting 'execute_request' message to 'yield_execute' message, specifically while encoding the new message header: %v", err)
		panic(err) // TODO(Ben): Handle this error more gracefully.
		// return nil, err
	}

	// Replace the frames of the cloned message.
	newMessage.Frames = jFrames
	jMsg.Msg = &newMessage

	return jMsg, nil
}

// func (d *SchedulerDaemonImpl) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
// 	jFrames := jupyter.JupyterFrames(frames)
// 	if err := jFrames.Validate(); err != nil {
// 		d.log.Debug("Failed to validate message frames while extracting header.")
// 		return nil, err
// 	}

// 	var header jupyter.MessageHeader
// 	if err := jFrames.DecodeHeader(&header); err != nil {
// 		d.log.Error("Failed to decode header %v from message frames: %v", string(jFrames[jupyter.JupyterFrameHeader]), err)
// 		return nil, err
// 	}

// 	return &header, nil
// }

// func (d *SchedulerDaemonImpl) headerAndOffsetFromMsg(msg *jupyter.JupyterMessage) (kernelId string, header *jupyter.MessageHeader, offset int, err error) {
// 	kernelId, _, offset = jupyter.ExtractDestFrame(msg.Frames)

// 	header, err = d.headerFromFrames(msg.Frames[offset:])

// 	return kernelId, header, offset, err
// }

func (d *SchedulerDaemonImpl) kernelFromMsg(msg *jupyter.JupyterMessage) (kernel *client.KernelReplicaClient, err error) {
	// id, _, err := d.kernelIdFromMsg(msg)
	// if err != nil {
	// 	return nil, err
	// }

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

func (d *SchedulerDaemonImpl) forwardRequest(ctx context.Context, kernel *client.KernelReplicaClient, typ jupyter.MessageType, msg *jupyter.JupyterMessage, done func()) (err error) {
	// goroutineId := goid.Get()
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	return kernel.RequestWithHandler(ctx, "Forwarding", typ, msg, d.kernelResponseForwarder, done)
}

func (d *SchedulerDaemonImpl) kernelResponseForwarder(from scheduling.KernelInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
	var (
		sender         jupyter.Sender
		connectionInfo *jupyter.ConnectionInfo
		requiresAck    bool
	)

	kernelClient := from.(*client.KernelReplicaClient)
	socket := from.Socket(typ)
	if socket == nil {
		requiresAck = d.router.ShouldAckMessages()

		socket = d.router.Socket(typ)

		// TODO (Ben): If we're forwarding a response back to the Cluster Gateway here, then the Cluster Gateway will ACK the response.
		// We need to:
		// (a) Listen for the ACK.
		// (b) Re-forward the kernel's response to the Cluster Gateway if we don't receive an ACK.
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

	if typ == jupyter.ShellMessage {
		// _, header, offset, err := jupyter.HeaderFromMsg(msg)
		// if err != nil {
		// 	d.log.Error("Failed to extract header from %v message for kernel %s because: %v", typ, from.ID(), err)
		// } else if header.MsgType == domain.ShellExecuteReply {
		// 	d.processExecuteReply(msg, from, offset)
		// }

		if msg.JupyterMessageType() == domain.ShellExecuteReply {
			err := d.processExecuteReply(msg, from)
			if err != nil {
				d.log.Error("Error while processing 'execute_reply' message from %s: %v", from.String(), err)
				return err
			}
		}
	}

	builder := jupyter.NewRequestBuilder(context.Background(), from.ID(), from.ID(), connectionInfo).
		WithAckRequired(jupyter.ShouldMessageRequireAck(typ) && requiresAck).
		WithMessageType(typ).
		WithBlocking(true).
		WithTimeout(jupyter.DefaultRequestTimeout).
		WithDoneCallback(jupyter.DefaultDoneHandler).
		WithMessageHandler(jupyter.DefaultMessageHandler).
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
	// err := sender.SendRequest(true, socket, "" /* will be auto-resolved */, msg, sender, from.(*client.KernelReplicaClient), -1 /* will be auto-resolved */)
	// err := socket.Send(*msg)
	err = sender.SendRequest(request, socket)
	if err != nil {
		d.log.Error("Error while forwarding %v response from kernel %s: %s", typ, from.ID(), err.Error())
	}

	return nil // Will be nil on success.
}

func (d *SchedulerDaemonImpl) handleErrorReport(kernel scheduling.Kernel, frames jupyter.JupyterFrames, _ *jupyter.JupyterMessage) error {
	var errorReport jupyter.ErrorReport
	if err := frames.DecodeContent(&errorReport); err != nil {
		d.log.Error("Failed to decode content of 'error report' message: %v", err)
		return err
	}

	d.log.Error("Received '%s' error from kernel %s: %v", errorReport.ErrorTitle, kernel.ID(), errorReport.ErrorMessage)

	if errorReport.KernelId != kernel.ID() {
		d.log.Error("Error report specifies kernel %s, but our records indicate that the report originated from kernel %s...", errorReport.KernelId, kernel.ID())
	}

	go d.notifyClusterGatewayOfError(context.TODO(), &proto.Notification{
		Title:            errorReport.ErrorTitle,
		Message:          errorReport.ErrorMessage,
		NotificationType: 0,
		Panicked:         false,
	})

	return nil
}

// notifyClusterGatewayOfError calls the Cluster Gateway's 'Notify' gRPC method.
// This is used to report errors to the Gateway.
// In general, this is done so that the errors can then be
// pushed to the frontend UI to inform the user.
func (d *SchedulerDaemonImpl) notifyClusterGatewayOfError(ctx context.Context, notification *proto.Notification) {
	_, err := d.provisioner.Notify(ctx, notification)

	if err != nil {
		d.log.Error("Failed to notify Cluster Gateway of error because: %v", err)
	}
}

func (d *SchedulerDaemonImpl) handleSMRLeadTask(kernel scheduling.Kernel, frames jupyter.JupyterFrames, _ *jupyter.JupyterMessage) error {
	messageType, err := frames.GetMessageType()
	if err != nil {
		d.log.Error("Failed to extract message type from SMR Lead ZMQ message: %v", err)
		return err
	}

	if messageType == jupyter.MessageTypeSMRLeadTask {
		var leadMessage jupyter.MessageSMRLeadTask
		if err := frames.DecodeContent(&leadMessage); err != nil {
			d.log.Error("Failed to decode content of SMR Lead ZMQ message: %v", err)
			return err
		}

		kernelReplicaClient := kernel.(*client.KernelReplicaClient)

		d.log.Debug("%v leads the task, GPU required (%v), notify the scheduler. Resources required: %v.", kernel, leadMessage.GPURequired, kernelReplicaClient.ResourceSpec())

		// We pass the ResourceSpec, which for now should be identical to the resource request already stored within the ResourceManager.
		// However, we may eventually submit updated resource requests on a per-training-event basis, so we just want the API to
		// support being able to adjust the resource requests dynamically.
		//
		// TODO: Verify that all the cases in which the ResourceManager panics are legitimately panic-worthy, rather than scenarios
		// that could arise during regular operation and should just be handled using the failure handler of whatever
		// scheduling procedure we have in place.
		if err := d.resourceManager.CommitResources(kernelReplicaClient.ReplicaID(), kernelReplicaClient.ID(), kernelReplicaClient.ResourceSpec()); err != nil {
			d.log.Error("Could not allocate actual GPUs to replica %d of kernel %s because: %v.", kernelReplicaClient.ReplicaID(), kernelReplicaClient.ID(), err)
			panic(err) // TODO(Ben): Handle gracefully.
		}

		_ = kernelReplicaClient.TrainingStarted()

		// Don't return here -- we want his to be forwarded to the Cluster Gateway.
		// return commonTypes.ErrStopPropagation
	} else if messageType == jupyter.MessageTypeLeadAfterYield {
		// TODO(Ben): Need a better way to propagate errors back to the user, either at the Jupyter Notebook or the Workload Driver.
		panic(fmt.Sprintf("Our replica of kernel %s was selected to lead an execution after explicitly yielding.", kernel.ID()))
	} else {
		d.log.Error("Received message of unexpected type '%s' for SMR Lead ZMQ message topic.", messageType)
		return domain.ErrUnexpectedZMQMessageType
	}

	return nil
}

func (d *SchedulerDaemonImpl) handleIgnoreMsg(kernel scheduling.Kernel, _ jupyter.JupyterFrames, raw *jupyter.JupyterMessage) error {
	d.log.Debug("%v ignores %v", kernel, raw)
	return types.ErrStopPropagation
}

func (d *SchedulerDaemonImpl) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *SchedulerDaemonImpl) statusErrorf(kernel *client.KernelReplicaClient, status jupyter.KernelStatus, err error) (*proto.KernelStatus, error) {
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

func (d *SchedulerDaemonImpl) getInvoker(kernel scheduling.Kernel) invoker.KernelInvoker {
	return kernel.Context().Value(ctxKernelInvoker).(invoker.KernelInvoker)
}

func (d *SchedulerDaemonImpl) closeKernel(kernel *client.KernelReplicaClient, reason string) {
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

func (d *SchedulerDaemonImpl) clearHandler(_ string, kernel *client.KernelReplicaClient) (contd bool) {
	err := d.getInvoker(kernel).Close()
	if err != nil {
		d.log.Error("Error while closing kernel %s: %v", kernel.String(), err)
	}
	return true
}

func (d *SchedulerDaemonImpl) gcHandler(kernelId string, kernel *client.KernelReplicaClient) (contd bool) {
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
