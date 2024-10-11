package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/proto"

	"github.com/elliotchance/orderedmap/v2"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/petermattis/goid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	dockerClient "github.com/docker/docker/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
)

const (
	KubernetesKernelName = "kernel-%s"
	ConnectionFileFormat = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat     = "config-%s-*.json"     // "*" is a placeholder for random string

	ErrorHostname = "ERROR" // We return this from certain gRPC calls when there's an error.

	// TargetReplicaArg is passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg  = "target_replica"
	ForceReprocessArg = "force_reprocess"

	SchedulingPolicyLocal     SchedulingPolicy = "local"
	SchedulingPolicyStatic    SchedulingPolicy = "static"
	SchedulingPolicyDynamicV3 SchedulingPolicy = "dynamic-v3"
	SchedulingPolicyDynamicV4 SchedulingPolicy = "dynamic-v4"
)

var (
	// gRPC errors

	ErrNotImplemented = status.Error(codes.Unimplemented, "not implemented in daemon")
	ErrNotSupported   = status.Error(codes.Unimplemented, "not supported in daemon")

	NotificationTypeNames = []string{"ERROR", "WARNING", "INFO", "SUCCESS"}

	// Internal errors

	ErrKernelNotFound          = status.Error(codes.InvalidArgument, "kernel not found")
	ErrKernelNotReady          = status.Error(codes.Unavailable, "kernel not ready")
	ErrActiveExecutionNotFound = status.Error(codes.InvalidArgument, "active execution for specified kernel could not be found")
	ErrKernelSpecNotFound      = status.Error(codes.InvalidArgument, "kernel spec not found")
	ErrResourceSpecNotFound    = status.Error(codes.InvalidArgument, "the kernel does not have a resource spec included with its kernel spec")
	ErrKernelIDRequired        = status.Error(codes.InvalidArgument, "kernel id frame is required for kernel_info_request")
	ErrDaemonNotFoundOnNode    = status.Error(codes.InvalidArgument, "could not find a local daemon on the specified kubernetes node")
	ErrFailedToVerifyMessage   = status.Error(codes.Internal, "failed to verify ZMQ message after (re)encoding it with modified contents")
	ErrSessionNotTraining      = status.Error(codes.Internal, "expected session to be training")
	ErrSessionNotFound         = status.Error(codes.InvalidArgument, "could not locate the requested scheduling.Session instance")
	ErrContainerNotFound       = status.Error(codes.InvalidArgument, "could not locate the requested scheduling.Container instance")
)

// SchedulingPolicy indicates the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
type SchedulingPolicy string

type GatewayDaemonConfig func(scheduling.ClusterGateway)

// FailureHandler defines a recovery callback for panics.
// The primary purpose is simply to send a notification to the dashboard that a panic occurred before exiting.
// This makes error detection easier (i.e., it's immediately obvious when the system breaks as we're notified
// visually of the panic in the cluster dashboard).
type FailureHandler func(c *client.DistributedKernelClient) error

// ClusterGatewayImpl serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bidirectional gRPC calls.
//
// Some useful resources for jupyter protocol:
// https://jupyter-client.readthedocs.io/en/stable/messaging.html
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type ClusterGatewayImpl struct {
	sync.Mutex

	// DebugMode is a configuration parameter that, when enabled, causes the RequestTrace to be enabled as well
	// as the request history.
	DebugMode bool

	id string
	// createdAt is the time at which the ClusterGatewayImpl struct was created.
	createdAt time.Time

	// schedulingPolicy refers to the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
	schedulingPolicy SchedulingPolicy
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    *scheduling.ClusterSchedulerOptions

	// cluster provisioning related members
	listener net.Listener
	cluster  scheduling.Cluster
	placer   scheduling.Placer

	// kernel members
	transport        string
	ip               string
	kernels          hashmap.HashMap[string, *client.DistributedKernelClient] // Map with possible duplicate values. We map kernel ID and session ID to the associated kernel. There may be multiple sessions per kernel.
	kernelIdToKernel hashmap.HashMap[string, *client.DistributedKernelClient] // Map from Kernel ID to client.DistributedKernelClient.
	kernelSpecs      hashmap.HashMap[string, *proto.KernelSpec]

	// numActiveKernels is the number of actively-running kernels.
	numActiveKernels atomic.Int32

	log logger.Logger

	// lifetime
	closed  int32
	cleaned chan struct{}

	// failureHandler is the ClusterGatewayImpl's FailureHandler (i.e., recovery callback for panics).
	// The primary purpose is simply to send a notification to the dashboard that a panic occurred before exiting.
	// This makes error detection easier (i.e., it's immediately obvious when the system breaks as we're notified
	// visually of the panic in the cluster dashboard).
	failureHandler FailureHandler

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// dockerNodeMutex is used to synchronize the operations involved with getting or modifying the
	// number of Local Daemon Docker nodes/containers.
	dockerNodeMutex sync.Mutex

	// waitGroups hashmap.HashMap[string, *sync.WaitGroup]
	waitGroups hashmap.HashMap[string, *registrationWaitGroups]

	// numResendAttempts is the number of times to try resending a message before giving up.
	numResendAttempts int

	// We configure a pool of available ports through Kubernetes.
	// This is the pool of ports. We use these ports to create ZMQ sockets for kernels.
	// If a kernel stops, then its ports are returned to the pool for future reuse.
	availablePorts *utils.AvailablePorts

	// The IOPub socket that all Jupyter clients subscribe to.
	// io pub *jupyter.Socket
	smrPort int

	// Map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting *hashmap.CornelkMap[string, chan struct{}]

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from Operation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, *AddReplicaOperation]]

	// Mapping from new kernel-replica key (i.e., <kernel-id>-<replica-id>) to AddReplicaOperation.
	addReplicaOperationsByKernelReplicaId *hashmap.CornelkMap[string, *AddReplicaOperation]

	// Mapping from NewPodName to chan string.
	// In theory, it's possible to receive a PodCreated notification from Kubernetes AFTER the replica within the new Pod
	// has started running and has registered with the Gateway. In this case, we won't be able to retrieve the AddReplicaOperation
	// associated with that replica via the new Pod's name, as that mapping is created when the PodCreated notification is received.
	// In this case, the goroutine handling the replica registration waits on a channel for the associated AddReplicaOperation.
	addReplicaNewPodNotifications *hashmap.CornelkMap[string, chan *AddReplicaOperation]

	// Used to wait for an explicit notification that a particular node was successfully removed from its SMR cluster.
	// smrNodeRemovedNotifications *hashmap.CornelkMap[string, chan struct{}]

	// Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.
	hdfsNameNodeEndpoint string

	// Kubernetes client. This is shared with the associated internalCluster Gateway.
	kubeClient scheduling.KubeClient

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerWatcher scheduling.ContainerWatcher

	// gRPC connection to the Dashboard.
	clusterDashboard proto.ClusterDashboardClient

	// Run via Docker on a single system rather than using the Kubernetes-based deployment.
	deploymentMode types.DeploymentMode

	// Docker client.
	dockerApiClient *dockerClient.Client

	// The name of the Docker network that the container is running within. Only used in Docker mode.
	dockerNetworkName string

	// MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements
	// for the ZMQ messages that we're forwarding back and forth between the Jupyter Server and the Local Daemons.
	//
	// MessageAcknowledgementsEnabled is controlled by the "acks_enabled" field of the configuration file.
	MessageAcknowledgementsEnabled bool

	// gatewayPrometheusManager serves Prometheus metrics and responds to HTTP GET queries
	// issued by Grafana to create Grafana Variables for use in creating Grafana Dashboards.
	gatewayPrometheusManager *metrics.GatewayPrometheusManager
	// Indicates that a goroutine has been started to publish metrics to Prometheus.
	servingPrometheus atomic.Int32
	// prometheusInterval is how often we publish metrics to Prometheus.
	prometheusInterval time.Duration
	// prometheusPort is the port on which this local daemon will serve Prometheus metrics.
	prometheusPort int

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	// TODO: Make this an field of the ClusterGateway and LocalDaemon structs.
	//		 Update in forwardRequest and kernelResponseForwarder, rather than in here.
	RequestLog *metrics.RequestLog
}

func New(opts *jupyter.ConnectionInfo, clusterDaemonOptions *domain.ClusterDaemonOptions, configs ...GatewayDaemonConfig) *ClusterGatewayImpl {
	clusterGateway := &ClusterGatewayImpl{
		id:                                    uuid.New().String(),
		connectionOptions:                     opts,
		createdAt:                             time.Now(),
		transport:                             "tcp",
		ip:                                    opts.IP,
		DebugMode:                             clusterDaemonOptions.CommonOptions.DebugMode,
		availablePorts:                        utils.NewAvailablePorts(opts.StartingResourcePort, opts.NumResourcePorts, 2),
		kernels:                               hashmap.NewCornelkMap[string, *client.DistributedKernelClient](128),
		kernelIdToKernel:                      hashmap.NewCornelkMap[string, *client.DistributedKernelClient](128),
		kernelSpecs:                           hashmap.NewCornelkMap[string, *proto.KernelSpec](128),
		waitGroups:                            hashmap.NewCornelkMap[string, *registrationWaitGroups](128),
		cleaned:                               make(chan struct{}),
		smrPort:                               clusterDaemonOptions.SMRPort,
		activeAddReplicaOpsPerKernel:          hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, *AddReplicaOperation]](64),
		addReplicaOperationsByKernelReplicaId: hashmap.NewCornelkMap[string, *AddReplicaOperation](64),
		kernelsStarting:                       hashmap.NewCornelkMap[string, chan struct{}](64),
		addReplicaNewPodNotifications:         hashmap.NewCornelkMap[string, chan *AddReplicaOperation](64),
		hdfsNameNodeEndpoint:                  clusterDaemonOptions.HdfsNameNodeEndpoint,
		dockerNetworkName:                     clusterDaemonOptions.DockerNetworkName,
		numResendAttempts:                     clusterDaemonOptions.NumResendAttempts,
		MessageAcknowledgementsEnabled:        clusterDaemonOptions.MessageAcknowledgementsEnabled,
		prometheusInterval:                    time.Second * time.Duration(clusterDaemonOptions.PrometheusInterval),
	}
	for _, configFunc := range configs {
		configFunc(clusterGateway)
	}
	config.InitLogger(&clusterGateway.log, clusterGateway)

	if clusterGateway.DebugMode {
		clusterGateway.log.Debug("Running in DebugMode.")
		clusterGateway.RequestLog = metrics.NewRequestLog()
	} else {
		clusterGateway.log.Debug("Not running in DebugMode.")
	}

	clusterGateway.router = router.New(context.Background(), clusterGateway.connectionOptions, clusterGateway,
		clusterGateway.MessageAcknowledgementsEnabled, "ClusterGatewayRouter", false,
		metrics.ClusterGateway, clusterGateway.DebugMode)

	clusterGateway.gatewayPrometheusManager = metrics.NewGatewayPrometheusManager(clusterDaemonOptions.PrometheusPort, clusterGateway)
	err := clusterGateway.gatewayPrometheusManager.Start()
	if err != nil {
		panic(err)
	}

	clusterGateway.publishPrometheusMetrics()
	clusterGateway.router.AssignPrometheusManager(clusterGateway.gatewayPrometheusManager)
	clusterGateway.router.SetComponentId(clusterGateway.id)

	// Initial values for these metrics.
	clusterGateway.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
		With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Set(0)
	clusterGateway.gatewayPrometheusManager.DemandGpusGauge.Set(0)
	clusterGateway.gatewayPrometheusManager.BusyGpusGauge.Set(0)

	if clusterGateway.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			clusterGateway.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			clusterGateway.ip = ip
		}
	}

	switch clusterDaemonOptions.SchedulingPolicy {
	case "default":
		{
			clusterGateway.schedulingPolicy = "default"
			clusterGateway.log.Debug("Using the 'DEFAULT' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.defaultFailureHandler
		}
	case string(SchedulingPolicyStatic):
		{
			clusterGateway.schedulingPolicy = SchedulingPolicyStatic
			clusterGateway.log.Debug("Using the 'STATIC' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.staticSchedulingFailureHandler
		}
	case string(SchedulingPolicyDynamicV3):
		{
			clusterGateway.schedulingPolicy = SchedulingPolicyDynamicV3
			clusterGateway.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.dynamicV3FailureHandler
		}
	case string(SchedulingPolicyDynamicV4):
		{
			clusterGateway.schedulingPolicy = SchedulingPolicyDynamicV4
			clusterGateway.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.dynamicV4FailureHandler
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", clusterDaemonOptions.SchedulingPolicy))
		}
	}

	switch clusterDaemonOptions.DeploymentMode {
	case "":
		{
			clusterGateway.log.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			clusterGateway.deploymentMode = types.LocalMode
		}
	case "local":
		{
			clusterGateway.log.Info("Running in LOCAL mode.")
			clusterGateway.deploymentMode = types.LocalMode
		}
	case "docker":
		{
			clusterGateway.log.Error("\"docker\" mode is no longer a valid deployment mode")
			clusterGateway.log.Error("The supported deployment modes are: ")
			clusterGateway.log.Error("- \"docker-swarm\"")
			clusterGateway.log.Error("- \"docker-compose\"")
			clusterGateway.log.Error("- \"kubernetes\"")
			clusterGateway.log.Error("- \"local\"")
			os.Exit(1)
		}
	case "docker-compose":
		{
			clusterGateway.log.Info("Running in DOCKER COMPOSE mode.")
			clusterGateway.deploymentMode = types.DockerComposeMode

			apiClient, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv)
			if err != nil {
				panic(err)
			}

			clusterGateway.dockerApiClient = apiClient
		}
	case "docker-swarm":
		{
			clusterGateway.log.Info("Running in DOCKER SWARM mode.")
			clusterGateway.deploymentMode = types.DockerSwarmMode

			apiClient, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv)
			if err != nil {
				panic(err)
			}

			clusterGateway.dockerApiClient = apiClient
		}
	case "kubernetes":
		{
			clusterGateway.log.Info("Running in KUBERNETES mode.")
			clusterGateway.deploymentMode = types.KubernetesMode
		}
	default:
		{
			clusterGateway.log.Error("Unknown/unsupported deployment mode: \"%s\"", clusterDaemonOptions.DeploymentMode)
			clusterGateway.log.Error("The supported deployment modes are: ")
			clusterGateway.log.Error("- \"kubernetes\"")
			clusterGateway.log.Error("- \"docker-swarm\"")
			clusterGateway.log.Error("- \"docker-compose\"")
			clusterGateway.log.Error("- \"local\"")
			os.Exit(1)
		}
	}

	// Create the internalCluster Scheduler.
	clusterSchedulerOptions := clusterDaemonOptions.ClusterSchedulerOptions
	hostSpec := &types.Float64Spec{
		GPUs:      float64(clusterSchedulerOptions.GpusPerHost),
		VRam:      scheduling.VramPerHostGb,
		Millicpus: scheduling.MillicpusPerHost,
		MemoryMb:  scheduling.MemoryMbPerHost,
	}
	if clusterGateway.KubernetesMode() {
		clusterGateway.kubeClient = NewKubeClient(clusterGateway, clusterDaemonOptions)
		clusterGateway.containerWatcher = clusterGateway.kubeClient

		clusterGateway.cluster = scheduling.NewKubernetesCluster(clusterGateway, clusterGateway.kubeClient, hostSpec, clusterGateway.gatewayPrometheusManager, &clusterSchedulerOptions)
	} else if clusterGateway.DockerMode() {
		clusterGateway.containerWatcher = NewDockerContainerWatcher(domain.DockerProjectName) /* TODO: Don't hardcode this (the project name parameter). */
		clusterGateway.cluster = scheduling.NewDockerComposeCluster(clusterGateway, hostSpec, clusterGateway.gatewayPrometheusManager, &clusterSchedulerOptions)
	}

	clusterGateway.gatewayPrometheusManager.ClusterSubscriptionRatioGauge.Set(clusterGateway.cluster.SubscriptionRatio())

	return clusterGateway
}

type registrationWaitGroups struct {
	// Decremented each time a kernel registers.
	registered    sync.WaitGroup
	numRegistered int

	// Decremented each time we've notified a kernel of its ID.
	notified    sync.WaitGroup
	numNotified int

	// The SMR node replicas in order according to their registration IDs.
	replicas map[int32]string

	// Synchronizes access to the `replicas` slice.
	replicasMutex sync.Mutex
}

// Create and return a pointer to a new registrationWaitGroups struct.
//
// Parameters:
// - numReplicas (int): Value to be added to the "notified" and "registered" sync.WaitGroups of the registrationWaitGroups.
func newRegistrationWaitGroups(numReplicas int) *registrationWaitGroups {
	wg := &registrationWaitGroups{
		replicas: make(map[int32]string),
	}

	wg.notified.Add(numReplicas)
	wg.registered.Add(numReplicas)

	return wg
}

func (wg *registrationWaitGroups) String() string {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()
	return fmt.Sprintf("RegistrationWaitGroups[NumRegistered=%d, NumNotified=%d]", wg.numRegistered, wg.numNotified)
}

// Notify calls `Done()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) Notify() {
	wg.notified.Done()

	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()
	wg.numNotified += 1
}

// Register calls `Done()` on the "registered" sync.WaitGroup.
func (wg *registrationWaitGroups) Register() {
	wg.registered.Done()

	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	wg.numRegistered += 1
}

func (wg *registrationWaitGroups) SetReplica(idx int32, hostname string) {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	wg.replicas[idx] = hostname
}

func (wg *registrationWaitGroups) GetReplicas() map[int32]string {
	return wg.replicas
}

func (wg *registrationWaitGroups) NumReplicas() int {
	return len(wg.replicas)
}

// RemoveReplica returns true if the node with the given ID was actually removed.
// If the node with the given ID was not present in the WaitGroup, then returns false.
func (wg *registrationWaitGroups) RemoveReplica(nodeId int32) bool {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	if _, ok := wg.replicas[nodeId]; !ok {
		return false
	}

	delete(wg.replicas, nodeId)

	return true
}

func (wg *registrationWaitGroups) AddReplica(nodeId int32, hostname string) map[int32]string {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	// var idx int32 = nodeId - 1

	// if idx > int32(len(wg.replicas)) {
	// 	panic(fmt.Sprintf("Cannot add node %d at index %d, as there is/are only %d replica(s) in the list already.", nodeId, idx, len(wg.replicas)))
	// }

	// // Add it to the end.
	// if idx == int32(len(wg.replicas)) {
	// 	wg.replicas = append(wg.replicas, hostname)
	// } else {
	// 	fmt.Printf("WARNING: Replacing replica %d (%s) at index %d with new replica %s.\n", nodeId, wg.replicas[idx], idx, hostname)
	// 	wg.replicas[idx] = hostname
	// }

	if _, ok := wg.replicas[nodeId]; ok {
		fmt.Printf("WARNING: Replacing replica %d (%s) with new replica %s.\n", nodeId, wg.replicas[nodeId], hostname)
	}

	wg.replicas[nodeId] = hostname

	return wg.replicas
}

// GetNotified returns the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) GetNotified() *sync.WaitGroup {
	return &wg.notified
}

// GetRegistered returns the "registered" sync.WaitGroup.
func (wg *registrationWaitGroups) GetRegistered() *sync.WaitGroup {
	return &wg.registered
}

// WaitNotified calls `Wait()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) WaitNotified() {
	wg.notified.Wait()
}

// WaitRegistered calls `Wait()` on the "registered" sync.WaitGroup.
func (wg *registrationWaitGroups) WaitRegistered() {
	wg.registered.Wait()
}

// Wait first calls `Wait()` on the "registered" sync.WaitGroup.
// Then, Wait calls `Wait()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) Wait() {
	wg.WaitRegistered()
	wg.WaitNotified()
}

func (d *ClusterGatewayImpl) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	receivedAt := time.Now()
	kernelId := in.KernelId

	var messageType string
	var socketType jupyter.MessageType
	if in.SocketType == "shell" {
		socketType = jupyter.ShellMessage
		messageType = "ping_kernel_shell_request"
	} else if in.SocketType == "control" {
		socketType = jupyter.ControlMessage
		messageType = "ping_kernel_ctrl_request"
	} else {
		d.log.Error("Invalid socket type specified: \"%s\"", in.SocketType)
		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, types.ErrInvalidSocketType
	}

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		d.log.Error("Received 'ping-kernel' request for unknown kernel \"%s\"...", kernelId)
		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, ErrKernelNotFound
	}

	var (
		msgId = uuid.NewString()
		msg   zmq4.Msg
		err   error
	)
	frames := jupyter.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.Sessions()[0])

	// If DebugMode is enabled, then add a buffers frame with a RequestTrace.
	var requestTrace *proto.RequestTrace
	if d.DebugMode {
		frames.Frames = append(frames.Frames, make([]byte, 0))

		requestTrace = proto.NewRequestTrace()

		// Then we'll populate the sort of metadata fields of the RequestTrace.
		requestTrace.MessageId = msgId
		requestTrace.MessageType = messageType
		requestTrace.KernelId = kernelId
		requestTrace.RequestReceivedByGateway = receivedAt.UnixMilli()

		// Create the wrapper/frame itself.
		wrapper := &proto.JupyterRequestTraceFrame{RequestTrace: requestTrace}

		marshalledFrame, err := json.Marshal(&wrapper)
		if err != nil {
			d.log.Error("Failed to marshall RequestTraceWrapper when creating %s \"%s\" request \"%s\".",
				msgId, socketType.String(), messageType)
			return nil, status.Error(codes.Internal, err.Error())
		}

		frames.Frames[jupyter.JupyterFrameRequestTrace] = marshalledFrame
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v", kernelId, kernel.ConnectionInfo().SignatureScheme, err)
		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, ErrFailedToVerifyMessage
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	respChan := make(chan interface{}, d.ClusterOptions.NumReplicas)

	startTime := time.Now()
	var numRepliesReceived atomic.Int32
	responseHandler := func(from scheduling.KernelReplicaInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)

		// Notify that all replies have been received.
		if msg.RequestTrace != nil {
			requestTrace := msg.RequestTrace
			if requestTrace.ReplicaId != -1 && requestTrace.ReplicaId != from.ReplicaID() {
				d.log.Warn("Overwriting existing replica ID of %d with %d in RequestTrace for %s \"%s\" message %s (JupyterID=\"%s\")",
					requestTrace.ReplicaId, from.ReplicaID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
			}

			requestTrace.ReplicaId = from.ReplicaID()

			d.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/3 replies. Time elapsed: %v. Request trace: %s.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, time.Since(startTime), msg.RequestTrace.String())
			respChan <- requestTrace
		} else {
			d.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/3 replies. Time elapsed: %v.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, time.Since(startTime))
			respChan <- struct{}{}
		}

		return nil
	}

	jMsg := jupyter.NewJupyterMessage(&msg)
	err = kernel.RequestWithHandler(ctx, "Forwarding", socketType, jMsg, responseHandler, func() {})
	if err != nil {
		d.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v", socketType.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)
		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, err
	}

	if d.DebugMode && d.RequestLog != nil {
		err = d.RequestLog.AddEntry(jMsg, socketType, requestTrace)
		if err != nil {
			d.log.Error("Failed to add entry to RequestLog for %s PingKernel: %v", socketType.String(), err)
		}
	}

	requestTraces := make([]*proto.RequestTrace, 0, d.ClusterOptions.NumReplicas)

	for numRepliesReceived.Load() < int32(d.ClusterOptions.NumReplicas) {
		select {
		case <-ctx.Done():
			{
				err := ctx.Err()

				var errorMessage string
				if err != nil {
					errorMessage = fmt.Sprintf("'ping-kernel' %v request for kernel %s failed after receiving %d/3 replies for ping message \"%s\": %v",
						socketType.String(), kernelId, numRepliesReceived.Load(), msgId, err)
				} else {
					errorMessage = fmt.Sprintf("'ping-kernel' %v request for kernel %s timed-out after receiving %d/3 replies for ping message \"%s\".",
						socketType.String(), kernelId, numRepliesReceived.Load(), msgId)
				}
				d.log.Error(errorMessage)

				return &proto.Pong{
					Id:            kernelId,
					Success:       false,
					Msg:           errorMessage,
					RequestTraces: requestTraces,
				}, types.ErrRequestTimedOut
			}
		case v := <-respChan:
			{
				requestTrace, ok := v.(*proto.RequestTrace)
				if ok {
					requestTraces = append(requestTraces, requestTrace)
				}
			}
		}
	}

	// Include the "ReplySentByGateway" entry, since we're returning the response via gRPC,
	// and thus it won't be added automatically by the ZMQ-forwarder server.
	replySentByGateway := time.Now().UnixMilli()
	for _, requestTrace := range requestTraces {
		requestTrace.ReplySentByGateway = replySentByGateway
	}

	d.log.Debug("Received all 3 %v 'ping_reply' responses from replicas of kernel %s for ping message \"%s\" in %v.",
		socketType.String(), kernelId, msgId, time.Since(startTime))
	return &proto.Pong{
		Id:            kernelId,
		Success:       true,
		RequestTraces: requestTraces,
	}, nil
}

func (d *ClusterGatewayImpl) SetClusterOptions(opts *scheduling.ClusterSchedulerOptions) {
	d.ClusterOptions = opts
}

func (d *ClusterGatewayImpl) ConnectionOptions() *jupyter.ConnectionInfo {
	return d.connectionOptions
}

func (d *ClusterGatewayImpl) NumLocalDaemonsConnected() int {
	return d.cluster.Len()
}

func (d *ClusterGatewayImpl) NumKernels() int {
	return d.kernels.Len()
}

// publishPrometheusMetrics creates a goroutine that publishes metrics to prometheus on a configurable interval.
func (d *ClusterGatewayImpl) publishPrometheusMetrics() {
	go func() {
		// Claim ownership of publishing metrics.
		if !d.servingPrometheus.CompareAndSwap(0, 1) {
			return
		}

		d.log.Debug("Beginning to publish metrics to Prometheus now. Interval: %v", d.prometheusInterval)

		for {
			if d.cluster != nil {
				d.gatewayPrometheusManager.DemandGpusGauge.Set(d.cluster.DemandGPUs())
				d.gatewayPrometheusManager.BusyGpusGauge.Set(d.cluster.BusyGPUs())
				d.gatewayPrometheusManager.ClusterSubscriptionRatioGauge.Set(d.cluster.SubscriptionRatio())
			}

			time.Sleep(d.prometheusInterval)
		}
	}()
}

// GetHostsOfKernel returns the Host instances on which replicas of the specified kernel are scheduled.
func (d *ClusterGatewayImpl) GetHostsOfKernel(kernelId string) ([]*scheduling.Host, error) {
	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		return nil, ErrKernelNotFound
	}

	hosts := make([]*scheduling.Host, 0, len(kernel.Replicas()))
	for _, replica := range kernel.Replicas() {
		hosts = append(hosts, replica.GetHost())
	}

	return hosts, nil
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (d *ClusterGatewayImpl) Listen(transport string, addr string) (net.Listener, error) {
	d.log.Debug("ClusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	d.listener = lis
	return d, nil
}

// Accept waits for and returns the next connection to the listener.
// Accept is part of the net.Listener interface implementation.
func (d *ClusterGatewayImpl) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := d.listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := incoming

	d.log.Debug("ClusterGatewayImpl is accepting a new connection.")

	// Initialize yamux session for bidirectional gRPC calls
	// At gateway side, we first wait an incoming replacement connection, then create a reverse provisioner connection to the host scheduler.
	cliSession, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		d.log.Error("Failed to create yamux client session: %v", err)
		return incoming, nil
	}

	// Create a new session to replace the incoming connection.
	conn, err = cliSession.Accept()
	if err != nil {
		d.log.Error("Failed to wait for the replacement of host scheduler connection: %v", err)
		return incoming, nil
	}

	// Dial to create a reversion connection with dummy dialer.
	gConn, err := grpc.Dial(":0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return cliSession.Open()
			// return conn, err
		}))
	if err != nil {
		d.log.Error("Failed to open reverse provisioner connection: %v", err)
		return conn, nil
	}

	// Create a host scheduler client and register it.
	host, err := scheduling.NewHost(uuid.NewString(), incoming.RemoteAddr().String(), scheduling.MillicpusPerHost,
		scheduling.MemoryMbPerHost, scheduling.VramPerHostGb, d.cluster, d.gatewayPrometheusManager, gConn, d.localDaemonDisconnected)

	if err != nil {
		d.log.Error("Failed to create host scheduler client: %v", err)
		return nil, err
		//if errors.Is(err, scheduling.ErrRestoreRequired) {
		//	d.log.Warn("Restoration is required for newly-connected Local Daemon %s.", host.ID)
		//	// Restore host scheduler.
		//	registered, loaded := d.cluster.GetHostManager().LoadOrStore(host.ID, host)
		//	if loaded {
		//		err := registered.Restore(host, d.localDaemonDisconnected)
		//		if err != nil {
		//			d.log.Error("Error while restoring host %v: %v", host, err)
		//			return nil, err
		//		}
		//	} else {
		//		d.log.Warn("Host scheduler requested for restoration but not found: %s", host.ID)
		//		return nil, scheduling.ErrRestorationFailed
		//	}
		//} else {
		//	d.log.Error("Failed to create host scheduler client: %v", err)
		//	return nil, err
		//}
	}

	if host == nil {
		log.Fatalf(utils.RedStyle.Render("Newly-connected host from addr=%s is nil.\n"), incoming.RemoteAddr().String())
	}

	d.log.Info("Incoming host scheduler %s (node = %s) connected", host.ID, host.NodeName)

	d.cluster.NewHostAddedOrConnected(host)

	go d.notifyDashboardOfInfo("Local Daemon Connected", fmt.Sprintf("Local Daemon %s on node %s has connected to the internalCluster Gateway.", host.ID, host.NodeName))

	return conn, nil
}

// Close are compatible with ClusterGatewayImpl.Close().

// Addr returns the listener's network address.
// Addr is part of the net.Listener implementation.
func (d *ClusterGatewayImpl) Addr() net.Addr {
	return d.listener.Addr()
}

// ClusterScheduler returns the associated ClusterGateway.
func (d *ClusterGatewayImpl) ClusterScheduler() scheduling.ClusterScheduler {
	return d.cluster.ClusterScheduler()
}

func (d *ClusterGatewayImpl) SetID(_ context.Context, _ *proto.HostId) (*proto.HostId, error) {
	return nil, ErrNotImplemented
}

// issuePrepareMigrateRequest issues a 'prepare-to-migrate' request to a specific replica of a specific kernel.
// This will prompt the kernel to shut down its etcd process (but not remove itself from the cluster)
// before writing the contents of its data directory to HDFS.
//
// Returns the path to the data directory in HDFS.
func (d *ClusterGatewayImpl) issuePrepareMigrateRequest(kernelId string, nodeId int32) string {
	d.log.Info("Issuing 'prepare-to-migrate' request to replica %d of kernel %s now.", nodeId, kernelId)

	kernelClient, ok := d.kernels.Load(kernelId)
	if !ok {
		panic(fmt.Sprintf("Could not find distributed kernel client with ID %s.", kernelId))
	}

	targetReplica, err := kernelClient.GetReplicaByID(nodeId)
	if err != nil {
		d.log.Error("Could not find replica with ID %d for kernel %s: %v", nodeId, kernelId, err)
		panic(err)
	}

	host := targetReplica.Context().Value(client.CtxKernelHost).(*scheduling.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	replicaInfo := &proto.ReplicaInfo{
		ReplicaId: nodeId,
		KernelId:  kernelId,
	}

	d.log.Info("Calling PrepareToMigrate RPC targeting replica %d of kernel %s now.", nodeId, kernelId)
	// Issue the 'prepare-to-migrate' request. We panic if there was an error.
	resp, err := host.PrepareToMigrate(context.TODO(), replicaInfo)
	if err != nil {
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster because: %v", nodeId, kernelId, err))
	}

	var dataDirectory = resp.DataDir
	d.log.Debug("Successfully issued 'prepare-to-migrate' request to replica %d of kernel %s. Data directory: \"%s\"", nodeId, kernelId, dataDirectory)

	time.Sleep(time.Second * 5)

	return dataDirectory
}

// Issue an 'update-replica' request to a replica of a specific kernel, informing that replica and its peers
// that the replica with ID = `nodeId` has a new peer address, namely `newAddress`.
func (d *ClusterGatewayImpl) issueUpdateReplicaRequest(kernelId string, nodeId int32, newAddress string) {
	d.log.Info("Issuing 'update-replica' request to kernel %s for replica %d, newAddr = %s.", kernelId, nodeId, newAddress)

	kernelClient, ok := d.kernels.Load(kernelId)
	if !ok {
		panic(fmt.Sprintf("Could not find distributed kernel client with ID %s.", kernelId))
	}

	targetReplica := kernelClient.GetReadyReplica()
	if targetReplica == nil {
		panic(fmt.Sprintf("Could not find any ready replicas for kernel %s.", kernelId))
	}

	if !targetReplica.IsReady() {
		panic(fmt.Sprintf("Selected non-ready replica %d of kernel %s to be target of 'update-replica' request...", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	if targetReplica.ReplicaID() == nodeId { // This shouldn't happen, but it appears to have happened once already?
		panic(fmt.Sprintf("Cannot issue 'Update Replica' request to replica %d, as it is in the process of registering...", nodeId))
	}

	host := targetReplica.Context().Value(client.CtxKernelHost).(*scheduling.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	d.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s. Sending request to Local Daemon of replica %d.", nodeId, kernelId, targetReplica.ReplicaID())
	replicaInfo := &proto.ReplicaInfoWithAddr{
		Id:       nodeId,
		KernelId: kernelId,
		Hostname: fmt.Sprintf("%s:%d", newAddress, d.smrPort),
	}

	// Issue the 'update-replica' request. We panic if there was an error.
	if _, err := host.UpdateReplicaAddr(context.TODO(), replicaInfo); err != nil {
		d.log.Debug("Failed to add replica %d of kernel %s to SMR cluster because: %v", nodeId, kernelId, err)
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	d.log.Debug("Successfully updated peer address of replica %d of kernel %s to %s.", nodeId, kernelId, newAddress)
	time.Sleep(time.Second * 5)
}

func (d *ClusterGatewayImpl) SmrReady(_ context.Context, smrReadyNotification *proto.SmrReadyNotification) (*proto.Void, error) {
	kernelId := smrReadyNotification.KernelId

	// First, check if this notification is from a replica of a kernel that is starting up for the very first time.
	// If so, we'll send a notification in the associated channel, and then we'll return.
	kernelStartingChan, ok := d.kernelsStarting.Load(smrReadyNotification.KernelId)
	if ok {
		d.log.Debug("Received 'SMR-READY' notification for newly-starting kernel %s.", smrReadyNotification.KernelId)
		kernelStartingChan <- struct{}{}
		return proto.VOID, nil
	}

	// Check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	addReplicaOp, ok := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, smrReadyNotification.ReplicaId)
	if !ok {
		d.log.Warn("Received 'SMR-READY' notification replica %d, kernel %s; however, no add-replica operation found for specified kernel replica...",
			smrReadyNotification.ReplicaId, smrReadyNotification.KernelId)
		return proto.VOID, nil
	}

	if addReplicaOp.Completed() {
		log.Fatalf(utils.RedStyle.Render("Retrieved AddReplicaOperation \"%s\" targeting replica %d of kernel %s -- this operation has already completed.\n"),
			addReplicaOp.OperationID(), smrReadyNotification.ReplicaId, kernelId)
	}

	d.log.Debug("Received SMR-READY notification for replica %d of kernel %s [AddOperation.OperationID=%v]. "+
		"Notifying awaiting goroutine now...", smrReadyNotification.ReplicaId, kernelId, addReplicaOp.OperationID())
	addReplicaOp.ReplicaJoinedSmrChannel() <- struct{}{}

	return proto.VOID, nil
}

// func (d *ClusterGatewayImpl) SmrNodeRemoved(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
// 	kernelId := replicaInfo.KernelId
// 	d.log.Debug("Received SMR Node-Removed notification for replica %d of kernel %s.", replicaInfo.ReplicaID, kernelId)

// 	channelMapKey := fmt.Sprintf("%s-%s", kernelId, replicaInfo.ReplicaID)
// 	channel, ok := d.smrNodeRemovedNotifications.Load(channelMapKey)
// 	if !ok {
// 		panic(fmt.Sprintf("Could not find \"node-removed\" notification channel for replica %d of kernel %s.", replicaInfo.ReplicaID, kernelId))
// 	}

// 	channel <- struct{}{}

// 	return gateway.VOID, nil
// }

func (d *ClusterGatewayImpl) SmrNodeAdded(_ context.Context, replicaInfo *proto.ReplicaInfo) (*proto.Void, error) {
	kernelId := replicaInfo.KernelId
	d.log.Debug("Received SMR Node-Added notification for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId)

	// If there's no add-replica operation here, then we'll just return.
	op, opExists := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, replicaInfo.ReplicaId)

	if !opExists {
		d.log.Warn("No active add-replica operation found for replica %d, kernel %s.", replicaInfo.ReplicaId, kernelId)
		return proto.VOID, nil
	}

	if op.Completed() {
		log.Fatalf(utils.RedStyle.Render("Retrieved AddReplicaOperation %v targeting replica %d of kernel %s -- this operation has already completed.\n"),
			op.OperationID(), replicaInfo.ReplicaId, kernelId)
	}

	op.SetReplicaJoinedSMR()

	return proto.VOID, nil
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (d *ClusterGatewayImpl) kernelReconnectionFailed(kernel *client.KernelReplicaClient, msg *jupyter.JupyterMessage, reconnectionError error) { /* client *client.DistributedKernelClient,  */
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending \"%s\" message: %v", kernel.ReplicaID(), kernel.ID(), messageType, reconnectionError)
	d.log.Error(errorMessage)

	go d.notifyDashboardOfError("Connection to Kernel Lost & Reconnection Failed", errorMessage)
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
func (d *ClusterGatewayImpl) kernelRequestResubmissionFailedAfterReconnection(kernel *client.KernelReplicaClient, msg *jupyter.JupyterMessage, resubmissionError error) { /* client *client.DistributedKernelClient, */
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to forward \"'%s'\" request to replica %d of kernel %s following successful connection re-establishment because: %v", messageType, kernel.ReplicaID(), kernel.ID(), resubmissionError)
	d.log.Error(errorMessage)

	d.notifyDashboardOfError("Connection to Kernel Lost, Reconnection Succeeded, but Request Resubmission Failed", errorMessage)
}

func (d *ClusterGatewayImpl) executionFailed(c *client.DistributedKernelClient) error {
	execution := c.ActiveExecution()
	d.log.Warn("Execution %s (attempt %d) failed for kernel %s.", execution.ExecutionId, execution.AttemptId, c.ID())

	return d.failureHandler(c)
}

func (d *ClusterGatewayImpl) defaultFailureHandler(_ *client.DistributedKernelClient) error {
	d.log.Warn("There is no failure handler for the DEFAULT policy.")
	return fmt.Errorf("there is no failure handler for the DEFAULT policy; cannot handle error")
}

func (d *ClusterGatewayImpl) notifyDashboard(notificationName string, notificationMessage string, typ jupyter.NotificationType) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            notificationName,
			Message:          notificationMessage,
			NotificationType: int32(typ),
		})

		if err != nil {
			d.log.Error("Failed to send notification to internalCluster Dashboard because: %s", err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=%d) notification to internalCluster Dashboard.", notificationName, typ)
		}
	}
}

func (d *ClusterGatewayImpl) localDaemonDisconnected(localDaemonId string, nodeName string, errorName string, errorMessage string) (err error) {
	d.log.Warn("Local Daemon %s (Node %s) has disconnected. Removing from Cluster.", localDaemonId, nodeName)
	_, err = d.RemoveHost(context.TODO(), &proto.HostId{
		Id:       localDaemonId,
		NodeName: nodeName, /* Not needed */
	})

	if err != nil {
		d.log.Error("Error while removing local daemon %s (node: %s): %v", localDaemonId, nodeName, err)
	}

	go d.notifyDashboard(errorName, errorMessage, jupyter.WarningNotification)

	return err
}

// Used to issue an "info" notification to the internalCluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfInfo(notificationName string, message string) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            notificationName,
			Message:          message,
			NotificationType: int32(jupyter.InfoNotification),
		})

		if err != nil {
			d.log.Error("Failed to send \"%s\" notification to internalCluster Dashboard because: %s", notificationName, err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=INFO) notification to internalCluster Dashboard.", notificationName)
		}
	}
}

// Used to issue an "error" notification to the internalCluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfError(errorName string, errorMessage string) {
	sendStart := time.Now()
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            errorName,
			Message:          errorMessage,
			NotificationType: int32(jupyter.ErrorNotification),
		})

		if err != nil {
			d.log.Error("Failed to send \"%s\" error notification to internalCluster Dashboard because: %s", errorName, err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=ERROR) notification to internalCluster Dashboard in %v.", errorName, time.Since(sendStart))
		}
	}
}

// Used to issue an "error" notification to the internalCluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfWarning(warningName string, warningMessage string) {
	sendStart := time.Now()
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            warningName,
			Message:          warningMessage,
			NotificationType: int32(jupyter.WarningNotification),
		})

		if err != nil {
			d.log.Error("Failed to send \"%s\" warning notification to internalCluster Dashboard because: %s", warningName, err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=WARNING) notification to internalCluster Dashboard in %v.", warningName, time.Since(sendStart))
		}
	}
}

// staticSchedulingFailureHandler is a callback to be invoked when all replicas of a
// kernel propose 'YIELD' while static scheduling is set as the configured scheduling policy.
func (d *ClusterGatewayImpl) staticSchedulingFailureHandler(c *client.DistributedKernelClient) error {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplica := rand.Intn(c.Size()) + 1
	d.log.Debug(utils.LightBlueStyle.Render("Static Failure Handler: migrating replica %d of kernel %s now."),
		targetReplica, c.ID())

	// Notify the cluster dashboard that we're performing a migration.
	go d.notifyDashboardOfWarning(fmt.Sprintf("All Replicas of Kernel \"%s\" Have Proposed 'YIELD'", c.ID()),
		fmt.Sprintf("All replicas of kernel %s proposed 'YIELD' during code execution.", c.ID()))

	// TODO: There could be race conditions here with how we are creating and linking and assigning the ...
	// TODO: ... ActiveExecution structs here. That is, if we receive additional "execute_request" messages during ...
	// TODO: ... this process, then things could get messed-up. We need to put a big lock around this or something ...
	// TODO: ... Like, a lock around/in the DistributedKernelClient, specifically.

	activeExecution := c.ActiveExecution()
	if activeExecution == nil {
		d.log.Error("Could not find active execution for kernel %s after static scheduling failure.", c.ID())
		go d.notifyDashboardOfError("ErrActiveExecutionNotFound", "active execution for specified kernel could not be found")
		return ErrActiveExecutionNotFound
	}

	msg := activeExecution.Msg()

	// TODO(Ben): Pre-reserve resources on the host that we're migrating the replica to.
	// For now, we'll just let the standard scheduling logic handle things, which will prioritize the least-loaded host.
	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     c.ID(),
			ReplicaId:    int32(targetReplica),
			PersistentId: c.PersistentID(),
		},
		TargetNodeId: nil,
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	errorChan := make(chan error, 1)

	// Start the migration operation in another thread so that we can do some stuff while we wait.
	go func() {
		resp, err := d.MigrateKernelReplica(context.TODO(), req)

		if err != nil {
			d.log.Error(utils.RedStyle.Render("Static Failure Handler: failed to migrate replica %d of kernel %s because: %s"),
				targetReplica, c.ID(), err.Error())
			errorChan <- err
		} else {
			d.log.Debug(utils.GreenStyle.Render("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s."),
				targetReplica, c.ID(), resp.Hostname)
		}

		waitGroup.Done()
	}()

	// Next, let's update the message so that we target the new replica.
	// We do this now, before calling waitGroup.Wait(), just to overlap the two tasks
	// (the kernel starting + registering and us updating the "execute_request" Jupyter
	// message for resubmission).
	//metadataFrame := msg.JupyterFrames.MetadataFrame()
	//var metadataDict map[string]interface{}
	//
	//// Don't try to unmarshal the metadata frame unless the size of the frame is non-zero.
	//if len(metadataFrame.Frame()) > 0 {
	//	// Unmarshal the frame. This way, we preserve any other metadata that was
	//	// originally included with the message when we specify the target replica.
	//	err := metadataFrame.Decode(&metadataDict)
	//	if err != nil {
	//		d.log.Error("Error unmarshalling metadata frame for 'execute_request' message: %v", err)
	//		return err
	//	} else {
	//		d.log.Debug("Unmarshalled metadata frame for 'execute_request' message: %v", metadataDict)
	//	}
	//}

	metadataDict, err := msg.DecodeMetadata()
	if err != nil {
		d.log.Warn("Failed to unmarshal metadata frame for \"execute_request\" message \"%s\" (JupyterID=\"%s\"): %v",
			msg.RequestId, msg.JupyterMessageId())

		// We'll assume the metadata frame was empty, and we'll create a new dictionary to use as the metadata frame.
		metadataDict = make(map[string]interface{})
	}

	// Specify the target replica.
	metadataDict[TargetReplicaArg] = targetReplica
	metadataDict[ForceReprocessArg] = true
	err = msg.EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		return err
	}

	signatureScheme := c.ConnectionInfo().SignatureScheme
	if signatureScheme == "" {
		d.log.Warn("Kernel %s's signature scheme is blank. Defaulting to \"%s\"", jupyter.JupyterSignatureScheme)
		signatureScheme = jupyter.JupyterSignatureScheme
	}

	// Regenerate the signature.
	if _, err := msg.JupyterFrames.Sign(signatureScheme, []byte(c.ConnectionInfo().Key)); err != nil {
		// Ignore the error; just log it.
		d.log.Warn("Failed to sign frames because %v", err)
	}

	// Ensure that the frames are now correct.
	if err := msg.JupyterFrames.Verify(signatureScheme, []byte(c.ConnectionInfo().Key)); err != nil {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v': %v",
			signatureScheme, c.ConnectionInfo().Key, err)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", msg)
		return ErrFailedToVerifyMessage
	}

	// Now, we wait for the migration operation to proceed.
	waitGroup.Wait()
	select {
	case err := <-errorChan:
		{
			// If there was an error during execution, then we'll return that error rather than proceed.
			return err
		}
	default:
		{
			// Do nothing. The migration operation completed successfully.
		}
	}

	d.log.Debug(utils.LightBlueStyle.Render("Resubmitting 'execute_request' message targeting kernel %s now."), c.ID())
	err = d.ShellHandler(c, msg)
	if err != nil {
		d.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go d.notifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) dynamicV3FailureHandler(_ *client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

func (d *ClusterGatewayImpl) dynamicV4FailureHandler(_ *client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// DockerComposeMode returns true if we're running in Docker via "docker compose".
// If we're running via "docker swarm", then DockerComposeMode returns false.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, DockerComposeMode also returns false.
func (d *ClusterGatewayImpl) DockerComposeMode() bool {
	return d.deploymentMode == types.DockerComposeMode
}

// DockerSwarmMode returns true if we're running in Docker via "docker swarm".
// If we're running via "docker compose", then DockerSwarmMode returns false.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, DockerSwarmMode also returns false.
func (d *ClusterGatewayImpl) DockerSwarmMode() bool {
	return d.deploymentMode == types.DockerComposeMode
}

// DockerMode returns true if we're running in either "docker swarm" or "docker compose".
// That is, DockerMode turns true if and only if one of DockerSwarmMode or DockerComposeMode return true.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *ClusterGatewayImpl) DockerMode() bool {
	return d.DockerComposeMode() || d.DockerComposeMode()
}

// KubernetesMode returns true if we're running in Kubernetes.
func (d *ClusterGatewayImpl) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// startNewKernel is called by StartKernel when creating a brand-new kernel, rather than restarting an existing kernel.
func (d *ClusterGatewayImpl) initNewKernel(in *proto.KernelSpec) (*client.DistributedKernelClient, error) {
	d.log.Debug("Did not find existing DistributedKernelClient with KernelID=\"%s\". Creating new DistributedKernelClient now.", in.Id)

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		panic(err)
	}

	// Initialize kernel with new context.
	kernel := client.NewDistributedKernel(context.Background(), in, d.ClusterOptions.NumReplicas, d.id,
		d.connectionOptions, listenPorts[0], listenPorts[1], uuid.NewString(), d.DebugMode, d.executionFailed,
		d.executionLatencyCallback, d.gatewayPrometheusManager)

	d.log.Debug("Initializing Shell Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)
	_, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
	if err != nil {
		if closeErr := kernel.Close(); closeErr != nil {
			d.log.Error("Error while closing kernel %s: %v.", kernel.ID(), closeErr)
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}
	d.log.Debug("Initializing IO Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)

	if _, err = kernel.InitializeIOForwarder(); err != nil {
		if closeErr := kernel.Close(); closeErr != nil {
			d.log.Error("Error while closing kernel %s: %v.", kernel.ID(), closeErr)
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}

	d.log.Debug("Allocating the following \"listen\" ports to kernel %s: %v", kernel.ID(), listenPorts)

	// Create a new Session for scheduling purposes.
	resourceUtil := scheduling.NewEmptyResourceUtilization().
		WithCpuUtilization(in.ResourceSpec.CPU()).
		WithMemoryUsageMb(in.ResourceSpec.MemoryMB()).
		WithNGpuUtilizationValues(in.ResourceSpec.Gpu, 0)
	session := scheduling.NewUserSession(context.Background(), kernel.ID(), in, resourceUtil, d.cluster, d.ClusterOptions)
	d.cluster.Sessions().Store(kernel.ID(), session)

	// Assign the Session to the DistributedKernelClient.
	kernel.SetSession(session)

	return kernel, nil
}

// StartKernel launches a new kernel.
func (d *ClusterGatewayImpl) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	startTime := time.Now()
	d.log.Info("ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%v].", in.Id, in.Session, in.ResourceSpec)
	d.log.Debug("KernelSpec: %v", in)

	var (
		kernel *client.DistributedKernelClient
		ok     bool
		err    error
	)

	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok = d.kernels.Load(in.Id)
	if !ok {
		kernel, err = d.initNewKernel(in)
		if err != nil {
			d.log.Error("Failed to create new kernel %s because: %v", in.Id, err)
			return nil, err
		}
	} else {
		d.log.Info("Restarting %v...", kernel)
		kernel.BindSession(in.Session)
	}

	// Record that this kernel is starting.
	kernelStartedChan := make(chan struct{})
	d.kernelsStarting.Store(in.Id, kernelStartedChan)
	created := newRegistrationWaitGroups(d.ClusterOptions.NumReplicas)

	d.kernelIdToKernel.Store(in.Id, kernel)
	d.kernels.Store(in.Id, kernel)
	d.kernelSpecs.Store(in.Id, in)
	d.waitGroups.Store(in.Id, created)

	d.log.Debug("Created and stored new DistributedKernel %s.", in.Id)

	err = d.cluster.ClusterScheduler().DeployNewKernel(ctx, in, []*scheduling.Host{ /* No blacklisted hosts */ })
	if err != nil {
		d.log.Error("Error while deploying infrastructure for new kernel %s's: %v", in.Id, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Wait for all replicas to be created.
	// Note that creation just means that the Container/Pod was created.
	// It does not mean that the Container/Pod has entered the active/running state.
	d.log.Debug("Waiting for replicas of new kernel %s to register.", in.Id)
	created.Wait()
	d.log.Debug("All %d replicas of new kernel %s have been created and registered with their local daemons. Waiting for replicas to join their SMR cluster now.", d.ClusterOptions.NumReplicas, in.Id)

	// Wait until all replicas have started.
	for i := 0; i < d.ClusterOptions.NumReplicas; i++ {
		<-kernelStartedChan // Wait for all replicas to join their SMR cluster.
	}

	// Clean up.
	d.kernelsStarting.Delete(in.Id)
	d.log.Debug("All %d replicas of new kernel %s have registered and joined their SMR cluster.", d.ClusterOptions.NumReplicas, in.Id)

	// Sanity check.
	if kernel.Size() == 0 {
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// Map all the sessions (probably just one?) to the kernel client.
	for _, sess := range kernel.Sessions() {
		d.log.Debug("Storing kernel %v under session ID %s.", kernel, sess)
		d.kernels.Store(sess, kernel)
	}

	// TODO: Do we need to implement the below?
	// Now that all replicas have started, we need to remove labels from all the other Kubernetes nodes.

	// Option #1:
	// - When scheduling a new kernel, add labels to ALL the Kubernetes nodes and allow system to schedule kernels whenever.
	// - Once the kernels have been created and registered, remove labels from all the nodes except the nodes that the kernels are presently running on.

	// Option #2:
	// - When creating a dynamic replica for a new kernel on a particular node, identify the replica that is to be stopped.
	// - Add labels to nodes hosting the other two replicas.
	// - Add a label to the target node for the new dynamic replica.
	// - Update the CloneSet to have a nodeAffinity constraint for matching labels.
	// - Once the new replica has been created, remove the scheduling constraint (but not the node labels).
	// - Whenever we migrate a replica, we also need to update node labels (if they exist).
	//		- This involves removing the label from the old node and adding the label to the new node, for the migrated replica.

	info := &proto.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(kernel.Socket(jupyter.ShellMessage).Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(kernel.Socket(jupyter.IOMessage).Port),
		IosubPort:       int32(kernel.Socket(jupyter.IOMessage).Port),
		SignatureScheme: kernel.KernelSpec().SignatureScheme,
		Key:             kernel.KernelSpec().Key,
	}
	d.log.Info("Kernel(%s) started after %v: %v", kernel.ID(), time.Since(startTime), info)

	session, ok := d.cluster.Sessions().Load(kernel.ID())
	if ok {
		p := session.SessionStarted()
		err = p.Error()
		if err != nil {
			d.notifyDashboardOfError(fmt.Sprintf("Error Starting Session \"%s\"", kernel.ID()), err.Error())
			panic(err)
		}
	} else {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session associated with kernel \"%s\", even though that kernel just started running successfully...", kernel.ID())
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Session Not Found", errorMessage)
		panic(errorMessage)
	}

	// Tell the Dashboard that the kernel has successfully started running.
	go d.notifyDashboard("Kernel Started", fmt.Sprintf("Kernel %s has started running. Launch took approximately %v.", kernel.ID(), time.Since(startTime)), jupyter.SuccessNotification)

	numActiveKernels := d.numActiveKernels.Add(1)
	d.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
		With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
		Set(float64(numActiveKernels))
	d.gatewayPrometheusManager.TotalNumKernelsCounterVec.
		With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Inc()
	d.gatewayPrometheusManager.KernelCreationLatencyHistogram.Observe(float64(time.Since(startTime).Milliseconds()))

	return info, nil
}

// Handle a registration notification from a new kernel replica that was created during an add-replica/migration operation.
//
// IMPORTANT: This must be called with the main mutex held. Otherwise, there are race conditions with the
// addReplicaNewPodNotifications field.
//
// IMPORTANT: This will release the main mutex before returning.
func (d *ClusterGatewayImpl) handleAddedReplicaRegistration(in *proto.KernelRegistrationNotification, kernel *client.DistributedKernelClient, waitGroup *registrationWaitGroups) (*proto.KernelRegistrationNotificationResponse, error) {
	key := fmt.Sprintf("%s-%d", in.KernelId, in.ReplicaId)
	addReplicaOp, ok := d.addReplicaOperationsByKernelReplicaId.Load(key)

	// If we cannot find the migration operation, then we have an unlikely race here.
	// Basically, the new replica Pod was created, started running, and contacted its Local Daemon, which then contacted us,
	// all before we received and processed the associated pod-created notification from kubernetes.
	//
	// So, we have to wait to receive the notification so we can get the migration operation and get the correct SMR node ID for the Pod.
	//
	// This race would be simplified if we added the constraint that there may be only one active migration per kernel at any given time,
	// but I've not yet enforced this.
	if !ok {
		channel := make(chan *AddReplicaOperation, 1)
		d.addReplicaNewPodNotifications.Store(key, channel)

		d.Unlock()
		d.log.Debug("Waiting to receive AddReplicaNotification on NewPodNotification channel. Key: %s.", key)
		// Just need to provide a mechanism to wait until we receive the pod-created notification, and get the migration operation that way.
		addReplicaOp = <-channel
		d.log.Debug("Received AddReplicaNotification on NewPodNotification channel. Key: %s.", key)
		d.Lock()

		// Clean up mapping. Don't need it anymore.
		d.addReplicaNewPodNotifications.Delete(key)
	}

	host, loaded := d.cluster.GetHost(in.HostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", in.HostId)) // TODO(Ben): Handle gracefully.
	}

	// The replica spec that was specifically prepared for the new replica during the initiation of the migration operation.
	replicaSpec := addReplicaOp.KernelSpec()
	addReplicaOp.SetReplicaHostname(in.KernelIp)

	if in.NodeName == "" {
		if !d.DockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		in.NodeName = host.ID
	}

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec, jupyter.ConnectionInfoFromKernelConnectionInfo(in.ConnectionInfo),
		d.id, false, d.numResendAttempts, -1, -1, in.PodName, in.NodeName,
		nil, nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), in.HostId,
		host, metrics.ClusterGateway, true, true, d.DebugMode, d.gatewayPrometheusManager,
		d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection)

	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	session, ok := d.cluster.Sessions().Load(in.KernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", in.SessionId)
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := scheduling.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the KernelReplicaClient.
	replica.SetContainer(container)

	// Add the Container to the Host.
	d.log.Debug("Adding scheduling.Container for replica %d of kernel %s onto Host %s",
		replicaSpec.ReplicaId, addReplicaOp.KernelId(), host.ID)
	if err = host.ContainerScheduled(container); err != nil {
		d.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		d.notifyDashboardOfError("Failed to Place Container onto Host", err.Error())
		panic(err)
	}

	// Register the Container with the Session.
	d.log.Debug("Registering/adding scheduling.Container for replica %d of kernel %s with the associated scheduling.Session",
		replicaSpec.ReplicaId, addReplicaOp.KernelId())
	if err = session.AddReplica(container); err != nil {
		d.log.Error("Error while registering container %v with session %v: %v", container, session, err)
		d.notifyDashboardOfError("Failed to Register Container with Session", err.Error())
		panic(err)
	}

	// Attempt to load the Docker container ID metadata, which will be attached to the metadata of the add replica
	// operation if we're running in Docker mode. If we're not in Docker mode, then this will do nothing.
	if dockerContainerId, loaded := addReplicaOp.GetMetadata(domain.DockerContainerFullId); loaded {
		container.SetDockerContainerID(dockerContainerId.(string))
	}

	d.log.Debug("Adding replica for kernel %s, replica %d on host %s. Resource spec: %v", addReplicaOp.KernelId(), replicaSpec.ReplicaId, host.ID, replicaSpec.Kernel.ResourceSpec)
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("KernelReplicaClient::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// d.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaID, in.KernelId, waitGroup.NumReplicas())

	// Store the new replica in the list of replicas for the kernel (at the correct position, based on the SMR node ID).
	// Then, return the list of replicas so that we can pass it to the new replica.
	// updatedReplicas := waitGroup.UpdateAndGetReplicasAfterMigration(migrationOperation.OriginalSMRNodeID()-1, in.KernelIp)
	updatedReplicas := waitGroup.AddReplica(replicaSpec.ReplicaId, in.KernelIp)

	persistentId := addReplicaOp.PersistentID()
	// dataDirectory := addReplicaOp.DataDirectory()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                     replicaSpec.ReplicaId,
		Replicas:               updatedReplicas,
		PersistentId:           &persistentId,
		ShouldReadDataFromHdfs: true,
		ResourceSpec:           replicaSpec.Kernel.ResourceSpec,
		// DataDirectory: &dataDirectory,
		SmrPort: int32(d.smrPort),
	}

	d.Unlock()

	d.log.Debug("Sending notification that replica %d of kernel \"%s\" has registered during AddOperation \"%s\".",
		replicaSpec.ReplicaId, in.KernelId, addReplicaOp.OperationID())

	addReplicaOp.SetReplicaRegistered() // This just sets a flag to true in the migration operation object.

	d.log.Debug("About to issue 'update replica' request for replica %d of kernel %s. Client ready: %v", replicaSpec.ReplicaId, in.KernelId, replica.IsReady())

	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	// Issue the AddNode request now, so that the node can join when it starts up.
	// d.issueAddNodeRequest(in.KernelId, replicaSpec.ReplicaID, in.KernelIp)

	d.log.Debug("Done handling registration of added replica %d of kernel %s.", replicaSpec.ReplicaId, in.KernelId)

	return response, nil
}

// AddReplicaDynamic dynamically creates and adds a replica for a particular kernel.
//
// Under the static scheduling policy, we dynamically create a new replica to execute
// code when the existing replicas are unable to do so due to resource contention.
//
// Possible errors:
// - ErrKernelSpecNotFound: If there is no mapping from the provided kernel's ID to a kernel spec.
// - ErrResourceSpecNotFound: If there is no resource spec included within the kernel spec of the specified kernel.
func (d *ClusterGatewayImpl) AddReplicaDynamic(_ context.Context, in *proto.KernelId) error {
	// Steps:
	// - Identify a target node with sufficient resources to serve an execution request for the associated kernel.
	// - Add a label to that node to ensure the new replica is scheduled onto that node.
	// - Increase the size of the associated kernel's CloneSet.

	kernelSpec, ok := d.kernelSpecs.Load(in.Id)
	if !ok {
		d.log.Error("Could not load kernel spec associated with kernel %s", in.Id)
		return ErrKernelSpecNotFound
	}

	resourceSpec := kernelSpec.GetResourceSpec()
	if resourceSpec == nil {
		d.log.Error("Kernel %s does not have a resource spec included with its kernel spec.", in.Id)
		return ErrResourceSpecNotFound
	}

	// Identify a target node with sufficient resources to serve an execution request for the associated kernel.

	// Add a label to that node to ensure the new replica is scheduled onto that node.

	// Increase the size of the associated kernel's CloneSet.

	return nil
}

func (d *ClusterGatewayImpl) NotifyKernelRegistered(_ context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	d.log.Info("Received kernel registration notification.")

	connectionInfo := in.ConnectionInfo
	sessionId := in.SessionId
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	kernelPodName := in.PodName
	nodeName := in.NodeName
	replicaId := in.ReplicaId

	d.log.Info("Connection info: %v", connectionInfo)
	d.log.Info("Session ID: %v", sessionId)
	d.log.Info("Kernel ID: %v", kernelId)
	d.log.Info("Replica ID: %v", replicaId)
	d.log.Info("Kernel IP: %v", kernelIp)
	d.log.Info("Pod name: %v", kernelPodName)
	d.log.Info("Host ID: %v", hostId)
	d.log.Info("Node ID: %v", nodeName)

	d.Lock()

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing kernel with ID %s", kernelId))
	}

	kernelSpec, loaded := d.kernelSpecs.Load(kernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing kernel spec for kernel with ID %s", kernelId))
	}

	waitGroup, loaded := d.waitGroups.Load(kernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing WaitGroup associated with kernel with ID %s", kernelId))
	}

	host, loaded := d.cluster.GetHost(hostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", hostId)) // TODO(Ben): Handle gracefully.
	}

	if kernel.NumActiveMigrationOperations() >= 1 {
		d.log.Debug("There is/are %d active add-replica operation(s) targeting kernel %s. Assuming currently-registering replica is for an add-replica operation.", kernel.NumActiveMigrationOperations(), kernel.ID())
		// Must be holding the main mutex before calling handleAddedReplicaRegistration.
		// It will release the lock.
		result, err := d.handleAddedReplicaRegistration(in, kernel, waitGroup)
		return result, err
	} else {
		d.log.Debug("There are 0 active add-replica operations targeting kernel %s.", kernel.ID())
	}

	// If this is the first replica we're registering, then its ID should be 1.
	// The size will be 0, so we'll assign it a replica ID of 0 + 1 = 1.
	if replicaId == -1 {
		replicaId = int32(kernel.Size()) + 1
		d.log.Debug("Kernel does not already have a replica ID assigned to it. Assigning ID: %d.", replicaId)
	}

	// We're registering a new replica, so the number of replicas is based on the cluster configuration.
	replicaSpec := &proto.KernelReplicaSpec{
		Kernel:      kernelSpec,
		ReplicaId:   replicaId,
		NumReplicas: int32(d.ClusterOptions.NumReplicas),
	}

	if nodeName == "" || nodeName == types.DockerNode {
		if !d.DockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		nodeName = host.ID
	}

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec, jupyter.ConnectionInfoFromKernelConnectionInfo(connectionInfo), d.id,
		false, d.numResendAttempts, -1, -1, kernelPodName, nodeName, nil,
		nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), hostId, host, metrics.ClusterGateway,
		true, true, d.DebugMode, d.gatewayPrometheusManager, d.kernelReconnectionFailed,
		d.kernelRequestResubmissionFailedAfterReconnection)

	session, ok := d.cluster.Sessions().Load(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", kernelId)
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := scheduling.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the KernelReplicaClient.
	replica.SetContainer(container)

	// Add the Container to the Host.
	if err := host.ContainerScheduled(container); err != nil {
		d.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		d.notifyDashboardOfError("Failed to Place Container onto Host", err.Error())
		panic(err)
	}

	// Register the Container with the Session.
	if err := session.AddReplica(container); err != nil {
		d.log.Error("Error while registering container %v with session %v: %v", container, session, err)
		d.notifyDashboardOfError("Failed to Register Container with Session", err.Error())
		panic(err)
	}

	d.log.Debug("Validating new KernelReplicaClient for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("KernelReplicaClient::Validate call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	d.log.Debug("Adding Replica for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("KernelReplicaClient::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()
	d.Unlock()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register()
	d.log.Debug("Done registering KernelReplicaClient for kernel %s, replica %d on host %s. Resource spec: %v",
		kernelId, replicaId, hostId, kernelSpec.ResourceSpec)
	d.log.Debug("WaitGroup for Kernel \"%s\": %s", kernelId, waitGroup.String())
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                     replicaId,
		Replicas:               waitGroup.GetReplicas(),
		PersistentId:           &persistentId,
		ResourceSpec:           kernelSpec.ResourceSpec,
		SmrPort:                int32(d.smrPort), // The kernel should already have this info, but we'll send it anyway.
		ShouldReadDataFromHdfs: false,
		// DataDirectory: nil,
	}

	d.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v", kernelId, replicaId, response)

	waitGroup.Notify()
	return response, nil
}

// QueryMessage is used to query whether a given ZMQ message has been seen by any of the Cluster components
// and what the status of that message is (i.e., sent, response received, etc.)
func (d *ClusterGatewayImpl) QueryMessage(_ context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error) {
	if !d.DebugMode {
		d.log.Warn("Received QueryMessage request, but we're not running in DebugMode.")
		return nil, status.Errorf(codes.Internal, "DebugMode is not enabled; cannot query messages")
	}

	if in.MessageId == "" {
		d.log.Warn("QueryMessage request did not contain a Message ID.")
		return nil, status.Errorf(codes.InvalidArgument,
			"you must specify a Jupyter message ID when querying for the status of a particular message")
	}

	if in.MessageId == "*" {
		if d.RequestLog.Len() == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "RequestLog is empty")
		}

		d.log.Debug("Received message query for all messages in log. Will be returning %d message(s).",
			d.RequestLog.EntriesByJupyterMsgId.Len())

		requestTraces := make([]*proto.RequestTrace, 0, d.RequestLog.EntriesByJupyterMsgId.Len())

		d.RequestLog.Lock()
		d.RequestLog.EntriesByJupyterMsgId.Range(func(msgId string, wrapper *metrics.RequestLogEntryWrapper) (contd bool) {
			wrapper.EntriesByNodeId.Range(func(i int32, entry *metrics.RequestLogEntry) (contd bool) {
				requestTraces = append(requestTraces, entry.RequestTrace)
				return true
			})
			return true
		})
		d.RequestLog.Unlock()

		// Build the response.
		resp := &proto.QueryMessageResponse{
			RequestTraces: requestTraces,
		}

		return resp, nil
	}

	wrapper, loaded := d.RequestLog.EntriesByJupyterMsgId.Load(in.MessageId)
	if !loaded {
		d.log.Warn("No request log entry found for request with Jupyter message ID \"%s\"", in.MessageId)
		d.log.Warn("#Entries in RequestLog (by Jupyter message ID): %d", d.RequestLog.EntriesByJupyterMsgId.Len())
		return nil, status.Errorf(codes.InvalidArgument, "no request entry found in request log for entry with ID=\"%s\"", in.MessageId)
	}

	// Make sure the Jupyter message types match (if the caller specified a Jupyter message type).
	if in.MessageType != "" && in.MessageType != wrapper.JupyterMessageType {
		d.log.Warn("Found request log entry for request with Jupyter message ID \"%s\", but request had type \"%s\" whereas the request type is \"%s\"",
			in.MessageId, wrapper.JupyterMessageType, in.MessageType)
		return nil, status.Errorf(codes.InvalidArgument,
			"found request log entry for request with Jupyter message ID \"%s\", but request had type \"%s\" whereas the request type is \"%s\"",
			in.MessageId, wrapper.JupyterMessageType, in.MessageType)
	}

	// Make sure the Kernel IDs types match (if the caller specified a Jupyter kernel ID).
	if in.KernelId != "" && in.KernelId != wrapper.KernelId {
		d.log.Warn("Found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
		return nil, status.Errorf(codes.InvalidArgument,
			"found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
	}

	//Make sure that the RequestTrace is non-nil. If it is, then we'll panic.
	//requestTrace := entry.RequestTrace
	//if requestTrace == nil {
	//	errorMessage := fmt.Sprintf("RequestTrace field of RequestLogEntry for request \"%s\" of type \"%s\" targeting kernel \"%s\" is nil.",
	//		in.MessageId, entry.JupyterMessageType, entry.KernelId)
	//	d.notifyDashboardOfError("RequestLogEntry's RequestTrace Field is Nil", errorMessage)
	//	panic(utils.RedStyle.Render(errorMessage))
	//}

	d.log.Debug("Received QueryMessage request for Jupyter %s \"%s\" request with JupyterID=\"%s\" targeting kernel \"%s\"",
		wrapper.MessageType.String(), wrapper.JupyterMessageType, wrapper.JupyterMessageId, wrapper.KernelId)

	// Build the response.
	requestTraces := make([]*proto.RequestTrace, 0, wrapper.EntriesByNodeId.Len())
	wrapper.EntriesByNodeId.Range(func(i int32, entry *metrics.RequestLogEntry) (contd bool) {
		requestTraces = append(requestTraces, entry.RequestTrace)
		return true
	})

	resp := &proto.QueryMessageResponse{
		RequestTraces: requestTraces,
	}

	return resp, nil
}

// RegisterDashboard is called by the internalCluster Dashboard backend server to both verify that a connection has been
// established and to obtain any important configuration information, such as the deployment mode (i.e., Docker or
// Kubernetes), from the internalCluster Gateway.
func (d *ClusterGatewayImpl) RegisterDashboard(_ context.Context, _ *proto.Void) (*proto.DashboardRegistrationResponse, error) {
	resp := &proto.DashboardRegistrationResponse{
		DeploymentMode:   string(d.deploymentMode),
		SchedulingPolicy: string(d.schedulingPolicy),
		NumReplicas:      int32(d.ClusterOptions.NumReplicas),
	}

	return resp, nil
}

func (d *ClusterGatewayImpl) StartKernelReplica(_ context.Context, _ *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	d.log.Debug("StartKernelReplica has been instructed to StartKernel. This is actually not supported/implemented.")

	return nil, ErrNotSupported
}

// GetKernelStatus returns the status of a kernel.
func (d *ClusterGatewayImpl) GetKernelStatus(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		// d.log.Debug("Returning kernel status directly: %v", jupyter.KernelStatusExited)
		d.log.Warn("Attempted to retrieve kernel status for unknown kernel \"%s\". Returning KernelStatusExited (%v).", in.Id, jupyter.KernelStatusExited)
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	kernelStatus := kernel.Status()

	return d.statusErrorf(kernelStatus, nil)
}

// KillKernel kills a kernel.
func (d *ClusterGatewayImpl) KillKernel(_ context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	d.log.Debug("KillKernel RPC called for kernel %s.", in.Id)

	// Call the impl rather than the RPC stub.
	return d.stopKernelImpl(in)
}

// GetId returns the ID of the ClusterGatewayImpl.
func (d *ClusterGatewayImpl) GetId() string {
	return d.id
}

func (d *ClusterGatewayImpl) stopKernelImpl(in *proto.KernelId) (ret *proto.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find Kernel %s; cannot stop kernel.", in.GetId())
		return nil, ErrKernelNotFound
	}

	restart := false
	if in.Restart != nil {
		restart = *in.Restart
	}
	d.log.Info("Stopping %v, will restart %v", kernel, restart)
	ret = proto.VOID

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err = d.errorf(kernel.Shutdown(d.cluster.Placer().Reclaim, restart))
		if err != nil {
			d.log.Warn("Failed to close kernel: %s", err.Error())
			return
		}

		d.log.Debug("Clearing session records for kernel %s.", kernel)
		// Clear session records.
		for _, sess := range kernel.Sessions() {
			d.kernels.Delete(sess)
		}
		d.log.Debug("Cleaned sessions %v after replicas stopped.", kernel.Sessions())
		if restart {
			// Keep kernel records if restart is requested.
			kernel.ClearSessions()
		} else {
			d.kernels.Delete(kernel.ID())
			d.kernelIdToKernel.Delete(kernel.ID())

			// Return the ports allocated to the kernel.
			listenPorts := []int{kernel.ShellListenPort(), kernel.IOPubListenPort()}
			if err := d.availablePorts.ReturnPorts(listenPorts); err != nil {
				d.log.Error("Error returning ports %v: %v", listenPorts, err)
			}

			d.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}

		wg.Done()
	}()

	wg.Wait()
	d.log.Debug("Finished deleting kernel %s.", kernel.ID())

	if !restart && d.KubernetesMode() /* Only delete CloneSet if we're in Kubernetes mode */ {
		d.log.Debug("Deleting CloneSet of deleted kernel %s now.", kernel.ID())

		// Delete the CloneSet.
		err := d.kubeClient.DeleteCloneset(kernel.ID())

		if err != nil {
			d.log.Error("Error encountered while deleting k8s CloneSet for kernel %s: %v", kernel.ID(), err)
		} else {
			d.log.Debug("Successfully deleted k8s CloneSet of deleted kernel %s.", kernel.ID())
		}
	}

	if err == nil {
		go d.notifyDashboard(
			"Kernel Stopped", fmt.Sprintf("Kernel %s has been terminated successfully.",
				kernel.ID()), jupyter.SuccessNotification)
		d.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Sub(1)
	} else {
		go d.notifyDashboardOfError("Failed to Terminate Kernel",
			fmt.Sprintf("An error was encountered while trying to terminate kernel %s: %v.", kernel.ID(), err))
	}

	return
}

// StopKernel stops a kernel.
func (d *ClusterGatewayImpl) StopKernel(_ context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	d.log.Debug("StopKernel RPC called for kernel %s.", in.Id)

	return d.stopKernelImpl(in)
}

// WaitKernel waits for a kernel to exit.
func (d *ClusterGatewayImpl) WaitKernel(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.WaitClosed(), nil)
}

func (d *ClusterGatewayImpl) Notify(_ context.Context, in *proto.Notification) (*proto.Void, error) {
	d.log.Debug(utils.NotificationStyles[in.NotificationType].Render("Received %s notification \"%s\": %s"), NotificationTypeNames[in.NotificationType], in.Title, in.Message)
	go d.notifyDashboard(in.Title, in.Message, jupyter.NotificationType(in.NotificationType))
	return proto.VOID, nil
}

func (d *ClusterGatewayImpl) SpoofNotifications(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	go func() {
		d.notifyDashboard("Spoofed Error", "This is a made-up error message sent by the internalCluster Gateway.", jupyter.ErrorNotification)
		d.notifyDashboard("Spoofed Warning", "This is a made-up warning message sent by the internalCluster Gateway.", jupyter.WarningNotification)
		d.notifyDashboard("Spoofed Info Notification", "This is a made-up 'info' message sent by the internalCluster Gateway.", jupyter.InfoNotification)
		d.notifyDashboard("Spoofed Success Notification", "This is a made-up 'success' message sent by the internalCluster Gateway.", jupyter.SuccessNotification)
	}()

	return proto.VOID, nil
}

// ClusterGateway implementation.

// ID returns the unique ID of the provisioner.
func (d *ClusterGatewayImpl) ID(_ context.Context, _ *proto.Void) (*proto.ProvisionerId, error) {
	d.log.Debug("Returning ID for RPC. ID=%s", d.id)
	return &proto.ProvisionerId{Id: d.id}, nil
}

func (d *ClusterGatewayImpl) RemoveHost(ctx context.Context, in *proto.HostId) (*proto.Void, error) {
	// d.cluster.ReleaseSpecificHosts(ctx, []string{in.Id})
	d.cluster.RemoveHost(in.Id)
	return proto.VOID, nil
}

// GetActualGpuInfo is not implemented for the internalCluster Gateway.
// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterActualGpuInfo'.
func (d *ClusterGatewayImpl) GetActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.GpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetVirtualGpuInfo is not implemented for the internalCluster Gateway.
// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterVirtualGpuInfo'.
func (d *ClusterGatewayImpl) GetVirtualGpuInfo(_ context.Context, _ *proto.Void) (*proto.VirtualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (d *ClusterGatewayImpl) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	resp := &proto.ClusterActualGpuInfo{
		GpuInfo: make(map[string]*proto.GpuInfo),
	}

	d.cluster.RangeOverHosts(func(hostId string, host *scheduling.Host) (contd bool) {
		data, err := host.GetActualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve actual GPU info from Local Daemon %s on node %s because: %v", hostId, host.NodeName, err)
			resp.GpuInfo[host.NodeName] = nil
		} else {
			resp.GpuInfo[host.NodeName] = data
		}
		return true
	})

	return resp, nil
}

// Return the current vGPU (or "deflated GPU") resource metrics on the node.
func (d *ClusterGatewayImpl) getClusterVirtualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterVirtualGpuInfo, error) {
	resp := &proto.ClusterVirtualGpuInfo{
		GpuInfo: make(map[string]*proto.VirtualGpuInfo),
	}

	d.cluster.RangeOverHosts(func(hostId string, host *scheduling.Host) (contd bool) {
		data, err := host.GetVirtualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve virtual GPU info from Local Daemon %s on node %s because: %v", hostId, host.NodeName, err)
			resp.GpuInfo[host.NodeName] = nil
		} else {
			resp.GpuInfo[host.NodeName] = data
		}
		return true
	})

	return resp, nil
}

// setTotalVirtualGPUs adjusts the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (d *ClusterGatewayImpl) setTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	d.log.Debug("Received 'SetTotalVirtualGPUs' request targeting node %s with %d vGPU(s).", in.KubernetesNodeName, in.Value)
	var targetHost *scheduling.Host
	d.log.Debug("We currently have %d LocalDaemons connected.", d.cluster.Len())
	d.cluster.RangeOverHosts(func(hostId string, host *scheduling.Host) bool {
		if host.NodeName == in.GetKubernetesNodeName() {
			d.log.Debug("Found LocalDaemon running on target node %s.", in.KubernetesNodeName)
			targetHost = host
			return false // Stop looping.
		} else {
			d.log.Debug("LocalDaemon %s is running on different node: %s.", host.ID, host.NodeName)
		}

		return true // Continue looping.
	})

	// If we didn't find a local daemon running on a node with the specified name, then return an error.
	if targetHost == nil {
		d.log.Error("Could not find a Local Daemon running on Kubernetes node %s.", in.KubernetesNodeName)
		return nil, ErrDaemonNotFoundOnNode
	}

	d.log.Debug("Attempting to set vGPUs available on node %s to %d.", in.KubernetesNodeName, in.Value)
	resp, err := targetHost.SetTotalVirtualGPUs(ctx, in)
	if err != nil {
		d.log.Error("Failed to set vGPUs available on node %s to %d because: %v", in.KubernetesNodeName, in.Value, err)
	} else if resp.TotalVirtualGPUs != in.Value {
		d.log.Error("Expected available vGPUs on node %s to be %d; however, it is actually %d...", in.KubernetesNodeName, in.Value, resp.TotalVirtualGPUs)
	} else {
		d.log.Debug("Successfully set vGPUs available on node %s to %d.", in.KubernetesNodeName, in.Value)
	}

	return resp, err
}

// migrateReplicaRemoveFirst migrates a kernel replica from its current node to another node.
//
// If the targetNodeId parameter contains a valid node ID, then the replica is migrated to the specified node if
// possible. If not, then the migration operation will be aborted.
func (d *ClusterGatewayImpl) migrateReplicaRemoveFirst(in *proto.ReplicaInfo, targetNodeId string) (*proto.MigrateKernelResponse, error) {
	// We pass 'false' for `wait` here, as we don't really need to wait for the CloneSet to scale-down.
	// As long as the replica is stopped, we can continue.
	dataDirectory := d.issuePrepareMigrateRequest(in.KernelId, in.ReplicaId)

	if targetNodeId != "" {
		host, hostExists := d.cluster.GetHost(targetNodeId)
		if !hostExists {
			d.log.Error("Cannot migrate replica %d of kernel %s to node %s, as that node does not exist within the cluster.",
				in.ReplicaId, in.KernelId, targetNodeId)
			return nil, fmt.Errorf("%w: no host with ID \"%s\" found in cluster",
				scheduling.ErrHostNotFound, targetNodeId)
		}

		container := host.GetAnyReplicaOfKernel(in.KernelId)
		if container != nil {
			// If the host has ANY replica of the kernel whose replica we're trying to migrate, then the host is not
			// viable.
			//
			// This is because the host either already has another replica of that same kernel on it, or the host is
			// hosting the replica we're presently trying to migrate.
			if container.ReplicaID() != in.ReplicaId {
				d.log.Error("Cannot migrate replica %d of kernel %s to node %s, as that node is already hosting another replica of that kernel (replica %d)",
					in.ReplicaId, in.KernelId, targetNodeId, container.ReplicaID())
				return nil, fmt.Errorf("%w: host %s is currently hosting another replica (%d) of the same kernel",
					scheduling.ErrHostNotViable, targetNodeId, container.ReplicaID())
			} else {
				d.log.Error("Cannot migrate replica %d of kernel %s to node %s, as the replica we're migrating is currently scheduled on that node",
					in.ReplicaId, in.KernelId, targetNodeId, container.ReplicaID())
				return nil, fmt.Errorf("%w: replica being migrated (%d) is already running on host %s",
					scheduling.ErrHostNotViable, in.ReplicaId, targetNodeId)
			}
		}

		kernel, loaded := d.kernels.Load(in.KernelId)
		if !loaded {
			d.log.Error("Cannot find client for kernel %s during migration of one of kernel %s's replicas...",
				in.KernelId, in.KernelId)
			return nil, ErrKernelNotFound
		}

		if !host.ResourceSpec().Validate(kernel.ResourceSpec()) {
			d.log.Error("Cannot migrate replica %d of kernel %s to host %s, as host does not have sufficiently-many allocatable resources to accommodate the replica.",
				in.ReplicaId, in.KernelId, targetNodeId)
			return nil, fmt.Errorf("%w: host lacks sufficiently-many allocatable resourecs", scheduling.ErrHostNotViable)
		}
	}

	oldHost, err := d.removeReplica(in.ReplicaId, in.KernelId)
	if err != nil {
		d.log.Error("Error while removing replica %d of kernel %s: %v", in.ReplicaId, in.KernelId, err)
	}

	d.log.Debug("Done removing replica %d of kernel %s.", in.ReplicaId, in.KernelId)

	// Add a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
	opts := NewAddReplicaWaitOptions(true, true, true)
	addReplicaOp, err := d.addReplica(in, opts, dataDirectory, []*scheduling.Host{oldHost})
	if err != nil {
		d.log.Error("Failed to add new replica %d to kernel %s: %v", in.ReplicaId, in.KernelId, err)
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	}

	replica, err := addReplicaOp.KernelReplicaClient().GetReplicaByID(addReplicaOp.ReplicaId())
	if err != nil {
		d.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v", addReplicaOp.ReplicaId(), in.KernelId, err)
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	} else {
		d.log.Debug("Successfully added new replica %d to kernel %s during migration operation.", addReplicaOp.ReplicaId(), in.KernelId)
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()

	return &proto.MigrateKernelResponse{Id: addReplicaOp.ReplicaId(), Hostname: addReplicaOp.ReplicaPodHostname()}, err
}

func (d *ClusterGatewayImpl) GetKubernetesNodes() ([]corev1.Node, error) {
	if d.DockerMode() {
		return make([]corev1.Node, 0), types.ErrIncompatibleDeploymentMode
	}

	return d.kubeClient.GetKubernetesNodes()
}

func (d *ClusterGatewayImpl) MigrateKernelReplica(_ context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	startTime := time.Now()
	replicaInfo := in.TargetReplica
	targetNodeId := in.GetTargetNodeId()

	if replicaInfo.GetPersistentId() == "" {
		// Automatically populate the persistent ID field.
		associatedKernel, loaded := d.kernels.Load(replicaInfo.KernelId)

		if !loaded {
			panic(fmt.Sprintf("Could not find kernel with ID \"%s\" during migration of that kernel's replica %d.", replicaInfo.KernelId, replicaInfo.ReplicaId))
		}

		replicaInfo.PersistentId = associatedKernel.PersistentID()

		d.log.Debug("Automatically populated PersistentID field of migration request of replica %d of kernel %s: '%s'", replicaInfo.ReplicaId, replicaInfo.KernelId, replicaInfo.PersistentId)
	}

	if targetNodeId != "" {
		d.log.Debug("Migrating replica %d of kernel %s to target node %s now.",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId)
	} else {
		d.log.Debug("Migrating replica %d of kernel %s to unspecified node now.",
			replicaInfo.ReplicaId, replicaInfo.KernelId)
	}

	resp, err := d.migrateReplicaRemoveFirst(replicaInfo, targetNodeId)
	duration := time.Since(startTime)
	if err != nil {
		d.log.Error("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration, err.Error())
		d.gatewayPrometheusManager.NumFailedMigrations.Inc()
	} else {
		d.log.Debug("Migration operation of replica %d of kernel %s to target node %s completed successfully after %v.",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration)
		d.gatewayPrometheusManager.NumSuccessfulMigrations.Inc()
	}

	d.gatewayPrometheusManager.KernelMigrationLatencyHistogram.Observe(float64(duration.Milliseconds()))

	// If there was an error, then err will be non-nil.
	// If there was no error, then err will be nil.
	return resp, err
}

func (d *ClusterGatewayImpl) Start() error {
	d.log.Info("Starting router...")

	if d.KubernetesMode() {
		// Start the HTTP Kubernetes Scheduler service.
		go d.cluster.ClusterScheduler().(scheduling.KubernetesClusterScheduler).StartHttpKubernetesSchedulerService()
	}

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Clean up
	d.cleanUp()

	return err
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
// Close is part of the net.Listener implementation.
func (d *ClusterGatewayImpl) Close() error {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the router
	if err := d.router.Close(); err != nil {
		d.log.Error("Failed to cleanly shutdown router because: %v", err)
	}

	// Wait for the kernels to be cleaned up
	<-d.cleaned

	// Close the listener
	if d.listener != nil {
		if err := d.listener.Close(); err != nil {
			d.log.Error("Failed to cleanly shutdown listener because: %v", err)
		}
	}

	return nil
}

////////////////////////////////////
// RouterProvider implementations //
////////////////////////////////////

func (d *ClusterGatewayImpl) ControlHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	// If this is a shutdown request, then use the RPC pathway instead.
	if msg.JupyterMessageType() == jupyter.MessageTypeShutdownRequest {
		sessionId := msg.JupyterSession()
		d.log.Debug("Intercepting \"%v\" message targeting session \"%s\" and using RPC pathway instead...",
			jupyter.MessageTypeShutdownRequest, sessionId)

		kernel, ok := d.kernels.Load(sessionId)
		if !ok {
			errorMessage := fmt.Sprintf("Could not find Kernel \"%s\"; cannot stop kernel.", sessionId)
			d.log.Error(errorMessage)
			// Spawn a separate goroutine to send an error notification to the dashboard.
			go d.notifyDashboardOfError(errorMessage, errorMessage)

			return ErrKernelNotFound
		}

		// Stop the kernel. If we get an error, print it here, and then we'll return it.
		var err error
		if _, err = d.stopKernelImpl(&proto.KernelId{Id: kernel.ID()}); err != nil {
			d.log.Error("Failed to (cleanly) terminate session \"%s\", kernel \"%s\" because: %v", sessionId, kernel.ID(), err)

			// Spawn a separate goroutine to send an error notification to the dashboard.
			go d.notifyDashboardOfError(fmt.Sprintf("Failed to Terminate Kernel %s, Session %s", kernel.ID(), sessionId), err.Error())
			return err
		}

		session, ok := d.cluster.Sessions().Load(kernel.ID())
		if !ok || session == nil {
			errorMessage := fmt.Sprintf("Could not find scheduling.Session %s associated with kernel %s, which is being shutdown", sessionId, kernel.ID())
			d.log.Error(errorMessage)
			go d.notifyDashboardOfError(fmt.Sprintf("Failed to Find scheduling.Session of Terminating Kernel \"%s\", Session ID=%s", kernel.ID(), sessionId), errorMessage)
		} else {
			p := session.SessionStopped()
			err := p.Error()
			if err != nil {
				d.log.Error("Error while descheduling kernel \"%s\" associated with session \"%s\"", kernel.ID(), sessionId)
				go d.notifyDashboardOfError(fmt.Sprintf("Error while Descheduling Session \"%s\"", kernel.ID()), err.Error())
				return err
			}
		}

		// TODO: This doesn't actually send an error response back to the client/Jupyter server.
		// This just returns to our underlying server's request handler code.
		// To send a response to Jupyter, we'd need to use the ClusterGatewayImpl::kernelResponseForwarder method.
		return err // Will be nil if we successfully shut down the kernel.
	}

	err := d.forwardRequest(nil, jupyter.ControlMessage, msg)

	// When a kernel is first created/being nudged, Jupyter Server will send both a Shell and Control request.
	// The Control request will just have a Session, and the mapping between the Session and the Kernel will not
	// be established until the Shell message is processed. So, if we see a ErrKernelNotFound error here, we will
	// simply retry it after some time has passed, as the requests are often received very close together.
	if errors.Is(err, ErrKernelNotFound) {
		time.Sleep(time.Millisecond * 750)

		// We won't re-try more than once.
		err = d.forwardRequest(nil, jupyter.ControlMessage, msg)
	}

	return err
}

func (d *ClusterGatewayImpl) kernelShellHandler(kernel scheduling.KernelInfo, _ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
	return d.ShellHandler(kernel, msg)
}

func (d *ClusterGatewayImpl) ShellHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	kernel, ok := d.kernels.Load(msg.JupyterSession())
	if !ok && (msg.JupyterMessageType() == jupyter.ShellKernelInfoRequest || msg.JupyterMessageType() == jupyter.ShellExecuteRequest) {
		// Register kernel on ShellKernelInfoRequest
		if msg.DestinationId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(msg.DestinationId)
		if !ok {
			d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v",
				msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())
			return ErrKernelNotFound
		}

		kernel.BindSession(msg.JupyterSession())
		d.kernels.Store(msg.JupyterSession(), kernel)
	}
	if kernel == nil {
		d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v",
			msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

		if len(msg.DestinationId) == 0 {
			d.log.Error("Extracted empty kernel ID from ZMQ \"%s\" message: %v", msg.JupyterMessageType(), msg)
			debug.PrintStack()
		}

		return ErrKernelNotFound
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	if msg.JupyterMessageType() == jupyter.ShellExecuteRequest {
		err := d.processExecuteRequest(msg, kernel)
		if err != nil {
			return err
		}
	} else {
		d.log.Debug("Forwarding shell message to kernel %s: %s", msg.DestinationId, msg)
	}

	if err := d.forwardRequest(kernel, jupyter.ShellMessage, msg); err != nil {
		return err
	}

	return nil
}

// processExecutionReply handles the scheduling and resource allocation/de-allocation logic required when a
// kernel finishes executing user-submitted code.
//
// TODO: Will there be race conditions here if we've sent multiple "execute_request" messages to the kernel?
// TODO: Could we receive a notification that a subsequent training has started before getting the reply that the last train completed?
// TODO: If so, how can we handle these out-of-order requests? We can associate trainings with Jupyter message IDs so that, if we get a >>
// TODO: >> training stopped notification, then it needs to match up with the current training, maybe in a queue structure, so that out-of-order >>
// TODO: >> messages can be handled properly.
func (d *ClusterGatewayImpl) processExecutionReply(kernelId string, msg *jupyter.JupyterMessage) error {
	d.log.Debug("Received execute_reply from kernel %s.", kernelId)

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		d.log.Error("Failed to load DistributedKernelClient %s while processing \"execute_reply\" message...", kernelId)
		go d.notifyDashboardOfError("Failed to Load Distributed Kernel Client", fmt.Sprintf("Failed to load DistributedKernelClient %s while processing \"execute_reply\" message...", kernelId))
		return ErrKernelNotFound
	}

	activeExecution := kernel.ActiveExecution()
	if activeExecution == nil {
		d.log.Error("No active execution registered for kernel %s...", kernelId)
		// return nil
	} else if activeExecution.ExecuteRequestMessageId != msg.JupyterParentMessageId() {
		// It's possible that we receive an "execute_reply" for execution i AFTER the "smr_lead_task" message for
		// execution i+1 (i.e., out of order). When this happens, we just stop the current training upon receiving
		// the "smr_lead_task" message for execution i+1. If and when we receive the "execute_reply" message for
		// execution i (after we've already moved on to execution i+1), we discard the "old" "execute_reply" message.
		d.log.Error(utils.RedStyle.Render("Received \"execute_reply\" for \"execute_request\" \"%s\"; however, current execution is for \"execute_request\" \"%s\"."),
			msg.JupyterParentMessageId(), activeExecution.ExecuteRequestMessageId)

		oldActiveExecution, loaded := kernel.GetActiveExecutionByExecuteRequestMsgId(msg.JupyterParentMessageId())
		if !loaded {
			d.log.Error(utils.RedStyle.Render("Could not find old ActiveExecution associated with \"execute_request\" \"%s\"..."), msg.JupyterParentMessageId())
		} else if oldActiveExecution.OriginalTimestampOrCreatedAt().After(kernel.ActiveExecution().OriginalTimestampOrCreatedAt()) {
			// If the "old" active execution -- the one associated with the Jupyter message ID of the "execute_request"
			// for which we just received the subsequent/complementary "execute_reply -- has an original send timestamp
			// (or was simply created) after the kernel's current active execution, then that indicates that we are in
			// an error state.
			//
			// Specifically, we should conceivably be receiving an "execute_reply" message out-of-order here. That is,
			// we received the "smr_lead_task" message for the next execution BEFORE receiving the "execute_reply"
			// message for the last execution. Upon receiving the "smr_lead_task" message, we ended the last execution
			// and began processing the next execution.
			//
			// So, what we'd like to do here is just discard/ignore the "execute_reply", as we received it late, and
			// we already did the processing that is required. However, if the current execution is actually older than
			// the execution associated with the "execute_reply" that we just received, then our assumptions are wrong!
			errorMessage := fmt.Sprintf("Thought we received out-of-order \"smr_lead_task\" and \"execute_reply\" messages for execution i+1 and i respectively, but current execution is actually older than execution associated with the \"execute_reply\" message that we just received...")
			d.notifyDashboardOfError("Old Active Execution Isn't Actually Old...", errorMessage)
			d.log.Error(errorMessage)
			d.log.Error(utils.RedStyle.Render("Old active execution isn't actually old. Current execution: %v. \"Old\" execution: %v.\n"),
				activeExecution.String(), oldActiveExecution.String())
			// log.Fatalf(utils.RedStyle.Render("Received out of order \"smr_lead_task\" and \"execute_reply\" messages from kernel %s.\n"), kernelId)
			d.log.Error(utils.RedStyle.Render("Received out of order \"smr_lead_task\" and \"execute_reply\" messages from kernel %s.\n"), kernelId)
		}
	}

	d.gatewayPrometheusManager.NumTrainingEventsCompletedCounterVec.
		With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Inc()

	var snapshotWrapper *scheduling.MetadataResourceWrapperSnapshot
	metadataFrame := msg.JupyterFrames.MetadataFrame()
	d.log.Debug("Attempting to decode metadata frame of Jupyter \"%s\" message %s (JupyterID=%s): %s",
		msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), string(*metadataFrame))
	err := json.Unmarshal(*metadataFrame, &snapshotWrapper)
	if err != nil {
		d.log.Error("Failed to decode metadata frame of \"%s\" message: %v", msg.JupyterMessageType(), err)
		return err // TODO: Should I panic here?
	}

	if snapshotWrapper.ResourceWrapperSnapshot != nil {
		d.log.Debug(utils.LightBlueStyle.Render("Extracted ResourceWrapperSnapshot from metadata frame of Jupyter \"%s\" message: %s"),
			jupyter.ShellExecuteReply, snapshotWrapper.ResourceWrapperSnapshot.String())
	} else {
		d.log.Warn(utils.OrangeStyle.Render("Jupyter \"%s\" did not contain an \"%s\" entry..."), msg.JupyterMessageType(), scheduling.ResourceSnapshotMetadataKey)
	}

	if _, err := kernel.ExecutionComplete(snapshotWrapper.ResourceWrapperSnapshot, msg); err != nil {
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) processExecuteRequest(msg *jupyter.JupyterMessage, kernel client.AbstractDistributedKernelClient) error {
	kernelId := kernel.ID()
	d.log.Debug("Forwarding shell \"execute_request\" message to kernel %s: %s", kernelId, msg)

	// activeExecution := scheduling.NewActiveExecution(kernelId, 1, kernel.Size(), msg)
	_ = kernel.EnqueueActiveExecution(1, msg)

	session, ok := d.cluster.GetSession(kernelId)
	if !ok {
		d.log.Error("Could not find scheduling.Session associated with kernel \"%s\"...", kernelId)
		return fmt.Errorf("%w: kernelID=\"%s\"", ErrSessionNotFound, kernelId)
	}

	if session.IsTraining() {
		d.log.Debug("Session %s is already training.", session.ID())
		return nil
	}

	// TODO: If Session is already training, then this will fail, and that's okay!
	p := session.SetExpectingTraining()
	err := p.Error()
	if err != nil {
		d.notifyDashboardOfError("Failed to Set Session to 'Expecting Training'", err.Error())
		// panic(err)
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) ClusterAge(_ context.Context, _ *proto.Void) (*proto.ClusterAgeResponse, error) {
	return &proto.ClusterAgeResponse{Age: d.createdAt.UnixMilli()}, nil
}

func (d *ClusterGatewayImpl) executionLatencyCallback(latency time.Duration, workloadId string, kernelId string) {
	d.gatewayPrometheusManager.JupyterTrainingStartLatency.
		With(prometheus.Labels{
			"workload_id": workloadId,
			"kernel_id":   kernelId,
		}).
		Observe(float64(latency.Milliseconds()))
}

func (d *ClusterGatewayImpl) StdinHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *ClusterGatewayImpl) HBHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (d *ClusterGatewayImpl) FailNextExecution(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	d.log.Debug("Received 'FailNextExecution' request targeting kernel %s.", in.Id)

	var (
		kernel *client.DistributedKernelClient
		loaded bool
	)

	// Ensure that the kernel exists.
	if kernel, loaded = d.kernels.Load(in.Id); !loaded {
		d.log.Error("Could not find kernel %s specified in 'FailNextExecution' request...", in.Id)
		return proto.VOID, ErrKernelNotFound
	}

	for _, replica := range kernel.Replicas() {
		replicaClient := replica.(*client.KernelReplicaClient)
		hostId := replicaClient.HostId()
		host, ok := d.cluster.GetHost(hostId)

		if !ok {
			d.log.Error("Could not find host %s on which replica %d of kernel %s is supposedly running...", hostId, replicaClient.ReplicaID(), in.Id)
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed", fmt.Sprintf("Could not find host %s on which replica %d of kernel %s is supposedly running...", hostId, replicaClient.ReplicaID(), in.Id))
			return proto.VOID, scheduling.ErrHostNotFound
		}

		// Even if there's an error here, we'll just keep trying. If only some of these succeed, then the system won't explode.
		// The kernels for which the `YieldNextExecution` succeeded will simply yield.
		_, err := host.YieldNextExecution(ctx, in)
		if err != nil {
			d.log.Error("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s", hostId, host.Addr, err.Error())
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed", fmt.Sprintf("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s", hostId, host.Addr, err.Error()))
		} else {
			d.log.Debug("Successfully issued 'FailNextExecution' to Local Daemon %s (%s) targeting kernel %s.", hostId, host.Addr, in.Id)
		}
	}

	return &proto.Void{}, nil
}

// Return the add-replica operation associated with the given Kernel ID and SMR Node ID of the new replica.
//
// This looks for the most-recently-added AddReplicaOperation associated with the specified replica of the specified kernel.
// If `mustBeActive` is true, then we skip any AddReplicaOperation structs that have already been marked as completed.
func (d *ClusterGatewayImpl) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (*AddReplicaOperation, bool) {
	d.addReplicaMutex.Lock()
	defer d.addReplicaMutex.Unlock()

	d.log.Debug("Searching for an active AddReplicaOperation for replica %d of kernel \"%s\".",
		smrNodeId, kernelId)

	activeOps, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		return nil, false
	}

	d.log.Debug("Number of AddReplicaOperation struct(s) associated with kernel \"%s\": %d", kernelId)

	var op *AddReplicaOperation
	// Iterate from newest to oldest, which entails beginning at the back.
	// We want to return the newest AddReplicaOperation that matches the replica ID for this kernel.
	for el := activeOps.Back(); el != nil; el = el.Prev() {
		d.log.Debug("AddReplicaOperation \"%s\": %s", el.Value.OperationID(), el.Value.String())
		// Check that the replica IDs match.
		// If they do match, then we either must not be bothering to check if the operation is still active, or it must still be active.
		if op == nil && el.Value.ReplicaId() == smrNodeId && el.Value.IsActive() {
			op = el.Value
		}
	}

	if op != nil {
		d.log.Debug("Returning AddReplicaOperation \"%s\": %s", op.OperationID(), op.String())
		return op, true
	}

	return nil, false
}

// Extract the Kernel ID and the message type from the given ZMQ message.
func (d *ClusterGatewayImpl) kernelAndTypeFromMsg(msg *jupyter.JupyterMessage) (kernel *client.DistributedKernelClient, messageType string, err error) {
	// This is initially the kernel's ID, which is the DestID field of the message.
	// But we may not have set a destination ID field within the message yet.
	// In this case, we'll fall back to the session ID within the message's Jupyter header.
	// This may not work either, though, if that session has not been bound to the kernel yet.
	//
	// When Jupyter clients connect for the first time, they send both a shell and a control "kernel_info_request" message.
	// This message is used to bind the session to the kernel (specifically the shell message).
	var kernelKey = msg.DestinationId

	// If there is no destination ID, then we'll try to use the session ID in the message's header instead.
	if len(kernelKey) == 0 {
		kernelKey = msg.JupyterSession()
		d.log.Debug("Message does not have Destination ID. Using session ID \"%s\" from Jupyter header instead.", kernelKey)

		// Sanity check.
		// Make sure we got a valid session ID out of the Jupyter message header.
		// If we didn't, then we'll return an error.
		if len(kernelKey) == 0 {
			d.log.Error("Jupyter Session ID is invalid for message: %s", msg.String())
			return nil, msg.JupyterMessageType(), fmt.Errorf("%w: message did not contain a destination ID, and session ID was invalid (i.e., the empty string)", ErrKernelNotFound)
		}
	}

	kernel, ok := d.kernels.Load(kernelKey) // kernelId)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", kernelKey)
		return nil, messageType, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, messageType, ErrKernelNotReady
	}

	return kernel, messageType, nil
}

func (d *ClusterGatewayImpl) forwardRequest(kernel *client.DistributedKernelClient, typ jupyter.MessageType, msg *jupyter.JupyterMessage) (err error) {
	goroutineId := goid.Get()
	// var messageType string
	if kernel == nil {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting unknown kernel/session. Inspecting now: %v"), goroutineId, typ.String(), msg.JupyterFrames.String())
		kernel, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	} else {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting kernel %s. Inspecting now..."), goroutineId, typ.String(), kernel.ID())
		// _, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	}

	if err != nil {
		d.log.Error("[gid=%d] Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.", goroutineId, typ, err, msg)
		return err
	}

	if kernel == nil {
		// Should not happen; if the error was nil, then kernel is non-nil.
		panic("Kernel is nil")
	}

	if connInfo := kernel.ConnectionInfo(); connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	if d.DebugMode && d.RequestLog != nil && msg.RequestTrace != nil {
		// If we added a RequestTrace for the first time, then let's also add an entry to our RequestLog.
		err = d.RequestLog.AddEntry(msg, typ, msg.RequestTrace)
		if err != nil {
			d.log.Warn("Failed to add entry to RequestLog for Jupyter %s \"%s\" message %s (JupyterID=%s) because: %v",
				typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)
		}
	}

	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelResponseForwarder, func() {})
}

func (d *ClusterGatewayImpl) kernelResponseForwarder(from scheduling.KernelReplicaInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
	goroutineId := goid.Get()
	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if msg.RequestTrace != nil {
		requestTrace := msg.RequestTrace
		if requestTrace.ReplicaId != -1 && requestTrace.ReplicaId != from.ReplicaID() {
			d.log.Warn("Overwriting existing replica ID of %d with %d in RequestTrace for %s \"%s\" message %s (JupyterID=\"%s\")",
				requestTrace.ReplicaId, from.ReplicaID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		}

		msg.RequestTrace.ReplicaId = from.ReplicaID()
	}

	if typ == jupyter.ShellMessage {
		if msg.JupyterMessageType() == jupyter.ShellExecuteReply {
			err := d.processExecutionReply(from.ID(), msg)
			if err != nil {
				go d.notifyDashboardOfError("Error While Processing \"execute_reply\" Message", err.Error())
				panic(err)
			}
		}
	}

	if d.DebugMode {
		requestTrace, _, err := jupyter.AddOrUpdateRequestTraceToJupyterMessage(msg, socket, time.Now(), d.log)
		if err != nil {
			d.log.Debug("Failed to update RequestTrace in %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\"): %v",
				typ, msg.JupyterMessageType(), msg.DestinationId, msg.JupyterMessageId(), err)
		}

		// If we added a RequestTrace for the first time, then let's also add an entry to our RequestLog.
		err = d.RequestLog.AddEntry(msg, typ, requestTrace)
		if err != nil {
			d.log.Warn("Failed to add entry to RequestLog for Jupyter %s \"%s\" message %s (JupyterID=%s) because: %v",
				typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)
		}

		// Commented Out:
		//
		// This works, but we actually don't need to do this, as the buffers ARE being sent to the kernel after all.
		// I was just confused by the output in the console. (Buffers was rendered as "[{}]", which I thought meant
		// that they were empty -- but apparently that is not the case, which is good.)
		//
		//if from.KernelSpec() != nil {
		//	signatureScheme := from.KernelSpec().SignatureScheme
		//	key := from.KernelSpec().Key
		//
		//	if key != "" { // We can default to hmac-sha256 if we don't have the signature scheme (though we should have it)
		//		jupyter.CopyRequestTraceFromBuffersToMetadata(msg, signatureScheme, key, d.log)
		//	} else {
		//		d.log.Warn("Cannot copy RequestTrace to metadata frame for %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\"). "+
		//			"We do not have the kernel's key (so we cannot re-sign the message).", typ, msg.JupyterMessageId(),
		//			msg.DestinationId, msg.JupyterMessageId())
		//	}
		//} else {
		//	d.log.Warn("Cannot copy RequestTrace to metadata frame for %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\"). "+
		//		"We do not have the kernel's signature scheme or key (so we cannot re-sign the message).", typ, msg.JupyterMessageId(),
		//		msg.DestinationId, msg.JupyterMessageId())
		//}
	}

	d.log.Debug(utils.DarkGreenStyle.Render("[gid=%d] Forwarding %v response from kernel %s via %s: %v"),
		goroutineId, typ, from.ID(), socket.Name, msg)
	zmqMsg := *msg.GetZmqMsg()
	sendStart := time.Now()
	err := socket.Send(zmqMsg)
	sendDuration := time.Since(sendStart)

	// Display a warning if the send operation took a while.
	if sendDuration >= time.Millisecond*50 {
		style := utils.YellowStyle

		// If it took over 100ms, then we'll use orange-colored text instead of yellow.
		if sendDuration >= time.Millisecond*100 {
			style = utils.OrangeStyle
		}

		d.log.Warn(style.Render("Forwarding %s \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s took %v."),
			socket.Type.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), from.ID(), sendDuration)
	}

	if metricError := d.gatewayPrometheusManager.SentMessage(d.id, sendDuration, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
		d.log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if metricError := d.gatewayPrometheusManager.SentMessageUnique(d.id, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
		d.log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if err != nil {
		d.log.Error(utils.RedStyle.Render("[gid=%d] Error while forwarding %v \"%s\" response %s (JupyterID=\"%s\") from kernel %s via %s: %s"),
			goroutineId, typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), from.ID(), socket.Name, err.Error())
	} else {
		d.log.Debug("Successfully forwarded %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\"): %v",
			typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), jupyter.FramesToString(zmqMsg.Frames))
	}

	return err // Will be nil on success.
}

func (d *ClusterGatewayImpl) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *ClusterGatewayImpl) statusErrorf(status jupyter.KernelStatus, err error) (*proto.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &proto.KernelStatus{Status: int32(status)}, nil
}

func (d *ClusterGatewayImpl) cleanUp() {
	// Clear nothing for now:
	// Hosts and kernels may contact other gateways to restore status.
	close(d.cleaned)
}

// Add a new replica to a particular distributed kernel.
// This is only used for adding new replicas beyond the base set of replicas created
// when the CloneSet is first created. The first 3 (or however many there are configured
// to be) replicas are created automatically by the CloneSet.
//
// Parameters:
// - kernelId (string): The ID of the kernel to which we're adding a new replica.
// - opts (AddReplicaWaitOptions): Specifies whether we'll wait for registration and/or SMR-joining.
// - dataDirectory (string): Path to etcd-raft data directory in HDFS.
func (d *ClusterGatewayImpl) addReplica(in *proto.ReplicaInfo, opts domain.AddReplicaWaitOptions, dataDirectory string, blacklistedHosts []*scheduling.Host) (*AddReplicaOperation, error) {
	var kernelId = in.KernelId
	var persistentId = in.PersistentId

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Cannot add replica %d to kernel %s: cannot find kernel %s", in.ReplicaId, kernelId, kernelId)
		return nil, d.errorf(ErrKernelNotFound)
	}

	kernel.AddOperationStarted()

	var smrNodeId int32 = -1

	// Reuse the same SMR node ID if we've been told to do so.
	if opts.ReuseSameNodeId() {
		smrNodeId = in.ReplicaId
	}

	// The spec to be used for the new replica that is created during the migration.
	var newReplicaSpec = kernel.PrepareNewReplica(persistentId, smrNodeId)

	addReplicaOp := NewAddReplicaOperation(kernel, newReplicaSpec, dataDirectory)

	d.log.Debug("Adding replica %d to kernel %s now.", newReplicaSpec.ReplicaId, kernelId)

	// Add the AddReplicaOperation to the associated maps belonging to the Gateway Daemon.
	d.addReplicaMutex.Lock()
	ops, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		ops = orderedmap.NewOrderedMap[string, *AddReplicaOperation]()
	}
	ops.Set(addReplicaOp.OperationID(), addReplicaOp)
	d.activeAddReplicaOpsPerKernel.Store(kernelId, ops)
	d.addReplicaMutex.Unlock()

	d.containerWatcher.RegisterChannel(kernelId, addReplicaOp.ReplicaStartedChannel())

	err := d.cluster.ClusterScheduler().ScheduleKernelReplica(newReplicaSpec, nil, blacklistedHosts)
	if err != nil {
		return addReplicaOp, err
	}

	// In Kubernetes deployments, the key is the Pod name, which is also the kernel ID + replica suffix.
	// In Docker deployments, the container name isn't really the container's name, but its ID, which is a hash
	// or something like that.
	var key, podOrContainerName string
	if d.KubernetesMode() {
		d.log.Debug("Waiting for new replica to be created for kernel %s.", kernelId)

		// Always wait for the scale-out operation to complete and the new replica to be created.
		key = <-addReplicaOp.ReplicaStartedChannel()
		podOrContainerName = key
	} else {
		// connInfo, err := d.launchReplicaDocker(int(newReplicaSpec.ReplicaID), host, 3, nil, newReplicaSpec) /* Only 1 of arguments 3 and 4 can be non-nil */
		// connInfo, err := d.placer.Place(host, newReplicaSpec)

		d.log.Debug("Waiting for new replica to be created for kernel %s.", kernelId)

		// Always wait for the scale-out operation to complete and the new replica to be created.
		notificationMarshalled := <-addReplicaOp.ReplicaStartedChannel()

		// In Docker mode, we receive a DockerContainerStartedNotification that was marshalled to JSON, which returns
		// a []byte, and then converted to a string via string(marshalledDockerContainerStartedNotification).
		//
		// Unmarshal it so we can extract the metadata.
		//
		// We'll store the metadata from the DockerContainerStartedNotification in the AddReplicaOperation's metadata.
		var notification *DockerContainerStartedNotification
		if err := json.Unmarshal([]byte(notificationMarshalled), &notification); err != nil {
			d.log.Error("Failed to unmarshal DockerContainerStartedNotification because: %v", err)
			go d.notifyDashboardOfError("Failed to Unmarshal DockerContainerStartedNotification", err.Error())
			panic(err)
		}

		addReplicaOp.SetMetadata(domain.DockerContainerFullId, notification.FullContainerId)
		addReplicaOp.SetMetadata(domain.DockerContainerShortId, notification.ShortContainerId)
		podOrContainerName = notification.FullContainerId
		key = fmt.Sprintf("%s-%d", notification.KernelId, addReplicaOp.ReplicaId())
	}

	d.log.Debug("New replica %d has been created for kernel %s.", addReplicaOp.ReplicaId(), kernelId)
	addReplicaOp.SetContainerName(podOrContainerName)
	d.Lock()
	d.addReplicaOperationsByKernelReplicaId.Store(key, addReplicaOp)

	if channel, ok := d.addReplicaNewPodNotifications.Load(key); ok {
		d.log.Debug("Sending AddReplicaOperation for replica %d of kernel %s over channel.",
			addReplicaOp.ReplicaId(), addReplicaOp.KernelId())
		channel <- addReplicaOp
	} else {
		d.log.Debug("Skipping the sending of AddReplicaOperation for replica %d of kernel %s over channel.",
			addReplicaOp.ReplicaId(), addReplicaOp.KernelId())
	}

	d.Unlock()

	if opts.WaitRegistered() {
		d.log.Debug("Waiting for new replica %d of kernel \"%s\" to register during AddOperation \"%s\"",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		replicaRegisteredChannel := addReplicaOp.ReplicaRegisteredChannel()
		_ = <-replicaRegisteredChannel
		d.log.Debug("New replica %d of kernel \"%s\" has registered with the Gateway during AddOperation \"%s\".",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
	}

	var smrWg sync.WaitGroup
	smrWg.Add(1)
	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted when the new replica joins its SMR cluster.
	go func() {
		d.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster... [AddOperation.OperationID=%v]]",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		<-addReplicaOp.ReplicaJoinedSmrChannel()
		d.log.Debug("New replica %d of kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
		kernel.AddOperationCompleted()
		smrWg.Done()
	}()

	if opts.WaitSmrJoined() {
		d.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster...", addReplicaOp.ReplicaId(), kernelId)
		smrWg.Wait()
	}

	// Return nil on success.
	return addReplicaOp, nil
}

// Remove a replica from a distributed kernel.
//
// Parameters:
// - smrNodeId (int32): The SMR node ID of the replica that should be removed.
// - kernelId (string): The ID of the kernel from which we're removing a replica.
//
// Returns:
// - previous host (*scheduling.Host): the scheduling.Host that the replica was hosted on prior to being removed
// - error: an error, if one occurred
func (d *ClusterGatewayImpl) removeReplica(smrNodeId int32, kernelId string) (*scheduling.Host, error) {
	kernelClient, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernel client for kernel %s.", kernelId)
		return nil, ErrKernelNotFound
	}

	replica, err := kernelClient.GetReplicaByID(smrNodeId)
	if err != nil {
		d.log.Error("Could not find replica of kernel %s with ID %d.", kernelId, smrNodeId)
		return nil, ErrKernelIDRequired
	}

	oldPodName := replica.PodName()

	session, loaded := d.cluster.Sessions().Load(kernelId)
	if !loaded {
		d.log.Error("Could not find scheduling.Session associated with kernel \"%s\"...", kernelId)
		return nil, fmt.Errorf("%w: kernelID=\"%s\"", ErrSessionNotFound, kernelId)
	}

	container, ok := session.GetReplicaContainer(smrNodeId)
	if !ok {
		d.log.Error("Could not load scheduling.Container associated with replica %d of kernel \"%s\" from associated scheduling.Session",
			smrNodeId, kernelId)
		return nil, fmt.Errorf("%w: kernelID=\"%s\", replicaId=%d", ErrContainerNotFound, smrNodeId, kernelId)
	}

	oldHost := container.GetHost()
	if oldHost == nil {
		d.log.Error("scheduling.Container for replica %d of kernel \"%s\" does not know what host it is on (prior to removal)...",
			smrNodeId, kernelId)
		return nil, scheduling.ErrNilHost
	}

	wg, ok := d.waitGroups.Load(kernelId)
	if !ok {
		d.log.Error("Could not find WaitGroup for kernel %s after removing replica %d of said kernel...", kernelId, smrNodeId)
		return nil, err
	}

	// First, stop the kernel on the replica we'd like to remove.
	_, err = kernelClient.RemoveReplicaByID(smrNodeId, d.cluster.Placer().Reclaim, false)
	if err != nil {
		d.log.Error("Error while stopping replica %d of kernel %s: %v", smrNodeId, kernelId, err)
		return nil, err
	}

	if err = session.RemoveReplicaById(smrNodeId); err != nil {
		d.log.Error("Failed to remove replica %d from session \"%s\" because: %v", smrNodeId, kernelId, err)
		return nil, err
	}

	removed := wg.RemoveReplica(smrNodeId)
	if !removed {
		// TODO(Ben): We won't necessarily always return an error for this, but it's definitely problematic.
		// For now, I will return an error so I can debug the situation if it arises, because I don't think
		// it ever should if things are working correctly.
		d.log.Error("Now-removed replica %d of kernel %s was not present in associated WaitGroup...")
		return nil, err
	}

	d.log.Debug("Successfully removed replica %d of kernel %s.", smrNodeId, kernelId)

	// If we're running in Kubernetes mode, then we'll explicitly scale-in the CloneSet and wait for the Pod to stop.
	if d.KubernetesMode() {
		podStoppedChannel := make(chan struct{}, 1) // Buffered.

		// Next, scale-in the CloneSet, taking care to ensure the correct Pod is deleted.
		err = d.kubeClient.ScaleInCloneSet(kernelId, oldPodName, podStoppedChannel)
		if err != nil {
			d.log.Error("Error while scaling-in CloneSet for kernel %s: %v", kernelId, err)
			return nil, err
		}

		<-podStoppedChannel
		d.log.Debug("Successfully scaled-in CloneSet by deleting Pod %s.", oldPodName)
	}

	return oldHost, nil
}

func (d *ClusterGatewayImpl) listKernels() (*proto.ListKernelsResponse, error) {
	resp := &proto.ListKernelsResponse{
		Kernels: make([]*proto.DistributedJupyterKernel, 0, max(d.kernelIdToKernel.Len(), 1)),
	}

	d.Lock()
	defer d.Unlock()

	d.kernelIdToKernel.Range(func(id string, kernel *client.DistributedKernelClient) bool {
		// d.log.Debug("Will be returning Kernel %s with %d replica(s) [%v] [%v].", id, kernel.Len(), kernel.Status(), kernel.AggregateBusyStatus())

		respKernel := &proto.DistributedJupyterKernel{
			KernelId:            id,
			NumReplicas:         int32(kernel.Size()),
			Status:              kernel.Status().String(),
			AggregateBusyStatus: kernel.AggregateBusyStatus(),
			KernelSpec:          kernel.KernelSpec(),
		}

		replicas := make([]*proto.JupyterKernelReplica, 0, len(kernel.Replicas()))
		for _, replica := range kernel.Replicas() {
			kernelReplica := &proto.JupyterKernelReplica{
				KernelId:  id,
				ReplicaId: replica.ReplicaID(),
				PodId:     replica.PodName(),
				NodeId:    replica.NodeName(),
			}
			replicas = append(replicas, kernelReplica)
		}
		respKernel.Replicas = replicas

		resp.Kernels = append(resp.Kernels, respKernel)

		return true
	})

	return resp, nil
}

// GetVirtualDockerNodes returns a (pointer to a) proto.GetVirtualDockerNodesResponse struct describing the virtual,
// simulated nodes currently provisioned within the cluster.
//
// When deployed in Docker Swarm mode, our cluster has both "actual" nodes, which correspond to the nodes that
// Docker Swarm knows about, and virtual nodes that correspond to each local daemon container.
//
// In a "real" deployment, there would be one local daemon per Docker Swarm node. But for development and debugging,
// we may provision many local daemons per Docker Swarm node, where each local daemon manages its own virtual node.
//
// If the internalCluster is not running in Docker mode, then this will return an error.
func (d *ClusterGatewayImpl) GetVirtualDockerNodes(_ context.Context, _ *proto.Void) (*proto.GetVirtualDockerNodesResponse, error) {
	d.log.Debug("Received request for the cluster's virtual docker nodes.")

	d.dockerNodeMutex.Lock()
	defer d.dockerNodeMutex.Unlock()

	// TODO: For now, both Docker Swarm mode and Docker Compose mode support Virtual Docker Nodes.
	// Eventually, Docker Swarm mode will ~only~ support Docker Swarm nodes, which correspond to real machines or VMs.
	// Virtual Docker nodes correspond to each Local Daemon container, and are primarily used for development or small, local simulations.
	if !d.DockerMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	nodes := make([]*proto.VirtualDockerNode, 0, d.cluster.Len())

	d.cluster.RangeOverHosts(func(_ string, host *scheduling.Host) (contd bool) {
		//d.log.Debug("Host %s (ID=%s):", host.NodeName, host.ID)
		//d.log.Debug("Idle resources: %s", host.IdleResources().String())
		//d.log.Debug("Pending resources: %s", host.PendingResources().String())
		//d.log.Debug("Committed resources: %s", host.CommittedResources().String())
		//d.log.Debug("Spec resources: %s", host.ResourceSpec().String())

		virtualDockerNode := host.ToVirtualDockerNode()
		nodes = append(nodes, virtualDockerNode)

		//d.log.Debug("Returning VirtualDockerNode: %s\n", virtualDockerNode.String())
		return true
	})

	resp := &proto.GetVirtualDockerNodesResponse{
		Nodes: nodes,
	}

	return resp, nil
}

func (d *ClusterGatewayImpl) GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	d.dockerNodeMutex.Lock()
	defer d.dockerNodeMutex.Unlock()

	// TODO: For now, both Docker Swarm mode and Docker Compose mode support Virtual Docker Nodes.
	// Eventually, Docker Swarm mode will ~only~ support Docker Swarm nodes, which correspond to real machines or VMs.
	// Virtual Docker nodes correspond to each Local Daemon container, and are primarily used for development or small, local simulations.
	if !d.DockerMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	hostIds := make([]string, 0, d.cluster.Len())

	d.cluster.RangeOverHosts(func(hostId string, _ *scheduling.Host) (contd bool) {
		hostIds = append(hostIds, hostId)
		return true
	})

	resp := &proto.GetLocalDaemonNodeIDsResponse{
		HostIds: hostIds,
	}

	return resp, nil
}

// GetDockerSwarmNodes returns a (pointer to a) proto.GetDockerSwarmNodesResponse struct describing the Docker Swarm
// nodes that exist within the Docker Swarm cluster.
//
// When deployed in Docker Swarm mode, our cluster has both "actual" nodes, which correspond to the nodes that
// Docker Swarm knows about, and virtual nodes that correspond to each local daemon container.
//
// In a "real" deployment, there would be one local daemon per Docker Swarm node. But for development and debugging,
// we may provision many local daemons per Docker Swarm node, where each local daemon manages its own virtual node.
//
// If the internalCluster is not running in Docker mode, then this will return an error.
func (d *ClusterGatewayImpl) GetDockerSwarmNodes(_ context.Context, _ *proto.Void) (*proto.GetDockerSwarmNodesResponse, error) {
	d.dockerNodeMutex.Lock()
	defer d.dockerNodeMutex.Unlock()

	// We can only get Docker Swarm nodes if we're running in Docker Swarm.
	if !d.DockerSwarmMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	return nil, status.Errorf(codes.Unimplemented, "method GetDockerSwarmNodes not implemented")
}

func (d *ClusterGatewayImpl) GetNumNodes(_ context.Context, _ *proto.Void) (*proto.NumNodesResponse, error) {
	return &proto.NumNodesResponse{
		NumNodes: int32(d.cluster.Len()),
		NodeType: d.cluster.NodeType(),
	}, nil
}

func (d *ClusterGatewayImpl) SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error) {
	initialSize := d.cluster.Len()
	d.log.Debug("Received request to scale the cluster to %d nodes (current size: %d).", in.TargetNumNodes, initialSize)
	p := d.cluster.ScaleToSize(ctx, in.TargetNumNodes)

	if err := p.Error(); err != nil {
		d.log.Error("Failed to set cluster scale to %d nodes because: %v", in.TargetNumNodes, err)
		return nil, err
	}

	result, err := p.Result()
	if err != nil {
		d.log.Error("Failed to set cluster scale to %d nodes because: %v", in.TargetNumNodes, err)
		return nil, err
	}

	scaleResult := result.(scheduling.ScaleOperationResult)
	d.log.Debug("Successfully fulfilled set-scale request: %s", scaleResult.String())
	return &proto.SetNumClusterNodesResponse{
		RequestId:   in.RequestId,
		OldNumNodes: int32(initialSize),
		NewNumNodes: int32(d.cluster.Len()),
	}, nil
}

func (d *ClusterGatewayImpl) AddClusterNodes(ctx context.Context, in *proto.AddClusterNodesRequest) (*proto.AddClusterNodesResponse, error) {
	d.log.Debug("Received request to add %d node(s) to the cluster.", in.NumNodes)
	p := d.cluster.RequestHosts(ctx, in.NumNodes)

	if err := p.Error(); err != nil {
		d.log.Error("Failed to add %d nodes because: %v", in.NumNodes, err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		d.log.Error("Failed to add %d nodes because: %v", in.NumNodes, err)
		return nil, err
	}

	scaleOutOperationResult := scaleResult.(*scheduling.ScaleOutOperationResult)
	d.log.Debug("Successfully fulfilled add node request: %s", scaleOutOperationResult.String())
	return &proto.AddClusterNodesResponse{
		RequestId:         in.RequestId,
		PrevNumNodes:      scaleOutOperationResult.PreviousNumNodes,
		NumNodesCreated:   scaleOutOperationResult.NumNodesCreated,
		NumNodesRequested: in.NumNodes,
	}, nil
}

func (d *ClusterGatewayImpl) RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error) {
	d.log.Debug("Received request to remove %d specific node(s) from the cluster.", len(in.NodeIDs), strings.Join(in.NodeIDs, ", "))
	p := d.cluster.ReleaseSpecificHosts(ctx, in.NodeIDs)

	if err := p.Error(); err != nil {
		d.log.Error("Failed to remove %d specific nodes because: %v", len(in.NodeIDs), err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		d.log.Error("Failed to remove %d specific nodes because: %v", len(in.NodeIDs), err)
		return nil, err
	}

	scaleInOperationResult := scaleResult.(*scheduling.ScaleInOperationResult)
	d.log.Debug("Successfully fulfilled specific node removal request: %s", scaleInOperationResult.String())
	return &proto.RemoveSpecificClusterNodesResponse{
		RequestId:       in.RequestId,
		OldNumNodes:     scaleInOperationResult.PreviousNumNodes,
		NumNodesRemoved: scaleInOperationResult.NumNodesTerminated,
		NewNumNodes:     scaleInOperationResult.CurrentNumNodes,
		NodesRemoved:    scaleInOperationResult.Nodes(),
	}, nil
}

func (d *ClusterGatewayImpl) RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error) {
	d.log.Debug("Received request to remove %d node(s) from the cluster.", in.NumNodesToRemove)
	p := d.cluster.ReleaseHosts(ctx, in.NumNodesToRemove)

	if err := p.Error(); err != nil {
		d.log.Error("Failed to remove %d nodes because: %v", in.NumNodesToRemove, err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		d.log.Error("Failed to remove %d nodes because: %v", in.NumNodesToRemove, err)
		return nil, err
	}

	scaleInOperationResult := scaleResult.(*scheduling.ScaleInOperationResult)
	d.log.Debug("Successfully fulfilled node removal request: %s", scaleInOperationResult.String())
	return &proto.RemoveClusterNodesResponse{
		RequestId:       in.RequestId,
		OldNumNodes:     scaleInOperationResult.PreviousNumNodes,
		NumNodesRemoved: scaleInOperationResult.NumNodesTerminated,
		NewNumNodes:     scaleInOperationResult.CurrentNumNodes,
	}, nil
}

func (d *ClusterGatewayImpl) ModifyClusterNodes(_ context.Context, _ *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error) {
	return nil, ErrNotImplemented
}
