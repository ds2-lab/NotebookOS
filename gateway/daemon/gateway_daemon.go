package daemon

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/shopspring/decimal"

	"github.com/scusemua/distributed-notebook/common/proto"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
	"github.com/petermattis/goid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	dockerClient "github.com/docker/docker/client"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/domain"
)

const (
	GpuDeviceIdsArg   = "gpu_device_ids"
	ForceReprocessArg = "force_reprocess"

	// SkipValidationKey is passed in Context of NotifyKernelRegistered to skip the connection validation step.
	SkipValidationKey contextKey = "SkipValidationKey"
)

type contextKey string

var (
	// gRPC errors

	ErrNotImplemented = status.Error(codes.Unimplemented, "not implemented in daemon")
	ErrNotSupported   = status.Error(codes.Unimplemented, "not supported in daemon")

	NotificationTypeNames = []string{"ERROR", "WARNING", "INFO", "SUCCESS"}

	// Internal errors

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

	ErrConcurrentRemoval = errors.New("cannot remove all replicas of specified kernel because a previous removal attempt is still in-progress")

	ErrUnsupportedMsgTypeForArtificialResponse = errors.New("unsupported message type for artificial response")
)

type GatewayDaemonConfig func(ClusterGateway)

type DistributedClientBuilder interface {
	WithContext(ctx context.Context) DistributedClientBuilder
	WithSpec(spec *proto.KernelSpec) DistributedClientBuilder
	WithNumReplicas(numReplicas int) DistributedClientBuilder
	WithHostId(hostId string) DistributedClientBuilder
	WithConnectionInfo(connectionInfo *jupyter.ConnectionInfo) DistributedClientBuilder
	WithPersistentId(persistentId string) DistributedClientBuilder
	WithDebugMode(debugMode bool) DistributedClientBuilder
	WithExecutionFailedCallback(callback scheduling.ExecutionFailedCallback) DistributedClientBuilder
	WithExecutionLatencyCallback(callback scheduling.ExecutionLatencyCallback) DistributedClientBuilder
	WithStatisticsProvider(provider scheduling.StatisticsProvider) DistributedClientBuilder
	WithNotificationCallback(callback scheduling.NotificationCallback) DistributedClientBuilder
	BuildKernel() scheduling.Kernel
}

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
		numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
		statisticsProvider scheduling.StatisticsProvider, callbackProvider scheduling.CallbackProvider) scheduling.Kernel

	//NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
	//	numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
	//	executionFailedCallback scheduling.ExecutionFailedCallback, ExecutionLatencyCallback scheduling.ExecutionLatencyCallback,
	//	statisticsProvider scheduling.StatisticsProvider, notificationCallback scheduling.NotificationCallback) scheduling.Kernel

	// GetBuilder() DistributedClientBuilder
}

// ClusterGateway is an interface for the "main" scheduler/manager of the distributed notebook cluster.
// This interface exists so that we can spoof it during unit tests.
type ClusterGateway interface {
	proto.ClusterGatewayServer

	SetDistributedClientProvider(provider DistributedClientProvider)

	SetClusterOptions(*scheduling.SchedulerOptions)

	ConnectionOptions() *jupyter.ConnectionInfo

	// Scheduler returns the associated Scheduler.
	Scheduler() scheduling.Scheduler

	// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
	GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error)

	// KubernetesMode returns true if we're running in a Kubernetes cluster (rather than as a docker-compose application).
	KubernetesMode() bool

	// DockerMode returns true if we're running in "docker swarm" mode or "docker compose" mode.
	DockerMode() bool

	// DockerSwarmMode returns true if we're running in "docker swarm" mode.
	DockerSwarmMode() bool

	// DockerComposeMode returns true if we're running in "docker compose" mode.
	DockerComposeMode() bool

	// GetHostsOfKernel returns the Host instances on which replicas of the specified kernel are scheduled.
	GetHostsOfKernel(kernelId string) ([]scheduling.Host, error)

	// GetId returns the ID of the ClusterGateway.
	GetId() string

	// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
	GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error)
}

// ensureErrorGrpcCompatible ensures that the given error is compatible with gRPC.
//
// If the given error is not compatible with gRPC, then ensureErrorGrpcCompatible wraps it in a gRPC error and returns
// the new error.
//
// If the given error is already compatible, then ensureErrorGrpcCompatible just returns the original error as-is.
func ensureErrorGrpcCompatible(err error, code codes.Code) error {
	if err == nil {
		return nil
	}

	if _, ok := status.FromError(err); ok {
		return err // Already compatible.
	}

	return status.Error(code, err.Error())
}

// PrometheusMetricsProvider defines the general interface of Prometheus metric managers.
//
// This interface is designed to be used by external entities who need to store/publish metrics.
type PrometheusMetricsProvider interface {
	// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
	Start() error

	// NodeId returns the node ID associated with the metrics manager.
	NodeId() string

	// IsRunning returns true if the PrometheusMetricsProvider has been started and is serving metrics.
	IsRunning() bool

	// Stop instructs the PrometheusMetricsProvider to shut down its HTTP server.
	Stop() error
}

// ClusterGatewayImpl serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bidirectional gRPC calls.
//
// Some useful resources for jupyter protocol:
// https://jupyter-client.readthedocs.io/en/stable/messaging.html
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type ClusterGatewayImpl struct {
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer
	// createdAt is the time at which the ClusterGatewayImpl struct was created.
	createdAt time.Time

	lastFullStatisticsUpdate time.Time

	DistributedClientProvider DistributedClientProvider

	// cluster provisioning related members
	listener net.Listener
	cluster  scheduling.Cluster

	// kernels is a map from kernel and session IDs to kernels.
	// There may be duplicate values (i.e., multiple sessions mapping to the same kernel).
	kernels hashmap.HashMap[string, scheduling.Kernel]

	// kernelIdToKernel is a map from kernel ID to client.DistributedKernelClient.
	kernelIdToKernel hashmap.HashMap[string, scheduling.Kernel]

	// kernelSpecs is a map from kernel ID to the *proto.KernelSpec specified when the kernel was first created.
	kernelSpecs hashmap.HashMap[string, *proto.KernelSpec]

	// Map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting hashmap.HashMap[string, chan struct{}]

	// kernelsWithReplicasBeingCreated is a map whose keys are kernel IDs.
	// The entries in the kernelsWithReplicasBeingCreated map correspond to kernels whose replicas are in the process
	// of being created/scheduled. This is used to prevent multiple concurrent requests to schedule the kernel replicas
	// and kernel replica containers of the same kernel.
	// kernelsWithReplicasBeingCreated *hashmap.ThreadsafeCornelkMap[string, *kernelCreateContainers]

	// kernelRegisteredNotifications is a map from notification ID to *proto.KernelRegistrationNotification
	// to keep track of the notifications that we've received so we can discard duplicates.
	kernelRegisteredNotifications hashmap.HashMap[string, *proto.KernelRegistrationNotification]

	log logger.Logger

	// waitGroups hashmap.HashMap[string, *sync.primarSemaphore]
	waitGroups hashmap.HashMap[string, *registrationWaitGroups]

	// Kubernetes client. This is shared with the associated internalCluster Gateway.
	kubeClient scheduling.KubeClient

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerEventHandler scheduling.ContainerWatcher

	// gRPC connection to the Dashboard.
	clusterDashboard proto.ClusterDashboardClient

	router *router.Router

	// SchedulerOptions
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    *scheduling.SchedulerOptions

	cleaned chan struct{}

	// ClusterStatistics encapsulates a number of statistics/metrics.
	ClusterStatistics *metrics.ClusterStatistics

	// executionFailedCallback is the ClusterGatewayImpl's scheduling.ExecutionFailedCallback (i.e., recovery callback for panics).
	// The primary purpose is simply to send a notification to the dashboard that a panic occurred before exiting.
	// This makes error detection easier (i.e., it's immediately obvious when the system breaks as we're notified
	// visually of the panic in the cluster dashboard).
	executionFailedCallback scheduling.ExecutionFailedCallback

	// executeRequestForwarder forwards "execute_request" (or "yield_request") messages to kernels one-at-a-time.
	executeRequestForwarder *client.ExecuteRequestForwarder[[]*messaging.JupyterMessage]

	// remoteDockerEventAggregator listens for docker events that occur on remote nodes in Docker Swarm mode.
	remoteDockerEventAggregator *RemoteDockerEventAggregator

	// Docker client.
	dockerApiClient *dockerClient.Client

	// MetricsProvider provides all metrics to the members of the scheduling package.
	MetricsProvider *metrics.ClusterMetricsProvider

	// hostSpec is the resource spec of Hosts in the Cluster
	hostSpec *types.DecimalSpec

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	RequestLog *metrics.RequestLog

	id string

	// policyKey refers to the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
	policyKey scheduling.PolicyKey

	// kernel members
	transport string
	ip        string

	// Hostname of the RemoteStorage NameNode. The SyncLog's RemoteStorage client will connect to this.
	remoteStorageEndpoint string

	// Run via Docker on a single system rather than using the Kubernetes-based deployment.
	deploymentMode types.DeploymentMode

	// The name of the Docker network that the container is running within. Only used in Docker mode.
	dockerNetworkName string

	// numResendAttempts is the number of times to try resending a message before giving up.
	numResendAttempts int

	// The IOPub socket that all Jupyter clients subscribe to.
	// io pub *messaging.Socket
	smrPort int

	// prometheusInterval is how often we publish metrics to Prometheus.
	prometheusInterval time.Duration

	// IdleSessionReclamationInterval is the interval of real-life clock time that must elapse before a Session
	// is considered idle and is eligible for reclamation of IdleSessionReclamationEnabled is set to true.
	//
	// If IdleSessionReclamationInterval is set to 0, then idle session reclamation is disabled, regardless of the
	// value of the IdleSessionReclamationEnabled flag.
	IdleSessionReclamationInterval time.Duration

	idleSessionReclaimer *IdleSessionReclaimer

	mu sync.RWMutex

	clusterStatisticsMutex sync.Mutex

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// dockerNodeMutex is used to synchronize the operations involved with getting or modifying the
	// number of Local Daemon Docker nodes/containers.
	dockerNodeMutex sync.Mutex

	// numActiveKernels is the number of actively-running kernels.
	numActiveKernels atomic.Int32

	// lifetime
	closed int32

	started atomic.Bool

	notifier *Notifier

	// Indicates that a goroutine has been started to publish metrics to Prometheus.
	servingPrometheus atomic.Int32

	// numActiveTrainings is the cluster-wide number of active training events.
	numActiveTrainings atomic.Int32

	// IdleSessionReclamationEnabled if a flag that, when true, instructs the system to consider sessions to be idle
	// and eligible for "reclamation" after IdleSessionReclamationInterval elapses. When a session is reclaimed, its
	// kernel replica containers are de-scheduled. They will have to be rescheduled if the client submits execute
	// requests again in the future.
	//
	// If IdleSessionReclamationInterval is set to 0, then idle session reclamation is disabled, regardless of the
	// value of the IdleSessionReclamationEnabled flag.
	IdleSessionReclamationEnabled bool

	// DebugMode is a configuration parameter that, when enabled, causes the RequestTrace to be enabled as well
	// as the request history.
	DebugMode bool

	// SubmitExecuteRequestsOneAtATime indicates whether the client.ExecuteRequestForwarder should be used to submit
	// execute requests, which forces requests to be submitted one-at-a-time.
	SubmitExecuteRequestsOneAtATime bool

	// MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements
	// for the ZMQ messages that we're forwarding back and forth between the Jupyter Server and the Local Daemons.
	//
	// MessageAcknowledgementsEnabled is controlled by the "acks_enabled" field of the configuration file.
	MessageAcknowledgementsEnabled bool
}

func New(opts *jupyter.ConnectionInfo, clusterDaemonOptions *domain.ClusterDaemonOptions, configs ...GatewayDaemonConfig) *ClusterGatewayImpl {
	clusterGateway := &ClusterGatewayImpl{
		id:                              uuid.New().String(),
		connectionOptions:               opts,
		createdAt:                       time.Now(),
		transport:                       "tcp",
		ip:                              opts.IP,
		DebugMode:                       clusterDaemonOptions.CommonOptions.DebugMode,
		kernels:                         hashmap.NewConcurrentMap[scheduling.Kernel](32),
		kernelIdToKernel:                hashmap.NewConcurrentMap[scheduling.Kernel](32),
		kernelSpecs:                     hashmap.NewConcurrentMap[*proto.KernelSpec](32),
		waitGroups:                      hashmap.NewConcurrentMap[*registrationWaitGroups](32),
		kernelRegisteredNotifications:   hashmap.NewCornelkMap[string, *proto.KernelRegistrationNotification](64),
		kernelsStarting:                 hashmap.NewCornelkMap[string, chan struct{}](64),
		cleaned:                         make(chan struct{}),
		smrPort:                         clusterDaemonOptions.SMRPort,
		remoteStorageEndpoint:           clusterDaemonOptions.RemoteStorageEndpoint,
		dockerNetworkName:               clusterDaemonOptions.DockerNetworkName,
		numResendAttempts:               clusterDaemonOptions.NumResendAttempts,
		MessageAcknowledgementsEnabled:  clusterDaemonOptions.MessageAcknowledgementsEnabled,
		prometheusInterval:              time.Second * time.Duration(clusterDaemonOptions.PrometheusInterval),
		ClusterStatistics:               metrics.NewClusterStatistics(),
		SubmitExecuteRequestsOneAtATime: clusterDaemonOptions.SubmitExecuteRequestsOneAtATime,
		IdleSessionReclamationEnabled:   clusterDaemonOptions.IdleSessionReclamationEnabled,
		// Set the interval to a minimum value of 0 seconds, which disables idle session reclamation.
		IdleSessionReclamationInterval: time.Second * time.Duration(math.Max(0, float64(clusterDaemonOptions.IdleSessionReclamationIntervalSec))),
	}

	// If the interval is set to 0, then we just disable idle session reclamation.
	if clusterGateway.IdleSessionReclamationInterval == 0 {
		clusterGateway.IdleSessionReclamationEnabled = false
	}

	clusterGateway.notifier = NewNotifier(nil) // We'll reassign this if and when the dashboard connects.

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

	clusterGateway.executeRequestForwarder = client.NewExecuteRequestForwarder[[]*messaging.JupyterMessage](
		clusterGateway.notifier.NotifyDashboard, nil)

	clusterGateway.MetricsProvider = metrics.NewClusterMetricsProvider(
		clusterDaemonOptions.PrometheusPort, clusterGateway, clusterGateway.UpdateClusterStatistics,
		clusterGateway.IncrementResourceCountsForNewHost, clusterGateway.DecrementResourceCountsForRemovedHost,
		&clusterGateway.numActiveTrainings)

	clusterGateway.router = router.New(context.Background(), clusterGateway.id, clusterGateway.connectionOptions, clusterGateway,
		clusterGateway.MessageAcknowledgementsEnabled, "ClusterGatewayRouter", false,
		metrics.ClusterGateway, clusterGateway.DebugMode, clusterGateway.MetricsProvider.GetGatewayPrometheusManager())

	if clusterGateway.MetricsProvider.PrometheusMetricsEnabled() {
		err := clusterGateway.MetricsProvider.StartGatewayPrometheusManager()
		if err != nil {
			panic(err)
		}

		clusterGateway.router.AssignPrometheusManager(clusterGateway.MetricsProvider.GetGatewayPrometheusManager())

		// Initial values for these metrics.
		clusterGateway.MetricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Set(0)
		clusterGateway.MetricsProvider.GetGatewayPrometheusManager().DemandGpusGauge.Set(0)
		clusterGateway.MetricsProvider.GetGatewayPrometheusManager().BusyGpusGauge.Set(0)
	} else {
		clusterGateway.log.Warn("PrometheusPort is set to a negative number. Skipping initialization of Prometheus-related components.")
	}

	if !clusterDaemonOptions.DisablePrometheusMetricsPublishing && clusterDaemonOptions.PrometheusPort > 0 {
		clusterGateway.log.Debug("Initializing \"Prometheus Metrics Publisher\" goroutine now.")
		clusterGateway.publishPrometheusMetrics()
	} else {
		clusterGateway.log.Warn("\"Prometheus Metrics Publisher\" goroutine is disabled. Skipping initialization.")
	}

	if clusterGateway.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			clusterGateway.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			clusterGateway.ip = ip
		}
	}

	switch clusterDaemonOptions.SchedulingPolicy {
	case string(scheduling.DefaultSchedulingPolicy):
		{
			clusterGateway.policyKey = scheduling.DefaultSchedulingPolicy
			clusterGateway.log.Debug("Using the 'DEFAULT' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.defaultFailureHandler
		}
	case string(scheduling.Static):
		{
			clusterGateway.policyKey = scheduling.Static
			clusterGateway.log.Debug("Using the 'STATIC' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.staticSchedulingFailureHandler
		}
	case string(scheduling.DynamicV3):
		{
			clusterGateway.policyKey = scheduling.DynamicV3
			clusterGateway.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.dynamicV3FailureHandler
		}
	case string(scheduling.DynamicV4):
		{
			clusterGateway.policyKey = scheduling.DynamicV4
			clusterGateway.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.dynamicV4FailureHandler
		}
	case string(scheduling.Gandiva):
		{
			clusterGateway.policyKey = scheduling.Gandiva
			clusterGateway.log.Debug("Using the 'GANDIVA' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.gandivaV4FailureHandler
		}
	case string(scheduling.FcfsBatch):
		{
			clusterGateway.policyKey = scheduling.FcfsBatch
			clusterGateway.log.Debug("Using the 'FCFS Batch' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.fcfsBatchSchedulingFailureHandler
		}
	case string(scheduling.MiddleGround):
		{
			clusterGateway.policyKey = scheduling.MiddleGround
			clusterGateway.log.Debug("Using the 'Middle Ground' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.middleGroundSchedulingFailureHandler
		}
	case string(scheduling.Reservation):
		{
			clusterGateway.policyKey = scheduling.Reservation
			clusterGateway.log.Debug("Using the 'Reservation' scheduling policy.")
			clusterGateway.executionFailedCallback = clusterGateway.reservationSchedulingFailureHandler
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", clusterDaemonOptions.SchedulingPolicy))
		}
	}

	// Create the internalCluster Scheduler.
	clusterSchedulerOptions := clusterDaemonOptions.SchedulerOptions

	gpusPerHost := clusterSchedulerOptions.GpusPerHost
	if gpusPerHost <= 0 {
		clusterGateway.log.Error("Invalid number of simulated GPUs specified: %d. Value must be >= 1 (even if there are no real GPUs available).",
			gpusPerHost)
		panic(fmt.Sprintf("invalid number of simulated GPUs specified: %d. Value must be >= 1 (even if there are no real GPUs available).",
			gpusPerHost))
	}

	// It's possible one of the config functions already set this, so check that it is nil first.
	if clusterGateway.hostSpec == nil {
		vram := clusterDaemonOptions.VramGbPerHost
		if vram <= 0 {
			vram = scheduling.DefaultVramPerHostGb
		}

		millicpus := clusterDaemonOptions.MillicpusPerHost
		if millicpus <= 0 {
			millicpus = scheduling.DefaultMillicpusPerHost
		}

		memoryMb := clusterDaemonOptions.MemoryMbPerHost
		if memoryMb <= 0 {
			memoryMb = scheduling.DefaultMemoryMbPerHost
		}

		// millicpu and GPU values are rounded to 0 decimal places. memory (mb) values are rounded to 3
		// decimal places. vram values are rounded to 6 decimal places. This is to be consistent with the granularities
		// supported by Kubernetes for resource requests/limits (millicpus and kilobytes/kibibytes).
		clusterGateway.hostSpec = &types.DecimalSpec{
			GPUs:      decimal.NewFromFloat(float64(gpusPerHost)).Round(0),
			VRam:      decimal.NewFromFloat(vram).Round(6),
			Millicpus: decimal.NewFromFloat(float64(millicpus)).Round(3),
			MemoryMb:  decimal.NewFromFloat(memoryMb).Round(0),
		}
	}

	clusterProvider := func() scheduling.Cluster {
		if clusterGateway == nil {
			return nil
		}

		return clusterGateway.cluster
	}

	schedulingPolicy, policyError := scheduler.GetSchedulingPolicy(&clusterDaemonOptions.SchedulerOptions, clusterProvider)
	if policyError != nil {
		panic(policyError)
	}

	// Note: we don't construct the scheduling.cluster struct within the switch statement below.
	// We construct the scheduling.cluster struct immediately following the switch statement.
	var (
		clusterPlacer scheduling.Placer
		clusterType   cluster.Type
		err           error
	)
	switch clusterDaemonOptions.DeploymentMode {
	case "":
		{
			clusterGateway.log.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			panic("Not supported")
		}
	case "local":
		{
			clusterGateway.log.Info("Running in LOCAL mode.")
			clusterGateway.deploymentMode = types.LocalMode
			panic("Not supported")
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

			dockerEventHandler := NewDockerEventHandler()
			clusterGateway.containerEventHandler = dockerEventHandler

			clusterType = cluster.DockerCompose
			break
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

			clusterType = cluster.DockerSwarm

			break
		}
	case "kubernetes":
		{
			clusterGateway.log.Info("Running in KUBERNETES mode.")
			clusterGateway.deploymentMode = types.KubernetesMode

			clusterGateway.kubeClient = NewKubeClient(clusterGateway, clusterDaemonOptions)
			clusterGateway.containerEventHandler = clusterGateway.kubeClient

			clusterType = cluster.Kubernetes

			break
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

	clusterPlacer, err = schedulingPolicy.GetNewPlacer(clusterGateway.MetricsProvider)
	if err != nil {
		clusterGateway.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}

	// This is where we actually construct the scheduling.cluster struct.
	distributedNotebookCluster, err := cluster.NewBuilder(clusterType).
		WithKubeClient(clusterGateway.kubeClient).
		WithHostSpec(clusterGateway.hostSpec).
		WithPlacer(clusterPlacer).
		WithSchedulingPolicy(schedulingPolicy).
		WithHostMapper(clusterGateway).
		WithKernelProvider(clusterGateway).
		WithClusterMetricsProvider(clusterGateway.MetricsProvider).
		WithNotificationBroker(clusterGateway).
		WithStatisticsUpdateProvider(clusterGateway.UpdateClusterStatistics).
		WithOptions(&clusterSchedulerOptions).
		BuildCluster()

	if err != nil {
		clusterGateway.log.Error("Failed to construct scheduling.cluster: %v", err)
		panic(err)
	}

	clusterGateway.cluster = distributedNotebookCluster

	if clusterGateway.MetricsProvider.PrometheusMetricsEnabled() {
		clusterGateway.MetricsProvider.GetGatewayPrometheusManager().
			ClusterSubscriptionRatioGauge.Set(clusterGateway.cluster.SubscriptionRatio())
	}

	clusterGateway.gatherClusterStatistics()

	if clusterGateway.IdleSessionReclamationEnabled {
		idleReclaimFunc := func(kernel scheduling.Kernel, inSeparateGoroutine bool, noop bool) error {
			return clusterGateway.removeAllReplicasOfKernel(kernel, inSeparateGoroutine, true, noop)
		}

		clusterGateway.idleSessionReclaimer = NewIdleSessionReclaimer(clusterGateway.kernels,
			clusterGateway.IdleSessionReclamationInterval, clusterGateway.NumReplicas(), idleReclaimFunc)

		clusterGateway.idleSessionReclaimer.Start()
	}

	return clusterGateway
}

// containerPrewarmer returns the scheduling.ContainerPrewarmer used by the cluster's Scheduler.
func (d *ClusterGatewayImpl) containerPrewarmer() scheduling.ContainerPrewarmer {
	clusterScheduler := d.Scheduler()
	if clusterScheduler == nil {
		return nil
	}

	return clusterScheduler.ContainerPrewarmer()
}
func (d *ClusterGatewayImpl) SetDistributedClientProvider(provider DistributedClientProvider) {
	d.DistributedClientProvider = provider
}

func (d *ClusterGatewayImpl) SetClusterOptions(options *scheduling.SchedulerOptions) {
	d.ClusterOptions = options
}

// SendErrorNotification sends an 'error' notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) SendErrorNotification(errorName string, errorMessage string) {
	go d.notifier.NotifyDashboardOfError(errorName, errorMessage)
}

// SendInfoNotification sends an 'info' notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) SendInfoNotification(title string, message string) {
	go d.notifier.NotifyDashboardOfInfo(title, message)
}

// NumReplicas is a helper function that returns the number of replicas as specified by the configured scheduling policy.
func (d *ClusterGatewayImpl) NumReplicas() int {
	return d.Scheduler().Policy().NumReplicas()
}

func (d *ClusterGatewayImpl) Scheduler() scheduling.Scheduler {
	return d.cluster.Scheduler()
}

func (d *ClusterGatewayImpl) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	receivedAt := time.Now()
	kernelId := in.KernelId

	var messageType string
	var socketType messaging.MessageType
	if in.SocketType == "shell" {
		socketType = messaging.ShellMessage
		messageType = "ping_kernel_shell_request"
	} else if in.SocketType == "control" {
		socketType = messaging.ControlMessage
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
		}, types.ErrKernelNotFound
	}

	// If we're using, for example, batch FCFS scheduling, then the replicas may not be scheduled.
	// In this case, we'll just return a result directly.
	if !kernel.ReplicasAreScheduled() {
		if d.shouldReplicasBeRunning(kernel) {
			return nil, ErrKernelNotReady
		}

		return &proto.Pong{
			Id:            kernelId,
			Success:       true,
			RequestTraces: nil,
		}, nil
	}

	var (
		msgId = uuid.NewString()
		msg   zmq4.Msg
		err   error
	)
	// frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.sessions()[0])
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.ID())

	// If DebugMode is enabled, then add a buffers frame with a RequestTrace.
	var requestTrace *proto.RequestTrace
	if d.DebugMode {
		frames.Frames = append(frames.Frames, make([]byte, 0))

		requestTrace = proto.NewRequestTrace(msgId, messageType, kernelId)

		// Then we'll populate the sort of metadata fields of the RequestTrace.
		requestTrace.RequestReceivedByGateway = receivedAt.UnixMilli()

		// Create the wrapper/frame itself.
		wrapper := &proto.JupyterRequestTraceFrame{RequestTrace: requestTrace}

		marshalledFrame, err := json.Marshal(&wrapper)
		if err != nil {
			d.log.Error("Failed to marshall RequestTraceWrapper when creating %s \"%s\" request \"%s\".",
				msgId, socketType.String(), messageType)
			return nil, status.Error(codes.Internal, err.Error())
		}

		frames.Frames[messaging.JupyterFrameRequestTrace] = marshalledFrame
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernelId, kernel.ConnectionInfo().SignatureScheme, err)

		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, ErrFailedToVerifyMessage
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	respChan := make(chan interface{}, d.NumReplicas())

	startTime := time.Now()
	var numRepliesReceived atomic.Int32
	responseHandler := func(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)

		// Notify that all replies have been received.
		if msg.RequestTrace != nil {
			requestTrace := msg.RequestTrace
			if requestTrace.ReplicaId != -1 && requestTrace.ReplicaId != from.ReplicaID() {
				d.log.Warn("Overwriting existing replica ID of %d with %d in RequestTrace for %s \"%s\" message %s (JupyterID=\"%s\")",
					requestTrace.ReplicaId, from.ReplicaID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
			}

			requestTrace.ReplicaId = from.ReplicaID()

			d.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v. Request trace: %s.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, kernel.Size(), time.Since(startTime),
				msg.RequestTrace.String())

			respChan <- requestTrace
		} else {
			d.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, kernel.Size(), time.Since(startTime))

			respChan <- struct{}{}
		}

		return nil
	}

	jMsg := messaging.NewJupyterMessage(&msg)
	err = kernel.RequestWithHandler(ctx, "Forwarding", socketType, jMsg, responseHandler, nil)
	if err != nil {
		d.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v",
			socketType.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)

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

	requestTraces := make([]*proto.RequestTrace, 0, d.NumReplicas())

	for numRepliesReceived.Load() < int32(d.NumReplicas()) {
		select {
		case <-ctx.Done():
			{
				ctxError := ctx.Err()

				var errorMessage string
				if ctxError != nil {
					errorMessage = fmt.Sprintf("%v 'ping-kernel' request for kernel %s failed after receiving %d/%d replies: %v",
						socketType.String(), kernelId, numRepliesReceived.Load(), kernel.Size(), ctxError)
				} else {
					errorMessage = fmt.Sprintf("%v 'ping-kernel' request for kernel %s timed-out after receiving %d/%d replies.",
						socketType.String(), kernelId, numRepliesReceived.Load(), kernel.Size())
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
				reqTrace, ok := v.(*proto.RequestTrace)
				if ok {
					requestTraces = append(requestTraces, reqTrace)
				}
			}
		}
	}

	// Include the "ReplySentByGateway" entry, since we're returning the response via gRPC,
	// and thus it won't be added automatically by the ZMQ-forwarder server.
	replySentByGateway := time.Now().UnixMilli()
	for _, reqTrace := range requestTraces {
		reqTrace.ReplySentByGateway = replySentByGateway
	}

	d.log.Debug("Received all 3 %v 'ping_reply' responses from replicas of kernel %s for ping message \"%s\" in %v.",
		socketType.String(), kernelId, msgId, time.Since(startTime))
	return &proto.Pong{
		Id:            kernelId,
		Success:       true,
		RequestTraces: requestTraces,
	}, nil
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
				d.MetricsProvider.GetGatewayPrometheusManager().
					DemandGpusGauge.Set(d.cluster.DemandGPUs())

				d.MetricsProvider.GetGatewayPrometheusManager().
					BusyGpusGauge.Set(d.cluster.BusyGPUs())

				d.MetricsProvider.GetGatewayPrometheusManager().
					ClusterSubscriptionRatioGauge.Set(d.cluster.SubscriptionRatio())
			}

			time.Sleep(d.prometheusInterval)
		}
	}()
}

func (d *ClusterGatewayImpl) assignClusterDashboardClient(clusterDashboard proto.ClusterDashboardClient) {
	d.clusterDashboard = clusterDashboard
	d.notifier = NewNotifier(clusterDashboard)
}

// GetHostsOfKernel returns the Host instances on which replicas of the specified kernel are scheduled.
func (d *ClusterGatewayImpl) GetHostsOfKernel(kernelId string) ([]scheduling.Host, error) {
	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		return nil, types.ErrKernelNotFound
	}

	hosts := make([]scheduling.Host, 0, len(kernel.Replicas()))
	for _, replica := range kernel.Replicas() {
		hosts = append(hosts, replica.Host())
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

// acceptHostConnection accepts an incoming connection from a Local Daemon and establishes a bidirectional
// gRPC connection with that Local Daemon.
//
// This returns the gRPC connection, the initial connection, the replacement connection, and an error if one occurs.
func (d *ClusterGatewayImpl) acceptHostConnection() (*grpc.ClientConn, net.Conn, net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := d.listener.Accept()
	if err != nil {
		return nil, nil, nil, err
	}

	d.log.Debug("ClusterGatewayImpl is accepting a new connection.")

	// Initialize yamux session for bidirectional gRPC calls
	// At gateway side, we first wait an incoming replacement connection, then create a reverse provisioner connection to the host scheduler.
	cliSession, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		d.log.Error("Failed to create yamux client session: %v", err)
		d.log.Error("Incoming remote: %v", incoming.RemoteAddr().String())
		d.log.Error("Incoming local: %v", incoming.LocalAddr().String())
		return nil, nil, nil, err
	}

	// Create a new session to replace the incoming connection.
	conn, err := cliSession.Accept()
	if err != nil {
		d.log.Error("Failed to wait for the replacement of host scheduler connection: %v", err)
		d.log.Error("Incoming remote: %v", incoming.RemoteAddr().String())
		d.log.Error("Incoming local: %v", incoming.LocalAddr().String())
		d.log.Error("CLI Session addr: %v", cliSession.Addr().String())
		d.log.Error("CLI Session remote: %v", cliSession.RemoteAddr().String())
		d.log.Error("CLI Session local: %v", cliSession.LocalAddr().String())
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	return gConn, conn, incoming, err
}

// restoreHost is used to restore an existing Host when a Local Daemon loses connection with the Cluster Gateway
// and then reconnects. This will return nil on success.
//
// If the cluster gateway recently crashed and the container restarted, then the restoration will fail, and
// restoreHost will simply treat the scheduling.Host as if it were a new host and pass it to RegisterNewHost,
// the result of which will be returned from restoreHost.
func (d *ClusterGatewayImpl) restoreHost(host scheduling.Host) error {
	d.log.Warn("Newly-connected Local Daemon actually already exists.")

	// Sanity check.
	if host == nil {
		errorMessage := "We're supposed to restore a Local Daemon, but the host with which we would perform the restoration is nil...\n"
		d.notifier.NotifyDashboardOfError("Failed to Re-Register Local Daemon", errorMessage)
		log.Fatalf(utils.RedStyle.Render(errorMessage))
	}

	// Restore the Local Daemon.
	// This replaces the gRPC connection of the existing Host struct with that of a new one,
	// as well as a few other fields.
	registered, loaded := d.cluster.GetHost(host.GetID())
	if loaded {
		err := registered.Restore(host, d.localDaemonDisconnected)
		if err != nil {
			d.log.Error("Error while restoring host %v: %v", host, err)
			return err
		}

		d.log.Debug("Successfully restored existing Local Daemon %s (ID=%s).", registered.GetNodeName(), registered.GetID())
		go d.notifier.NotifyDashboardOfInfo(
			fmt.Sprintf("Local Daemon %s Reconnected", registered.GetNodeName()),
			fmt.Sprintf("Local Daemon %s on node %s has reconnected to the cluster Gateway.",
				registered.GetID(),
				registered.GetNodeName()))
		return nil
	}

	// This may occur if the cluster Gateway crashes and restarts.
	d.log.Warn("Supposedly existing Local Daemon (re)connected, but cannot find associated Host struct... "+
		"Node claims to be Local Daemon %s (ID=%s).", host.GetID(), host.GetNodeName())

	// Just register the Host as a new Local Daemon, despite the fact that the Host thinks it already exists.
	return d.RegisterNewHost(host)
}

// RegisterNewHost is used to register a new Host (i.e., Local Daemon) with the Cluster after the Host connects
// to the Cluster Gateway.
//
// This will return nil on success.
func (d *ClusterGatewayImpl) RegisterNewHost(host scheduling.Host) error {
	if !host.IsProperlyInitialized() {
		log.Fatalf(utils.RedStyle.Render("Newly-connected Host %s (ID=%s) was NOT properly initialized..."),
			host.GetNodeName(), host.GetID())
	}

	d.log.Info("Incoming Local Scheduler %s (ID=%s) connected", host.GetNodeName(), host.GetID())

	err := d.cluster.NewHostAddedOrConnected(host)
	if err != nil {
		d.log.Error("Error while adding newly-connected host %s (ID=%s) to the cluster: %v", host.GetNodeName(), host.GetID(), err)
		return err
	}

	go d.notifier.NotifyDashboardOfInfo("Local Scheduler Connected", fmt.Sprintf("Local Scheduler %s (ID=%s) has connected to the cluster Gateway.", host.GetNodeName(), host.GetID()))

	return nil
}

// Accept waits for and returns the next connection to the listener.
// Accept is part of the net.Listener interface implementation.
func (d *ClusterGatewayImpl) Accept() (net.Conn, error) {
	gConn, conn, incoming, connectionError := d.acceptHostConnection()
	if connectionError != nil {
		return nil, connectionError
	}

	// Create a host scheduler client and register it.
	host, err := entity.NewHostWithConn(uuid.NewString(), incoming.RemoteAddr().String(), d.cluster.NumReplicas(),
		d.cluster, d.cluster, d.MetricsProvider, gConn, d.Scheduler().Policy(), d.localDaemonDisconnected)

	if err != nil {
		if errors.Is(err, entity.ErrRestoreRequired) {
			err = d.restoreHost(host)
			if err != nil {
				return nil, err
			}

			return conn, nil
		} else {
			d.log.Error("Failed to create host scheduler client: %v", err)
			return nil, err
		}
	}

	registrationError := d.RegisterNewHost(host)
	if registrationError != nil {
		d.log.Error("Failed to register new host %s (ID=%s) because: %v", host.GetNodeName(), host.GetID(), registrationError)
		return nil, registrationError
	}

	return conn, nil
}

// Close are compatible with ClusterGatewayImpl.Close().

// Addr returns the listener's network address.
// Addr is part of the net.Listener implementation.
func (d *ClusterGatewayImpl) Addr() net.Addr {
	return d.listener.Addr()
}

func (d *ClusterGatewayImpl) SetID(_ context.Context, _ *proto.HostId) (*proto.HostId, error) {
	return nil, ErrNotImplemented
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

	// host := targetReplica.Context().Value(client.CtxKernelHost).(scheduling.Host)
	host := targetReplica.Host()
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	d.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s. Sending request to Local Daemon of replica %d.",
		nodeId, kernelId, targetReplica.ReplicaID())
	replicaInfo := &proto.ReplicaInfoWithAddr{
		Id:       nodeId,
		KernelId: kernelId,
		Hostname: fmt.Sprintf("%s:%d", newAddress, d.smrPort),
	}

	// Issue the 'update-replica' request. We panic if there was an error.
	if _, err := host.UpdateReplicaAddr(context.Background(), replicaInfo); err != nil {
		d.log.Debug("Failed to add replica %d of kernel %s to SMR cluster because: %v", nodeId, kernelId, err)
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	d.log.Debug("Successfully updated peer address of replica %d of kernel %s to %s.", nodeId, kernelId, newAddress)
	// time.Sleep(time.Second * 5)
}

// SmrReady is an RPC handler called by the Local Daemon to the Cluster Gateway to notify the Gateway that a
// "smr_node_ready" message was received.
func (d *ClusterGatewayImpl) SmrReady(_ context.Context, smrReadyNotification *proto.SmrReadyNotification) (*proto.Void, error) {
	kernelId := smrReadyNotification.KernelId

	// First, check if this notification is from a replica of a kernel that is starting up for the very first time.
	// If so, we'll send a notification in the associated channel, and then we'll return.
	kernelStartingChan, ok := d.kernelsStarting.Load(smrReadyNotification.KernelId)
	if ok {
		d.log.Debug("Received 'SMR-READY' notification for newly-starting replica %d of kernel %s.",
			smrReadyNotification.ReplicaId, smrReadyNotification.KernelId)
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
	addReplicaOp.SetReplicaJoinedSMR()

	return proto.VOID, nil
}

// func (d *ClusterGatewayImpl) SmrNodeRemoved(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
// 	kernelId := replicaInfo.kernelId
// 	d.log.Debug("Received SMR Node-Removed notification for replica %d of kernel %s.", replicaInfo.ReplicaID, kernelId)

// 	channelMapKey := fmt.Sprintf("%s-%s", kernelId, replicaInfo.ReplicaID)
// 	channel, ok := d.smrNodeRemovedNotifications.Load(channelMapKey)
// 	if !ok {
// 		panic(fmt.Sprintf("Could not find \"node-removed\" notification channel for replica %d of kernel %s.", replicaInfo.ReplicaID, kernelId))
// 	}

// 	channel <- struct{}{}

// 	return gateway.VOID, nil
// }

// SmrNodeAdded is an RPC function called by the Local Daemon to the Cluster Gateway when the Local Daemon
// receives a "smr_node_added" IOPub message.
//
// NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called. We use SMR_READY instead.
func (d *ClusterGatewayImpl) SmrNodeAdded(_ context.Context, replicaInfo *proto.ReplicaInfo) (*proto.Void, error) {
	d.log.Error(utils.RedStyle.Render("Unexpected -- we haven't been using SmrNodeAdded, so why is it being called? ReplicaInfo: %s"),
		replicaInfo.String())
	panic("SmrNodeAdded is no longer supported.")
	//
	// NOTHING BELOW THE CALL TO log.Fatalf WILL BE EXECUTED, AS log.Fatalf PANICS.
	//
	// I am just leaving the code below as a reference or in case we ever start using SmrNodeAdded again.
	// For now, we just use SmrReady instead.
	//

	//kernelId := replicaInfo.kernelId
	//d.log.Debug("Received SMR Node-Added notification for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId)
	//
	//// If there's no add-replica operation here, then we'll just return.
	//op, opExists := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, replicaInfo.ReplicaId)
	//
	//if !opExists {
	//	d.log.Warn("No active add-replica operation found for replica %d, kernel %s.", replicaInfo.ReplicaId, kernelId)
	//	return proto.VOID, nil
	//}
	//
	//if op.Completed() {
	//	log.Fatalf(utils.RedStyle.Render("Retrieved AddReplicaOperation %v targeting replica %d of kernel %s -- this operation has already completed.\n"),
	//		op.OperationID(), replicaInfo.ReplicaId, kernelId)
	//}
	//
	//op.SetReplicaJoinedSMR()
	//
	//return proto.VOID, nil
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (d *ClusterGatewayImpl) kernelReconnectionFailed(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, reconnectionError error) { /* client scheduling.kernel,  */
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending \"%s\" message: %v",
		kernel.ReplicaID(), kernel.ID(), messageType, reconnectionError)
	d.log.Error(errorMessage)

	go d.notifier.NotifyDashboardOfError("Connection to kernel Lost & Reconnection Failed", errorMessage)
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
func (d *ClusterGatewayImpl) kernelRequestResubmissionFailedAfterReconnection(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, resubmissionError error) {
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to forward \"'%s'\" request to replica %d of kernel %s following successful connection re-establishment because: %v",
		messageType, kernel.ReplicaID(), kernel.ID(), resubmissionError)
	d.log.Error(errorMessage)

	d.notifier.NotifyDashboardOfError("Connection to kernel Lost, Reconnection Succeeded, but Request Resubmission Failed", errorMessage)
}

// ExecutionFailedCallback is a callback to be executed if all replicas propose "YIELD".
//
// The passed message is the "execute_reply" or "yield_reply".
func (d *ClusterGatewayImpl) ExecutionFailedCallback(c scheduling.Kernel, msg *messaging.JupyterMessage) error {
	return d.executionFailedCallback(c, msg)
}

// defaultFailureHandler is invoked when an "execute_request" cannot be processed when using the Default
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) defaultFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	d.log.Warn("There is no failure handler for the DEFAULT scheduling policy.")
	return fmt.Errorf("there is no failure handler for the DEFAULT scheduling policy; cannot handle error")
}

// fcfsBatchSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the FCFS
// Batch scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) fcfsBatchSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	d.log.Warn("There is no failure handler for the FCFS Batch scheduling policy.")
	return fmt.Errorf("there is no failure handler for the FCFS Batch policy; cannot handle error")
}

// middleGroundSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the FCFS
// Batch scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) middleGroundSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	d.log.Warn("There is no failure handler for the 'Middle Ground' scheduling policy.")
	return fmt.Errorf("there is no failure handler for the 'Middle Ground policy; cannot handle error")
}

// reservationSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the
// Reservation scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) reservationSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	d.log.Warn("There is no failure handler for the Reservation scheduling policy.")
	return fmt.Errorf("there is no failure handler for the Reservation scheduling policy; cannot handle error")
}

// staticSchedulingFailureHandler is a callback to be invoked when all replicas of a
// kernel propose 'YIELD' while static scheduling is set as the configured scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) staticSchedulingFailureHandler(kernel scheduling.Kernel, executeRequestMsg *messaging.JupyterMessage) error {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplicaId := rand.Intn(kernel.Size()) + 1
	d.log.Debug(utils.LightBlueStyle.Render("Static Failure Handler: migrating replica %d of kernel %s now."),
		targetReplicaId, kernel.ID())

	// Notify the cluster dashboard that we're performing a migration.
	go d.notifier.NotifyDashboardOfInfo(fmt.Sprintf("All Replicas of kernel \"%s\" Have Proposed 'YIELD'", kernel.ID()),
		fmt.Sprintf("All replicas of kernel %s proposed 'YIELD' during code execution.", kernel.ID()))

	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    int32(targetReplicaId),
			PersistentId: kernel.PersistentID(),
		},
		ForTraining:  true,
		TargetNodeId: nil,
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	errorChan := make(chan error, 1)

	// Start the migration operation in another thread so that we can do some stuff while we wait.
	go func() {
		resp, err := d.MigrateKernelReplica(context.TODO(), req)

		if err != nil {
			d.log.Warn(utils.OrangeStyle.Render("Static Failure Handler: failed to migrate replica %d of kernel %s because: %s"),
				targetReplicaId, kernel.ID(), err.Error())

			var migrationError error

			if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
				migrationError = errors.Join(scheduling.ErrMigrationFailed, err)
			} else {
				migrationError = err
			}

			errorChan <- migrationError
		} else {
			d.log.Debug(utils.GreenStyle.Render("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s."),
				targetReplicaId, kernel.ID(), resp.Hostname)
		}

		waitGroup.Done()
	}()

	metadataDict, err := executeRequestMsg.DecodeMetadata()
	if err != nil {
		d.log.Warn("Failed to unmarshal metadata frame for \"execute_request\" message \"%s\" (JupyterID=\"%s\"): %v",
			executeRequestMsg.RequestId, executeRequestMsg.JupyterMessageId(), executeRequestMsg)

		// We'll assume the metadata frame was empty, and we'll create a new dictionary to use as the metadata frame.
		metadataDict = make(map[string]interface{})
	}

	// Specify the target replica.
	metadataDict[messaging.TargetReplicaArg] = targetReplicaId
	metadataDict[ForceReprocessArg] = true
	err = executeRequestMsg.EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		return err
	}

	// If this is a "yield_request" message, then we need to convert it to an "execute_request" before resubmission.
	if executeRequestMsg.JupyterMessageType() == messaging.ShellYieldRequest {
		err = executeRequestMsg.SetMessageType(messaging.ShellExecuteRequest, true)
		if err != nil {
			d.log.Error("Failed to re-encode message header while converting \"yield_request\" message \"%s\" to \"execute_request\" before resubmitting it: %v",
				executeRequestMsg.JupyterMessageId(), err)
			return err
		}

		d.log.Debug("Successfully converted \"yield_request\" message \"%s\" to \"execute_request\" before resubmitting it.",
			executeRequestMsg.JupyterMessageId())
	}

	signatureScheme := kernel.ConnectionInfo().SignatureScheme
	if signatureScheme == "" {
		d.log.Warn("kernel %s's signature scheme is blank. Defaulting to \"%s\"", messaging.JupyterSignatureScheme)
		signatureScheme = messaging.JupyterSignatureScheme
	}

	// Regenerate the signature.
	if _, err := executeRequestMsg.JupyterFrames.Sign(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		// Ignore the error; just log it.
		d.log.Warn("Failed to sign frames because %v", err)
	}

	// Ensure that the frames are now correct.
	if err := executeRequestMsg.JupyterFrames.Verify(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v': %v",
			signatureScheme, kernel.ConnectionInfo().Key, err)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", executeRequestMsg)
		return ErrFailedToVerifyMessage
	}

	// Now, we wait for the migration operation to proceed.
	waitGroup.Wait()
	select {
	case err := <-errorChan:
		{
			// If there was an error during execution, then we'll return that error rather than proceed.
			go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Migrate Replica of kernel \"%s\"",
				kernel.ID()), err.Error())

			return err
		}
	default:
		{
			// Do nothing. The migration operation completed successfully.
		}
	}

	d.log.Debug(utils.LightBlueStyle.Render("Resubmitting 'execute_request' message targeting kernel %s now."), kernel.ID())
	err = d.ShellHandler(kernel, executeRequestMsg)

	if errors.Is(err, types.ErrKernelNotFound) {
		d.log.Error("ShellHandler couldn't identify kernel \"%s\"...", kernel.ID())

		d.kernels.Store(executeRequestMsg.DestinationId, kernel)
		d.kernels.Store(executeRequestMsg.JupyterSession(), kernel)

		kernel.BindSession(executeRequestMsg.JupyterSession())

		err = d.executeRequestHandler(kernel, executeRequestMsg)
	}

	if err != nil {
		d.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go d.notifier.NotifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

// dynamicV3FailureHandler is invoked when an "execute_request" cannot be processed when using the Dynamic v3
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) dynamicV3FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// dynamicV4FailureHandler is invoked when an "execute_request" cannot be processed when using the Dynamic v4
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) dynamicV4FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// gandivaV4FailureHandler is invoked when an "execute_request" cannot be processed when using the Gandiva
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (d *ClusterGatewayImpl) gandivaV4FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) error {
	panic("The 'GANDIVA' scheduling policy is not yet supported.")
}

func (d *ClusterGatewayImpl) NotificationCallback(notificationName string, notificationMessage string, typ messaging.NotificationType) {
	d.notifier.NotifyDashboard(notificationName, notificationMessage, typ)
}

func (d *ClusterGatewayImpl) localDaemonDisconnected(localDaemonId string, nodeName string, errorName string, errorMessage string) (err error) {
	d.log.Warn("Local Daemon %s (Node %s) has disconnected. Removing from cluster.", localDaemonId, nodeName)
	_, err = d.RemoveHost(context.TODO(), &proto.HostId{
		Id:       localDaemonId,
		NodeName: nodeName, /* Not needed */
	})

	if err != nil {
		d.log.Error("Error while removing local daemon %s (node: %s): %v", localDaemonId, nodeName, err)
	}

	go d.notifier.NotifyDashboard(errorName, errorMessage, messaging.WarningNotification)

	return err
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
	return d.deploymentMode == types.DockerSwarmMode
}

// DockerMode returns true if we're running in either "docker swarm" or "docker compose".
// That is, DockerMode turns true if and only if one of DockerSwarmMode or DockerComposeMode return true.
//
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *ClusterGatewayImpl) DockerMode() bool {
	return d.DockerComposeMode() || d.DockerSwarmMode()
}

// KubernetesMode returns true if we're running in Kubernetes.
func (d *ClusterGatewayImpl) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// startNewKernel is called by StartKernel when creating a brand-new kernel, rather than restarting an existing kernel.
func (d *ClusterGatewayImpl) initNewKernel(in *proto.KernelSpec) (scheduling.Kernel, error) {
	d.log.Debug("Did not find existing DistributedKernelClient with KernelID=\"%s\". Creating new DistributedKernelClient now.", in.Id)

	// Initialize kernel with new context.
	//kernel := d.DistributedClientProvider.NewDistributedKernelClient(context.Background(), in, d.NumReplicas(), d.id,
	//	d.connectionOptions, uuid.NewString(), d.DebugMode, d.ExecutionFailedCallback, d.ExecutionLatencyCallback,
	//	d.metricsProvider, d.NotifyDashboard)

	kernel := d.DistributedClientProvider.NewDistributedKernelClient(context.Background(), in, d.NumReplicas(), d.id,
		d.connectionOptions, uuid.NewString(), d.DebugMode, d.MetricsProvider, d)

	d.log.Debug("Initializing Shell Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)
	_, err := kernel.InitializeShellForwarder(d.kernelShellHandler)
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

	// Create a new Session for scheduling purposes.
	session := entity.NewSessionBuilder().
		WithContext(context.Background()).
		WithID(kernel.ID()).
		WithKernelSpec(in).
		WithTrainingTimeSampleWindowSize(d.ClusterOptions.ExecutionTimeSamplingWindow).
		WithMigrationTimeSampleWindowSize(d.ClusterOptions.MigrationTimeSamplingWindow).
		Build()

	d.cluster.AddSession(kernel.ID(), session)

	// Assign the Session to the DistributedKernelClient.
	kernel.SetSession(session)

	return kernel, nil
}

// scheduleReplicas actually scheduled the replicas of the specified kernel.
//
// Important: if the attemptChan argument is non-nil, then it should be a buffered channel so that the operation
// to place the scheduling.CreateReplicaContainersAttempt into it will not block. (We don't want to get stuck
// there forever in case the caller goes away for whatever reason.)
func (d *ClusterGatewayImpl) scheduleReplicas(ctx context.Context, kernel scheduling.Kernel, in *proto.KernelSpec,
	attemptChan chan<- scheduling.CreateReplicaContainersAttempt) error {

	// Check if any replicas are being migrated and, if so, then wait for them to finish being migrated.
	migrationCtx, migrateCancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer migrateCancel()

	err := kernel.WaitForMigrationsToComplete(migrationCtx)
	if err != nil {
		return err
	}

	replicasToSchedule := kernel.MissingReplicaIds()
	numReplicasToSchedule := len(replicasToSchedule)

	if numReplicasToSchedule == 0 {
		d.log.Warn("All replicas of kernel \"%s\" are already scheduled...?", kernel.ID())
		return nil
	}

	d.log.Debug("Scheduling %d replica container(s) of kernel %s.",
		numReplicasToSchedule, kernel.ID())

	var (
		startedScheduling bool
		attempt           scheduling.CreateReplicaContainersAttempt
	)

	// We'll keep executing this loop as long as the replicas of the target kernel are not scheduled.
	// We break from the loop internally if (a) we claim ownership over a container creation attempt, in which case we
	// break out so that we can orchestrate the container creation attempt, or (b) if we find that the replicas are in
	// fact scheduled. This may occur if, for example, a previous attempt concludes.
	for {
		// Try to start a new attempt at scheduling the replica container(s) of this kernel.
		startedScheduling, attempt = kernel.InitSchedulingReplicaContainersOperation()

		// If we started a new attempt, then we'll break out of the loop and orchestrate the creation of
		// the containers for the replicas of the target kernel.
		if startedScheduling {
			d.log.Debug(utils.LightBlueStyle.Render("Started attempt to schedule %d replica container(s) for kernel \"%s\"."),
				d.NumReplicas(), kernel.ID())
			break
		}

		// We didn't start a new scheduling attempt.
		// If the returned attempt is also nil, then that means that there was also not an active attempt.
		// So, the replicas are apparently already scheduled.
		if attempt == nil {
			d.log.Debug("Tried to start attempt to schedule %d replica container(s) for kernel \"%s\", but apparently they're already scheduled.",
				d.NumReplicas(), kernel.ID())

			// Double-check that the kernel's replicas are scheduled. If they are, then we'll just return entirely.
			if kernel.ReplicasAreScheduled() {
				return nil
			}

			// This would be truly bizarre, but if this occurs, then we'll just sleep briefly and then try again...
			d.log.Error("We were lead to believe that kernel %s's replicas were scheduled, but they're not...",
				kernel.ID())

			time.Sleep(time.Millisecond * (5 + time.Duration(rand.Intn(25))))
			continue
		}

		if attemptChan != nil {
			attemptChan <- attempt
		}

		// If we did not start a new attempt, then a previous attempt must still be active.
		// We'll just wait for the attempt to conclude.
		// If the scheduling is successful, then this will eventually return nil.
		// If the context passed to scheduleReplicas has a time-out, and we time out, then this will return an error.
		d.log.Debug("Found existing 'create replica containers' operation for kernel %s that began %v ago. Waiting for operation to complete.",
			kernel.ID(), attempt.TimeElapsed())
		return attempt.Wait(ctx)
	}

	if attemptChan != nil {
		attemptChan <- attempt
	}

	// Verify that the replicas aren't scheduled.
	// If we encountered an existing scheduling operation up above that we waited for and that completed successfully,
	// then the replicas may well be available now, so we can just return.
	if kernel.ReplicasAreScheduled() {
		d.log.Debug("Replicas of kernel \"%s\" are apparently scheduled now. Returning.", kernel.ID())
		return nil
	}

	scheduleReplicasStartedEvents := make(map[int32]*metrics.ClusterEvent)
	replicaRegisteredTimestamps := make(map[int32]time.Time)
	var replicaRegisteredEventsMutex sync.Mutex

	d.clusterStatisticsMutex.Lock()
	startTime := time.Now()
	for _, replicaId := range replicasToSchedule {
		event := &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.ScheduleReplicasStarted,
			KernelId:            in.Id,
			ReplicaId:           replicaId,
			Timestamp:           startTime,
			TimestampUnixMillis: startTime.UnixMilli(),
		}
		d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, event)
		scheduleReplicasStartedEvents[replicaId] = event
	}
	d.clusterStatisticsMutex.Unlock()

	// Record that this kernel is starting.
	kernelStartedChan := make(chan struct{})
	d.kernelsStarting.Store(in.Id, kernelStartedChan)
	created := newRegistrationWaitGroups(numReplicasToSchedule)
	created.AddOnReplicaRegisteredCallback(func(replicaId int32) {
		replicaRegisteredEventsMutex.Lock()
		defer replicaRegisteredEventsMutex.Unlock()

		replicaRegisteredTimestamps[replicaId] = time.Now()
	})
	d.waitGroups.Store(in.Id, created)

	err = d.cluster.Scheduler().DeployKernelReplicas(ctx, kernel, int32(numReplicasToSchedule), []scheduling.Host{ /* No blacklisted hosts */ })
	if err != nil {
		d.log.Warn("Failed to deploy kernel replica(s) for kernel \"%s\" because: %v", kernel.ID(), err)

		// Only notify if there's an "actual" error.
		if !errors.Is(err, scheduling.ErrInsufficientHostsAvailable) && !errors.As(err, &scheduling.InsufficientResourcesError{}) {
			go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Create kernel \"%s\"", in.Id), err.Error())
		}

		// Record that the container creation attempt has completed with an error (i.e., it failed).
		attempt.SetDone(err)

		return err
	}

	// Wait for all replicas to be created.
	// Note that creation just means that the Container/Pod was created.
	// It does not mean that the Container/Pod has entered the active/running state.
	d.log.Debug("Waiting for replicas of new kernel %s to register. Number of kernels starting: %d.",
		in.Id, numReplicasToSchedule)
	created.Wait()
	d.log.Debug("All %d replica(s) of new kernel %s have been created and registered with their local daemons. Waiting for replicas to join their SMR cluster startTime.",
		numReplicasToSchedule, in.Id)

	// Wait until all replicas have started.
	for i := 0; i < numReplicasToSchedule; i++ {
		<-kernelStartedChan // Wait for all replicas to join their SMR cluster.
	}

	// Clean up.
	d.kernelsStarting.Delete(in.Id)
	d.log.Debug("All %d replica(s) of kernel %s have registered and joined their SMR cluster. Number of kernels starting: %d.",
		numReplicasToSchedule, in.Id, numReplicasToSchedule)

	// Sanity check.
	if kernel.Size() == 0 {
		// Record that the container creation attempt has completed with an error (i.e., it failed).
		attempt.SetDone(client.ErrFailureUnspecified)
		return status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// Map all the sessions to the kernel client.
	for _, sess := range kernel.Sessions() {
		d.log.Debug("Storing kernel %v under session ID \"%s\".", kernel, sess)
		d.kernels.Store(sess, kernel)
	}

	// Create corresponding events for when the replicas registered.
	for replicaId, timestamp := range replicaRegisteredTimestamps {
		timeElapsed := timestamp.Sub(startTime)
		event := &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.ScheduleReplicasComplete,
			KernelId:            in.Id,
			ReplicaId:           replicaId,
			Timestamp:           timestamp,
			TimestampUnixMillis: timestamp.UnixMilli(),
			Duration:            timeElapsed,
			DurationMillis:      timeElapsed.Milliseconds(),
			Metadata: map[string]interface{}{
				"start_time_unix_millis":       startTime.UnixMilli(),
				"corresponding_start_event_id": scheduleReplicasStartedEvents[replicaId].EventId,
			},
		}
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, event)
		d.clusterStatisticsMutex.Unlock()
	}

	// Record that the container creation attempt has completed successfully.
	attempt.SetDone(nil)
	return nil
}

func (d *ClusterGatewayImpl) sendStatusMessage(kernel scheduling.Kernel, executionState string) (*messaging.JupyterMessage, error) {
	var (
		msg   zmq4.Msg
		err   error
		msgId = uuid.NewString()
	)
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageIdAndIdentity(msgId,
		messaging.IOStatusMessage, kernel.ID(), messaging.IOStatusMessage)

	content := map[string]string{
		"execution_state": executionState,
	}

	err = frames.EncodeContent(&content)
	if err != nil {
		d.log.Error("Failed to encode content of IOPub status message for kernel \"%s\": %v", kernel.ID(), err)
		return nil, err
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return nil, jupyter.ErrFailedToVerifyMessage
	}

	jMsg := messaging.NewJupyterMessage(&msg)

	d.log.Debug("Sending io/iopub message %s (JupyterID=\"%s\") encoding execution state/status of \"%s\" to client of kernel \"%s\" now: %v",
		jMsg.RequestId, jMsg.JupyterMessageId(), executionState, kernel.ID(), jMsg)

	err = kernel.(*client.DistributedKernelClient).SendIOMessage(jMsg)
	return jMsg, err
}

// sendStartingStatusIoPub is used when first starting a kernel.
//
// The Jupyter Server expects at least one IOPub message to be broadcast during the start-up procedure.
// This satisfies that requirement.
func (d *ClusterGatewayImpl) sendStartingStatusIoPub(kernel scheduling.Kernel) error {
	iopubSocket := kernel.Socket(messaging.IOMessage)
	if iopubSocket == nil {
		return fmt.Errorf("%w: IO socket", messaging.ErrSocketNotAvailable)
	}

	// Send the "starting" status now.
	msg, err := d.sendStatusMessage(kernel, "starting")
	if err != nil {
		d.log.Error("Failed to send 'starting' IOPub status message during creation of kernel \"%s\": %v",
			kernel.ID(), err)
		return err
	}

	d.log.Debug("Sent IOPub message: %v", msg)

	// Send another "status" message in ~2 seconds with the "idle" status.
	//go func(sleepInterval time.Duration) {
	//	time.Sleep(sleepInterval)
	//
	//	msg, err = d.sendStatusMessage(kernel, "idle")
	//	if err != nil {
	//		d.log.Error("Failed to send 'idle' IOPub status message after waiting %v during creation of kernel \"%s\": %v",
	//			sleepInterval, kernel.ID(), err)
	//	} else {
	//		d.log.Debug("Sent IOPub message: %v", msg)
	//	}
	//}(time.Millisecond * 2)

	return nil
}

// sendIdleStatusIoPub is used to inform the Jupyter Server that the target kernel is idle.
func (d *ClusterGatewayImpl) sendIdleStatusIoPub(kernel scheduling.Kernel) error {
	iopubSocket := kernel.Socket(messaging.IOMessage)
	if iopubSocket == nil {
		return fmt.Errorf("%w: IO socket", messaging.ErrSocketNotAvailable)
	}

	msg, err := d.sendStatusMessage(kernel, "idle")
	if err != nil {
		d.log.Error("Failed to send 'idle' IOPub status message during creation of kernel \"%s\": %v",
			kernel.ID(), err)
		return err
	}

	d.log.Debug("Sent IOPub message: %v", msg)

	return nil
}

// startLongRunningKernel runs some long-running-kernel-specific start-up code.
//
// startLongRunningKernel will return as soon as the container creation process for the long-running kernel enters
// the "placement" stage, as it is very likely to succeed at that point, but the remaining process can take anywhere
// from 15 to 45 seconds (on average).
func (d *ClusterGatewayImpl) startLongRunningKernel(ctx context.Context, kernel scheduling.Kernel, in *proto.KernelSpec) error {
	notifyChan := make(chan interface{}, 1)
	attemptChan := make(chan scheduling.CreateReplicaContainersAttempt, 1)

	// Use a separate goroutine for this step.
	go func() {
		// We pass a new/separate context, because if we end up returning all the way back to the gRPC handler (and
		// then return from there), then the scheduling operation will fail, as the context will be cancelled (when we
		// return from the gRPC handler).
		err := d.scheduleReplicas(context.Background(), kernel, in, attemptChan)

		if err == nil {
			notifyChan <- struct{}{}
			return
		}

		if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
			d.log.Warn("Insufficient hosts available to schedule replica container(s) of new kernel %s: %v",
				in.Id, err)
		} else {
			d.log.Error("Failed to schedule replica container(s) of new kernel %s at creation time: %v",
				in.Id, err)
		}

		// Set the kernel's status to KernelStatusError.
		kernel.InitialContainerCreationFailed()

		notifyChan <- err
	}()

	//handleSchedulingError := func(err error) error {
	//	d.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
	//
	//	// Clean up everything since we failed to create the long-running kernel.
	//	d.kernelIdToKernel.Delete(in.Id)
	//	d.kernelsStarting.Delete(in.Id)
	//	d.kernels.Delete(in.Id)
	//	d.kernelSpecs.Delete(in.Id)
	//	d.waitGroups.Delete(in.Id)
	//
	//	closeKernelError := kernel.Close()
	//	if closeKernelError != nil {
	//		d.log.Warn("Error while closing failed-to-be-created kernel \"%s\": %v", in.Id, closeKernelError)
	//	}
	//
	//	// The error should already be compatible with gRPC. But just in case it isn't...
	//	_, ok := status.FromError(err)
	//	if !ok {
	//		err = status.Error(codes.Internal, err.Error())
	//	}
	//
	//	return err
	//}

	var attempt scheduling.CreateReplicaContainersAttempt
	select {
	case <-ctx.Done(): // Original context passed to us from the gRPC handler.
		{
			err := ctx.Err()

			d.log.Error("gRPC context cancelled while scheduling replicas of new kernel \"%s\": %v",
				in.Id, err)

			if err != nil {
				return fmt.Errorf("%w: failed to schedule replicas of kernel \"%s\"", err, in.Id)
			}

			return fmt.Errorf("failed to schedule replicas of kernel \"%s\" because: %w",
				in.Id, types.ErrRequestTimedOut)
		}
	case v := <-notifyChan:
		{
			// If we received an error, then we already know that the operation failed (and we know why -- it is
			// whatever the error is/says), so we can just return the error.
			if err, ok := v.(error); ok {
				d.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
			}

			// Print a warning message because this is suspicious, but not necessarily indicative
			// that something is wrong. (It is really, really weird, though...)
			d.log.Warn("Received non-error response to creation of new, "+
				"long-running kernel \"%s\" before receiving attempt value...", in.Id)

			// If we receive a non-error response here, then we apparently already scheduled the replicas?
			// This is very unexpected, but technically it's possible...
			//
			// It's unexpected because the overhead of starting containers is high enough that the case in which
			// we receive the value from the attemptChan should occur first. We receive the attempt as soon as the
			// placement of the containers begins, which should be anywhere from 15 to 45 seconds before the
			// containers are fully created.
			return nil
		}
	case attempt = <-attemptChan:
		{
			break
		}
	}

	// Sanity check. We should only get to this point if the attempt was received from the attempt channel.
	if attempt == nil {
		panic("Expected scheduling.CreateReplicaContainersAttempt variable to be non-nil at this point.")
	}

	err := attempt.WaitForPlacementPhaseToBegin(ctx)
	if err != nil {
		d.log.Error("Error waiting for placement to begin during creation of new kernel \"%s\": %v", in.Id, err)
		return err
	}

	return attempt.Wait(ctx)

	//select {
	//// Check if there's already an error available, in which case we'll return it.
	//case v := <-notifyChan:
	//	{
	//		// If we received an error, then we already know that the operation failed (and we know why -- it is
	//		// whatever the error is/says), so we can just return the error. Otherwise, we just return optimistically.
	//		var ok bool
	//		if err, ok = v.(error); ok {
	//			d.log.Warn("Failed to schedule replicas of new kernel \"%s\" because: %v", in.Id, err)
	//		}
	//	}
	//default:
	//	{
	//		// No-op.
	//	}
	//}
	//
	//d.log.Debug("Placement phase began for new kernel \"%s\".", in.Id)
	//return nil
}

// StartKernel launches a new kernel.
func (d *ClusterGatewayImpl) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	startTime := time.Now()

	if in == nil {
		panic("Received nil proto.KernelSpec argument to ClusterGatewayImpl::StartKernel...")
	}

	// If the resource spec of the KernelSpec argument is non-nil, then we will "sanitize" it.
	var originalSpec *proto.ResourceSpec
	if in.ResourceSpec != nil {
		// In rare cases, the ResourceSpec will be received with certain quantities -- particularly memory -- different
		// from how they were originally sent.
		//
		// For example, there is a spec from the workload trace in which the memory is 3.908 (MB), but we receive it
		// here as "3.9079999923706055". It is still correct in the Jupyter Server and in the Gateway Provisioner (the
		// Python object), but we receive the 3.908 as 3.9079999923706055, which leads to errors.
		//
		// So, we just round everything to 3 decimal places again here, to be safe.
		originalSpec = in.ResourceSpec.Clone()
		in.ResourceSpec = &proto.ResourceSpec{
			Cpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Cpu)).Round(0).InexactFloat64()),
			Memory: float32(decimal.NewFromFloat(float64(in.ResourceSpec.Memory)).Round(3).InexactFloat64()),
			Gpu:    int32(decimal.NewFromFloat(float64(in.ResourceSpec.Gpu)).Round(0).InexactFloat64()),
			Vram:   float32(decimal.NewFromFloat(float64(in.ResourceSpec.Vram)).Round(3).InexactFloat64()),
		}
	} else {
		// Assign a default, "empty" resource spec.
		in.ResourceSpec = &proto.ResourceSpec{
			Cpu:    0,
			Memory: 0,
			Gpu:    0,
			Vram:   0,
		}
	}

	d.log.Info(
		utils.LightBlueStyle.Render(
			"↪ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, OriginalSpec=%v, Spec=%v]"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), originalSpec, in)

	// For logging/debugging purposes, we check if the rounded spec and the original spec that we received are
	// unequal. If so, we'll log a message indicating as such.
	if originalSpec != nil {
		// For logging/debugging purposes, we check if the rounded spec and the original spec that we received are
		// unequal. If so, we'll log a message indicating as such.
		if isEqual, unequalField := in.ResourceSpec.EqualsWithField(originalSpec); !isEqual {
			d.log.Warn(
				"Original ResourceSpec included in KernelSpec for new kernel \"%s\" has been rounded, and their \"%s\" fields differ.",
				in.Id, unequalField)

			d.log.Warn("Original \"%s\" field: %f. Rounded \"%s\" field: %v.",
				originalSpec.GetResourceQuantity(unequalField), in.ResourceSpec.GetResourceQuantity(unequalField))
		}
	} else {
		d.log.Warn("KernelSpec for new kernel \"%s\" did not originally contain a ResourceSpec...")
	}

	d.clusterStatisticsMutex.Lock()
	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
		EventId:             uuid.NewString(),
		Name:                metrics.KernelCreationStarted,
		KernelId:            in.Id,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()

	var (
		kernel scheduling.Kernel
		ok     bool
		err    error
	)

	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok = d.kernels.Load(in.Id)
	if !ok {
		kernel, err = d.initNewKernel(in)
		if err != nil {
			d.log.Error("Failed to create new kernel %s because: %v", in.Id, err)
			d.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, ensureErrorGrpcCompatible(err, codes.Unknown)
		}
	} else {
		d.log.Info("Restarting kernel \"%s\".", kernel.ID())
		kernel.BindSession(in.Session)

		// If we're restarting the kernel, then the resource spec being used is probably outdated.
		// So, we'll replace it with the current resource spec.
		in.ResourceSpec = proto.ResourceSpecFromSpec(kernel.ResourceSpec())
	}

	d.kernelIdToKernel.Store(in.Id, kernel)
	d.kernels.Store(in.Id, kernel)
	d.kernelSpecs.Store(in.Id, in)

	// Make sure to associate the Jupyter Session with the kernel.
	kernel.BindSession(in.Session)
	d.kernels.Store(in.Session, kernel)

	err = d.sendStartingStatusIoPub(kernel)

	if err != nil {
		d.log.Error("Failed to send IOPub status messages during start-up of kernel %s: %v", in.Id, err)
		d.log.Error(
			utils.RedStyle.Render(
				"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
			in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
		return nil, ensureErrorGrpcCompatible(err, codes.Unknown)
	}

	if d.Scheduler().Policy().ContainerLifetime() == scheduling.SingleTrainingEvent {
		d.log.Debug("Will wait to schedule container(s) for kernel %s until we receive an 'execute_request'.", in.Id)

		// Since we're not going to schedule any replicas now, we'll send an 'idle' status update in 1.5-3 seconds.
		go func() {
			time.Sleep(time.Millisecond * time.Duration(1500+rand.Intn(1500)))
			err = d.sendIdleStatusIoPub(kernel)
			if err != nil {
				d.log.Error("Failed to send 'idle' status update for new kernel \"%s\": %v", in.Id, err)
			}
		}()

		// Since we won't be adding any replicas to the kernel right now, we need to assign a value to the
		// SignatureScheme and Key fields of the connectionInfo used by the DistributedKernelClient's server.
		//
		// If we skipped this step, then the kernel would not be able to sign messages correctly.
		kernel.SetSignatureScheme(in.SignatureScheme)
		kernel.SetKernelKey(in.Key)
	} else {
		err = d.startLongRunningKernel(ctx, kernel, in)

		if err != nil {
			if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
				d.log.Warn("Failed to start long-running kernel \"%s\" due to resource contention: %v", in.Id, err)
				d.log.Warn(
					utils.OrangeStyle.Render(
						"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
					in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
				return nil, ensureErrorGrpcCompatible(err, codes.Internal)
			}

			d.log.Error("Error while starting long-running kernel \"%s\": %v", in.Id, err)
			d.log.Error(
				utils.RedStyle.Render(
					"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Failure ✗"),
				in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)
			return nil, ensureErrorGrpcCompatible(err, codes.Internal)
		}
	}

	d.log.Debug("Created and stored new DistributedKernel %s.", in.Id)

	info := &proto.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(messaging.ControlMessage).Port),
		ShellPort:       int32(kernel.GetSocketPort(messaging.ShellMessage)),
		StdinPort:       int32(d.router.Socket(messaging.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(messaging.HBMessage).Port),
		IopubPort:       int32(kernel.GetSocketPort(messaging.IOMessage)),
		IosubPort:       int32(kernel.GetSocketPort(messaging.IOMessage)),
		SignatureScheme: kernel.KernelSpec().SignatureScheme,
		Key:             kernel.KernelSpec().Key,
	}

	if kernel.ReplicasAreScheduled() {
		d.log.Info("Kernel %s started after %v: %v", kernel.ID(), time.Since(startTime), info)
	} else {
		d.log.Info("Finished initialization (but not necessarily container creation) for kernel %s after %v: %v",
			kernel.ID(), time.Since(startTime), info)
	}

	session, ok := d.cluster.GetSession(kernel.ID())
	if ok {
		p := session.SessionStarted()
		err = p.Error()
		if err != nil {
			d.notifier.NotifyDashboardOfError(fmt.Sprintf("Error Starting Session \"%s\"", kernel.ID()), err.Error())
			panic(err)
		}
	} else {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session associated with kernel \"%s\", even though that kernel just started running successfully...", kernel.ID())
		d.log.Error(errorMessage)
		d.notifier.NotifyDashboardOfError("Session Not Found", errorMessage)
		panic(errorMessage)
	}

	d.registerKernelWithExecReqForwarder(kernel)

	d.newKernelCreated(startTime, kernel.ID())

	d.log.Info("Returning from ClusterGatewayImpl::StartKernel for kernel %s after %v:\n%v",
		kernel.ID(), time.Since(startTime), info.PrettyString())

	d.log.Info(
		utils.DarkGreenStyle.Render(
			"↩ ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%s, Spec=%v] Success ✓"),
		in.Id, in.Session, in.ResourceSpec.ToDecimalSpec().String(), in)

	return info, nil
}

func (d *ClusterGatewayImpl) registerKernelWithExecReqForwarder(kernel scheduling.Kernel) {
	// Create a wrapper around the kernel's RequestWithHandlerAndReplicas method.
	forwarder := func(ctx context.Context, op string, typ messaging.MessageType, jupyterMessages []*messaging.JupyterMessage,
		handler scheduling.KernelReplicaMessageHandler, done func()) error {
		return kernel.RequestWithHandlerAndReplicas(ctx, op, typ, jupyterMessages, handler, done, kernel.Replicas()...)
	}

	d.executeRequestForwarder.RegisterKernel(kernel, forwarder, d.kernelReplicaResponseForwarder)
}

// newKernelCreated is to be called from StartKernel if and when the procedure succeeds.
//
// newKernelCreated pushes some metrics to Kubernetes and sends a notification to the Dashboard.
func (d *ClusterGatewayImpl) newKernelCreated(startTime time.Time, kernelId string) {
	// Tell the Dashboard that the kernel has successfully started running.
	go d.notifier.NotifyDashboard("kernel Started", fmt.Sprintf("kernel %s has started running. Launch took approximately %v from when the cluster Gateway began processing the 'create kernel' request.",
		kernelId, time.Since(startTime)), messaging.SuccessNotification)

	numActiveKernels := d.numActiveKernels.Add(1)

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.NumIdleSessions.Add(1)

	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
		EventId:             uuid.NewString(),
		Name:                metrics.KernelCreationComplete,
		KernelId:            kernelId,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()

	if d.MetricsProvider.PrometheusMetricsEnabled() {
		d.MetricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))

		d.MetricsProvider.GetGatewayPrometheusManager().TotalNumKernelsCounterVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Inc()

		d.MetricsProvider.GetGatewayPrometheusManager().KernelCreationLatencyHistogram.
			Observe(float64(time.Since(startTime).Milliseconds()))
	}
}

// handleMigratedReplicaRegistered is called by NotifyKernelRegistered to handle the registration of a
// scheduling.KernelReplica that was created during a migration operation. as opposed to the scheduling.KernelReplica
//
// The alternative(s) to the scenarios described above is/are when the scheduling.KernelReplica that is registering is
// being created for the first time or as an on-demand replica to serve a single training event when using scheduling
// policies that are configured to use this approach. In either of these alternative scenarios, the registration of the
// scheduling.KernelReplica is handled by the handleStandardKernelReplicaRegistration function.
func (d *ClusterGatewayImpl) handleMigratedReplicaRegistered(in *proto.KernelRegistrationNotification, kernel scheduling.Kernel) (*proto.KernelRegistrationNotificationResponse, error) {
	waitGroup, loaded := d.waitGroups.Load(in.KernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing primarSemaphore associated with kernel with ID %s", in.KernelId))
	}

	// We load-and-delete the entry so that, if we migrate the same replica again in the future, then we can't load
	// the old AddReplicaOperation struct...
	key := fmt.Sprintf("%s-%d", in.KernelId, in.ReplicaId)
	addReplicaOp, ok := d.Scheduler().GetAddReplicaOperationManager().LoadAndDelete(key)

	if !ok {
		errorMessage := fmt.Errorf("could not find AddReplicaOperation struct under key \"%s\"", key)
		d.log.Error(errorMessage.Error())
		d.notifier.NotifyDashboardOfError("kernel Registration Error", errorMessage.Error())

		return nil, errorMessage
	}

	if d.DockerMode() {
		dockerContainerId := in.DockerContainerId
		if dockerContainerId == "" {
			d.log.Error("kernel registration notification did not contain docker container ID: %v", in)
			go d.notifier.NotifyDashboardOfError("Missing Docker Container ID in kernel Registration Notification",
				fmt.Sprintf("kernel registration notification for replica %d of kernel \"%s\" did not contain a valid Docker container ID",
					in.ReplicaId, in.KernelId))
		}

		addReplicaOp.SetContainerName(dockerContainerId)
	}

	host, loaded := d.cluster.GetHost(in.HostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", in.HostId)) // TODO(Ben): Handle gracefully.
	}

	// The replica spec that was specifically prepared for the new replica during the initiation of the migration operation.
	replicaSpec := addReplicaOp.KernelSpec()
	addReplicaOp.SetReplicaHostname(in.KernelIp)
	addReplicaOp.SetReplicaStarted()

	if in.NodeName == "" {
		if !d.DockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		in.NodeName = host.GetID()
	}

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec,
		jupyter.ConnectionInfoFromKernelConnectionInfo(in.ConnectionInfo),
		d.id, d.numResendAttempts, in.PodOrContainerName, in.NodeName,
		nil, nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), in.HostId,
		host, metrics.ClusterGateway, true, true, d.DebugMode, d.MetricsProvider,
		d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection, d.UpdateClusterStatistics,
		d.SubmitExecuteRequestsOneAtATime, scheduling.StandardContainer)

	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	session, ok := d.cluster.GetSession(in.KernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", in.SessionId)
		d.log.Error(errorMessage)
		d.notifier.NotifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the kernel.
	replica.SetContainer(container)

	// AddHost the Container to the Host.
	d.log.Debug("Adding scheduling.Container for replica %d of kernel %s onto Host %s",
		replicaSpec.ReplicaId, addReplicaOp.KernelId(), host.GetID())
	if err = host.ContainerStartedRunningOnHost(container); err != nil {
		d.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		d.notifier.NotifyDashboardOfError("Failed to Place Container onto Host", err.Error())
		panic(err)
	}

	// Register the Container with the Session.
	//d.log.Debug("Registering/adding Container for replica %d of kernel %s with the associated Session during migration",
	//	replicaSpec.ReplicaId, addReplicaOp.KernelId())

	//err = session.AddReplica(container)
	//if err != nil {
	//	if errors.Is(err, entity.ErrInvalidContainer) {
	//		d.log.Error("Error while registering container %v with session %v during migration:\n%v", container, session, err)
	//	} else {
	//		d.log.Error("Unexpected error while registering container %v with session %v during migration:\n%v", container, session, err)
	//	}
	//
	//	go d.notifier.NotifyDashboardOfError("Failed to Register Container with Session During Migration", err.Error())
	//
	//	return nil, status.Error(codes.Internal, err.Error())
	//}
	//
	//d.log.Debug("Adding replica for kernel %s, replica %d on host %s during migration. Resource spec: %v", addReplicaOp.KernelId(), replicaSpec.ReplicaId, host.GetID(), replicaSpec.Kernel.ResourceSpec)
	//err = kernel.AddReplica(replica, host)
	//if err != nil {
	//	panic(fmt.Sprintf("kernel::AddReplica call failed during migration: %v", err)) // TODO(Ben): Handle gracefully.
	//}

	// d.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaID, in.kernelId, waitGroup.NumReplicas())

	// Store the new replica in the list of replicas for the kernel (at the correct position, based on the SMR node ID).
	// Then, return the list of replicas so that we can pass it to the new replica.
	// updatedReplicas := waitGroup.UpdateAndGetReplicasAfterMigration(migrationOperation.OriginalSMRNodeID()-1, in.KernelIp)
	updatedReplicas := waitGroup.AddReplica(replicaSpec.ReplicaId, in.KernelIp)

	persistentId := addReplicaOp.PersistentID()
	// dataDirectory := addReplicaOp.DataDirectory()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                              replicaSpec.ReplicaId,
		Replicas:                        updatedReplicas,
		PersistentId:                    &persistentId,
		ShouldReadDataFromRemoteStorage: d.shouldKernelReplicaReadStateFromRemoteStorage(kernel, true),
		ResourceSpec:                    replicaSpec.Kernel.ResourceSpec,
		SmrPort:                         int32(d.smrPort),
	}

	// d.mu.Unlock()

	d.log.Debug("Sending notification that replica %d of kernel \"%s\" has registered during migration \"%s\".",
		replicaSpec.ReplicaId, in.KernelId, addReplicaOp.OperationID())

	err = addReplicaOp.SetReplicaRegistered(replica)
	if err != nil {
		errorMessage := fmt.Sprintf("We're using the WRONG AddReplicaOperation... AddReplicaOperation \"%s\" has already recorded that its replica has registered: %v",
			addReplicaOp.OperationID(), addReplicaOp.String())
		d.log.Error(errorMessage)
		d.notifier.NotifyDashboardOfError("Using Incorrect AddReplicaOperation", errorMessage)
		panic(err)
	}

	//d.log.Debug("About to issue 'update replica' request for replica %d of kernel %s. Client ready: %v", replicaSpec.ReplicaId, in.KernelId, replica.IsReady())
	//
	//if d.cluster.Scheduler().Policy().NumReplicas() > 1 {
	//	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)
	//}

	kernel.RecordContainerCreated(in.WasPrewarmContainer)

	d.log.Debug("SetDone handling registration of added replica %d of kernel %s.", replicaSpec.ReplicaId, in.KernelId)

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
	// - AddHost a label to that node to ensure the new replica is scheduled onto that node.
	// - Increase the size of the associated kernel's CloneSet.

	kernelSpec, ok := d.kernelSpecs.Load(in.Id)
	if !ok {
		d.log.Error("Could not load kernel spec associated with kernel %s", in.Id)
		return ErrKernelSpecNotFound
	}

	resourceSpec := kernelSpec.GetResourceSpec()
	if resourceSpec == nil {
		d.log.Error("kernel %s does not have a resource spec included with its kernel spec.", in.Id)
		return ErrResourceSpecNotFound
	}

	// Identify a target node with sufficient resources to serve an execution request for the associated kernel.

	// AddHost a label to that node to ensure the new replica is scheduled onto that node.

	// Increase the size of the associated kernel's CloneSet.

	return nil
}

func (d *ClusterGatewayImpl) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	d.log.Info("Received kernel registration notification for replica %d of kernel %s from host %s (ID=%s).",
		in.ReplicaId, in.KernelId, in.NodeName, in.HostId)

	kernelId := in.KernelId

	d.log.Info("Connection info: %v", in.ConnectionInfo)
	d.log.Info("Session ID: %v", in.SessionId)
	d.log.Info("kernel ID: %v", kernelId)
	d.log.Info("Replica ID: %v", in.ReplicaId)
	d.log.Info("kernel IP: %v", in.KernelIp)
	d.log.Info("Node ID: %v", in.HostId)
	d.log.Info("Node Name: %v", in.NodeName)
	d.log.Info("Notification ID: %v", in.NotificationId)
	d.log.Info("Container name: %v", in.PodOrContainerName)

	d.clusterStatisticsMutex.Lock()
	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
		EventId:             uuid.NewString(),
		Name:                metrics.KernelReplicaRegistered,
		KernelId:            kernelId,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()

	_, loaded := d.kernelRegisteredNotifications.LoadOrStore(in.NotificationId, in)
	if loaded {
		d.log.Warn("Received duplicate \"kernel Registered\" notification with ID=%s", in.NotificationId)

		go d.notifier.NotifyDashboardOfWarning("Received Duplicate \"Kernel Registered\" Notification",
			fmt.Sprintf("NotificationID=\"%s\", KernelID=\"%s\"", in.NotificationId, in.KernelId))

		return nil, status.Error(codes.InvalidArgument, types.ErrDuplicateRegistrationNotification.Error())
	}

	// d.mu.Lock()

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		d.log.Error("Could not find kernel with ID \"%s\"; however, just received 'kernel registered' notification for that kernel...", kernelId)

		title := fmt.Sprintf("Unknown Kernel \"%s\" Specified by 'Kernel Registered' Notification", kernelId)
		go d.notifier.NotifyDashboardOfError(title, "The cluster Gateway has no record of the referenced kernel.")

		// d.mu.Unlock()
		return nil, fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, kernelId)
	}

	numActiveMigrationOperations := kernel.NumActiveMigrationOperations()
	if numActiveMigrationOperations >= 1 && kernel.IsActivelyMigratingReplica(in.ReplicaId) {
		d.log.Debug("There is/are %d active add-replica operation(s) targeting kernel %s. "+
			"Assuming currently-registering replica is for an add-replica operation.",
			numActiveMigrationOperations, kernel.ID())

		// Must be holding the main mutex before calling handleMigratedReplicaRegistered.
		// It will release the lock.
		result, err := d.handleMigratedReplicaRegistered(in, kernel)

		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return result, err
	}

	return d.handleStandardKernelReplicaRegistration(ctx, kernel, in)
}

// handleStandardKernelReplicaRegistration is called to handle the registration of a scheduling.KernelReplica that is
// either being created for the very first time or as an on-demand replica to handle a single training event.
//
// The alternative to the scenarios described above is when the scheduling.KernelReplica that is registering was
// created by/during a migration operation, in which case the registration is handled by the
// handleMigratedReplicaRegistered function.
func (d *ClusterGatewayImpl) handleStandardKernelReplicaRegistration(ctx context.Context, kernel scheduling.Kernel,
	in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {

	// Load the 'registrationWaitGroup' struct created for this kernel's creation.
	waitGroup, loaded := d.waitGroups.Load(in.KernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing primarSemaphore associated with kernel with ID %s", in.KernelId))
	}

	connectionInfo := in.ConnectionInfo
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	replicaId := in.ReplicaId

	kernelSpec, loaded := d.kernelSpecs.Load(kernelId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing kernel spec for kernel with ID %s", kernelId))
	}

	host, loaded := d.cluster.GetHost(hostId)
	if !loaded {
		host, enabled, err := d.cluster.GetHostEvenIfDisabled(hostId)
		if err != nil {
			d.log.Error("Expected to find existing Host (enabled or disabled) with ID \"%v\": %v", hostId, err)
			panic(err)
		}

		if !enabled {
			d.log.Error("Registering replica %d of kernel %s on disabled host %s (ID=%s)...",
				replicaId, kernelId, host.GetNodeName(), hostId)
		} else {
			panic("what is going on")
		}

		errorTitle := fmt.Sprintf("Received Registration from Replica %d of Kernel \"%s\" On DISABLED Host %s (ID=%s)",
			replicaId, kernelId, host.GetNodeName(), hostId)
		d.notifier.NotifyDashboardOfError(errorTitle, "")

		return nil, status.Error(codes.Internal, fmt.Errorf("%w: cannot register kernel replica", scheduling.ErrHostDisabled).Error())
	}

	// If this is the first replica we're registering, then its ID should be 1.
	// The size will be 0, so we'll assign it a replica ID of 0 + 1 = 1.
	if replicaId == -1 {
		replicaId = int32(kernel.Size()) + 1
		d.log.Debug("kernel does not already have a replica ID assigned to it. Assigning ID: %d.", replicaId)
	}

	// We're registering a new replica, so the number of replicas is based on the cluster configuration.
	replicaSpec := &proto.KernelReplicaSpec{
		Kernel:      kernelSpec,
		ReplicaId:   replicaId,
		NumReplicas: int32(d.NumReplicas()),
		WorkloadId:  kernelSpec.WorkloadId,
	}

	nodeName := in.NodeName

	if nodeName == "" || nodeName == types.DockerNode {
		if !d.DockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		nodeName = host.GetID()
	}

	d.log.Debug("Creating new KernelReplicaClient for replica %d of kernel %s now...", in.ReplicaId, in.KernelId)

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec,
		jupyter.ConnectionInfoFromKernelConnectionInfo(connectionInfo), d.id,
		d.numResendAttempts, in.PodOrContainerName, nodeName, nil,
		nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), hostId, host, metrics.ClusterGateway,
		true, true, d.DebugMode, d.MetricsProvider, d.kernelReconnectionFailed,
		d.kernelRequestResubmissionFailedAfterReconnection, d.UpdateClusterStatistics,
		d.SubmitExecuteRequestsOneAtATime, scheduling.StandardContainer)

	session, ok := d.cluster.GetSession(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", kernelId)
		d.log.Error(errorMessage)
		d.notifier.NotifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the kernel.
	replica.SetContainer(container)

	// Register the Container with the Session.
	if err := session.AddReplica(container); err != nil {
		d.log.Error("Error while registering container %v with session %v: %v", container, session, err)
		go d.notifier.NotifyDashboardOfError("Failed to Register Container with Session", err.Error())

		// TODO: Handle this more gracefully.
		return nil, err
	}

	// d.mu.Unlock() // Need to unlock before calling ContainerStartedRunningOnHost, or deadlock can occur.

	// AddHost the Container to the Host.
	if err := host.ContainerStartedRunningOnHost(container); err != nil {
		d.log.Error("Error while placing container %v onto host %v: %v", container, host, err)
		go d.notifier.NotifyDashboardOfError("Failed to Place Container onto Host", err.Error())

		// TODO: Handle this more gracefully.
		return nil, err
	}

	d.log.Debug("Validating new kernel for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	if val := ctx.Value(SkipValidationKey); val == nil {
		err := replica.Validate()
		if err != nil {
			d.log.Error("kernel::Validate call failed: %v", err)
			go d.notifier.NotifyDashboardOfError(fmt.Sprintf("kernel::Validate call failed for replica %d of kernel %s",
				replica.ReplicaID(), in.KernelId), err.Error())

			// TODO: Handle this more gracefully.
			return nil, err
		}
	} else {
		d.log.Warn("Skipping validation and establishment of actual network connections with newly-registered replica %d of kernel %s.",
			replica.ReplicaID(), in.KernelId)
	}

	d.log.Debug("Adding Replica for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := kernel.AddReplica(replica, host)
	if err != nil {
		d.log.Error("kernel::AddReplica call failed: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register(replicaId)
	d.log.Debug("SetDone registering kernel for kernel %s, replica %d on host %s. Resource spec: %v",
		kernelId, replicaId, hostId, kernelSpec.ResourceSpec)
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                              replicaId,
		Replicas:                        waitGroup.GetReplicas(),
		PersistentId:                    &persistentId,
		ResourceSpec:                    kernelSpec.ResourceSpec,
		SmrPort:                         int32(d.smrPort), // The kernel should already have this info, but we'll send it anyway.
		ShouldReadDataFromRemoteStorage: d.shouldKernelReplicaReadStateFromRemoteStorage(kernel, false),
		Ok:                              true,
	}

	d.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v",
		kernelId, replicaId, response)

	kernel.RecordContainerCreated(in.WasPrewarmContainer)

	waitGroup.Notify()
	return response, nil
}

// shouldKernelReplicaReadStateFromRemoteStorage is called by NotifyKernelRegistered and is used to determine whether
// the scheduling.KernelReplica that is registering should read/restore state from remote storage or not.
//
// The basis for this decision depends on several factors.
//
// First of all, if the scheduling policy uses short-lived containers that are created on-demand for each training
// event, then they should always restore state from intermediate remote_storage.
//
// Next, for policies that use long-lived containers (regardless of the number of replicas), the kernel should restore
// state if the kernel replicas are being recreated following an idle kernel/session reclamation.
//
// Finally, when using policy.WarmContainerPoolPolicy, scheduling.KernelReplica instances should retrieve state from
// remote storage if this is not the very first time that they're being created.
func (d *ClusterGatewayImpl) shouldKernelReplicaReadStateFromRemoteStorage(kernel scheduling.Kernel, forMigration bool) bool {
	policy := d.Scheduler().Policy()

	// If the scheduling policy uses short-lived containers that are created on-demand for each training event,
	// then they should always restore state from intermediate remote_storage.
	if policy.ContainerLifetime() == scheduling.SingleTrainingEvent {
		return true
	}

	// If the kernel replica that is registering was created for a migration operation, then it should read and
	// restore its state from intermediate remote_storage.
	if forMigration {
		return true
	}

	// For policies that use long-lived containers (regardless of the number of replicas), the kernel should restore
	// state if the kernel replicas are being recreated following an idle kernel/session reclamation.
	if kernel.IsIdleReclaimed() {
		return true
	}

	// When using policy.WarmContainerPoolPolicy, scheduling.KernelReplica instances should retrieve state from remote
	// remote_storage if this is not the very first time that they're being created. We can test for this by checking if the
	// total number of containers created for this kernel is greater than zero.
	//
	// We also need to check if the number of containers created for this kernel is greater than or equal to the number
	// of replicas mandated by the scheduling policy. Although this isn't supported at the time of writing this, if we
	// enable warm container reuse by (e.g.,) the Static policy, then we'll be creating 3 containers when the kernel is
	// first created. We don't want the second or third replica to attempt to read state from remote storage when they
	// are being created for the first time. So, it's only once we've created at least as many containers as there are
	// replicas of an individual kernel that a new kernel replica should attempt to read state from remote storage.
	//
	// For single-replica policies, like the WarmContainerPoolPolicy, this logic will still work appropriately.
	if policy.ReuseWarmContainers() && kernel.NumContainersCreated() > 0 && kernel.NumContainersCreated() >= int32(d.NumReplicas()) {
		return true
	}

	return false
}

// PingGateway is a no-op for testing connectivity.
func (d *ClusterGatewayImpl) PingGateway(_ context.Context, in *proto.Void) (*proto.Void, error) {
	return in, nil
}

// ForceLocalDaemonToReconnect is used to tell a Local Daemon to reconnect to the Cluster Gateway.
// This is mostly used for testing/debugging the reconnection process.
func (d *ClusterGatewayImpl) ForceLocalDaemonToReconnect(_ context.Context, in *proto.ForceLocalDaemonToReconnectRequest) (*proto.Void, error) {
	daemonId := in.LocalDaemonId

	host, ok := d.cluster.GetHost(daemonId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, scheduling.ErrHostNotFound.Error())
	}

	_, err := host.ReconnectToGateway(context.Background(), &proto.ReconnectToGatewayRequest{
		Delay: in.Delay,
	})

	if err != nil {
		d.log.Error("Error while instruction Local Daemon %s to reconnect to us: %v", daemonId, err)

		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return nil, err
	}

	return &proto.Void{}, nil
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

	// Make sure the kernel IDs types match (if the caller specified a Jupyter kernel ID).
	if in.KernelId != "" && in.KernelId != wrapper.KernelId {
		d.log.Warn("Found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
		return nil, status.Errorf(codes.InvalidArgument,
			"found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
	}

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
		SchedulingPolicy: string(d.policyKey),
		NumReplicas:      int32(d.NumReplicas()),
	}

	return resp, nil
}

func (d *ClusterGatewayImpl) StartKernelReplica(_ context.Context, _ *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	d.log.Debug("StartKernelReplica has been instructed to StartKernel. This is actually not supported/implemented.")

	return nil, ErrNotSupported
}

func (d *ClusterGatewayImpl) GetKernel(kernelId string) (scheduling.Kernel, bool) {
	return d.kernels.Load(kernelId)
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
func (d *ClusterGatewayImpl) KillKernel(ctx context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	d.log.Debug("KillKernel RPC called for kernel %s.", in.Id)

	// Call the impl rather than the RPC stub.
	ret, err = d.stopKernelImpl(ctx, in)

	if _, ok := status.FromError(err); !ok {
		err = status.Error(codes.Internal, err.Error())
	}

	if err == nil {
		numActiveKernels := d.numActiveKernels.Add(-1)

		session := d.cluster.RemoveSession(in.Id)

		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumStoppedSessions.Add(1)

		if session != nil {
			lifetimeSeconds := time.Since(session.StartedAt()).Seconds()
			d.ClusterStatistics.AggregateSessionLifetimeSec.Add(lifetimeSeconds)
			d.ClusterStatistics.AggregateSessionLifetimesSec = append(d.ClusterStatistics.AggregateSessionLifetimesSec, lifetimeSeconds)
		}

		now := time.Now()
		d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelStopped,
			KernelId:            in.Id,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})

		d.clusterStatisticsMutex.Unlock()

		if d.MetricsProvider.PrometheusMetricsEnabled() {
			d.MetricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
				With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
				Set(float64(numActiveKernels))
		}
	}

	return ret, err
}

// GetId returns the ID of the ClusterGatewayImpl.
func (d *ClusterGatewayImpl) GetId() string {
	return d.id
}

func (d *ClusterGatewayImpl) stopKernelImpl(ctx context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find kernel %s; cannot stop kernel.", in.GetId())
		return nil, types.ErrKernelNotFound
	}

	restart := false
	if in.Restart != nil {
		restart = *in.Restart
	}
	d.log.Info("Stopping %v, restart=%v", kernel, restart)
	ret = proto.VOID

	notifyChan := make(chan interface{}, 1)

	go func() {
		err = kernel.Shutdown(d.cluster.Placer().Reclaim, restart)
		if err != nil && errors.Is(err, client.ErrAlreadyShuttingDown) {
			notifyChan <- err
			return
		}

		err = d.errorf(err)
		if err != nil {
			d.log.Warn("Failed to close kernel: %s", err.Error())
			notifyChan <- err
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

			d.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}

		notifyChan <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		{
			d.log.Error("Context cancelled while deleting kernel \"%s\": %v", in.Id, ctx.Err())
			return proto.VOID, ctx.Err()
		}
	case v := <-notifyChan:
		{
			var ok bool
			if err, ok = v.(error); !ok {
				break
			}

			if errors.Is(err, client.ErrAlreadyShuttingDown) {
				d.log.Warn("Kernel \"%s\" is already in the process of shutting down...", kernel.ID())
				return proto.VOID, err
			}

			d.log.Error("Failed to stop kernel \"%s\": %v", in.Id, err)
			return proto.VOID, err
		}
	}

	d.log.Debug("Finished deleting kernel %s.", kernel.ID())

	if !restart && d.KubernetesMode() /* Only delete CloneSet if we're in Kubernetes mode */ {
		d.log.Debug("Deleting CloneSet of deleted kernel %s now.", kernel.ID())

		// Delete the CloneSet.
		err = d.kubeClient.DeleteCloneset(kernel.ID())

		if err != nil {
			d.log.Error("Error encountered while deleting k8s CloneSet for kernel %s: %v", kernel.ID(), err)
		} else {
			d.log.Debug("Successfully deleted k8s CloneSet of deleted kernel %s.", kernel.ID())
		}
	}

	if err == nil {
		go d.notifier.NotifyDashboard(
			"kernel Stopped", fmt.Sprintf("kernel %s has been terminated successfully.",
				kernel.ID()), messaging.SuccessNotification)

		if d.MetricsProvider.PrometheusMetricsEnabled() {
			d.MetricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
				With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
				Sub(1)
		}
	} else {
		go d.notifier.NotifyDashboardOfError("Failed to Terminate kernel",
			fmt.Sprintf("An error was encountered while trying to terminate kernel %s: %v.", kernel.ID(), err))
	}

	stopped := d.executeRequestForwarder.UnregisterKernel(in.Id)
	if !stopped {
		d.log.Warn("Failed to stop 'execute_request' forwarder for kernel \"%s\"...", in.Id)
	}

	return
}

// StopKernel stops a kernel.
func (d *ClusterGatewayImpl) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	d.log.Debug("StopKernel RPC called for kernel %s.", in.Id)

	ret, err := d.stopKernelImpl(ctx, in)
	if err != nil {
		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return ret, err
	}

	numActiveKernels := d.numActiveKernels.Add(-1)

	session := d.cluster.RemoveSession(in.Id)

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.NumStoppedSessions.Add(1)

	if session != nil {
		lifetimeSeconds := time.Since(session.StartedAt()).Seconds()
		d.ClusterStatistics.AggregateSessionLifetimeSec.Add(lifetimeSeconds)
		d.ClusterStatistics.AggregateSessionLifetimesSec = append(d.ClusterStatistics.AggregateSessionLifetimesSec, lifetimeSeconds)
	}

	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
		EventId:             uuid.NewString(),
		Name:                metrics.KernelStopped,
		KernelId:            in.Id,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})

	d.clusterStatisticsMutex.Unlock()

	if d.MetricsProvider.PrometheusMetricsEnabled() {
		d.MetricsProvider.GetGatewayPrometheusManager().
			NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))
	}

	return ret, nil
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
	var logFunc func(format string, args ...interface{})
	if in.NotificationType == int32(messaging.ErrorNotification) {
		logFunc = d.log.Error
	} else if in.NotificationType == int32(messaging.WarningNotification) {
		logFunc = d.log.Warn
	} else {
		logFunc = d.log.Debug
	}

	logFunc(utils.NotificationStyles[in.NotificationType].Render("Received %s notification \"%s\": %s"),
		NotificationTypeNames[in.NotificationType], in.Title, in.Message)
	go d.notifier.NotifyDashboard(in.Title, in.Message, messaging.NotificationType(in.NotificationType))
	return proto.VOID, nil
}

func (d *ClusterGatewayImpl) SpoofNotifications(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	go func() {
		d.notifier.NotifyDashboard("Spoofed Error",
			"This is a made-up error message sent by the internalCluster Gateway.",
			messaging.ErrorNotification)
		d.notifier.NotifyDashboard("Spoofed Warning",
			"This is a made-up warning message sent by the internalCluster Gateway.",
			messaging.WarningNotification)
		d.notifier.NotifyDashboard("Spoofed Info Notification",
			"This is a made-up 'info' message sent by the internalCluster Gateway.",
			messaging.InfoNotification)
		d.notifier.NotifyDashboard("Spoofed Success Notification",
			"This is a made-up 'success' message sent by the internalCluster Gateway.",
			messaging.SuccessNotification)
	}()

	return proto.VOID, nil
}

// ClusterGateway implementation.

// ID returns the unique ID of the provisioner.
func (d *ClusterGatewayImpl) ID(_ context.Context, _ *proto.Void) (*proto.ProvisionerId, error) {
	d.log.Debug("Returning ID for RPC. ID=%s", d.id)
	return &proto.ProvisionerId{Id: d.id}, nil
}

func (d *ClusterGatewayImpl) RemoveHost(_ context.Context, in *proto.HostId) (*proto.Void, error) {
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

	d.cluster.RangeOverHosts(func(hostId string, host scheduling.Host) (contd bool) {
		data, err := host.GetActualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve actual GPU info from Local Daemon %s on node %s because: %v", hostId, host.GetNodeName(), err)
			resp.GpuInfo[host.GetNodeName()] = nil
		} else {
			resp.GpuInfo[host.GetNodeName()] = data
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

	d.cluster.RangeOverHosts(func(hostId string, host scheduling.Host) (contd bool) {
		data, err := host.GetVirtualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve virtual GPU info from Local Daemon %s on node %s because: %v", hostId, host.GetNodeName(), err)
			resp.GpuInfo[host.GetNodeName()] = nil
		} else {
			resp.GpuInfo[host.GetNodeName()] = data
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
	var targetHost scheduling.Host
	d.log.Debug("We currently have %d LocalDaemons connected.", d.cluster.Len())
	d.cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		if host.GetNodeName() == in.GetKubernetesNodeName() {
			d.log.Debug("Found LocalDaemon running on target node %s.", in.KubernetesNodeName)
			targetHost = host
			return false // Stop looping.
		} else {
			d.log.Debug("LocalDaemon %s is running on different node: %s.", host.GetID(), host.GetNodeName())
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

func (d *ClusterGatewayImpl) GetKubernetesNodes() ([]corev1.Node, error) {
	if d.DockerMode() {
		return make([]corev1.Node, 0), types.ErrIncompatibleDeploymentMode
	}

	return d.kubeClient.GetKubernetesNodes()
}

func (d *ClusterGatewayImpl) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	startTime := time.Now()
	replicaInfo := in.TargetReplica
	targetNodeId := in.GetTargetNodeId()

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
		EventId:             uuid.NewString(),
		Name:                metrics.KernelMigrationStarted,
		KernelId:            replicaInfo.KernelId,
		ReplicaId:           replicaInfo.ReplicaId,
		Timestamp:           startTime,
		TimestampUnixMillis: startTime.UnixMilli(),
		Metadata:            map[string]interface{}{"target_node_id": targetNodeId},
	})
	d.clusterStatisticsMutex.Unlock()

	kernel, loaded := d.kernels.Load(replicaInfo.KernelId)
	if !loaded {
		d.log.Error("Could not find target of migration, kernel \"%s\"", replicaInfo.KernelId)
		go d.notifier.NotifyDashboardOfError("Cannot Migrate kernel.",
			fmt.Sprintf("Cannot find kernel with ID=%s.", replicaInfo.KernelId))
		return nil, fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, replicaInfo.KernelId)
	}

	kernelReplica, err := kernel.GetReplicaByID(replicaInfo.ReplicaId)
	if err != nil {
		d.log.Error("kernel %s does not have a replica with SMR node ID of %d. Cannot migrate.",
			replicaInfo.KernelId, replicaInfo.ReplicaId)
		go d.notifier.NotifyDashboardOfError("Cannot Migrate kernel.",
			fmt.Sprintf("kernel %s does not have a replica with ID=%d.",
				replicaInfo.KernelId, replicaInfo.ReplicaId))
		return nil, err
	}

	resp, reason, err := d.cluster.Scheduler().MigrateKernelReplica(ctx, kernelReplica, targetNodeId, in.ForTraining,
		in.CanCreateNewHost)

	duration := time.Since(startTime)
	if err != nil || reason != nil {
		targetNodeIdForLogging := "unspecified"
		if resp != nil && resp.NewNodeId != "" && resp.NewNodeId != " " {
			targetNodeIdForLogging = resp.NewNodeId
		} else if targetNodeId != "" {
			targetNodeIdForLogging = targetNodeId
		} else {
			targetNodeId = in.GetTargetNodeId()
		}

		if reason != nil { // simply couldn't migrate the container, presumably due to insufficient resources available
			d.log.Warn("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
				replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeIdForLogging, duration, reason.Error())
		} else { // actual error
			d.log.Error("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
				replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeIdForLogging, duration, err)
		}

		if d.MetricsProvider.PrometheusMetricsEnabled() {
			d.MetricsProvider.GetGatewayPrometheusManager().NumFailedMigrations.Inc()
		}

		d.clusterStatisticsMutex.Lock()

		d.ClusterStatistics.NumFailedMigrations.Add(1)

		now := time.Now()
		d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelMigrationComplete,
			KernelId:            replicaInfo.KernelId,
			ReplicaId:           replicaInfo.ReplicaId,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
			Duration:            duration,
			DurationMillis:      duration.Milliseconds(),
			Metadata:            map[string]interface{}{"target_node_id": targetNodeIdForLogging, "succeeded": "true"},
		})
		d.clusterStatisticsMutex.Unlock()

		if !errors.Is(reason, scheduling.ErrMigrationFailed) {
			reason = errors.Join(scheduling.ErrMigrationFailed, reason)
		}

		return nil, reason
	} else {
		d.log.Debug("Migration operation of replica %d of kernel %s to target node %s completed successfully after %v.",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration)

		if d.MetricsProvider.PrometheusMetricsEnabled() {
			d.MetricsProvider.GetGatewayPrometheusManager().NumSuccessfulMigrations.Inc()
		}

		d.clusterStatisticsMutex.Lock()

		d.ClusterStatistics.NumSuccessfulMigrations.Add(1)

		now := time.Now()
		d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelMigrationComplete,
			KernelId:            replicaInfo.KernelId,
			ReplicaId:           replicaInfo.ReplicaId,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
			Duration:            duration,
			DurationMillis:      duration.Milliseconds(),
			Metadata:            map[string]interface{}{"target_node_id": targetNodeId, "succeeded": "false"},
		})
		d.clusterStatisticsMutex.Unlock()
	}

	if d.MetricsProvider.PrometheusMetricsEnabled() {
		d.MetricsProvider.GetGatewayPrometheusManager().KernelMigrationLatencyHistogram.Observe(float64(duration.Milliseconds()))
	}

	// If there was an error, then err will be non-nil.
	// If there was no error, then err will be nil.
	return resp, err
}

func (d *ClusterGatewayImpl) Start() error {
	d.started.CompareAndSwap(false, true)

	d.log.Info("Starting router...")

	if d.KubernetesMode() {
		// Start the HTTP Kubernetes Scheduler service.
		go d.cluster.Scheduler().(scheduling.KubernetesClusterScheduler).StartHttpKubernetesSchedulerService()
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

	if d.router != nil {
		// Close the router
		if err := d.router.Close(); err != nil {
			d.log.Error("Failed to cleanly shutdown router because: %v", err)
		}
	}

	if d.idleSessionReclaimer != nil {
		d.idleSessionReclaimer.Close()
	}

	if d.started.Load() {
		// Wait for the newKernels to be cleaned up
		<-d.cleaned
	}

	// Close the listener
	if d.listener != nil {
		if err := d.listener.Close(); err != nil {
			d.log.Error("Failed to cleanly shutdown listener because: %v", err)
		}
	}

	if d.cluster != nil {
		d.cluster.Close()
	}

	return nil
}

////////////////////////////////////
// Provider implementations //
////////////////////////////////////

func (d *ClusterGatewayImpl) handleShutdownRequest(msg *messaging.JupyterMessage) error {
	sessionId := msg.JupyterSession()
	d.log.Debug(utils.LightPurpleStyle.Render("Intercepting \"%v\" message targeting session \"%s\" and using RPC pathway instead..."),
		messaging.MessageTypeShutdownRequest, sessionId)

	kernel, ok := d.kernels.Load(sessionId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find kernel associated with session \"%s\"; cannot stop kernel.", sessionId)
		d.log.Warn(errorMessage)

		// For long-running containers, this error is actually problematic / indicating that something is wrong.
		// For short-lived containers, it's not a big deal.
		if d.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning {
			// Spawn a separate goroutine to send an error notification to the dashboard.
			go d.notifier.NotifyDashboardOfWarning(errorMessage, errorMessage)
		}

		err := fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, sessionId)

		sendErr := d.sendErrorResponse(kernel, msg, err, messaging.ControlMessage)
		if sendErr != nil {
			d.log.Error("Failed to send error response for failed shutdown request for kernel \"%s\" because: %v",
				kernel.ID(), sendErr)
		}

		return err
	}

	err := d.removeAllReplicasOfKernel(kernel, true, false, false)
	if err != nil {
		d.log.Error("Failed to remove all replicas of kernel \"%s\" because: %v", kernel.ID(), err)

		sendErr := d.sendErrorResponse(kernel, msg, err, messaging.ControlMessage)
		if sendErr != nil {
			d.log.Error("Failed to send error response for failed shutdown request for kernel \"%s\" because: %v",
				kernel.ID(), sendErr)
		}

		return err
	}

	// Stop the kernel. If we get an error, print it here, and then we'll return it.
	if _, err = d.stopKernelImpl(context.Background(), &proto.KernelId{Id: kernel.ID()}); err != nil {
		d.log.Error("Failed to (cleanly) terminate session \"%s\", kernel \"%s\" because: %v",
			sessionId, kernel.ID(), err)

		sendErr := d.sendErrorResponse(kernel, msg, err, messaging.ControlMessage)
		if sendErr != nil {
			d.log.Error("Failed to send error response for failed shutdown request for kernel \"%s\" because: %v",
				kernel.ID(), sendErr)
		}

		// Spawn a separate goroutine to send an error notification to the dashboard.
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Terminate kernel %s, Session %s",
			kernel.ID(), sessionId), err.Error())
		return err
	}

	session, ok := d.cluster.GetSession(kernel.ID())
	if !ok || session == nil {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session %s associated with kernel %s, which is being shutdown",
			kernel.ID(), kernel.ID())
		d.log.Error(errorMessage)
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Find scheduling.Session of Terminating kernel \"%s\", Session ID=%s",
			kernel.ID(), sessionId), errorMessage)
	} else {
		p := session.SessionStopped()
		err = p.Error()
		if err != nil {
			d.log.Error("Error while de-scheduling kernel \"%s\" associated with session \"%s\"",
				kernel.ID(), sessionId)
			go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Error while Descheduling Session \"%s\"",
				kernel.ID()), err.Error())
			return err
		}
	}

	// This just returns to our underlying server's request handler code.
	// To send a response to Jupyter, we'd need to use the ClusterGatewayImpl::kernelReplicaResponseForwarder method.
	return nil // Will be nil if we successfully shut down the kernel.
}

func (d *ClusterGatewayImpl) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	// If this is a shutdown request, then use the RPC pathway instead.
	if msg.JupyterMessageType() == messaging.MessageTypeShutdownRequest {
		return d.handleShutdownRequest(msg)
	}

	err := d.forwardRequest(nil, messaging.ControlMessage, msg)

	// When a kernel is first created/being nudged, Jupyter Server will send both a Shell and Control request.
	// The Control request will just have a Session, and the mapping between the Session and the kernel will not
	// be established until the Shell message is processed. So, if we see a types.ErrKernelNotFound error here, we will
	// simply retry it after some time has passed, as the requests are often received very close together.
	if errors.Is(err, types.ErrKernelNotFound) {
		time.Sleep(time.Millisecond * 1500)

		// We won't re-try more than once.
		err = d.forwardRequest(nil, messaging.ControlMessage, msg)
	}

	// If the error is non-nil and the error is NOT types.ErrKernelNotFound, then we'll send an error response.
	// We can't send an error reply if the error is types.ErrKernelNotFound, as we won't be able to figure out
	// which kernel to send the response to.
	if err != nil && !errors.Is(err, types.ErrKernelNotFound) {
		kernel, _ /* messageType */, kernelResolveError := d.kernelAndTypeFromMsg(msg)
		if kernelResolveError != nil {
			d.log.Error("Could not determine kernel associated with JupyterMessage when attempting to send 'error reply': %s",
				msg.StringFormatted())

			return err // Return the original error
		}

		_ = d.sendErrorResponse(kernel, msg, err, messaging.ControlMessage)
	}

	return err
}

func (d *ClusterGatewayImpl) kernelShellHandler(kernelInfo scheduling.KernelInfo, _ messaging.MessageType, msg *messaging.JupyterMessage) error {
	return d.ShellHandler(kernelInfo, msg)
}

// getArtificialKernelInfoReply creates and returns a "kernel_info_reply"
func (d *ClusterGatewayImpl) getArtificialKernelInfoReply() map[string]interface{} {
	content := make(map[string]interface{})

	langaugeInfo := make(map[string]interface{})
	langaugeInfo["name"] = "Any text"
	langaugeInfo["mimetype"] = "text/plain"
	langaugeInfo["file_extension"] = ".txt"

	content["status"] = "ok"
	content["protocol_version"] = "5.3"
	content["implementation"] = "Distributed Python 3"
	content["implementation_version"] = "0.2"
	content["language_info"] = langaugeInfo
	content["banner"] = "Distributed kernel - as useful as a parrot"
	content["help_links"] = []map[string]string{
		{
			"text": "Python Reference",
			"url":  "https://docs.python.org/3.12/",
		},
		{
			"text": "IPython Reference",
			"url":  "https://ipython.org/documentation.html",
		},
		{
			"text": "NumPy Reference",
			"url":  "https://docs.scipy.org/doc/numpy/reference/",
		},
		{
			"text": "SciPy Reference",
			"url":  "https://docs.scipy.org/doc/scipy/reference/",
		},
		{
			"text": "Matplotlib Reference",
			"url":  "https://matplotlib.org/contents.html/",
		},
		{
			"text": "SymPy Reference",
			"url":  "http://docs.sympy.org/latest/index.html",
		},
		{
			"text": "pandas Reference",
			"url":  "https://pandas.pydata.org/pandas-docs/stable/",
		},
	}

	return content
}

// generateArtificialResponse returns an artificial messaging.JupyterMessage response when a kernel receives a message and
// its replica container(s) is/are not actively scheduled.
func (d *ClusterGatewayImpl) generateArtificialResponse(kernel scheduling.Kernel, msg *messaging.JupyterMessage, typ messaging.MessageType) (*messaging.JupyterMessage, error) {
	jupyterMsgType := msg.JupyterMessageType()
	resp := msg.Clone()

	if typ == messaging.ShellMessage {
		ioMsg, err := d.sendStatusMessage(kernel, "busy")
		if err != nil {
			d.log.Error("Failed to send IOPub \"busy\" status message to client of kernel \"%s\": %v",
				kernel.ID(), err)
			return nil, err
		} else {
			d.log.Debug("Successfully sent IOPub \"busy\" status message to client of kernel \"%s\": %v", kernel.ID(), ioMsg)
		}
	}

	var (
		content      map[string]interface{}
		responseType messaging.JupyterMessageType
	)
	if jupyterMsgType == messaging.KernelInfoRequest {
		content = d.getArtificialKernelInfoReply()
		responseType = messaging.KernelInfoReply
	} else {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnsupportedMsgTypeForArtificialResponse, jupyterMsgType)
	}

	header, err := resp.GetHeader()
	if err != nil {
		d.log.Error("Failed to get header of artificial \"%s\" message: %v", jupyterMsgType, err)
		return nil, err
	}

	err = resp.JupyterFrames.EncodeParentHeader(&header)
	if err != nil {
		d.log.Error("Failed to encode parent header of artificial \"%s\" message: %v", jupyterMsgType, err)
		return nil, err
	}

	// Create the message header.
	header = &messaging.MessageHeader{
		Date:     time.Now().UTC().Format(messaging.JavascriptISOString),
		MsgID:    uuid.NewString(),
		MsgType:  responseType,
		Session:  resp.JupyterSession(),
		Username: resp.JupyterUsername(),
		Version:  resp.JupyterVersion(),
	}

	err = resp.EncodeMessageHeader(header)
	if err != nil {
		d.log.Error("Failed to encode new header for artificial response to Jupyter %s \"%s\" message: %v",
			typ.String, jupyterMsgType, err)
		return nil, err
	}

	err = resp.JupyterFrames.EncodeContent(&content)
	if err != nil {
		d.log.Error("Failed to encode content for artificial response to Jupyter %s \"%s\" message: %v",
			typ.String, jupyterMsgType, err)
		return nil, err
	}

	resp.JupyterFrames.Frames, err = resp.JupyterFrames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return nil, err
	}

	d.log.Debug("Returning artificial response to %s \"%s\" message \"%s\" (JupyterID=\"%s\"): %v",
		typ.String(), jupyterMsgType, resp.RequestId, resp.JupyterMessageId(), resp)

	return resp, nil
}

// waitForDeschedulingToEnd is called while handling an "execute_request" if it is found that there is a "descheduling
// attempt" in progress for the target scheduling.Kernel.
//
// waitForDeschedulingToEnd first checks if the attempt has finished. If so, then waitForDeschedulingToEnd will simply
// delete the record of it and return.
//
// If the attempt is not finished, then waitForDeschedulingToEnd will wait for (as of the time of writing this
// comment) 7.5 minutes. If after 6 minutes, the attempt has not completed, then waitForDeschedulingToEnd will return
// an error.
//
// It's possible that there is simply a large network I/O occurring or something like that, so the fact that the attempt
// has not resolved is not necessarily indicative that something is wrong.
func (d *ClusterGatewayImpl) waitForDeschedulingToEnd(kernel scheduling.Kernel, removalAttempt scheduling.RemoveReplicaContainersAttempt) error {
	// If the removal attempt is nil, then there is nothing to wait for.
	if removalAttempt == nil {
		d.log.Debug("Nil removal attempt passed for kernel \"%s\". Nothing to wait for.", kernel.ID())
		return nil
	}

	// First, check if this attempt has finished. If so, then we'll simply delete the record of it and return.
	if !removalAttempt.IsComplete() {
		d.log.Debug("Target kernel \"%s\" is being descheduled as of %v ago.",
			kernel.ID(), removalAttempt.TimeElapsed())

		startTime := time.Now()

		// We do this in a loop so we can incrementally print warning messages after we've been waiting for a while,
		// in the event that the descheduling operation does not resolve quickly.
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*150 /* 2.5 minutes */)

			// Wait for the replicas to finish being descheduled.
			err := removalAttempt.Wait(ctx)
			cancel()

			// No error means that the descheduling attempt concluded successfully.
			if err == nil {
				d.log.Debug("Descheduling complete for kernel \"%s\". Waited: %v. Time to reschedule.",
					kernel.ID(), time.Since(startTime))
				return nil
			}

			// Not necessarily an error. Context timeout is set to a low value so that we can incrementally print
			// warning messages after we've been waiting for a while, in the event that the descheduling operation
			// does not resolve quickly.
			d.log.Warn("Awaiting the completion of replica descheduling for kernel \"%s\". Time elapsed: %v.",
				kernel.ID(), time.Since(startTime))

			// But if the error is NOT a context.DeadlineExceeded error, then something went wrong.
			if !errors.Is(err, context.DeadlineExceeded) {
				// TODO: How to handle?
				d.log.Error("Attempt to remove replicas of kernel %s resulted in an error: %v",
					kernel.ID(), err)

				errorTitle := fmt.Sprintf("Failed to RemoveHost Replicas of Kernel \"%s\"", kernel.ID())
				go d.notifier.NotifyDashboardOfError(errorTitle, err.Error())

				return err
			}
		}

		return fmt.Errorf("%w: kernel \"%s\" has been stuck in a state of being de-scheduled for %v",
			ErrKernelNotReady, kernel.ID(), time.Since(removalAttempt.StartedAt()))
	}

	return nil
}

// ensureKernelReplicasAreScheduled ensures that the replicas of the specified scheduling.Kernel are already scheduled.
//
// If they're not, then ensureKernelReplicasAreScheduled will either schedule the replicas, if the given msg is an
// "execute_request" message, or it will simply return an artificial response, in the case of all other message types.
//
// ensureKernelReplicasAreScheduled possibly returns a dummy message response generated by the Cluster Gateway,
// a bool flag indicating whether the replicas were already scheduled (true) or not (false), and an optional error.
//
// Important: in general, if ensureKernelReplicasAreScheduled returns an error, then that error should be sent back
// to the client by the caller of ensureKernelReplicasAreScheduled.
func (d *ClusterGatewayImpl) ensureKernelReplicasAreScheduled(kernel scheduling.Kernel, msg *messaging.JupyterMessage, typ messaging.MessageType) (*messaging.JupyterMessage, bool, error) {
	d.log.Debug("Verifying that replicas of kernel %s are all scheduled before processing %s \"%s\" request \"%s\"",
		kernel.ID(), typ.String(), msg.JupyterMessageType(), msg.JupyterMessageId())

	// Check if any replicas are being migrated and, if so, then wait for them to finish being migrated.
	migrationCtx, migrateCancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer migrateCancel()

	err := kernel.WaitForMigrationsToComplete(migrationCtx)
	if err != nil {
		return nil, false, err
	}

	// Check if the kernel is being descheduled. If so, then we'll wait for a bit for it to finish being descheduled.
	_, removalAttempt := kernel.ReplicaContainersAreBeingRemoved()
	if err = d.waitForDeschedulingToEnd(kernel, removalAttempt); err != nil {
		return nil, false, err
	}

	// If the replica(s) are scheduled, then we have nothing to do.
	if kernel.ReplicasAreScheduled() {
		d.log.Debug("Replicas of kernel %s are scheduled.", kernel.ID())
		return nil, true, nil
	}

	// For any message that isn't an "execute_request" message, we'll return an artificial response when the
	// replicas of the kernel are not actively running.
	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		d.log.Debug("Replicas of kernel %s are NOT scheduled. Generating artificial response to %s \"%s\" message %s (JupyterID=\"%s\").",
			kernel.ID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		resp, err := d.generateArtificialResponse(kernel, msg, typ)
		return resp, false, err
	}

	d.log.Debug("Replicas of kernel %s are NOT scheduled. Scheduling replicas now before handling Jupyter \"execute_request\" message %s (JupyterID=\"%s\").",
		kernel.ID(), msg.RequestId, msg.JupyterMessageId())

	// We'll wait up to 5 minutes for this operation to complete successfully.
	// That's a long time.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// We'll use this as a means for the other goroutine to communicate the result of the scheduling operation to us.
	notifyChan := make(chan interface{}, 1)
	startTime := time.Now()

	// Create the kernel containers in another goroutine.
	go func() {
		scheduleError := d.scheduleReplicas(ctx, kernel, kernel.KernelSpec(), nil)

		if scheduleError == nil {
			// No error. Just send a struct{}{}.
			notifyChan <- struct{}{}
		} else {
			// Send the error.
			notifyChan <- scheduleError
		}
	}()

	// Wait for either the context to time out, for the operation to succeed, or for the operation to fail explicitly
	// for some other reason.
	select {
	case <-ctx.Done():
		{
			// If there's an error attached to the context, then we'll return it.
			if err = ctx.Err(); err != nil {
				d.log.Error("Timed-out waiting for replica container(s) of kernel \"%s\" to be created after %v: %v",
					kernel.ID(), time.Since(startTime), err)
			} else {
				d.log.Error("Timed-out waiting for replica container(s) of kernel \"%s\" to be created after %v.",
					kernel.ID(), time.Since(startTime))

				// We'll return an error indicating that the operation timed out.
				err = fmt.Errorf("%w: creation of replica container(s) for kernel \"%s\" timed out after %v",
					types.ErrRequestTimedOut, kernel.ID(), time.Since(startTime))
			}

			return nil, false, err
		}
	case v := <-notifyChan:
		{
			// If we received an error over the channel, then we'll log an error message and return the error.
			if err, ok := v.(error); ok {
				d.log.Warn("Failed to schedule replica container(s) of kernel \"%s\" after receiving Jupyter \"%s\" message: %v",
					kernel.ID(), msg.JupyterMessageType(), err)
				return nil, false, err
			} else {
				// Not an error. Operation must have succeeded.
				return nil, false, nil
			}
		}
	}
}

func (d *ClusterGatewayImpl) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	kernel, ok := d.kernels.Load(msg.JupyterSession())

	// Couldn't load the kernel. Let's check if we've registered the kernel under a different ID that
	// is also encoded within the Jupyter message.
	if !ok && (msg.JupyterMessageType() == messaging.KernelInfoRequest || msg.JupyterMessageType() == messaging.ShellExecuteRequest) {
		// Register kernel on KernelInfoRequest
		if msg.DestinationId == "" {
			d.log.Error("Shell '%s' message '%s' does not contain a destination ID:\n%s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), msg.StringFormatted())

			// We don't know which kernel this came from, so we can't really send an error message in response.
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(msg.DestinationId)
		if !ok {
			d.log.Error("Could not find kernel or session \"%s\" while handling shell message %v of type '%v', session=%v",
				msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

			d.log.Error("Valid kernels/sessions are (%d):", d.kernels.Len())
			d.kernels.Range(func(id string, kernel scheduling.Kernel) (contd bool) {
				d.log.Error("%s (kernel \"%s\")", id, kernel.ID())
				return true
			})

			// We don't know which kernel this came from, so we can't really send an error message in response.
			return fmt.Errorf("%w: could not find kernel associated with session \"%s\" or destination \"%s\"",
				types.ErrKernelNotFound, msg.JupyterSession(), msg.DestinationId)
		}

		kernel.BindSession(msg.JupyterSession())
		d.kernels.Store(msg.JupyterSession(), kernel)
	}

	// If the kernel is still nil, then that's a bug.
	if kernel == nil {
		d.log.Error("Could not find kernel or session \"%s\" while handling shell message %v of type '%v', session=%v",
			msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

		d.log.Error("Valid kernels/sessions are (%d):", d.kernels.Len())
		d.kernels.Range(func(id string, kernel scheduling.Kernel) (contd bool) {
			d.log.Error("%s (kernel \"%s\")", id, kernel.ID())
			return true
		})

		// If there's no message ID, then something is really wrong.
		// We'll print the stack so we can better trace through what happened while handling this message.
		if len(msg.DestinationId) == 0 {
			d.log.Error("Extracted empty kernel ID from ZMQ \"%s\" message: %v", msg.JupyterMessageType(), msg)
			debug.PrintStack()
		}

		// We don't know which kernel this came from, so we can't really send an error message in response.
		return fmt.Errorf("%w: could not find kernel associated with session \"%s\" or destination \"%s\"",
			types.ErrKernelNotFound, msg.JupyterSession(), msg.DestinationId)
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		// executeRequestHandler handles sending an error response if it encounters an error.
		return d.executeRequestHandler(kernel, msg)
	}

	d.log.Debug("Forwarding shell message to kernel %s: %s", msg.DestinationId, msg.StringFormatted())
	err := d.forwardRequest(kernel, messaging.ShellMessage, msg)
	if err != nil {
		d.log.Error("Error while handling/forwarding shell \"%s\" message \"%s\" (JupyterID=\"%s\"): %v.",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)

		// We'll send an error message to the associated client here, though it's possible that we were able to
		// send a reply, and the error came from something that occurred after sending our response (I think?).
		_ = d.sendErrorResponse(kernel, msg, err, messaging.ShellMessage)

		return err
	}

	return nil
}

// updateTargetedExecuteRequestMetadata is used to embed the GPU device IDs in the metadata frame of the given
// "execute_request" message targeting the specified scheduling.KernelReplica.
//
// Warning: this modifies the given messaging.JupyterMessage.
func (d *ClusterGatewayImpl) updateTargetedExecuteRequestMetadata(jMsg *messaging.JupyterMessage, targetReplica scheduling.KernelReplica) error {
	// Validate that the message is of the proper type.
	if jMsg.JupyterMessageType() != messaging.ShellExecuteRequest {
		return fmt.Errorf("%w: expected message of type \"%s\"; however, message \"%s\" targeting kernel \"%s\" is of type \"%s\"",
			client.ErrInvalidExecuteRegistrationMessage, messaging.ShellExecuteRequest, jMsg.JupyterMessageId(), targetReplica.ID(), jMsg.JupyterMessageType())
	}

	// Deserialize the message's metadata frame into a dictionary.
	var metadataDict map[string]interface{}
	if err := jMsg.JupyterFrames.DecodeMetadata(&metadataDict); err != nil {
		d.log.Error("Failed to decode metadata frame of \"execute_request\" message \"%s\" targeting kernel \"%s\" with JSON: %v",
			jMsg.JupyterMessageId(), targetReplica.ID(), err)
		return err
	}

	// Get the GPU device IDs assigned to the target kernel replica.
	gpuDeviceIds, err := targetReplica.Host().GetGpuDeviceIdsAssignedToReplica(targetReplica.ReplicaID(), targetReplica.ID())
	if err != nil {
		d.log.Error("Failed to retrieve GPU device IDs assigned to replica %d of kernel \"%s\" because: %v",
			targetReplica.ReplicaID(), targetReplica.ID(), err)

		return err
	}

	// Embed the GPU device IDs in the metadata dictionary, which we'll re-encode into the message's metadata frame.
	metadataDict[GpuDeviceIdsArg] = gpuDeviceIds
	metadataDict[messaging.TargetReplicaArg] = targetReplica.ReplicaID()

	// Re-encode the metadata frame. It will have the number of idle GPUs available,
	// as well as the reason that the request was yielded (if it was yielded).
	err = jMsg.EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		d.notifier.NotifyDashboardOfError("Failed to Encode Metadata Frame", err.Error())
		panic(err)
	}

	// Regenerate the signature.
	_, err = jMsg.JupyterFrames.Sign(targetReplica.ConnectionInfo().SignatureScheme, []byte(targetReplica.ConnectionInfo().Key))
	if err != nil {
		message := fmt.Sprintf("Failed to sign updated JupyterFrames for \"%s\" message because: %v",
			jMsg.JupyterMessageType(), err)
		d.notifier.NotifyDashboardOfError("Failed to Sign JupyterFrames", message)
		panic(err)
	}

	// Validate the updated message/frames.
	verified := messaging.ValidateFrames([]byte(targetReplica.ConnectionInfo().Key),
		targetReplica.ConnectionInfo().SignatureScheme, jMsg.JupyterFrames)
	if !verified {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'",
			targetReplica.ConnectionInfo().SignatureScheme, targetReplica.ConnectionInfo().Key)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", jMsg.StringFormatted())
	}

	return nil
}

// forwardExecuteRequest forwards the given "execute_request" message to all eligible replicas of the
// specified kernel and a converted "yield_request" to all ineligible replicas of the kernel.
func (d *ClusterGatewayImpl) forwardExecuteRequest(originalJupyterMessage *messaging.JupyterMessage, kernel scheduling.Kernel,
	targetReplica scheduling.KernelReplica) error {

	targetReplicaId := int32(-1)
	if targetReplica != nil {
		targetReplicaId = targetReplica.ReplicaID()
	}

	replicas := kernel.Replicas()

	originalJupyterMessage.AddDestFrameIfNecessary(kernel.ID())

	jupyterMessages := make([]*messaging.JupyterMessage, kernel.Size())
	for _, replica := range replicas {
		// TODO: If we make it so we can toggle on/off the gateway-assisted replica selection,
		// 		 then we need to update the logic here to only convert a message to a yield request
		//		 if gateway-assisted replica selection is enabled and the target replica is nil.
		// 		 This is because if gateway-assisted replica selection is disabled, then target replica
		//		 will be nil, but that'll be okay -- with gateway-assisted replica selection disabled,
		//		 the target replica is supposed to be nil.
		if replica.ReplicaID() == targetReplicaId {
			jupyterMessage := originalJupyterMessage.Clone()

			err := d.updateTargetedExecuteRequestMetadata(jupyterMessage, replica)
			if err != nil {
				d.log.Error("Failed to embed GPU device IDs in \"%s\" message \"%s\" targeting replica %d of kernel \"%s\": %v",
					jupyterMessage.JupyterMessageType(), jupyterMessage.JupyterMessageId(), replica.ReplicaID(), kernel.ID(), err)
				return err
			}

			jupyterMessages[replica.ReplicaID()-1] = jupyterMessage
			continue
		}

		// Convert the "execute_request" message to a "yield_request" message.
		// The returned message is initially created as a clone of the target message.
		jupyterMessage, err := originalJupyterMessage.CreateAndReturnYieldRequestMessage(targetReplicaId)
		if err != nil {
			d.log.Error("Failed to convert \"execute_request\" message \"%s\" to a \"yield_request\" message: %v",
				originalJupyterMessage.JupyterMessageId(), err)

			d.log.Error("Original \"execute_request\" message that we failed to convert: %v", originalJupyterMessage)

			d.notifier.NotifyDashboard("Failed to Convert Message of Type \"execute_request\" to a \"yield_request\" Message",
				err.Error(), messaging.ErrorNotification)

			originalJupyterMessage.IsFailedExecuteRequest = true

			// We'll send an error message to the associated client here.
			_ = d.sendErrorResponse(kernel, originalJupyterMessage, err, messaging.ShellMessage)

			return err
		}

		d.log.Debug("Converted \"execute_request\" \"%s\" to a \"yield_request\" message for replica %d of kernel \"%s\" [targetReplicaId=%d]: %v",
			originalJupyterMessage.JupyterMessageId(), replica.ReplicaID(), replica.ID(), targetReplicaId, jupyterMessage.JupyterFrames.StringFormatted())

		// We subtract 1 because replica IDs start at 1.
		jupyterMessages[replica.ReplicaID()-1] = jupyterMessage
	}

	var numExecRequests, numYieldRequests int
	for idx, msg := range jupyterMessages {
		d.log.Debug("Execution request \"%s\" targeting replica %d of kernel \"%s\" is a(n) \"%s\" message.",
			msg.JupyterMessageId(), idx+1, kernel.ID(), msg.JupyterMessageType())

		if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
			numExecRequests += 1
		} else {
			numYieldRequests += 1
		}
	}

	if kernel.SupposedToYieldNextExecutionRequest() {
		// Sanity check.
		if numExecRequests > 0 {
			d.log.Error("Kernel \"%s\" is supposed to yield/fail its next execution, but only %d/%d replica-specific messages have type \"%s\"...",
				kernel.ID(), numYieldRequests, numExecRequests+numYieldRequests, messaging.ShellYieldRequest)

			return fmt.Errorf("ClusterGatewayImpl::forwardExecuteRequest: kernel \"%s\" should be yielding next execution, but it's not")
		}

		// Record that we yielded the request.
		kernel.YieldedNextExecutionRequest()
	}

	if d.SubmitExecuteRequestsOneAtATime {
		return d.forwardExecuteRequestOneAtATime(jupyterMessages, kernel)
	}

	// We're not using the request forwarder, apparently. So, we'll call RequestWithHandlerAndReplicas ourselves.
	// We call RequestWithHandlerAndReplicas instead of RequestWithHandler because RequestWithHandler essentially does
	// what we just did up above before calling RequestWithHandlerAndReplicas; however, RequestWithHandler assumes that
	// all replicas are to receive an identical message.
	//
	// That's obviously not what we want to happen here, and so we manually created the different messages for the
	// different replicas ourselves.
	return kernel.RequestWithHandlerAndReplicas(context.Background(), "Forwarding", messaging.ShellMessage, jupyterMessages,
		d.kernelReplicaResponseForwarder, nil, replicas...)
}

func (d *ClusterGatewayImpl) forwardExecuteRequestOneAtATime(jupyterMessages []*messaging.JupyterMessage, kernel scheduling.Kernel) error {
	jupyterMessageId := jupyterMessages[0].JupyterMessageId()

	d.log.Debug("Enqueuing \"execute_request\" \"%s\" targeting kernel \"%s\" with \"execute_request\" forwarder.",
		jupyterMessageId, kernel.ID())

	resultChan, closeFlag, err := d.executeRequestForwarder.EnqueueRequest(jupyterMessages, kernel, jupyterMessageId)

	if err != nil {
		d.log.Error("Failed to enqueue \"%s\" message(s) \"%s\" targeting kernel \"%s\": %v",
			messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID(), err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*6)
	defer cancel()

	handleRes := func(res interface{}) error {
		// Return the result as an error or nil if there was no error.
		switch res.(type) {
		case error:
			return res.(error)
		default:
			return nil
		}
	}

	// Wait for the result.
	// We need to wait for the result, or the execute forwarder (for this particular kernel) will block.
	select {
	case res := <-resultChan:
		{
			return handleRes(res)
		}
	case <-ctx.Done():
		{
			err = ctx.Err()
			d.log.Error("Timed-out waiting for response to \"%s\" message \"%s\" targeting kernel \"%s\": %v.",
				messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID(), err)

			// Record that we're giving up, so if a result comes later, the execute request forwarder won't get
			// stuck trying to send it over the channel when we're never going to be around to receive it.
			if !closeFlag.CompareAndSwap(0, 1) {
				d.log.Warn("Failed to flip ClosedFlag. There should be a result available now for \"%s\" message \"%s\" targeting kernel \"%s\".",
					messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID())

				res := <-resultChan
				return handleRes(res)
			}

			return err
		}
	}
}

// executeRequestHandler is a specialized version of ShellHandler that is used explicitly/exclusively for
// "execute_request" messages. It first calls processExecuteRequest before forwarding the "execute_request"
// to the replicas (well, to the Local Schedulers first).
func (d *ClusterGatewayImpl) executeRequestHandler(kernel scheduling.Kernel, jMsg *messaging.JupyterMessage) error {
	// First, update the kernel's resource request (for replica-based policies) if there's an updated
	// resource request in the metadata of the "execute_request" message.
	//
	// We do this before even checking if the replicas are scheduled.
	// If they aren't scheduled, then it would be best for the spec to be up to date before we bother scheduling them.
	// And if they're already scheduled, then their specs will be updated.
	err := d.processExecuteRequestMetadata(jMsg, kernel)
	if err != nil {
		jMsg.IsFailedExecuteRequest = true
		return err
	}

	// Now we check if the replicas are scheduled. For static and dynamic, they will be, as well as with reservation,
	// unless idle session reclamation is enabled.
	//
	// For FCFS, they will not already be scheduled. (I say "they", but for FCFS, there's just 1 replica.)
	_, replicasAlreadyScheduled, err := d.ensureKernelReplicasAreScheduled(kernel, jMsg, messaging.ShellMessage)
	if err != nil {
		d.log.Warn("Error encountered while ensuring replica container(s) of kernel %s are scheduled in order to handle shell \"%s\" message: %v",
			kernel.ID(), jMsg.JupyterMessageType(), err)

		// We'll send an error message to the associated client here.
		go func() {
			sendErr := d.sendErrorResponse(kernel, jMsg, err, messaging.ShellMessage)
			if sendErr != nil {
				d.log.Error("Failed to send error response for shell \"%s\" message \"%s\": %v",
					jMsg.JupyterMessageType(), jMsg.JupyterMessageId(), sendErr)
			}
		}()
		return err
	}

	// For policies that create replicas on-demand each time code is submitted,
	// 'replicasAlreadyScheduled' will always be false.
	if !replicasAlreadyScheduled {
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumTimesKernelReplicaNotAvailableImmediately.Add(1)
		d.clusterStatisticsMutex.Unlock()
	}

	targetReplica, processingError := d.processExecuteRequest(jMsg, kernel)
	if processingError != nil {
		// Send a response with the error as the content.
		_ = d.sendErrorResponse(kernel, jMsg, processingError, messaging.ShellMessage)
		return processingError
	}

	if targetReplica != nil {
		d.log.Debug("Identified target replica %d of kernel '%s' to lead \"execute_request\" \"%s\"",
			targetReplica.ReplicaID(), targetReplica.ID(), jMsg.JupyterMessageId())
	}

	// Broadcast an "execute_request" to all eligible replicas and a "yield_request" to all ineligible replicas.
	err = d.forwardExecuteRequest(jMsg, kernel, targetReplica)
	if err != nil {
		_ = d.sendErrorResponse(kernel, jMsg, err, messaging.ShellMessage)
	}

	return err // Will be nil on success.
}

// sendErrorResponse is used to respond to a shell message immediately, before we've routed it to any local
// schedulers or kernel replicas, because we encountered an unrecoverable error while (pre)processing the message.
func (d *ClusterGatewayImpl) sendErrorResponse(kernel scheduling.Kernel, request *messaging.JupyterMessage, errContent error, typ messaging.MessageType) error {
	d.log.Warn("Sending error response to shell \"%s\" message \"%s\" targeting kernel \"%s\": %v",
		request.JupyterMessageType(), request.JupyterMessageId(), kernel.ID(), errContent)

	// First, update the header to be a "_reply" message type.
	header, err := request.GetHeader()
	if err != nil {
		d.log.Error("Failed to extract header from shell \"%s\" message \"%s\": %v",
			request.JupyterMessageType(), request.JupyterMessageId(), err)
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Extract Header from Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	err = request.JupyterFrames.EncodeParentHeader(&header)
	if err != nil {
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Encode Parent Header for Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	requestType := request.JupyterMessageType()
	replyType := fmt.Sprintf("%s_reply", requestType[0:strings.Index(requestType, "_request")])
	_ = request.SetMessageType(messaging.JupyterMessageType(replyType), false)
	_ = request.SetMessageId(fmt.Sprintf("%s_1", request.JupyterMessageId()), false)
	_ = request.SetDate(time.Now().Format(time.RFC3339Nano), false)

	// Re-encode the header.
	header, err = request.GetHeader()
	if err != nil {
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Get Header from Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	err = request.EncodeMessageHeader(header)
	if err != nil {
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Re-Encode Header of Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	// Second, embed the error in the response content.
	errorContent := messaging.MessageError{
		Status:   messaging.MessageStatusError,
		ErrName:  fmt.Sprintf("Failed to Handle \"%s\" Message", requestType),
		ErrValue: errContent.Error(),
	}
	err = request.JupyterFrames.EncodeContent(&errorContent)
	if err != nil {
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Encode Error Content of Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	// Regenerate the signature. Don't include the buffer frames as part of the signature.
	if kernel.ConnectionInfo().SignatureScheme != "" && kernel.ConnectionInfo().Key != "" {
		_, err = request.JupyterFrames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key))
		if err != nil {
			go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Sign Response to Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
				request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
			return err
		}
	}

	if typ == messaging.ShellMessage && requestType == messaging.ShellExecuteRequest {
		request.IsFailedExecuteRequest = true
	}

	// Finally, send the message back to the Jupyter client.
	return d.forwardResponse(kernel, typ, request)
}

// selectTargetReplicaForExecuteRequest selects a target scheduling.KernelReplica of the given scheduling.Kernel for
// the specified "execute_request" message.
//
// selectTargetReplicaForExecuteRequest checks if there is a target replica specified in the request's metadata. If so,
// then that replica is selected. If not, then selectTargetReplicaForExecuteRequest will invoke the scheduling.Scheduler
// and the configured scheduling.Policy to select a target scheduling.KernelReplica.
func (d *ClusterGatewayImpl) selectTargetReplicaForExecuteRequest(msg *messaging.JupyterMessage, kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	metadata, err := msg.DecodeMetadata()
	if err != nil {
		d.log.Error("Failed to decode metadata of \"%s\" request \"%s\": %v", msg.JupyterMessageType(),
			msg.JupyterParentMessageId(), err)

		return nil, err
	}

	// Check if a specific replica was explicitly specified.
	val, loaded := metadata[messaging.TargetReplicaArg]
	if !loaded {
		d.log.Debug("Target replica unspecified for execution \"%s\" targeting kernel \"%s\".",
			msg.JupyterMessageId(), kernel.ID())
		return d.Scheduler().FindReadyReplica(kernel, msg.JupyterMessageId())
	}

	var targetReplicaId int32
	switch val.(type) {
	case string:
		var targetReplicaIdAsInt int
		targetReplicaIdAsInt, err = strconv.Atoi(val.(string))

		if err != nil {
			d.log.Error("Failed to convert string target replica ID \"%s\" to valid integer: %v", val, err)
			return nil, err
		} else {
			targetReplicaId = int32(targetReplicaIdAsInt)
		}
	case float32:
		targetReplicaId = int32(val.(float32))
	case float64:
		targetReplicaId = int32(val.(float64))
	case int:
		targetReplicaId = int32(val.(int))
	case int32:
		targetReplicaId = val.(int32)
	case int64:
		targetReplicaId = int32(val.(int64))
	default:
		errorMessage := fmt.Sprintf("Unknown or unexpected type of target replica ID found in metadata of \"%s\" request \"%s\": %v",
			msg.JupyterMessageId(), kernel.ID(), reflect.TypeOf(val).Name())
		d.log.Error(errorMessage)
		d.notifier.NotifyDashboardOfError("Failed to Extract Target Replica ID", errorMessage)
		panic(errorMessage)
	}

	// If the specified target replica is invalid (e.g., less than or equal to 0), then we'll just
	// use the scheduler/scheduling policy to select a target replica.
	//
	// Typically, a value of -1 is specified when no explicit target is indicated, so this is an
	// expected outcome.
	if targetReplicaId <= 0 {
		d.log.Debug("Target replica unspecified for execution \"%s\" targeting kernel \"%s\".",
			msg.JupyterMessageId(), kernel.ID())
		return d.Scheduler().FindReadyReplica(kernel, msg.JupyterMessageId())
	}

	d.log.Debug("Target replica specified as replica %d for execution \"%s\" targeting kernel \"%s\".",
		targetReplicaId, msg.JupyterMessageId(), kernel.ID())

	// TODO: Could there be a race here where we migrate the new replica right after scheduling it, such as
	// 		 while using dynamic scheduling? (Yes, almost certainly.)
	targetReplica, err := kernel.GetReplicaByID(targetReplicaId)
	if err != nil {
		d.log.Error("Failed to get replica %d of kernel \"%s\": %v", targetReplicaId, kernel.ID(), err)
		return nil, err
	}

	// Reserve resources for the target kernel if resources are not already reserved.
	if !targetReplica.Host().HasResourcesCommittedToKernel(kernel.ID()) {
		d.log.Debug("Specified target replica %d of kernel \"%s\" does not have resources committed to it on host %s yet. Pre-committing resources now.",
			targetReplica.ReplicaID(), kernel.ID(), targetReplica.Host().GetNodeName())

		// Attempt to pre-commit resources on the specified replica, or return an error if we cannot do so.
		_, err = targetReplica.Host().PreCommitResources(targetReplica.Container(), msg.JupyterMessageId(), nil)
		if err != nil {
			d.log.Error("Failed to reserve resources for replica %d of kernel \"%s\" for execution \"%s\": %v",
				targetReplica.ReplicaID(), kernel.ID(), msg.JupyterMessageId(), err)
			return nil, err
		}

		d.log.Debug("Successfully pre-committed resources for explicitly-specified target replica %d of kernel \"%s\" on host %s.",
			targetReplica.ReplicaID(), kernel.ID(), targetReplica.Host().GetNodeName())
	}

	return targetReplica, nil
}

// processExecuteRequest is an important step of the path of handling an "execute_request".
//
// processExecuteRequest handles pre-committing resources, migrating a replica if no replicas are viable, etc.
func (d *ClusterGatewayImpl) processExecuteRequest(msg *messaging.JupyterMessage, kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	kernelId := kernel.ID()
	d.log.Debug("Processing shell \"execute_request\" message targeting kernel %s: %s", kernelId, msg.StringFormatted())

	// Get the session associated with the kernel.
	session, ok := d.cluster.GetSession(kernelId)
	if !ok {
		d.log.Error("Could not find scheduling.Session associated with kernel \"%s\"...", kernelId)
		msg.IsFailedExecuteRequest = true
		return nil, fmt.Errorf("%w: kernelID=\"%s\"", ErrSessionNotFound, kernelId)
	}

	// Verify that the session isn't already training.
	if session.IsTraining() {
		d.log.Debug("Session %s is already training.", session.ID())
		msg.IsFailedExecuteRequest = true
		return nil, fmt.Errorf("session \"%s\" is already training", kernel.ID())
	}

	// Transition the session to the "expecting to start training soon" state.
	err := session.SetExpectingTraining().Error()
	if err != nil {
		d.notifier.NotifyDashboardOfError("Failed to Set Session to 'Expecting Training'", err.Error())
		msg.IsFailedExecuteRequest = true
		return nil, err
	}

	// Register the execution with the kernel.
	err = kernel.RegisterActiveExecution(msg)
	if err != nil {
		d.log.Error("Failed to register new active execution \"%s\" targeting kernel \"%s\": %v",
			msg.JupyterMessageId(), kernel.ID(), err)
		return nil, err
	}

	if kernel.SupposedToYieldNextExecutionRequest() {
		return nil, nil
	}

	// Find a "ready" replica to handle this execution request.
	targetReplica, err := d.selectTargetReplicaForExecuteRequest(msg, kernel)
	if err != nil {
		// If an error is returned, then we should return the error here so that we send an
		// error message back to the client.
		d.log.Error("Error while searching for ready replica of kernel '%s': %v", kernel.ID(), err)
		msg.IsFailedExecuteRequest = true
		return nil, err
	}

	// If the target replica is non-nil at this point, then we can just return it.
	if targetReplica != nil {
		// If we're using a long-running, replica-based scheduling policy, then we'll
		// increment the metric about a kernel replica being available right away.
		if d.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning {
			d.clusterStatisticsMutex.Lock()
			d.ClusterStatistics.NumTimesKernelReplicaAvailableImmediately.Add(1)
			d.clusterStatisticsMutex.Unlock()
		}

		// If the kernel has a valid (i.e., non-nil) "previous primary replica", then we'll update another statistic...
		if kernel.LastPrimaryReplica() != nil {
			d.clusterStatisticsMutex.Lock()
			// If we selected the same replica again, then update the corresponding metric.
			if kernel.LastPrimaryReplica().ReplicaID() == targetReplica.ReplicaID() {
				d.ClusterStatistics.NumTimesPreviousPrimaryReplicaSelectedConsecutively.Add(1)
			} else {
				d.ClusterStatistics.NumTimesPreviousPrimaryReplicaUnavailable.Add(1)
			}
			d.clusterStatisticsMutex.Unlock()
		}

		return targetReplica, nil
	}

	// If we're using a long-running, replica-based scheduling policy, then we'll
	// increment the metric about a kernel replica not being available right away.
	if d.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning {
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumTimesKernelReplicaNotAvailableImmediately.Add(1)
		d.clusterStatisticsMutex.Unlock()
	}

	// TODO: Update this code.
	// 		 Specifically, the dynamic policies should return a list of replicas to migrate.
	targetReplica, err = d.tryPerformMigration(kernel, msg)
	if err != nil {
		return nil, err
	}

	d.log.Debug("Returning eligible replica %d of kernel '%s' for \"execute_request\" message %s.",
		targetReplica.ReplicaID(), kernel.ID(), msg.JupyterMessageId())
	return targetReplica, nil
}

// tryPerformMigration attempts to migrate one of the replicas of the specified kernel during the handling of
// a code execution request. If successful, tryPerformMigration will return the ID of the migrated replica.
func (d *ClusterGatewayImpl) tryPerformMigration(kernel scheduling.Kernel, msg *messaging.JupyterMessage) (scheduling.KernelReplica, error) {
	d.log.Debug("All %d replicas of kernel \"%s\" are ineligible to execute code. Initiating migration.",
		len(kernel.Replicas()), kernel.ID())

	targetReplica, err := d.Scheduler().SelectReplicaForMigration(kernel)
	if targetReplica == nil {
		return nil, fmt.Errorf("could not identify replica eligible for migration because: %w", err)
	}

	d.log.Debug(utils.LightBlueStyle.Render("Preemptively migrating replica %d of kernel %s now."),
		targetReplica.ReplicaID(), kernel.ID())
	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    targetReplica.ReplicaID(),
			PersistentId: kernel.PersistentID(),
		},
		ForTraining:      true,
		CanCreateNewHost: true,
		TargetNodeId:     nil,
	}

	resp, migrationError := d.MigrateKernelReplica(context.Background(), req)
	if migrationError != nil {
		d.log.Warn("Failed to preemptively migrate replica %d of kernel \"%s\": %v",
			targetReplica.ReplicaID(), kernel.ID(), migrationError)
		msg.IsFailedExecuteRequest = true
		return nil, migrationError
	}

	d.log.Debug("Successfully, preemptively migrated replica %d of kernel \"%s\" to host \"%s\"",
		targetReplica.ReplicaID(), kernel.ID(), resp.NewNodeId)

	return targetReplica, nil
}

// processExecuteRequestMetadata processes the metadata frame of an "execute_request" message.
// The main thing we do here is possibly update the resource request of the associated kernel.
func (d *ClusterGatewayImpl) processExecuteRequestMetadata(msg *messaging.JupyterMessage, kernel scheduling.Kernel) error {
	// If there is nothing in the message's metadata frame, then we just return immediately.
	if len(*msg.JupyterFrames.MetadataFrame()) == 0 {
		return nil
	}

	metadataDict, err := msg.DecodeMetadata()
	if err != nil {
		d.log.Error("Failed to decode metadata frame of \"execute_request\" message \"%s\" with JSON: %v",
			msg.JupyterMessageId(), err)
		return err
	}

	var requestMetadata *messaging.ExecuteRequestMetadata
	if err := mapstructure.Decode(metadataDict, &requestMetadata); err != nil {
		d.log.Error("Failed to parse decoded metadata frame of \"execute_request\" message \"%s\" with mapstructure: %v",
			msg.JupyterMessageId(), err)
		return err
	}

	d.log.Debug("Decoded metadata of \"execute_request\" message \"%s\": %s", msg.JupyterMessageId(), requestMetadata.String())

	// If there is no resource request embedded in the request metadata, then we can just return at this point.
	if requestMetadata.ResourceRequest == nil {
		return nil
	}

	// Are we permitted to dynamically change the resource request(s) of kernels? If not, then we'll just return.
	if !d.Scheduler().Policy().SupportsDynamicResourceAdjustments() {
		return nil
	}

	// If there is a resource request in the metadata, but it is equal to the kernel's current resources,
	// then we can just return.
	specsAreEqual, firstUnequalField := kernel.ResourceSpec().EqualsWithField(requestMetadata.ResourceRequest)
	if specsAreEqual {
		d.log.Debug("Current spec [%v] and new spec [%v] for kernel \"%s\" are equal. No need to update.",
			requestMetadata.ResourceRequest.String(), kernel.ResourceSpec().String(), kernel.ID())
		return nil
	}

	d.log.Debug("Found new resource request for kernel \"%s\" in \"execute_request\" message \"%s\". "+
		"Old spec: %v. New spec: %v. Differ in field '%v' [old=%f, new=%f].",
		kernel.ID(), msg.JupyterMessageId(), kernel.ResourceSpec().String(), requestMetadata.ResourceRequest.String(),
		firstUnequalField, kernel.ResourceSpec().GetResourceQuantity(firstUnequalField),
		requestMetadata.ResourceRequest.GetResourceQuantity(firstUnequalField))

	err = d.updateKernelResourceSpec(kernel, requestMetadata.ResourceRequest)
	if err != nil {
		d.log.Warn("Failed to update resource spec of kernel \"%s\": %v", kernel.ID(), err)
		return err
	}

	return nil
}

// updateKernelResourceSpec attempts to update the resource spec of the specified kernel.
//
// updateKernelResourceSpec will return nil on success. updateKernelResourceSpec will return an error if the kernel
// presently has resources committed to it, and the adjustment cannot occur due to resource contention.
func (d *ClusterGatewayImpl) updateKernelResourceSpec(kernel scheduling.Kernel, newSpec types.CloneableSpec) error {
	if !d.Scheduler().Policy().SupportsDynamicResourceAdjustments() {
		d.log.Debug("Cannot update resource spec of kernel \"%s\" as \"%s\" scheduling policy prohibits this.",
			kernel.ID(), d.Scheduler().Policy().Name())
	}

	if newSpec.GPU() < 0 || newSpec.CPU() < 0 || newSpec.VRAM() < 0 || newSpec.MemoryMB() < 0 {
		d.log.Error("Requested updated resource spec for kernel %s is invalid, as one or more quantities are negative: %s",
			kernel.ID(), newSpec.String())
		return fmt.Errorf("%w: %s", client.ErrInvalidResourceSpec, newSpec.String())
	}

	if newSpec.Equals(kernel.ResourceSpec()) {
		d.log.Debug("Current spec [%v] and new spec [%v] for kernel \"%s\" are equal. No need to update.",
			newSpec.String(), kernel.ResourceSpec().String(), kernel.ID())
		return nil
	}

	d.log.Debug("Attempting to update resource request for kernel %s from %s to %s.",
		kernel.ID(), kernel.ResourceSpec().String(), newSpec.String())

	return kernel.UpdateResourceSpec(newSpec)
}

func (d *ClusterGatewayImpl) ClusterAge(_ context.Context, _ *proto.Void) (*proto.ClusterAgeResponse, error) {
	return &proto.ClusterAgeResponse{Age: d.createdAt.UnixMilli()}, nil
}

func (d *ClusterGatewayImpl) ExecutionLatencyCallback(latency time.Duration, workloadId string, kernelId string) {
	milliseconds := float64(latency.Milliseconds())

	if d.MetricsProvider.PrometheusMetricsEnabled() {
		d.MetricsProvider.GetGatewayPrometheusManager().JupyterTrainingStartLatency.
			With(prometheus.Labels{
				"workload_id": workloadId,
				"kernel_id":   kernelId,
			}).
			Observe(milliseconds)
	}

	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	d.ClusterStatistics.JupyterTrainingStartLatencyMillis.Add(milliseconds)
	d.ClusterStatistics.JupyterTrainingStartLatenciesMillis = append(
		d.ClusterStatistics.JupyterTrainingStartLatenciesMillis, milliseconds)
}

func (d *ClusterGatewayImpl) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return d.forwardRequest(nil, messaging.StdinMessage, msg)
}

func (d *ClusterGatewayImpl) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return d.forwardRequest(nil, messaging.HBMessage, msg)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (d *ClusterGatewayImpl) FailNextExecution(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	d.log.Debug("Received 'FailNextExecution' request targeting kernel %s.", in.Id)

	var (
		kernel scheduling.Kernel
		loaded bool
	)

	// Ensure that the kernel exists.
	if kernel, loaded = d.kernels.Load(in.Id); !loaded {
		d.log.Error("Could not find kernel %s specified in 'FailNextExecution' request...", in.Id)
		return proto.VOID, types.ErrKernelNotFound
	}

	for _, replica := range kernel.Replicas() {
		hostId := replica.HostId()
		host, ok := d.cluster.GetHost(hostId)

		if !ok {
			d.log.Error("Could not find host %s on which replica %d of kernel %s is supposedly running...",
				hostId, replica.ReplicaID(), in.Id)
			go d.notifier.NotifyDashboardOfError("'FailNextExecution' Request Failed",
				fmt.Sprintf("Could not find host %s on which replica %d of kernel %s is supposedly running...",
					hostId, replica.ReplicaID(), in.Id))
			return proto.VOID, scheduling.ErrHostNotFound
		}

		// Even if there's an error here, we'll just keep trying. If only some of these succeed, then the system won't explode.
		// The newKernels for which the `YieldNextExecution` succeeded will simply yield.
		_, err := host.YieldNextExecution(ctx, in)
		if err != nil {
			d.log.Error("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s",
				hostId, host.GetAddress(), err.Error())
			go d.notifier.NotifyDashboardOfError("'FailNextExecution' Request Failed",
				fmt.Sprintf("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s",
					hostId, host.GetAddress(), err.Error()))
		} else {
			d.log.Debug("Successfully issued 'FailNextExecution' to Local Daemon %s (%s) targeting kernel %s.",
				hostId, host.GetAddress(), in.Id)
		}
	}

	kernel.YieldNextExecutionRequest()

	return &proto.Void{}, nil
}

// Return the add-replica operation associated with the given kernel ID and SMR Node ID of the new replica.
//
// This looks for the most-recently-added AddReplicaOperation associated with the specified replica of the specified kernel.
// If `mustBeActive` is true, then we skip any AddReplicaOperation structs that have already been marked as completed.
func (d *ClusterGatewayImpl) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (*scheduling.AddReplicaOperation, bool) {
	d.addReplicaMutex.Lock()
	defer d.addReplicaMutex.Unlock()

	d.log.Debug("Searching for an active AddReplicaOperation for replica %d of kernel \"%s\".",
		smrNodeId, kernelId)

	//activeOps, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	activeOps, ok := d.Scheduler().GetActiveAddReplicaOperationsForKernel(kernelId)
	if !ok {
		return nil, false
	}

	d.log.Debug("Number of AddReplicaOperation struct(s) associated with kernel \"%s\": %d",
		kernelId, activeOps.Len())

	var op *scheduling.AddReplicaOperation
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

// kernelAndTypeFromMsg extracts the kernel ID and the message type from the given ZMQ message.
func (d *ClusterGatewayImpl) kernelAndTypeFromMsg(msg *messaging.JupyterMessage) (kernel scheduling.Kernel, messageType string, err error) {
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
			return nil, msg.JupyterMessageType(), fmt.Errorf("%w: message did not contain a destination ID, and session ID was invalid (i.e., the empty string)", types.ErrKernelNotFound)
		}
	}

	kernel, ok := d.kernels.Load(kernelKey) // kernelId)
	if !ok {
		d.log.Error("Could not find kernel with ID \"%s\"", kernelKey)
		return nil, messageType, types.ErrKernelNotFound
	}

	// TODO: This only matters if the replicas should already be running.
	if kernel.Status() != jupyter.KernelStatusRunning && d.shouldReplicasBeRunning(kernel) {
		return kernel, messageType, ErrKernelNotReady
	}

	return kernel, messageType, nil
}

// IsKernelActivelyTraining is used to query whether a particular kernel is actively training.
func (d *ClusterGatewayImpl) IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	kernel, loaded := d.kernels.Load(in.Id)

	if !loaded {
		d.log.Warn("Queried training status of unknown kernel \"%s\"", in.Id)

		return nil, d.errorf(fmt.Errorf("%w: \"%s\"", types.ErrKernelNotFound, in.Id))
	}

	resp := &proto.IsKernelTrainingReply{
		KernelId:   in.Id,
		IsTraining: kernel.IsTraining(),
	}

	return resp, nil
}

// shouldReplicasBeRunning returns a flag indicating whether the containers of kernels should be running already.
//
// For scheduling policies in which the ContainerLifetime is scheduling.LongRunning, this is true.
func (d *ClusterGatewayImpl) shouldReplicasBeRunning(kernel scheduling.Kernel) bool {
	// If the kernel has been idle-reclaimed, then its replicas should not be running.
	if kernel.IsIdleReclaimed() {
		return false
	}

	// If the containers are in the process of being scheduled, then it's OK that they aren't running yet.
	//
	// If the caller is handling a message that can be spoofed, then it'll be spoofed.
	//
	// If the caller is handling something like an "execute_request", then the "execute_request" handler will end
	// up waiting for the container creation operation to complete.
	if kernel.ReplicaContainersAreBeingScheduled() {
		return false
	}

	return d.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning
}

func (d *ClusterGatewayImpl) forwardRequest(kernel scheduling.Kernel, typ messaging.MessageType, msg *messaging.JupyterMessage) (err error) {
	goroutineId := goid.Get()

	if kernel == nil {
		d.log.Debug(utils.CyanStyle.Render("[gid=%d] Received %s message targeting unknown kernel/session. Inspecting now: %v"), goroutineId, typ.String(), msg.JupyterFrames.String())
		kernel, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	} else {
		d.log.Debug(utils.CyanStyle.Render("[gid=%d] Received %s message targeting kernel %s. Inspecting now..."), goroutineId, typ.String(), kernel.ID())

		// Check availability.
		if kernel.Status() != jupyter.KernelStatusRunning && d.shouldReplicasBeRunning(kernel) {
			return ErrKernelNotReady
		}
	}

	if err != nil {
		d.log.Error("[gid=%d] Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.", goroutineId, typ, err, msg)
		return err
	}

	if kernel == nil {
		// Should not happen; if the error was nil, then kernel is non-nil.
		panic("kernel is nil")
	}

	resp, _, err := d.ensureKernelReplicasAreScheduled(kernel, msg, typ)
	if err != nil {
		d.log.Warn("Error encountered while ensuring replica container(s) of kernel %s are scheduled in order to handle shell \"%s\" message: %v",
			kernel.ID(), msg.JupyterMessageType(), err)
		_ = d.sendErrorResponse(kernel, msg, err, messaging.ShellMessage)
		return
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

	// If resp is non-nil, then one or more replicas are not scheduled.
	if resp != nil {
		d.log.Debug("Replying with artificial \"%s\" response for Jupyter %s \"%s\" message \"%s\" (JupyterID=\"%s\") for kernel \"%s\".",
			resp.JupyterMessageType(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID())

		err = d.kernelReplicaResponseForwarder(kernel.TemporaryKernelReplicaClient(), typ, resp)
		if err != nil {
			d.log.Error(utils.RedStyle.Render("Failed to forward %v \"%s\" response \"%s\" (JupyterID=\"%s\") to client of kernel %s: %v"),
				typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID(), resp)
			return err
		}

		if typ == messaging.ShellMessage {
			ioMsg, err := d.sendStatusMessage(kernel, "idle")
			if err != nil {
				d.log.Error("Failed to send IOPub \"idle\" status message to client of kernel \"%s\": %v",
					kernel.ID(), err)
				return err
			} else {
				d.log.Debug("Successfully sent IOPub \"idle\" status message to client of kernel \"%s\": %v", kernel.ID(), ioMsg)
			}
		}

		return nil
	}

	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelReplicaResponseForwarder, nil)
}

// sendZmqMessage sends the specified *messaging.JupyterMessage on/using the specified *messaging.Socket.
func (d *ClusterGatewayImpl) sendZmqMessage(msg *messaging.JupyterMessage, socket *messaging.Socket, senderId string) error {
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

		d.log.Warn(style.Render("Sending %s \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s took %v."),
			socket.Type.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, sendDuration)
	}

	if d.MetricsProvider.PrometheusMetricsEnabled() {
		if metricError := d.MetricsProvider.GetGatewayPrometheusManager().SentMessage(d.id, sendDuration, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
			d.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}

		if metricError := d.MetricsProvider.GetGatewayPrometheusManager().SentMessageUnique(d.id, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
			d.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}
	}

	if err != nil {
		d.log.Error(utils.RedStyle.Render("[gid=%d] Error while forwarding %v \"%s\" response %s (JupyterID=\"%s\") from kernel %s via %s: %s"),
			goid.Get(), socket.Type, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, socket.Name, err.Error())

		return err
	}

	//d.log.Debug("Successfully forwarded %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\") in %v: %v",
	//	socket.Type, msg.JupyterMessageType(), senderId, msg.JupyterMessageId(), sendDuration, messaging.FramesToString(zmqMsg.Frames))
	d.log.Debug("Successfully forwarded %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\") in %v.",
		socket.Type, msg.JupyterMessageType(), senderId, msg.JupyterMessageId(), sendDuration)
	return nil
}

func (d *ClusterGatewayImpl) updateStatisticsFromShellExecuteReply(trace *proto.RequestTrace) {
	if trace == nil {
		d.log.Warn("RequestTrace is nil when attempting to extract metrics/statistics from shell \"execute_reply\"")
		return
	}

	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	if trace.CudaInitMicroseconds > 0 {
		d.ClusterStatistics.CumulativeCudaInitMicroseconds.Add(float64(trace.CudaInitMicroseconds))
		d.ClusterStatistics.NumCudaRuntimesInitialized.Add(1)
	}

	if trace.ReplayTimeMicroseconds > 0 {
		d.ClusterStatistics.CumulativeReplayTimeMicroseconds.Add(float64(trace.ReplayTimeMicroseconds))
		d.ClusterStatistics.TotalNumReplays.Add(1)

		session, loaded := d.cluster.GetSession(trace.KernelId)
		if !loaded || session == nil {
			d.log.Warn("Could not find session \"%s\" specified in RequestTrace: %s", trace.KernelId, trace.String())
		} else {
			// Subtract 1 to exclude the last training event that just completed.
			d.ClusterStatistics.TotalNumCellsReplayed.Add(int64(session.NumTrainingEventsProcessed() - 1))
		}
	}

	//if trace.DownloadDependencyMicroseconds > 0 {
	//	d.ClusterStatistics.CumulativeTimeDownloadingDependenciesMicroseconds += float64(trace.DownloadDependencyMicroseconds)
	//	d.ClusterStatistics.NumTimesDownloadedDependencies.AddHost(1)
	//}

	if trace.DownloadModelMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeDownloadModelMicroseconds.Add(float64(trace.DownloadDatasetMicroseconds))
		d.ClusterStatistics.NumTimesDownloadModelMicroseconds.Add(1)
	}

	// Downloading the dataset overhead.
	if trace.DownloadDatasetMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeDownloadTrainingDataMicroseconds.Add(float64(trace.DownloadDatasetMicroseconds))
		d.ClusterStatistics.NumTimesDownloadTrainingDataMicroseconds.Add(1)
	}

	// Tokenization overhead. Only relevant for NLP datasets.
	if trace.TokenizeDatasetMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTokenizeDatasetMicroseconds.Add(float64(trace.TokenizeDatasetMicroseconds))
		d.ClusterStatistics.NumTimesTokenizeDatasetMicroseconds.Add(1)
	}

	if trace.UploadModelAndTrainingDataMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeUploadModelAndTrainingDataMicroseconds.Add(float64(trace.UploadModelAndTrainingDataMicroseconds))
		d.ClusterStatistics.NumTimesUploadModelAndTrainingDataMicroseconds.Add(1)
	}

	if trace.CopyFromCpuToGpuMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeCopyDataHostToDeviceMicroseconds.Add(float64(trace.CopyFromCpuToGpuMicroseconds))
		d.ClusterStatistics.NumTimesCopyDataHostToDeviceMicroseconds.Add(1)
	}

	if trace.CopyFromGpuToCpuMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeCopyDataDeviceToHostMicroseconds.Add(float64(trace.CopyFromGpuToCpuMicroseconds))
		d.ClusterStatistics.NumTimesCopyDataDeviceToHostMicroseconds.Add(1)
	}

	if trace.LeaderElectionTimeMicroseconds > 0 {
		d.ClusterStatistics.CumulativeLeaderElectionTimeMicroseconds.Add(float64(trace.LeaderElectionTimeMicroseconds))
	}

	if trace.RequestReceivedByKernelReplica > 0 && trace.ElectionCreationTime > 0 {
		preprocessDuration := trace.ElectionCreationTime - trace.RequestReceivedByKernelReplica
		d.ClusterStatistics.CumulativeKernelCreateElectionMillis.Add(float64(preprocessDuration))
	}

	if trace.ElectionCreationTime > 0 && trace.ElectionProposalPhaseStartTime > 0 {
		electionCreationDuration := trace.ElectionProposalPhaseStartTime - trace.ElectionCreationTime
		d.ClusterStatistics.CumulativeKernelCreateElectionMillis.Add(float64(electionCreationDuration))
	}

	if trace.ElectionProposalPhaseStartTime > 0 && trace.ElectionExecutionPhaseStartTime > 0 {
		proposalVotePhaseDuration := trace.ElectionExecutionPhaseStartTime - trace.ElectionProposalPhaseStartTime
		d.ClusterStatistics.CumulativeKernelProposalVotePhaseMillis.Add(float64(proposalVotePhaseDuration))
	}

	if trace.ReplySentByKernelReplica > 0 && trace.ExecutionEndUnixMillis > 0 {
		postprocessDuration := trace.ReplySentByKernelReplica - trace.ExecutionEndUnixMillis
		d.ClusterStatistics.CumulativeKernelPostprocessMillis.Add(float64(postprocessDuration))
	}

	d.ClusterStatistics.CumulativeExecutionTimeMicroseconds.Add(float64(trace.ExecutionTimeMicroseconds))

	if trace.MessageType == messaging.ShellExecuteRequest || trace.MessageType == messaging.ShellExecuteReply || trace.MessageType == messaging.ShellYieldRequest {
		d.ClusterStatistics.ExecuteRequestTraces = append(d.ClusterStatistics.ExecuteRequestTraces, trace)
	}
}

func (d *ClusterGatewayImpl) forwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	goroutineId := goid.Get()
	socket := from.Socket(typ)
	if socket == nil {
		d.log.Debug("Using router's %s socket to forward Jupyter \"%s\" response message \"%s\"",
			typ.String(), msg.JupyterMessageType(), msg.JupyterMessageId())
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if d.DebugMode {
		requestTrace, _, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(msg, time.Now(), d.log)
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

		// Extract the data from the RequestTrace.
		if typ == messaging.ShellMessage {
			d.updateStatisticsFromShellExecuteReply(requestTrace)
		}
	}

	d.log.Debug(utils.LightBlueStyle.Render("[gid=%d] Forwarding %v \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel \"%s\" via %s:\n%v"),
		goroutineId, typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), from.ID(), socket.Name, msg.StringFormatted())

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if msg.JupyterMessageType() == messaging.ShellExecuteReply {
		d.cleanUpBeforeForwardingExecuteReply(from, msg)
	}

	sendError := d.sendZmqMessage(msg, socket, from.ID())
	if sendError == nil {
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumJupyterRepliesSentByClusterGateway.Add(1)
		d.clusterStatisticsMutex.Unlock()
	}

	return sendError
}

// cleanUpBeforeForwardingExecuteReply is called when forwarding an "execute_reply" message back to the client.
//
// cleanUpBeforeForwardingExecuteReply performs any necessary "clean up" steps. The steps that are required ultimately
// depend upon the configured scheduling.Policy.
//
// For example, scheduling.Policy instances in which the scheduling.ContainerLifetime is scheduling.SingleTrainingEvent
// will either terminate the scheduling.KernelContainer instance(s) or return them to the warm container pool.
func (d *ClusterGatewayImpl) cleanUpBeforeForwardingExecuteReply(from router.Info, execReplyMsg *messaging.JupyterMessage) {
	// If the scheduling policy isn't a single-training-event policy, then we can just return immediately.
	if d.Scheduler().Policy().ContainerLifetime() != scheduling.SingleTrainingEvent {
		return
	}

	// Attempt to load the kernel. If we do, and we find that the kernel has no replicas and the message is designated
	// as being a failed "execute_request" message, then we can just return. There are no replicas to clean up, and
	// the execution failed.
	kernel, loaded := d.kernels.Load(from.ID())
	if !loaded {
		d.log.Error("Could not find Distributed Kernel Client for kernel \"%s\"...", from.ID())
		return
	}

	if kernel.Size() == 0 && execReplyMsg.IsFailedExecuteRequest {
		return
	}

	d.log.Debug("Kernel \"%s\" has finished training. Removing container.", from.ID())

	if !d.Scheduler().Policy().ReuseWarmContainers() {
		_ = d.removeAllReplicasOfKernel(kernel, true, false, false)
		return
	}

	// For the "middle ground" policy, we return the kernel's container to the warm container pool.
	if d.Scheduler().Policy().ReuseWarmContainers() {
		d.log.Debug("Reusing warm kernel container.")

		// Send 'reset' request.
		err := d.resetKernel(kernel, true)
		if err != nil {
			d.log.Error("Failed to reset kernel \"%s\": %v", kernel.ID(), err)
		}
	}
}

// resetKernelReply is used by the ClusterGatewayImpl's resetKernel and processResetKernelReplies methods.
//
// resetKernelReply associates a particular scheduling.KernelReplica with a "reset_kernel_reply" message.
type resetKernelReply struct {
	KernelReplica scheduling.KernelReplica
	Reply         *messaging.JupyterMessage
}

// resetKernel sends a "reset_kernel_request" to the specified kernel. This message instructs the kernel to, at a
// minimum, completely wipe its user namespace, removing all user-defined data/variables.
//
// The "reset_kernel_request" may also instruct the kernel replica to revert to a scheduling.PrewarmContainer (or to
// become a scheduling.PrewarmContainer, if it had never been one before).
func (d *ClusterGatewayImpl) resetKernel(kernel scheduling.Kernel, revertToPrewarm bool) error {
	d.log.Debug("Preparing to send \"%s\" to kernel \"%s\" with revertToPrewarm=%v.",
		messaging.ControlResetKernelRequest, kernel.ID(), revertToPrewarm)

	msgId := uuid.NewString()
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messaging.ControlResetKernelRequest, kernel.ID())

	content := map[string]interface{}{
		"revert_to_prewarm": revertToPrewarm,
	}

	err := frames.EncodeContent(&content)
	if err != nil {
		d.log.Error("Failed to encode content of IOPub status message for kernel \"%s\": %v", kernel.ID(), err)
		return err
	}

	var msg zmq4.Msg
	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return ErrFailedToVerifyMessage
	}

	respChan := make(chan *resetKernelReply, d.NumReplicas())
	startTime := time.Now()
	numRepliesReceived := atomic.Int32{}

	responseHandler := func(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)

		// Notify that all replies have been received.
		d.log.Debug("Received %s \"%s\" from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v.",
			typ.String(), messaging.ControlResetKernelReply, from.String(), msgId, latestNumRepliesReceived, kernel.Size(),
			time.Since(startTime))

		var replica scheduling.KernelReplica

		id := from.ReplicaID()
		if id >= 1 {
			replica, _ = kernel.GetReplicaByID(id)
		}

		resp := &resetKernelReply{
			KernelReplica: replica,
			Reply:         msg,
		}

		respChan <- resp

		return nil
	}

	jMsg := messaging.NewJupyterMessage(&msg)
	msgTyp := messaging.ControlMessage

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	err = kernel.RequestWithHandler(ctx, "Forwarding", msgTyp, jMsg, responseHandler, nil)
	if err != nil {
		d.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v",
			msgTyp.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)
		return err
	}

	replies := make([]*resetKernelReply, 0, kernel.Size())
	for numRepliesReceived.Load() < int32(d.NumReplicas()) {
		select {
		case <-ctx.Done():
			{
				ctxError := ctx.Err()

				nRepliesReceived := numRepliesReceived.Load()

				var errorMessage string
				if ctxError != nil {
					errorMessage = fmt.Sprintf("%v '%s' request for kernel '%s' failed after receiving %d/%d replies: %v",
						msgTyp.String(), jMsg.JupyterMessageId(), kernel.ID(), nRepliesReceived, kernel.Size(), ctxError)
				} else {
					errorMessage = fmt.Sprintf("%v '%s' request for kernel '%s' timed-out after receiving %d/%d replies.",
						msgTyp.String(), jMsg.JupyterMessageId(), kernel.ID(), nRepliesReceived, kernel.Size())
				}
				d.log.Error(errorMessage)

				err = types.ErrRequestTimedOut

				// If we received any replies, then we'll process the ones that we did receive.
				if len(replies) > 0 {
					processReplyErr := d.processResetKernelReplies(kernel, replies)
					if processReplyErr != nil {
						d.log.Error("Error while processing the %d reply/replies we did receive while resetting kernel \"%s\": %v",
							nRepliesReceived, kernel.ID(), processReplyErr)
						err = errors.Join(err, processReplyErr)
					}
				}

				return err
			}
		case resp := <-respChan:
			{
				replies = append(replies, resp)
			}
		}
	}

	return d.processResetKernelReplies(kernel, replies)
}

// processResetKernelReplies is called by resetKernel to process the replies send by the kernel replicas.
func (d *ClusterGatewayImpl) processResetKernelReplies(kernel scheduling.Kernel, replies []*resetKernelReply) error {
	prewarmer := d.Scheduler().ContainerPrewarmer()
	if prewarmer == nil {
		d.log.Warn("Container Prewarmer is nil...")
		return nil
	}

	// First, remove the replicas from the kernel.
	_ = d.removeAllReplicasOfKernel(kernel, true, false, true)

	errs := make([]error, 0, len(replies))
	for _, reply := range replies {
		msg := reply.Reply
		replica := reply.KernelReplica

		host := replica.Host()
		if host == nil {
			d.log.Error("Replica %d of kernel \"%s\" has a nil Host...", replica.ReplicaID(), replica.ID())
			errs = append(errs, scheduling.ErrNilHost)
			continue
		}

		var content map[string]interface{}
		err := msg.JupyterFrames.DecodeContent(&content)
		if err != nil {
			d.log.Error("Failed to decode content of \"%s\" message from replica %d of kernel \"%s\": %v",
				msg.JupyterMessageType(), replica.ReplicaID(), replica.ID(), err)
			errs = append(errs, err)
			continue
		}

		var kernelId string
		val, loaded := content["kernel_id"]
		if loaded {
			kernelId = val.(string)
		} else {
			kernelId = replica.ID()
		}

		kernelReplicaSpec := replica.KernelReplicaSpec().Clone()
		kernelReplicaSpec.Kernel.Id = kernelId

		prewarmedContainer := prewarm.NewPrewarmedContainerBuilder().
			WithHost(host).
			WithKernelConnectionInfo(jupyter.KernelConnectionInfoFromJupyterConnectionInfo(replica.ConnectionInfo())).
			WithKernelReplicaSpec(kernelReplicaSpec).
			WithPrewarmedContainerUsedCallback(nil).
			Build()

		err = prewarmer.ReturnPrewarmContainer(prewarmedContainer)
		if err != nil {
			d.log.Error("Failed to return container to pre-warm pool after resetting: %v.", err)
			errs = append(errs, err)
		}

		err = replica.Close()
		if err != nil {
			d.log.Error("Failed to close replica %d of kernel \"%s\" after demoting it to a %v container: %v",
				replica.ReplicaID(), replica.ID(), scheduling.PrewarmContainer, err)
			errs = append(errs, err)
		}
	}

	if len(errs) == 1 {
		return errs[0]
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// kernelReplicaResponseForwarder is used as the response handler for a variety of requests/forwarded messages.
//
// kernelReplicaResponseForwarder forwards a response from a particular replica of a kernel.
//
// kernelReplicaResponseForwarder forwards the given messaging.JupyterMessage to the remote entity connected to the
// socket of specified messaging.MessageType belonging to the specified scheduling.KernelReplicaInfo.
func (d *ClusterGatewayImpl) kernelReplicaResponseForwarder(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	if msg.RequestTrace != nil {
		requestTrace := msg.RequestTrace
		if requestTrace.ReplicaId != -1 && requestTrace.ReplicaId != from.ReplicaID() {
			d.log.Warn("Overwriting existing replica ID of %d with %d in RequestTrace for %s \"%s\" message %s (JupyterID=\"%s\")",
				requestTrace.ReplicaId, from.ReplicaID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		}

		msg.RequestTrace.ReplicaId = from.ReplicaID()
	}

	return d.forwardResponse(from, typ, msg)
}

// removeAllReplicasOfKernel is used to de-schedule the replicas of the given kernel without removing the kernel itself.
//
// This does not remove the kernel itself.
func (d *ClusterGatewayImpl) removeAllReplicasOfKernel(kernel scheduling.Kernel, inSeparateGoroutine bool,
	isIdleReclaim bool, noop bool) error {

	var (
		startedRemoving   bool
		descheduleAttempt scheduling.RemoveReplicaContainersAttempt
	)

	// We'll keep executing this loop as long as the replicas of the target kernel are not removed.
	// We break from the loop internally if (a) we claim ownership over a container removal descheduleAttempt, in which
	// case we  break out so that we can orchestrate the container removal descheduleAttempt, or (b) if we find that the
	// replicas are in fact removed. This may occur if, for example, a previous descheduleAttempt concludes.
	for {
		// Try to start a new descheduleAttempt at scheduling the replica container(s) of this kernel.
		startedRemoving, descheduleAttempt = kernel.InitRemoveReplicaContainersOperation()

		// If we started a new descheduleAttempt, then we'll break out of the loop and orchestrate the removal of the
		// containers of the replicas of the target kernel.
		if startedRemoving {
			d.log.Debug(
				utils.LightBlueStyle.Render(
					"Started 'descheduling' attempt to remove %d replica container(s) for kernel \"%s\"."),
				d.NumReplicas(), kernel.ID())
			break
		}

		// We didn't start a new removal descheduleAttempt.
		// If the returned descheduleAttempt is also nil, then that means that there was also not an active
		// descheduleAttempt. So, the replicas are apparently already removed.
		if descheduleAttempt == nil {
			d.log.Debug("Tried to start descheduleAttempt to remove replica container(s) for kernel \"%s\", "+
				"but apparently they're already removed.", kernel.ID())

			// Double-check that the kernel's replicas are removed. If they are, then we'll just return entirely.
			kernelSize := kernel.Size()
			if kernelSize == 0 {
				return nil
			}

			// This would be truly bizarre, but if this occurs, then we'll just sleep briefly and then try again...
			d.log.Error("Thought kernel \"%s\" was fully descheduled, but kernel has %d replica(s).",
				kernel.ID(), kernelSize)

			time.Sleep(time.Millisecond * (5 + time.Duration(rand.Intn(25))))
			continue
		}

		// If we did not start a new descheduleAttempt, then a previous descheduleAttempt must still be active.
		// We'll just wait for the descheduleAttempt to conclude.
		// If the scheduling is successful, then this will eventually return nil.
		// If the context passed to scheduleReplicas has a time-out, and we time out, then this will return an error.
		d.log.Debug("Found existing 'create replica containers' operation for kernel %s that began %v ago. "+
			"Waiting for operation to complete.", kernel.ID(), descheduleAttempt.TimeElapsed())

		return d.waitForDeschedulingToEnd(kernel, descheduleAttempt)
	}

	// doRemoveReplicas removes the kernel's replicas and returns an error if one occurs.
	doRemoveReplicas := func() error {
		err := kernel.RemoveAllReplicas(d.cluster.Placer().Reclaim, noop, isIdleReclaim)
		if err != nil {
			d.log.Error("Failed to remove all replicas of kernel \"%s\" because: %v", kernel.ID(), err)
		}

		setDoneErr := descheduleAttempt.SetDone(err /* will be nil on success */)
		if setDoneErr != nil {
			d.log.Error("Error while calling SetDone on deschedule attempt for kernel \"%s\": %v",
				kernel.ID(), setDoneErr)
		}

		return err // Will be nil on success.
	}

	// Spawn a separate goroutine to execute the doRemoveReplicas function if we've been instructed to do so.
	if inSeparateGoroutine {
		go func() {
			// RemoveHost the replicas.
			_ = doRemoveReplicas()
		}()

		return nil
	}

	// RemoveHost the replicas.
	err := doRemoveReplicas()

	// This will be nil if de-schedule was successful,
	// or if the caller specified that we should use a separate goroutine for the replica removal.
	if err != nil {
		go d.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to RemoveHost One or More Replicas of kernel \"%s\"",
			kernel.ID()), err.Error())

		return err
	}

	return nil
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
	// Hosts and newKernels may contact other gateways to restore status.
	close(d.cleaned)
}

func (d *ClusterGatewayImpl) listKernels() (*proto.ListKernelsResponse, error) {
	resp := &proto.ListKernelsResponse{
		Kernels: make([]*proto.DistributedJupyterKernel, 0, max(d.kernelIdToKernel.Len(), 1)),
	}

	d.mu.RLock()
	kernels := make([]scheduling.Kernel, 0, d.kernelIdToKernel.Len())
	d.kernelIdToKernel.Range(func(id string, kernel scheduling.Kernel) bool {
		kernels = append(kernels, kernel)
		return true
	})
	d.mu.RUnlock()

	for _, kernel := range kernels {
		respKernel := &proto.DistributedJupyterKernel{
			KernelId:            kernel.ID(),
			NumReplicas:         int32(kernel.Size()),
			Status:              kernel.Status().String(),
			AggregateBusyStatus: kernel.AggregateBusyStatus(),
			KernelSpec:          kernel.KernelSpec(),
		}

		replicas := make([]*proto.JupyterKernelReplica, 0, len(kernel.Replicas()))
		kernelReplicas := kernel.Replicas()
		for _, replica := range kernelReplicas {
			kernelReplica := &proto.JupyterKernelReplica{
				KernelId:  kernel.ID(),
				ReplicaId: replica.ReplicaID(),
				PodId:     replica.GetPodOrContainerId(),
				NodeId:    replica.NodeName(),
			}
			replicas = append(replicas, kernelReplica)
		}
		respKernel.Replicas = replicas

		resp.Kernels = append(resp.Kernels, respKernel)
	}

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

	d.cluster.RangeOverHosts(func(_ string, host scheduling.Host) (contd bool) {
		virtualDockerNode := host.ToVirtualDockerNode()
		nodes = append(nodes, virtualDockerNode)

		return true
	})

	d.cluster.RangeOverDisabledHosts(func(_ string, host scheduling.Host) (cont bool) {
		virtualDockerNode := host.ToVirtualDockerNode()
		nodes = append(nodes, virtualDockerNode)

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

	d.cluster.RangeOverHosts(func(hostId string, _ scheduling.Host) (contd bool) {
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

	scaleResult := result.(scheduler.ScaleOperationResult)
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

	scaleOutOperationResult := scaleResult.(*scheduler.ScaleOutOperationResult)
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

	scaleInOperationResult := scaleResult.(*scheduler.ScaleInOperationResult)
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

	scaleInOperationResult := scaleResult.(*scheduler.ScaleInOperationResult)
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

// GetSerializedClusterStatistics serializes and returns the current ClusterStatistics field of the ClusterGatewayImpl.
func (d *ClusterGatewayImpl) GetSerializedClusterStatistics(req *proto.ClusterStatisticsRequest) (*proto.ClusterStatisticsResponse, error) {
	var requestId string
	if req != nil {
		requestId = req.RequestId

		if req.UpdateFirst {
			d.gatherClusterStatistics()
		}
	}

	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)

	serializableClusterStatistics := d.ClusterStatistics.ConvertToSerializable()

	err := encoder.Encode(&serializableClusterStatistics)
	if err != nil {
		d.log.Error("Failed to encode ClusterStatistics: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &proto.ClusterStatisticsResponse{
		RequestId:                   requestId, // match ID in response to ID in request
		SerializedClusterStatistics: buffer.Bytes(),
	}

	return resp, nil
}

// ClearClusterStatistics resets the ClusterStatistics field.
//
// ClearClusterStatistics returns the value of the ClusterStatistics field before it was cleared.
//
// If there's an error returning the ClusterStatistics field, then an error will be returned instead.
// The ClusterStatistics field will NOT be cleared in this case.
func (d *ClusterGatewayImpl) ClearClusterStatistics() (*proto.ClusterStatisticsResponse, error) {
	resp, err := d.GetSerializedClusterStatistics(&proto.ClusterStatisticsRequest{UpdateFirst: true, RequestId: uuid.NewString()})
	if err != nil {
		return nil, err
	}

	d.lastFullStatisticsUpdate = time.Time{}
	d.ClusterStatistics = metrics.NewClusterStatistics()

	// Basically initialize the statistics with some values, but in a separate goroutine.
	go d.gatherClusterStatistics()

	return resp, nil
}

// UpdateClusterStatistics is passed to Distributed kernel Clients so that they may atomically update statistics.
func (d *ClusterGatewayImpl) UpdateClusterStatistics(updaterFunc func(statistics *metrics.ClusterStatistics)) {
	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	updaterFunc(d.ClusterStatistics)
}

// IncrementResourceCountsForNewHost is intended to be called when a Host is added to the Cluster.
// IncrementResourceCountsForNewHost will increment the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (d *ClusterGatewayImpl) IncrementResourceCountsForNewHost(host metrics.Host) {
	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	d.incrIdleResourcesForHost(host)
	d.incrSpecResourcesForHost(host)
}

// DecrementResourceCountsForRemovedHost is intended to be called when a Host is removed from the Cluster.
// DecrementResourceCountsForRemovedHost will decrement the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (d *ClusterGatewayImpl) DecrementResourceCountsForRemovedHost(host metrics.Host) {
	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	//d.log.Debug("Decrementing idle and spec resource counts for freshly-removed host %s (ID=%s)",
	//	host.GetNodeName(), host.GetID())

	d.decrIdleResourcesForHost(host)
	d.decrSpecResourcesForHost(host)
}

// resetResourceCounts sets all resource counts in the ClusterStatistics to 0.
//
// Important: resetResourceCounts is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) resetResourceCounts() {
	d.ClusterStatistics.IdleCPUs.Store(0.0)
	d.ClusterStatistics.IdleMemory.Store(0.0)
	d.ClusterStatistics.IdleGPUs.Store(0.0)
	d.ClusterStatistics.IdleVRAM.Store(0.0)

	d.ClusterStatistics.PendingCPUs.Store(0.0)
	d.ClusterStatistics.PendingMemory.Store(0.0)
	d.ClusterStatistics.PendingGPUs.Store(0.0)
	d.ClusterStatistics.PendingVRAM.Store(0.0)

	d.ClusterStatistics.CommittedCPUs.Store(0.0)
	d.ClusterStatistics.CommittedMemory.Store(0.0)
	d.ClusterStatistics.CommittedGPUs.Store(0.0)
	d.ClusterStatistics.CommittedVRAM.Store(0.0)

	d.ClusterStatistics.SpecCPUs.Store(0.0)
	d.ClusterStatistics.SpecMemory.Store(0.0)
	d.ClusterStatistics.SpecGPUs.Store(0.0)
	d.ClusterStatistics.SpecVRAM.Store(0.0)
}

// incrIdleResourcesForHost increments the idle resource counts of the ClusterStatistics for a particular host.
//
// Important: incrIdleResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) incrIdleResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		d.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing idle resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	d.ClusterStatistics.IdleCPUs.Add(host.IdleCPUs())
	d.ClusterStatistics.IdleMemory.Add(host.IdleMemoryMb())
	d.ClusterStatistics.IdleGPUs.Add(host.IdleGPUs())
	d.ClusterStatistics.IdleVRAM.Add(host.IdleVRAM())
}

// incrPendingResourcesForHost increments the pending resource counts of the ClusterStatistics for a particular host.
//
// Important: incrPendingResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) incrPendingResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		d.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing pending resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	d.ClusterStatistics.PendingCPUs.Add(host.PendingCPUs())
	d.ClusterStatistics.PendingMemory.Add(host.PendingMemoryMb())
	d.ClusterStatistics.PendingGPUs.Add(host.PendingGPUs())
	d.ClusterStatistics.PendingVRAM.Add(host.PendingVRAM())
}

// incrCommittedResourcesForHost increments the committed resource counts of the ClusterStatistics for a particular host.
//
// Important: incrCommittedResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) incrCommittedResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		d.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing committed resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	d.ClusterStatistics.CommittedCPUs.Add(host.CommittedCPUs())
	d.ClusterStatistics.CommittedMemory.Add(host.CommittedMemoryMb())
	d.ClusterStatistics.CommittedGPUs.Add(host.CommittedGPUs())
	d.ClusterStatistics.CommittedVRAM.Add(host.CommittedVRAM())
}

// incrSpecResourcesForHost increments the spec resource counts of the ClusterStatistics for a particular host.
//
// Important: incrSpecResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) incrSpecResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		d.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing spec resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	d.ClusterStatistics.SpecCPUs.Add(host.ResourceSpec().CPU())
	d.ClusterStatistics.SpecMemory.Add(host.ResourceSpec().MemoryMB())
	d.ClusterStatistics.SpecGPUs.Add(host.ResourceSpec().GPU())
	d.ClusterStatistics.SpecVRAM.Add(host.ResourceSpec().VRAM())
}

// incrementResourceCountsForHost increments the resource counts of the ClusterStatistics for a particular host.
//
// Important: incrementResourceCountsForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) incrementResourceCountsForHost(host scheduling.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.incrIdleResourcesForHost(host)
	d.incrPendingResourcesForHost(host)
	d.incrCommittedResourcesForHost(host)
	d.incrSpecResourcesForHost(host)
}

// decrIdleResourcesForHost decrements the idle resource counts of the ClusterStatistics for a particular host.
//
// Important: decrIdleResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) decrIdleResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.ClusterStatistics.IdleCPUs.Sub(host.IdleCPUs())
	d.ClusterStatistics.IdleMemory.Sub(host.IdleMemoryMb())
	d.ClusterStatistics.IdleGPUs.Sub(host.IdleGPUs())
	d.ClusterStatistics.IdleVRAM.Sub(host.IdleVRAM())
}

// decrPendingResourcesForHost decrements the pending resource counts of the ClusterStatistics for a particular host.
//
// Important: decrPendingResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) decrPendingResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.ClusterStatistics.PendingCPUs.Sub(host.PendingCPUs())
	d.ClusterStatistics.PendingMemory.Sub(host.PendingMemoryMb())
	d.ClusterStatistics.PendingGPUs.Sub(host.PendingGPUs())
	d.ClusterStatistics.PendingVRAM.Sub(host.PendingVRAM())
}

// decrCommittedResourcesForHost decrements the committed resource counts of the ClusterStatistics for a particular host.
//
// Important: decrCommittedResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) decrCommittedResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.ClusterStatistics.CommittedCPUs.Sub(host.CommittedCPUs())
	d.ClusterStatistics.CommittedMemory.Sub(host.CommittedMemoryMb())
	d.ClusterStatistics.CommittedGPUs.Sub(host.CommittedGPUs())
	d.ClusterStatistics.CommittedVRAM.Sub(host.CommittedVRAM())
}

// decrSpecResourcesForHost decrements the spec resource counts of the ClusterStatistics for a particular host.
//
// Important: decrSpecResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) decrSpecResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.ClusterStatistics.SpecCPUs.Sub(host.ResourceSpec().CPU())
	d.ClusterStatistics.SpecMemory.Sub(host.ResourceSpec().MemoryMB())
	d.ClusterStatistics.SpecGPUs.Sub(host.ResourceSpec().GPU())
	d.ClusterStatistics.SpecVRAM.Sub(host.ResourceSpec().VRAM())
}

// decrementResourceCountsForHost decrements the resource counts of the ClusterStatistics for a particular host.
//
// Important: decrementResourceCountsForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (d *ClusterGatewayImpl) decrementResourceCountsForHost(host scheduling.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	d.decrIdleResourcesForHost(host)
	d.decrPendingResourcesForHost(host)
	d.decrCommittedResourcesForHost(host)
	d.decrSpecResourcesForHost(host)
}

// recomputeResourceCounts iterates over all the hosts in the cluster and updates the related resource count stats.
//
// Important: recomputeResourceCounts is NOT thread safe. The cluster statistics mutex must be acquired first.
//
// recomputeResourceCounts returns a tuple such that:
// - 1st element is the number of non-empty hosts
// - 2nd element is the number of empty hosts
func (d *ClusterGatewayImpl) recomputeResourceCounts() (int, int) {
	d.resetResourceCounts()

	var numNonEmptyHosts, numEmptyHosts int

	// The aggregate, cumulative lifetime of the hosts that are currently running.
	var aggregateHostLifetimeOfRunningHosts float64

	d.cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		if !host.Enabled() {
			// If the host is not enabled, then just continue to the next host.
			return true
		}

		d.incrementResourceCountsForHost(host)

		if host.NumContainers() == 0 {
			numEmptyHosts += 1
		} else {
			numNonEmptyHosts += 1
		}

		aggregateHostLifetimeOfRunningHosts += time.Since(host.GetCreatedAt()).Seconds()

		return true
	})

	d.ClusterStatistics.AggregateHostLifetimeOfRunningHosts.Store(aggregateHostLifetimeOfRunningHosts)

	return numNonEmptyHosts, numEmptyHosts
}

// gatherClusterStatistics updates all the values in the ClusterStatistics field.
//
// gatherClusterStatistics is thread-safe.
func (d *ClusterGatewayImpl) gatherClusterStatistics() {
	d.clusterStatisticsMutex.Lock()

	now := time.Now()
	var lastTime time.Time // Last update time

	if d.lastFullStatisticsUpdate.IsZero() {
		lastTime = now // We're doing the first update
	} else {
		lastTime = d.lastFullStatisticsUpdate
	}

	//var cpuUtil, gpuUtil, memUtil, vramUtil, demandGpus float64
	var demandCpus, demandMem, demandGpus, demandVram float64

	numNonEmptyHosts, numEmptyHosts := d.recomputeResourceCounts()

	activeTime := time.Since(lastTime) * time.Duration(numNonEmptyHosts)
	idleTime := time.Since(lastTime) * time.Duration(numEmptyHosts)

	///////////
	// Hosts //
	///////////

	d.ClusterStatistics.Hosts.Store(int32(d.cluster.Len()))
	d.ClusterStatistics.NumDisabledHosts.Store(int32(d.cluster.NumDisabledHosts()))
	d.ClusterStatistics.NumEmptyHosts.Store(int32(numEmptyHosts))

	d.ClusterStatistics.CumulativeHostActiveTime.Add(activeTime.Seconds())
	d.ClusterStatistics.CumulativeHostIdleTime.Add(idleTime.Seconds())
	d.ClusterStatistics.AggregateHostLifetime.Add(time.Since(lastTime).Seconds() * float64(d.cluster.Len()))

	var numRunning, numIdle, numTraining, numStopped int
	d.cluster.RangeOverSessions(func(key string, value scheduling.UserSession) bool {
		demandCpus += value.ResourceSpec().CPU()
		demandMem += value.ResourceSpec().MemoryMB()
		demandGpus += value.ResourceSpec().GPU()
		demandVram += value.ResourceSpec().VRAM()

		if value.IsIdle() {
			numIdle += 1
			numRunning += 1
		} else if value.IsTraining() {
			numTraining += 1
			numRunning += 1
		} else if value.IsMigrating() {
			numRunning += 1
		} else if value.IsStopped() {
			numStopped += 1
		}

		return true
	})

	d.ClusterStatistics.NumSeenSessions.Store(int32(d.cluster.Sessions().Len()))
	d.ClusterStatistics.NumRunningSessions.Store(int32(numRunning))
	d.ClusterStatistics.NumIdleSessions.Store(int32(numIdle))
	d.ClusterStatistics.NumTrainingSessions.Store(int32(numTraining))
	d.ClusterStatistics.NumStoppedSessions.Store(int32(numStopped))

	d.ClusterStatistics.DemandGPUs.Store(demandCpus)
	d.ClusterStatistics.DemandMemMb.Store(demandMem)
	d.ClusterStatistics.DemandGPUs.Store(demandGpus)
	d.ClusterStatistics.DemandVRAMGb.Store(demandVram)

	///////////
	// Hosts //
	///////////

	d.ClusterStatistics.Hosts.Store(int32(d.cluster.Len()))
	d.ClusterStatistics.NumDisabledHosts.Store(int32(d.cluster.NumDisabledHosts()))

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////
	d.ClusterStatistics.SubscriptionRatio.Store(d.cluster.Scheduler().SubscriptionRatio())

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	//////////////
	// sessions //
	//////////////
	d.ClusterStatistics.NumNonTerminatedSessions.Store(d.numActiveKernels.Load())
	d.ClusterStatistics.NumRunningSessions.Store(int32(d.cluster.Sessions().Len()))

	d.lastFullStatisticsUpdate = time.Now()

	stats := d.ClusterStatistics

	d.clusterStatisticsMutex.Unlock()

	d.log.Debug("=== Updated cluster Statistics ===")
	d.log.Debug("Idle CPUs: %.0f, Idle Mem: %.0f, Idle GPUs: %.0f, Idle VRAM: %.0f",
		stats.IdleCPUs.Load(), stats.IdleMemory.Load(), stats.IdleGPUs.Load(), stats.IdleVRAM.Load())
	d.log.Debug("Pending CPUs: %.0f, Pending Mem: %.0f, Pending GPUs: %.0f, Pending VRAM: %.0f",
		stats.PendingCPUs.Load(), stats.PendingMemory.Load(), stats.PendingGPUs.Load(), stats.PendingVRAM.Load())
	d.log.Debug("Committed CPUs: %.0f, Committed Mem: %.0f, Committed GPUs: %.0f, Committed VRAM: %.0f",
		stats.CommittedCPUs.Load(), stats.CommittedMemory.Load(), stats.CommittedGPUs.Load(), stats.CommittedVRAM.Load())
	d.log.Debug("Spec CPUs: %.0f, Spec Mem: %.0f, Spec GPUs: %.0f, Spec VRAM: %.0f",
		stats.SpecCPUs.Load(), stats.SpecMemory.Load(), stats.SpecGPUs.Load(), stats.SpecVRAM.Load())
	d.log.Debug("NumSeenSessions: %d, NumRunningSessions: %d, NumNonTerminatedSessions: %d, NumTraining: %d, NumIdle: %d, NumStopped: %d.",
		stats.NumSeenSessions.Load(), stats.NumRunningSessions.Load(), stats.NumNonTerminatedSessions.Load(),
		stats.NumTrainingSessions.Load(), stats.NumIdleSessions.Load(), stats.NumStoppedSessions.Load())
	d.log.Debug("NumHosts: %d, NumDisabledHosts: %d, NumEmptyHosts: %d",
		stats.Hosts.Load(), stats.NumDisabledHosts.Load(), stats.NumEmptyHosts.Load())
}

// IncrementNumActiveExecutions increments the global counter of the number of active executions.
func (d *ClusterGatewayImpl) IncrementNumActiveExecutions() {
	d.numActiveTrainings.Add(1)
}

// DecrementNumActiveExecutions decrements the global counter of the number of active executions.
func (d *ClusterGatewayImpl) DecrementNumActiveExecutions() {
	d.numActiveTrainings.Add(-1)
}

// NumActiveExecutions returns the global number of active executions.
func (d *ClusterGatewayImpl) NumActiveExecutions() int32 {
	return d.numActiveTrainings.Load()
}

func (d *ClusterGatewayImpl) Cluster() scheduling.Cluster {
	return d.cluster
}

// GetJupyterMessage enables frontend clients to request a Jupyter message via gRPC in situations where
// the ZMQ message appears to have been delayed or dropped or otherwise lost in transit to the client.
func (d *ClusterGatewayImpl) GetJupyterMessage(_ context.Context, in *proto.GetJupyterMessageRequest) (*proto.GetJupyterMessageResponse, error) {
	kernel, loaded := d.kernels.Load(in.KernelId)
	if !loaded {
		d.log.Warn("GetJupyterMessage: unknown kernel \"%s\"", in.KernelId)
		return nil, d.errorf(fmt.Errorf("%w: \"%s\"", types.ErrKernelNotFound, in.KernelId))
	}

	if in.MessageType == messaging.ShellExecuteReply {
		return d.getExecuteReplyMessage(kernel, in)
	} else if in.MessageType == messaging.MessageTypeSMRLeadTask {
		return d.getSmrLeadTaskMessage(kernel, in)
	} else {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid message type: \"%s\"", in.MessageType))
	}
}

// getExecuteReplyMessage attempts to retrieve the specified "execute_reply" message associated
// with the given scheduling.Kernel and specified "execute_request" message.
func (d *ClusterGatewayImpl) getExecuteReplyMessage(kernel scheduling.Kernel, in *proto.GetJupyterMessageRequest) (*proto.GetJupyterMessageResponse, error) {
	executionManager := kernel.GetExecutionManager()

	msg, loaded := executionManager.GetExecuteReplyMessage(in.JupyterMessageId)
	if msg == nil || !loaded {
		errorMessage := fmt.Sprintf("no \"execute_reply\" message with ID=\"%s\"", in.JupyterMessageId)
		return nil, status.Error(codes.FailedPrecondition, errorMessage)
	}

	convertedMessage, err := msg.ToProto()
	if err != nil {
		d.log.Error("Failed to convert \"execute_reply\" message \"%s\" targeting kernel \"%s\" to proto version: %v",
			msg.JupyterMessageId(), kernel.ID(), err)
		d.log.Error("Message in question: %v", msg.JupyterFrames.StringFormatted())
		return nil, status.Error(codes.Internal, err.Error())
	}

	activeExecution := executionManager.GetActiveExecution(in.JupyterMessageId)
	if activeExecution == nil {
		d.log.Error("Successfully retrieved \"execute_reply\" message associated with \"execute_request\" message \"%s\"; however, could not find associated active execution...",
			in.JupyterMessageId)

		resp := &proto.GetJupyterMessageResponse{
			JupyterMessageId:     in.JupyterMessageId,
			MessageType:          in.MessageType,
			KernelId:             in.KernelId,
			ReceivedAtUnixMillis: -1,
			Message:              convertedMessage,
		}

		return resp, nil
	}

	resp := &proto.GetJupyterMessageResponse{
		JupyterMessageId:     in.JupyterMessageId,
		MessageType:          in.MessageType,
		KernelId:             in.KernelId,
		ReceivedAtUnixMillis: activeExecution.GetReceivedExecuteReplyAt().UnixMilli(),
		Message:              convertedMessage,
	}

	return resp, nil
}

// getExecuteReplyMessage attempts to retrieve the specified "smr_lead_task" message associated
// with the given scheduling.Kernel and specified "execute_request" message.
func (d *ClusterGatewayImpl) getSmrLeadTaskMessage(kernel scheduling.Kernel, in *proto.GetJupyterMessageRequest) (*proto.GetJupyterMessageResponse, error) {
	executionManager := kernel.GetExecutionManager()

	msg, loaded := executionManager.GetSmrLeadTaskMessage(in.JupyterMessageId)
	if msg == nil || !loaded {
		errorMessage := fmt.Sprintf("no \"smr_lead_task\" message with ID=\"%s\"", in.JupyterMessageId)
		return nil, status.Error(codes.FailedPrecondition, errorMessage)
	}

	convertedMessage, err := msg.ToProto()
	if err != nil {
		d.log.Error("Failed to convert \"smr_lead_task\" message \"%s\" targeting kernel \"%s\" to proto version: %v",
			msg.JupyterMessageId(), kernel.ID(), err)
		d.log.Error("Message in question: %v", msg.JupyterFrames.StringFormatted())
		return nil, status.Error(codes.Internal, err.Error())
	}

	activeExecution := executionManager.GetActiveExecution(in.JupyterMessageId)
	if activeExecution == nil {
		d.log.Error("Successfully retrieved \"smr_lead_task\" message associated with \"execute_request\" message \"%s\"; however, could not find associated active execution...",
			in.JupyterMessageId)

		resp := &proto.GetJupyterMessageResponse{
			JupyterMessageId:     in.JupyterMessageId,
			MessageType:          in.MessageType,
			KernelId:             in.KernelId,
			ReceivedAtUnixMillis: -1,
			Message:              convertedMessage,
		}

		return resp, nil
	}

	resp := &proto.GetJupyterMessageResponse{
		JupyterMessageId:     in.JupyterMessageId,
		MessageType:          in.MessageType,
		KernelId:             in.KernelId,
		ReceivedAtUnixMillis: activeExecution.GetReceivedSmrLeadTaskAt().UnixMilli(),
		Message:              convertedMessage,
	}

	return resp, nil
}
