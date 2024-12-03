package daemon

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/statistics"
	"github.com/shopspring/decimal"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	KubernetesKernelName = "kernel-%s"
	ConnectionFileFormat = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat     = "config-%s-*.json"     // "*" is a placeholder for random string

	ErrorHostname = "ERROR" // We return this from certain gRPC calls when there's an error.

	// TargetReplicaArg is passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg  = "target_replica"
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

	ErrUnsupportedMsgTypeForArtificialResponse = errors.New("unsupported message type for artificial response")
)

type GatewayDaemonConfig func(ClusterGateway)

// FailureHandler defines a recovery callback for panics.
// The primary purpose is simply to send a notification to the dashboard that a panic occurred before exiting.
// This makes error detection easier (i.e., it's immediately obvious when the system breaks as we're notified
// visually of the panic in the cluster dashboard).
type FailureHandler func(c scheduling.Kernel) error

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec, numReplicas int, hostId string,
		connectionInfo *jupyter.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string,
		debugMode bool, executionFailedCallback scheduling.ExecutionFailedCallback,
		executionLatencyCallback scheduling.ExecutionLatencyCallback, messagingMetricsProvider metrics.MessagingMetricsProvider,
		updater func(func(statistics *statistics.ClusterStatistics)), notificationCallback scheduling.NotificationCallback) scheduling.Kernel
}

// ClusterGateway is an interface for the "main" scheduler/manager of the distributed notebook Cluster.
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

	// KubernetesMode returns true if we're running in a Kubernetes Cluster (rather than as a docker-compose application).
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

	// GetLocalDaemonNodeIDs returns the IDs of the active DefaultSchedulingPolicy Daemon nodes.
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

	// policyKey refers to the scheduling policy/methodology/algorithm that the internalCluster Gateway is configured to use.
	policyKey scheduling.PolicyKey
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer
	router *router.Router

	// SchedulerOptions
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    *scheduling.SchedulerOptions

	DistributedClientProvider DistributedClientProvider

	// cluster provisioning related members
	listener net.Listener
	cluster  scheduling.Cluster

	// kernel members
	transport        string
	ip               string
	kernels          hashmap.HashMap[string, scheduling.Kernel] // Map with possible duplicate values. We map kernel ID and session ID to the associated kernel. There may be multiple sessions per kernel.
	kernelIdToKernel hashmap.HashMap[string, scheduling.Kernel] // Map from Kernel ID to client.DistributedKernelClient.
	kernelSpecs      hashmap.HashMap[string, *proto.KernelSpec]

	// numActiveKernels is the number of actively-running kernels.
	numActiveKernels atomic.Int32

	log logger.Logger

	// lifetime
	closed  int32
	cleaned chan struct{}

	// ClusterStatistics encapsulates a number of statistics/metrics.
	ClusterStatistics        *statistics.ClusterStatistics
	clusterStatisticsMutex   sync.Mutex
	lastFullStatisticsUpdate time.Time

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
	// number of DefaultSchedulingPolicy Daemon Docker nodes/containers.
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
	// io pub *messaging.Socket
	smrPort int

	// Map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting *hashmap.CornelkMap[string, chan struct{}]

	// kernelRegisteredNotifications is a map from notification ID to *proto.KernelRegistrationNotification
	// to keep track of the notifications that we've received so we can discard duplicates.
	kernelRegisteredNotifications *hashmap.CornelkMap[string, *proto.KernelRegistrationNotification]

	// Used to wait for an explicit notification that a particular node was successfully removed from its SMR cluster.
	// smrNodeRemovedNotifications *hashmap.CornelkMap[string, chan struct{}]

	// Hostname of the RemoteStorage NameNode. The SyncLog's RemoteStorage client will connect to this.
	remoteStorageEndpoint string

	// Kubernetes client. This is shared with the associated internalCluster Gateway.
	kubeClient scheduling.KubeClient

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerEventHandler scheduling.ContainerWatcher

	// remoteDockerEventAggregator listens for docker events that occur on remote nodes in Docker Swarm mode.
	remoteDockerEventAggregator *RemoteDockerEventAggregator

	// gRPC connection to the Dashboard.
	clusterDashboard proto.ClusterDashboardClient

	// Run via Docker on a single system rather than using the Kubernetes-based deployment.
	deploymentMode types.DeploymentMode

	// Docker client.
	dockerApiClient *dockerClient.Client

	// The name of the Docker network that the container is running within. Only used in Docker mode.
	dockerNetworkName string

	// MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements
	// for the ZMQ messages that we're forwarding back and forth between the Jupyter Server and the DefaultSchedulingPolicy Daemons.
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

	// hostSpec is the resource spec of Hosts in the Cluster
	hostSpec *types.DecimalSpec

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	// TODO: Make this an field of the ClusterGateway and LocalDaemon structs.
	//		 Update in forwardRequest and kernelResponseForwarder, rather than in here.
	RequestLog *metrics.RequestLog

	// The initial size of the cluster.
	// If more than this many DefaultSchedulingPolicy Daemons connect during the 'initial connection period',
	// then the extra nodes will be disabled until a scale-out event occurs.
	//
	// Specifying this as a negative number will disable this feature (so any number of hosts can connect).
	//
	// TODO: If a DefaultSchedulingPolicy Daemon connects "unexpectedly", then perhaps it should be disabled by default?
	initialClusterSize int

	// The initial connection period is the time immediately after the Cluster Gateway begins running during
	// which it expects all DefaultSchedulingPolicy Daemons to connect. If greater than N local daemons connect during this period,
	// where N is the initial cluster size, then those extra daemons will be disabled.
	//
	// TODO: If a DefaultSchedulingPolicy Daemon connects "unexpectedly", then perhaps it should be disabled by default?
	initialConnectionPeriod time.Duration

	// inInitialConnectionPeriod indicates whether we're still in the "initial connection period" or not.
	inInitialConnectionPeriod atomic.Bool

	// numHostsDisabledDuringInitialConnectionPeriod keeps track of the number of Host instances we disabled
	// during the initial connection period.
	numHostsDisabledDuringInitialConnectionPeriod int
}

func New(opts *jupyter.ConnectionInfo, clusterDaemonOptions *domain.ClusterDaemonOptions, configs ...GatewayDaemonConfig) *ClusterGatewayImpl {
	clusterGateway := &ClusterGatewayImpl{
		id:                             uuid.New().String(),
		connectionOptions:              opts,
		createdAt:                      time.Now(),
		transport:                      "tcp",
		ip:                             opts.IP,
		DebugMode:                      clusterDaemonOptions.CommonOptions.DebugMode,
		availablePorts:                 utils.NewAvailablePorts(opts.StartingResourcePort, opts.NumResourcePorts, 2),
		kernels:                        hashmap.NewCornelkMap[string, scheduling.Kernel](128),
		kernelIdToKernel:               hashmap.NewCornelkMap[string, scheduling.Kernel](128),
		kernelSpecs:                    hashmap.NewCornelkMap[string, *proto.KernelSpec](128),
		waitGroups:                     hashmap.NewCornelkMap[string, *registrationWaitGroups](128),
		kernelRegisteredNotifications:  hashmap.NewCornelkMap[string, *proto.KernelRegistrationNotification](128),
		cleaned:                        make(chan struct{}),
		smrPort:                        clusterDaemonOptions.SMRPort,
		kernelsStarting:                hashmap.NewCornelkMap[string, chan struct{}](64),
		remoteStorageEndpoint:          clusterDaemonOptions.RemoteStorageEndpoint,
		dockerNetworkName:              clusterDaemonOptions.DockerNetworkName,
		numResendAttempts:              clusterDaemonOptions.NumResendAttempts,
		MessageAcknowledgementsEnabled: clusterDaemonOptions.MessageAcknowledgementsEnabled,
		initialClusterSize:             clusterDaemonOptions.InitialClusterSize,
		initialConnectionPeriod:        time.Second * time.Duration(clusterDaemonOptions.InitialClusterConnectionPeriodSec),
		prometheusInterval:             time.Second * time.Duration(clusterDaemonOptions.PrometheusInterval),
		gatewayPrometheusManager:       nil,
		ClusterStatistics:              statistics.NewClusterStatistics(),
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
		metrics.ClusterGateway, clusterGateway.DebugMode, clusterGateway.updateClusterStatistics)

	if clusterDaemonOptions.PrometheusPort > 0 {
		clusterGateway.gatewayPrometheusManager = metrics.NewGatewayPrometheusManager(clusterDaemonOptions.PrometheusPort, clusterGateway)
		err := clusterGateway.gatewayPrometheusManager.Start()
		if err != nil {
			panic(err)
		}
	} else {
		clusterGateway.log.Warn("PrometheusPort is set to a negative number. Skipping initialization of Prometheus-related components.")
	}

	if !clusterDaemonOptions.CommonOptions.DisablePrometheusMetricsPublishing {
		clusterGateway.log.Debug("Initializing \"Prometheus Metrics Publisher\" goroutine now.")
		clusterGateway.publishPrometheusMetrics()
	} else {
		clusterGateway.log.Warn("\"Prometheus Metrics Publisher\" goroutine is disabled. Skipping initialization.")
	}

	clusterGateway.router.SetComponentId(clusterGateway.id)
	if clusterDaemonOptions.PrometheusPort > 0 {
		clusterGateway.router.AssignPrometheusManager(clusterGateway.gatewayPrometheusManager)

		// Initial values for these metrics.
		clusterGateway.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Set(0)
		clusterGateway.gatewayPrometheusManager.DemandGpusGauge.Set(0)
		clusterGateway.gatewayPrometheusManager.BusyGpusGauge.Set(0)
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
			clusterGateway.failureHandler = clusterGateway.defaultFailureHandler
		}
	case string(scheduling.Static):
		{
			clusterGateway.policyKey = scheduling.Static
			clusterGateway.log.Debug("Using the 'STATIC' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.staticSchedulingFailureHandler
		}
	case string(scheduling.DynamicV3):
		{
			clusterGateway.policyKey = scheduling.DynamicV3
			clusterGateway.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.dynamicV3FailureHandler
		}
	case string(scheduling.DynamicV4):
		{
			clusterGateway.policyKey = scheduling.DynamicV4
			clusterGateway.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.dynamicV4FailureHandler
		}
	case string(scheduling.FcfsBatch):
		{
			clusterGateway.policyKey = scheduling.FcfsBatch
			clusterGateway.log.Debug("Using the 'FCFS Batch' scheduling policy.")
			clusterGateway.failureHandler = clusterGateway.fcfsBatchSchedulingFailureHandler
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", clusterDaemonOptions.SchedulingPolicy))
		}
	}

	// Create the internalCluster Scheduler.
	clusterSchedulerOptions := clusterDaemonOptions.SchedulerOptions
	clusterGateway.hostSpec = &types.DecimalSpec{
		GPUs:      decimal.NewFromFloat(float64(clusterSchedulerOptions.GpusPerHost)),
		VRam:      decimal.NewFromFloat(scheduling.VramPerHostGb),
		Millicpus: decimal.NewFromFloat(scheduling.MillicpusPerHost),
		MemoryMb:  decimal.NewFromFloat(scheduling.MemoryMbPerHost),
	}

	schedulingPolicy, policyError := policy.GetSchedulingPolicy(&clusterDaemonOptions.SchedulerOptions)
	if policyError != nil {
		panic(policyError)
	}

	// Note: we don't construct the scheduling.Cluster struct within the switch statement below.
	// We construct the scheduling.Cluster struct immediately following the switch statement.
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

	clusterPlacer, err = schedulingPolicy.GetNewPlacer(clusterGateway.gatewayPrometheusManager)
	if err != nil {
		clusterGateway.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}

	// This is where we actually construct the scheduling.Cluster struct.
	distributedNotebookCluster, err := cluster.NewBuilder(clusterType).
		WithKubeClient(clusterGateway.kubeClient).
		WithHostSpec(clusterGateway.hostSpec).
		WithPlacer(clusterPlacer).
		WithSchedulingPolicy(schedulingPolicy).
		WithHostMapper(clusterGateway).
		WithKernelProvider(clusterGateway).
		WithClusterMetricsProvider(clusterGateway.gatewayPrometheusManager).
		WithNotificationBroker(clusterGateway).
		WithStatisticsUpdateProvider(clusterGateway.updateClusterStatistics).
		WithOptions(&clusterSchedulerOptions).
		BuildCluster()
	if err != nil {
		clusterGateway.log.Error("Failed to construct scheduling.Cluster: %v", err)
		panic(err)
	}

	clusterGateway.cluster = distributedNotebookCluster

	if clusterGateway.gatewayPrometheusManager != nil {
		clusterGateway.gatewayPrometheusManager.ClusterSubscriptionRatioGauge.Set(clusterGateway.cluster.SubscriptionRatio())
	}

	// If initialClusterSize is specified as a negative number, then the feature is disabled, so we only bother
	// setting inInitialConnectionPeriod to true and creating a goroutine to eventually set inInitialConnectionPeriod
	// to false if the feature is enabled in the first place.
	if clusterGateway.initialClusterSize >= 0 {
		clusterGateway.inInitialConnectionPeriod.Store(true)

		go func() {
			clusterGateway.log.Debug("Initial Connection Period will end in %v.", clusterGateway.initialConnectionPeriod)
			time.Sleep(clusterGateway.initialConnectionPeriod)
			clusterGateway.inInitialConnectionPeriod.Store(false)
			clusterGateway.log.Debug("Initial Connection Period has ended after %v. Cluster size: %d.",
				clusterGateway.initialConnectionPeriod, clusterGateway.cluster.Len())
		}()
	} else {
		clusterGateway.log.Debug("Initial Cluster Size specified as negative number (%d). "+
			"Disabling 'initial connection period' feature.", clusterGateway.initialClusterSize)
		clusterGateway.inInitialConnectionPeriod.Store(false) // It defaults to false, so this is unnecessary.
	}

	clusterGateway.ClusterStatistics.CumulativeNumHostsProvisioned = clusterGateway.initialClusterSize
	clusterGateway.gatherClusterStatistics()

	return clusterGateway
}

func (d *ClusterGatewayImpl) SetDistributedClientProvider(provider DistributedClientProvider) {
	d.DistributedClientProvider = provider
}

func (d *ClusterGatewayImpl) SetClusterOptions(options *scheduling.SchedulerOptions) {
	d.ClusterOptions = options
}

// SendErrorNotification sends an 'error' notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) SendErrorNotification(errorName string, errorMessage string) {
	go d.notifyDashboardOfError(errorName, errorMessage)
}

// SendInfoNotification sends an 'info' notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) SendInfoNotification(title string, message string) {
	go d.notifyDashboardOfInfo(title, message)
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
		if d.replicasShouldBeRunning() {
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
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.Sessions()[0])

	// If DebugMode is enabled, then add a buffers frame with a RequestTrace.
	var requestTrace *proto.RequestTrace
	if d.DebugMode {
		frames.Frames = append(frames.Frames, make([]byte, 0))

		requestTrace = proto.NewRequestTrace(msgId, messageType, kernelId)

		//startTimeUnixMilliseconds := in.CreatedAtTimestamp.AsTime().UnixMicro()
		//endTimeUnixMilliseconds := receivedAt.UnixMicro()
		//requestTrace.Traces = append(requestTrace.Traces, &proto.Trace{
		//	Id:                   uuid.NewString(),
		//	Name:                 "dashboard_to_gateway",
		//	StartTimeUnixMicro:   startTimeUnixMilliseconds,
		//	EndTimeUnixMicro:     endTimeUnixMilliseconds,
		//	StartTimestamp:       in.CreatedAtTimestamp,
		//	EndTimestamp:         timestamppb.New(time.UnixMicro(receivedAt.UnixMicro())),
		//	DurationMicroseconds: endTimeUnixMilliseconds - startTimeUnixMilliseconds,
		//})

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
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v", kernelId, kernel.ConnectionInfo().SignatureScheme, err)
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

	jMsg := messaging.NewJupyterMessage(&msg)
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

	requestTraces := make([]*proto.RequestTrace, 0, d.NumReplicas())

	for numRepliesReceived.Load() < int32(d.NumReplicas()) {
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
		// traces := reqTrace.Traces
		// lastTrace := traces[len(traces) - 1]

		//reqTrace.Traces = append(reqTrace.Traces, &proto.Trace{
		//	Id:                   uuid.NewString(),
		//	Name:                 "gateway_to_dashboard",
		//	StartTimeUnixMicro:   replySentByGateway.UnixMicro(),
		//	EndTimeUnixMicro:     -1,
		//	StartTimestamp:       timestamppb.New(replySentByGateway),
		//	EndTimestamp:         nil,
		//	DurationMicroseconds: -1,
		//})
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
func (d *ClusterGatewayImpl) GetHostsOfKernel(kernelId string) ([]scheduling.Host, error) {
	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		return nil, types.ErrKernelNotFound
	}

	hosts := make([]scheduling.Host, 0, len(kernel.Replicas()))
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

// acceptHostConnection accepts an incoming connection from a DefaultSchedulingPolicy Daemon and establishes a bidirectional
// gRPC connection with that DefaultSchedulingPolicy Daemon.
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
		return nil, nil, nil, err
	}

	// Create a new session to replace the incoming connection.
	conn, err := cliSession.Accept()
	if err != nil {
		d.log.Error("Failed to wait for the replacement of host scheduler connection: %v", err)
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

// restoreHost is used to restore an existing Host when a DefaultSchedulingPolicy Daemon loses connection with the Cluster Gateway
// and then reconnects.
//
// This will return nil on success.
func (d *ClusterGatewayImpl) restoreHost(host scheduling.Host) error {
	d.log.Warn("Newly-connected DefaultSchedulingPolicy Daemon actually already exists.")

	// Sanity check.
	if host == nil {
		errorMessage := "We're supposed to restore a DefaultSchedulingPolicy Daemon, but the host with which we would perform the restoration is nil...\n"
		d.notifyDashboardOfError("Failed to Re-Register DefaultSchedulingPolicy Daemon", errorMessage)
		log.Fatalf(utils.RedStyle.Render(errorMessage))
	}

	// Restore the DefaultSchedulingPolicy Daemon.
	// This replaces the gRPC connection of the existing Host struct with that of a new one,
	// as well as a few other fields.
	registered, loaded := d.cluster.GetHost(host.GetID())
	if loaded {
		err := registered.Restore(host, d.localDaemonDisconnected)
		if err != nil {
			d.log.Error("Error while restoring host %v: %v", host, err)
			return err
		}

		d.log.Debug("Successfully restored existing DefaultSchedulingPolicy Daemon %s (ID=%s).", registered.GetNodeName(), registered.GetID())
		go d.notifyDashboardOfInfo(
			fmt.Sprintf("DefaultSchedulingPolicy Daemon %s Reconnected", registered.GetNodeName()),
			fmt.Sprintf("DefaultSchedulingPolicy Daemon %s on node %s has reconnected to the Cluster Gateway.",
				registered.GetID(),
				registered.GetNodeName()))
		return nil
	}

	errorMessage := fmt.Sprintf("Supposedly existing DefaultSchedulingPolicy Daemon (re)connected, but cannot find associated Host struct... "+
		"Node claims to be DefaultSchedulingPolicy Daemon %s (ID=%s).", host.GetID(), host.GetNodeName())
	d.log.Error(errorMessage)

	go d.notifyDashboardOfError(
		fmt.Sprintf("DefaultSchedulingPolicy Daemon %s Restoration has Failed", registered.GetNodeName()),
		fmt.Sprintf(errorMessage,
			registered.GetID(),
			registered.GetNodeName()))

	// TODO: We could conceivably just register the Host as a new DefaultSchedulingPolicy Daemon, despite the fact
	// 		 that the Host thinks it already exists. We may have to re-contact the Host through the
	//	     SetID procedure, though. We'll at least have to re-create the Host struct, as it was only
	//		 populated with some of the required fields.
	return entity.ErrRestorationFailed
}

// registerNewHost is used to register a new Host (i.e., DefaultSchedulingPolicy Daemon) with the Cluster after the Host connects
// to the Cluster Gateway.
//
// This will return nil on success.
func (d *ClusterGatewayImpl) registerNewHost(host scheduling.Host) error {
	if !host.IsProperlyInitialized() {
		log.Fatalf(utils.RedStyle.Render("Newly-connected Host %s (ID=%s) was NOT properly initialized..."),
			host.GetNodeName(), host.GetID())
	}

	d.log.Info("Incoming DefaultSchedulingPolicy Daemon %s (ID=%s) connected", host.GetNodeName(), host.GetID())

	if d.inInitialConnectionPeriod.Load() && d.cluster.Len() >= d.initialClusterSize {
		d.numHostsDisabledDuringInitialConnectionPeriod += 1
		d.log.Debug("We are still in the Initial Connection Period, and cluster has size %d. Disabling "+
			"newly-connected host %s (ID=%s). Disabled %d host(s) during initial connection period so far.",
			d.cluster.Len(), host.GetNodeName(), host.GetID(), d.numHostsDisabledDuringInitialConnectionPeriod)
		err := host.Disable()
		if err != nil {
			// As of right now, the only reason Disable will fail/return an error is if the Host is already disabled.
			d.log.Warn("Failed to disable newly-connected host %s (ID=%s) because: %v", host.GetNodeName(), host.GetID(), err)
		}
	}

	err := d.cluster.NewHostAddedOrConnected(host)
	if err != nil {
		d.log.Error("Error while adding newly-connected host %s (ID=%s) to the Cluster: %v", host.GetNodeName(), host.GetID(), err)
		return err
	}

	go d.notifyDashboardOfInfo("DefaultSchedulingPolicy Daemon Connected", fmt.Sprintf("DefaultSchedulingPolicy Daemon %s (ID=%s) has connected to the Cluster Gateway.", host.GetNodeName(), host.GetID()))

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
	host, err := entity.NewHostWithConn(uuid.NewString(), incoming.RemoteAddr().String(), scheduling.MillicpusPerHost,
		scheduling.MemoryMbPerHost, scheduling.VramPerHostGb, d.cluster.NumReplicas(), d.cluster, d.gatewayPrometheusManager,
		gConn, d.Scheduler().Policy().ResourceBindingMode(), d.localDaemonDisconnected)

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

	registrationError := d.registerNewHost(host)
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

	host := targetReplica.Context().Value(client.CtxKernelHost).(scheduling.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	d.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s. Sending request to DefaultSchedulingPolicy Daemon of replica %d.", nodeId, kernelId, targetReplica.ReplicaID())
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

// SmrReady is an RPC handler called by the DefaultSchedulingPolicy Daemon to the Cluster Gateway to notify the Gateway that a
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
	// addReplicaOp.ReplicaJoinedSmrChannel() <- struct{}{}
	addReplicaOp.SetReplicaJoinedSMR()

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

// SmrNodeAdded is an RPC function called by the DefaultSchedulingPolicy Daemon to the Cluster Gateway when the DefaultSchedulingPolicy Daemon
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

	//kernelId := replicaInfo.KernelId
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
func (d *ClusterGatewayImpl) kernelReconnectionFailed(kernel scheduling.KernelReplica, msg *messaging.JupyterMessage, reconnectionError error) { /* client scheduling.Kernel,  */
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending \"%s\" message: %v",
		kernel.ReplicaID(), kernel.ID(), messageType, reconnectionError)
	d.log.Error(errorMessage)

	go d.notifyDashboardOfError("Connection to Kernel Lost & Reconnection Failed", errorMessage)
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

	d.notifyDashboardOfError("Connection to Kernel Lost, Reconnection Succeeded, but Request Resubmission Failed", errorMessage)
}

func (d *ClusterGatewayImpl) executionFailed(c scheduling.Kernel) error {
	execution := c.ActiveExecution()
	d.log.Warn("Execution %s (attempt %d) failed for kernel %s.",
		execution.GetExecutionId(), execution.GetAttemptId(), c.ID())

	return d.failureHandler(c)
}

func (d *ClusterGatewayImpl) defaultFailureHandler(_ scheduling.Kernel) error {
	d.log.Warn("There is no failure handler for the DEFAULT scheduling policy.")
	return fmt.Errorf("there is no failure handler for the DEFAULT scheduling policy; cannot handle error")
}

func (d *ClusterGatewayImpl) fcfsBatchSchedulingFailureHandler(_ scheduling.Kernel) error {
	d.log.Warn("There is no failure handler for the FCFS Batch scheduling policy.")
	return fmt.Errorf("there is no failure handler for the FCFS Batch policy; cannot handle error")
}

func (d *ClusterGatewayImpl) notifyDashboard(notificationName string, notificationMessage string, typ messaging.NotificationType) {
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
	d.log.Warn("DefaultSchedulingPolicy Daemon %s (Node %s) has disconnected. Removing from Cluster.", localDaemonId, nodeName)
	_, err = d.RemoveHost(context.TODO(), &proto.HostId{
		Id:       localDaemonId,
		NodeName: nodeName, /* Not needed */
	})

	if err != nil {
		d.log.Error("Error while removing local daemon %s (node: %s): %v", localDaemonId, nodeName, err)
	}

	go d.notifyDashboard(errorName, errorMessage, messaging.WarningNotification)

	return err
}

// Used to issue an "info" notification to the internalCluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfInfo(notificationName string, message string) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
			Id:               uuid.NewString(),
			Title:            notificationName,
			Message:          message,
			NotificationType: int32(messaging.InfoNotification),
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
			NotificationType: int32(messaging.ErrorNotification),
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
			NotificationType: int32(messaging.WarningNotification),
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
func (d *ClusterGatewayImpl) staticSchedulingFailureHandler(kernel scheduling.Kernel) error {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplica := rand.Intn(kernel.Size()) + 1
	d.log.Debug(utils.LightBlueStyle.Render("Static Failure Handler: migrating replica %d of kernel %s now."),
		targetReplica, kernel.ID())

	// Notify the cluster dashboard that we're performing a migration.
	go d.notifyDashboardOfWarning(fmt.Sprintf("All Replicas of Kernel \"%s\" Have Proposed 'YIELD'", kernel.ID()),
		fmt.Sprintf("All replicas of kernel %s proposed 'YIELD' during code execution.", kernel.ID()))

	// TODO: There could be race conditions here with how we are creating and linking and assigning the ...
	// TODO: ... ActiveExecution structs here. That is, if we receive additional "execute_request" messages during ...
	// TODO: ... this process, then things could get messed-up. We need to put a big lock around this or something ...
	// TODO: ... Like, a lock around/in the DistributedKernelClient, specifically.

	activeExecution := kernel.ActiveExecution()
	if activeExecution == nil {
		d.log.Error("Could not find active execution for kernel %s after static scheduling failure.", kernel.ID())
		go d.notifyDashboardOfError("ErrActiveExecutionNotFound", "active execution for specified kernel could not be found")
		return ErrActiveExecutionNotFound
	}

	msg := activeExecution.Msg()

	// TODO(Ben): Pre-reserve resources on the host that we're migrating the replica to.
	// For now, we'll just let the standard scheduling logic handle things, which will prioritize the least-loaded host.
	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    int32(targetReplica),
			PersistentId: kernel.PersistentID(),
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
				targetReplica, kernel.ID(), err.Error())
			errorChan <- err
		} else {
			d.log.Debug(utils.GreenStyle.Render("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s."),
				targetReplica, kernel.ID(), resp.Hostname)
		}

		waitGroup.Done()
	}()

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

	signatureScheme := kernel.ConnectionInfo().SignatureScheme
	if signatureScheme == "" {
		d.log.Warn("Kernel %s's signature scheme is blank. Defaulting to \"%s\"", messaging.JupyterSignatureScheme)
		signatureScheme = messaging.JupyterSignatureScheme
	}

	// Regenerate the signature.
	if _, err := msg.JupyterFrames.Sign(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		// Ignore the error; just log it.
		d.log.Warn("Failed to sign frames because %v", err)
	}

	// Ensure that the frames are now correct.
	if err := msg.JupyterFrames.Verify(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v': %v",
			signatureScheme, kernel.ConnectionInfo().Key, err)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", msg)
		return ErrFailedToVerifyMessage
	}

	// Now, we wait for the migration operation to proceed.
	waitGroup.Wait()
	select {
	case err := <-errorChan:
		{
			// If there was an error during execution, then we'll return that error rather than proceed.
			go d.notifyDashboardOfError(fmt.Sprintf("Failed to Migrate Replica of Kernel \"%s\"",
				kernel.ID()), err.Error())

			return err
		}
	default:
		{
			// Do nothing. The migration operation completed successfully.
		}
	}

	d.log.Debug(utils.LightBlueStyle.Render("Resubmitting 'execute_request' message targeting kernel %s now."), kernel.ID())
	err = d.ShellHandler(kernel, msg)
	if err != nil {
		d.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go d.notifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) dynamicV3FailureHandler(_ scheduling.Kernel) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

func (d *ClusterGatewayImpl) dynamicV4FailureHandler(_ scheduling.Kernel) error {
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

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		panic(err)
	}

	d.log.Debug("Allocating the following \"listen\" ports to kernel %s: %v", in.Id, listenPorts)

	// Initialize kernel with new context.
	kernel := d.DistributedClientProvider.NewDistributedKernelClient(context.Background(), in, d.NumReplicas(), d.id,
		d.connectionOptions, listenPorts[0], listenPorts[1], uuid.NewString(), d.DebugMode, d.executionFailed,
		d.executionLatencyCallback, d.gatewayPrometheusManager, d.updateClusterStatistics, d.notifyDashboard)

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

	// Create a new Session for scheduling purposes.
	resourceUtil := resource.NewEmptyUtilization().
		WithCpuUtilization(in.ResourceSpec.CPU()).
		WithMemoryUsageMb(in.ResourceSpec.MemoryMB()).
		WithNGpuUtilizationValues(in.ResourceSpec.Gpu, 0)
	session := entity.NewSessionBuilder().
		WithContext(context.Background()).
		WithID(kernel.ID()).
		WithKernelSpec(in).
		WithResourceUtilization(resourceUtil).
		WithTrainingTimeSampleWindowSize(d.ClusterOptions.ExecutionTimeSamplingWindow).
		WithMigrationTimeSampleWindowSize(d.ClusterOptions.MigrationTimeSamplingWindow).
		Build()

	d.cluster.AddSession(kernel.ID(), session)

	// Assign the Session to the DistributedKernelClient.
	kernel.SetSession(session)

	return kernel, nil
}

// scheduleReplicas actually scheduled the replicas of the specified kernel.
func (d *ClusterGatewayImpl) scheduleReplicas(ctx context.Context, kernel scheduling.Kernel, in *proto.KernelSpec) error {
	d.log.Debug("Scheduling replica container(s) of kernel %s now.", kernel.ID())

	// Record that this kernel is starting.
	kernelStartedChan := make(chan struct{})
	d.kernelsStarting.Store(in.Id, kernelStartedChan)
	created := newRegistrationWaitGroups(d.NumReplicas())
	d.waitGroups.Store(in.Id, created)

	err := d.cluster.Scheduler().DeployKernelReplicas(ctx, in, []scheduling.Host{ /* No blacklisted hosts */ })
	if err != nil {
		d.log.Error("Error while deploying infrastructure for new kernel %s's: %v", in.Id, err)

		// Clean up everything since we failed to create the kernel.
		d.kernelIdToKernel.Delete(in.Id)
		d.kernelsStarting.Delete(in.Id)
		d.kernels.Delete(in.Id)
		d.kernelSpecs.Delete(in.Id)
		d.waitGroups.Delete(in.Id)

		listenPorts := []int{kernel.ShellListenPort(), kernel.IOPubListenPort()}
		returnPortsErr := d.availablePorts.ReturnPorts(listenPorts)
		if returnPortsErr != nil {
			d.log.Warn("Failed to return listen ports %d and %d after failing to launch new kernel \"%s\" because: %v",
				kernel.ShellListenPort(), kernel.IOPubListenPort(), in.Id, err)
		}

		closeKernelError := kernel.Close()
		if closeKernelError != nil {
			d.log.Warn("Error while closing failed-to-be-created kernel \"%s\": %v", in.Id, closeKernelError)
		}

		// Only notify if there's an "actual" error.
		if !errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
			go d.notifyDashboardOfError(fmt.Sprintf("Failed to Create Kernel \"%s\"", in.Id), err.Error())
		}

		// The error should already be compatible with gRPC. But just in case it isn't...
		_, ok := status.FromError(err)
		if !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return err
	}

	// Wait for all replicas to be created.
	// Note that creation just means that the Container/Pod was created.
	// It does not mean that the Container/Pod has entered the active/running state.
	d.log.Debug("Waiting for replicas of new kernel %s to register. Number of kernels starting: %d.",
		in.Id, d.kernelsStarting.Len())
	created.Wait()
	d.log.Debug("All %d replicas of new kernel %s have been created and registered with their local daemons. Waiting for replicas to join their SMR cluster now.",
		d.NumReplicas(), in.Id)

	// Wait until all replicas have started.
	for i := 0; i < d.NumReplicas(); i++ {
		<-kernelStartedChan // Wait for all replicas to join their SMR cluster.
	}

	// Clean up.
	d.kernelsStarting.Delete(in.Id)
	d.log.Debug("All %d replicas of new kernel %s have registered and joined their SMR cluster. Number of kernels starting: %d.",
		d.NumReplicas(), in.Id, d.kernelsStarting.Len())

	// Sanity check.
	if kernel.Size() == 0 {
		return status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// Map all the sessions (probably just one?) to the kernel client.
	for _, sess := range kernel.Sessions() {
		d.log.Debug("Storing kernel %v under session ID \"%s\".", kernel, sess)
		d.kernels.Store(sess, kernel)
	}

	return nil
}

func (d *ClusterGatewayImpl) sendStatusMessage(kernel scheduling.Kernel, executionState string) (*messaging.JupyterMessage, error) {
	var (
		msg   zmq4.Msg
		err   error
		msgId = uuid.NewString()
	)
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageIdAndIdentity(msgId, "status", kernel.ID(), "status")

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
		return nil, ErrFailedToVerifyMessage
	}

	jMsg := messaging.NewJupyterMessage(&msg)

	d.log.Debug("Sending io/iopub message %s (JupyterID=\"%s\") encoding execution state/status of \"%s\" to client of kernel \"%s\" now: %v",
		jMsg.RequestId, jMsg.JupyterMessageId(), executionState, kernel.ID(), jMsg)

	err = kernel.(*client.DistributedKernelClient).SendIOMessage(jMsg)
	return jMsg, err
}

// sendIoPubStatusesOnStart is used by scheduling policies that only create kernel containers when processing
// a training event. The Jupyter Server expects at least one IOPub message to be broadcast during the start-up
// procedure. This satisfies that requirement.
func (d *ClusterGatewayImpl) sendIoPubStatusesOnStart(kernel scheduling.Kernel) error {
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
	} else {
		d.log.Debug("Sent IOPub message: %v", msg)
	}

	// Send another "status" message in ~2 seconds with the "idle" status.
	go func(sleepInterval time.Duration) {
		time.Sleep(sleepInterval)

		msg, err = d.sendStatusMessage(kernel, "idle")
		if err != nil {
			d.log.Error("Failed to send 'idle' IOPub status message after waiting %v during creation of kernel \"%s\": %v",
				sleepInterval, kernel.ID(), err)
		} else {
			d.log.Debug("Sent IOPub message: %v", msg)
		}
	}(time.Millisecond * 2)

	return nil
}

// StartKernel launches a new kernel.
func (d *ClusterGatewayImpl) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	startTime := time.Now()
	d.log.Info("ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%v]. NumKernelsStarting: %d. Spec: %v.",
		in.Id, in.Session, in.ResourceSpec, d.kernelsStarting.Len(), in)

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
			return nil, ensureErrorGrpcCompatible(err, codes.Unknown)
		}
	} else {
		d.log.Info("Restarting %v...", kernel)
		kernel.BindSession(in.Session)
	}

	d.kernelIdToKernel.Store(in.Id, kernel)
	d.kernels.Store(in.Id, kernel)
	d.kernelSpecs.Store(in.Id, in)

	if d.Scheduler().Policy().ContainerLifetime() == scheduling.SingleTrainingEvent {
		d.log.Debug("Will wait to schedule container(s) for kernel %s until we receive an 'execute_request'.", in.Id)

		// Since we won't be adding any replicas to the kernel right now, we need to assign a value to the
		// SignatureScheme and Key fields of the ConnectionInfo used by the DistributedKernelClient's server.
		//
		// If we skipped this step, then the kernel would not be able to sign messages correctly.
		kernel.SetSignatureScheme(in.SignatureScheme)
		kernel.SetKernelKey(in.Key)

		err = d.sendIoPubStatusesOnStart(kernel)
		if err != nil {
			d.log.Error("Failed to send IOPub status messages during start-up of kernel %s: %v", in.Id, err)
			return nil, ensureErrorGrpcCompatible(err, codes.Unknown)
		}
	} else {
		err = d.scheduleReplicas(ctx, kernel, in)
		if err != nil {
			d.log.Error("Failed to schedule replica container(s) of new kernel %s at creation time: %v", in.Id, err)
			return nil, ensureErrorGrpcCompatible(err, codes.Unknown)
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

	d.log.Info("Kernel(%s) started after %v: %v", kernel.ID(), time.Since(startTime), info)

	session, ok := d.cluster.GetSession(kernel.ID())
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

	d.newKernelCreated(startTime, kernel.ID())

	d.log.Info("Returning from ClusterGatewayImpl::StartKernel for kernel %s after %v.", kernel.ID(), time.Since(startTime))

	return info, nil
}

// newKernelCreated is to be called from StartKernel if and when the procedure succeeds.
//
// newKernelCreated pushes some metrics to Kubernetes and sends a notification to the Dashboard.
func (d *ClusterGatewayImpl) newKernelCreated(startTime time.Time, kernelId string) {
	// Tell the Dashboard that the kernel has successfully started running.
	go d.notifyDashboard("Kernel Started", fmt.Sprintf("Kernel %s has started running. Launch took approximately %v from when the Cluster Gateway began processing the 'create kernel' request.",
		kernelId, time.Since(startTime)), messaging.SuccessNotification)

	numActiveKernels := d.numActiveKernels.Add(1)

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.NumIdleSessions += 1
	d.clusterStatisticsMutex.Unlock()

	if d.gatewayPrometheusManager != nil {
		d.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))
		d.gatewayPrometheusManager.TotalNumKernelsCounterVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Inc()
		d.gatewayPrometheusManager.KernelCreationLatencyHistogram.Observe(float64(time.Since(startTime).Milliseconds()))
	}
}

// Handle a registration notification from a new kernel replica that was created during an add-replica/migration operation.
//
// IMPORTANT: This must be called with the main mutex held. Otherwise, there are race conditions with the
// addReplicaNewPodOrContainerNotifications field.
//
// IMPORTANT: This will release the main mutex before returning.
func (d *ClusterGatewayImpl) handleAddedReplicaRegistration(in *proto.KernelRegistrationNotification, kernel scheduling.Kernel, waitGroup *registrationWaitGroups) (*proto.KernelRegistrationNotificationResponse, error) {
	// We load-and-delete the entry so that, if we migrate the same replica again in the future, then we can't load
	// the old AddReplicaOperation struct...
	key := fmt.Sprintf("%s-%d", in.KernelId, in.ReplicaId)
	// addReplicaOp, ok := d.addReplicaOperationsByKernelReplicaId.LoadAndDelete(key)
	addReplicaOp, ok := d.Scheduler().GetAddReplicaOperationManager().LoadAndDelete(key)

	if !ok {
		errorMessage := fmt.Errorf("could not find AddReplicaOperation struct under key \"%s\"", key)
		d.log.Error(errorMessage.Error())
		d.notifyDashboardOfError("Kernel Registration Error", errorMessage.Error())

		return nil, errorMessage
	}

	if d.DockerMode() {
		dockerContainerId := in.DockerContainerId
		if dockerContainerId == "" {
			d.log.Error("Kernel registration notification did not contain docker container ID: %v", dockerContainerId)
			go d.notifyDashboardOfError("Missing Docker Container ID in Kernel Registration Notification",
				fmt.Sprintf("Kernel registration notification for replica %d of kernel \"%s\" did not contain a valid Docker container ID",
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
		d.id, d.numResendAttempts, -1, -1, in.PodOrContainerName, in.NodeName,
		nil, nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), in.HostId,
		host, metrics.ClusterGateway, true, true, d.DebugMode, d.gatewayPrometheusManager,
		d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection, d.updateClusterStatistics)

	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	session, ok := d.cluster.GetSession(in.KernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", in.SessionId)
		d.log.Error(errorMessage)
		go d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the Kernel.
	replica.SetContainer(container)

	// Add the Container to the Host.
	d.log.Debug("Adding scheduling.Container for replica %d of kernel %s onto Host %s",
		replicaSpec.ReplicaId, addReplicaOp.KernelId(), host.GetID())
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
	//if dockerContainerId, loaded := addReplicaOp.GetMetadata(domain.DockerContainerFullId); loaded {
	//	container.SetDockerContainerID(dockerContainerId.(string))
	//}

	d.log.Debug("Adding replica for kernel %s, replica %d on host %s. Resource spec: %v", addReplicaOp.KernelId(), replicaSpec.ReplicaId, host.GetID(), replicaSpec.Kernel.ResourceSpec)
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("Kernel::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// d.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaID, in.KernelId, waitGroup.NumReplicas())

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
		ShouldReadDataFromRemoteStorage: true,
		ResourceSpec:                    replicaSpec.Kernel.ResourceSpec,
		SmrPort:                         int32(d.smrPort),
	}

	d.Unlock()

	d.log.Debug("Sending notification that replica %d of kernel \"%s\" has registered during AddOperation \"%s\".",
		replicaSpec.ReplicaId, in.KernelId, addReplicaOp.OperationID())

	err = addReplicaOp.SetReplicaRegistered() // This just sets a flag to true in the migration operation object.
	if err != nil {
		errorMessage := fmt.Sprintf("We're using the WRONG AddReplicaOperation... AddReplicaOperation \"%s\" has already recorded that its replica has registered: %v",
			addReplicaOp.OperationID(), addReplicaOp.String())
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Using Incorrect AddReplicaOperation", errorMessage)
		panic(err)
	}

	d.log.Debug("About to issue 'update replica' request for replica %d of kernel %s. Client ready: %v", replicaSpec.ReplicaId, in.KernelId, replica.IsReady())

	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	// Issue the AddHost request now, so that the node can join when it starts up.
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

func (d *ClusterGatewayImpl) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	d.log.Info("Received kernel registration notification.")

	connectionInfo := in.ConnectionInfo
	sessionId := in.SessionId
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	kernelPodOrContainerName := in.PodOrContainerName
	nodeName := in.NodeName
	replicaId := in.ReplicaId

	d.log.Info("Connection info: %v", connectionInfo)
	d.log.Info("Session ID: %v", sessionId)
	d.log.Info("Kernel ID: %v", kernelId)
	d.log.Info("Replica ID: %v", replicaId)
	d.log.Info("Kernel IP: %v", kernelIp)

	if d.KubernetesMode() {
		d.log.Info("Pod name: %v", kernelPodOrContainerName)
	} else {
		d.log.Info("Container name: %v", kernelPodOrContainerName)
	}

	d.log.Info("Node ID: %v", hostId)
	d.log.Info("Node Name: %v", nodeName)
	d.log.Info("Notification ID: %v", in.NotificationId)

	d.Lock()

	_, loaded := d.kernelRegisteredNotifications.LoadOrStore(in.NotificationId, in)
	if loaded {
		d.log.Warn("Received duplicate \"Kernel Registered\" notification with ID=%s", in.NotificationId)
		d.Unlock()
		return nil, status.Error(codes.InvalidArgument, types.ErrDuplicateRegistrationNotification.Error())
	}

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

		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

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
		NumReplicas: int32(d.NumReplicas()),
		WorkloadId:  kernelSpec.WorkloadId,
	}

	if nodeName == "" || nodeName == types.DockerNode {
		if !d.DockerMode() {
			log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid node name.\n"),
				replicaSpec.ReplicaId, in.KernelId)
		}

		// In Docker mode, we'll just use the Host ID as the node name.
		nodeName = host.GetID()
	}

	d.log.Debug("Creating new Kernel Client for replica %d of kernel %s now...", in.ReplicaId, in.KernelId)

	// Initialize kernel client
	replica := client.NewKernelReplicaClient(context.Background(), replicaSpec,
		jupyter.ConnectionInfoFromKernelConnectionInfo(connectionInfo), d.id,
		d.numResendAttempts, -1, -1, kernelPodOrContainerName, nodeName, nil,
		nil, d.MessageAcknowledgementsEnabled, kernel.PersistentID(), hostId, host, metrics.ClusterGateway,
		true, true, d.DebugMode, d.gatewayPrometheusManager, d.kernelReconnectionFailed,
		d.kernelRequestResubmissionFailedAfterReconnection, d.updateClusterStatistics)

	session, ok := d.cluster.GetSession(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", kernelId)
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	// Create the new Container.
	container := entity.NewContainer(session, replica, host, in.KernelIp)

	// Assign the Container to the Kernel.
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

	d.log.Debug("Validating new Kernel for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	if val := ctx.Value(SkipValidationKey); val == nil {
		err := replica.Validate()
		if err != nil {
			panic(fmt.Sprintf("Kernel::Validate call failed: %v", err)) // TODO(Ben): Handle gracefully.
		}
	} else {
		d.log.Warn("Skipping validation and establishment of actual network connections with newly-registered replica %d of kernel %s.",
			replica.ReplicaID(), in.KernelId)
	}

	d.log.Debug("Adding Replica for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("Kernel::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()
	d.Unlock()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register()
	d.log.Debug("Done registering Kernel for kernel %s, replica %d on host %s. Resource spec: %v",
		kernelId, replicaId, hostId, kernelSpec.ResourceSpec)
	d.log.Debug("WaitGroup for Kernel \"%s\": %s", kernelId, waitGroup.String())
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &proto.KernelRegistrationNotificationResponse{
		Id:                              replicaId,
		Replicas:                        waitGroup.GetReplicas(),
		PersistentId:                    &persistentId,
		ResourceSpec:                    kernelSpec.ResourceSpec,
		SmrPort:                         int32(d.smrPort), // The kernel should already have this info, but we'll send it anyway.
		ShouldReadDataFromRemoteStorage: false,
		// DataDirectory: nil,
	}

	d.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v", kernelId, replicaId, response)

	waitGroup.Notify()
	return response, nil
}

// PingGateway is a no-op for testing connectivity.
func (d *ClusterGatewayImpl) PingGateway(_ context.Context, in *proto.Void) (*proto.Void, error) {
	return in, nil
}

// ForceLocalDaemonToReconnect is used to tell a DefaultSchedulingPolicy Daemon to reconnect to the Cluster Gateway.
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
		d.log.Error("Error while instruction DefaultSchedulingPolicy Daemon %s to reconnect to us: %v", daemonId, err)

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
func (d *ClusterGatewayImpl) KillKernel(_ context.Context, in *proto.KernelId) (ret *proto.Void, err error) {
	d.log.Debug("KillKernel RPC called for kernel %s.", in.Id)

	// Call the impl rather than the RPC stub.
	ret, err = d.stopKernelImpl(in)

	if _, ok := status.FromError(err); !ok {
		err = status.Error(codes.Internal, err.Error())
	}

	return ret, err
}

// GetId returns the ID of the ClusterGatewayImpl.
func (d *ClusterGatewayImpl) GetId() string {
	return d.id
}

func (d *ClusterGatewayImpl) stopKernelImpl(in *proto.KernelId) (ret *proto.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Error("Could not find Kernel %s; cannot stop kernel.", in.GetId())
		return nil, types.ErrKernelNotFound
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
				kernel.ID()), messaging.SuccessNotification)
		d.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Sub(1)
	} else {
		go d.notifyDashboardOfError("Failed to Terminate Kernel",
			fmt.Sprintf("An error was encountered while trying to terminate kernel %s: %v.", kernel.ID(), err))
	}

	return
}

// StopKernel stops a kernel.
func (d *ClusterGatewayImpl) StopKernel(_ context.Context, in *proto.KernelId) (*proto.Void, error) {
	d.log.Debug("StopKernel RPC called for kernel %s.", in.Id)

	ret, err := d.stopKernelImpl(in)
	if err != nil {
		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return ret, err
	}

	numActiveKernels := d.numActiveKernels.Add(-1)

	session := d.cluster.RemoveSession(in.Id)

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.NumStoppedSessions += 1
	d.clusterStatisticsMutex.Unlock()

	if d.gatewayPrometheusManager != nil {
		d.gatewayPrometheusManager.NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))
	}

	if session != nil {
		d.clusterStatisticsMutex.Lock()
		lifetimeSeconds := time.Since(session.StartedAt()).Seconds()
		d.ClusterStatistics.AggregateSessionLifetimeSec += lifetimeSeconds
		d.ClusterStatistics.AggregateSessionLifetimesSec = append(d.ClusterStatistics.AggregateSessionLifetimesSec, lifetimeSeconds)
		d.clusterStatisticsMutex.Unlock()
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
	d.log.Debug(utils.NotificationStyles[in.NotificationType].Render("Received %s notification \"%s\": %s"), NotificationTypeNames[in.NotificationType], in.Title, in.Message)
	go d.notifyDashboard(in.Title, in.Message, messaging.NotificationType(in.NotificationType))
	return proto.VOID, nil
}

func (d *ClusterGatewayImpl) SpoofNotifications(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	go func() {
		d.notifyDashboard("Spoofed Error", "This is a made-up error message sent by the internalCluster Gateway.", messaging.ErrorNotification)
		d.notifyDashboard("Spoofed Warning", "This is a made-up warning message sent by the internalCluster Gateway.", messaging.WarningNotification)
		d.notifyDashboard("Spoofed Info Notification", "This is a made-up 'info' message sent by the internalCluster Gateway.", messaging.InfoNotification)
		d.notifyDashboard("Spoofed Success Notification", "This is a made-up 'success' message sent by the internalCluster Gateway.", messaging.SuccessNotification)
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
			d.log.Error("Failed to retrieve actual GPU info from DefaultSchedulingPolicy Daemon %s on node %s because: %v", hostId, host.GetNodeName(), err)
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
			d.log.Error("Failed to retrieve virtual GPU info from DefaultSchedulingPolicy Daemon %s on node %s because: %v", hostId, host.GetNodeName(), err)
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
		d.log.Error("Could not find a DefaultSchedulingPolicy Daemon running on Kubernetes node %s.", in.KubernetesNodeName)
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

func (d *ClusterGatewayImpl) MigrateKernelReplica(_ context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	startTime := time.Now()
	replicaInfo := in.TargetReplica
	targetNodeId := in.GetTargetNodeId()

	kernel, loaded := d.kernels.Load(replicaInfo.KernelId)
	if !loaded {
		d.log.Error("Could not find target of migration, kernel \"%s\"", replicaInfo.KernelId)
		go d.notifyDashboardOfError("Cannot Migrate Kernel.",
			fmt.Sprintf("Cannot find kernel with ID=%s.", replicaInfo.KernelId))
		return nil, fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, replicaInfo.KernelId)
	}

	kernelReplica, err := kernel.GetReplicaByID(replicaInfo.ReplicaId)
	if err != nil {
		d.log.Error("Kernel %s does not have a replica with SMR node ID of %d. Cannot migrate.",
			replicaInfo.KernelId, replicaInfo.ReplicaId)
		go d.notifyDashboardOfError("Cannot Migrate Kernel.",
			fmt.Sprintf("Kernel %s does not have a replica with ID=%d.",
				replicaInfo.KernelId, replicaInfo.ReplicaId))
		return nil, err
	}

	resp, err := d.cluster.Scheduler().MigrateKernelReplica(kernelReplica, targetNodeId, true)

	duration := time.Since(startTime)
	if err != nil {
		d.log.Error("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration, err.Error())

		if d.gatewayPrometheusManager != nil {
			d.gatewayPrometheusManager.NumFailedMigrations.Inc()
		}
	} else {
		d.log.Debug("Migration operation of replica %d of kernel %s to target node %s completed successfully after %v.",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration)

		if d.gatewayPrometheusManager != nil {
			d.gatewayPrometheusManager.NumSuccessfulMigrations.Inc()
		}
	}

	if d.gatewayPrometheusManager != nil {
		d.gatewayPrometheusManager.KernelMigrationLatencyHistogram.Observe(float64(duration.Milliseconds()))
	}

	// If there was an error, then err will be non-nil.
	// If there was no error, then err will be nil.
	return resp, err
}

func (d *ClusterGatewayImpl) Start() error {
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

	// Close the router
	if err := d.router.Close(); err != nil {
		d.log.Error("Failed to cleanly shutdown router because: %v", err)
	}

	// Wait for the newKernels to be cleaned up
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
// Provider implementations //
////////////////////////////////////

func (d *ClusterGatewayImpl) handleShutdownRequest(msg *messaging.JupyterMessage) error {
	sessionId := msg.JupyterSession()
	d.log.Debug("Intercepting \"%v\" message targeting session \"%s\" and using RPC pathway instead...",
		messaging.MessageTypeShutdownRequest, sessionId)

	kernel, ok := d.kernels.Load(sessionId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find Kernel \"%s\"; cannot stop kernel.", sessionId)
		d.log.Error(errorMessage)
		// Spawn a separate goroutine to send an error notification to the dashboard.
		go d.notifyDashboardOfError(errorMessage, errorMessage)

		return types.ErrKernelNotFound
	}

	// Stop the kernel. If we get an error, print it here, and then we'll return it.
	var err error
	if _, err = d.stopKernelImpl(&proto.KernelId{Id: kernel.ID()}); err != nil {
		d.log.Error("Failed to (cleanly) terminate session \"%s\", kernel \"%s\" because: %v", sessionId, kernel.ID(), err)

		// Spawn a separate goroutine to send an error notification to the dashboard.
		go d.notifyDashboardOfError(fmt.Sprintf("Failed to Terminate Kernel %s, Session %s", kernel.ID(), sessionId), err.Error())
		return err
	}

	session, ok := d.cluster.GetSession(kernel.ID())
	if !ok || session == nil {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session %s associated with kernel %s, which is being shutdown", sessionId, kernel.ID())
		d.log.Error(errorMessage)
		go d.notifyDashboardOfError(fmt.Sprintf("Failed to Find scheduling.Session of Terminating Kernel \"%s\", Session ID=%s", kernel.ID(), sessionId), errorMessage)
	} else {
		p := session.SessionStopped()
		err := p.Error()
		if err != nil {
			d.log.Error("Error while de-scheduling kernel \"%s\" associated with session \"%s\"", kernel.ID(), sessionId)
			go d.notifyDashboardOfError(fmt.Sprintf("Error while Descheduling Session \"%s\"", kernel.ID()), err.Error())
			return err
		}
	}

	// TODO: This doesn't actually send an error response back to the client/Jupyter server.
	// This just returns to our underlying server's request handler code.
	// To send a response to Jupyter, we'd need to use the ClusterGatewayImpl::kernelResponseForwarder method.
	return err // Will be nil if we successfully shut down the kernel.
}

func (d *ClusterGatewayImpl) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	// If this is a shutdown request, then use the RPC pathway instead.
	if msg.JupyterMessageType() == messaging.MessageTypeShutdownRequest {
		return d.handleShutdownRequest(msg)
	}

	err := d.forwardRequest(nil, messaging.ControlMessage, msg)

	// When a kernel is first created/being nudged, Jupyter Server will send both a Shell and Control request.
	// The Control request will just have a Session, and the mapping between the Session and the Kernel will not
	// be established until the Shell message is processed. So, if we see a types.ErrKernelNotFound error here, we will
	// simply retry it after some time has passed, as the requests are often received very close together.
	if errors.Is(err, types.ErrKernelNotFound) {
		time.Sleep(time.Millisecond * 1500)

		// We won't re-try more than once.
		err = d.forwardRequest(nil, messaging.ControlMessage, msg)
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

// ensureKernelReplicasAreScheduled ensures that the replicas of the specified scheduling.Kernel are already scheduled.
//
// If they're not, then ensureKernelReplicasAreScheduled will either schedule the replicas, if the given msg is an
// "execute_request" message, or it will simply return an artificial response, in the case of all other message types.
func (d *ClusterGatewayImpl) ensureKernelReplicasAreScheduled(kernel scheduling.Kernel, msg *messaging.JupyterMessage, typ messaging.MessageType) (*messaging.JupyterMessage, error) {
	// If the replica(s) are scheduled, then we have nothing to do.
	if kernel.ReplicasAreScheduled() {
		d.log.Debug("Replicas of kernel %s are scheduled.", kernel.ID())
		return nil, nil
	}

	// For any message that isn't an "execute_request" message, we'll return an artificial response when the
	// replicas of the kernel are not actively running.
	if typ != messaging.ShellMessage || msg.JupyterMessageType() != messaging.ShellExecuteRequest {
		d.log.Debug("Replicas of kernel %s are NOT scheduled. Generating artificial response to %s \"%s\" message %s (JupyterID=\"%s\").",
			kernel.ID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		return d.generateArtificialResponse(kernel, msg, typ)
	}

	// TODO: We should only bother scheduling a container for an "execute_request".
	// 		 For other message types, we should just return an artificial response.
	d.log.Debug("Replicas of kernel %s are NOT scheduled. Scheduling replicas now before handling Jupyter \"execute_request\" message %s (JupyterID=\"%s\").",
		kernel.ID(), msg.RequestId, msg.JupyterMessageId())

	err := d.scheduleReplicas(context.Background(), kernel, kernel.KernelSpec())
	if err != nil {
		d.log.Error("Failed to schedule replica container(s) of kernel \"%s\" after receiving Jupyter \"%s\" message: %v",
			kernel.ID(), msg.JupyterMessageType(), err)

		return nil, err // TODO: Should we return this? It won't necessarily make it back to the client, which it probably should.
	}

	return nil, nil
}

func (d *ClusterGatewayImpl) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	kernel, ok := d.kernels.Load(msg.JupyterSession())

	if !ok && (msg.JupyterMessageType() == messaging.KernelInfoRequest || msg.JupyterMessageType() == messaging.ShellExecuteRequest) {
		// Register kernel on KernelInfoRequest
		if msg.DestinationId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(msg.DestinationId)
		if !ok {
			d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v",
				msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())
			return types.ErrKernelNotFound
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

		return types.ErrKernelNotFound
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		err := d.processExecuteRequest(msg, kernel)
		if err != nil {
			return err
		}
	} else {
		d.log.Debug("Forwarding shell message to kernel %s: %s", msg.DestinationId, msg.StringFormatted())
	}

	if err := d.forwardRequest(kernel, messaging.ShellMessage, msg); err != nil {
		d.log.Error("Error while handling/forwarding shell \"%s\" message \"%s\" (JupyterID=\"%s\"): %v.",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)
		return err
	}

	return nil
}

// processExecuteReply handles the scheduling and resource allocation/de-allocation logic required when a
// kernel finishes executing user-submitted code.
//
// TODO: Will there be race conditions here if we've sent multiple "execute_request" messages to the kernel?
// TODO: Could we receive a notification that a subsequent training has started before getting the reply that the last train completed?
// TODO: If so, how can we handle these out-of-order requests? We can associate trainings with Jupyter message IDs so that, if we get a >>
// TODO: >> training stopped notification, then it needs to match up with the current training, maybe in a queue structure, so that out-of-order >>
// TODO: >> messages can be handled properly.
func (d *ClusterGatewayImpl) processExecuteReply(kernelId string, msg *messaging.JupyterMessage) error {
	d.log.Debug("Received \"execute_reply\" with JupyterID=\"%s\" from kernel %s.", kernelId, msg.JupyterMessageId())

	// If this message is actually from a failed attempt to handle all replicas proposing 'yield', then we just
	// return immediately. The execution wasn't successful. We want this error to be sent back to the client.
	if msg.IsFailedExecuteRequest {
		d.log.Warn("\"execute_reply\" with JupyterID=\"%s\" from kernel %s is from a failed 'all replicas yielded' handler...",
			kernelId, msg.JupyterMessageId())
		return nil
	}

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		d.log.Error("Failed to load DistributedKernelClient %s while processing \"execute_reply\" message...", kernelId)
		go d.notifyDashboardOfError("Failed to Load Distributed Kernel Client", fmt.Sprintf("Failed to load DistributedKernelClient %s while processing \"execute_reply\" message...", kernelId))
		return types.ErrKernelNotFound
	}

	activeExecution := kernel.ActiveExecution()
	if activeExecution != nil && activeExecution.GetExecuteRequestMessageId() != msg.JupyterParentMessageId() {
		// It's possible that we receive an "execute_reply" for execution i AFTER the "smr_lead_task" message for
		// execution i+1 (i.e., out of order). When this happens, we just stop the current training upon receiving
		// the "smr_lead_task" message for execution i+1. If and when we receive the "execute_reply" message for
		// execution i (after we've already moved on to execution i+1), we discard the "old" "execute_reply" message.
		d.log.Error(utils.RedStyle.Render("Received \"execute_reply\" for \"execute_request\" \"%s\"; however, current execution is for \"execute_request\" \"%s\"."),
			msg.JupyterParentMessageId(), activeExecution.GetExecuteRequestMessageId())

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
			errorMessage := fmt.Sprintf("Thought we received out-of-order \"smr_lead_task\" and \"execute_reply\" " +
				"messages for execution i+1 and i respectively, but current execution is actually older than execution " +
				"associated with the \"execute_reply\" message that we just received...")
			d.notifyDashboardOfError("Old Active Execution Isn't Actually Old...", errorMessage)
			d.log.Error(errorMessage)
			d.log.Error(utils.RedStyle.Render("Old active execution isn't actually old. Current execution: %v. \"Old\" execution: %v.\n"),
				activeExecution.String(), oldActiveExecution.String())
			d.log.Error(utils.RedStyle.Render("Received out of order \"smr_lead_task\" and \"execute_reply\" messages from kernel %s.\n"), kernelId)
		}
	} else if activeExecution == nil {
		d.log.Error("No active execution registered for kernel %s...", kernelId)
	}

	if d.gatewayPrometheusManager != nil && d.gatewayPrometheusManager.NumTrainingEventsCompletedCounterVec != nil {
		d.gatewayPrometheusManager.NumTrainingEventsCompletedCounterVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).Inc()
	}

	if _, err := kernel.ExecutionComplete(msg); err != nil {
		return err
	}

	d.clusterStatisticsMutex.Lock()
	d.ClusterStatistics.CompletedTrainings += 1
	d.ClusterStatistics.NumIdleSessions += 1
	d.clusterStatisticsMutex.Unlock()

	return nil
}

func (d *ClusterGatewayImpl) processExecuteRequest(msg *messaging.JupyterMessage, kernel scheduling.Kernel) error {
	kernelId := kernel.ID()
	d.log.Debug("Forwarding shell \"execute_request\" message to kernel %s: %s", kernelId, msg.StringFormatted())

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
	milliseconds := float64(latency.Milliseconds())

	if d.gatewayPrometheusManager != nil {
		d.gatewayPrometheusManager.JupyterTrainingStartLatency.
			With(prometheus.Labels{
				"workload_id": workloadId,
				"kernel_id":   kernelId,
			}).
			Observe(milliseconds)
	}

	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	d.ClusterStatistics.JupyterTrainingStartLatencyMillis += milliseconds
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
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed",
				fmt.Sprintf("Could not find host %s on which replica %d of kernel %s is supposedly running...",
					hostId, replica.ReplicaID(), in.Id))
			return proto.VOID, scheduling.ErrHostNotFound
		}

		// Even if there's an error here, we'll just keep trying. If only some of these succeed, then the system won't explode.
		// The newKernels for which the `YieldNextExecution` succeeded will simply yield.
		_, err := host.YieldNextExecution(ctx, in)
		if err != nil {
			d.log.Error("Failed to issue 'FailNextExecution' to DefaultSchedulingPolicy Daemon %s (%s) because: %s",
				hostId, host.GetAddress(), err.Error())
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed",
				fmt.Sprintf("Failed to issue 'FailNextExecution' to DefaultSchedulingPolicy Daemon %s (%s) because: %s",
					hostId, host.GetAddress(), err.Error()))
		} else {
			d.log.Debug("Successfully issued 'FailNextExecution' to DefaultSchedulingPolicy Daemon %s (%s) targeting kernel %s.",
				hostId, host.GetAddress(), in.Id)
		}
	}

	return &proto.Void{}, nil
}

// Return the add-replica operation associated with the given Kernel ID and SMR Node ID of the new replica.
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

// Extract the Kernel ID and the message type from the given ZMQ message.
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
	if kernel.Status() != jupyter.KernelStatusRunning && d.replicasShouldBeRunning() {
		return kernel, messageType, ErrKernelNotReady
	}

	return kernel, messageType, nil
}

// replicasShouldBeRunning returns a flag indicating whether the containers of kernels should be running already.
//
// For scheduling policies in which the ContainerLifetime is scheduling.LongRunning, this is true.
func (d *ClusterGatewayImpl) replicasShouldBeRunning() bool {
	return d.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning
}

func (d *ClusterGatewayImpl) forwardRequest(kernel scheduling.Kernel, typ messaging.MessageType, msg *messaging.JupyterMessage) (err error) {
	goroutineId := goid.Get()

	if kernel == nil {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting unknown kernel/session. Inspecting now: %v"), goroutineId, typ.String(), msg.JupyterFrames.String())
		kernel, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	} else {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting kernel %s. Inspecting now..."), goroutineId, typ.String(), kernel.ID())

		// Check availability.
		// TODO: This only matters if the replicas should already be running.
		if kernel.Status() != jupyter.KernelStatusRunning && d.replicasShouldBeRunning() {
			return ErrKernelNotReady
		}
	}

	if err != nil {
		d.log.Error("[gid=%d] Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.", goroutineId, typ, err, msg)
		return err
	}

	if kernel == nil {
		// Should not happen; if the error was nil, then kernel is non-nil.
		panic("Kernel is nil")
	}

	resp, err := d.ensureKernelReplicasAreScheduled(kernel, msg, typ)
	if err != nil {
		d.log.Error("Error encountered while ensuring replica container(s) of kernel %s are scheduled in order to handle shell \"%s\" message: %v",
			kernel.ID(), msg.JupyterMessageType(), err)
		return err // TODO: Should this be returned? Or should it be sent back to the client?
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

	if resp != nil {
		d.log.Debug("Replying with artificial \"%s\" response for Jupyter %s \"%s\" message \"%s\" (JupyterID=\"%s\") for kernel \"%s\".",
			resp.JupyterMessageType(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID())

		err = d.kernelResponseForwarder(kernel.TemporaryKernelReplicaClient(), typ, resp)
		if err != nil {
			d.log.Error(utils.DarkGreenStyle.Render("Failed to forward %v \"%s\" response \"%s\" (JupyterID=\"%s\") to client of kernel %s: %v"),
				goroutineId, typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID(), resp)
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

	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelResponseForwarder, func() {})
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

		d.log.Warn(style.Render("Forwarding %s \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s took %v."),
			socket.Type.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, sendDuration)
	}

	if metricError := d.gatewayPrometheusManager.SentMessage(d.id, sendDuration, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
		d.log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if metricError := d.gatewayPrometheusManager.SentMessageUnique(d.id, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType()); metricError != nil {
		d.log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if err != nil {
		d.log.Error(utils.RedStyle.Render("[gid=%d] Error while forwarding %v \"%s\" response %s (JupyterID=\"%s\") from kernel %s via %s: %s"),
			goid.Get(), socket.Type, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, socket.Name, err.Error())
	} else {
		d.log.Debug("Successfully forwarded %v \"%s\" message from kernel \"%s\" (JupyterID=\"%s\"): %v",
			socket.Type, msg.JupyterMessageType(), senderId, msg.JupyterMessageId(), messaging.FramesToString(zmqMsg.Frames))
	}

	return err // Will be nil on success.
}

func (d *ClusterGatewayImpl) updateStatisticsFromShellExecuteReply(trace *proto.RequestTrace) {
	if trace == nil {
		d.log.Warn("RequestTrace is nil when attempting to extract metrics/statistics from shell \"execute_reply\"")
		return
	}

	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	if trace.CudaInitMicroseconds > 0 {
		d.ClusterStatistics.CumulativeCudaInitMicroseconds += float64(trace.CudaInitMicroseconds)
		d.ClusterStatistics.NumCudaRuntimesInitialized += 1
	}

	if trace.ReplayTimeMicroseconds > 0 {
		d.ClusterStatistics.CumulativeReplayTimeMicroseconds += float64(trace.ReplayTimeMicroseconds)
		d.ClusterStatistics.TotalNumReplays += 1

		session, loaded := d.cluster.GetSession(trace.KernelId)
		if !loaded || session == nil {
			d.log.Warn("Could not find session \"%s\" specified in RequestTrace: %s", trace.KernelId, trace.String())
		} else {
			// Subtract 1 to exclude the last training event that just completed.
			d.ClusterStatistics.TotalNumCellsReplayed += int64(session.NumTrainingEventsProcessed() - 1)
		}
	}

	if trace.DownloadDependencyMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeDownloadingDependenciesMicroseconds += float64(trace.DownloadDependencyMicroseconds)
		d.ClusterStatistics.NumTimesDownloadedDependencies += 1
	}

	if trace.UploadModelAndTrainingDataMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeUploadModelAndTrainingDataMicroseconds += float64(trace.UploadModelAndTrainingDataMicroseconds)
		d.ClusterStatistics.NumTimesUploadModelAndTrainingDataMicroseconds += 1
	}

	if trace.CopyFromCpuToGpuMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeCopyDataHostToDeviceMicroseconds += float64(trace.CopyFromCpuToGpuMicroseconds)
		d.ClusterStatistics.NumTimesCopyDataHostToDeviceMicroseconds += 1
	}

	if trace.CopyFromGpuToCpuMicroseconds > 0 {
		d.ClusterStatistics.CumulativeTimeCopyDataDeviceToHostMicroseconds += float64(trace.CopyFromGpuToCpuMicroseconds)
		d.ClusterStatistics.NumTimesCopyDataDeviceToHostMicroseconds += 1
	}

	if trace.LeaderElectionTimeMicroseconds > 0 {
		d.ClusterStatistics.CumulativeLeaderElectionTimeMicroseconds += float64(trace.LeaderElectionTimeMicroseconds)
	}

	d.ClusterStatistics.CumulativeExecutionTimeMicroseconds += float64(trace.ExecutionTimeMicroseconds)
}

// kernelResponseForwarder is used as the response handler for a variety of requests/forwarded messages.
//
// kernelResponseForwarder forwards the given messaging.JupyterMessage to the remote entity connected to the
// socket of specified messaging.MessageType belonging to the specified scheduling.KernelReplicaInfo.
func (d *ClusterGatewayImpl) kernelResponseForwarder(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
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

	isShellExecuteReply := typ == messaging.ShellMessage && msg.JupyterMessageType() == messaging.ShellExecuteReply
	if isShellExecuteReply {
		err := d.processExecuteReply(from.ID(), msg)
		if err != nil {
			go d.notifyDashboardOfError("Error While Processing \"execute_reply\" Message", err.Error())
			panic(err)
		}
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
		if isShellExecuteReply {
			d.updateStatisticsFromShellExecuteReply(requestTrace)
		}
	}

	d.log.Debug(utils.DarkGreenStyle.Render("[gid=%d] Forwarding %v \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s via %s: %v"),
		goroutineId, typ, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), from.ID(), socket.Name, msg)

	sendError := d.sendZmqMessage(msg, socket, from.ID())
	if sendError == nil {
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumJupyterRepliesSentByClusterGateway += 1
		d.clusterStatisticsMutex.Unlock()
	}

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if isShellExecuteReply && d.Scheduler().Policy().ContainerLifetime() == scheduling.SingleTrainingEvent {
		d.log.Debug("Kernel \"%s\" has finished training. Removing container.", from.ID())

		kernel, loaded := d.kernels.Load(from.ID())
		if !loaded {
			d.log.Error("Could not find Distributed Kernel Client for kernel \"%s\"...", from.ID())

		}

		go func() {
			_ = d.removeAllReplicasOfKernel(kernel)
		}()
	}

	return sendError
}

// removeAllReplicasOfKernel is used to de-schedule the replicas of the given kernel without removing the kernel itself.
//
// This does not remove the kernel itself.
func (d *ClusterGatewayImpl) removeAllReplicasOfKernel(kernel scheduling.Kernel) error {
	err := kernel.RemoveAllReplicas(d.cluster.Placer().Reclaim, false)
	if err != nil {
		d.log.Error("Failed to remove one or more replicas of kernel \"%s\": %v", kernel.ID(), err)

		go d.notifyDashboardOfError(fmt.Sprintf("Failed to Remove One or More Replicas of Kernel \"%s\"",
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

	d.Lock()
	defer d.Unlock()

	d.kernelIdToKernel.Range(func(id string, kernel scheduling.Kernel) bool {
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
				PodId:     replica.GetPodOrContainerName(),
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
	// Virtual Docker nodes correspond to each DefaultSchedulingPolicy Daemon container, and are primarily used for development or small, local simulations.
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
	// Virtual Docker nodes correspond to each DefaultSchedulingPolicy Daemon container, and are primarily used for development or small, local simulations.
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

	err := encoder.Encode(&d.ClusterStatistics)
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

	d.ClusterStatistics = statistics.NewClusterStatistics()

	// Basically initialize the statistics with some values, but in a separate goroutine.
	go d.gatherClusterStatistics()

	return resp, nil
}

// updateClusterStatistics is passed to Distributed Kernel Clients so that they may atomically update statistics.
func (d *ClusterGatewayImpl) updateClusterStatistics(updaterFunc func(statistics *statistics.ClusterStatistics)) {
	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	updaterFunc(d.ClusterStatistics)
}

// gatherClusterStatistics updates all the values in the ClusterStatistics field.
//
// gatherClusterStatistics is thread-safe.
func (d *ClusterGatewayImpl) gatherClusterStatistics() {
	d.clusterStatisticsMutex.Lock()
	defer d.clusterStatisticsMutex.Unlock()

	now := time.Now()
	var lastTime time.Time // Last update time

	if d.lastFullStatisticsUpdate.IsZero() {
		lastTime = now // We're doing the first update
	} else {
		lastTime = d.lastFullStatisticsUpdate
	}

	d.log.Debug("Updating ClusterStatistics now.")

	var idleCpu, idleMem, idleGpu, idleVram float64
	var pendingCpu, pendingMem, pendingGpu, pendingVram float64
	var committedCpu, committedMem, committedGpu, committedVram float64
	var specCpu, specMem, specGpu, specVram float64

	//var cpuUtil, gpuUtil, memUtil, vramUtil, demandGpus float64
	var demandCpus, demandMem, demandGpus, demandVram float64

	var numNonEmptyHosts, numEmptyHosts int

	// The aggregate, cumulative lifetime of the hosts that are currently running.
	var aggregateHostLifetimeOfRunningHosts float64

	d.cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		idleCpu += host.IdleCPUs()
		idleMem += host.IdleMemoryMb()
		idleGpu += host.IdleGPUs()
		idleVram += host.IdleVRAM()

		pendingCpu += host.PendingCPUs()
		pendingMem += host.PendingMemoryMb()
		pendingGpu += host.PendingGPUs()
		pendingVram += host.PendingVRAM()

		committedCpu += host.CommittedCPUs()
		committedMem += host.CommittedMemoryMb()
		committedGpu += host.CommittedGPUs()
		committedVram += host.CommittedVRAM()

		specCpu += host.ResourceSpec().CPU()
		specMem += host.ResourceSpec().MemoryMB()
		specGpu += host.ResourceSpec().GPU()
		specVram += host.ResourceSpec().VRAM()

		if host.NumContainers() == 0 {
			numEmptyHosts += 1
		} else {
			numNonEmptyHosts += 1
		}

		aggregateHostLifetimeOfRunningHosts += time.Since(host.GetCreatedAt()).Seconds()

		return true
	})

	activeTime := time.Since(lastTime) * time.Duration(numNonEmptyHosts)
	idleTime := time.Since(lastTime) * time.Duration(numEmptyHosts)

	d.ClusterStatistics.CumulativeHostActiveTime += activeTime.Seconds()
	d.ClusterStatistics.CumulativeHostIdleTime += idleTime.Seconds()
	d.ClusterStatistics.AggregateHostLifetimeOfRunningHosts = aggregateHostLifetimeOfRunningHosts
	d.ClusterStatistics.AggregateHostLifetime += time.Since(lastTime).Seconds() * float64(d.cluster.Len())

	d.cluster.RangeOverSessions(func(key string, value scheduling.UserSession) bool {
		demandCpus += value.ResourceSpec().CPU()
		demandMem += value.ResourceSpec().MemoryMB()
		demandGpus += value.ResourceSpec().GPU()
		demandVram += value.ResourceSpec().VRAM()

		return true
	})

	d.ClusterStatistics.DemandGPUs = demandCpus
	d.ClusterStatistics.DemandMemMb = demandMem
	d.ClusterStatistics.DemandGPUs = demandGpus
	d.ClusterStatistics.DemandVRAMGb = demandVram

	///////////
	// Hosts //
	///////////

	d.ClusterStatistics.Hosts = d.cluster.Len()
	d.ClusterStatistics.NumDisabledHosts = d.cluster.NumDisabledHosts()
	d.ClusterStatistics.NumEmptyHosts = numEmptyHosts

	///////////////
	// Resources //
	///////////////

	d.ClusterStatistics.IdleCPUs = idleCpu
	d.ClusterStatistics.IdleMemory = idleMem
	d.ClusterStatistics.IdleGPUs = idleGpu
	d.ClusterStatistics.IdleVRAM = idleVram

	d.ClusterStatistics.PendingCPUs = pendingCpu
	d.ClusterStatistics.PendingMemory = pendingMem
	d.ClusterStatistics.PendingGPUs = pendingGpu
	d.ClusterStatistics.PendingVRAM = pendingVram

	d.ClusterStatistics.CommittedCPUs = committedCpu
	d.ClusterStatistics.CommittedMemory = committedMem
	d.ClusterStatistics.CommittedGPUs = committedGpu
	d.ClusterStatistics.CommittedVRAM = committedVram

	d.ClusterStatistics.SpecCPUs = specCpu
	d.ClusterStatistics.SpecMemory = specMem
	d.ClusterStatistics.SpecGPUs = specGpu
	d.ClusterStatistics.SpecVRAM = specVram

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////
	d.ClusterStatistics.SubscriptionRatio = d.cluster.Scheduler().SubscriptionRatio()

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	//////////////
	// Sessions //
	//////////////
	d.ClusterStatistics.NumNonTerminatedSessions = int(d.numActiveKernels.Load())
	d.ClusterStatistics.NumRunningSessions = d.cluster.Sessions().Len()

	d.lastFullStatisticsUpdate = time.Now()
}
