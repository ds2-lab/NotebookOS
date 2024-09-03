package daemon

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	"github.com/zhangjyr/distributed-notebook/gateway/scheduler"
)

const (
	ShellExecuteRequest    = "execute_request"
	ShellExecuteReply      = "execute_reply"
	ShellYieldExecute      = "yield_execute"
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"
	KubernetesKernelName   = "kernel-%s"
	ConnectionFileFormat   = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat       = "config-%s-*.json"     // "*" is a placeholder for random string

	ErrorHostname = "ERROR" // We return this from certain gRPC calls when there's an error.

	// TargetReplicaArg is passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg  = "target_replica"
	ForceReprocessArg = "force_reprocess"

	DockerKernelDebugPortDefault int32 = 32000
)

var (
	// gRPC errors

	ErrNotImplemented = status.Errorf(codes.Unimplemented, "not implemented in daemon")
	ErrNotSupported   = status.Errorf(codes.Unimplemented, "not supported in daemon")
	//ErrInvalidParameter   = status.Errorf(codes.InvalidArgument, "invalid parameter")
	//ErrFailedToRemove     = status.Errorf(codes.Internal, "replica removal failed")
	// ErrNotFound         = errors.New("function not defined: %s")
	// ErrNoHandler          = status.Errorf(codes.NotFound, "handler not defined")
	//ErrNotImplementedKube = status.Errorf(codes.Unimplemented, "not supported in Kubernetes-based implementation (yet)")

	NotificationTypeNames = []string{"ERROR", "WARNING", "INFO", "SUCCESS"}

	// Internal errors

	ErrKernelNotFound          = errors.New("kernel not found")
	ErrHostNotFound            = errors.New("host not found")
	ErrInvalidSocketType       = errors.New("invalid socket type specified")
	ErrKernelNotReady          = errors.New("kernel not ready")
	ErrActiveExecutionNotFound = errors.New("active execution for specified kernel could not be found")
	ErrKernelSpecNotFound      = errors.New("kernel spec not found")
	ErrResourceSpecNotFound    = errors.New("the kernel does not have a resource spec included with its kernel spec")
	ErrKernelIDRequired        = errors.New("kernel id frame is required for kernel_info_request")
	ErrDaemonNotFoundOnNode    = errors.New("could not find a local daemon on the specified kubernetes node")
	ErrFailedToVerifyMessage   = errors.New("failed to verify ZMQ message after (re)encoding it with modified contents")
	ErrRequestTimedOut         = errors.New("request timed out")
	ErrSessionNotTraining      = errors.New("expected session to be training")
	ErrSessionNotFound         = errors.New("could not locate scheduling.Session instance")
	//ErrResourceSpecNotRegistered = errors.New("there is no resource spec registered with the kernel")
	//ErrInvalidJupyterMessage     = errors.New("invalid jupter message")
	//ErrHeaderNotFound            = errors.New("message header not found")
)

type GatewayDaemonConfig func(domain.ClusterGateway)

type FailureHandler func(c client.DistributedKernelClient) error

// ClusterGatewayImpl serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bi-directional gRPC calls.
//
// Some useful resources for jupyter protocol:
// https://jupyter-client.readthedocs.io/en/stable/messaging.html
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type ClusterGatewayImpl struct {
	sync.Mutex

	id string

	schedulingPolicy string
	gateway.UnimplementedClusterGatewayServer
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    *scheduling.CoreOptions

	// cluster provisioning related members
	listener net.Listener
	cluster  scheduling.Cluster
	placer   scheduling.Placer

	// kernel members
	transport        string
	ip               string
	sessions         hashmap.HashMap[string, scheduling.Session]             // Map of Sessions
	kernels          hashmap.HashMap[string, client.DistributedKernelClient] // Map with possible duplicate values. We map kernel ID and session ID to the associated kernel. There may be multiple sessions per kernel.
	kernelIdToKernel hashmap.HashMap[string, client.DistributedKernelClient] // Map from Kernel ID to  client.DistributedKernelClient.
	kernelSpecs      hashmap.HashMap[string, *gateway.KernelSpec]

	log logger.Logger

	// lifetime
	closed  int32
	cleaned chan struct{}

	failureHandler FailureHandler

	// Makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// waitGroups hashmap.HashMap[string, *sync.WaitGroup]
	waitGroups hashmap.HashMap[string, *registrationWaitGroups]

	activeExecutions hashmap.HashMap[string, *client.ActiveExecution]

	// Responsible for orchestrating and managing migration operations.
	// migrationManager MigrationManager

	// We configure a pool of available ports through Kubernetes.
	// This is the pool of ports. We use these ports to create ZMQ sockets for kernels.
	// If a kernel stops, then its ports are returned to the pool for future reuse.
	availablePorts *utils.AvailablePorts

	// The IOPub socket that all Jupyter clients subscribe to.
	// iopub *jupyter.Socket
	smrPort int

	// Map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting *hashmap.CornelkMap[string, chan struct{}]

	// Mapping from AddReplicaOperation ID to AddReplicaOperation.
	addReplicaOperations *hashmap.CornelkMap[string, domain.AddReplicaOperation]

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from Operation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, domain.AddReplicaOperation]]

	// Mapping from new pod name to AddReplicaOperation.
	addReplicaOperationsByNewPodName *hashmap.CornelkMap[string, domain.AddReplicaOperation]

	// Mapping from NewPodName to chan string.
	// In theory, it's possible to receive a PodCreated notifcation from Kubernetes AFTER the replica within the new Pod
	// has started running and has registered with the Gateway. In this case, we won't be able to retrieve the AddReplicaOperation
	// associated with that replica via the new Pod's name, as that mapping is created when the PodCreated notification is received.
	// In this case, the goroutine handling the replica registration waits on a channel for the associated AddReplicaOperation.
	addReplicaNewPodNotifications *hashmap.CornelkMap[string, chan domain.AddReplicaOperation]

	// Used to wait for an explicit notification that a particular node was successfully removed from its SMR cluster.
	// smrNodeRemovedNotifications *hashmap.CornelkMap[string, chan struct{}]

	// Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.
	hdfsNameNodeEndpoint string

	// Kubernetes client. This is shared with the associated Cluster Gateway.
	kubeClient domain.KubeClient

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerWatcher domain.ContainerWatcher

	// Called by Kubernetes to involve the Cluster Gateway in scheduling decisions.
	clusterScheduler domain.ClusterScheduler

	// gRPC connection to the Dashboard.
	clusterDashboard gateway.ClusterDashboardClient

	// Run via Docker on a single system rather than using the Kubernetes-based deployment.
	deploymentMode types.DeploymentMode

	// Used in Docker mode. Assigned to individual kernel replicas, incremented after each assignment.
	dockerModeKernelDebugPort atomic.Int32

	// Docker client.
	dockerApiClient *dockerClient.Client

	// The name of the Docker network that the container is running within. Only used in Docker mode.
	dockerNetworkName string
}

func New(opts *jupyter.ConnectionInfo, clusterDaemonOptions *domain.ClusterDaemonOptions, configs ...GatewayDaemonConfig) *ClusterGatewayImpl {
	daemon := &ClusterGatewayImpl{
		id:                               uuid.New().String(),
		connectionOptions:                opts,
		transport:                        "tcp",
		ip:                               opts.IP,
		availablePorts:                   utils.NewAvailablePorts(opts.StartingResourcePort, opts.NumResourcePorts, 2),
		sessions:                         hashmap.NewCornelkMap[string, scheduling.Session](128),
		kernels:                          hashmap.NewCornelkMap[string, client.DistributedKernelClient](128),
		kernelIdToKernel:                 hashmap.NewCornelkMap[string, client.DistributedKernelClient](128),
		kernelSpecs:                      hashmap.NewCornelkMap[string, *gateway.KernelSpec](128),
		waitGroups:                       hashmap.NewCornelkMap[string, *registrationWaitGroups](128),
		cleaned:                          make(chan struct{}),
		smrPort:                          clusterDaemonOptions.SMRPort,
		addReplicaOperations:             hashmap.NewCornelkMap[string, domain.AddReplicaOperation](64),
		activeAddReplicaOpsPerKernel:     hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, domain.AddReplicaOperation]](64),
		addReplicaOperationsByNewPodName: hashmap.NewCornelkMap[string, domain.AddReplicaOperation](64),
		kernelsStarting:                  hashmap.NewCornelkMap[string, chan struct{}](64),
		addReplicaNewPodNotifications:    hashmap.NewCornelkMap[string, chan domain.AddReplicaOperation](64),
		activeExecutions:                 hashmap.NewCornelkMap[string, *client.ActiveExecution](64),
		hdfsNameNodeEndpoint:             clusterDaemonOptions.HdfsNameNodeEndpoint,
		dockerNetworkName:                clusterDaemonOptions.DockerNetworkName,
	}
	for _, configFunc := range configs {
		configFunc(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon, "ClusterGatewayRouter", false)
	daemon.cluster = scheduling.NewCluster()

	placer, err := scheduling.NewRandomPlacer(daemon.cluster, daemon.ClusterOptions)
	if err != nil {
		daemon.log.Error("Failed to create new placer because: %v", err)
		panic(err)
	}

	daemon.placer = placer

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	if len(clusterDaemonOptions.HdfsNameNodeEndpoint) == 0 {
		panic("HDFS NameNode endpoint is empty.")
	}

	switch clusterDaemonOptions.SchedulingPolicy {
	case "default":
		{
			daemon.schedulingPolicy = "default"
			daemon.log.Debug("Using the 'DEFAULT' scheduling policy.")
			daemon.failureHandler = daemon.defaultFailureHandler
		}
	case "static":
		{
			daemon.schedulingPolicy = "static"
			daemon.log.Debug("Using the 'STATIC' scheduling policy.")
			daemon.failureHandler = daemon.staticSchedulingFailureHandler
		}
	case "dynamic-v3":
		{
			daemon.schedulingPolicy = "dynamic-v3"
			daemon.log.Debug("Using the 'DYNAMIC v3' scheduling policy.")
			daemon.failureHandler = daemon.dynamicV3FailureHandler
		}
	case "dynamic-v4":
		{
			daemon.schedulingPolicy = "dynamic-v4"
			daemon.log.Debug("Using the 'DYNAMIC v4' scheduling policy.")
			daemon.failureHandler = daemon.dynamicV4FailureHandler
		}
	default:
		{
			panic(fmt.Sprintf("Unsupported or unknown scheduling policy specified: '%s'", clusterDaemonOptions.SchedulingPolicy))
		}
	}

	switch clusterDaemonOptions.DeploymentMode {
	case "":
		{
			daemon.log.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			daemon.deploymentMode = types.LocalMode
		}
	case "local":
		{
			daemon.log.Info("Running in LOCAL mode.")
			daemon.deploymentMode = types.LocalMode
		}
	case "docker":
		{
			daemon.log.Info("Running in DOCKER mode.")
			daemon.deploymentMode = types.DockerMode

			apiClient, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv)
			if err != nil {
				panic(err)
			}

			daemon.dockerApiClient = apiClient

			daemon.dockerModeKernelDebugPort.Store(DockerKernelDebugPortDefault)
			// daemon.initializeDockerKernelDebugPort()
		}
	case "kubernetes":
		{
			daemon.log.Info("Running in KUBERNETES mode.")
			daemon.deploymentMode = types.KubernetesMode
			daemon.dockerModeKernelDebugPort.Store(-1)
		}
	default:
		{
			daemon.log.Error("Unknown/unsupported deployment mode: \"%s\"", clusterDaemonOptions.DeploymentMode)
			daemon.log.Error("The supported deployment modes are: ")
			daemon.log.Error("- \"kubernetes\"")
			daemon.log.Error("- \"docker\"")
			daemon.log.Error("- \"local\"")
			os.Exit(1)
		}
	}

	if daemon.KubernetesMode() {
		daemon.kubeClient = NewKubeClient(daemon, clusterDaemonOptions)
		daemon.containerWatcher = daemon.kubeClient
	} else if daemon.DockerMode() {
		daemon.containerWatcher = NewDockerContainerWatcher(domain.DockerProjectName) /* TODO: Don't hardcode this (the project name parameter). */
	}

	daemon.clusterScheduler = scheduler.NewClusterScheduler(daemon, daemon.kubeClient, clusterDaemonOptions.ClusterSchedulerOptions)

	return daemon
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
// - numReplicas (int): Value to be added to the registrationWaitGroups's "notified" and "registered" sync.WaitGroups.
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

// Returns true if the node with the given ID was actually removed.
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

// func (wg *registrationWaitGroups) UpdateAndGetReplicasAfterMigration(idx int32, hostname string) []string {
// wg.replicasMutex.Lock()
// defer wg.replicasMutex.Unlock()

// 	wg.replicas[idx] = hostname

// 	return wg.replicas
// }

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

func (d *ClusterGatewayImpl) PingKernel(ctx context.Context, in *gateway.PingInstruction) (*gateway.Pong, error) {
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
		return &gateway.Pong{
			Id:      kernelId,
			Success: false,
		}, ErrInvalidSocketType
	}

	kernel, loaded := d.kernels.Load(kernelId)
	if !loaded {
		d.log.Error("Received 'ping-kernel' request for unknown kernel \"%s\"...", kernelId)
		return &gateway.Pong{
			Id:      kernelId,
			Success: false,
		}, ErrKernelNotFound
	}

	var (
		msg zmq4.Msg
		err error
	)
	frames := jupyter.NewJupyterFramesWithHeader(messageType, kernel.Sessions()[0])
	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		d.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v", kernelId, kernel.ConnectionInfo().SignatureScheme, err)
		return &gateway.Pong{
			Id:      kernelId,
			Success: false,
		}, ErrFailedToVerifyMessage
	}

	// TODO: Really need to rework the request/server system so that timeouts are more centralized.
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	doneChan := make(chan struct{})

	startTime := time.Now()
	var numRepliesReceived atomic.Int32
	responseHandler := func(from scheduling.KernelInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)
		d.log.Debug("Received %s ping_reply from kernel %s. Received %d/3 replies. Time elapsed: %v.", typ.String(), from.ID(), latestNumRepliesReceived, time.Since(startTime))

		// Notify that all replies have been received.
		if latestNumRepliesReceived == 3 {
			doneChan <- struct{}{}
		}

		return nil
	}

	jMsg := jupyter.NewJupyterMessage(&msg)
	err = kernel.RequestWithHandler(ctx, "Forwarding", socketType, jMsg, responseHandler, func() {})
	if err != nil {
		d.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v", socketType.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)
		return &gateway.Pong{
			Id:      kernelId,
			Success: false,
		}, err
	}

	select {
	case <-ctx.Done():
		{
			err := ctx.Err()
			if err != nil {
				errorMessage := fmt.Sprintf("'ping-kernel' %v request for kernel %s failed after receiving %d/3 replies: %v", socketType.String(), kernelId, numRepliesReceived.Load(), err)
				d.log.Error(errorMessage)
				return &gateway.Pong{
					Id:      kernelId,
					Success: false,
					Msg:     errorMessage,
				}, ErrRequestTimedOut
			}
		}
	case <-doneChan:
		{
			d.log.Debug("Received all 3 %v 'ping_reply' responses from replicas of kernel %s in %v.", socketType.String(), kernelId, time.Since(startTime))
		}
	}

	return &gateway.Pong{
		Id:      kernelId,
		Success: true,
	}, nil
}

func (d *ClusterGatewayImpl) SetClusterOptions(opts *scheduling.CoreOptions) {
	d.ClusterOptions = opts
}

func (d *ClusterGatewayImpl) ConnectionOptions() *jupyter.ConnectionInfo {
	return d.connectionOptions
}

func (d *ClusterGatewayImpl) NumLocalDaemonsConnected() int {
	return d.cluster.GetHostManager().Len()
}

func (d *ClusterGatewayImpl) NumKernelsRegistered() int {
	return d.kernels.Len()
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

// func (d *ClusterGatewayImpl) initializeDockerKernelDebugPort() {
// Need to find a series of at least 5 available ports.
// startingPort := DockerKernelDebugPortDefault

// var listOptions container.ListOptions
// if len(d.dockerNetworkName) > 0 {
// 	listOptions = container.ListOptions{
// 		Filters: filters.NewArgs(filters.KeyValuePair{
// 			Key:   "network",
// 			Value: d.dockerNetworkName,
// 		}),
// 	}
// }

// containers, err := d.dockerApiClient.ContainerList(context.Background(), listOptions)
// if err != nil {
// 	d.log.Error("Failed to list Docker containers because: %v", err)
// 	panic(err)
// }

// // Iterate over all containers...
// for _, container := range containers {
// 	container_names := container.Names

// 	// Inspect the name(s) of the container...
// 	for _, container_name := range container_names {
// 		// If it is a kernel replica container...
// 		if strings.HasPrefix(container_name, "kernel") {
// 			// Check the ports.
// 		}
// 	}
// }

// for startingPort < 64000 {
// 	// If we get through the check without this being flipped to false, then we'll know we succeeded in finding 5 free ports available.
// 	success := true

// 	// Look for 5 free ports in a row.
// 	port := startingPort
// 	for ; port < startingPort+5; port++ {
// 		ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))

// 		// If there was an error, then the port is already in-use.
// 		if err != nil {
// 			d.log.Warn("Port %d was unavailable; cannot use for docker kernel debug port.", port)
// 			// Indicate that we failed.
// 			success = false
// 			break
// 		}

// 		d.log.Debug("Port %d appears to be available...", port)

// 		// Close the listener.
// 		ln.Close()
// 	}

// 	// If we found 5 free ports in a row, then we'll start here.
// 	if success {
// 		d.log.Debug("Assigning 'docker kernel debug port' an initial value of %d.", startingPort)
// 		d.dockerModeKernelDebugPort.Store(startingPort)
// 		return
// 	}

// 	// We'll try again (to find 5 free ports in a row), beginning with the next port we've not yet tested.
// 	startingPort = port + 1
// }
// }

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

	// Initialize yamux session for bi-directional gRPC calls
	// At gateway side, we first wait a incoming replacement connection, then create a reverse provisioner connection to the host scheduler.
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
	host, err := NewHostScheduler(incoming.RemoteAddr().String(), gConn, time.Duration(30)*time.Second,
		d.localDaemonDisconnected, 8, 16384, d.cluster)

	if err != nil {
		if errors.Is(err, errRestoreRequired) {
			// Restore host scheduler.
			registered, loaded := d.cluster.GetHostManager().LoadOrStore(host.ID(), host)
			if loaded {
				err := registered.Restore(host, d.localDaemonDisconnected)
				if err != nil {
					d.log.Error("Error while restoring host %v: %v", host, err)
					return nil, err
				}
			} else {
				d.log.Warn("Host scheduler requested for restoration but not found: %s", host.ID())
				return nil, ErrRestorationFailed
			}
		} else {
			d.log.Error("Failed to create host scheduler client: %v", err)
			return nil, err
		}
	}

	d.cluster.GetHostManager().Store(host.ID(), host)
	d.log.Info("Incoming host scheduler %s (node = %s) connected", host.ID(), host.NodeName())
	go d.notifyDashboardOfInfo("Local Daemon Connected", fmt.Sprintf("Local Daemon %s on node %s has connected to the Cluster Gateway.", host.ID(), host.NodeName()))

	return conn, nil
}

// Close are compatible with ClusterGatewayImpl.Close().

// Addr returns the listener's network address.
// Addr is part of the net.Listener implementation.
func (d *ClusterGatewayImpl) Addr() net.Addr {
	return d.listener.Addr()
}

// ClusterScheduler returns the associated ClusterGateway.
func (d *ClusterGatewayImpl) ClusterScheduler() domain.ClusterScheduler {
	return d.clusterScheduler
}

func (d *ClusterGatewayImpl) SetID(ctx context.Context, hostId *gateway.HostId) (*gateway.HostId, error) {
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

	host := targetReplica.Context().Value(client.CtxKernelHost).(scheduling.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	replicaInfo := &gateway.ReplicaInfo{
		ReplicaId: nodeId,
		KernelId:  kernelId,
	}

	d.log.Info("Calling PrepareToMigrate RPC targeting replica %d of kernel %s now.", nodeId, kernelId)
	// Issue the 'prepare-to-migrate' request. We panic if there was an error.
	resp, err := host.PrepareToMigrate(context.TODO(), replicaInfo)
	if err != nil {
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster because: %v", nodeId, kernelId, err))
	}

	// TODO(Ben): Keep track of this. Pass it to the migrated replica so it can read its data directory before it starts running again.
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

	host := targetReplica.Context().Value(client.CtxKernelHost).(scheduling.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	d.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s. Sending request to Local Daemon of replica %d.", nodeId, kernelId, targetReplica.ReplicaID())
	replicaInfo := &gateway.ReplicaInfoWithAddr{
		Id:       nodeId,
		KernelId: kernelId,
		Hostname: fmt.Sprintf("%s:%d", newAddress, d.smrPort),
	}

	// Issue the 'update-replica' request. We panic if there was an error.
	if _, err := host.UpdateReplicaAddr(context.TODO(), replicaInfo); err != nil {
		d.log.Debug("Failed to add replica %d of kernel %s to SMR cluster because: %v", nodeId, kernelId, err)
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	d.log.Debug("Sucessfully updated peer address of replica %d of kernel %s to %s.", nodeId, kernelId, newAddress)
	time.Sleep(time.Second * 5)
}

// Issue an 'add-replica' request to a random replica of a specific kernel, informing that replica and its peers
// to add a new replica to the cluster (with ID `nodeId`).
// func (d *ClusterGatewayImpl) issueAddNodeRequest(kernelId string, nodeId int32, address string) {
// 	d.log.Info("Issuing 'add-replica' request to kernel %s for replica %d.", kernelId, nodeId)

// 	kernelClient, ok := d.kernels.Load(kernelId)
// 	if !ok {
// 		panic(fmt.Sprintf("Could not find distributed kernel client with ID %s.", kernelId))
// 	}

// 	targetReplica := kernelClient.GetReadyReplica()
// 	if targetReplica == nil {
// 		panic(fmt.Sprintf("Could not find any ready replicas for kernel %s.", kernelId))
// 	}

// 	host := targetReplica.Context().Value(client.CtxKernelHost).(scheduling.Host)
// 	if host == nil {
// 		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
// 	}

// 	d.log.Debug("Issuing AddReplica RPC for new replica %d of kernel %s.", nodeId, kernelId)
// 	replicaInfo := &gateway.ReplicaInfoWithAddr{
// 		Id:       nodeId,
// 		KernelId: kernelId,
// 		Hostname: fmt.Sprintf("%s:%d", address, d.smrPort),
// 	}

// 	// Issue the 'add-replica' request. We panic if there was an error.
// 	if _, err := host.AddReplica(context.TODO(), replicaInfo); err != nil {
// 		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
// 	}

// 	d.log.Debug("Sucessfully notified existing replicas of kernel %s that new replica %d has been created.", kernelId, nodeId)
// 	time.Sleep(time.Second * 5)
// }

func (d *ClusterGatewayImpl) SmrReady(ctx context.Context, smrReadyNotification *gateway.SmrReadyNotification) (*gateway.Void, error) {
	kernelId := smrReadyNotification.KernelId

	// First, check if this notification is from a replica of a kernel that is starting up for the very first time.
	// If so, we'll send a notification in the associated channel, and then we'll return.
	kernelStartingChan, ok := d.kernelsStarting.Load(smrReadyNotification.KernelId)
	if ok {
		d.log.Debug("Received 'SMR-READY' notification for newly-starting kernel %s.", smrReadyNotification.KernelId)
		kernelStartingChan <- struct{}{}
		return gateway.VOID, nil
	}

	// Check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	addReplicaOp, ok := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, smrReadyNotification.ReplicaId, true)
	if !ok {
		d.log.Warn("Received 'SMR-READY' notification replica %d, kernel %s; however, no add-replica operation found for specified kernel replica...", smrReadyNotification.ReplicaId, smrReadyNotification.KernelId)
		return gateway.VOID, nil
	}

	if addReplicaOp.Completed() {
		log.Fatalf("Retrieved AddReplicaOperation %v targeting replica %d of kernel %s -- this operation has already completed.\n", addReplicaOp.OperationID(), smrReadyNotification.ReplicaId, kernelId)
	}

	d.log.Debug("Received SMR-READY notification for replica %d of kernel %s [AddOperation.OperationID=%v]", smrReadyNotification.ReplicaId, kernelId, addReplicaOp.OperationID())
	addReplicaOp.ReplicaJoinedSmrChannel() <- struct{}{}

	return gateway.VOID, nil
}

// func (d *ClusterGatewayImpl) SmrNodeRemoved(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
// 	kernelId := replicaInfo.KernelId
// 	d.log.Debug("Received SMR Node-Removed notification for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId)

// 	channelMapKey := fmt.Sprintf("%s-%s", kernelId, replicaInfo.ReplicaId)
// 	channel, ok := d.smrNodeRemovedNotifications.Load(channelMapKey)
// 	if !ok {
// 		panic(fmt.Sprintf("Could not find \"node-removed\" notification channel for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId))
// 	}

// 	channel <- struct{}{}

// 	return gateway.VOID, nil
// }

func (d *ClusterGatewayImpl) SmrNodeAdded(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
	kernelId := replicaInfo.KernelId
	d.log.Debug("Received SMR Node-Added notification for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId)

	// If there's no add-replica operation here, then we'll just return.
	op, op_exists := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, replicaInfo.ReplicaId, true)

	if !op_exists {
		d.log.Warn("No active add-replica operation found for replica %d, kernel %s.", replicaInfo.ReplicaId, kernelId)
		return gateway.VOID, nil
	}

	if op.Completed() {
		log.Fatalf("Retrieved AddReplicaOperation %v targeting replica %d of kernel %s -- this operation has already completed.\n", op.OperationID(), replicaInfo.ReplicaId, kernelId)
	}

	op.SetReplicaJoinedSMR()

	return gateway.VOID, nil
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
func (d *ClusterGatewayImpl) kernelReconnectionFailed(kernel client.KernelReplicaClient, msg *jupyter.JupyterMessage, reconnectionError error) { /* client client.DistributedKernelClient,  */
	_, messageType, err := d.kernelAndTypeFromMsg(msg)
	if err != nil {
		d.log.Error("Failed to extract message type from ZMQ message because: %v", err)
		d.log.Error("ZMQ message in question: %v", msg)
		messageType = "N/A"
	}

	errorMessage := fmt.Sprintf("Failed to reconnect to replica %d of kernel %s while sending %v \"%s\" message: %v", kernel.ReplicaID(), kernel.ID(), msg.Type, messageType, reconnectionError)
	d.log.Error(errorMessage)

	go d.notifyDashboardOfError("Connection to Kernel Lost & Reconnection Failed", errorMessage)
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
func (d *ClusterGatewayImpl) kernelRequestResubmissionFailedAfterReconnection(kernel client.KernelReplicaClient, msg *jupyter.JupyterMessage, resubmissionError error) { /* client client.DistributedKernelClient, */
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

func (d *ClusterGatewayImpl) ExecutionFailed(c client.DistributedKernelClient) error {
	execution := c.ActiveExecution()
	d.log.Warn("Execution %s (attempt %d) failed for kernel %s.", execution.ExecutionId(), execution.AttemptId(), c.ID())

	return d.failureHandler(c)
}

func (d *ClusterGatewayImpl) defaultFailureHandler(c client.DistributedKernelClient) error {
	d.log.Warn("There is no failure handler for the DEFAULT policy.")
	return fmt.Errorf("there is no failure handler for the DEFAULT policy; cannot handle error")
}

func (d *ClusterGatewayImpl) notifyDashboard(notificationName string, notificationMessage string, typ jupyter.NotificationType) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &gateway.Notification{
			Title:            notificationName,
			Message:          notificationMessage,
			NotificationType: int32(typ),
		})

		if err != nil {
			d.log.Error("Failed to send notification to Cluster Dashboard because: %s", err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=%d) notification to Cluster Dashboard.", notificationName, typ)
		}
	}
}

func (d *ClusterGatewayImpl) localDaemonDisconnected(localDaemonId string, nodeName string, errorName string, errorMessage string) (err error) {
	_, err = d.RemoveHost(context.TODO(), &gateway.HostId{
		Id:       localDaemonId,
		NodeName: nodeName, /* Not needed */
	})

	if err != nil {
		d.log.Error("Error while removing local daemon %s (node: %s): %v", localDaemonId, nodeName, err)
	}

	go d.notifyDashboard(errorName, errorMessage, jupyter.WarningNotification)

	return err
}

// Used to issue an "info" notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfInfo(notificationName string, message string) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &gateway.Notification{
			Title:            notificationName,
			Message:          message,
			NotificationType: int32(jupyter.InfoNotfication),
		})

		if err != nil {
			d.log.Error("Failed to send \"%s\" notification to Cluster Dashboard because: %s", notificationName, err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=INFO) notification to Cluster Dashboard.", notificationName)
		}
	}
}

// Used to issue an "error" notification to the Cluster Dashboard.
func (d *ClusterGatewayImpl) notifyDashboardOfError(errorName string, errorMessage string) {
	if d.clusterDashboard != nil {
		_, err := d.clusterDashboard.SendNotification(context.TODO(), &gateway.Notification{
			Title:            errorName,
			Message:          errorMessage,
			NotificationType: int32(jupyter.ErrorNotification),
		})

		if err != nil {
			d.log.Error("Failed to send \"%s\" error notification to Cluster Dashboard because: %s", errorName, err.Error())
		} else {
			d.log.Debug("Successfully sent \"%s\" (typ=ERROR) notification to Cluster Dashboard.", errorName)
		}
	}
}

// staticSchedulingFailureHandler is a callback to be invoked when all replicas of a
// kernel propose 'YIELD' while static scheduling is set as the configured scheduling policy.
func (d *ClusterGatewayImpl) staticSchedulingFailureHandler(c client.DistributedKernelClient) error {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplica := rand.Intn(c.Size()) + 1
	d.log.Debug("Static Failure Handler: migrating replica %d of kernel %s now.", targetReplica, c.ID())

	// Notify the cluster dashboard that we're performing a migration.
	go d.notifyDashboardOfError("ErrAllReplicasProposedYield", fmt.Sprintf("all replicas of kernel %s proposed 'YIELD' during execution", c.ID()))

	activeExecution, ok := d.activeExecutions.Load(c.ID())
	if !ok {
		d.log.Error("Could not find active execution for kernel %s after static scheduling failure.", c.ID())
		go d.notifyDashboardOfError("ErrActiveExecutionNotFound", "active execution for specified kernel could not be found")
		return ErrActiveExecutionNotFound
	}

	msg := activeExecution.Msg()
	// _, header, _, err := jupyter.HeaderFromMsg(msg)

	// TODO(Ben): How to handle this more elegantly?
	// if err != nil {
	// 	d.log.Error("Failed to extract header from execution request because: %v", err)
	// 	return err
	// }

	// TODO(Ben): Pre-reserve resources on the host that we're migrating the replica to.
	// For now, we'll just let the standard scheduling logic handle things, which will prioritize the least-loaded host.
	req := &gateway.MigrationRequest{
		TargetReplica: &gateway.ReplicaInfo{
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
			d.log.Error("Static Failure Handler: failed to migrate replica %d of kernel %s because: %s", targetReplica, c.ID(), err.Error())
			errorChan <- err
		} else {
			d.log.Debug("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s.", targetReplica, c.ID(), resp.Hostname)
		}

		waitGroup.Done()
	}()

	// We'll need this if the migration operation completes successfully.
	nextExecutionAttempt := client.NewActiveExecution(c.ID(), msg.JupyterSession(), activeExecution.AttemptId()+1, c.Size(), msg)

	// Next, let's update the message so that we target the new replica.
	_, _, offset := jupyter.ExtractDestFrame(msg.Frames)
	var frames jupyter.JupyterFrames = msg.Frames
	var metadataFrame = frames[offset:].MetadataFrame()
	var metadataDict map[string]interface{}

	// Don't try to unmarshal the metadata frame unless the size of the frame is non-zero.
	if len(metadataFrame.Frame()) > 0 {
		// Unmarshal the frame. This way, we preserve any other metadata that was
		// originally included with the message when we specify the target replica.
		err := metadataFrame.Decode(&metadataDict)
		if err != nil {
			d.log.Error("Error unmarshalling metadata frame for 'execute_request' message: %v", err)
			return err
		} else {
			d.log.Debug("Unmarshalled metadata frame for 'execute_request' message: %v", metadataDict)
		}
	}

	// Specify the target replica.
	metadataDict[TargetReplicaArg] = targetReplica
	metadataDict[ForceReprocessArg] = true
	err := frames[offset:].EncodeMetadata(metadataDict)
	if err != nil {
		d.log.Error("Failed to encode metadata frame because: %v", err)
		// TODO(Ben): What do we do here?
		return err
	}

	// Regenerate the signature.
	framesWithoutIdentities, _ := jupyter.SkipIdentitiesFrame(msg.Frames)
	if _, err := framesWithoutIdentities.Sign(c.ConnectionInfo().SignatureScheme, []byte(c.ConnectionInfo().Key)); err != nil {
		// Ignore the error; just log it.
		d.log.Warn("Failed to sign frames because %v", err)
	}

	// Ensure that the frames are now correct.
	if verified := d.verifyFrames([]byte(c.ConnectionInfo().Key), c.ConnectionInfo().SignatureScheme, offset, msg.Frames); !verified {
		d.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'", c.ConnectionInfo().SignatureScheme, c.ConnectionInfo().Key)
		d.log.Error("This message will likely be rejected by the kernel:\n%v", msg)
		// TODO(Ben): What do we do here?
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

	// Since the migration operation completed successfully, we can store the new ActiveExecution struct
	// corresponding to this next execution attempt in the 'activeExecutions' map and on the kernel client.
	d.activeExecutions.Store(msg.JupyterMessageId(), activeExecution)
	c.SetActiveExecution(activeExecution)
	nextExecutionAttempt.LinkPreviousAttempt(activeExecution)
	activeExecution.LinkNextAttempt(nextExecutionAttempt)

	d.log.Debug("Resubmitting 'execute_request' message targeting kernel %s now.", c.ID())
	err = d.ShellHandler(c, msg)
	if err != nil {
		d.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go d.notifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) verifyFrames(signkey []byte, signatureScheme string, offset int, frames jupyter.JupyterFrames) bool {
	expect, err := frames.CreateSignature(signatureScheme, signkey, offset)
	if err != nil {
		d.log.Error("Error when creating signature to verify JFrames: %v", err)
		return false
	}

	signature := make([]byte, hex.DecodedLen(len(frames[offset+jupyter.JupyterFrameSignature])))
	if _, err = hex.Decode(signature, frames[offset+jupyter.JupyterFrameSignature]); err != nil {
		d.log.Error("Failed to decode Jupyter message/signature because: %v", err)
		d.log.Error("Erroneous message/frames: %s", frames.String())
	}
	verified := hmac.Equal(expect, signature)

	if !verified {
		d.log.Error("Failed to verify JFrames.\nExpect: '%v'\nSignature: '%v'", expect, signature)
	}

	return verified
}

func (d *ClusterGatewayImpl) dynamicV3FailureHandler(c client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

func (d *ClusterGatewayImpl) dynamicV4FailureHandler(c client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// DockerMode returns true if we're running in Docker (i.e., the Docker-based deployment).
// We could technically be running within a Docker container that is managed/orchestrated
// by Kubernetes. In this case, this function would return false.
func (d *ClusterGatewayImpl) DockerMode() bool {
	return d.deploymentMode == types.DockerMode
}

// KubernetesMode returns true if we're running in Kubernetes.
func (d *ClusterGatewayImpl) KubernetesMode() bool {
	return d.deploymentMode == types.KubernetesMode
}

// Important: exactly ONE of `kernelSpec` and `replicaSpec` must be non-nil. That is, they cannot both be nil, and they cannot both be non-nil.
func (d *ClusterGatewayImpl) launchReplicaDocker(replicaId int, host scheduling.Host, numReplicas int32, kernelSpec *gateway.KernelSpec, replicaSpec *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	var err error

	if kernelSpec == nil && replicaSpec == nil {
		panic("Both `kernelSpec` and `replicaSpec` cannot be nil; exactly one of these two arguments must be non-nil.")
	}

	if kernelSpec != nil && replicaSpec != nil {
		panic("Both `kernelSpec` and `replicaSpec` cannot be non-nil; exactly one of these two arguments must be non-nil.")
	}

	var kernelId string
	if kernelSpec != nil {
		kernelId = kernelSpec.Id
		d.log.Debug("Launching replica %d of kernel %s on host %v now.", replicaId, kernelId, host)
		replicaSpec = &gateway.KernelReplicaSpec{
			Kernel:                    kernelSpec,
			ReplicaId:                 int32(replicaId),
			NumReplicas:               numReplicas,
			DockerModeKernelDebugPort: d.dockerModeKernelDebugPort.Add(1),
		}
	} else {
		kernelId = replicaSpec.Kernel.Id

		// Make sure to assign a value to DockerModeKernelDebugPort if one is not already set.
		if replicaSpec.DockerModeKernelDebugPort <= 1023 {
			replicaSpec.DockerModeKernelDebugPort = d.dockerModeKernelDebugPort.Add(1)
		}

		d.log.Debug("Launching replica %d of kernel %s on host %v now.", replicaSpec.ReplicaId, kernelId, host)
	}

	replicaConnInfo, err := d.placer.Place(host, replicaSpec)
	if err != nil {
		if kernelSpec != nil {
			d.log.Warn("Failed to start kernel replica(%s:%d): %v", kernelId, replicaId, err)
		} else {
			d.log.Warn("Failed to start kernel replica(%s:%d): %v", kernelId, replicaId, err)
		}
		return nil, err
	}

	d.log.Debug("Received replica connection info after calling placer.Place: %v", replicaConnInfo)
	return replicaConnInfo, nil
}

// startNewKernel is called by StartKernel when creating a brand-new kernel, rather than restarting an existing kernel.
func (d *ClusterGatewayImpl) initNewKernel(in *gateway.KernelSpec) (*client.BaseDistributedKernelClient, error) {
	d.log.Debug("Did not find existing interactivePriorityBase with KernelID=\"%s\". Creating new distributedKernelClientImpl now.", in.Id)

	listenPorts, err := d.availablePorts.RequestPorts()
	if err != nil {
		panic(err)
	}

	// Initialize kernel with new context.
	kernel := client.NewDistributedKernel(context.Background(), in, d.ClusterOptions.NumReplicas, d.connectionOptions,
		listenPorts[0], listenPorts[1], uuid.NewString(), d.ExecutionFailed)
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
	resourceUtil := scheduling.NewEmptyResourceUtilization().WithCpuUtilization(0).WithMemoryUsageMb(0).WithNGpuUtilizationValues(in.ResourceSpec.Gpu, 0)
	session := scheduling.NewUserSession(context.Background(), kernel, resourceUtil, d.cluster, d.ClusterOptions)
	d.sessions.Store(kernel.ID(), session)

	// Assign the Session to the DistributedKernelClient.
	kernel.SetSession(session)

	return kernel, nil
}

// handleNewDockerKernel is called by StartKernel to handle Docker-specific initialization steps.
// That is, handleNewDockerKernel is only called when running in Docker Mode.
//
// This function is responsible for interfacing with the Scheduler to determine which virtual nodes to
// place the new kernel replicas on.
//
// This function initiates the scheduling and provisioning process before waiting for the replicas to register
// with the Cluster Gateway (via their Local Daemons).
func (d *ClusterGatewayImpl) handleNewDockerKernel(ctx context.Context, in *gateway.KernelSpec) error {
	// Channel to send either notifications that we successfully launched a replica (in the form of a struct{}{})
	// or errors that occurred when launching a replica.
	resultChan := make(chan interface{}, 3)

	// Identify the hosts onto which we will place replicas of the kernel.
	hosts := d.placer.FindHosts(in.ResourceSpec)

	// For each host, launch a Docker replica on that host.
	for i, host := range hosts {
		// Launch replicas in parallel.
		go func(replicaId int, targetHost scheduling.Host) {
			_, err := d.launchReplicaDocker(replicaId, targetHost, int32(len(hosts)), in, nil) /* Only 1 of arguments 3 and 4 can be non-nil */

			if err != nil {
				// An error occurred. Send it over the channel.
				resultChan <- err
			} else {
				// Send a notification that a replica was launched successfully.
				resultChan <- struct{}{}
			}
		}(i+1, host)
	}

	// Keep looping until we've received all responses or the context times-out.
	responsesReceived := 0
	responsesRequired := len(hosts)
	for responsesReceived < responsesRequired {
		select {
		// Context time-out, meaning the operation itself has timed-out or been cancelled.
		case <-ctx.Done():
			{
				err := ctx.Err()
				if err != nil {
					d.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. Error extracted from now-cancelled context: %v", in.Id, err)
					return err
				} else {
					d.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. No error extracted from now-cancelled context.", in.Id, err)
					return ErrRequestTimedOut // Return generic error if we can't get one from the Context for some reason.
				}
			}
		// Received response.
		case val := <-resultChan:
			{
				if err, ok := val.(error); ok {
					d.log.Error("Error while launching at least one of the replicas of kernel %s: %v", in.Id, err)
					return err
				}

				responsesReceived += 1

				d.log.Debug("Launched %d/%d replica(s) of kernel %s.", responsesReceived, responsesRequired, in.Id)
			}
		}
	}

	return nil
}

// StartKernel launches a new kernel.
func (d *ClusterGatewayImpl) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Info("ClusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%v].", in.Id, in.Session, in.ResourceSpec)

	var (
		kernel client.DistributedKernelClient
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

	// If we're in KubeMode, then
	if d.KubernetesMode() {
		_, err = d.kubeClient.DeployDistributedKernels(ctx, in)
		if err != nil {
			d.log.Error("Error encountered while attempting to create the Kubernetes resources for Session %s", in.Id)
			d.log.Error("%v", err)
			return nil, status.Errorf(codes.Internal, "Failed to start kernel")
		}
	} else if d.DockerMode() {
		err = d.handleNewDockerKernel(ctx, in)
		if err != nil {
			d.log.Error("Error while handling Docker-specific initiation steps for new kernel %s: %v", in.Id, err)
			return nil, err
		}
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

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(kernel.Socket(jupyter.ShellMessage).Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(kernel.Socket(jupyter.IOMessage).Port), // TODO(Ben): Need to set these correctly.
		IosubPort:       int32(kernel.Socket(jupyter.IOMessage).Port), // TODO(Ben): Need to set these correctly.
		SignatureScheme: kernel.KernelSpec().SignatureScheme,
		Key:             kernel.KernelSpec().Key,
	}
	d.log.Info("Kernel(%s) started: %v", kernel.ID(), info)

	// Tell the Dashboard that the kernel has successfully started running.
	go d.notifyDashboard("Kernel Started", fmt.Sprintf("Kernel %s has started running.", kernel.ID()), jupyter.SuccessNotification)

	return info, nil
}

// Handle a registration notification from a new kernel replica that was created during an add-replica/migration operation.
// TODO(Ben): Do I really need the main lock for this function?
// IMPORTANT: This must be called with the main mutex held.
// IMPORTANT: This will release the main mutex before returning.
func (d *ClusterGatewayImpl) handleAddedReplicaRegistration(in *gateway.KernelRegistrationNotification, kernel client.DistributedKernelClient, waitGroup *registrationWaitGroups) (*gateway.KernelRegistrationNotificationResponse, error) {
	addReplicaOp, ok := d.addReplicaOperationsByNewPodName.Load(in.PodName)

	// If we cannot find the migration operation, then we have an unlikely race here.
	// Basically, the new replica Pod was created, started running, and contacted its Local Daemon, which then contacted us,
	// all before we received and processed the associated pod-created notification from kubernetes.
	//
	// So, we have to wait to receive the notification so we can get the migration operation and get the correct SMR node ID for the Pod.
	//
	// This race would be simplified if we added the constraint that there may be only one active migration per kernel at any given time,
	// but I've not yet enforced this.
	if !ok {
		channel := make(chan domain.AddReplicaOperation, 1)
		d.addReplicaNewPodNotifications.Store(in.PodName, channel)

		d.Unlock()
		d.log.Debug("Waiting to receive AddReplicaNotification on NewPodNotification channel. NewPodName: %s.", in.PodName)
		// Just need to provide a mechanism to wait until we receive the pod-created notification, and get the migration operation that way.
		addReplicaOp = <-channel
		d.log.Debug("Received AddReplicaNotification on NewPodNotification channel. NewPodName: %s.", in.PodName)
		d.Lock()

		// Clean up mapping. Don't need it anymore.
		d.addReplicaNewPodNotifications.Delete(in.PodName)
	}

	host, loaded := d.cluster.GetHostManager().Load(in.HostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", in.HostId)) // TODO(Ben): Handle gracefully.
	}

	// The replica spec that was specifically prepared for the new replica during the initiation of the migration operation.
	replicaSpec := addReplicaOp.KernelSpec()
	addReplicaOp.SetReplicaHostname(in.KernelIp)

	// Initialize kernel client
	replica := client.NewKernelClient(context.Background(), replicaSpec, in.ConnectionInfo.ConnectionInfo(), false, -1, -1, in.PodName, in.NodeName, nil, nil, kernel.PersistentID(), in.HostId, host, true, true, d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection)
	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	session, ok := d.sessions.Load(in.PodName)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", in.PodName)
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	container := scheduling.NewBasicContainer(session, replica, host)
	// Assign the Container to the KernelReplicaClient.
	replica.SetContainer(container)
	// Add the Container to the Host.
	host.ContainerScheduled(container)

	d.log.Debug("Adding replica for kernel %s, replica %d on host %s.", addReplicaOp.KernelId(), replicaSpec.ReplicaId, host.ID())
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("interactivePriorityBase::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// d.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaId, in.KernelId, waitGroup.NumReplicas())

	// Store the new replica in the list of replicas for the kernel (at the correct position, based on the SMR node ID).
	// Then, return the list of replicas so that we can pass it to the new replica.
	// updatedReplicas := waitGroup.UpdateAndGetReplicasAfterMigration(migrationOperation.OriginalSMRNodeID()-1, in.KernelIp)
	updatedReplicas := waitGroup.AddReplica(replicaSpec.ReplicaId, in.KernelIp)

	persistentId := addReplicaOp.PersistentID()
	// dataDirectory := addReplicaOp.DataDirectory()
	response := &gateway.KernelRegistrationNotificationResponse{
		Id:                     replicaSpec.ReplicaId,
		Replicas:               updatedReplicas,
		PersistentId:           &persistentId,
		ShouldReadDataFromHdfs: true,
		// DataDirectory: &dataDirectory,
		SmrPort: int32(d.smrPort),
	}

	d.Unlock()

	addReplicaOp.SetReplicaRegistered() // This just sets a flag to true in the migration operation object.

	d.log.Debug("About to issue 'update replica' request for replica %d of kernel %s. Client ready: %v", replicaSpec.ReplicaId, in.KernelId, replica.IsReady())

	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	// Issue the AddNode request now, so that the node can join when it starts up.
	// d.issueAddNodeRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

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
func (d *ClusterGatewayImpl) AddReplicaDynamic(_ context.Context, in *gateway.KernelId) error {
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

func (d *ClusterGatewayImpl) NotifyKernelRegistered(_ context.Context, in *gateway.KernelRegistrationNotification) (*gateway.KernelRegistrationNotificationResponse, error) {
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

	host, loaded := d.cluster.GetHostManager().Load(hostId)
	if !loaded {
		panic(fmt.Sprintf("Expected to find existing Host with ID \"%v\"", hostId)) // TODO(Ben): Handle gracefully.
	}

	if kernel.NumActiveAddOperations() >= 1 {
		d.log.Debug("There is/are %d active add-replica operation(s) targeting kernel %s. Assuming currently-registering replica is for an add-replica operation.", kernel.NumActiveAddOperations(), kernel.ID())
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
	replicaSpec := &gateway.KernelReplicaSpec{
		Kernel:      kernelSpec,
		ReplicaId:   replicaId,
		NumReplicas: int32(d.ClusterOptions.NumReplicas), // TODO(Ben): Don't hardcode this.
	}

	// Initialize kernel client
	replica := client.NewKernelClient(context.Background(), replicaSpec, connectionInfo.ConnectionInfo(), false, -1, -1, kernelPodName, nodeName, nil, nil, kernel.PersistentID(), hostId, host, true, true, d.kernelReconnectionFailed, d.kernelRequestResubmissionFailedAfterReconnection)
	session, ok := d.sessions.Load(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Could not find scheduling.Session with ID \"%s\"...", kernelId)
		d.log.Error(errorMessage)
		d.notifyDashboardOfError("Failed to Find scheduling.Session", errorMessage)
		panic(errorMessage)
	}

	container := scheduling.NewBasicContainer(session, replica, host)
	// Assign the Container to the KernelReplicaClient.
	replica.SetContainer(container)
	// Add the Container to the Host.
	host.ContainerScheduled(container)

	d.log.Debug("Validating new interactivePriorityBase for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("interactivePriorityBase::Validate call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	d.log.Debug("Adding Replica interactivePriorityBase for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("interactivePriorityBase::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()
	d.Unlock()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register()
	d.log.Debug("Done registering interactivePriorityBase for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	d.log.Debug("WaitGroup for Kernel \"%s\": %s", kernelId, waitGroup.String())
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &gateway.KernelRegistrationNotificationResponse{
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

func (d *ClusterGatewayImpl) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Debug("StartKernelReplica has been instructed to StartKernel. This is actually not supported/implemented.")

	return nil, ErrNotSupported
}

// GetKernelStatus returns the status of a kernel.
func (d *ClusterGatewayImpl) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
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
func (d *ClusterGatewayImpl) KillKernel(_ context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	d.log.Debug("KillKernel RPC called for kernel %s.", in.Id)

	// Call the impl rather than the RPC stub.
	return d.stopKernelImpl(in)
}

func (d *ClusterGatewayImpl) stopKernelImpl(in *gateway.KernelId) (ret *gateway.Void, err error) {
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
	ret = gateway.VOID

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err = d.errorf(kernel.Shutdown(d.placer.Reclaim, restart))
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
			d.log.Error("Error encountered while deleting cloneset for kernel %s: %v", kernel.ID(), err)
		} else {
			d.log.Debug("Successfully deleted cloneset of deleted kernel %s.", kernel.ID())
		}
	}

	if err == nil {
		go d.notifyDashboard("Kernel Stopped", fmt.Sprintf("Kernel %s has been terminated successfully.", kernel.ID()), jupyter.SuccessNotification)
	} else {
		go d.notifyDashboardOfError("Failed to Terminate Kernel", fmt.Sprintf("An error was encountered while trying to terminate kernel %s: %v.", kernel.ID(), err))
	}

	return
}

// StopKernel stops a kernel.
func (d *ClusterGatewayImpl) StopKernel(_ context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	d.log.Debug("StopKernel RPC called for kernel %s.", in.Id)

	return d.stopKernelImpl(in)
}

// WaitKernel waits for a kernel to exit.
func (d *ClusterGatewayImpl) WaitKernel(_ context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.WaitClosed(), nil)
}

func (d *ClusterGatewayImpl) Notify(_ context.Context, in *gateway.Notification) (*gateway.Void, error) {
	d.log.Debug(utils.NotificationStyles[in.NotificationType].Render("Received %s notification \"%s\": %s"), NotificationTypeNames[in.NotificationType], in.Title, in.Message)
	go d.notifyDashboard(in.Title, in.Message, jupyter.NotificationType(in.NotificationType))
	return gateway.VOID, nil
}

func (d *ClusterGatewayImpl) SpoofNotifications(_ context.Context, _ *gateway.Void) (*gateway.Void, error) {
	go func() {
		d.notifyDashboard("Spoofed Error", "This is a made-up error message sent by the Cluster Gateway.", jupyter.ErrorNotification)
		d.notifyDashboard("Spoofed Warning", "This is a made-up warning message sent by the Cluster Gateway.", jupyter.WarningNotification)
		d.notifyDashboard("Spoofed Info Notification", "This is a made-up 'info' message sent by the Cluster Gateway.", jupyter.InfoNotfication)
		d.notifyDashboard("Spoofed Success Notification", "This is a made-up 'success' message sent by the Cluster Gateway.", jupyter.SuccessNotification)
	}()

	return gateway.VOID, nil
}

// ClusterGateway implementation.

// ID returns the unique ID of the provisioner.
func (d *ClusterGatewayImpl) ID(_ context.Context, _ *gateway.Void) (*gateway.ProvisionerId, error) {
	d.log.Debug("Returning ID for RPC. ID=%s", d.id)
	return &gateway.ProvisionerId{Id: d.id}, nil
}

func (d *ClusterGatewayImpl) RemoveHost(_ context.Context, in *gateway.HostId) (*gateway.Void, error) {
	d.cluster.GetHostManager().Delete(in.Id)
	return gateway.VOID, nil
}

// GetActualGpuInfo is not implemented for the Cluster Gateway.
// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterActualGpuInfo'.
func (d *ClusterGatewayImpl) GetActualGpuInfo(_ context.Context, _ *gateway.Void) (*gateway.GpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetVirtualGpuInfo is not implemented for the Cluster Gateway.
// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterVirtualGpuInfo'.
func (d *ClusterGatewayImpl) GetVirtualGpuInfo(_ context.Context, _ *gateway.Void) (*gateway.VirtualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (d *ClusterGatewayImpl) GetClusterActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterActualGpuInfo, error) {
	resp := &gateway.ClusterActualGpuInfo{
		GpuInfo: make(map[string]*gateway.GpuInfo),
	}

	d.cluster.GetHostManager().Range(func(hostId string, host scheduling.Host) (contd bool) {
		data, err := host.GetActualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve actual GPU info from Local Daemon %s on node %s because: %v", hostId, host.NodeName(), err)
			resp.GpuInfo[host.NodeName()] = nil
		} else {
			resp.GpuInfo[host.NodeName()] = data
		}
		return true
	})

	return resp, nil
}

// Return the current vGPU (or "deflated GPU") resource metrics on the node.
func (d *ClusterGatewayImpl) getClusterVirtualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterVirtualGpuInfo, error) {
	resp := &gateway.ClusterVirtualGpuInfo{
		GpuInfo: make(map[string]*gateway.VirtualGpuInfo),
	}

	d.cluster.GetHostManager().Range(func(hostId string, host scheduling.Host) (contd bool) {
		data, err := host.GetVirtualGpuInfo(ctx, in)
		if err != nil {
			d.log.Error("Failed to retrieve virtual GPU info from Local Daemon %s on node %s because: %v", hostId, host.NodeName(), err)
			resp.GpuInfo[host.NodeName()] = nil
		} else {
			resp.GpuInfo[host.NodeName()] = data
		}
		return true
	})

	return resp, nil
}

// Adjust the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (d *ClusterGatewayImpl) setTotalVirtualGPUs(ctx context.Context, in *gateway.SetVirtualGPUsRequest) (*gateway.VirtualGpuInfo, error) {
	d.log.Debug("Recevied 'SetTotalVirtualGPUs' request targeting node %s with %d vGPU(s).", in.KubernetesNodeName, in.Value)
	var targetHost scheduling.Host
	d.log.Debug("We currently have %d LocalDaemons connected.", d.cluster.GetHostManager().Len())
	d.cluster.GetHostManager().Range(func(hostId string, host scheduling.Host) bool {
		if host.NodeName() == in.GetKubernetesNodeName() {
			d.log.Debug("Found LocalDaemon running on target node %s.", in.KubernetesNodeName)
			targetHost = host
			return false // Stop looping.
		} else {
			d.log.Debug("LocalDaemon %s is running on different node: %s.", host.ID(), host.NodeName())
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

func (d *ClusterGatewayImpl) migrateReplicaRemoveFirst(in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
	// We pass 'false' for `wait` here, as we don't really need to wait for the CloneSet to scale-down.
	// As long as the replica is stopped, we can continue.
	dataDirectory := d.issuePrepareMigrateRequest(in.KernelId, in.ReplicaId)

	// TODO: Add support/logic for Docker-based deployment.
	err := d.removeReplica(in.ReplicaId, in.KernelId)
	if err != nil {
		d.log.Error("Error while removing replica %d of kernel %s: %v", in.ReplicaId, in.KernelId, err)
	}

	var numSeconds = 5
	d.log.Debug("Done removing replica %d of kernel %s. Sleeping for %d seconds.", in.ReplicaId, in.KernelId, numSeconds)
	time.Sleep(time.Second * time.Duration(numSeconds))

	// Add a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
	opts := NewAddReplicaWaitOptions(true, true, true)
	addReplicaOp, err := d.addReplica(in, opts, dataDirectory)
	if err != nil {
		d.log.Error("Failed to add new replica %d to kernel %s: %v", in.ReplicaId, in.KernelId, err)
		return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	}

	replica, err := addReplicaOp.KernelReplicaClient().GetReplicaByID(addReplicaOp.ReplicaId())
	if err != nil {
		d.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v", addReplicaOp.ReplicaId(), in.KernelId, err)
		return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	} else {
		d.log.Debug("Successfully added new replica %d to kernel %s during migration operation.", addReplicaOp.ReplicaId(), in.KernelId)
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()

	return &gateway.MigrateKernelResponse{Id: addReplicaOp.ReplicaId(), Hostname: addReplicaOp.ReplicaPodHostname()}, err
}

func (d *ClusterGatewayImpl) GetKubernetesNodes() ([]corev1.Node, error) {
	if d.DockerMode() {
		return make([]corev1.Node, 0), types.ErrIncompatibleDeploymentMode /* TODO: Should I return an error here? Probably? */
	}

	return d.kubeClient.GetKubernetesNodes()
}

func (d *ClusterGatewayImpl) MigrateKernelReplica(_ context.Context, in *gateway.MigrationRequest) (*gateway.MigrateKernelResponse, error) {
	replicaInfo := in.TargetReplica
	targetNode := in.GetTargetNodeId()

	if replicaInfo.GetPersistentId() == "" {
		// Automatically populate the persistent ID field.
		associatedKernel, loaded := d.kernels.Load(replicaInfo.KernelId)

		if !loaded {
			panic(fmt.Sprintf("Could not find kernel with ID \"%s\" during migration of that kernel's replica %d.", replicaInfo.KernelId, replicaInfo.ReplicaId))
		}

		replicaInfo.PersistentId = associatedKernel.PersistentID()

		d.log.Debug("Automatically populated PersistentID field of migration request of replica %d of kernel %s: '%s'", replicaInfo.ReplicaId, replicaInfo.KernelId, replicaInfo.PersistentId)
	}

	if targetNode != "" {
		d.log.Warn("Ignoring specified target node of \"%s\" for now.")
	}

	d.log.Debug("Migrating replica %d of kernel %s now.", replicaInfo.ReplicaId, replicaInfo.KernelId)

	resp, err := d.migrateReplicaRemoveFirst(replicaInfo)
	if err != nil {
		d.log.Error("Migration operation of replica %d of kernel %s to target node %s failed because: %s", replicaInfo.ReplicaId, replicaInfo.KernelId, targetNode, err.Error())
	} else {
		d.log.Debug("Migration operation of replica %d of kernel %s to target node %s completed successfully.", replicaInfo.ReplicaId, replicaInfo.KernelId, targetNode)
	}

	// If there was an error, then err will be non-nil.
	// If there was no error, then err will be nil.
	return resp, err
}

func (d *ClusterGatewayImpl) Start() error {
	d.log.Info("Starting router...")

	// Start the HTTP Kubernetes Scheduler service.
	go d.ClusterScheduler().StartHttpKubernetesSchedulerService()

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
		d.log.Debug("Intercepting \"%v\" message targeting session \"%s\" and using RPC pathway instead...", jupyter.MessageTypeShutdownRequest, sessionId)

		// Stop the kernel. If we get an error, print it here, and then we'll return it.
		var err error
		if _, err = d.stopKernelImpl(&gateway.KernelId{Id: sessionId}); err != nil {
			d.log.Error("Failed to (cleanly) terminate session/kernel \"%s\" because: %v", sessionId, err)

			// Spawn a separate goroutine to send an error notification to the dashboard.
			go d.notifyDashboardOfError(fmt.Sprintf("Failed to Terminate Session %s", sessionId), err.Error())
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
	// kernelId, header, _, err := jupyter.HeaderFromMsg(msg)
	// if err != nil {
	// 	d.log.Error("Could not parse Shell message from %s because: %v", info.String(), err)
	// 	d.log.Error("Message in question: %s", msg.String())
	// 	return err
	// }

	kernel, ok := d.kernels.Load(msg.JupyterSession())
	if !ok && (msg.JupyterMessageType() == ShellKernelInfoRequest || msg.JupyterMessageType() == ShellExecuteRequest) {
		// Register kernel on ShellKernelInfoRequest
		if msg.DestinationId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(msg.DestinationId)
		if !ok {
			d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v", msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())
			return ErrKernelNotFound
		}

		kernel.BindSession(msg.JupyterSession())
		d.kernels.Store(msg.JupyterSession(), kernel)
	}
	if kernel == nil {
		d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v", msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

		if len(msg.DestinationId) == 0 {
			d.log.Error("Extracted empty kernel ID from ZMQ %v message: %v", msg.Type, msg)
			debug.PrintStack()
		}

		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	if msg.JupyterMessageType() == ShellExecuteRequest {
		d.processExecuteRequest(msg, kernel) // , header)
	} else {
		d.log.Debug("Forwarding shell message to kernel %s: %s", msg.DestinationId, msg)
	}

	if err := d.forwardRequest(kernel, jupyter.ShellMessage, msg); err != nil {
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) processExecutionReply(kernelId string) error {
	d.log.Debug("Received execute-reply from kernel %s.", kernelId)

	activeExecution, ok := d.activeExecutions.Load(kernelId)
	if !ok {
		d.log.Error("No active execution registered for kernel %s...", kernelId)
		return nil
	}

	d.log.Debug("Unregistered active execution %s for kernel %s.", activeExecution.ExecutionId(), kernelId)
	d.activeExecutions.Delete(kernelId)

	// Attempt to load the associated Session.
	session, ok := d.sessions.Load(kernelId)
	if !ok {
		errorMessage := fmt.Sprintf("Failed to locate scheduling.Session[ID=\"%s\"] when handling \"execute_reply\" message...", kernelId)
		d.log.Error(errorMessage)
		return fmt.Errorf("%w: Kernel ID: \"%s\"", ErrSessionNotFound, kernelId)
	}

	// Validate that the Session is currently training.
	if !session.IsTraining() {
		sessionState := session.GetState()
		d.log.Error("Session \"%s\" is supposed to be training (as we just received \"execute_reply\"); however, Session \"%s\" is in state '%v'...", kernelId, kernelId, sessionState)
		return fmt.Errorf("%w: found session in state '%v'", ErrSessionNotTraining, sessionState)
	}

	// Record that the Session has stopped training.
	if p := session.TrainingStopped(); p.Error() != nil {
		err := p.Error()
		d.log.Error("Error when stopping training for Session \"%s\": %v", kernelId, err)
		return err
	}

	return nil
}

func (d *ClusterGatewayImpl) processExecuteRequest(msg *jupyter.JupyterMessage, kernel client.DistributedKernelClient) {
	d.log.Debug("Forwarding shell EXECUTE_REQUEST message to kernel %s: %s", kernel.ID(), msg)

	activeExecution := client.NewActiveExecution(kernel.ID(), msg.JupyterSession(), 1, kernel.Size(), msg)
	d.activeExecutions.Store(kernel.ID(), activeExecution)
	kernel.SetActiveExecution(activeExecution)

	d.log.Debug("Created and assigned new ActiveExecution to Kernel %s: %v", kernel.ID(), activeExecution)
}

func (d *ClusterGatewayImpl) StdinHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *ClusterGatewayImpl) HBHandler(_ router.RouterInfo, msg *jupyter.JupyterMessage) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (d *ClusterGatewayImpl) FailNextExecution(ctx context.Context, in *gateway.KernelId) (*gateway.Void, error) {
	d.log.Debug("Received 'FailNextExecution' request targeting kernel %s.", in.Id)

	var (
		kernel client.DistributedKernelClient
		loaded bool
	)

	// Ensure that the kernel exists.
	if kernel, loaded = d.kernels.Load(in.Id); !loaded {
		d.log.Error("Could not find kernel %s specified in 'FailNextExecution' request...", in.Id)
		return gateway.VOID, ErrKernelNotFound
	}

	hostManager := d.cluster.GetHostManager()
	for _, replica := range kernel.Replicas() {
		replicaClient := replica.(client.KernelReplicaClient)
		hostId := replicaClient.HostId()
		host, ok := hostManager.Load(hostId)

		if !ok {
			d.log.Error("Could not find host %s on which replica %d of kernel %s is supposedly running...", hostId, replicaClient.ReplicaID(), in.Id)
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed", fmt.Sprintf("Could not find host %s on which replica %d of kernel %s is supposedly running...", hostId, replicaClient.ReplicaID(), in.Id))
			return gateway.VOID, ErrHostNotFound
		}

		// Even if there's an error here, we'll just keeping trying. If only some of these succeed, then the system won't explode.
		// The kernels for which the `YieldNextExecution` succeeded will simply yield.
		_, err := host.YieldNextExecution(ctx, in)
		if err != nil {
			d.log.Error("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s", hostId, host.Addr(), err.Error())
			go d.notifyDashboardOfError("'FailNextExecution' Request Failed", fmt.Sprintf("Failed to issue 'FailNextExecution' to Local Daemon %s (%s) because: %s", hostId, host.Addr(), err.Error()))
		} else {
			d.log.Debug("Successfully issued 'FailNextExecution' to Local Daemon %s (%s) targeting kernel %s.", hostId, host.Addr(), in.Id)
		}
	}

	return &gateway.Void{}, nil
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
// func (d *ClusterGatewayImpl) idFromMsg(msg *jupyter.JupyterMessage) (id string, sessId bool, err error) {
// 	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)
// 	if kernelId != "" {
// 		return kernelId, false, nil
// 	}

// 	header, err := d.headerFromFrames(msg.Frames[offset:])
// 	if err != nil {
// 		return "", false, err
// 	}

// 	return header.Session, true, nil
// }

func (d *ClusterGatewayImpl) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
	jFrames := jupyter.JupyterFrames(frames)
	if err := jFrames.Validate(); err != nil {
		d.log.Error(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header from message: %v"), err)
		return nil, err
	}

	var header jupyter.MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		return nil, err
	}

	return &header, nil
}

// Return the add-replica operation associated with the given Kernel ID and SMR Node ID of the new replica.
//
// This looks for the most-recently-added AddReplicaOperation associated with the specified replica of the specified kernel.
// If `mustBeActive` is true, then we skip any AddReplicaOperation structs that have already been marked as completed.
func (d *ClusterGatewayImpl) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32, mustBeActive bool) (domain.AddReplicaOperation, bool) {
	d.addReplicaMutex.Lock()
	defer d.addReplicaMutex.Unlock()

	activeOps, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		return nil, false
	}

	var op domain.AddReplicaOperation
	// Iterate from newest to oldest, which entails beginning at the back.
	// We want to return the newest AddReplicaOperation that matches the replica ID for this kernel.
	for el := activeOps.Back(); el != nil; el = el.Prev() {
		op = el.Value

		// Check that the replica IDs match.
		// If they do match, then we either must not be bothering to check if the operation is still active, or it must still be active.
		if op.ReplicaId() == smrNodeId && (!mustBeActive || op.IsActive()) {
			return op, true
		}
	}

	return nil, false
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *ClusterGatewayImpl) kernelIdAndTypeFromMsg(msg *jupyter.JupyterMessage) (id string, messageType string, sessId bool, err error) {
	kernelId, _, offset := jupyter.ExtractDestFrame(msg.Frames)
	header, err := d.headerFromFrames(msg.Frames[offset:])
	if err != nil {
		return "", "", false, err
	}

	if kernelId == "" {
		d.log.Error("Extracted empty string for kernel ID from message. Will try using session ID instead.")
		kernelId = header.Session
	}

	return kernelId, header.MsgType, true, nil
}

// Extract the Kernel ID and the message type from the given ZMQ message.
func (d *ClusterGatewayImpl) kernelAndTypeFromMsg(msg *jupyter.JupyterMessage) (kernel client.DistributedKernelClient, messageType string, err error) {
	// var (
	// 	kernelId string
	// )

	// kernelId, messageType, _, err = d.kernelIdAndTypeFromMsg(msg)
	// if err != nil {
	// 	return nil, messageType, err
	// }

	// This is initially the kernel's ID, which is the DestID field of the message.
	// But we may not have set a destination ID field within the message yet.
	// In this case, we'll fall back to the session ID within the message's Jupyter header.
	// This may not work either, though, if that session has not been bound to the kernel yet.
	//
	// When Jupyter clients connect for the first time, they send both a shell and a control "kernel_info_request" message.
	// This message is used to bind the session to the kernel (specifically the shell message).
	var kernelKey string = msg.DestinationId

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

// func (d *ClusterGatewayImpl) kernelFromMsg(msg *jupyter.JupyterMessage) (client.DistributedKernelClient, error) {
// 	kernelId, header, err := d.headerFromMsg(msg)
// 	if err != nil {
// 		return nil, err
// 	}

// 	kernel, ok := d.kernels.Load(kernelId)
// 	if !ok {
// 		d.log.Error("Cannot find kernel \"%s\" specified in %v message %s of type '%v'. Message: %v", kernelId, header.MsgType, header.MsgID, header.MsgType, msg)
// 		return nil, ErrKernelNotFound
// 	}

// 	if kernel.Status() != jupyter.KernelStatusRunning {
// 		return kernel, ErrKernelNotReady
// 	}

// 	return kernel, nil
// }

func (d *ClusterGatewayImpl) forwardRequest(kernel client.DistributedKernelClient, typ jupyter.MessageType, msg *jupyter.JupyterMessage) (err error) {
	goroutineId := goid.Get()
	// var messageType string
	if kernel == nil {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting unknown kernel/session. Inspecting now: %v"), goroutineId, typ.String(), msg.Msg.String())
		kernel, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	} else {
		d.log.Debug(utils.BlueStyle.Render("[gid=%d] Received %s message targeting kernel %s. Inspecting now..."), goroutineId, typ.String(), kernel.ID())
		// _, _ /* messageType */, err = d.kernelAndTypeFromMsg(msg)
	}

	if err != nil {
		d.log.Error("[gid=%d] Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.", goroutineId, typ, err, msg)
		return err
	}

	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelResponseForwarder, func() {})
}

func (d *ClusterGatewayImpl) kernelResponseForwarder(from scheduling.KernelInfo, typ jupyter.MessageType, msg *jupyter.JupyterMessage) error {
	goroutineId := goid.Get()
	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if typ == jupyter.ShellMessage {
		if msg.JupyterMessageType() == ShellExecuteReply {
			err := d.processExecutionReply(from.ID())
			if err != nil {
				panic(err)
			}
		}
	}

	d.log.Debug("[gid=%d] Forwarding %v response from kernel %s via %s: %v", goroutineId, typ, from.ID(), socket.Name, msg)
	err := socket.Send(*msg.Msg)

	if err != nil {
		d.log.Error("[gid=%d] Error while forwarding %v response from kernel %s via %s: %s", goroutineId, typ, from.ID(), socket.Name, err.Error())
	}

	return err // Will be nil on success.
}

func (d *ClusterGatewayImpl) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *ClusterGatewayImpl) statusErrorf(status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *ClusterGatewayImpl) cleanUp() {
	// Clear nothing for now:
	// Hosts and kernels may contact other gateways to restore status.
	close(d.cleaned)
}

// func (d *ClusterGatewayImpl) closeReplica(host scheduling.Host, kernel client.DistributedKernelClient, replica *client.interactivePriorityBase, replicaId int, reason string) {
// 	defer replica.Close()

// 	if err := d.placer.Reclaim(host, kernel, false); err != nil {
// 		d.log.Warn("Failed to close kernel(%s:%d) after %s, failure: %v", kernel.ID(), replicaId, reason, err)
// 	}
// }

// Add a new replica to a particular distributed kernel.
// This is only used for adding new replicas beyond the base set of replicas created
// when the CloneSet is first created. The first 3 (or however many there are configured
// to be) replicas are created automatically by the CloneSet.
//
// Parameters:
// - kernelId (string): The ID of the kernel to which we're adding a new replica.
// - opts (AddReplicaWaitOptions): Specifies whether we'll wait for registration and/or SMR-joining.
// - dataDirectory (string): Path to etcd-raft data directory in HDFS.
func (d *ClusterGatewayImpl) addReplica(in *gateway.ReplicaInfo, opts domain.AddReplicaWaitOptions, dataDirectory string) (domain.AddReplicaOperation, error) {
	// TODO: Add support/logic for Docker-based deployment.

	var kernelId string = in.KernelId
	var persistentId string = in.PersistentId

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
	var newReplicaSpec *gateway.KernelReplicaSpec = kernel.PrepareNewReplica(persistentId, smrNodeId)

	addReplicaOp := NewAddReplicaOperation(kernel, newReplicaSpec, dataDirectory)

	d.log.Debug("Adding replica %d to kernel %s now.", newReplicaSpec.ReplicaId, kernelId)

	// Add the AddReplicaOperation to the associated maps belonging to the Gateway Daemon.
	d.addReplicaMutex.Lock()
	d.addReplicaOperations.Store(addReplicaOp.OperationID(), addReplicaOp)
	ops, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		ops = orderedmap.NewOrderedMap[string, domain.AddReplicaOperation]()
	}
	ops.Set(addReplicaOp.OperationID(), addReplicaOp)
	d.activeAddReplicaOpsPerKernel.Store(kernelId, ops)
	d.addReplicaMutex.Unlock()

	d.containerWatcher.RegisterChannel(kernelId, addReplicaOp.ReplicaStartedChannel())

	if d.KubernetesMode() {
		err := d.kubeClient.ScaleOutCloneSet(kernelId)
		if err != nil {
			d.log.Error("Failed to add replica to kernel %s. Could not scale-up CloneSet because: %v", kernelId, err)
			return addReplicaOp, err
		}
	} else {
		blacklist := make([]interface{}, 0)

		// We "blacklist" all of the hosts for which other replicas of this kernel are scheduled.
		// That way, we'll necessarily select a host on which no other replicas of this kernel are running.
		for _, replica := range kernel.Replicas() {
			host := replica.GetHost()
			if host == nil {
				// This shouldn't happen as far as I know, but if it does, then we can't really identify the proper host to migrate to.
				d.log.Error("Replica %d of kernel %s does NOT have a host...", replica.ReplicaID(), replica.ID())

				go d.notifyDashboard("No Host Assigned to Kernel", fmt.Sprintf("Replica %d of kernel %s does NOT have a host...", replica.ReplicaID(), replica.ID()), jupyter.WarningNotification)
				continue
			}

			d.log.Debug("Adding host %s (on node %s) of kernel %s-%d to blacklist.", replica.GetHost().ID(), replica.GetHost().NodeName(), replica.ID(), replica.ReplicaID())
			blacklist = append(blacklist, replica.GetHost().GetMeta(scheduling.HostMetaRandomIndex))
		}

		host := d.placer.FindHost(blacklist, newReplicaSpec.ResourceSpec())
		d.log.Debug("Selected host %s as target for migration. Will migrate kernel %s-%d to host %s.", host.ID(), kernelId, in.ReplicaId, host.ID())

		connInfo, err := d.launchReplicaDocker(int(newReplicaSpec.ReplicaId), host, 3, nil, newReplicaSpec) /* Only 1 of arguments 3 and 4 can be non-nil */
		// connInfo, err := d.placer.Place(host, newReplicaSpec)

		if err != nil {
			d.log.Error("Failed to add replica to kernel %s. Exception encountered by Placer: %v.", kernelId, err)
			return addReplicaOp, err
		} else {
			d.log.Debug("Received replica connection info after calling placer.Place: %v", connInfo)
		}
	}

	d.log.Debug("Waiting for new replica to be created for kernel %s.", kernelId)

	// Always wait for the scale-out operation to complete and the new replica to be created.
	newReplicaName := <-addReplicaOp.ReplicaStartedChannel()
	d.log.Debug("New replica %s has been created for kernel %s.", newReplicaName, kernelId)
	addReplicaOp.SetPodName(newReplicaName)
	d.addReplicaOperationsByNewPodName.Store(newReplicaName, addReplicaOp)

	d.Lock()

	channel, ok := d.addReplicaNewPodNotifications.Load(newReplicaName)

	if ok {
		channel <- addReplicaOp
	}

	d.Unlock()

	if opts.WaitRegistered() {
		d.log.Debug("Waiting for new replica %d of kernel %s to register...", addReplicaOp.ReplicaId(), kernelId)
		<-addReplicaOp.ReplicaRegisteredChannel()
		d.log.Debug("New replica %d of kernel %s has registered with the Gateway.", addReplicaOp.ReplicaId(), kernelId)
	}

	var smrWg sync.WaitGroup
	smrWg.Add(1)
	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted when the new replica joins its SMR cluster.
	go func() {
		d.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster... [AddOperation.OperationID=%v]]", addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		<-addReplicaOp.ReplicaJoinedSmrChannel()
		d.log.Debug("New replica %d of kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
		kernel.AddOperationCompleted()
		smrWg.Done()
	}()

	if opts.WaitSmrJoined() {
		d.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster...", addReplicaOp.ReplicaId(), kernelId)
		smrWg.Wait()
	}

	// TODO:
	// - Wait for new Pod to register with the Gateway.
	// - Need to avoid race between when we begin waiting and when the registration occurs.
	// - Also need to wait for the new Pod to join the SMR cluster.
	// - I don't think we have a notification for this yet? I'm not sure, though.
	// - We have a notification for "SMR Ready" but not "SMR Joined" as far as I know.

	// Return nil on success.
	return addReplicaOp, nil
}

// Remove a replica from a distributed kernel.
//
// Parameters:
// - smrNodeId (int32): The SMR node ID of the replica that should be removed.
// - kernelId (string): The ID of the kernel from which we're removing a replica.
func (d *ClusterGatewayImpl) removeReplica(smrNodeId int32, kernelId string) error {
	// TODO: Add support/logic for Docker-based deployment.

	kernelClient, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernel client for kernel %s.", kernelId)
		return ErrKernelNotFound
	}

	replica, err := kernelClient.GetReplicaByID(smrNodeId)
	if err != nil {
		d.log.Error("Could not find replica of kernel %s with ID %d.", kernelId, smrNodeId)
		return ErrKernelIDRequired
	}

	oldPodName := replica.PodName()

	// Create a channel that will be used to signal that the node has been removed from its SMR cluster.
	// nodeRemovedNotificationChannel := make(chan struct{}, 1)
	// channelMapKey := fmt.Sprintf("%s-%s", kernelId, smrNodeId)
	// d.smrNodeRemovedNotifications.Store(channelMapKey, nodeRemovedNotificationChannel)

	// First, stop the kernel on the replica we'd like to remove.
	_, err = kernelClient.RemoveReplicaByID(smrNodeId, d.placer.Reclaim, false)
	if err != nil {
		d.log.Error("Error while stopping replica %d of kernel %s: %v", smrNodeId, kernelId, err)
		return err
	}

	wg, ok := d.waitGroups.Load(kernelId)
	if !ok {
		d.log.Error("Could not find WaitGroup for kernel %s after removing replica %d of said kernel...", kernelId, smrNodeId)
		return err
	}

	removed := wg.RemoveReplica(smrNodeId)
	if !removed {
		// TODO(Ben): We won't necessarily always return an error for this, but it's definitely problematic.
		// For now, I will return an error so I can debug the situation if it arises, because I don't think
		// it ever should if things are working correctly.
		d.log.Error("Now-removed replica %d of kernel %s was not present in associated WaitGroup...")
		return err
	}

	d.log.Debug("Successfully removed replica %d of kernel %s.", smrNodeId, kernelId)

	// If we're running in Kubernetes mode, then we'll explicitly scale-in the CloneSet and wait for the Pod to stop.
	if d.KubernetesMode() {
		podStoppedChannel := make(chan struct{}, 1) // Buffered.

		// Next, scale-in the CloneSet, taking care to ensure the correct Pod is deleted.
		err = d.kubeClient.ScaleInCloneSet(kernelId, oldPodName, podStoppedChannel)
		if err != nil {
			d.log.Error("Error while scaling-in CloneSet for kernel %s: %v", kernelId, err)
			return err
		}

		<-podStoppedChannel
		d.log.Debug("Successfully scaled-in CloneSet by deleting Pod %s.", oldPodName)
	}

	return nil
}

func (d *ClusterGatewayImpl) listKernels() (*gateway.ListKernelsResponse, error) {
	resp := &gateway.ListKernelsResponse{
		Kernels: make([]*gateway.DistributedJupyterKernel, 0, max(d.kernelIdToKernel.Len(), 1)),
	}

	d.Lock()
	defer d.Unlock()

	d.kernelIdToKernel.Range(func(id string, kernel client.DistributedKernelClient) bool {
		// d.log.Debug("Will be returning Kernel %s with %d replica(s) [%v] [%v].", id, kernel.Size(), kernel.Status(), kernel.AggregateBusyStatus())

		respKernel := &gateway.DistributedJupyterKernel{
			KernelId:            id,
			NumReplicas:         int32(kernel.Size()),
			Status:              kernel.Status().String(),
			AggregateBusyStatus: kernel.AggregateBusyStatus(),
			KernelSpec:          kernel.KernelSpec(),
		}

		replicas := make([]*gateway.JupyterKernelReplica, 0, len(kernel.Replicas()))
		for _, replica := range kernel.Replicas() {
			kernelReplica := &gateway.JupyterKernelReplica{
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
