package daemon

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
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

	// Passed within the metadata dict of an 'execute_request' ZMQ message.
	// This indicates that a specific replica should execute the code.
	TargetReplicaArg = "target_replica"
)

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")
	ErrNoHandler          = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented     = status.Errorf(codes.Unimplemented, "not implemented in daemon")
	ErrNotImplementedKube = status.Errorf(codes.Unimplemented, "not supported in Kubernetes-based implementation (yet)")
	ErrNotSupported       = status.Errorf(codes.Unimplemented, "not supported in daemon")
	ErrInvalidParameter   = status.Errorf(codes.InvalidArgument, "invalid parameter")
	ErrFailedToRemove     = status.Errorf(codes.Internal, "replica removal failed")

	// Internal errors
	ErrHeaderNotFound            = errors.New("message header not found")
	ErrKernelNotFound            = errors.New("kernel not found")
	ErrKernelNotReady            = errors.New("kernel not ready")
	ErrKernelSpecNotFound        = errors.New("kernel spec not found")
	ErrResourceSpecNotFound      = errors.New("the kernel does not have a resource spec included with its kernel spec")
	ErrResourceSpecNotRegistered = errors.New("there is no resource spec registered with the kernel")
	ErrInvalidJupyterMessage     = errors.New("invalid jupter message")
	ErrKernelIDRequired          = errors.New("kernel id frame is required for kernel_info_request")
	ErrDaemonNotFoundOnNode      = errors.New("could not find a local daemon on the specified kubernetes node")
)

type GatewayDaemonConfig func(domain.ClusterGateway)

type FailureHandler func(c client.DistributedKernelClient) error

// clusterGatewayImpl serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bi-directional gRPC calls.
//
// Some useful resources for jupyter protocol:
// https://jupyter-client.readthedocs.io/en/stable/messaging.html
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type clusterGatewayImpl struct {
	sync.Mutex

	id string

	schedulingPolicy string
	gateway.UnimplementedClusterGatewayServer
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    *core.CoreOptions

	// cluster provisioning related members
	listener net.Listener
	cluster  core.Cluster
	placer   core.Placer

	// kernel members
	transport        string
	ip               string
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

	clusterScheduler domain.ClusterScheduler
}

func New(opts *jupyter.ConnectionInfo, clusterDaemonOptions *domain.ClusterDaemonOptions, configs ...GatewayDaemonConfig) *clusterGatewayImpl {
	daemon := &clusterGatewayImpl{
		id:                               uuid.New().String(),
		connectionOptions:                opts,
		transport:                        "tcp",
		ip:                               opts.IP,
		availablePorts:                   utils.NewAvailablePorts(opts.StartingResourcePort, opts.NumResourcePorts, 2),
		kernels:                          hashmap.NewCornelkMap[string, client.DistributedKernelClient](1000),
		kernelIdToKernel:                 hashmap.NewCornelkMap[string, client.DistributedKernelClient](1000),
		kernelSpecs:                      hashmap.NewCornelkMap[string, *gateway.KernelSpec](100),
		waitGroups:                       hashmap.NewCornelkMap[string, *registrationWaitGroups](100),
		cleaned:                          make(chan struct{}),
		smrPort:                          clusterDaemonOptions.SMRPort,
		addReplicaOperations:             hashmap.NewCornelkMap[string, domain.AddReplicaOperation](64),
		activeAddReplicaOpsPerKernel:     hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, domain.AddReplicaOperation]](64),
		addReplicaOperationsByNewPodName: hashmap.NewCornelkMap[string, domain.AddReplicaOperation](64),
		addReplicaNewPodNotifications:    hashmap.NewCornelkMap[string, chan domain.AddReplicaOperation](64),
		activeExecutions:                 hashmap.NewCornelkMap[string, *client.ActiveExecution](64),
		hdfsNameNodeEndpoint:             clusterDaemonOptions.HDFSNameNodeEndpoint,
	}
	for _, config := range configs {
		config(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon)
	daemon.cluster = core.NewCluster()
	daemon.placer = core.NewRandomPlacer(daemon.cluster, daemon.ClusterOptions)

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	daemon.kubeClient = NewKubeClient(daemon, clusterDaemonOptions)
	daemon.clusterScheduler = scheduler.NewClusterScheduler(daemon, daemon.kubeClient, clusterDaemonOptions.ClusterSchedulerOptions)

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
			daemon.failureHandler = daemon.staticFailureHandler
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
func NewRegistrationWaitGroups(numReplicas int) *registrationWaitGroups {
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

// Call `Done()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) Notify() {
	wg.notified.Done()

	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()
	wg.numNotified += 1
}

// Call `Done()` on the "registered" sync.WaitGroup.
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

// Return the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) GetNotified() *sync.WaitGroup {
	return &wg.notified
}

// Return the "registered" sync.WaitGroup.
func (wg *registrationWaitGroups) GetRegistered() *sync.WaitGroup {
	return &wg.registered
}

// Call `Wait()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) WaitNotified() {
	wg.notified.Wait()
}

// Call `Wait()` on the "registered" sync.WaitGroup.
func (wg *registrationWaitGroups) WaitRegistered() {
	wg.registered.Wait()
}

// First, call `Wait()` on the "registered" sync.WaitGroup.
// Then, call `Wait()` on the "notified" sync.WaitGroup.
func (wg *registrationWaitGroups) Wait() {
	wg.WaitRegistered()
	wg.WaitNotified()
}

func (d *clusterGatewayImpl) SetClusterOptions(opts *core.CoreOptions) {
	d.ClusterOptions = opts
}

func (d *clusterGatewayImpl) ConnectionOptions() *jupyter.ConnectionInfo {
	return d.connectionOptions
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (d *clusterGatewayImpl) Listen(transport string, addr string) (net.Listener, error) {
	d.log.Debug("clusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	d.listener = lis
	return d, nil
}

// net.Listener implementation
func (d *clusterGatewayImpl) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := d.listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := incoming

	d.log.Debug("clusterGatewayImpl is accepting a new connection.")

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
	host, err := NewHostScheduler(incoming.RemoteAddr().String(), gConn, time.Duration(30)*time.Second)
	if err == nil {
		d.cluster.GetHostManager().Store(host.ID(), host)
	} else if err == errRestoreRequired {
		// Restore host scheduler.
		registered, loaded := d.cluster.GetHostManager().LoadOrStore(host.ID(), host)
		if loaded {
			registered.Restore(host)
		} else {
			d.log.Warn("Host scheduler requested for restoration but not found: %s", host.ID())
			// TODO: Notify scheduler to restore?
		}
	} else {
		d.log.Error("Failed to create host scheduler client: %v", err)
		return conn, nil
	}

	d.log.Info("Incoming host scheduler %s (node = %s) connected", host.ID(), host.NodeName())
	return conn, nil
}

// Close are compatible with clusterGatewayImpl.Close().

func (d *clusterGatewayImpl) Addr() net.Addr {
	return d.listener.Addr()
}

// Return the associated ClusterGateway.
func (d *clusterGatewayImpl) ClusterScheduler() domain.ClusterScheduler {
	return d.clusterScheduler
}

func (d *clusterGatewayImpl) SetID(ctx context.Context, hostId *gateway.HostId) (*gateway.HostId, error) {
	return nil, ErrNotImplemented
}

// Issue a 'prepare-to-migrate' request to a specific replica of a specific kernel.
// This will prompt the kernel to shutdown its etcd process (but not remove itself from the cluster)
// before writing the contents of its data directory to HDFS.
//
// Returns the path to the data directory in HDFS.
func (d *clusterGatewayImpl) issuePrepareMigrateRequest(kernelId string, nodeId int32) string {
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

	host := targetReplica.Context().Value(client.CtxKernelHost).(core.Host)
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
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	// TODO(Ben): Keep track of this. Pass it to the migrated replica so it can read its data directory before it starts running again.
	var dataDirectory string = resp.DataDir
	d.log.Debug("Sucessfully issued 'prepare-to-migrate' request to replica %d of kernel %s. Data directory: \"%s\"", nodeId, kernelId, dataDirectory)

	time.Sleep(time.Second * 5)

	return dataDirectory
}

// Issue an 'update-replica' request to a random replica of a specific kernel, informing that replica and its peers
// that the replica with ID = `nodeId` has a new peer address, namely `newAddress`.
func (d *clusterGatewayImpl) issueUpdateReplicaRequest(kernelId string, nodeId int32, newAddress string) {
	d.log.Info("Issuing 'update-replica' request to kernel %s for replica %d, newAddr = %s.", kernelId, nodeId, newAddress)

	kernelClient, ok := d.kernels.Load(kernelId)
	if !ok {
		panic(fmt.Sprintf("Could not find distributed kernel client with ID %s.", kernelId))
	}

	targetReplica := kernelClient.GetReadyReplica()
	if targetReplica == nil {
		panic(fmt.Sprintf("Could not find any ready replicas for kernel %s.", kernelId))
	}

	host := targetReplica.Context().Value(client.CtxKernelHost).(core.Host)
	if host == nil {
		panic(fmt.Sprintf("Target replica %d of kernel %s does not have a host.", targetReplica.ReplicaID(), targetReplica.ID()))
	}

	d.log.Debug("Issuing UpdateReplicaAddr RPC for replica %d of kernel %s.", nodeId, kernelId)
	replicaInfo := &gateway.ReplicaInfoWithAddr{
		Id:       nodeId,
		KernelId: kernelId,
		Hostname: fmt.Sprintf("%s:%d", newAddress, d.smrPort),
	}

	// Issue the 'update-replica' request. We panic if there was an error.
	if _, err := host.UpdateReplicaAddr(context.TODO(), replicaInfo); err != nil {
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	d.log.Debug("Sucessfully updated peer address of replica %d of kernel %s to %s.", nodeId, kernelId, newAddress)
	time.Sleep(time.Second * 5)
}

// Issue an 'add-replica' request to a random replica of a specific kernel, informing that replica and its peers
// to add a new replica to the cluster (with ID `nodeId`).
// func (d *clusterGatewayImpl) issueAddNodeRequest(kernelId string, nodeId int32, address string) {
// 	d.log.Info("Issuing 'add-replica' request to kernel %s for replica %d.", kernelId, nodeId)

// 	kernelClient, ok := d.kernels.Load(kernelId)
// 	if !ok {
// 		panic(fmt.Sprintf("Could not find distributed kernel client with ID %s.", kernelId))
// 	}

// 	targetReplica := kernelClient.GetReadyReplica()
// 	if targetReplica == nil {
// 		panic(fmt.Sprintf("Could not find any ready replicas for kernel %s.", kernelId))
// 	}

// 	host := targetReplica.Context().Value(client.CtxKernelHost).(core.Host)
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

func (d *clusterGatewayImpl) SmrReady(ctx context.Context, smrReadyNotification *gateway.SmrReadyNotification) (*gateway.Void, error) {
	kernelId := smrReadyNotification.KernelId

	// First, check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	_, ok := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, smrReadyNotification.ReplicaId)
	if !ok {
		return gateway.VOID, nil
	}

	d.log.Debug("Received SMR-READY notification for replica %d of kernel %s.", smrReadyNotification.ReplicaId, kernelId)
	return gateway.VOID, nil
}

// func (d *clusterGatewayImpl) SmrNodeRemoved(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
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

func (d *clusterGatewayImpl) SmrNodeAdded(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
	kernelId := replicaInfo.KernelId
	d.log.Debug("Received SMR Node-Added notification for replica %d of kernel %s.", replicaInfo.ReplicaId, kernelId)

	// If there's no add-replica operation here, then we'll just return.
	op, op_exists := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, replicaInfo.ReplicaId)
	if !op_exists {
		d.log.Debug("No add-replica operation for replica %d, kernel %s.", replicaInfo.ReplicaId, kernelId)
		return gateway.VOID, nil
	}

	op.SetReplicaJoinedSMR()

	return gateway.VOID, nil
}

func (d *clusterGatewayImpl) ExecutionFailed(c client.DistributedKernelClient) error {
	execution := c.ActiveExecution()
	d.log.Warn("Execution %s (attempt %d) failed for kernel %s.", execution.ExecutionId(), execution.AttemptId(), c.ID())

	return d.failureHandler(c)
}

func (d *clusterGatewayImpl) defaultFailureHandler(c client.DistributedKernelClient) error {
	d.log.Warn("There is no failure handler for the DEFAULT policy.")
	return fmt.Errorf("there is no failure handler for the DEFAULT policy; cannot handle error")
}

func (d *clusterGatewayImpl) staticFailureHandler(c client.DistributedKernelClient) error {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplica := rand.Intn(c.Size()) + 1
	d.log.Debug("Static Failure Handler: migrating replica %d of kernel %s now.", targetReplica, c.ID())

	// TODO(Ben): Pre-reserve resources on the host that we're migrating the replica to.
	req := &gateway.MigrationRequest{}
	resp, err := d.MigrateKernelReplica(context.TODO(), req)

	if err != nil {
		d.log.Error("Static Failure Handler: failed to migrate replica %d of kernel %s because: %v", targetReplica, c.ID(), err)
		return err
	} else {
		d.log.Debug("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s.", targetReplica, c.ID(), resp.Hostname)
	}

	// TODO(Ben): Now try to execute the code on the migrated replica.

	return nil
}

func (d *clusterGatewayImpl) dynamicV3FailureHandler(c client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

func (d *clusterGatewayImpl) dynamicV4FailureHandler(c client.DistributedKernelClient) error {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// StartKernel launches a new kernel.
func (d *clusterGatewayImpl) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Info("clusterGatewayImpl::StartKernel[KernelId=%s, Session=%s, ResourceSpec=%v].", in.Id, in.Session, in.ResourceSpec)

	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Debug("Did not find existing KernelClient with KernelID=\"%s\". Creating new distributedKernelClientImpl now.", in.Id)

		listenPorts, err := d.availablePorts.RequestPorts()
		if err != nil {
			panic(err)
		}

		// Initialize kernel with new context.
		kernel = client.NewDistributedKernel(context.Background(), in, d.ClusterOptions.NumReplicas, d.connectionOptions, listenPorts[0], listenPorts[1], uuid.NewString(), d.ExecutionFailed)
		d.log.Debug("Initializing Shell Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)
		_, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			kernel.Close()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		d.log.Debug("Initializing IO Forwarder for new distributedKernelClientImpl \"%s\" now.", in.Id)

		_, err = kernel.InitializeIOForwarder()

		if err != nil {
			kernel.Close()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	} else {
		d.log.Info("Restarting %v...", kernel)
		kernel.BindSession(in.Session)
	}

	created := NewRegistrationWaitGroups(d.ClusterOptions.NumReplicas)

	d.kernelIdToKernel.Store(in.Id, kernel)
	d.kernels.Store(in.Id, kernel)
	d.kernelSpecs.Store(in.Id, in)
	d.waitGroups.Store(in.Id, created)

	_, err := d.kubeClient.DeployDistributedKernels(ctx, in)
	if err != nil {
		d.log.Error("Error encountered while attempting to create the StatefulSet for Session %s", in.Id)
		d.log.Error("%v", err)
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	d.log.Debug("Waiting for all 3 replicas of new kernel %s to register.", in.Id)
	// Wait for all replicas to be created.
	created.Wait()
	d.log.Debug("All 3 replicas of new kernel %s have registered.", in.Id)

	if kernel.Size() == 0 {
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	for _, sess := range kernel.Sessions() {
		d.log.Debug("Storing kernel %v under session ID %s.", kernel, sess)
		d.kernels.Store(sess, kernel)
	}

	// Now that all 3 replicas have started, we need to remove labels from all of the other Kubernetes nodes.

	// Option #1:
	// - When scheduling a new kernel, add labels to ALL of the Kubernetes nodes and allow system to schedule kernels whenever.
	// - Once the kernels have been created and registered, remove labels from all the nodes except the nodes that the kernels are presently running on.

	// Option #2:
	// - When creating a dynamic replica for a new kernel on a particular node, identify the replica that is to be stopped.
	// - Add labels to nodes hosting the other two replicas.
	// - Add a label to the target node for the new dynamic replica.
	// - Update the cloneset to have a nodeAffinity constraint for matching labels.
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
	return info, nil
}

// Handle a registration notification from a new kernel replica that was created during an add-replica/migration operation.
// TODO(Ben): Do I really need the main lock for this function?
// IMPORTANT: This must be called with the main mutex held.
// IMPORTANT: This will release the main mutex before returning.
func (d *clusterGatewayImpl) handleAddedReplicaRegistration(in *gateway.KernelRegistrationNotification, kernel client.DistributedKernelClient, waitGroup *registrationWaitGroups) (*gateway.KernelRegistrationNotificationResponse, error) {
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
	replica := client.NewKernelClient(context.Background(), replicaSpec, in.ConnectionInfo.ConnectionInfo(), false, -1, -1, in.PodName, in.NodeName, nil, nil, kernel.PersistentID())
	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("Validation error for new replica %d of kernel %s.", addReplicaOp.ReplicaId(), in.KernelId))
	}

	d.log.Debug("Adding Replica KernelClient for kernel %s, replica %d on host %s.", addReplicaOp.KernelId(), replicaSpec.ReplicaId, host.ID())
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("KernelClient::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// d.log.Debug("Adding replica %d of kernel %s to waitGroup of %d other replicas.", replicaSpec.ReplicaId, in.KernelId, waitGroup.NumReplicas())

	// Store the new replica in the list of replicas for the kernel (at the correct position, based on the SMR node ID).
	// Then, return the list of replicas so that we can pass it to the new replica.
	// updatedReplicas := waitGroup.UpdateAndGetReplicasAfterMigration(migrationOperation.OriginalSMRNodeID()-1, in.KernelIp)
	updatedReplicas := waitGroup.AddReplica(replicaSpec.ReplicaId, in.KernelIp)

	persistent_id := addReplicaOp.PersistentID()
	dataDirectory := addReplicaOp.DataDirectory()
	response := &gateway.KernelRegistrationNotificationResponse{
		Id:            replicaSpec.ReplicaId,
		Replicas:      updatedReplicas,
		PersistentId:  &persistent_id,
		DataDirectory: &dataDirectory,
		SmrPort:       int32(d.smrPort),
	}

	d.Unlock()

	addReplicaOp.SetReplicaRegistered() // This just sets a flag to true in the migration operation object.

	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	// Issue the AddNode request now, so that the node can join when it starts up.
	// d.issueAddNodeRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	d.log.Debug("Done handling registration of added replica %d of kernel %s.", replicaSpec.ReplicaId, in.KernelId)

	return response, nil
}

// Under the static scheduling policy, we dynamically create a new replica to execute
// code when the existing replicas are unable to do so due to resource contention.
//
// Possible errors:
// - ErrKernelSpecNotFound: If there is no mapping from the provided kernel's ID to a kernel spec.
// - ErrResourceSpecNotFound: If there is no resource spec included within the kernel spec of the specified kernel.
func (d *clusterGatewayImpl) AddReplicaDynamic(ctx context.Context, in *gateway.KernelId) error {
	// Steps:
	// - Identify a target node with sufficient resources to serve an execution request for the associated kernel.
	// - Add a label to that node to ensure the new replica is scheduled onto that node.
	// - Increase the size of the associated kernel's Cloneset.

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

	// Increase the size of the associated kernel's Cloneset.

	return nil
}

func (d *clusterGatewayImpl) NotifyKernelRegistered(ctx context.Context, in *gateway.KernelRegistrationNotification) (*gateway.KernelRegistrationNotificationResponse, error) {
	d.log.Info("Received kernel registration notification.")

	connectionInfo := in.ConnectionInfo
	sessionId := in.SessionId
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	kernelPodName := in.PodName
	nodeName := in.NodeName

	d.log.Info("Connection info: %v", connectionInfo)
	d.log.Info("Session ID: %v", sessionId)
	d.log.Info("Kernel ID: %v", kernelId)
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
	var replicaId int32 = int32(kernel.Size()) + 1

	// We're registering a new replica, so the number of replicas is based on the cluster configuration.
	replicaSpec := &gateway.KernelReplicaSpec{
		Kernel:      kernelSpec,
		ReplicaId:   replicaId,
		NumReplicas: int32(d.ClusterOptions.NumReplicas), // TODO(Ben): Don't hardcode this.
	}

	// Initialize kernel client
	replica := client.NewKernelClient(context.Background(), replicaSpec, connectionInfo.ConnectionInfo(), false, -1, -1, kernelPodName, nodeName, nil, nil, kernel.PersistentID())
	d.log.Debug("Validating new KernelClient for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err := replica.Validate()
	if err != nil {
		panic(fmt.Sprintf("KernelClient::Validate call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	d.log.Debug("Adding Replica KernelClient for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	err = kernel.AddReplica(replica, host)
	if err != nil {
		panic(fmt.Sprintf("KernelClient::AddReplica call failed: %v", err)) // TODO(Ben): Handle gracefully.
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()
	d.Unlock()

	waitGroup.SetReplica(replicaId, kernelIp)

	waitGroup.Register()
	d.log.Debug("Done registering KernelClient for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	d.log.Debug("WaitGroup for Kernel \"%s\": %s", kernelId, waitGroup.String())
	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.WaitRegistered()

	persistentId := kernel.PersistentID()
	response := &gateway.KernelRegistrationNotificationResponse{
		Id:            replicaId,
		Replicas:      waitGroup.GetReplicas(),
		PersistentId:  &persistentId,
		ResourceSpec:  kernelSpec.ResourceSpec,
		DataDirectory: nil,
		SmrPort:       int32(d.smrPort), // The kernel should already have this info, but we'll send it anyway.
	}

	d.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v", kernelId, replicaId, response)

	waitGroup.Notify()
	return response, nil
}

func (d *clusterGatewayImpl) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Debug("StartKernelReplica has been instructed to StartKernel. This is actually not supported/implemented.")

	return nil, ErrNotSupported
}

// KernelStatus returns the status of a kernel.
func (d *clusterGatewayImpl) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.Status(), nil)
}

// KillKernel kills a kernel.
func (d *clusterGatewayImpl) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	return d.StopKernel(ctx, in)
}

// StopKernel stops a kernel.
func (d *clusterGatewayImpl) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
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
			d.availablePorts.ReturnPorts(listenPorts)

			d.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}

		wg.Done()
	}()

	wg.Wait()
	d.log.Debug("Finished deleting kernel %s.", kernel.ID())

	if !restart {
		d.log.Debug("Deleting cloneset of deleted kernel %s now.", kernel.ID())
		// Delete the CloneSet.
		err := d.kubeClient.DeleteCloneset(kernel.ID())

		if err != nil {
			d.log.Error("Error encountered while deleting cloneset for kernel %s: %v", kernel.ID(), err)
		} else {
			d.log.Debug("Successfully deleted cloneset of deleted kernel %s.", kernel.ID())
		}
	}

	return
}

// WaitKernel waits for a kernel to exit.
func (d *clusterGatewayImpl) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.WaitClosed(), nil)
}

// ClusterGateway implementation.
func (d *clusterGatewayImpl) ID(ctx context.Context, in *gateway.Void) (*gateway.ProvisionerId, error) {
	d.log.Debug("Returning ID for RPC. ID=%s", d.id)
	return &gateway.ProvisionerId{Id: d.id}, nil
}

func (d *clusterGatewayImpl) RemoveHost(ctx context.Context, in *gateway.HostId) (*gateway.Void, error) {
	d.cluster.GetHostManager().Delete(in.Id)
	return gateway.VOID, nil
}

// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterActualGpuInfo'.
func (d *clusterGatewayImpl) GetActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.GpuInfo, error) {
	return nil, ErrNotImplemented
}

// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetClusterVirtualGpuInfo'.
func (d *clusterGatewayImpl) GetVirtualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.VirtualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// Return the current GPU resource metrics on the node.
func (d *clusterGatewayImpl) GetClusterActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterActualGpuInfo, error) {
	resp := &gateway.ClusterActualGpuInfo{
		GpuInfo: make(map[string]*gateway.GpuInfo),
	}

	d.cluster.GetHostManager().Range(func(hostId string, host core.Host) (contd bool) {
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
func (d *clusterGatewayImpl) getClusterVirtualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterVirtualGpuInfo, error) {
	resp := &gateway.ClusterVirtualGpuInfo{
		GpuInfo: make(map[string]*gateway.VirtualGpuInfo),
	}

	d.cluster.GetHostManager().Range(func(hostId string, host core.Host) (contd bool) {
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
func (d *clusterGatewayImpl) setTotalVirtualGPUs(ctx context.Context, in *gateway.SetVirtualGPUsRequest) (*gateway.VirtualGpuInfo, error) {
	d.log.Debug("Recevied 'SetTotalVirtualGPUs' request targeting node %s with %d vGPU(s).", in.KubernetesNodeName, in.Value)
	var targetHost core.Host
	d.log.Debug("We currently have %d LocalDaemons connected.", d.cluster.GetHostManager().Len())
	d.cluster.GetHostManager().Range(func(hostId string, host core.Host) bool {
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

func (d *clusterGatewayImpl) migrate_removeFirst(in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
	// We pass 'false' for `wait` here, as we don't really need to wait for the CloneSet to scale-down.
	// As long as the replica is stopped, we can continue.
	dataDirectory := d.issuePrepareMigrateRequest(in.KernelId, in.ReplicaId)

	err := d.removeReplica(in.ReplicaId, in.KernelId)
	if err != nil {
		d.log.Error("Error while removing replica %d of kernel %s: %v", in.ReplicaId, in.KernelId, err)
	}

	var num_seconds int = 5
	d.log.Debug("Done removing replica %d of kernel %s. Sleeping for %d seconds.", in.ReplicaId, in.KernelId, num_seconds)
	time.Sleep(time.Second * time.Duration(num_seconds))

	// Add a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
	opts := NewAddReplicaWaitOptions(true, true, true)
	addReplicaOp, err := d.addReplica(in, opts, dataDirectory)
	if err != nil {
		d.log.Error("Failed to add new replica %d to kernel %s: %v", addReplicaOp.ReplicaId(), in.KernelId, err)
		return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	}

	replica, err := addReplicaOp.KernelClient().GetReplicaByID(addReplicaOp.ReplicaId())
	if err != nil {
		d.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v", addReplicaOp.ReplicaId(), in.KernelId, err)
		return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
	}

	// The replica is fully operational at this point, so record that it is ready.
	replica.SetReady()

	return &gateway.MigrateKernelResponse{Id: addReplicaOp.ReplicaId(), Hostname: addReplicaOp.ReplicaPodHostname()}, err
}

// func (d *clusterGatewayImpl) migrate_removeLast(ctx context.Context, in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
// 	// First, add a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
// 	opts := NewAddReplicaWaitOptions(true, true, true)
// 	addReplicaOp, err := d.addReplica(in, opts, nil)
// 	if err != nil {
// 		d.log.Error("Failed to add new replica %d to kernel %s: %v", addReplicaOp.ReplicaId(), in.KernelId, err)
// 		return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, err
// 	}

// 	var num_seconds int = 5
// 	d.log.Debug("Done adding replica %d of kernel %s. Sleeping for %d seconds.", addReplicaOp.ReplicaId(), in.KernelId, num_seconds)
// 	time.Sleep(time.Second * time.Duration(num_seconds))
// 	d.log.Debug("Removing replica %d of kernel %s now.", in.ReplicaId, in.KernelId)

// 	// We pass 'false' for `wait` here, as we don't really need to wait for the CloneSet to scale-down.
// 	// As long as the replica is stopped, we can continue.
// 	err = d.removeReplica(in.ReplicaId, in.KernelId, false)
// 	if err != nil {
// 		d.log.Error("Error while removing replica %d of kernel %s: %v", in.ReplicaId, in.KernelId, err)
// 	}

// 	d.log.Debug("Done migrating replica %d of kernel %s now.", in.ReplicaId, in.KernelId)

// 	return &gateway.MigrateKernelResponse{Id: addReplicaOp.ReplicaId(), Hostname: addReplicaOp.ReplicaPodHostname()}, err
// }

func (d *clusterGatewayImpl) GetKubernetesNodes() ([]corev1.Node, error) {
	return d.kubeClient.GetKubernetesNodes()
}

func (d *clusterGatewayImpl) MigrateKernelReplica(ctx context.Context, in *gateway.MigrationRequest) (*gateway.MigrateKernelResponse, error) {
	replicaInfo := in.TargetReplica
	targetNode := in.GetTargetNodeId()

	if replicaInfo.GetPersistentId() == "" {
		// Automatically populate the persistent ID field.
		associatedKernel, loaded := d.kernels.Load(replicaInfo.KernelId)

		if !loaded {
			panic(fmt.Sprintf("Could not find kernel with ID %s during migration of that kernel's replica %d.", replicaInfo.KernelId, replicaInfo.ReplicaId))
		}

		replicaInfo.PersistentId = associatedKernel.PersistentID()

		d.log.Debug("Automatically populated PersistentID field of migration request of replica %d of kernel %s: '%s'", replicaInfo.ReplicaId, replicaInfo.KernelId, replicaInfo.PersistentId)
	}

	if targetNode != "" {
		d.log.Warn("Ignoring specified target node of \"%s\" for now.")
	}

	d.log.Debug("Migrating replica %d of kernel %s now.", replicaInfo.ReplicaId, replicaInfo.KernelId)

	return d.migrate_removeFirst(replicaInfo)
	// return d.migrate_removeLast(ctx, in)

	// The ID of the replica that we're migrating.
	// var targetReplicaId int32 = in.ReplicaId

	// The spec to be used for the new replica that is created during the migration.
	// var newSpec *gateway.KernelReplicaSpec = kernel.PrepareNewReplica(in.PersistentId)

	// d.log.Debug("Migrating replica %d of kernel %s now. New replica ID: %d.", targetReplicaId, kernel.ID(), newSpec.ReplicaId)
	// d.log.Warn("WARNING: This feature has not been reimplemented for Kubernetes yet. This will fail.")

	// replicaSpec := kernel.PrepareNewReplica(in.PersistentId)

	// TODO: Pass decision to the cluster scheduler.
	// host := d.placer.FindHost(replicaSpec.Kernel.Resource)

	// var err error
	// defer func() {
	// 	if err != nil {
	// 		d.log.Warn("Failed to start replica(%s:%d): %v", kernel.KernelSpec().Id, replicaSpec.ReplicaId, err)
	// 	}
	// }()

	// hostname, err := d.kubeClient.InitiateKernelMigration(ctx, kernel, targetReplicaId, newSpec)

	// if err != nil {
	// 	d.log.Error("Error encountered while migrating replica %d of kernel %s: %v", targetReplicaId, kernel.ID(), err)
	// 	err = d.errorf(err) // Reassign err.
	// }

	// `err` may be nil here. The fact that we're returning `err` here doesn't mean `err` is necessarily non-nil.
	// return &gateway.MigrateKernelResponse{Id: targetReplicaId, Hostname: hostname}, err

	// replicaConnInfo, err := d.placer.Place(host, replicaSpec)
	// if err != nil {
	// 	return nil, d.errorf(err)
	// }

	// // Initialize kernel client
	// replica := client.NewKernelClient(context.Background(), replicaSpec, replicaConnInfo.ConnectionInfo())
	// err = replica.Validate()
	// if err != nil {
	// 	d.closeReplica(host, kernel, replica, int(replicaSpec.ReplicaId), "validation error")
	// 	return nil, d.errorf(err)
	// }

	// err = kernel.AddReplica(replica, host)
	// if err != nil {
	// 	d.closeReplica(host, kernel, replica, int(replicaSpec.ReplicaId), "failed adding to the kernel")
	// 	return nil, d.errorf(err)
	// }

	// // Simply remove the replica from the kernel, the caller should stop the replica after confirmed that the new replica is ready.
	// go func() {
	// 	_, err := kernel.RemoveReplicaByID(in.ReplicaId, d.placer.Reclaim, true)
	// 	if err != nil {
	// 		d.log.Warn("Failed to remove replica(%s:%d): %v", kernel.ID(), in.ReplicaId, err)
	// 	}
	// }()

	// return &gateway.ReplicaId{Id: replicaSpec.ReplicaId}, nil
}

func (d *clusterGatewayImpl) Start() error {
	d.log.Info("Starting router...")

	// Start the HTTP Kubernetes Scheduler service.
	go d.ClusterScheduler().StartHttpKubernetesSchedulerService()

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Clean up
	d.cleanUp()

	return err
}

func (d *clusterGatewayImpl) Close() error {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the router
	d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned

	// Close the listener
	if d.listener != nil {
		d.listener.Close()
	}

	return nil
}

// RouterProvider implementations.
func (d *clusterGatewayImpl) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	err := d.forwardRequest(nil, jupyter.ControlMessage, msg)

	// When a kernel is first created/being nudged, Jupyter Server will send both a Shell and Control request.
	// The Control request will just have a Session, and the mapping between the Session and the Kernel will not
	// be established until the Shell message is processed. So, if we see a ErrKernelNotFound error here, we will
	// simply retry it after some time has passed, as the requests are often received very close together.
	if errors.Is(err, ErrKernelNotFound) {
		time.Sleep(time.Millisecond * 500)

		// We won't re-try more than once.
		err = d.forwardRequest(nil, jupyter.ControlMessage, msg)
	}

	return err
}

func (d *clusterGatewayImpl) kernelShellHandler(kernel core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	return d.ShellHandler(kernel, msg)
}

func (d *clusterGatewayImpl) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	kernelId, header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok && (header.MsgType == ShellKernelInfoRequest || header.MsgType == ShellExecuteRequest) {
		// Register kernel on ShellKernelInfoRequest
		if kernelId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(kernelId)
		if !ok {
			d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v", kernelId, header.MsgID, header.MsgType, header.Session)
			return ErrKernelNotFound
		}

		kernel.BindSession(header.Session)
		d.kernels.Store(header.Session, kernel)
	}
	if kernel == nil {
		d.log.Error("Could not find kernel or session %s while handling shell message %v of type '%v', session=%v", kernelId, header.MsgID, header.MsgType, header.Session)

		if len(kernelId) == 0 {
			d.log.Error("Extracted empty kernel ID from ZMQ %v message: %v", msg.Type, msg)
			debug.PrintStack()
		}

		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	if header.MsgType == ShellExecuteRequest {
		d.processExecuteRequest(msg, kernel, header)
	} else {
		d.log.Debug("Forwarding shell message to kernel %s: %s", kernelId, msg)
	}

	if err := d.forwardRequest(kernel, jupyter.ShellMessage, msg); err != nil {
		return err
	}

	return nil
}

func (d *clusterGatewayImpl) processExecutionReply(kernel client.DistributedKernelClient) {
	kernelId := kernel.ID()
	d.log.Debug("Received execute-reply from kernel %s.", kernelId)

	_, ok := d.activeExecutions.Load(kernelId)
	if !ok {
		d.log.Error("No active execution registered for kernel %s...", kernelId)
		return
	}

	d.activeExecutions.Delete(kernelId)
}

func (d *clusterGatewayImpl) processExecuteRequest(msg *zmq4.Msg, kernel client.DistributedKernelClient, header *jupyter.MessageHeader) {
	d.log.Debug("Forwarding shell EXECUTE_REQUEST message to kernel %s: %s", kernel.ID(), msg)

	activeExecution := client.NewActiveExecution(kernel.ID(), header.Session, 1, kernel.Size())
	d.activeExecutions.Store(header.MsgID, activeExecution)
	kernel.SetActiveExecution(activeExecution)

	d.log.Debug("Created and assigned new ActiveExecution to Kernel %s: %v", kernel.ID(), activeExecution)
}

func (d *clusterGatewayImpl) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *clusterGatewayImpl) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
// func (d *clusterGatewayImpl) idFromMsg(msg *zmq4.Msg) (id string, sessId bool, err error) {
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

func (d *clusterGatewayImpl) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
	jFrames := jupyter.JupyterFrames(frames)
	if err := jFrames.Validate(); err != nil {
		return nil, err
	}

	var header jupyter.MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		return nil, err
	}

	return &header, nil
}

// Return the add-replica operation associated with the given Kernel ID and SMR Node ID of the new replica.
func (d *clusterGatewayImpl) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (domain.AddReplicaOperation, bool) {
	d.addReplicaMutex.Lock()
	defer d.addReplicaMutex.Unlock()

	activeOps, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		return nil, false
	}

	var op domain.AddReplicaOperation
	for el := activeOps.Front(); el != nil; el = el.Next() {
		op = el.Value

		if op.ReplicaId() == smrNodeId {
			return op, true
		}
	}

	return nil, false
}

func (d *clusterGatewayImpl) headerFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, err error) {
	// d.log.Debug("Extracting kernel ID from ZMQ %v message: %v", msg.Type, msg)
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)

	// if len(kernelId) == 0 {
	// 	d.log.Error("Extracted empty kernel ID from ZMQ %v message: %v", msg.Type, msg)
	// 	debug.PrintStack()
	// }

	header, err = d.headerFromFrames(msg.Frames[offset:])

	return kernelId, header, err
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *clusterGatewayImpl) kernelIdAndTypeFromMsg(msg *zmq4.Msg) (id string, messageType string, sessId bool, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)
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
func (d *clusterGatewayImpl) kernelAndTypeFromMsg(msg *zmq4.Msg) (kernel client.DistributedKernelClient, messageType string, err error) {
	var (
		kernelId string
	)

	kernelId, messageType, _, err = d.kernelIdAndTypeFromMsg(msg)
	if err != nil {
		return nil, messageType, err
	}

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
		d.log.Error("Could not find kernel with ID %s", kernelId)
		return nil, messageType, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, messageType, ErrKernelNotReady
	}

	return kernel, messageType, nil
}

// func (d *clusterGatewayImpl) kernelFromMsg(msg *zmq4.Msg) (client.DistributedKernelClient, error) {
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

func (d *clusterGatewayImpl) forwardRequest(kernel client.DistributedKernelClient, typ jupyter.MessageType, msg *zmq4.Msg) (err error) {
	var messageType string
	if kernel == nil {
		kernel, messageType, err = d.kernelAndTypeFromMsg(msg)
	} else {
		_, messageType, err = d.kernelAndTypeFromMsg(msg)
	}

	if err != nil {
		d.log.Error("Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.", typ, err, msg)
		return err
	}

	d.log.Debug("Forwarding %v message of type %s to replicas of kernel %s.", typ, messageType, kernel.ID())
	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelResponseForwarder, func() {})
}

func (d *clusterGatewayImpl) kernelResponseForwarder(from core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}

	if typ == jupyter.ShellMessage {
		kernelId, header, err := d.headerFromMsg(msg)

		if err != nil {
			d.log.Error("Failed to extract header from %v message.", typ)
			return socket.Send(*msg)
		}

		if header.MsgType == ShellExecuteReply {
			kernel, ok := d.kernels.Load(kernelId)
			if !ok {
				d.log.Error("Could not find kernel associated with Session %s specified in ShellExecuteReply", header.Session)
			} else {
				d.processExecutionReply(kernel)
			}
		}
	}

	return socket.Send(*msg)
}

func (d *clusterGatewayImpl) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *clusterGatewayImpl) statusErrorf(status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *clusterGatewayImpl) cleanUp() {
	// Clear nothing for now:
	// Hosts and kernels may contact other gateways to restore status.
	close(d.cleaned)
}

// func (d *clusterGatewayImpl) closeReplica(host core.Host, kernel client.DistributedKernelClient, replica *client.KernelClient, replicaId int, reason string) {
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
func (d *clusterGatewayImpl) addReplica(in *gateway.ReplicaInfo, opts domain.AddReplicaWaitOptions, dataDirectory string) (domain.AddReplicaOperation, error) {
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

	err := d.kubeClient.ScaleOutCloneSet(kernelId, addReplicaOp.PodStartedChannel())
	if err != nil {
		d.log.Error("Failed to add replica to kernel %s. Could not scale-up CloneSet.", kernelId)
		d.log.Error("Reason: %v", err)
		return addReplicaOp, err
	}

	d.log.Debug("Waiting for new Pod to be created for kernel %s.", kernelId)

	// Always wait for the scale-out operation to complete and the new Pod to be created.
	var newPodName string

	<-addReplicaOp.PodStartedChannel()
	d.log.Debug("New Pod %s has been created for kernel %s.", newPodName, kernelId)
	addReplicaOp.SetPodName(newPodName)
	d.addReplicaOperationsByNewPodName.Store(newPodName, addReplicaOp)

	d.Lock()

	channel, ok := d.addReplicaNewPodNotifications.Load(newPodName)

	if ok {
		channel <- addReplicaOp
	}

	d.Unlock()

	if opts.WaitRegistered() {
		d.log.Debug("Waiting for new replica %d of kernel %s to register.", addReplicaOp.ReplicaId(), kernelId)
		<-addReplicaOp.ReplicaRegisteredChannel()
		d.log.Trace("New replica %d for kernel %s has registered with the Gateway.", addReplicaOp.ReplicaId(), kernelId)
	}

	var smrWg sync.WaitGroup
	smrWg.Add(1)
	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted when the new replica joins its SMR cluster.
	go func() {
		<-addReplicaOp.ReplicaJoinedSmrChannel()
		d.log.Debug("New replica %d for kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
		kernel.AddOperationCompleted()
		smrWg.Done()
	}()

	if opts.WaitSmrJoined() {
		d.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
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
func (d *clusterGatewayImpl) removeReplica(smrNodeId int32, kernelId string) error {
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

	podStoppedChannel := make(chan struct{}, 1) // Buffered.

	// Next, scale-down the CloneSet, taking care to ensure the correct Pod is deleted.
	err = d.kubeClient.ScaleInCloneSet(kernelId, oldPodName, podStoppedChannel)
	if err != nil {
		d.log.Error("Error while scaling-in CloneSet for kernel %s: %v", kernelId, err)
		return err
	}

	<-podStoppedChannel
	d.log.Debug("Successfully scaled-in CloneSet by deleting Pod %s.", oldPodName)

	// if wait {
	// 	select {
	// 	case <-podStoppedChannel:
	// 		{
	// 			d.log.Debug("Successfully scaled-in CloneSet by deleting Pod %s.", oldPodName)
	// 		}
	// 	}
	// }

	return nil
}

func (d *clusterGatewayImpl) listKernels(ctx context.Context, in *gateway.Void) (*gateway.ListKernelsResponse, error) {
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
