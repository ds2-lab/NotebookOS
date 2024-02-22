package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
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

	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/driver"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

const (
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"
	KubernetesKernelName   = "kernel-%s"
	ConnectionFileFormat   = "connection-%s-*.json" // "*" is a placeholder for random string
	ConfigFileFormat       = "config-%s-*.json"     // "*" is a placeholder for random string

	ErrorHostname = "ERROR" // We return this from certain gRPC calls when there's an error.
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
	ErrHeaderNotFound        = errors.New("message header not found")
	ErrKernelNotFound        = errors.New("kernel not found")
	ErrKernelNotReady        = errors.New("kernel not ready")
	ErrInvalidJupyterMessage = errors.New("invalid jupter message")
	ErrKernelIDRequired      = errors.New("kernel id frame is required for kernel_info_request")
)

type DaemonKubeClientOptions struct {
	LocalDaemonServiceName  string `name:"local-daemon-service-name" description:"Name of the Kubernetes service that manages the local-only networking of local daemons."`
	LocalDaemonServicePort  int    `name:"local-daemon-service-port" description:"Port exposed by the Kubernetes service that manages the local-only  networking of local daemons."`
	GlobalDaemonServiceName string `name:"global-daemon-service-name" description:"Name of the Kubernetes service that manages the global networking of local daemons."`
	GlobalDaemonServicePort int    `name:"global-daemon-service-port" description:"Port exposed by the Kubernetes service that manages the global networking of local daemons."`
	SMRPort                 int    `name:"smr-port" description:"Port used by the state machine replication (SMR) protocol."`
	KubeNamespace           string `name:"kube-namespace" description:"Kubernetes namespace that all of these components reside in."`
	UseStatefulSet          bool   `name:"use-stateful-set" description:"If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet."`
	HDFSNameNodeEndpoint    string `name:"hdfs-namenode-endpoint" description:"Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this."`
}

func (o DaemonKubeClientOptions) String() string {
	return fmt.Sprintf("LocalDaemonServiceName: %s, LocalDaemonServicePort: %d, SMRPort: %d, KubeNamespace: %s, UseStatefulSet: %v, HDFSNameNodeEndpoint: %s", o.LocalDaemonServiceName, o.LocalDaemonServicePort, o.SMRPort, o.KubeNamespace, o.UseStatefulSet, o.HDFSNameNodeEndpoint)
}

type GatewayDaemonConfig func(*GatewayDaemon)

// GatewayDaemon serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bi-directional gRPC calls.
//
// Some useful resources for jupyter protocol:
// https://jupyter-client.readthedocs.io/en/stable/messaging.html
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type GatewayDaemon struct {
	id string

	driver.UnimplementedDistributedNotebookClusterServer
	gateway.UnimplementedClusterGatewayServer
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    core.CoreOptions

	// cluster provisioning related members
	listener net.Listener
	cluster  core.Cluster
	placer   core.Placer

	// kernel members
	transport   string
	ip          string
	kernels     hashmap.HashMap[string, *client.DistributedKernelClient]
	kernelSpecs hashmap.HashMap[string, *gateway.KernelSpec]
	log         logger.Logger

	// lifetime
	closed  int32
	cleaned chan struct{}

	// Makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	mutex           sync.Mutex
	addReplicaMutex sync.Mutex

	// waitGroups hashmap.HashMap[string, *sync.WaitGroup]
	waitGroups hashmap.HashMap[string, *registrationWaitGroups]

	// Responsible for orchestrating and managing migration operations.
	migrationManager MigrationManager

	// We configure a pool of available ports through Kubernetes.
	// This is the pool of ports. We use these ports to create ZMQ sockets for kernels.
	// If a kernel stops, then its ports are returned to the pool for future reuse.
	availablePorts *utils.AvailablePorts

	// The IOPub socket that all Jupyter clients subscribe to.
	// iopub *jupyter.Socket
	smrPort int

	// Mapping from AddReplicaOperation ID to AddReplicaOperation.
	addReplicaOperations *hashmap.CornelkMap[string, AddReplicaOperation]

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from Operation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, AddReplicaOperation]]

	// Mapping from new pod name to AddReplicaOperation.
	addReplicaOperationsByNewPodName *hashmap.CornelkMap[string, AddReplicaOperation]

	// Mapping from NewPodName to chan string.
	// In theory, it's possible to receive a PodCreated notifcation from Kubernetes AFTER the replica within the new Pod
	// has started running and has registered with the Gateway. In this case, we won't be able to retrieve the AddReplicaOperation
	// associated with that replica via the new Pod's name, as that mapping is created when the PodCreated notification is received.
	// In this case, the goroutine handling the replica registration waits on a channel for the associated AddReplicaOperation.
	addReplicaNewPodNotifications *hashmap.CornelkMap[string, chan AddReplicaOperation]

	// Used to wait for an explicit notification that a particular node was successfully removed from its SMR cluster.
	// smrNodeRemovedNotifications *hashmap.CornelkMap[string, chan struct{}]

	// Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this.
	hdfsNameNodeEndpoint string

	// Kubernetes client.
	kubeClient KubeClient
}

func New(opts *jupyter.ConnectionInfo, daemonKubeClientOptions *DaemonKubeClientOptions, configs ...GatewayDaemonConfig) *GatewayDaemon {
	daemon := &GatewayDaemon{
		id:                uuid.New().String(),
		connectionOptions: opts,
		transport:         "tcp",
		ip:                opts.IP,
		availablePorts:    utils.NewAvailablePorts(opts.StartingResourcePort, opts.NumResourcePorts, 2),
		kernels:           hashmap.NewCornelkMap[string, *client.DistributedKernelClient](1000),
		kernelSpecs:       hashmap.NewCornelkMap[string, *gateway.KernelSpec](100),
		// waitGroups:        hashmap.NewCornelkMap[string, *sync.WaitGroup](100),
		waitGroups:                       hashmap.NewCornelkMap[string, *registrationWaitGroups](100),
		cleaned:                          make(chan struct{}),
		smrPort:                          daemonKubeClientOptions.SMRPort,
		addReplicaOperations:             hashmap.NewCornelkMap[string, AddReplicaOperation](64),
		activeAddReplicaOpsPerKernel:     hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, AddReplicaOperation]](64),
		addReplicaOperationsByNewPodName: hashmap.NewCornelkMap[string, AddReplicaOperation](64),
		addReplicaNewPodNotifications:    hashmap.NewCornelkMap[string, chan AddReplicaOperation](64),
		hdfsNameNodeEndpoint:             daemonKubeClientOptions.HDFSNameNodeEndpoint,
		// smrNodeRemovedNotifications:      hashmap.NewCornelkMap[string, chan struct{}](64),
	}
	for _, config := range configs {
		config(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon)
	daemon.cluster = core.NewCluster()
	daemon.placer = core.NewRandomPlacer(daemon.cluster, &daemon.ClusterOptions)

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	daemon.kubeClient = NewKubeClient(daemon, daemonKubeClientOptions)

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

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (d *GatewayDaemon) Listen(transport string, addr string) (net.Listener, error) {
	d.log.Debug("GatewayDaemon is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	d.listener = lis
	return d, nil
}

// net.Listener implementation
func (d *GatewayDaemon) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := d.listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := incoming

	d.log.Debug("GatewayDaemon is accepting a new connection.")

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
	host, err := NewHostScheduler(incoming.RemoteAddr().String(), gConn)
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

	d.log.Info("Incoming host scheduler %v connected", host)
	return conn, nil
}

// Close are compatible with GatewayDaemon.Close().

func (d *GatewayDaemon) Addr() net.Addr {
	return d.listener.Addr()
}

func (d *GatewayDaemon) SetID(ctx context.Context, hostId *gateway.HostId) (*gateway.HostId, error) {
	return nil, ErrNotImplemented
}

// Issue a 'prepare-to-migrate' request to a specific replica of a specific kernel.
// This will prompt the kernel to shutdown its etcd process (but not remove itself from the cluster)
// before writing the contents of its data directory to HDFS.
//
// Returns the path to the data directory in HDFS.
func (d *GatewayDaemon) issuePrepareMigrateRequest(kernelId string, nodeId int32) string {
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
func (d *GatewayDaemon) issueUpdateReplicaRequest(kernelId string, nodeId int32, newAddress string) {
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
func (d *GatewayDaemon) issueAddNodeRequest(kernelId string, nodeId int32, address string) {
	d.log.Info("Issuing 'add-replica' request to kernel %s for replica %d.", kernelId, nodeId)

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

	d.log.Debug("Issuing AddReplica RPC for new replica %d of kernel %s.", nodeId, kernelId)
	replicaInfo := &gateway.ReplicaInfoWithAddr{
		Id:       nodeId,
		KernelId: kernelId,
		Hostname: fmt.Sprintf("%s:%d", address, d.smrPort),
	}

	// Issue the 'add-replica' request. We panic if there was an error.
	if _, err := host.AddReplica(context.TODO(), replicaInfo); err != nil {
		panic(fmt.Sprintf("Failed to add replica %d of kernel %s to SMR cluster.", nodeId, kernelId))
	}

	d.log.Debug("Sucessfully notified existing replicas of kernel %s that new replica %d has been created.", kernelId, nodeId)
	time.Sleep(time.Second * 5)
}

func (d *GatewayDaemon) SmrReady(ctx context.Context, smrReadyNotification *gateway.SmrReadyNotification) (*gateway.Void, error) {
	kernelId := smrReadyNotification.KernelId

	// First, check if we have an active addReplica operation for this replica. If we don't, then we'll just ignore the notification.
	_, ok := d.getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId, smrReadyNotification.ReplicaId)
	if !ok {
		return gateway.VOID, nil
	}

	d.log.Debug("Received SMR-READY notification for replica %d of kernel %s.", smrReadyNotification.ReplicaId, kernelId)
	return gateway.VOID, nil
}

// func (d *GatewayDaemon) SmrNodeRemoved(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
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

func (d *GatewayDaemon) SmrNodeAdded(ctx context.Context, replicaInfo *gateway.ReplicaInfo) (*gateway.Void, error) {
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

// StartKernel launches a new kernel.
func (d *GatewayDaemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Debug("GatewayDaemon has been instructed to StartKernel.")

	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Debug("Did not find existing KernelClient with KernelID=\"%s\". Creating new DistributedKernelClient now.", in.Id)

		listenPorts, err := d.availablePorts.RequestPorts()
		if err != nil {
			panic(err)
		}

		// Initialize kernel with new context.
		kernel = client.NewDistributedKernel(context.Background(), in, d.ClusterOptions.NumReplicas, d.connectionOptions, listenPorts[0], listenPorts[1])
		d.log.Debug("Initializing Shell Forwarder for new DistributedKernelClient \"%s\" now.", in.Id)
		_, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			kernel.Close()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		d.log.Debug("Initializing IO Forwarder for new DistributedKernelClient \"%s\" now.", in.Id)

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

	// var created sync.WaitGroup
	// created.Add(d.ClusterOptions.NumReplicas)

	d.kernels.Store(in.Id, kernel)
	d.kernelSpecs.Store(in.Id, in)
	d.waitGroups.Store(in.Id, created)

	// TODO(Ben):
	// This is likely where we'd create the new Deployment for the particular Session, I guess?
	// (In the Kubernetes version.)
	_, err := d.kubeClient.DeployDistributedKernels(ctx, in)
	if err != nil {
		d.log.Error("Error encountered while attempting to create the StatefulSet for Session %s", in.Id)
		d.log.Error("%v", err)
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// TODO: Handle replica creation error and ensure enough number of replicas are created.

	// Wait for all replicas to be created.
	created.Wait()

	if kernel.Size() == 0 {
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// TODO: Handle kernel response.
	for _, sess := range kernel.Sessions() {
		d.log.Debug("Storing kernel %v under session ID %s.", kernel, sess)
		d.kernels.Store(sess, kernel)
	}

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
func (d *GatewayDaemon) handleAddedReplicaRegistration(ctx context.Context, in *gateway.KernelRegistrationNotification, kernel *client.DistributedKernelClient, waitGroup *registrationWaitGroups) (*gateway.KernelRegistrationNotificationResponse, error) {
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
		channel := make(chan AddReplicaOperation, 1)
		d.addReplicaNewPodNotifications.Store(in.PodName, channel)

		d.mutex.Unlock()
		d.log.Debug("Waiting to receive AddReplicaNotification on NewPodNotification channel. NewPodName: %s.", in.PodName)
		// Just need to provide a mechanism to wait until we receive the pod-created notification, and get the migration operation that way.
		addReplicaOp = <-channel
		d.mutex.Lock()

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
	replica := client.NewKernelClient(context.Background(), replicaSpec, in.ConnectionInfo.ConnectionInfo(), false, -1, -1, in.PodName, nil, nil)
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

	d.mutex.Unlock()

	addReplicaOp.SetReplicaRegistered() // This just sets a flag to true in the migration operation object.

	d.issueUpdateReplicaRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	// Issue the AddNode request now, so that the node can join when it starts up.
	// d.issueAddNodeRequest(in.KernelId, replicaSpec.ReplicaId, in.KernelIp)

	d.log.Debug("Done handling registration of added replica %d of kernel %s.", replicaSpec.ReplicaId, in.KernelId)

	return response, nil
}

func (d *GatewayDaemon) NotifyKernelRegistered(ctx context.Context, in *gateway.KernelRegistrationNotification) (*gateway.KernelRegistrationNotificationResponse, error) {
	d.log.Info("Received kernel registration notification.")

	connectionInfo := in.ConnectionInfo
	sessionId := in.SessionId
	kernelId := in.KernelId
	hostId := in.HostId
	kernelIp := in.KernelIp
	kernelPodName := in.PodName

	d.log.Info("Connection info: %v", connectionInfo)
	d.log.Info("Session ID: %v", sessionId)
	d.log.Info("Kernel ID: %v", kernelId)
	d.log.Info("Kernel IP: %v", kernelIp)
	d.log.Info("Pod name: %v", kernelPodName)
	d.log.Info("Host ID: %v", hostId)

	d.mutex.Lock()

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
		result, err := d.handleAddedReplicaRegistration(ctx, in, kernel, waitGroup)
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
	replica := client.NewKernelClient(context.Background(), replicaSpec, connectionInfo.ConnectionInfo(), false, -1, -1, kernelPodName, nil, nil)
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
	d.mutex.Unlock()

	// Wait until all replicas have registered before continuing, as we need all of their IDs.
	waitGroup.SetReplica(replicaId, kernelIp)
	waitGroup.Register()

	d.log.Debug("Done registering KernelClient for kernel %s, replica %d on host %s.", kernelId, replicaId, hostId)
	d.log.Debug("WaitGroup for Kernel \"%s\": %s", kernelId, waitGroup.String())

	waitGroup.WaitRegistered()

	response := &gateway.KernelRegistrationNotificationResponse{
		Id:            replicaId,
		Replicas:      waitGroup.GetReplicas(),
		PersistentId:  nil,
		DataDirectory: nil,
		SmrPort:       int32(d.smrPort), // The kernel should already have this info, but we'll send it anyway.
	}

	d.log.Debug("Sending response to associated LocalDaemon for kernel %s, replica %d: %v", kernelId, replicaId, response)

	waitGroup.Notify()
	return response, nil
}

func (d *GatewayDaemon) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	d.log.Debug("StartKernelReplica has been instructed to StartKernel. This is actually not supported/implemented.")

	return nil, ErrNotSupported
}

// KernelStatus returns the status of a kernel.
func (d *GatewayDaemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.Status(), nil)
}

// KillKernel kills a kernel.
func (d *GatewayDaemon) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	return d.StopKernel(ctx, in)
}

// StopKernel stops a kernel.
func (d *GatewayDaemon) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	restart := false
	if in.Restart != nil {
		restart = *in.Restart
	}
	d.log.Info("Stopping %v, will restart %v", kernel, restart)
	ret = gateway.VOID
	go func() {
		err = d.errorf(kernel.Shutdown(d.placer.Reclaim, restart))
		if err != nil {
			d.log.Warn("Failed to close kernel: %s", err.Error())
			return
		}

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

			// Return the ports allocated to the kernel.
			listenPorts := []int{kernel.ShellListenPort(), kernel.IOPubListenPort()}
			d.availablePorts.ReturnPorts(listenPorts)

			d.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}
	}()
	return
}

// WaitKernel waits for a kernel to exit.
func (d *GatewayDaemon) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.WaitClosed(), nil)
}

// ClusterGateway implementation.
func (d *GatewayDaemon) ID(ctx context.Context, in *gateway.Void) (*gateway.ProvisionerId, error) {
	return &gateway.ProvisionerId{Id: d.id}, nil
}

func (d *GatewayDaemon) RemoveHost(ctx context.Context, in *gateway.HostId) (*gateway.Void, error) {
	d.cluster.GetHostManager().Delete(in.Id)
	return gateway.VOID, nil
}

func (d *GatewayDaemon) migrate_removeFirst(ctx context.Context, in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
	// We pass 'false' for `wait` here, as we don't really need to wait for the CloneSet to scale-down.
	// As long as the replica is stopped, we can continue.
	dataDirectory := d.issuePrepareMigrateRequest(in.KernelId, in.ReplicaId)

	err := d.removeReplica(in.ReplicaId, in.KernelId, false)
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

// func (d *GatewayDaemon) migrate_removeLast(ctx context.Context, in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
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

func (d *GatewayDaemon) MigrateKernelReplica(ctx context.Context, in *gateway.ReplicaInfo) (*gateway.MigrateKernelResponse, error) {
	// client, ok := d.kernels.Load(in.KernelId)
	// if !ok {
	// 	d.log.Error("Failed to find client of kernel %s.", in.KernelId)
	// 	return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, ErrKernelNotFound
	// }

	// if client.Size() >= 4 {
	// 	d.log.Debug("We already have 4 replicas. Not adding another.")
	// 	return &gateway.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname}, nil
	// }

	d.log.Debug("Migrating replica %d of kernel %s now.", in.ReplicaId, in.KernelId)

	return d.migrate_removeFirst(ctx, in)
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

func (d *GatewayDaemon) Start() error {
	d.log.Info("Starting router...")

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Clean up
	d.cleanUp()

	return err
}

func (d *GatewayDaemon) Close() error {
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
func (d *GatewayDaemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.ControlMessage, msg)
}

func (d *GatewayDaemon) kernelShellHandler(kernel core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	return d.ShellHandler(kernel, msg)
}

func (d *GatewayDaemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	kernelId, header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok && header.MsgType == ShellKernelInfoRequest {
		// Register kernel on ShellKernelInfoRequest
		if kernelId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(kernelId)
		if !ok {
			return ErrKernelNotFound
		}

		kernel.BindSession(header.Session)
		d.kernels.Store(header.Session, kernel)
	}
	if kernel == nil {
		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	if err := d.forwardRequest(kernel, jupyter.ShellMessage, msg); err != nil {
		return err
	}

	return nil
}

func (d *GatewayDaemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *GatewayDaemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *GatewayDaemon) idFromMsg(msg *zmq4.Msg) (id string, sessId bool, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)
	if kernelId != "" {
		return kernelId, false, nil
	}

	header, err := d.headerFromFrames(msg.Frames[offset:])
	if err != nil {
		return "", false, err
	}

	return header.Session, true, nil
}

func (d *GatewayDaemon) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
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
func (d *GatewayDaemon) getAddReplicaOperationByKernelIdAndNewReplicaId(kernelId string, smrNodeId int32) (AddReplicaOperation, bool) {
	d.addReplicaMutex.Lock()
	defer d.addReplicaMutex.Unlock()

	activeOps, ok := d.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		return nil, false
	}

	var op AddReplicaOperation
	for el := activeOps.Front(); el != nil; el = el.Next() {
		op = el.Value

		if op.ReplicaId() == smrNodeId {
			return op, true
		}
	}

	return nil, false
}

func (d *GatewayDaemon) headerFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)

	header, err = d.headerFromFrames(msg.Frames[offset:])

	return kernelId, header, err
}

func (d *GatewayDaemon) kernelFromMsg(msg *zmq4.Msg) (*client.DistributedKernelClient, error) {
	id, _, err := d.idFromMsg(msg)
	if err != nil {
		return nil, err
	}

	kernel, ok := d.kernels.Load(id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, ErrKernelNotReady
	}

	return kernel, nil
}

func (d *GatewayDaemon) forwardRequest(kernel *client.DistributedKernelClient, typ jupyter.MessageType, msg *zmq4.Msg) (err error) {
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	return kernel.RequestWithHandler(context.Background(), "Forwarding", typ, msg, d.kernelResponseForwarder, func() {})
}

func (d *GatewayDaemon) kernelResponseForwarder(from core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}
	// d.log.Debug("Forwarding %v response to %v [addr=%v].", socket.Type.String(), from, socket.Addr().String())
	return socket.Send(*msg)
}

func (d *GatewayDaemon) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *GatewayDaemon) statusErrorf(status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *GatewayDaemon) cleanUp() {
	// Clear nothing for now:
	// Hosts and kernels may contact other gateways to restore status.
	close(d.cleaned)
}

func (d *GatewayDaemon) closeReplica(host core.Host, kernel *client.DistributedKernelClient, replica *client.KernelClient, replicaId int, reason string) {
	defer replica.Close()

	if err := d.placer.Reclaim(host, kernel, false); err != nil {
		d.log.Warn("Failed to close kernel(%s:%d) after %s, failure: %v", kernel.ID(), replicaId, reason, err)
	}
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
func (d *GatewayDaemon) addReplica(in *gateway.ReplicaInfo, opts AddReplicaWaitOptions, dataDirectory string) (AddReplicaOperation, error) {
	var kernelId string = in.KernelId
	var persistentId string = in.PersistentId

	kernel, ok := d.kernels.Load(kernelId)
	if !ok {
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
		ops = orderedmap.NewOrderedMap[string, AddReplicaOperation]()
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
	select {
	case newPodName = <-addReplicaOp.PodStartedChannel():
		{
			d.log.Debug("New Pod %s has been created for kernel %s.", newPodName, kernelId)
			addReplicaOp.SetPodName(newPodName)
			d.addReplicaOperationsByNewPodName.Store(newPodName, addReplicaOp)

			d.mutex.Lock()

			channel, ok := d.addReplicaNewPodNotifications.Load(newPodName)

			if ok {
				channel <- addReplicaOp
			}

			d.mutex.Unlock()
			break
		}
	}

	// var regWg sync.WaitGroup
	// regWg.Add(1)
	// go func() {
	// 	select {
	// 	case _ = <-addReplicaOp.ReplicaRegisteredChannel():
	// 		{
	// 			d.log.Debug("New replica %d for kernel %s has registered with the Gateway.", addReplicaOp.ReplicaId(), kernelId)
	// 			regWg.Done()
	// 			break
	// 		}
	// 	}
	// }()

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
func (d *GatewayDaemon) removeReplica(smrNodeId int32, kernelId string, wait bool) error {
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

	// d.log.Debug("Waiting to receive notification that replica %d of kernel %s has been removed from its SMR cluster.", smrNodeId, kernelId)
	// select {
	// case _ = <-nodeRemovedNotificationChannel:
	// 	{
	// 		d.log.Debug("Successfully removed replica %d of kernel %s.", smrNodeId, kernelId)
	// 		d.smrNodeRemovedNotifications.Delete(channelMapKey)
	// 	}
	// }

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

	if wait {
		select {
		case <-podStoppedChannel:
			{
				d.log.Debug("Successfully scaled-in CloneSet by deleting Pod %s.", oldPodName)
			}
		}
	}

	return nil
}

// Driver gRPC.
func (d *GatewayDaemon) ListKernels(ctx context.Context, in *driver.Void) (*driver.ListKernelsResponse, error) {
	resp := &driver.ListKernelsResponse{
		Kernels: make([]*driver.JupyterKernel, 0, d.kernels.Len()),
	}

	d.kernels.Range(func(id string, kernel *client.DistributedKernelClient) bool {
		respKernel := &driver.JupyterKernel{
			KernelId:            kernel.ID(),
			NumReplicas:         int32(kernel.Size()),
			Status:              kernel.Status().String(),
			AggregateBusyStatus: kernel.Status().String(),
		}
		resp.Kernels = append(resp.Kernels, respKernel)
		return true
	})

	return resp, nil
}
