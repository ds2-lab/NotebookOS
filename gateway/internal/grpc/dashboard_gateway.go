package grpc

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"sync/atomic"
)

type DashboardGatewayHandler interface {
	ID() string
	GetSerializedClusterStatistics() []byte
	SpoofNotifications() (*proto.Void, error)
	Close() error
	ListKernels() ([]*proto.DistributedJupyterKernel, error)
	MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error)
	PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
	FailNextExecution(kernelId string) error
	GetVirtualDockerNodes() ([]*proto.VirtualDockerNode, error)
	GetNumNodes() int
	SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error)
	AddClusterNodes(ctx context.Context, n int32) (*proto.AddClusterNodesResponse, error)
	RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error)
	RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error)
	ClusterAge() int64
	ModifyClusterNodes(ctx context.Context, in *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error)
	QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error)
	ForceLocalDaemonToReconnect(hostId string, delay bool) error

	// ClearClusterStatistics clears the current metrics.ClusterStatistics struct.
	//
	// ClearClusterStatistics returns the serialized metrics.ClusterStatistics struct before it was cleared.
	ClearClusterStatistics() []byte
	IsKernelActivelyTraining(kernelId string) (bool, error)

	DeploymentMode() types.DeploymentMode
	PolicyKey() scheduling.PolicyKey
	NumReplicasPerKernel() int
}

type ClusterDashboardClientConsumer interface {
	SetClusterDashboardClient(client proto.ClusterDashboardClient)
}

// DashboardGateway fulfills a different gRPC interface required by the Admin/Workload Dashboard.
//
// DashboardGateway establishes a two-way gRPC connection with the Admin/Workload Dashboard.
type DashboardGateway struct {
	proto.UnimplementedDistributedClusterServer
	clusterDashboardClient proto.ClusterDashboardClient
	consumer               ClusterDashboardClientConsumer
	handler                DashboardGatewayHandler
	listener               net.Listener
	log                    logger.Logger
	closed                 int32
}

func NewDistributedGateway(handler DashboardGatewayHandler, consumer ClusterDashboardClientConsumer) *DashboardGateway {
	dg := &DashboardGateway{
		handler:  handler,
		consumer: consumer,
	}

	config.InitLogger(&dg.log, dg)

	return dg
}

// HandlePanic is used to notify the Dashboard that a panic has occurred.
// We'll still panic, but we send a notification first so that the user can be notified clearly that something bad has happened.
//
// Parameter:
// - identity (string): The identity of the entity/goroutine that panicked.
func (dg *DashboardGateway) HandlePanic(identity string, fatalErr interface{}) {
	dg.log.Error("Entity %s has panicked with error: %v", identity, fatalErr)

	if dg.clusterDashboardClient == nil {
		dg.log.Warn("We do not have an active connection with the cluster dashboard.")
		panic(fatalErr)
	}

	_, err := dg.clusterDashboardClient.SendNotification(context.TODO(), &proto.Notification{
		Id:               uuid.NewString(),
		Title:            fmt.Sprintf("%s panicked.", identity),
		Message:          fmt.Sprintf("%v", fatalErr),
		NotificationType: int32(messaging.ErrorNotification),
		Panicked:         true,
	})

	if err != nil {
		dg.log.Error("Failed to inform cluster Dashboard that a fatal error occurred because: %v", err)
	}
}

// SpoofNotifications is used to test notifications.
func (dg *DashboardGateway) SpoofNotifications(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	return dg.handler.SpoofNotifications()
}

// InducePanic is used for debugging/testing. Causes a Panic.
func (dg *DashboardGateway) InducePanic(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	panic("Inducing a panic.")
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (dg *DashboardGateway) Listen(transport string, addr string) (net.Listener, error) {
	dg.log.Debug("clusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	dg.listener = lis
	return dg, nil
}

// Accept waits for and returns the next connection to the listener.
func (dg *DashboardGateway) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := dg.listener.Accept()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	conn := incoming

	dg.log.Debug("Accepting gRPC connection from cluster Dashboard. LocalAddr: %v. RemoteAddr: %v.", incoming.LocalAddr(), incoming.RemoteAddr())

	// Initialize yamux session for bidirectional gRPC calls
	// At gateway side, we first wait for an incoming replacement connection, then create a reverse provisioner connection to the cluster Dashboard.
	cliSession, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		dg.log.Error("Failed to create yamux client session with cluster Dashboard: %v", err)
		return incoming, nil
	}

	dg.log.Debug("Created yamux client for cluster Dashboard. Creating new session to replace the incoming connection now...")
	dg.log.Debug("cliSession.LocalAddr(): %v, cliSession.RemoteAddr(): %v, cliSession.Addr(): %v", cliSession.LocalAddr(), cliSession.RemoteAddr(), cliSession.Addr())

	// Create a new session to replace the incoming connection.
	conn, err = cliSession.Accept()
	if err != nil {
		dg.log.Error("Failed to wait for the replacement of cluster Dashboard connection: %v", err)
		return incoming, nil
	}

	dg.log.Debug("Accepted new session for cluster Dashboard. Dialing to create reverse connection with dummy dialer now...")

	// Dial to create a reversion connection with dummy dialer.
	gConn, err := grpc.Dial(":0", // grpc.NewClient("passthrough://:0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := cliSession.Open()
			if err != nil {
				dg.log.Error("Failed to open CLI session during dial: %v", err)
				return nil, status.Error(codes.Internal, err.Error())
			} else {
				dg.log.Debug("Opened cliSession. conn.LocalAddr(): %v, conn.RemoteAddr(): %v", conn.LocalAddr(), conn.RemoteAddr())
			}

			return conn, nil
		}))
	if err != nil {
		dg.log.Error("Failed to open reverse Distributed cluster connection with cluster Dashboard: %v", err)
		return conn, nil
	}

	dg.log.Debug("Successfully dialed to create reverse connection with cluster Dashboard. Target: %v", gConn.Target())

	// Create a cluster Dashboard client and register it.
	dg.clusterDashboardClient = proto.NewClusterDashboardClient(gConn)

	if dg.consumer != nil {
		dg.consumer.SetClusterDashboardClient(dg.clusterDashboardClient)
	}

	return conn, nil
}

// Close closes the listener.
// Any blocked 'Accept' operations will be unblocked and return errors.
func (dg *DashboardGateway) Close() error {
	if !atomic.CompareAndSwapInt32(&dg.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the listener
	if dg.listener != nil {
		err := dg.listener.Close()

		if err != nil {
			dg.log.Error("Error while closing DistributedCluster listener: %v", err)
		}
	}

	return nil
}

// Addr returns the listener's network address.
func (dg *DashboardGateway) Addr() net.Addr {
	return dg.listener.Addr()
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (dg *DashboardGateway) GetClusterActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetClusterVirtualGpuInfo returns the current vGPU (or "deflated GPU") resource metrics on the node.
func (dg *DashboardGateway) GetClusterVirtualGpuInfo(_ context.Context, _ *proto.Void) (*proto.ClusterVirtualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// SetTotalVirtualGPUs adjusts the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (dg *DashboardGateway) SetTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	return nil, ErrNotImplemented
}

// IsKernelActivelyTraining is used to query whether a particular kernel is actively training.
func (dg *DashboardGateway) IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	isTraining, err := dg.handler.IsKernelActivelyTraining(in.Id)
	if err != nil {
		return nil, errorf(err)
	}

	return &proto.IsKernelTrainingReply{IsTraining: isTraining, KernelId: in.Id}, nil
}

func (dg *DashboardGateway) ListKernels(_ context.Context, _ *proto.Void) (*proto.ListKernelsResponse, error) {
	kernels, err := dg.handler.ListKernels()
	if err != nil {
		return nil, errorf(err)
	}

	return &proto.ListKernelsResponse{Kernels: kernels}, nil
}

func (dg *DashboardGateway) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	resp, err := dg.handler.MigrateKernelReplica(ctx, in)
	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

func (dg *DashboardGateway) Ping(_ context.Context, _ *proto.Void) (*proto.Pong, error) {
	return &proto.Pong{Id: dg.handler.ID()}, nil
}

// RegisterDashboard is called by the cluster Dashboard backend server to both verify that a connection has been
// established and to obtain any important configuration information, such as the deployment mode (i.e., Docker or
// Kubernetes), from the cluster Gateway.
func (dg *DashboardGateway) RegisterDashboard(_ context.Context, _ *proto.Void) (*proto.DashboardRegistrationResponse, error) {
	resp := &proto.DashboardRegistrationResponse{
		DeploymentMode:   string(dg.handler.DeploymentMode()),
		SchedulingPolicy: string(dg.handler.PolicyKey()),
		NumReplicas:      int32(dg.handler.NumReplicasPerKernel()),
	}

	return resp, nil
}

func (dg *DashboardGateway) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return dg.handler.PingKernel(ctx, in)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (dg *DashboardGateway) FailNextExecution(_ context.Context, in *proto.KernelId) (*proto.Void, error) {
	err := dg.handler.FailNextExecution(in.Id)
	if err != nil {
		return nil, errorf(err)
	}

	return proto.VOID, nil
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
// If the cluster is not running in Docker mode, then this will return an error.
func (dg *DashboardGateway) GetVirtualDockerNodes(_ context.Context, _ *proto.Void) (*proto.GetVirtualDockerNodesResponse, error) {
	nodes, err := dg.handler.GetVirtualDockerNodes()
	if err != nil {
		return nil, errorf(err)
	}

	resp := &proto.GetVirtualDockerNodesResponse{
		Nodes: nodes,
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
// If the cluster is not running in Docker mode, then this will return an error.
func (dg *DashboardGateway) GetDockerSwarmNodes(_ context.Context, _ *proto.Void) (*proto.GetDockerSwarmNodesResponse, error) {
	return nil, ErrNotImplemented
}

func (dg *DashboardGateway) GetNumNodes(_ context.Context, _ *proto.Void) (*proto.NumNodesResponse, error) {
	numNodes := dg.handler.GetNumNodes()

	return &proto.NumNodesResponse{NumNodes: int32(numNodes)}, nil
}

func (dg *DashboardGateway) SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error) {
	resp, err := dg.handler.SetNumClusterNodes(ctx, in)

	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

func (dg *DashboardGateway) AddClusterNodes(ctx context.Context, in *proto.AddClusterNodesRequest) (*proto.AddClusterNodesResponse, error) {
	resp, err := dg.handler.AddClusterNodes(ctx, in.NumNodes)

	if err != nil {
		return nil, errorf(err)
	}

	resp.RequestId = in.RequestId
	return resp, nil
}

func (dg *DashboardGateway) RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error) {
	resp, err := dg.handler.RemoveSpecificClusterNodes(ctx, in)

	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

func (dg *DashboardGateway) RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error) {
	resp, err := dg.handler.RemoveClusterNodes(ctx, in)

	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

func (dg *DashboardGateway) ClusterAge(_ context.Context, _ *proto.Void) (*proto.ClusterAgeResponse, error) {
	age := dg.handler.ClusterAge()

	return &proto.ClusterAgeResponse{Age: age}, nil
}

func (dg *DashboardGateway) ModifyClusterNodes(ctx context.Context, in *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error) {
	resp, err := dg.handler.ModifyClusterNodes(ctx, in)

	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

// QueryMessage is used to query whether a given ZMQ message has been seen by any of the cluster components
// and what the status of that message is (i.e., sent, response received, etc.)
func (dg *DashboardGateway) QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error) {
	resp, err := dg.handler.QueryMessage(ctx, in)

	if err != nil {
		return nil, errorf(err)
	}

	return resp, err
}

// ForceLocalDaemonToReconnect is used to tell a Local Daemon to reconnect to the cluster Gateway.
// This is mostly used for testing/debugging the reconnection process.
func (dg *DashboardGateway) ForceLocalDaemonToReconnect(_ context.Context, in *proto.ForceLocalDaemonToReconnectRequest) (*proto.Void, error) {
	err := dg.handler.ForceLocalDaemonToReconnect(in.LocalDaemonId, in.Delay)

	if err != nil {
		return nil, errorf(err)
	}

	return proto.VOID, err
}

// ClusterStatistics is used to request a serialized ClusterStatistics struct.
func (dg *DashboardGateway) ClusterStatistics(_ context.Context, req *proto.ClusterStatisticsRequest) (*proto.ClusterStatisticsResponse, error) {
	serializedStatistics := dg.handler.GetSerializedClusterStatistics()
	if serializedStatistics == nil {
		return nil, status.Error(codes.Internal, "failed to generate or retrieve cluster Statistics")
	}

	resp := &proto.ClusterStatisticsResponse{
		RequestId:                   req.RequestId, // match ID in response to ID in request
		SerializedClusterStatistics: serializedStatistics,
	}

	return resp, nil
}

// ClearClusterStatistics clears the current ClusterStatistics struct.
//
// ClearClusterStatistics returns the serialized ClusterStatistics struct before it was cleared.
// ClusterStatistics is used to request a serialized ClusterStatistics struct.
func (dg *DashboardGateway) ClearClusterStatistics(_ context.Context, _ *proto.Void) (*proto.ClusterStatisticsResponse, error) {
	serializedStatistics := dg.handler.ClearClusterStatistics()

	if serializedStatistics == nil {
		return nil, status.Error(codes.Internal, "failed to generate, retrieve, or clear cluster Statistics")
	}

	resp := &proto.ClusterStatisticsResponse{
		RequestId:                   uuid.NewString(), // match ID in response to ID in request
		SerializedClusterStatistics: serializedStatistics,
	}

	return resp, nil
}

func errorf(err error) error {
	if err == nil {
		return nil
	}

	_, ok := status.FromError(err)
	if ok {
		return err
	}

	return status.Errorf(codes.Internal, err.Error())
}
