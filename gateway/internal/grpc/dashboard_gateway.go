package grpc

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
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
	HandlePanic(identity string, fatalErr interface{})
	SpoofNotifications(ctx context.Context, in *proto.Void) (*proto.Void, error)
	InducePanic(_ context.Context, _ *proto.Void) (*proto.Void, error)
	Listen(transport string, addr string) (net.Listener, error)
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
	GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error)
	GetClusterVirtualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterVirtualGpuInfo, error)
	SetTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error)
	ListKernels(ctx context.Context) (*proto.ListKernelsResponse, error)
	MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error)
	Ping(_ context.Context, _ *proto.Void) (*proto.Pong, error)
	RegisterDashboard(ctx context.Context, in *proto.Void) (*proto.DashboardRegistrationResponse, error)
	PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
	FailNextExecution(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	GetVirtualDockerNodes(ctx context.Context, in *proto.Void) (*proto.GetVirtualDockerNodesResponse, error)
	GetDockerSwarmNodes(ctx context.Context, in *proto.Void) (*proto.GetDockerSwarmNodesResponse, error)
	GetNumNodes(ctx context.Context, in *proto.Void) (*proto.NumNodesResponse, error)
	SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error)
	AddClusterNodes(ctx context.Context, in *proto.AddClusterNodesRequest) (*proto.AddClusterNodesResponse, error)
	RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error)
	RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error)
	ClusterAge(ctx context.Context, in *proto.Void) (*proto.ClusterAgeResponse, error)
	ModifyClusterNodes(ctx context.Context, in *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error)
	QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error)
	ForceLocalDaemonToReconnect(ctx context.Context, in *proto.ForceLocalDaemonToReconnectRequest) (*proto.Void, error)
	ClusterStatistics(_ context.Context, req *proto.ClusterStatisticsRequest) (*proto.ClusterStatisticsResponse, error)
	ClearClusterStatistics(_ context.Context) ([]byte, error)
	SetClusterDashboardClient(clusterDashboardClient proto.ClusterDashboardClient)
	IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error)
}

// DashboardGateway fulfills a different gRPC interface required by the Admin/Workload Dashboard.
//
// DashboardGateway establishes a two-way gRPC connection with the Admin/Workload Dashboard.
type DashboardGateway struct {
	proto.UnimplementedDistributedClusterServer
	clusterDashboardClient proto.ClusterDashboardClient
	handler                DashboardGatewayHandler
	listener               net.Listener
	log                    logger.Logger
	closed                 int32
}

func NewDistributedCluster(handler DashboardGatewayHandler) *DashboardGateway {
	dg := &DashboardGateway{
		handler: handler,
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
func (dg *DashboardGateway) SpoofNotifications(ctx context.Context, in *proto.Void) (*proto.Void, error) {
	return dg.handler.SpoofNotifications(ctx, in)
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
	dg.handler.SetClusterDashboardClient(dg.clusterDashboardClient)
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
func (dg *DashboardGateway) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	return dg.handler.GetClusterActualGpuInfo(ctx, in)
}

// GetClusterVirtualGpuInfo returns the current vGPU (or "deflated GPU") resource metrics on the node.
func (dg *DashboardGateway) GetClusterVirtualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterVirtualGpuInfo, error) {
	return dg.handler.getClusterVirtualGpuInfo(ctx, in)
}

// SetTotalVirtualGPUs adjusts the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (dg *DashboardGateway) SetTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	return dg.handler.setTotalVirtualGPUs(ctx, in)
}

func (dg *DashboardGateway) IsKernelActivelyTraining(ctx context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	return dg.handler.IsKernelActivelyTraining(ctx, in)
}

func (dg *DashboardGateway) ListKernels(ctx context.Context) (*proto.ListKernelsResponse, error) {
	return dg.handler.ListKernels(ctx)
}

func (dg *DashboardGateway) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	return dg.handler.MigrateKernelReplica(ctx, in)
}

func (dg *DashboardGateway) Ping(_ context.Context, _ *proto.Void) (*proto.Pong, error) {
	return &proto.Pong{Id: dg.handler.ID()}, nil
}

// RegisterDashboard is called by the cluster Dashboard backend server to both verify that a connection has been
// established and to obtain any important configuration information, such as the deployment mode (i.e., Docker or
// Kubernetes), from the cluster Gateway.
func (dg *DashboardGateway) RegisterDashboard(ctx context.Context, in *proto.Void) (*proto.DashboardRegistrationResponse, error) {
	return dg.handler.RegisterDashboard(ctx, in)
}

func (dg *DashboardGateway) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return dg.handler.PingKernel(ctx, in)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (dg *DashboardGateway) FailNextExecution(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return dg.handler.FailNextExecution(ctx, in)
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
func (dg *DashboardGateway) GetVirtualDockerNodes(ctx context.Context, in *proto.Void) (*proto.GetVirtualDockerNodesResponse, error) {
	return dg.handler.GetVirtualDockerNodes(ctx, in)
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
func (dg *DashboardGateway) GetDockerSwarmNodes(ctx context.Context, in *proto.Void) (*proto.GetDockerSwarmNodesResponse, error) {
	return dg.handler.GetDockerSwarmNodes(ctx, in)
}

func (dg *DashboardGateway) GetNumNodes(ctx context.Context, in *proto.Void) (*proto.NumNodesResponse, error) {
	return dg.handler.GetNumNodes(ctx, in)
}

func (dg *DashboardGateway) SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error) {
	return dg.handler.SetNumClusterNodes(ctx, in)
}

func (dg *DashboardGateway) AddClusterNodes(ctx context.Context, in *proto.AddClusterNodesRequest) (*proto.AddClusterNodesResponse, error) {
	return dg.handler.AddClusterNodes(ctx, in)
}

func (dg *DashboardGateway) RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error) {
	return dg.handler.RemoveSpecificClusterNodes(ctx, in)
}

func (dg *DashboardGateway) RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error) {
	return dg.handler.RemoveClusterNodes(ctx, in)
}

func (dg *DashboardGateway) ClusterAge(ctx context.Context, in *proto.Void) (*proto.ClusterAgeResponse, error) {
	return dg.handler.ClusterAge(ctx, in)
}

func (dg *DashboardGateway) ModifyClusterNodes(ctx context.Context, in *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error) {
	return dg.handler.ModifyClusterNodes(ctx, in)
}

// QueryMessage is used to query whether a given ZMQ message has been seen by any of the cluster components
// and what the status of that message is (i.e., sent, response received, etc.)
func (dg *DashboardGateway) QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error) {
	return dg.handler.QueryMessage(ctx, in)
}

// ForceLocalDaemonToReconnect is used to tell a Local Daemon to reconnect to the cluster Gateway.
// This is mostly used for testing/debugging the reconnection process.
func (dg *DashboardGateway) ForceLocalDaemonToReconnect(ctx context.Context, in *proto.ForceLocalDaemonToReconnectRequest) (*proto.Void, error) {
	return dg.handler.ForceLocalDaemonToReconnect(ctx, in)
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
func (dg *DashboardGateway) ClearClusterStatistics(ctx context.Context, _ *proto.Void) (*proto.ClusterStatisticsResponse, error) {
	serializedStatistics, err := dg.handler.ClearClusterStatistics(ctx)
	if err != nil {
		return nil, errorf(err)
	}

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
