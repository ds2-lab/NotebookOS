package daemon

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"net"
	"sync/atomic"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/hashicorp/yamux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DistributedCluster struct {
	proto.UnimplementedDistributedClusterServer

	gatewayDaemon *ClusterGatewayImpl

	clusterDashboard proto.ClusterDashboardClient

	listener net.Listener
	closed   int32

	log logger.Logger
}

func NewDistributedCluster(gatewayDaemon *ClusterGatewayImpl) *DistributedCluster {
	dc := &DistributedCluster{
		gatewayDaemon: gatewayDaemon,
	}

	config.InitLogger(&dc.log, dc)

	return dc
}

// HandlePanic is used to notify the Dashboard that a panic has occurred.
// We'll still panic, but we send a notification first so that the user can be notified clearly that something bad has happened.
//
// Parameter:
// - identity (string): The identity of the entity/goroutine that panicked.
func (dc *DistributedCluster) HandlePanic(identity string, fatalErr interface{}) {
	dc.log.Error("Entity %s has panicked with error: %v", identity, fatalErr)

	if dc.clusterDashboard == nil {
		dc.log.Warn("We do not have an active connection with the cluster dashboard.")
		panic(fatalErr)
	}

	_, err := dc.clusterDashboard.SendNotification(context.TODO(), &proto.Notification{
		Id:               uuid.NewString(),
		Title:            fmt.Sprintf("%s panicked.", identity),
		Message:          fmt.Sprintf("%v", fatalErr),
		NotificationType: int32(types.ErrorNotification),
		Panicked:         true,
	})

	if err != nil {
		dc.log.Error("Failed to inform Cluster Dashboard that a fatal error occurred because: %v", err)
	}
}

// SpoofNotifications is used to test notifications.
func (dc *DistributedCluster) SpoofNotifications(ctx context.Context, in *proto.Void) (*proto.Void, error) {
	return dc.gatewayDaemon.SpoofNotifications(ctx, in)
}

// InducePanic is used for debugging/testing. Causes a Panic.
func (dc *DistributedCluster) InducePanic(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	panic("Inducing a panic.")
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (dc *DistributedCluster) Listen(transport string, addr string) (net.Listener, error) {
	dc.log.Debug("clusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	dc.listener = lis
	return dc, nil
}

// Accept waits for and returns the next connection to the listener.
func (dc *DistributedCluster) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := dc.listener.Accept()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	conn := incoming

	dc.log.Debug("Accepting gRPC connection from Cluster Dashboard. LocalAddr: %v. RemoteAddr: %v.", incoming.LocalAddr(), incoming.RemoteAddr())

	// Initialize yamux session for bidirectional gRPC calls
	// At gateway side, we first wait for an incoming replacement connection, then create a reverse provisioner connection to the Cluster Dashboard.
	cliSession, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		dc.log.Error("Failed to create yamux client session with Cluster Dashboard: %v", err)
		return incoming, nil
	}

	dc.log.Debug("Created yamux client for Cluster Dashboard. Creating new session to replace the incoming connection now...")
	dc.log.Debug("cliSession.LocalAddr(): %v, cliSession.RemoteAddr(): %v, cliSession.Addr(): %v", cliSession.LocalAddr(), cliSession.RemoteAddr(), cliSession.Addr())

	// Create a new session to replace the incoming connection.
	conn, err = cliSession.Accept()
	if err != nil {
		dc.log.Error("Failed to wait for the replacement of Cluster Dashboard connection: %v", err)
		return incoming, nil
	}

	dc.log.Debug("Accepted new session for Cluster Dashboard. Dialing to create reverse connection with dummy dialer now...")

	// Dial to create a reversion connection with dummy dialer.
	gConn, err := grpc.Dial(":0", // grpc.NewClient("passthrough://:0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := cliSession.Open()
			if err != nil {
				dc.log.Error("Failed to open CLI session during dial: %v", err)
				return nil, status.Error(codes.Internal, err.Error())
			} else {
				dc.log.Debug("Opened cliSession. conn.LocalAddr(): %v, conn.RemoteAddr(): %v", conn.LocalAddr(), conn.RemoteAddr())
			}

			return conn, nil
		}))
	if err != nil {
		dc.log.Error("Failed to open reverse Distributed Cluster connection with Cluster Dashboard: %v", err)
		return conn, nil
	}

	dc.log.Debug("Successfully dialed to create reverse connection with Cluster Dashboard. Target: %v", gConn.Target())

	// Create a Cluster Dashboard client and register it.
	dc.clusterDashboard = proto.NewClusterDashboardClient(gConn)
	dc.gatewayDaemon.clusterDashboard = dc.clusterDashboard
	return conn, nil
}

// Close closes the listener.
// Any blocked 'Accept' operations will be unblocked and return errors.
func (dc *DistributedCluster) Close() error {
	if !atomic.CompareAndSwapInt32(&dc.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the listener
	if dc.listener != nil {
		err := dc.listener.Close()

		if err != nil {
			dc.log.Error("Error while closing DistributedCluster listener: %v", err)
		}
	}

	return nil
}

// Addr returns the listener's network address.
func (dc *DistributedCluster) Addr() net.Addr {
	return dc.listener.Addr()
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (dc *DistributedCluster) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	return dc.gatewayDaemon.GetClusterActualGpuInfo(ctx, in)
}

// GetClusterVirtualGpuInfo returns the current vGPU (or "deflated GPU") resource metrics on the node.
func (dc *DistributedCluster) GetClusterVirtualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterVirtualGpuInfo, error) {
	return dc.gatewayDaemon.getClusterVirtualGpuInfo(ctx, in)
}

// SetTotalVirtualGPUs adjusts the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (dc *DistributedCluster) SetTotalVirtualGPUs(ctx context.Context, in *proto.SetVirtualGPUsRequest) (*proto.VirtualGpuInfo, error) {
	return dc.gatewayDaemon.setTotalVirtualGPUs(ctx, in)
}

func (dc *DistributedCluster) ListKernels(_ context.Context, _ *proto.Void) (*proto.ListKernelsResponse, error) {
	return dc.gatewayDaemon.listKernels()
}

func (dc *DistributedCluster) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	return dc.gatewayDaemon.MigrateKernelReplica(ctx, in)
}

func (dc *DistributedCluster) Ping(_ context.Context, _ *proto.Void) (*proto.Pong, error) {
	return &proto.Pong{Id: dc.gatewayDaemon.id}, nil
}

// RegisterDashboard is called by the Cluster Dashboard backend server to both verify that a connection has been
// established and to obtain any important configuration information, such as the deployment mode (i.e., Docker or
// Kubernetes), from the Cluster Gateway.
func (dc *DistributedCluster) RegisterDashboard(ctx context.Context, in *proto.Void) (*proto.DashboardRegistrationResponse, error) {
	return dc.gatewayDaemon.RegisterDashboard(ctx, in)
}

func (dc *DistributedCluster) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return dc.gatewayDaemon.PingKernel(ctx, in)
}

// FailNextExecution ensures that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (dc *DistributedCluster) FailNextExecution(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return dc.gatewayDaemon.FailNextExecution(ctx, in)
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
// If the Cluster is not running in Docker mode, then this will return an error.
func (dc *DistributedCluster) GetVirtualDockerNodes(ctx context.Context, in *proto.Void) (*proto.GetVirtualDockerNodesResponse, error) {
	return dc.gatewayDaemon.GetVirtualDockerNodes(ctx, in)
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
// If the Cluster is not running in Docker mode, then this will return an error.
func (dc *DistributedCluster) GetDockerSwarmNodes(ctx context.Context, in *proto.Void) (*proto.GetDockerSwarmNodesResponse, error) {
	return dc.gatewayDaemon.GetDockerSwarmNodes(ctx, in)
}

func (dc *DistributedCluster) GetNumNodes(ctx context.Context, in *proto.Void) (*proto.NumNodesResponse, error) {
	return dc.gatewayDaemon.GetNumNodes(ctx, in)
}

func (dc *DistributedCluster) SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error) {
	return dc.gatewayDaemon.SetNumClusterNodes(ctx, in)
}

func (dc *DistributedCluster) AddClusterNodes(ctx context.Context, in *proto.AddClusterNodesRequest) (*proto.AddClusterNodesResponse, error) {
	return dc.gatewayDaemon.AddClusterNodes(ctx, in)
}

func (dc *DistributedCluster) RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error) {
	return dc.gatewayDaemon.RemoveSpecificClusterNodes(ctx, in)
}

func (dc *DistributedCluster) RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error) {
	return dc.gatewayDaemon.RemoveClusterNodes(ctx, in)
}

func (dc *DistributedCluster) ClusterAge(ctx context.Context, in *proto.Void) (*proto.ClusterAgeResponse, error) {
	return dc.gatewayDaemon.ClusterAge(ctx, in)
}

func (dc *DistributedCluster) ModifyClusterNodes(ctx context.Context, in *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error) {
	return dc.gatewayDaemon.ModifyClusterNodes(ctx, in)
}

// QueryMessage is used to query whether a given ZMQ message has been seen by any of the Cluster components
// and what the status of that message is (i.e., sent, response received, etc.)
func (dc *DistributedCluster) QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error) {
	return dc.gatewayDaemon.QueryMessage(ctx, in)
}

// ForceLocalDaemonToReconnect is used to tell a Local Daemon to reconnect to the Cluster Gateway.
// This is mostly used for testing/debugging the reconnection process.
func (dc *DistributedCluster) ForceLocalDaemonToReconnect(ctx context.Context, in *proto.ForceLocalDaemonToReconnectRequest) (*proto.Void, error) {
	return dc.gatewayDaemon.ForceLocalDaemonToReconnect(ctx, in)
}
