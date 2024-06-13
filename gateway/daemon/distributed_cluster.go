package daemon

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/hashicorp/yamux"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DistributedCluster struct {
	gateway.UnimplementedDistributedClusterServer

	gatewayDaemon *clusterGatewayImpl

	clusterDashboard gateway.ClusterDashboardClient

	listener net.Listener
	closed   int32

	log logger.Logger
}

func NewDistributedCluster(gatewayDaemon *clusterGatewayImpl, opts *domain.ClusterDaemonOptions) *DistributedCluster {
	dc := &DistributedCluster{
		gatewayDaemon: gatewayDaemon,
	}

	config.InitLogger(&dc.log, dc)

	return dc
}

// This function is used to notify the Dashboard that a panic has occurred.
// We'll still panic, but we send a notification first so that the user can be notified clearly that something bad has happened.
//
// Parameter:
// - identity (string): The identity of the entity/goroutine that panicked.
func (dc *DistributedCluster) HandlePanic(identity string, fatalErr interface{}) {
	dc.log.Error("Entity %s has panicked.", identity)

	_, err := dc.clusterDashboard.ErrorOccurred(context.TODO(), &gateway.ErrorMessage{
		ErrorName:    fmt.Sprintf("%s panicked.", identity),
		ErrorMessage: fmt.Sprintf("%v", fatalErr),
	})

	if err != nil {
		dc.log.Error("Failed to inform Cluster Dashboard that a fatal error occurred because: %v", err)
	}
}

func (dc *DistributedCluster) InducePanic(ctx context.Context, in *gateway.Void) (*gateway.Void, error) {
	panic("Inducing a panic.")
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (dc *DistributedCluster) Listen(transport string, addr string) (net.Listener, error) {
	dc.log.Debug("clusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	dc.listener = lis
	return dc, nil
}

// Accept waits for and returns the next connection to the listener.
func (dc *DistributedCluster) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := dc.listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := incoming

	dc.log.Debug("Accepting gRPC connection from Cluster Dashboard. LocalAddr: %v. RemoteAddr: %v.", incoming.LocalAddr(), incoming.RemoteAddr())

	// Initialize yamux session for bi-directional gRPC calls
	// At gateway side, we first wait a incoming replacement connection, then create a reverse provisioner connection to the Cluster Dashboard.
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
	gConn, err := grpc.Dial(":0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := cliSession.Open()
			if err != nil {
				dc.log.Error("Failed to open CLI session during dial: %v", err)
			} else {
				dc.log.Debug("Opened cliSession. conn.LocalAddr(): %v, conn.RemoteAddr(): %v", conn.LocalAddr(), conn.RemoteAddr())
			}

			return conn, err
		}))
	if err != nil {
		dc.log.Error("Failed to open reverse Distributed Cluster connection with Cluster Dashboard: %v", err)
		return conn, nil
	}

	dc.log.Debug("Successfully dialed to create reverse connection with Cluster Dashboard. Target: %v", gConn.Target())

	// Create a Cluster Dashboard client and register it.
	dc.clusterDashboard = gateway.NewClusterDashboardClient(gConn)
	dc.gatewayDaemon.clusterDashboard = dc.clusterDashboard
	return conn, nil
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (dc *DistributedCluster) Close() error {
	if !atomic.CompareAndSwapInt32(&dc.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the listener
	if dc.listener != nil {
		dc.listener.Close()
	}

	return nil
}

// Addr returns the listener's network address.
func (dc *DistributedCluster) Addr() net.Addr {
	return dc.listener.Addr()
}

// Return the current GPU resource metrics on the node.
func (d *DistributedCluster) GetClusterActualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterActualGpuInfo, error) {
	return d.gatewayDaemon.GetClusterActualGpuInfo(ctx, in)
}

// Return the current vGPU (or "deflated GPU") resource metrics on the node.
func (d *DistributedCluster) GetClusterVirtualGpuInfo(ctx context.Context, in *gateway.Void) (*gateway.ClusterVirtualGpuInfo, error) {
	return d.gatewayDaemon.getClusterVirtualGpuInfo(ctx, in)
}

// Adjust the total number of virtual GPUs available on a particular node.
//
// This operation will fail if the new number of virtual GPUs is less than the number of allocated GPUs.
// For example, if this node has a total of 64 vGPUs, of which 48 are actively allocated, and
// this function is called with the new total number specified as 32, then the operation will fail.
// In this case (when the operation fails), an ErrInvalidParameter is returned.
func (d *DistributedCluster) SetTotalVirtualGPUs(ctx context.Context, in *gateway.SetVirtualGPUsRequest) (*gateway.VirtualGpuInfo, error) {
	return d.gatewayDaemon.setTotalVirtualGPUs(ctx, in)
}

func (dc *DistributedCluster) ListKernels(ctx context.Context, in *gateway.Void) (*gateway.ListKernelsResponse, error) {
	return dc.gatewayDaemon.listKernels()
}

func (dc *DistributedCluster) MigrateKernelReplica(ctx context.Context, in *gateway.MigrationRequest) (*gateway.MigrateKernelResponse, error) {
	return dc.gatewayDaemon.MigrateKernelReplica(ctx, in)
}

func (dc *DistributedCluster) Ping(ctx context.Context, in *gateway.Void) (*gateway.Pong, error) {
	return &gateway.Pong{Id: dc.gatewayDaemon.id}, nil
}

// Ensure that the next 'execute_request' for the specified kernel fails.
// This is to be used exclusively for testing/debugging purposes.
func (dc *DistributedCluster) FailNextExecution(ctx context.Context, in *gateway.KernelId) (*gateway.Void, error) {
	return dc.gatewayDaemon.FailNextExecution(ctx, in)
}
