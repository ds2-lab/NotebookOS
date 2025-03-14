package rpc

import (
	"context"
	"github.com/Scusemua/go-utils/config"
	"github.com/hashicorp/yamux"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"

	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	NotificationTypeNames = []string{"ERROR", "WARNING", "INFO", "SUCCESS"}

	ErrNotImplemented = status.Error(codes.Unimplemented, "not implemented in daemon")
)

type GatewayDaemon interface {
	Close() error
	Start() error

	NewHostConnected(gConn *grpc.ClientConn, incoming net.Conn) error

	RemoveHost(ctx context.Context, in *proto.HostId) (*proto.Void, error)
	GetClusterActualGpuInfo() (*proto.ClusterActualGpuInfo, error)
	GetLocalDaemonNodeIDs() (*proto.GetLocalDaemonNodeIDsResponse, error)

	StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error)
	GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error)
	KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error)
	NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error)
	PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
	IsKernelActivelyTraining(kernelId string) (bool, error)

	SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error)
	SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error)
}

type ClusterGatewayGrpcServer struct {
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer

	listener net.Listener
	daemon   GatewayDaemon
	notifier domain.Notifier

	id string

	log logger.Logger
}

func (srv *ClusterGatewayGrpcServer) Close() error {
	if srv.daemon != nil {
		err := srv.daemon.Close()
		if err != nil {
			srv.log.Error("Failed to close daemon: %v", err)
		}
	}

	if srv.listener != nil {
		if err := srv.listener.Close(); err != nil {
			srv.log.Error("Failed to cleanly shutdown listener because: %v", err)
		}
	}

	return nil
}

// Addr returns the listener's network address.
// Addr is part of the net.Listener implementation.
func (srv *ClusterGatewayGrpcServer) Addr() net.Addr {
	return srv.listener.Addr()
}

func NewClusterGatewayServer(id string, daemon GatewayDaemon, notifier domain.Notifier) *ClusterGatewayGrpcServer {
	srv := &ClusterGatewayGrpcServer{
		daemon:   daemon,
		notifier: notifier,
		id:       id,
	}

	config.InitLogger(&srv.log, srv)

	return srv
}

func (srv *ClusterGatewayGrpcServer) Start() error {
	return srv.daemon.Start()
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (srv *ClusterGatewayGrpcServer) Listen(transport string, addr string) (net.Listener, error) {
	srv.log.Debug("ClusterGatewayImpl is listening on transport %s, addr %s.", transport, addr)

	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	srv.listener = lis
	return srv, nil
}

func (srv *ClusterGatewayGrpcServer) Accept() (net.Conn, error) {
	gConn, conn, incoming, connectionError := srv.acceptHostConnection()
	if connectionError != nil {
		return nil, connectionError
	}

	return conn, srv.daemon.NewHostConnected(gConn, incoming)
}

// acceptHostConnection accepts an incoming connection from a Local Daemon and establishes a bidirectional
// gRPC connection with that Local Daemon.
//
// This returns the gRPC connection, the initial connection, the replacement connection, and an error if one occurs.
func (srv *ClusterGatewayGrpcServer) acceptHostConnection() (*grpc.ClientConn, net.Conn, net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := srv.listener.Accept()
	if err != nil {
		return nil, nil, nil, err
	}

	srv.log.Debug("acceptHostConnection: accepting a new connection [%s].", incoming.RemoteAddr().String())

	// Initialize yamux session for bidirectional gRPC calls
	// At gateway side, we first wait an incoming replacement connection, then create a reverse provisioner connection to the host scheduler.
	cliSession, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		srv.log.Error("Failed to create yamux client session: %v", err)
		srv.log.Error("Incoming remote: %v", incoming.RemoteAddr().String())
		srv.log.Error("Incoming local: %v", incoming.LocalAddr().String())
		return nil, nil, nil, err
	}

	// Create a new session to replace the incoming connection.
	conn, err := cliSession.Accept()
	if err != nil {
		srv.log.Error("Failed to wait for the replacement of host scheduler connection: %v", err)
		srv.log.Error("Incoming remote: %v", incoming.RemoteAddr().String())
		srv.log.Error("Incoming local: %v", incoming.LocalAddr().String())
		srv.log.Error("CLI Session addr: %v", cliSession.Addr().String())
		srv.log.Error("CLI Session remote: %v", cliSession.RemoteAddr().String())
		srv.log.Error("CLI Session local: %v", cliSession.LocalAddr().String())
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
		srv.log.Error("Failed to open reverse provisioner connection: %v", err)
		return nil, nil, nil, err
	}

	return gConn, conn, incoming, err
}

// ID returns the unique ID of the provisioner.
func (srv *ClusterGatewayGrpcServer) ID(_ context.Context, _ *proto.Void) (*proto.ProvisionerId, error) {
	return &proto.ProvisionerId{Id: srv.id}, nil
}

// RemoveHost removes a local gateway from the cluster.
func (srv *ClusterGatewayGrpcServer) RemoveHost(ctx context.Context, in *proto.HostId) (*proto.Void, error) {
	return srv.daemon.RemoveHost(ctx, in)
}

func (srv *ClusterGatewayGrpcServer) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return srv.daemon.PingKernel(ctx, in)
}

// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
// need to start immediately after replica started, e.g., preempting a training task.
//
// The function will simply remove the replica from the kernel without stopping it.
// The caller should stop the replica after confirmed that the new replica is ready.
func (srv *ClusterGatewayGrpcServer) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	return srv.daemon.MigrateKernelReplica(ctx, in)
}

// NotifyKernelRegistered notifies the Gateway that a distributed kernel replica has started somewhere.
func (srv *ClusterGatewayGrpcServer) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	return srv.daemon.NotifyKernelRegistered(ctx, in)
}

func (srv *ClusterGatewayGrpcServer) SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error) {
	return srv.daemon.SmrReady(ctx, in)
}

func (srv *ClusterGatewayGrpcServer) SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error) {
	return srv.daemon.SmrNodeAdded(ctx, in)
}

// PingGateway is a no-op for testing connectivity.
func (srv *ClusterGatewayGrpcServer) PingGateway(ctx context.Context, in *proto.Void) (*proto.Void, error) {
	return in, nil
}

// GetActualGpuInfo is not implemented for the internalCluster Gateway.
// This is the single-node version of the function; it's only supposed to be issued to local daemons.
// The cluster-level version of this method is 'GetActualGpuInfo'.
//
// @Deprecated: this should eventually be merged with the updated/unified ModifyClusterNodes API.
func (srv *ClusterGatewayGrpcServer) GetActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.GpuInfo, error) {
	return nil, ErrNotImplemented
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (srv *ClusterGatewayGrpcServer) GetClusterActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	//resp := &proto.ClusterActualGpuInfo{
	//	GpuInfo: make(map[string]*proto.GpuInfo),
	//}

	return srv.daemon.GetClusterActualGpuInfo()
}

// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
func (srv *ClusterGatewayGrpcServer) GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	return srv.daemon.GetLocalDaemonNodeIDs()
}

// StartKernel starts a kernel or kernel replica.
func (srv *ClusterGatewayGrpcServer) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return srv.daemon.StartKernel(ctx, in)
}

// PromotePrewarmedContainer is similar to StartKernelReplica, except that PromotePrewarmedContainer launches the new
// kernel using an existing, pre-warmed container that is already available on this host.
func (srv *ClusterGatewayGrpcServer) PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return nil, ErrNotImplemented
}

// GetKernelStatus returns the status of a kernel.
func (srv *ClusterGatewayGrpcServer) GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	return srv.daemon.GetKernelStatus(ctx, in)
}

// IsKernelActivelyTraining is used to query whether a particular kernel is actively training.
func (srv *ClusterGatewayGrpcServer) IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	isTraining, err := srv.daemon.IsKernelActivelyTraining(in.Id)
	if err != nil {
		return nil, errorf(err)
	}

	return &proto.IsKernelTrainingReply{KernelId: in.Id, IsTraining: isTraining}, nil
}

// KillKernel kills a kernel.
func (srv *ClusterGatewayGrpcServer) KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.KillKernel(ctx, in)
}

// StopKernel stops a kernel gracefully and return immediately.
func (srv *ClusterGatewayGrpcServer) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.StopKernel(ctx, in)
}

// SetID sets the local gateway id and return old id for failure tolerance.
// This also instructs the Local Daemon associated with the LocalGateway to create a PrometheusManager and begin serving metrics.
func (srv *ClusterGatewayGrpcServer) SetID(ctx context.Context, in *proto.HostId) (*proto.HostId, error) {
	return nil, ErrNotImplemented
}

// StartKernelReplica starts a kernel replica on the local host.
func (srv *ClusterGatewayGrpcServer) StartKernelReplica(ctx context.Context, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return nil, ErrNotImplemented
}

// Notify is used to report that an error occurred within one of the local daemons (or possibly a jupyter kernel).
func (srv *ClusterGatewayGrpcServer) Notify(ctx context.Context, in *proto.Notification) (*proto.Void, error) {
	var logFunc func(format string, args ...interface{})
	if in.NotificationType == int32(messaging.ErrorNotification) {
		logFunc = srv.log.Error
	} else if in.NotificationType == int32(messaging.WarningNotification) {
		logFunc = srv.log.Warn
	} else {
		logFunc = srv.log.Debug
	}

	logFunc(utils.NotificationStyles[in.NotificationType].Render("Received %s notification \"%s\": %s"),
		NotificationTypeNames[in.NotificationType], in.Title, in.Message)
	go srv.notifier.NotifyDashboard(in.Title, in.Message, messaging.NotificationType(in.NotificationType))
	return proto.VOID, nil
}

func (srv *ClusterGatewayGrpcServer) mustEmbedUnimplementedLocalGatewayServer() {}

func (srv *ClusterGatewayGrpcServer) mustEmbedUnimplementedClusterGatewayServer() {}
