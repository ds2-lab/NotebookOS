package grpc

import (
	"context"
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"

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
	ID(ctx context.Context, in *proto.Void) (*proto.ProvisionerId, error)
	RemoveHost(ctx context.Context, in *proto.HostId) (*proto.Void, error)
	GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error)
	GetLocalDaemonNodeIDs(ctx context.Context, in *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error)

	StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error)
	GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error)
	KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error)
	NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error)
	PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
	IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error)

	PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error)

	SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error)
	SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error)
}

type ClusterGatewayGrpcServer struct {
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer

	daemon GatewayDaemon

	notifier domain.Notifier

	id string

	log logger.Logger
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

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (srv *ClusterGatewayGrpcServer) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	return srv.daemon.GetClusterActualGpuInfo(ctx, in)
}

// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
func (srv *ClusterGatewayGrpcServer) GetLocalDaemonNodeIDs(ctx context.Context, in *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	return srv.daemon.GetLocalDaemonNodeIDs(ctx, in)
}

// StartKernel starts a kernel or kernel replica.
func (srv *ClusterGatewayGrpcServer) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return srv.daemon.StartKernel(ctx, in)
}

// PromotePrewarmedContainer is similar to StartKernelReplica, except that PromotePrewarmedContainer launches the new
// kernel using an existing, pre-warmed container that is already available on this host.
func (srv *ClusterGatewayGrpcServer) PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return srv.daemon.PromotePrewarmedContainer(ctx, in)
}

// GetKernelStatus returns the status of a kernel.
func (srv *ClusterGatewayGrpcServer) GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	return srv.daemon.GetKernelStatus(ctx, in)
}

func (srv *ClusterGatewayGrpcServer) IsKernelActivelyTraining(ctx context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	return srv.daemon.IsKernelActivelyTraining(ctx, in)
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

// GetActualGpuInfo return the current GPU resource metrics on the node.
// @Deprecated: this should eventually be merged with the updated/unified ModifyClusterNodes API.
func (srv *ClusterGatewayGrpcServer) GetActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.GpuInfo, error) {
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
