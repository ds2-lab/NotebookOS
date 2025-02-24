package grpc

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/utils"

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

type Notifier interface {
	NotifyDashboard(name string, content string, typ messaging.NotificationType)

	// NotifyDashboardOfInfo is used to issue an "info" notification to the internalCluster Dashboard.
	NotifyDashboardOfInfo(name string, content string)

	// NotifyDashboardOfWarning is used to issue a "warning" notification to the internalCluster Dashboard.
	NotifyDashboardOfWarning(name string, content string)

	// NotifyDashboardOfError is used to issue an "error" notification to the internalCluster Dashboard.
	NotifyDashboardOfError(name string, content string)
}

type GatewayDaemon interface {
	Notifier

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

	PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error)

	SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error)
	SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error)
}

type ClusterGatewayServer struct {
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer

	daemon GatewayDaemon

	id string

	log logger.Logger
}

// ID returns the unique ID of the provisioner.
func (srv *ClusterGatewayServer) ID(_ context.Context, _ *proto.Void) (*proto.ProvisionerId, error) {
	return &proto.ProvisionerId{Id: srv.id}, nil
}

// RemoveHost removes a local gateway from the cluster.
func (srv *ClusterGatewayServer) RemoveHost(ctx context.Context, in *proto.HostId) (*proto.Void, error) {
	return srv.daemon.RemoveHost(ctx, in)
}

func (srv *ClusterGatewayServer) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return srv.daemon.PingKernel(ctx, in)
}

// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
// need to start immediately after replica started, e.g., preempting a training task.
//
// The function will simply remove the replica from the kernel without stopping it.
// The caller should stop the replica after confirmed that the new replica is ready.
func (srv *ClusterGatewayServer) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	return srv.daemon.MigrateKernelReplica(ctx, in)
}

// NotifyKernelRegistered notifies the Gateway that a distributed kernel replica has started somewhere.
func (srv *ClusterGatewayServer) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	return srv.daemon.NotifyKernelRegistered(ctx, in)
}

func (srv *ClusterGatewayServer) SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error) {
	return srv.daemon.SmrReady(ctx, in)
}

func (srv *ClusterGatewayServer) SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error) {
	return srv.daemon.SmrNodeAdded(ctx, in)
}

// PingGateway is a no-op for testing connectivity.
func (srv *ClusterGatewayServer) PingGateway(ctx context.Context, in *proto.Void) (*proto.Void, error) {
	return in, nil
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (srv *ClusterGatewayServer) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	return srv.daemon.GetClusterActualGpuInfo(ctx, in)
}

// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
func (srv *ClusterGatewayServer) GetLocalDaemonNodeIDs(ctx context.Context, in *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	return srv.daemon.GetLocalDaemonNodeIDs(ctx, in)
}

// StartKernel starts a kernel or kernel replica.
func (srv *ClusterGatewayServer) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return srv.daemon.StartKernel(ctx, in)
}

// PromotePrewarmedContainer is similar to StartKernelReplica, except that PromotePrewarmedContainer launches the new
// kernel using an existing, pre-warmed container that is already available on this host.
func (srv *ClusterGatewayServer) PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return srv.daemon.PromotePrewarmedContainer(ctx, in)
}

// GetKernelStatus returns the status of a kernel.
func (srv *ClusterGatewayServer) GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	return srv.daemon.GetKernelStatus(ctx, in)
}

// KillKernel kills a kernel.
func (srv *ClusterGatewayServer) KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.KillKernel(ctx, in)
}

// StopKernel stops a kernel gracefully and return immediately.
func (srv *ClusterGatewayServer) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.StopKernel(ctx, in)
}

// SetID sets the local gateway id and return old id for failure tolerance.
// This also instructs the Local Daemon associated with the LocalGateway to create a PrometheusManager and begin serving metrics.
func (srv *ClusterGatewayServer) SetID(ctx context.Context, in *proto.HostId) (*proto.HostId, error) {
	return nil, ErrNotImplemented
}

// StartKernelReplica starts a kernel replica on the local host.
func (srv *ClusterGatewayServer) StartKernelReplica(ctx context.Context, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return nil, ErrNotImplemented
}

// GetActualGpuInfo return the current GPU resource metrics on the node.
// @Deprecated: this should eventually be merged with the updated/unified ModifyClusterNodes API.
func (srv *ClusterGatewayServer) GetActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.GpuInfo, error) {
	return nil, ErrNotImplemented
}

// Notify is used to report that an error occurred within one of the local daemons (or possibly a jupyter kernel).
func (srv *ClusterGatewayServer) Notify(ctx context.Context, in *proto.Notification) (*proto.Void, error) {
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
	go srv.daemon.NotifyDashboard(in.Title, in.Message, messaging.NotificationType(in.NotificationType))
	return proto.VOID, nil
}

func (srv *ClusterGatewayServer) mustEmbedUnimplementedLocalGatewayServer() {}

func (srv *ClusterGatewayServer) mustEmbedUnimplementedClusterGatewayServer() {}
