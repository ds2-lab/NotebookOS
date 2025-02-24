package grpc

import (
	"context"

	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
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

	AddHost()

	RemoveHost(id string)

	StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)

	KillKernel(ctx context.Context, in *proto.KernelId) (ret *proto.Void, err error)

	DeploymentMode() types.DeploymentMode

	HandlePing(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
}

type ClusterGatewayServer struct {
	proto.UnimplementedClusterGatewayServer
	proto.UnimplementedLocalGatewayServer

	daemon GatewayDaemon

	id string

	log logger.Logger
}

// ID returns the cluster gateway id and can be used to test connectivity.
func (srv *ClusterGatewayServer) ID(_ context.Context, _ *proto.Void) (*proto.ProvisionerId, error) {
	srv.log.Debug("Returning ID for RPC. ID=%s", srv.id)
	return &proto.ProvisionerId{Id: srv.id}, nil
}

// SetID sets the local gateway id and return old id for failure tolerance.
// This also instructs the Local Daemon associated with the LocalGateway to create a PrometheusManager and begin serving metrics.
func (srv *ClusterGatewayServer) SetID(_ context.Context, _ *proto.HostId) (*proto.HostId, error) {
	return nil, ErrNotImplemented
}

// RemoveHost removes a local gateway from the cluster.
func (srv *ClusterGatewayServer) RemoveHost(_ context.Context, in *proto.HostId) (*proto.Void, error) {
	srv.daemon.RemoveHost(in.Id)
	return proto.VOID, nil
}

func (srv *ClusterGatewayServer) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return srv.daemon.HandlePing(ctx, in)
}

// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
// need to start immediately after replica started, e.g., preempting a training task.
//
// The function will simply remove the replica from the kernel without stopping it.
// The caller should stop the replica after confirmed that the new replica is ready.
func (srv *ClusterGatewayServer) MigrateKernelReplica(_ context.Context, _ *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	panic("not implemented") // TODO: Implement
}

// NotifyKernelRegistered notifies the Gateway that a distributed kernel replica has started somewhere.
func (srv *ClusterGatewayServer) NotifyKernelRegistered(_ context.Context, _ *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (srv *ClusterGatewayServer) SmrReady(_ context.Context, _ *proto.SmrReadyNotification) (*proto.Void, error) {
	panic("not implemented") // TODO: Implement
}

func (srv *ClusterGatewayServer) SmrNodeAdded(_ context.Context, _ *proto.ReplicaInfo) (*proto.Void, error) {
	panic("not implemented") // TODO: Implement
}

// Notify is used to report that an error occurred within one of the local daemons (or possibly a jupyter kernel).
func (srv *ClusterGatewayServer) Notify(_ context.Context, in *proto.Notification) (*proto.Void, error) {
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

// PingGateway is a no-op for testing connectivity.
func (srv *ClusterGatewayServer) PingGateway(_ context.Context, _ *proto.Void) (*proto.Void, error) {
	panic("not implemented") // TODO: Implement
}

// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
func (srv *ClusterGatewayServer) GetClusterActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	panic("not implemented") // TODO: Implement
}

// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
func (srv *ClusterGatewayServer) GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	panic("not implemented") // TODO: Implement
}

// StartKernel starts a kernel or kernel replica.
func (srv *ClusterGatewayServer) StartKernel(_ context.Context, _ *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	panic("not implemented") // TODO: Implement
}

// StartKernelReplica starts a kernel replica on the local host.
func (srv *ClusterGatewayServer) StartKernelReplica(_ context.Context, _ *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	panic("not implemented") // TODO: Implement
}

// PromotePrewarmedContainer is similar to StartKernelReplica, except that PromotePrewarmedContainer launches the new
// kernel using an existing, pre-warmed container that is already available on this host.
func (srv *ClusterGatewayServer) PromotePrewarmedContainer(_ context.Context, _ *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	panic("not implemented") // TODO: Implement
}

// GetKernelStatus returns the status of a kernel.
func (srv *ClusterGatewayServer) GetKernelStatus(_ context.Context, _ *proto.KernelId) (*proto.KernelStatus, error) {
	panic("not implemented") // TODO: Implement
}

// KillKernel kills a kernel.
func (srv *ClusterGatewayServer) KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.KillKernel(ctx, in)
}

// StopKernel stops a kernel gracefully and return immediately.
func (srv *ClusterGatewayServer) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return srv.daemon.StopKernel(ctx, in)
}

// GetActualGpuInfo return the current GPU resource metrics on the node.
// @Deprecated: this should eventually be merged with the updated/unified ModifyClusterNodes API.
func (srv *ClusterGatewayServer) GetActualGpuInfo(_ context.Context, _ *proto.Void) (*proto.GpuInfo, error) {
	return nil, ErrNotImplemented
}

func (srv *ClusterGatewayServer) mustEmbedUnimplementedLocalGatewayServer() {
	panic("not implemented") // TODO: Implement
}

func (srv *ClusterGatewayServer) mustEmbedUnimplementedClusterGatewayServer() {
	panic("not implemented") // TODO: Implement
}
