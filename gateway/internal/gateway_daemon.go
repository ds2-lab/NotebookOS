package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"golang.org/x/net/context"
	"sync"
)

type MessageForwarder interface {
}

type KernelManager interface {
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

type GatewayDaemonBuilder struct {
	notifier      domain.Notifier
	forwarder     MessageForwarder
	kernelManager KernelManager
	router        *router.Router
	cluster       scheduling.Cluster
	options       *domain.ClusterGatewayOptions
}

func NewGatewayDaemonBuilder(options *domain.ClusterGatewayOptions) *GatewayDaemonBuilder {
	return &GatewayDaemonBuilder{
		options: options,
	}
}

func (b *GatewayDaemonBuilder) WithNotifier(notifier domain.Notifier) *GatewayDaemonBuilder {
	b.notifier = notifier
	return b
}

func (b *GatewayDaemonBuilder) WithForwarder(forwarder MessageForwarder) *GatewayDaemonBuilder {
	b.forwarder = forwarder
	return b
}

func (b *GatewayDaemonBuilder) WithKernelManager(kernelManager KernelManager) *GatewayDaemonBuilder {
	b.kernelManager = kernelManager
	return b
}

func (b *GatewayDaemonBuilder) WithCluster(cluster scheduling.Cluster) *GatewayDaemonBuilder {
	b.cluster = cluster
	return b
}

func (b *GatewayDaemonBuilder) Build() *GatewayDaemon {
	gatewayDaemon := &GatewayDaemon{
		notifier:      b.notifier,
		forwarder:     b.forwarder,
		kernelManager: b.kernelManager,
		options:       b.options,
	}

	metricsProvider := metrics.NewClusterMetricsProvider(
		b.options.PrometheusPort, gatewayDaemon, clusterGateway.UpdateClusterStatistics,
		clusterGateway.IncrementResourceCountsForNewHost, clusterGateway.DecrementResourceCountsForRemovedHost,
		&clusterGateway.numActiveTrainings)

	gatewayDaemon.metricsProvider = metricsProvider

	config.InitLogger(&gatewayDaemon.log, gatewayDaemon)

	return gatewayDaemon
}

type GatewayDaemon struct {
	DeploymentMode types.DeploymentMode

	notifier        domain.Notifier
	forwarder       MessageForwarder
	kernelManager   KernelManager
	metricsProvider *metrics.ClusterMetricsProvider
	cluster         scheduling.Cluster
	id              string

	options         *domain.ClusterGatewayOptions
	dockerNodeMutex sync.Mutex
	log             logger.Logger
}

func (g *GatewayDaemon) GetId() string {
	return g.id
}

func (g *GatewayDaemon) NotifyDashboard(name string, content string, typ messaging.NotificationType) {
	g.notifier.NotifyDashboard(name, content, typ)
}

func (g *GatewayDaemon) NotifyDashboardOfInfo(name string, content string) {
	g.notifier.NotifyDashboardOfInfo(name, content)
}

func (g *GatewayDaemon) NotifyDashboardOfWarning(name string, content string) {
	g.notifier.NotifyDashboardOfInfo(name, content)
}

func (g *GatewayDaemon) NotifyDashboardOfError(name string, content string) {
	g.notifier.NotifyDashboardOfInfo(name, content)
}

func (g *GatewayDaemon) RemoveHost(_ context.Context, in *proto.HostId) (*proto.Void, error) {
	g.cluster.RemoveHost(in.Id)
	return proto.VOID, nil
}

func (g *GatewayDaemon) GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error) {
	g.dockerNodeMutex.Lock()
	defer g.dockerNodeMutex.Unlock()

	resp := &proto.ClusterActualGpuInfo{
		GpuInfo: make(map[string]*proto.GpuInfo),
	}

	g.cluster.RangeOverHosts(func(hostId string, host scheduling.Host) (contd bool) {
		data, err := host.GetActualGpuInfo(ctx, in)
		if err != nil {
			g.log.Error("Failed to retrieve actual GPU info from Local Daemon %s on node %s because: %v", hostId, host.GetNodeName(), err)
			resp.GpuInfo[host.GetNodeName()] = nil
		} else {
			resp.GpuInfo[host.GetNodeName()] = data
		}
		return true
	})

	return resp, nil
}

func (g *GatewayDaemon) GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	g.dockerNodeMutex.Lock()
	defer g.dockerNodeMutex.Unlock()

	// TODO: For now, both Docker Swarm mode and Docker Compose mode support Virtual Docker Nodes.
	// 		 Eventually, Docker Swarm mode will only support Docker Swarm nodes, which correspond to real machines/VMs.
	// 		 Virtual Docker nodes correspond to each Local Daemon container, and are primarily used for development or
	//	   	 small, local simulations.
	if !g.DeploymentMode.IsDockerMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	hostIds := make([]string, 0, g.cluster.Len())

	g.cluster.RangeOverHosts(func(hostId string, _ scheduling.Host) (contd bool) {
		hostIds = append(hostIds, hostId)
		return true
	})

	resp := &proto.GetLocalDaemonNodeIDsResponse{
		HostIds: hostIds,
	}

	return resp, nil
}

func (g *GatewayDaemon) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	return g.kernelManager.StartKernel(ctx, in)
}

func (g *GatewayDaemon) GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	return g.kernelManager.GetKernelStatus(ctx, in)
}

func (g *GatewayDaemon) KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return g.kernelManager.KillKernel(ctx, in)
}

func (g *GatewayDaemon) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	return g.kernelManager.StopKernel(ctx, in)
}

func (g *GatewayDaemon) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	return g.kernelManager.MigrateKernelReplica(ctx, in)
}

func (g *GatewayDaemon) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	return g.kernelManager.NotifyKernelRegistered(ctx, in)
}

func (g *GatewayDaemon) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	return g.kernelManager.PingKernel(ctx, in)
}

func (g *GatewayDaemon) PromotePrewarmedContainer(ctx context.Context, in *proto.PrewarmedKernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	return g.kernelManager.PromotePrewarmedContainer(ctx, in)
}

func (g *GatewayDaemon) SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error) {
	return g.kernelManager.SmrReady(ctx, in)
}

func (g *GatewayDaemon) SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error) {
	return g.kernelManager.SmrNodeAdded(ctx, in)
}
