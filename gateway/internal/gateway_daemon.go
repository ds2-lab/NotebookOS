package daemon

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDaemonNotFoundOnNode = status.Error(codes.InvalidArgument, "could not find a local daemon on the specified kubernetes node")
)

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
		numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
		statisticsProvider scheduling.MetricsProvider, callbackProvider scheduling.CallbackProvider) scheduling.Kernel
}

type MetricsManager interface {
	scheduling.PrometheusMetricsProvider

	GetClusterStatistics() *metrics.ClusterStatistics
	LastFullStatisticsUpdate() time.Time

	// GatherClusterStatistics updates all the values in the ClusterStatistics field.
	//
	// GatherClusterStatistics is thread-safe.
	GatherClusterStatistics(cluster scheduling.Cluster)

	// ClearClusterStatistics clears the current metrics.ClusterStatistics struct.
	//
	// ClearClusterStatistics returns the serialized metrics.ClusterStatistics struct before it was cleared.
	ClearClusterStatistics()

	// UpdateClusterStatistics is passed to Distributed kernel Clients so that they may atomically update statistics.
	UpdateClusterStatistics(updaterFunc func(statistics *metrics.ClusterStatistics))
}

type MessageForwarder interface {
	GetRequestLogEntryByJupyterMessageId(msgId string) (*metrics.RequestLogEntryWrapper, bool)
	GetRequestTraces() []*proto.RequestTrace
	NumRequestLogEntriesByJupyterMsgId() int
	HasRequestLog() bool
	RequestLogLength() int
}

type KernelManager interface {
	ListKernels() ([]*proto.DistributedJupyterKernel, error)
	StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error)
	GetKernelStatus(ctx context.Context, in *proto.KernelId) (*proto.KernelStatus, error)
	KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error)
	MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error)
	NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error)
	PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error)
	IsKernelActivelyTraining(kernelId string) (bool, error)
	IsKernelActivelyMigrating(kernelId string) (bool, error)

	GetKernel(kernelId string) (scheduling.Kernel, bool)

	// GetHostsOfKernel returns the scheduling.Host instances on which replicas of the specified kernel are scheduled.
	GetHostsOfKernel(kernelId string) ([]scheduling.Host, error)

	SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error)
	SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error)

	FailNextExecution(kernelId string) error

	Close()
}

type GatewayDaemonBuilder struct {
	notifier                  domain.Notifier
	forwarder                 MessageForwarder
	kernelManager             KernelManager
	router                    *router.Router
	distributedClientProvider DistributedClientProvider
	connectionOptions         *jupyter.ConnectionInfo
	cluster                   scheduling.Cluster
	options                   *domain.ClusterGatewayOptions
	metricsManager            MetricsManager
	id                        string
}

func NewGatewayDaemonBuilder(options *domain.ClusterGatewayOptions) *GatewayDaemonBuilder {
	return &GatewayDaemonBuilder{
		options: options,
	}
}

func (b *GatewayDaemonBuilder) WithMetricsManager(metricsManager MetricsManager) *GatewayDaemonBuilder {
	b.metricsManager = metricsManager
	return b
}

func (b *GatewayDaemonBuilder) WithId(id string) *GatewayDaemonBuilder {
	b.id = id
	return b
}

func (b *GatewayDaemonBuilder) WithNotifier(notifier domain.Notifier) *GatewayDaemonBuilder {
	b.notifier = notifier
	return b
}

func (b *GatewayDaemonBuilder) WithForwarder(forwarder MessageForwarder) *GatewayDaemonBuilder {
	b.forwarder = forwarder
	return b
}

func (b *GatewayDaemonBuilder) WithDistributedClientProvider(provider DistributedClientProvider) *GatewayDaemonBuilder {
	b.distributedClientProvider = provider
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

func (b *GatewayDaemonBuilder) WithConnectionOptions(connectionOptions *jupyter.ConnectionInfo) *GatewayDaemonBuilder {
	b.connectionOptions = connectionOptions
	return b
}

func (b *GatewayDaemonBuilder) Build() *GatewayDaemon {
	if b.id == "" {
		b.id = uuid.NewString()
	}

	gatewayDaemon := &GatewayDaemon{
		id:                b.id,
		notifier:          b.notifier,
		forwarder:         b.forwarder,
		kernelManager:     b.kernelManager,
		options:           b.options,
		router:            b.router,
		metricsManager:    b.metricsManager,
		connectionOptions: b.connectionOptions,
		createdAt:         time.Now(),
		cleaned:           make(chan struct{}),
	}

	config.InitLogger(&gatewayDaemon.log, gatewayDaemon)

	return gatewayDaemon
}

type GatewayDaemon struct {
	closed  int32
	started int32
	cleaned chan struct{}

	id               string
	ipAddress        string
	networkTransport string

	createdAt time.Time

	deploymentMode types.DeploymentMode

	connectionOptions *jupyter.ConnectionInfo

	notifier       domain.Notifier
	forwarder      MessageForwarder
	kernelManager  KernelManager
	metricsManager MetricsManager
	router         *router.Router
	cluster        scheduling.Cluster

	options         *domain.ClusterGatewayOptions
	dockerNodeMutex sync.Mutex
	log             logger.Logger
}

func (g *GatewayDaemon) GetKernel(kernelId string) (scheduling.Kernel, bool) {
	return g.kernelManager.GetKernel(kernelId)
}

func (g *GatewayDaemon) ConnectionInfo() *jupyter.ConnectionInfo {
	return g.connectionOptions
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

func (g *GatewayDaemon) GetClusterActualGpuInfo() (*proto.ClusterActualGpuInfo, error) {
	g.dockerNodeMutex.Lock()
	defer g.dockerNodeMutex.Unlock()

	resp := &proto.ClusterActualGpuInfo{
		GpuInfo: make(map[string]*proto.GpuInfo),
	}

	g.cluster.RangeOverHosts(func(hostId string, host scheduling.Host) (contd bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		data, err := host.GetActualGpuInfo(ctx, proto.VOID)
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

func (g *GatewayDaemon) GetLocalDaemonNodeIDs() (*proto.GetLocalDaemonNodeIDsResponse, error) {
	g.dockerNodeMutex.Lock()
	defer g.dockerNodeMutex.Unlock()

	if !g.deploymentMode.IsDockerMode() {
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

func (g *GatewayDaemon) SmrReady(ctx context.Context, in *proto.SmrReadyNotification) (*proto.Void, error) {
	return g.kernelManager.SmrReady(ctx, in)
}

func (g *GatewayDaemon) SmrNodeAdded(ctx context.Context, in *proto.ReplicaInfo) (*proto.Void, error) {
	return g.kernelManager.SmrNodeAdded(ctx, in)
}

func (g *GatewayDaemon) IP() string {
	return g.ipAddress
}

func (g *GatewayDaemon) Transport() string {
	return g.networkTransport
}

func (g *GatewayDaemon) ControlPort() int32 {
	return int32(g.router.Socket(messaging.ControlMessage).Port)
}

func (g *GatewayDaemon) StdinPort() int32 {
	return int32(g.router.Socket(messaging.StdinMessage).Port)
}

func (g *GatewayDaemon) HbPort() int32 {
	return int32(g.router.Socket(messaging.HBMessage).Port)
}

func (g *GatewayDaemon) ID() string {
	return g.GetId()
}

func (g *GatewayDaemon) Start() error {
	if !atomic.CompareAndSwapInt32(&g.started, 0, 1) {
		g.log.Error("Start: GatewayDaemon already running.")
		return fmt.Errorf("GatewayDaemon::Start: already running")
	}

	g.log.Info("Starting router...")

	if g.options.IsKubernetesMode() {
		// Start the HTTP Kubernetes Scheduler service.
		go g.cluster.Scheduler().(scheduling.KubernetesClusterScheduler).StartHttpKubernetesSchedulerService()
	}

	// Start the router. The call will return on error or router.Close() is called.
	err := g.router.Start()

	// Clean up
	g.cleanUp()

	return err
}

func (g *GatewayDaemon) GetSerializedClusterStatistics() []byte {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)

	clusterStatistics := g.metricsManager.GetClusterStatistics()

	err := encoder.Encode(clusterStatistics.ConvertToSerializable())
	if err != nil {
		g.log.Error("GetSerializedClusterStatistics: %v", err)
		return nil
	}

	return buffer.Bytes()
}

func (g *GatewayDaemon) SpoofNotifications() (*proto.Void, error) {
	go func() {
		g.notifier.NotifyDashboard("Spoofed Error",
			"This is a made-up error message sent by the internalCluster Gateway.",
			messaging.ErrorNotification)
		g.notifier.NotifyDashboard("Spoofed Warning",
			"This is a made-up warning message sent by the internalCluster Gateway.",
			messaging.WarningNotification)
		g.notifier.NotifyDashboard("Spoofed Info Notification",
			"This is a made-up 'info' message sent by the internalCluster Gateway.",
			messaging.InfoNotification)
		g.notifier.NotifyDashboard("Spoofed Success Notification",
			"This is a made-up 'success' message sent by the internalCluster Gateway.",
			messaging.SuccessNotification)
	}()

	return proto.VOID, nil
}

func (g *GatewayDaemon) Close() error {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		// Closed already
		g.log.Warn("Close: already closed.")
		return nil
	}

	g.log.Warn("Closing.")

	if g.router != nil {
		g.log.Debug("Closing router")

		// Close the router
		if err := g.router.Close(); err != nil {
			g.log.Error("Failed to cleanly shutdown router because: %v", err)
		}
	}

	if g.kernelManager != nil {
		g.log.Debug("Closing kernel manager")
		g.kernelManager.Close()
	}

	if g.cluster != nil {
		g.cluster.Close()
	}

	if atomic.LoadInt32(&g.started) == 1 {
		// Wait for the newKernels to be cleaned up
		<-g.cleaned
	}

	if g.cluster != nil {
		g.cluster.Close()
	}

	return nil
}

func (g *GatewayDaemon) NewHostConnected(gConn *grpc.ClientConn, incoming net.Conn) error {
	// Create a host scheduler client and register it.
	host, err := entity.NewHostWithConn(uuid.NewString(), incoming.RemoteAddr().String(), g.cluster.NumReplicas(),
		g.cluster, g.cluster, g.metricsManager, gConn, g.cluster.Scheduler().Policy(), g.localDaemonDisconnected)

	if err != nil {
		if errors.Is(err, entity.ErrRestoreRequired) {
			err = g.restoreHost(host)
			if err != nil {
				return err
			}

			return nil
		} else {
			g.log.Error("Failed to create host scheduler client: %v", err)
			return err
		}
	}

	registrationError := g.registerNewHost(host)
	if registrationError != nil {
		g.log.Error("Failed to register new host %s (ID=%s) because: %v", host.GetNodeName(), host.GetID(), registrationError)
		return registrationError
	}

	return nil
}

func (g *GatewayDaemon) ListKernels() ([]*proto.DistributedJupyterKernel, error) {
	return g.kernelManager.ListKernels()
}

func (g *GatewayDaemon) FailNextExecution(kernelId string) error {
	return g.kernelManager.FailNextExecution(kernelId)
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
// If the internalCluster is not running in Docker mode, then this will return an error.
func (g *GatewayDaemon) GetVirtualDockerNodes() ([]*proto.VirtualDockerNode, error) {
	g.log.Debug("GetVirtualDockerNodes: Received request for the cluster's virtual docker nodes.")

	g.dockerNodeMutex.Lock()
	defer g.dockerNodeMutex.Unlock()

	if !g.options.IsDockerMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	nodes := make([]*proto.VirtualDockerNode, 0, g.cluster.Len())

	g.cluster.RangeOverHosts(func(_ string, host scheduling.Host) (contd bool) {
		virtualDockerNode := host.ToVirtualDockerNode()
		nodes = append(nodes, virtualDockerNode)

		return true
	})

	g.cluster.RangeOverDisabledHosts(func(_ string, host scheduling.Host) (cont bool) {
		virtualDockerNode := host.ToVirtualDockerNode()
		nodes = append(nodes, virtualDockerNode)

		return true
	})

	return nodes, nil
}

func (g *GatewayDaemon) GetNumNodes() int {
	return g.cluster.Len()
}

func (g *GatewayDaemon) SetNumClusterNodes(ctx context.Context, in *proto.SetNumClusterNodesRequest) (*proto.SetNumClusterNodesResponse, error) {
	initialSize := g.cluster.Len()
	g.log.Debug("SetNumClusterNodes: scaling from %d nodes to %d nodes",
		initialSize, in.TargetNumNodes)
	p := g.cluster.ScaleToSize(ctx, in.TargetNumNodes)
	if err := p.Error(); err != nil {
		g.log.Error("SetNumClusterNodes: scaling from %d nodes to %d nodes failed: %v",
			initialSize, in.TargetNumNodes, err)
		return nil, err
	}

	result, err := p.Result()
	if err != nil {
		g.log.Error("SetNumClusterNodes: scaling from %d nodes to %d nodes failed: %v",
			initialSize, in.TargetNumNodes, err)
		return nil, err
	}

	scaleResult := result.(scheduler.ScaleOperationResult)
	g.log.Debug("SetNumClusterNodes: successfully scaled from %d nodes to %d nodes: %s",
		initialSize, in.TargetNumNodes, scaleResult.String())

	return &proto.SetNumClusterNodesResponse{
		RequestId:   in.RequestId,
		OldNumNodes: int32(initialSize),
		NewNumNodes: int32(g.cluster.Len()),
	}, nil
}

func (g *GatewayDaemon) AddClusterNodes(ctx context.Context, n int32) (*proto.AddClusterNodesResponse, error) {
	g.log.Debug("AddClusterNodes: adding %d node(s).", n)
	p := g.cluster.RequestHosts(ctx, n)

	if err := p.Error(); err != nil {
		g.log.Error("AddClusterNodes: Failed to add %d node(s) because: %v", n, err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		g.log.Error("AddClusterNodes: Failed to add %d node(s) because: %v", n, err)
		return nil, err
	}

	scaleOutOperationResult := scaleResult.(*scheduler.ScaleOutOperationResult)

	g.log.Debug("AddClusterNodes: Successfully added %d node(s): %s", scaleOutOperationResult.String())

	return &proto.AddClusterNodesResponse{
		PrevNumNodes:      scaleOutOperationResult.PreviousNumNodes,
		NumNodesCreated:   scaleOutOperationResult.NumNodesCreated,
		NumNodesRequested: n,
	}, nil
}

func (g *GatewayDaemon) RemoveSpecificClusterNodes(ctx context.Context, in *proto.RemoveSpecificClusterNodesRequest) (*proto.RemoveSpecificClusterNodesResponse, error) {
	g.log.Debug("RemoveSpecificClusterNodes: Received request to remove %d specific node(s) from the cluster.",
		len(in.NodeIDs), strings.Join(in.NodeIDs, ", "))
	p := g.cluster.ReleaseSpecificHosts(ctx, in.NodeIDs)

	if err := p.Error(); err != nil {
		g.log.Error("RemoveSpecificClusterNodes: Failed to remove %d specific nodes because: %v",
			len(in.NodeIDs), err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		g.log.Error("RemoveSpecificClusterNodes: Failed to remove %d specific nodes because: %v",
			len(in.NodeIDs), err)
		return nil, err
	}

	scaleInOperationResult := scaleResult.(*scheduler.ScaleInOperationResult)
	g.log.Debug("RemoveSpecificClusterNodes: Successfully fulfilled specific node removal request: %s",
		scaleInOperationResult.String())

	return &proto.RemoveSpecificClusterNodesResponse{
		RequestId:       in.RequestId,
		OldNumNodes:     scaleInOperationResult.PreviousNumNodes,
		NumNodesRemoved: scaleInOperationResult.NumNodesTerminated,
		NewNumNodes:     scaleInOperationResult.CurrentNumNodes,
		NodesRemoved:    scaleInOperationResult.Nodes(),
	}, nil
}

func (g *GatewayDaemon) RemoveClusterNodes(ctx context.Context, in *proto.RemoveClusterNodesRequest) (*proto.RemoveClusterNodesResponse, error) {
	g.log.Debug("RemoveClusterNodes: Received request to remove %d node(s) from the cluster.", in.NumNodesToRemove)
	p := g.cluster.ReleaseHosts(ctx, in.NumNodesToRemove)

	if err := p.Error(); err != nil {
		g.log.Error("RemoveClusterNodes: Failed to remove %d nodes because: %v", in.NumNodesToRemove, err)
		return nil, err
	}

	scaleResult, err := p.Result()
	if err != nil {
		g.log.Error("RemoveClusterNodes: Failed to remove %d nodes because: %v", in.NumNodesToRemove, err)
		return nil, err
	}

	scaleInOperationResult := scaleResult.(*scheduler.ScaleInOperationResult)
	g.log.Debug("RemoveClusterNodes: Successfully fulfilled node removal request: %s",
		scaleInOperationResult.String())

	return &proto.RemoveClusterNodesResponse{
		RequestId:       in.RequestId,
		OldNumNodes:     scaleInOperationResult.PreviousNumNodes,
		NumNodesRemoved: scaleInOperationResult.NumNodesTerminated,
		NewNumNodes:     scaleInOperationResult.CurrentNumNodes,
	}, nil
}

func (g *GatewayDaemon) ClusterAge() int64 {
	return g.createdAt.UnixMilli()
}

func (g *GatewayDaemon) ModifyClusterNodes(_ context.Context, _ *proto.ModifyClusterNodesRequest) (*proto.ModifyClusterNodesResponse, error) {
	return nil, scheduling.ErrNotImplementedYet
}

func (g *GatewayDaemon) QueryMessage(ctx context.Context, in *proto.QueryMessageRequest) (*proto.QueryMessageResponse, error) {
	if !g.options.DebugMode {
		g.log.Warn("QueryMessage: We're not running in DebugMode.")
		return nil, status.Errorf(codes.Internal, "DebugMode is not enabled; cannot query messages")
	}

	if in.MessageId == "" {
		g.log.Warn("QueryMessage request did not contain a Message ID.")
		return nil, status.Errorf(codes.InvalidArgument,
			"you must specify a Jupyter message ID when querying for the status of a particular message")
	}

	if in.MessageId == "*" {
		if g.forwarder.RequestLogLength() == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "RequestLog is empty")
		}

		g.log.Debug("QueryMessage: Received message query for all messages in log. Will be returning %d message(s).",
			g.forwarder.NumRequestLogEntriesByJupyterMsgId())

		requestTraces := g.forwarder.GetRequestTraces()

		// Build the response.
		resp := &proto.QueryMessageResponse{
			RequestTraces: requestTraces,
		}

		return resp, nil
	}

	wrapper, loaded := g.forwarder.GetRequestLogEntryByJupyterMessageId(in.MessageId)
	if !loaded {
		g.log.Warn("QueryMessage: No request log entry found for request with Jupyter message ID \"%s\"", in.MessageId)
		g.log.Warn("QueryMessage: #Entries in RequestLog (by Jupyter message ID): %d", g.forwarder.NumRequestLogEntriesByJupyterMsgId())
		return nil, status.Errorf(codes.InvalidArgument, "no request entry found in request log for entry with ID=\"%s\"", in.MessageId)
	}

	// Make sure the Jupyter message types match (if the caller specified a Jupyter message type).
	if in.MessageType != "" && in.MessageType != wrapper.JupyterMessageType {
		g.log.Warn("QueryMessage: Found request log entry for request with Jupyter message ID \"%s\", but request had type \"%s\" whereas the request type is \"%s\"",
			in.MessageId, wrapper.JupyterMessageType, in.MessageType)
		return nil, status.Errorf(codes.InvalidArgument,
			"found request log entry for request with Jupyter message ID \"%s\", but request had type \"%s\" whereas the request type is \"%s\"",
			in.MessageId, wrapper.JupyterMessageType, in.MessageType)
	}

	// Make sure the kernel IDs types match (if the caller specified a Jupyter kernel ID).
	if in.KernelId != "" && in.KernelId != wrapper.KernelId {
		g.log.Warn("QueryMessage: Found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
		return nil, status.Errorf(codes.InvalidArgument,
			"found request log entry for request with Jupyter message ID \"%s\", but request is targeting kernel \"%s\" whereas the specified kernel ID is \"%s\"",
			in.MessageId, wrapper.KernelId, in.KernelId)
	}

	g.log.Debug("QueryMessage: Received request for Jupyter %s \"%s\" request with JupyterID=\"%s\" targeting kernel \"%s\"",
		wrapper.MessageType.String(), wrapper.JupyterMessageType, wrapper.JupyterMessageId, wrapper.KernelId)

	// Build the response.
	requestTraces := make([]*proto.RequestTrace, 0, wrapper.EntriesByNodeId.Len())
	wrapper.EntriesByNodeId.Range(func(i int32, entry *metrics.RequestLogEntry) (contd bool) {
		requestTraces = append(requestTraces, entry.RequestTrace)
		return true
	})

	resp := &proto.QueryMessageResponse{
		RequestTraces: requestTraces,
	}

	return resp, nil
}

func (g *GatewayDaemon) ForceLocalDaemonToReconnect(hostId string, delay bool) error {
	host, ok := g.cluster.GetHost(hostId)
	if !ok {
		return status.Error(codes.InvalidArgument, scheduling.ErrHostNotFound.Error())
	}

	_, err := host.ReconnectToGateway(context.Background(), &proto.ReconnectToGatewayRequest{
		Delay: delay,
	})

	if err != nil {
		g.log.Error("ForceLocalDaemonToReconnect: Error while instruction Local Daemon %s to reconnect to us: %v",
			hostId, err)

		if _, ok := status.FromError(err); !ok {
			err = status.Error(codes.Internal, err.Error())
		}

		return err
	}

	return nil
}

// ClearClusterStatistics resets the ClusterStatistics field.
//
// ClearClusterStatistics returns the value of the ClusterStatistics field before it was cleared.
//
// If there's an error returning the ClusterStatistics field, then an error will be returned instead.
// The ClusterStatistics field will NOT be cleared in this case.
func (g *GatewayDaemon) ClearClusterStatistics() []byte {
	serializedStatistics := g.GetSerializedClusterStatistics()

	g.metricsManager.ClearClusterStatistics()

	// Basically initialize the statistics with some values, but in a separate goroutine.
	go g.metricsManager.GatherClusterStatistics(g.cluster)

	return serializedStatistics
}

// GetHostsOfKernel returns the scheduling.Host instances on which replicas of the specified kernel are scheduled.
func (g *GatewayDaemon) GetHostsOfKernel(kernelId string) ([]scheduling.Host, error) {
	return g.kernelManager.GetHostsOfKernel(kernelId)
}

// IsKernelActivelyTraining is used to query whether a particular kernel is actively training.
func (g *GatewayDaemon) IsKernelActivelyTraining(kernelId string) (bool, error) {
	isTraining, err := g.kernelManager.IsKernelActivelyTraining(kernelId)
	if err != nil {
		return false, err
	}

	return isTraining, nil
}

func (g *GatewayDaemon) DeploymentMode() types.DeploymentMode {
	return g.deploymentMode
}

func (g *GatewayDaemon) PolicyKey() scheduling.PolicyKey {
	return g.cluster.Scheduler().Policy().PolicyKey()
}

func (g *GatewayDaemon) NumReplicasPerKernel() int {
	return g.cluster.Scheduler().Policy().NumReplicas()
}

func (g *GatewayDaemon) cleanUp() {
	// Clear nothing for now:
	// Hosts and newKernels may contact other gateways to restore status.
	close(g.cleaned)
}

func (g *GatewayDaemon) localDaemonDisconnected(localDaemonId string, nodeName string, errorName string, errorMessage string) (err error) {
	g.log.Warn("Local Daemon %s (Node %s) has disconnected. Removing from cluster.", localDaemonId, nodeName)
	_, err = g.RemoveHost(context.TODO(), &proto.HostId{
		Id:       localDaemonId,
		NodeName: nodeName, /* Not needed */
	})

	if err != nil {
		g.log.Error("Error while removing local daemon %s (node: %s): %v", localDaemonId, nodeName, err)
	}

	go g.notifier.NotifyDashboard(errorName, errorMessage, messaging.WarningNotification)

	return err
}

// registerNewHost is used to register a new Host (i.e., Local Daemon) with the Cluster after the Host connects
// to the Cluster Gateway.
//
// This will return nil on success.
func (g *GatewayDaemon) registerNewHost(host scheduling.Host) error {
	if !host.IsProperlyInitialized() {
		log.Fatalf(utils.RedStyle.Render("Newly-connected Host %s (ID=%s) was NOT properly initialized..."),
			host.GetNodeName(), host.GetID())
	}

	g.log.Info("Incoming Local Scheduler %s (ID=%s) connected", host.GetNodeName(), host.GetID())

	err := g.cluster.NewHostAddedOrConnected(host)
	if err != nil {
		g.log.Error("Error while adding newly-connected host %s (ID=%s) to the cluster: %v",
			host.GetNodeName(), host.GetID(), err)
		return err
	}

	go g.notifier.NotifyDashboardOfInfo("Local Scheduler Connected", fmt.Sprintf("Local Scheduler %s (ID=%s) has connected to the cluster Gateway.",
		host.GetNodeName(), host.GetID()))

	return nil
}

// restoreHost is used to restore an existing Host when a Local Daemon loses connection with the Cluster Gateway
// and then reconnects. This will return nil on success.
//
// If the cluster gateway recently crashed and the container restarted, then the restoration will fail, and
// restoreHost will simply treat the scheduling.Host as if it were a new host and pass it to RegisterNewHost,
// the result of which will be returned from restoreHost.
func (g *GatewayDaemon) restoreHost(host scheduling.Host) error {
	g.log.Warn("Newly-connected Local Daemon actually already exists.")

	// Sanity check.
	if host == nil {
		errorMessage := "We're supposed to restore a Local Daemon, but the host with which we would perform the restoration is nil...\n"
		g.notifier.NotifyDashboardOfError("Failed to Re-Register Local Daemon", errorMessage)
		log.Fatalf(utils.RedStyle.Render(errorMessage))
	}

	// Restore the Local Daemon.
	// This replaces the gRPC connection of the existing Host struct with that of a new one,
	// as well as a few other fields.
	registered, loaded := g.cluster.GetHost(host.GetID())
	if loaded {
		err := registered.Restore(host, g.localDaemonDisconnected)
		if err != nil {
			g.log.Error("Error while restoring host %v: %v", host, err)
			return err
		}

		g.log.Debug("Successfully restored existing Local Daemon %s (ID=%s).",
			registered.GetNodeName(), registered.GetID())

		go g.notifier.NotifyDashboardOfInfo(
			fmt.Sprintf("Local Daemon %s Reconnected", registered.GetNodeName()),
			fmt.Sprintf("Local Daemon %s on node %s has reconnected to the cluster Gateway.",
				registered.GetID(),
				registered.GetNodeName()))

		return nil
	}

	// This may occur if the cluster Gateway crashes and restarts.
	g.log.Warn("Supposedly existing Local Daemon (re)connected, but cannot find associated Host struct... "+
		"Node claims to be Local Daemon %s (ID=%s).", host.GetID(), host.GetNodeName())

	// Just register the Host as a new Local Daemon, despite the fact that the Host thinks it already exists.
	return g.registerNewHost(host)
}
