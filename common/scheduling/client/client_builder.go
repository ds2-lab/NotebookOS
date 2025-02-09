package client

//
//import (
//	"fmt"
//	"github.com/Scusemua/go-utils/config"
//	"github.com/go-zeromq/zmq4"
//	"github.com/scusemua/distributed-notebook/common/jupyter"
//	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
//	"github.com/scusemua/distributed-notebook/common/jupyter/server"
//	"github.com/scusemua/distributed-notebook/common/metrics"
//	"github.com/scusemua/distributed-notebook/common/proto"
//	"github.com/scusemua/distributed-notebook/common/scheduling"
//	"golang.org/x/net/context"
//)
//
//type DistributedKernelClientBuilder struct {
//	ctx                context.Context
//	spec               *proto.KernelSpec
//	numReplicas        int
//	hostId             string
//	connectionInfo     *jupyter.ConnectionInfo
//	persistentId       string
//	debugMode          bool
//	execFailedCallback scheduling.ExecutionFailedCallback
//	latencyCallback    scheduling.ExecutionLatencyCallback
//	statsProvider      scheduling.StatisticsProvider
//	notifyCallback     scheduling.NotificationCallback
//}
//
//func NewDistributedKernelClientBuilder() *DistributedKernelClientBuilder {
//	return &DistributedKernelClientBuilder{}
//}
//
//func (b *DistributedKernelClientBuilder) WithContext(ctx context.Context) *DistributedKernelClientBuilder {
//	b.ctx = ctx
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithSpec(spec *proto.KernelSpec) *DistributedKernelClientBuilder {
//	b.spec = spec
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithNumReplicas(numReplicas int) *DistributedKernelClientBuilder {
//	b.numReplicas = numReplicas
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithHostId(hostId string) *DistributedKernelClientBuilder {
//	b.hostId = hostId
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithConnectionInfo(connectionInfo *jupyter.ConnectionInfo) *DistributedKernelClientBuilder {
//	b.connectionInfo = connectionInfo
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithPersistentId(persistentId string) *DistributedKernelClientBuilder {
//	b.persistentId = persistentId
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithDebugMode(debugMode bool) *DistributedKernelClientBuilder {
//	b.debugMode = debugMode
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithExecutionFailedCallback(callback scheduling.ExecutionFailedCallback) *DistributedKernelClientBuilder {
//	b.execFailedCallback = callback
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithExecutionLatencyCallback(callback scheduling.ExecutionLatencyCallback) *DistributedKernelClientBuilder {
//	b.latencyCallback = callback
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithStatisticsProvider(provider scheduling.StatisticsProvider) *DistributedKernelClientBuilder {
//	b.statsProvider = provider
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) WithNotificationCallback(callback scheduling.NotificationCallback) *DistributedKernelClientBuilder {
//	b.notifyCallback = callback
//	return b
//}
//
//func (b *DistributedKernelClientBuilder) BuildKernel() scheduling.Kernel {
//	return b.Build()
//}
//
//func (b *DistributedKernelClientBuilder) Build() *DistributedKernelClient {
//	kernel := &DistributedKernelClient{
//		id:           b.spec.Id,
//		persistentId: b.persistentId,
//		debugMode:    b.debugMode,
//		server: server.New(b.ctx, &jupyter.ConnectionInfo{Transport: "tcp", SignatureScheme: b.connectionInfo.SignatureScheme, Key: b.connectionInfo.Key}, metrics.ClusterGateway, func(s *server.AbstractServer) {
//			s.Sockets.Shell = messaging.NewSocket(zmq4.NewRouter(s.Ctx), 0, messaging.ShellMessage, fmt.Sprintf("DK-Router-Shell[%s]", b.spec.Id))
//			s.Sockets.IO = messaging.NewSocket(zmq4.NewPub(s.Ctx), 0, messaging.IOMessage, fmt.Sprintf("DK-Pub-IO[%s]", b.spec.Id)) // connectionInfo.IOSubPort}
//			s.PrependId = true
//			/* The DistributedKernelClient lives on the Gateway. The Shell forwarder only receives messages from the frontend, which should not be acknowledged. */
//			s.ShouldAckMessages = false
//			s.ReconnectOnAckFailure = false
//			s.ComponentId = b.hostId
//			s.DebugMode = b.debugMode
//			s.Name = fmt.Sprintf("DistrKernelClient-%s", b.spec.Id)
//			s.StatisticsAndMetricsProvider = b.statsProvider
//			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", b.spec.Id))
//		}),
//		status:               jupyter.KernelStatusInitializing,
//		notificationCallback: b.notifyCallback,
//		spec:                 b.spec,
//		replicas:             make(map[int32]scheduling.KernelReplica, b.numReplicas), // make([]scheduling.Replica, numReplicas),
//		targetNumReplicas:    int32(b.numReplicas),
//		cleaned:              make(chan struct{}),
//	}
//	kernel.nextNodeId.Store(int32(b.numReplicas + 1))
//	kernel.BaseServer = kernel.server.Server()
//	kernel.SessionManager = NewSessionManager(b.spec.Session)
//	kernel.busyStatus = NewAggregateKernelStatus(kernel, b.numReplicas)
//	kernel.log = kernel.server.Log
//
//	temporaryKernelReplicaClient := &TemporaryKernelReplicaClient{kernel}
//	kernel.temporaryKernelReplicaClient = temporaryKernelReplicaClient
//
//	kernel.ExecutionManager = NewExecutionManager(kernel, b.numReplicas, b.execFailedCallback, b.notifyCallback,
//		b.latencyCallback, b.statsProvider)
//
//	return kernel
//}
