package kernel

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel/execution_failed"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel/provisioner"
	"time"
)

type ManagerBuilder struct {
	id                string
	cluster           scheduling.Cluster
	requestLog        *metrics.RequestLog
	responseForwarder ResponseForwarder
	metricsProvider   metricsProvider
	networkProvider   provisioner.NetworkProvider
	notifier          domain.Notifier
	schedulingPolicy  scheduling.Policy
	opts              *domain.ClusterGatewayOptions
}

// NewManagerBuilder initializes the builder.
func NewManagerBuilder() *ManagerBuilder {
	return &ManagerBuilder{}
}

// SetID sets the ID.
func (b *ManagerBuilder) SetID(id string) *ManagerBuilder {
	b.id = id
	return b
}

func (b *ManagerBuilder) SetSchedulingPolicy(schedulingPolicy scheduling.Policy) *ManagerBuilder {
	b.schedulingPolicy = schedulingPolicy
	return b
}

// SetCluster sets the cluster.
func (b *ManagerBuilder) SetCluster(cluster scheduling.Cluster) *ManagerBuilder {
	b.cluster = cluster
	return b
}

// SetRequestLog sets the request log.
func (b *ManagerBuilder) SetRequestLog(requestLog *metrics.RequestLog) *ManagerBuilder {
	b.requestLog = requestLog
	return b
}

// SetResponseForwarder sets the response forwarder.
func (b *ManagerBuilder) SetResponseForwarder(forwarder ResponseForwarder) *ManagerBuilder {
	b.responseForwarder = forwarder
	return b
}

// SetMetricsProvider sets the metrics provider.
func (b *ManagerBuilder) SetMetricsProvider(metricsProvider metricsProvider) *ManagerBuilder {
	b.metricsProvider = metricsProvider
	return b
}

// SetNetworkProvider sets the network provider.
func (b *ManagerBuilder) SetNetworkProvider(networkProvider provisioner.NetworkProvider) *ManagerBuilder {
	b.networkProvider = networkProvider
	return b
}

// SetNotifier sets the notifier.
func (b *ManagerBuilder) SetNotifier(notifier domain.Notifier) *ManagerBuilder {
	b.notifier = notifier
	return b
}

// SetOptions sets the cluster gateway options.
func (b *ManagerBuilder) SetOptions(opts *domain.ClusterGatewayOptions) *ManagerBuilder {
	b.opts = opts
	return b
}

// Build constructs the Manager object.
func (b *ManagerBuilder) Build() (*Manager, error) {
	if b.id == "" {
		return nil, fmt.Errorf("ID is required")
	}

	if b.cluster == nil {
		return nil, fmt.Errorf("cluster is required")
	}

	if b.opts == nil {
		return nil, fmt.Errorf("options struct is required")
	}

	if b.metricsProvider == nil {
		return nil, fmt.Errorf("metricsProvider is required")
	}

	//if b.networkProvider == nil {
	//	return nil, fmt.Errorf("networkProvider is required")
	//}

	manager := &Manager{
		id:                              b.id,
		cluster:                         b.cluster,
		requestLog:                      b.requestLog,
		responseForwarder:               b.responseForwarder,
		metricsProvider:                 b.metricsProvider,
		notifier:                        b.notifier,
		schedulingPolicy:                b.schedulingPolicy,
		provider:                        NewProvider(),
		sessions:                        hashmap.NewConcurrentMap[scheduling.Kernel](32),
		kernelsStarting:                 hashmap.NewCornelkMap[string, chan struct{}](64),
		handlers:                        make(map[messaging.MessageType]MessageHandler),
		debugMode:                       b.opts.DebugMode,
		submitExecuteRequestsOneAtATime: b.opts.SubmitExecuteRequestsOneAtATime,
	}

	config.InitLogger(&manager.log, manager)

	b.metricsProvider.SetNumActiveTrainingsPointer(&manager.numActiveTrainings)

	manager.handlers[messaging.ControlMessage] = manager.controlHandler
	manager.handlers[messaging.ShellMessage] = manager.shellHandler
	manager.handlers[messaging.StdinMessage] = manager.stdinHandler
	manager.handlers[messaging.HBMessage] = manager.heartbeatHandler

	failedExecutionHandler := execution_failed.NewHandler(b.opts, manager, b.notifier)
	manager.failedExecutionHandler = failedExecutionHandler

	manager.kernelProvisioner = provisioner.NewBuilder().
		SetID(b.id).
		SetCluster(b.cluster).
		SetNotifier(b.notifier).
		SetMetricsProvider(b.metricsProvider).
		SetKernelShellHandler(manager.shellHandlerWrapper).
		SetKernelProvider(manager.provider).
		SetKernelCallbackProvider(manager.CallbackProvider()).
		SetOptions(b.opts).
		Build()

	if b.opts.IdleSessionReclamationEnabled && b.opts.IdleSessionReclamationIntervalSec > 0 {
		interval := time.Duration(b.opts.IdleSessionReclamationIntervalSec) * time.Second
		numReplicasPerKernel := manager.cluster.Scheduler().Policy().NumReplicas()

		manager.idleSessionReclaimer = NewIdleSessionReclaimer(
			manager.provider.Kernels, interval, numReplicasPerKernel, nil)

		manager.idleSessionReclaimer.Start()
	}

	manager.executeRequestForwarder = client.NewExecuteRequestForwarder[[]*messaging.JupyterMessage](
		manager.notifier.NotifyDashboard, nil)

	return manager, nil
}
