package provisioner

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
)

type Builder struct {
	id                     string
	cluster                scheduling.Cluster
	notifier               domain.Notifier
	metricsProvider        metricsProvider
	kernelShellHandler     scheduling.KernelMessageHandler
	kernelProvider         KernelProvider
	kernelCallbackProvider scheduling.CallbackProvider
	opts                   *domain.ClusterGatewayOptions
}

// NewBuilder initializes a new builder instance.
func NewBuilder() *Builder {
	return &Builder{}
}

// SetID sets the ID.
func (b *Builder) SetID(id string) *Builder {
	b.id = id
	return b
}

// SetCluster sets the Cluster.
func (b *Builder) SetCluster(cluster scheduling.Cluster) *Builder {
	b.cluster = cluster
	return b
}

// SetNotifier sets the Notifier.
func (b *Builder) SetNotifier(notifier domain.Notifier) *Builder {
	b.notifier = notifier
	return b
}

// SetMetricsProvider sets the MetricsProvider.
func (b *Builder) SetMetricsProvider(metricsProvider metricsProvider) *Builder {
	b.metricsProvider = metricsProvider
	return b
}

// SetKernelShellHandler sets the KernelShellHandler.
func (b *Builder) SetKernelShellHandler(handler scheduling.KernelMessageHandler) *Builder {
	b.kernelShellHandler = handler
	return b
}

// SetKernelProvider sets the KernelProvider.
func (b *Builder) SetKernelProvider(provider KernelProvider) *Builder {
	b.kernelProvider = provider
	return b
}

// SetKernelCallbackProvider sets the KernelCallbackProvider.
func (b *Builder) SetKernelCallbackProvider(callbackProvider scheduling.CallbackProvider) *Builder {
	b.kernelCallbackProvider = callbackProvider
	return b
}

// SetOptions sets the ClusterGatewayOptions.
func (b *Builder) SetOptions(opts *domain.ClusterGatewayOptions) *Builder {
	b.opts = opts
	return b
}

// Build constructs the Provisioner object with validation.
func (b *Builder) Build() *Provisioner {
	provisioner := &Provisioner{
		id:                            b.id,
		cluster:                       b.cluster,
		kernelProvider:                b.kernelProvider,
		notifier:                      b.notifier,
		metricsProvider:               b.metricsProvider,
		opts:                          b.opts,
		kernelShellHandler:            b.kernelShellHandler,
		kernelCallbackProvider:        b.kernelCallbackProvider,
		kernelSpecs:                   hashmap.NewConcurrentMap[*proto.KernelSpec](32),
		waitGroups:                    hashmap.NewConcurrentMap[*RegistrationWaitGroups](32),
		kernelRegisteredNotifications: hashmap.NewCornelkMap[string, *proto.KernelRegistrationNotification](64),
	}

	config.InitLogger(&provisioner.log, provisioner)

	return provisioner
}
