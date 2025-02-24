package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
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

type MessageForwarder interface {
}

type KernelManager interface {
}

type GatewayDaemonBuilder struct {
	notifier      Notifier
	forwarder     MessageForwarder
	kernelManager KernelManager
	cluster       scheduling.Cluster
}

func (b *GatewayDaemonBuilder) WithNotifier(notifier Notifier) *GatewayDaemonBuilder {
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
	}

	config.InitLogger(&gatewayDaemon.log, gatewayDaemon)

	return gatewayDaemon
}

type GatewayDaemon struct {
	notifier      Notifier
	forwarder     MessageForwarder
	kernelManager KernelManager
	cluster       scheduling.Cluster

	log logger.Logger
}
