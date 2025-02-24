package daemon

import (
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type Notifier interface {
	NotifyDashboard(name string, content string, typ messaging.NotificationType)

	// Used to issue an "info" notification to the internalCluster Dashboard.
	NotifyDashboardOfInfo(name string, content string)

	// Used to issue a "warning" notification to the internalCluster Dashboard.
	NotifyDashboardOfWarning(name string, content string)

	// Used to issue an "error" notification to the internalCluster Dashboard.
	NotifyDashboardOfError(name string, content string)
}

type MessageForwarder interface {
}

type KernelManager interface {
}

type GatewayDaemon struct {
	notifier      Notifier
	forwarder     MessageForwarder
	kernelManager KernelManager
	cluster       scheduling.Cluster
}
