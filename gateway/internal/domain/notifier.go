package domain

import "github.com/scusemua/distributed-notebook/common/jupyter/messaging"

type Notifier interface {
	NotifyDashboard(name string, content string, typ messaging.NotificationType)

	// NotifyDashboardOfInfo is used to issue an "info" notification to the internalCluster Dashboard.
	NotifyDashboardOfInfo(name string, content string)

	// NotifyDashboardOfWarning is used to issue a "warning" notification to the internalCluster Dashboard.
	NotifyDashboardOfWarning(name string, content string)

	// NotifyDashboardOfError is used to issue an "error" notification to the internalCluster Dashboard.
	NotifyDashboardOfError(name string, content string)
}
