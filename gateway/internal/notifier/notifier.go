package notifier

import (
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"golang.org/x/net/context"
)

type DashboardNotifier struct {
	// clusterDashboard encapsulates a gRPC connection to the Dashboard.
	clusterDashboard proto.ClusterDashboardClient

	log logger.Logger
}

func NewDashboardNotifier(clusterDashboard proto.ClusterDashboardClient) *DashboardNotifier {
	return &DashboardNotifier{
		log:              config.GetLogger("DashboardNotifier "),
		clusterDashboard: clusterDashboard,
	}
}

func (n *DashboardNotifier) SetClusterDashboardClient(clusterDashboardClient proto.ClusterDashboardClient) {
	n.clusterDashboard = clusterDashboardClient
}

func (n *DashboardNotifier) NotificationCallback(name string, content string, typ messaging.NotificationType) {
	n.NotifyDashboard(name, content, typ)
}

func (n *DashboardNotifier) NotifyDashboard(name string, content string, typ messaging.NotificationType) {
	if n.clusterDashboard == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	_, err := n.clusterDashboard.SendNotification(ctx, &proto.Notification{
		Id:               uuid.NewString(),
		Title:            name,
		Message:          content,
		NotificationType: int32(typ),
	})

	if err != nil {
		n.log.Error("Failed to send notification to cluster Dashboard because: %s", err.Error())
		n.log.Error("Notification name: %s", name)
		n.log.Error("Notification message: %s", content)
		n.log.Error("Notification type: %s", typ.String())
	} else {
		n.log.Debug("Successfully sent \"%s\" (typ=%s (%n)) notification to internalCluster Dashboard.",
			name, typ.String(), typ.Int32())
	}
}

// SendErrorNotification sends an 'error' notification to the Cluster Dashboard in a separate goroutine.
func (n *DashboardNotifier) SendErrorNotification(errorName string, errorMessage string) {
	go n.NotifyDashboardOfError(errorName, errorMessage)
}

// SendInfoNotification sends an 'info' notification to the Cluster Dashboard in a separate goroutine.
func (n *DashboardNotifier) SendInfoNotification(title string, message string) {
	go n.NotifyDashboardOfInfo(title, message)
}

// NotifyDashboardOfInfo is used to issue an "info" notification to the internalCluster Dashboard.
func (n *DashboardNotifier) NotifyDashboardOfInfo(name string, content string) {
	n.NotifyDashboard(name, content, messaging.InfoNotification)
}

// NotifyDashboardOfWarning is used to issue a "warning" notification to the internalCluster Dashboard.
func (n *DashboardNotifier) NotifyDashboardOfWarning(name string, content string) {
	n.NotifyDashboard(name, content, messaging.WarningNotification)
}

// NotifyDashboardOfError is used to issue an "error" notification to the internalCluster Dashboard.
func (n *DashboardNotifier) NotifyDashboardOfError(name string, content string) {
	n.NotifyDashboard(name, content, messaging.ErrorNotification)
}
