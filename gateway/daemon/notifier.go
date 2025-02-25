package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"golang.org/x/net/context"
	"time"
)

type Notifier struct {
	// clusterDashboard encapsulates a gRPC connection to the Dashboard.
	clusterDashboard proto.ClusterDashboardClient

	log logger.Logger
}

func NewNotifier(clusterDashboard proto.ClusterDashboardClient) *Notifier {
	return &Notifier{
		clusterDashboard: clusterDashboard,
		log:              config.GetLogger("Notifier "),
	}
}

func (n *Notifier) NotifyDashboard(notificationName string, notificationMessage string, typ messaging.NotificationType) {
	if n.clusterDashboard != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
		defer cancel()

		_, err := n.clusterDashboard.SendNotification(ctx, &proto.Notification{
			Id:               uuid.NewString(),
			Title:            notificationName,
			Message:          notificationMessage,
			NotificationType: int32(typ),
		})

		if err != nil {
			n.log.Error("Failed to send notification to Cluster Dashboard because: %s", err.Error())
			n.log.Error("Notification name: %s", notificationName)
			n.log.Error("Notification message: %s", notificationMessage)
			n.log.Error("Notification type: %s", typ.String())
		} else {
			n.log.Debug("Successfully sent \"%s\" (typ=%s (%n)) notification to internalCluster Dashboard.",
				notificationName, typ.String(), typ.Int32())
		}
	}
}

// NotifyDashboardOfInfo is used to issue an "info" notification to the internalCluster Dashboard.
func (n *Notifier) NotifyDashboardOfInfo(name string, content string) {
	n.NotifyDashboard(name, content, messaging.InfoNotification)
}

// NotifyDashboardOfWarning is used to issue a "warning" notification to the internalCluster Dashboard.
func (n *Notifier) NotifyDashboardOfWarning(name string, content string) {
	n.NotifyDashboard(name, content, messaging.WarningNotification)
}

// NotifyDashboardOfError is used to issue an "error" notification to the internalCluster Dashboard.
func (n *Notifier) NotifyDashboardOfError(name string, content string) {
	n.NotifyDashboard(name, content, messaging.ErrorNotification)
}
