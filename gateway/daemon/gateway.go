package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/utils"
	"time"
)

const (
	NoKernelId = "<N/A>"
)

var (
	ErrInvalidJupyterSessionId = errors.New("message did not contain valid session id")
	ErrSocketUnavailable       = errors.New("socket is nil or otherwise unavailable")
)

// The Gateway is responsible for forwarding messages received from the Jupyter Server to the appropriate
// scheduling.Kernel (and subsequently any scheduling.KernelReplica instances associated with the scheduling.Kernel).
type Gateway struct {
	// GatewayId is the unique identifier of the Gateway.
	GatewayId string

	// Router is the underlying router.Router that listens for messages from the Jupyter Server.
	//
	// The Router uses the ControlHandler, ShellHandler, StdinHandler, and HBHandler methods of the Gateway
	// to forward the messages that it receives to the appropriate/target scheduling.Kernel.
	Router *router.Router

	// KernelManager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
	// scheduling.KernelReplica instances running within the cluster.
	KernelManager *KernelManager

	// DebugMode indicates that the cluster is running in "debug" mode and extra time may be spent
	// recording metrics and embedding additional metadata in messages.
	DebugMode bool

	// MetricsProvider provides all metrics to the members of the scheduling package.
	MetricsProvider *metrics.ClusterMetricsProvider

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	RequestLog *metrics.RequestLog

	// ClusterStatistics encapsulates a number of statistics/metrics.
	ClusterStatistics *metrics.ClusterStatistics

	log logger.Logger
}

// NewGateway creates a new Gateway struct and returns a pointer to it.
func NewGateway(manager *KernelManager) *Gateway {
	gateway := &Gateway{
		GatewayId:     uuid.NewString(),
		KernelManager: manager,
	}

	config.InitLogger(&gateway.log, gateway)

	return gateway
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return g.ForwardRequest(messaging.ControlMessage, msg)
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return g.ForwardRequest(messaging.ShellMessage, msg)
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return g.ForwardRequest(messaging.HBMessage, msg)
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return g.ForwardRequest(messaging.HBMessage, msg)
}

// ForwardRequest forwards the given message of the given type to the appropriate scheduling.Kernel.
func (g *Gateway) ForwardRequest(socketType messaging.MessageType, msg *messaging.JupyterMessage) error {
	kernelId, msgType, err := g.extractRequestMetadata(msg)
	if err != nil {
		g.log.Error("Metadata Extraction Error: %v", err)
		return err
	}

	g.log.Debug("Forwarding[SocketType=%v, MsgId='%s', MsgTyp='%s', TargetKernelId='%s']",
		socketType.String(), msg.JupyterMessageId(), msgType, kernelId)

	return g.KernelManager.ForwardRequestToKernel(kernelId, msg, socketType)
}

// ForwardResponse forwards a response from a scheduling.Kernel / scheduling.KernelReplica back to the Jupyter client.
func (g *Gateway) ForwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	// Validate argument.
	if msg == nil {
		panic("Message cannot be nil")
	}

	// Attempt to retrieve socket with which the message/response will be sent.
	socket := from.Socket(typ)
	if socket == nil {
		// Use the router's socket.
		socket = g.Router.Socket(typ)
	}

	// Couldn't resolve the socket...
	if socket == nil {
		g.log.Error("Socket Error: %v socket is unavailable; cannot forward \"%s\" message \"%s\".",
			typ, msg.JupyterMessageType(), msg.JupyterMessageId())
		return ErrSocketUnavailable
	}

	if g.DebugMode {
		_ = g.updateRequestLog(msg, typ) // Ignore the error... we already log an error message in updateRequestLog.
	}

	g.log.Debug(
		utils.LightBlueStyle.Render(
			"Forward Response [SocketType='%v', MsgType=\"%s\", MsgId=\"%s\", KernelId=\"%s\"]"),
		typ, msg.JupyterMessageType(), msg.JupyterMessageId(), from.ID())

	err := g.sendZmqMessage(msg, socket, from.ID())

	// TODO: Implement this.
	//if err == nil {
	//	g.clusterStatisticsMutex.Lock()
	//	g.ClusterStatistics.NumJupyterRepliesSentByClusterGateway += 1
	//	g.clusterStatisticsMutex.Unlock()
	//}

	return err // Will be nil on success.
}

// sendZmqMessage sends the specified *messaging.JupyterMessage on/using the specified *messaging.Socket.
func (g *Gateway) sendZmqMessage(msg *messaging.JupyterMessage, socket *messaging.Socket, senderId string) error {
	zmqMsg := *msg.GetZmqMsg()
	sendStart := time.Now()
	err := socket.Send(zmqMsg)
	sendDuration := time.Since(sendStart)

	// Display a warning if the send operation took a while.
	if sendDuration >= time.Millisecond*50 {
		style := utils.YellowStyle

		// If it took over 100ms, then we'll use orange-colored text instead of yellow.
		if sendDuration >= time.Millisecond*100 {
			style = utils.OrangeStyle
		}

		g.log.Warn(style.Render("Sending %s \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s took %v."),
			socket.Type.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, sendDuration)
	}

	if err != nil {
		g.log.Error(utils.RedStyle.Render("ZMQ Send Error [SocketType='%v', MsgId='%s', MsgTyp='%s', SenderID='%s']: %v"),
			socket.Type, msg.JupyterMessageId(), msg.JupyterMessageType(), senderId, err)
		return err
	}

	// Update prometheus metrics, if enabled and available.
	if g.MetricsProvider != nil && g.MetricsProvider.PrometheusMetricsEnabled() {
		metricError := g.MetricsProvider.
			GetGatewayPrometheusManager().
			SentMessage(g.GatewayId, sendDuration, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType())

		if metricError != nil {
			g.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}

		metricError = g.MetricsProvider.
			GetGatewayPrometheusManager().
			SentMessageUnique(g.GatewayId, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType())

		if metricError != nil {
			g.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}
	}

	g.log.Debug("Sent message [SocketType='%v', MsgId='%s', MsgTyp='%s', SenderID='%s', SendDuration=%v]:\n%s",
		socket.Type, msg.JupyterMessageId(), msg.JupyterMessageType(), senderId, sendDuration, msg.JupyterFrames.StringFormatted())

	return nil
}

// updateRequestLog updates the RequestLog contained within the given messaging.JupyterMessage's buffers/metadata.
//
// If the Gateway is not configured to run in DebugMode, then updateRequestLog is a no-op and returns immediately.
func (g *Gateway) updateRequestLog(msg *messaging.JupyterMessage, typ messaging.MessageType) error {
	if !g.DebugMode {
		return nil
	}

	requestTrace, _, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(msg, time.Now(), g.log)
	if err != nil {
		g.log.Error("Failed to update RequestTrace: %v", err)
		return err
	}

	// If we added a RequestTrace for the first time, then let's also add an entry to our RequestLog.
	err = g.RequestLog.AddEntry(msg, typ, requestTrace)
	if err != nil {
		g.log.Error("Failed to update RequestTrace: %v", err)
		return err
	}

	// Extract the data from the RequestTrace.
	if typ == messaging.ShellMessage {
		panic("TODO: Implement ClusterGatewayImpl::updateStatisticsFromShellExecuteReply")
		// g.updateStatisticsFromShellExecuteReply(requestTrace)
	}

	return nil
}

// extractRequestMetadata extracts the kernel (or Jupyter session) ID and the message type from the given ZMQ message.
func (g *Gateway) extractRequestMetadata(msg *messaging.JupyterMessage) (string, string, error) {
	// This is initially the kernel's ID, which is the DestID field of the message.
	// But we may not have set a destination ID field within the message yet.
	// In this case, we'll fall back to the session ID within the message's Jupyter header.
	// This may not work either, though, if that session has not been bound to the kernel yet.
	//
	// When Jupyter clients connect for the first time, they send both a shell and a control "kernel_info_request" message.
	// This message is used to bind the session to the kernel (specifically the shell message).
	kernelOrSessionId := msg.DestinationId
	msgType := msg.JupyterMessageType()

	// If there is no destination ID, then we'll try to use the session ID in the message's header instead.
	if len(kernelOrSessionId) == 0 {
		kernelOrSessionId = msg.JupyterSession()

		// Sanity check.
		// Make sure we got a valid session ID out of the Jupyter message header.
		// If we didn't, then we'll return an error.
		if len(kernelOrSessionId) == 0 {
			g.log.Error("Invalid Jupyter Session ID:\n%v", msg.JupyterFrames.StringFormatted())

			return NoKernelId, msgType, ErrInvalidJupyterSessionId
		}
	}

	return kernelOrSessionId, msgType, nil
}
