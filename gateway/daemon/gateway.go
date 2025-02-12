package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
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

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	RequestLog *metrics.RequestLog

	log logger.Logger
}

// NewGateway creates a new Gateway struct and returns a pointer to it.
func NewGateway(manager *KernelManager) *Gateway {
	gateway := &Gateway{
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

	return g.forwardRequest(messaging.ControlMessage, msg)
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return g.forwardRequest(messaging.ShellMessage, msg)
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return g.forwardRequest(messaging.HBMessage, msg)
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return g.forwardRequest(messaging.HBMessage, msg)
}

// forwardRequest forwards the given message of the given type to the appropriate scheduling.Kernel.
func (g *Gateway) forwardRequest(socketType messaging.MessageType, msg *messaging.JupyterMessage) error {
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

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if msg.JupyterMessageType() == messaging.ShellExecuteReply {
		d.cleanUpBeforeForwardingExecuteReply(from, msg)
	}

	sendError := d.sendZmqMessage(msg, socket, from.ID())
	if sendError == nil {
		d.clusterStatisticsMutex.Lock()
		d.ClusterStatistics.NumJupyterRepliesSentByClusterGateway += 1
		d.clusterStatisticsMutex.Unlock()
	}

	return sendError
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
		// d.updateStatisticsFromShellExecuteReply(requestTrace)
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
