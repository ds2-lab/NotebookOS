package routing

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"strings"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"golang.org/x/net/context"
)

const (
	NoKernelId = "<N/A>"
)

var (
	ErrInvalidJupyterSessionId = errors.New("message did not contain valid session id")
	ErrSocketUnavailable       = errors.New("socket is nil or otherwise unavailable")
)

// KernelForwarder receives a message from a Forwarder and forwards it to the specific, target kernel.
type KernelForwarder interface {
	// ForwardRequestToKernel forwards the given *messaging.JupyterMessage to the specified scheduling.Kernel using the
	// specified socket type (i.e., messaging.MessageType).
	ForwardRequestToKernel(id string, msg *messaging.JupyterMessage, socketTyp messaging.MessageType) error
}

// The Forwarder is responsible for forwarding messages received from the Jupyter Server to the appropriate
// scheduling.Kernel (and subsequently any scheduling.KernelReplica instances associated with the scheduling.Kernel).
type Forwarder struct {
	// GatewayId is the unique identifier of the Forwarder.
	GatewayId string

	// Router is the underlying router.Router that listens for messages from the Jupyter Server.
	//
	// The Router uses the ControlHandler, ShellHandler, StdinHandler, and HBHandler methods of the Forwarder
	// to forward the messages that it receives to the appropriate/target scheduling.Kernel.
	Router *router.Router

	// KernelManager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
	// scheduling.KernelReplica instances running within the cluster.
	KernelManager KernelForwarder

	// DebugMode indicates that the cluster is running in "debug" mode and extra time may be spent
	// recording metrics and embedding additional metadata in messages.
	DebugMode bool

	// MetricsProvider provides all metrics to the members of the scheduling package.
	MetricsProvider *metrics.ClusterMetricsProvider

	Notifier domain.Notifier

	// RequestLog is used to track the status/progress of requests when in DebugMode.
	RequestLog *metrics.RequestLog

	// ClusterStatistics encapsulates a number of statistics/metrics.
	ClusterStatistics *metrics.ClusterStatistics

	connectionOptions *jupyter.ConnectionInfo

	log logger.Logger
}

// NewForwarder creates a new Forwarder struct and returns a pointer to it.
func NewForwarder(connectionOptions *jupyter.ConnectionInfo, kernelForwarder KernelForwarder, notifier domain.Notifier,
	opts *domain.ClusterGatewayOptions) *Forwarder {

	forwarder := &Forwarder{
		GatewayId:         uuid.NewString(),
		connectionOptions: connectionOptions,
		KernelManager:     kernelForwarder,
		Notifier:          notifier,
	}

	config.InitLogger(&forwarder.log, forwarder)

	if opts.DebugMode {
		forwarder.log.Debug("Running in DebugMode.")
		forwarder.RequestLog = metrics.NewRequestLog()
	} else {
		forwarder.log.Debug("Not running in DebugMode.")
	}

	forwarder.Router = router.New(context.Background(), "ClusterGateway", connectionOptions, forwarder,
		opts.MessageAcknowledgementsEnabled, "ClusterGatewayRouter", false,
		metrics.ClusterGateway, forwarder.DebugMode, forwarder.MetricsProvider.GetGatewayPrometheusManager())

	return forwarder
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (f *Forwarder) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	f.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return f.ForwardRequest(messaging.ControlMessage, msg)
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (f *Forwarder) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	f.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return f.ForwardRequest(messaging.ShellMessage, msg)
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (f *Forwarder) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return f.ForwardRequest(messaging.HBMessage, msg)
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (f *Forwarder) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	return f.ForwardRequest(messaging.HBMessage, msg)
}

// ForwardRequest forwards the given message of the given type to the appropriate scheduling.Kernel.
func (f *Forwarder) ForwardRequest(socketType messaging.MessageType, msg *messaging.JupyterMessage) error {
	kernelId, msgType, err := f.extractRequestMetadata(msg)
	if err != nil {
		f.log.Error("Metadata Extraction Error: %v", err)
		return err
	}

	f.log.Debug("Forwarding[SocketType=%v, MsgId='%s', MsgTyp='%s', TargetKernelId='%s']",
		socketType.String(), msg.JupyterMessageId(), msgType, kernelId)

	return f.KernelManager.ForwardRequestToKernel(kernelId, msg, socketType)
}

// ForwardResponse forwards a response from a scheduling.Kernel / scheduling.KernelReplica back to the Jupyter client.
func (f *Forwarder) ForwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	// Validate argument.
	if msg == nil {
		panic("Message cannot be nil")
	}

	// Attempt to retrieve socket with which the message/response will be sent.
	socket := from.Socket(typ)
	if socket == nil {
		// Use the router's socket.
		socket = f.Router.Socket(typ)
	}

	// Couldn't resolve the socket...
	if socket == nil {
		f.log.Error("Socket Error: %v socket is unavailable; cannot forward \"%s\" message \"%s\".",
			typ, msg.JupyterMessageType(), msg.JupyterMessageId())
		return ErrSocketUnavailable
	}

	if f.DebugMode {
		_ = f.updateRequestLog(msg, typ) // Ignore the error... we already log an error message in updateRequestLog.
	}

	f.log.Debug(
		utils.LightBlueStyle.Render(
			"Forward Response [SocketType='%v', MsgType=\"%s\", MsgId=\"%s\", KernelId=\"%s\"]"),
		typ, msg.JupyterMessageType(), msg.JupyterMessageId(), from.ID())

	err := f.sendZmqMessage(msg, socket, from.ID())

	if err == nil {
		f.MetricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.NumJupyterRepliesSentByClusterGateway += 1
		})
	}

	return err // Will be nil on success.
}

// sendZmqMessage sends the specified *messaging.JupyterMessage on/using the specified *messaging.Socket.
func (f *Forwarder) sendZmqMessage(msg *messaging.JupyterMessage, socket *messaging.Socket, senderId string) error {
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

		f.log.Warn(style.Render("Sending %s \"%s\" response \"%s\" (JupyterID=\"%s\") from kernel %s took %v."),
			socket.Type.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), senderId, sendDuration)
	}

	if err != nil {
		f.log.Error(utils.RedStyle.Render("ZMQ Send Error [SocketType='%v', MsgId='%s', MsgTyp='%s', SenderID='%s']: %v"),
			socket.Type, msg.JupyterMessageId(), msg.JupyterMessageType(), senderId, err)
		return err
	}

	// Update prometheus metrics, if enabled and available.
	if f.MetricsProvider != nil && f.MetricsProvider.PrometheusMetricsEnabled() {
		metricError := f.MetricsProvider.
			GetGatewayPrometheusManager().
			SentMessage(f.GatewayId, sendDuration, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType())

		if metricError != nil {
			f.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}

		metricError = f.MetricsProvider.
			GetGatewayPrometheusManager().
			SentMessageUnique(f.GatewayId, metrics.ClusterGateway, socket.Type, msg.JupyterMessageType())

		if metricError != nil {
			f.log.Warn("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
		}
	}

	f.log.Debug("Sent message [SocketType='%v', MsgId='%s', MsgTyp='%s', SenderID='%s', SendDuration=%v]:\n%s",
		socket.Type, msg.JupyterMessageId(), msg.JupyterMessageType(), senderId, sendDuration, msg.JupyterFrames.StringFormatted())

	return nil
}

// updateRequestLog updates the RequestLog contained within the given messaging.JupyterMessage's buffers/metadata.
//
// If the Forwarder is not configured to run in DebugMode, then updateRequestLog is a no-op and returns immediately.
func (f *Forwarder) updateRequestLog(msg *messaging.JupyterMessage, typ messaging.MessageType) error {
	if !f.DebugMode {
		return nil
	}

	requestTrace, _, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(msg, time.Now(), f.log)
	if err != nil {
		f.log.Error("Failed to update RequestTrace: %v", err)
		return err
	}

	// If we added a RequestTrace for the first time, then let's also add an entry to our RequestLog.
	err = f.RequestLog.AddEntry(msg, typ, requestTrace)
	if err != nil {
		f.log.Error("Failed to update RequestTrace: %v", err)
		return err
	}

	return nil
}

// extractRequestMetadata extracts the kernel (or Jupyter session) ID and the message type from the given ZMQ message.
func (f *Forwarder) extractRequestMetadata(msg *messaging.JupyterMessage) (string, string, error) {
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
			f.log.Error("Invalid Jupyter Session ID:\n%v", msg.JupyterFrames.StringFormatted())

			return NoKernelId, msgType, ErrInvalidJupyterSessionId
		}
	}

	return kernelOrSessionId, msgType, nil
}

// SendErrorResponse is used to respond to a shell message immediately, before we've routed it to any local
// schedulers or kernel replicas, because we encountered an unrecoverable error while (pre)processing the message.
func (f *Forwarder) SendErrorResponse(kernel scheduling.Kernel, request *messaging.JupyterMessage, errContent error, typ messaging.MessageType) error {
	if kernel != nil {
		f.log.Warn("Sending error response to shell \"%s\" message \"%s\" targeting kernel \"%s\": %v",
			request.JupyterMessageType(), request.JupyterMessageId(), kernel.ID(), errContent)
	} else {
		f.log.Warn("Sending error response to shell \"%s\" message \"%s\" targeting unknown kernel: %v",
			request.JupyterMessageType(), request.JupyterMessageId(), errContent)
	}

	// First, update the header to be a "_reply" message type.
	header, err := request.GetHeader()
	if err != nil {
		f.log.Error("Failed to extract header from shell \"%s\" message \"%s\": %v",
			request.JupyterMessageType(), request.JupyterMessageId(), err)
		go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Extract Header from Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	err = request.JupyterFrames.EncodeParentHeader(&header)
	if err != nil {
		go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Encode Parent Header for Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	requestType := request.JupyterMessageType()
	replyType := fmt.Sprintf("%s_reply", requestType[0:strings.Index(requestType, "_request")])
	_ = request.SetMessageType(messaging.JupyterMessageType(replyType), false)
	_ = request.SetMessageId(fmt.Sprintf("%s_1", request.JupyterMessageId()), false)
	_ = request.SetDate(time.Now().Format(time.RFC3339Nano), false)

	// Re-encode the header.
	header, err = request.GetHeader()
	if err != nil {
		go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Get Header from Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	err = request.EncodeMessageHeader(header)
	if err != nil {
		go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Re-Encode Header of Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	// Second, embed the error in the response content.
	errorContent := messaging.MessageError{
		Status:   messaging.MessageStatusError,
		ErrName:  fmt.Sprintf("Failed to Handle \"%s\" Message", requestType),
		ErrValue: errContent.Error(),
	}
	err = request.JupyterFrames.EncodeContent(&errorContent)
	if err != nil {
		go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Encode Error Content of Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
			request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
		return err
	}

	// Regenerate the signature. Don't include the buffer frames as part of the signature.
	if kernel != nil && kernel.ConnectionInfo().SignatureScheme != "" && kernel.ConnectionInfo().Key != "" {
		_, err = request.JupyterFrames.Sign(kernel.ConnectionInfo().SignatureScheme, []byte(kernel.ConnectionInfo().Key))
		if err != nil {
			go f.Notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Sign Response to Shell \"%s\" Message \"%s\" While Sending Shell Error Response",
				request.JupyterMessageType(), request.JupyterMessageId()), err.Error())
			return err
		}
	}

	if typ == messaging.ShellMessage && requestType == messaging.ShellExecuteRequest {
		request.IsFailedExecuteRequest = true
	}

	// Finally, send the message back to the Jupyter client.
	return f.ForwardResponse(kernel /* may be nil */, typ, request)
}
