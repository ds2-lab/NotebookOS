package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
)

const (
	NoKernelId = "<N/A>"
)

var (
	ErrInvalidJupyterSessionId = errors.New("message did not contain valid session id")
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
