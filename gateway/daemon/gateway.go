package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
)

// The Gateway is responsible for forwarding messages received from the Jupyter Server to the appropriate
// scheduling.Kernel (and subsequently any scheduling.KernelReplica instances associated with the scheduling.Kernel).
type Gateway struct {
	// Router is the underlying router.Router that listens for messages from the Jupyter Server.
	//
	// The Router uses the ControlHandler, ShellHandler, StdinHandler, and HBHandler methods of the Gateway
	// to forward the messages that it receives to the appropriate/target scheduling.Kernel.
	Router *router.Router

	log logger.Logger
}

func NewGateway() *Gateway {
	gateway := &Gateway{}

	config.InitLogger(&gateway.log, gateway)

	return gateway
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *Gateway) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}
