package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
)

// KernelManager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
// scheduling.KernelReplica instances running within the cluster.
type KernelManager struct {
	log logger.Logger
}

func NewKernelManager() *KernelManager {
	manager := &KernelManager{}

	config.InitLogger(&manager.log, manager)

	return manager
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *KernelManager) ControlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *KernelManager) ShellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	g.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *KernelManager) StdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (g *KernelManager) HBHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}
