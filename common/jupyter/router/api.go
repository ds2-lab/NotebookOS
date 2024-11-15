package router

import "github.com/scusemua/distributed-notebook/common/jupyter/messaging"

// MessageHandler defines the interface of messages that a JupyterRouter can intercept and handle.
type MessageHandler func(Info, *messaging.JupyterMessage) error

type Info interface {
	messaging.JupyterServerInfo
}

// Provider defines the interface to provide handlers for a JupyterRouter.
type Provider interface {
	ControlHandler(Info, *messaging.JupyterMessage) error

	ShellHandler(Info, *messaging.JupyterMessage) error

	StdinHandler(Info, *messaging.JupyterMessage) error

	HBHandler(Info, *messaging.JupyterMessage) error
}
