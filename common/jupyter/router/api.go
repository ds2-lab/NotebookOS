package router

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

// API defines the interface of messages that a JupyterRouter can intercept and handle.
type RouterMessageHandler func(RouterInfo, *types.JupyterMessage) error

type RouterInfo interface {
	types.JupyterServerInfo
}

// RouterProvider defines the interface to provide handlers for a JupyterRouter.
type RouterProvider interface {
	ControlHandler(RouterInfo, *types.JupyterMessage) error

	ShellHandler(RouterInfo, *types.JupyterMessage) error

	StdinHandler(RouterInfo, *types.JupyterMessage) error

	HBHandler(RouterInfo, *types.JupyterMessage) error
}
