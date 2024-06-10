package router

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

// API defines the interface of messages that a JupyterRouter can intercept and handle.
type RouterMessageHandler func(RouterInfo, [][]byte) error

type RouterInfo interface {
	types.JupyterServerInfo
}

// RouterProvider defines the interface to provide handlers for a JupyterRouter.
type RouterProvider interface {
	ControlHandler(RouterInfo, [][]byte) error

	ShellHandler(RouterInfo, [][]byte) error

	StdinHandler(RouterInfo, [][]byte) error

	HBHandler(RouterInfo, [][]byte) error
}
