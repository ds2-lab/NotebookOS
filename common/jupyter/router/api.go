package router

import (
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	ErrStopPropagation = fmt.Errorf("stop propagation")
)

// API defines the interface of messages that a JupyterRouter can intercept and handle.
type RouterMessageHandler func(RouterInfo, *zmq4.Msg) error

type RouterInfo interface {
	types.JupyterServerInfo
}

// RouterProvider defines the interface to provide handlers for a JupyterRouter.
type RouterProvider interface {
	ControlHandler(RouterInfo, *zmq4.Msg) error

	ShellHandler(RouterInfo, *zmq4.Msg) error

	StdinHandler(RouterInfo, *zmq4.Msg) error

	HBHandler(RouterInfo, *zmq4.Msg) error
}
