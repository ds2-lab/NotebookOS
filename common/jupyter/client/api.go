package client

import (
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	ErrHandlerNotImplemented = fmt.Errorf("handler not implemented")
)

// API defines the interface of messages that a JupyterRouter can intercept and handle.
type MessageHandler func(ClientInfo, *zmq4.Msg) error

// Router defines the interface to provider infos of a JupyterRouter.
type ClientInfo interface {
	Socket(types.MessageType) *types.Socket
}
