package core

import (
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

// API defines the interface of messages that a JupyterRouter can intercept and handle.
type KernelMessageHandler func(router.RouterInfo, *zmq4.Msg) error

type KernelInfo interface {
	fmt.Stringer

	// ID returns kernel id.
	ID() string
}

// Kernel defines the interface for a jupyter kernel.
type Kernel interface {
	KernelInfo
	types.Contextable

	// ConnectionInfo returns the connection info of the kernel.
	ConnectionInfo() *jupyter.ConnectionInfo

	// Status returns the kernel status.
	// Including simulator features:
	// 	entity.Container.IsTraining()
	//  entity.Container.IsRescheduled()
	Status() jupyter.KernelStatus

	// Validate validates the kernel connections.
	// Including simulator features:
	// 	entity.Container.Start(), Start() will be implemented outside kernel abstraction. Validate() ensures the kernel is started.
	Validate() error

	// InitializeShellForwarder initializes the shell forwarder.
	InitializeShellForwarder(handler jupyter.MessageHandler) (*jupyter.Socket, error)

	// InitializeIOForwarder initializes the io forwarder.
	InitializeIOForwarder() (*jupyter.Socket, error)

	// RequestWithHandler sends a request and handles the response.
	// Includes simulator features:
	// 	entity.Container.Preprocess()
	// 	entity.Container.Train()
	// 	entity.Container.StopTrain()
	// 	entity.Container.Suspend()
	// 	entity.Container.Resume()
	RequestWithHandler(typ jupyter.MessageType, msg *zmq4.Msg, handler KernelMessageHandler) error

	// Close cleans up kernel resource.
	// Including simulator features:
	// 	entity.Container.Stop(), Stop() will be implemented outside kernel abstraction. Close() cleans up the kernel resource after kernel stopped.
	Close() error
}
