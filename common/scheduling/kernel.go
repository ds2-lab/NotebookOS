package scheduling

import (
	"context"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

// KernelMessageHandler is an API defines the interface of messages that a JupyterRouter can intercept and handle.
type KernelMessageHandler func(KernelInfo, jupyter.MessageType, *jupyter.JupyterMessage) error

type KernelInfo interface {
	// RouterInfo provides kernel specific routing information.
	router.RouterInfo

	// ID returns kernel id.
	ID() string

	// ResourceSpec returns resource spec, which defines the resource requirements of the kernel.
	ResourceSpec() *gateway.ResourceSpec

	// KernelSpec returns kernel spec.
	KernelSpec() *gateway.KernelSpec
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
	InitializeShellForwarder(handler KernelMessageHandler) (*jupyter.Socket, error)

	// InitializeIOForwarder initializes the io forwarder.
	InitializeIOForwarder() (*jupyter.Socket, error)

	// RequestWithHandler sends a request and handles the response.
	// Includes simulator features:
	// 	entity.Container.Preprocess()
	// 	entity.Container.Train()
	// 	entity.Container.StopTrain()
	// 	entity.Container.Suspend()
	// 	entity.Container.Resume()
	RequestWithHandler(ctx context.Context, prompt string, typ jupyter.MessageType, msg *jupyter.JupyterMessage, handler KernelMessageHandler, done func()) error

	// Close cleans up kernel resource.
	// Including simulator features:
	// 	entity.Container.Stop(), Stop() will be implemented outside kernel abstraction. Close() cleans up the kernel resource after kernel stopped.
	Close() error
}

// KernelReplica defines the interface for a jupyter kernel replica.
type KernelReplica interface {
	Kernel

	// ReplicaID returns the replica id.
	ReplicaID() int32

	// PodName returns the name of the Kubernetes Pod hosting the replica.
	PodName() string

	// NodeName returns the name of the node that the Pod is running on.
	NodeName() string

	// InitializeIOSub initializes the io subscriber of the replica with customized handler.
	InitializeIOSub(handler jupyter.MessageHandler, subscriptionTopic string) (*jupyter.Socket, error)

	// IsReady returns true if the replica has registered and joined its SMR cluster.
	// Only used by the Cluster Gateway, not by the Local Daemon.
	IsReady() bool

	// SetReady designates the replica as ready.
	// Only used by the Cluster Gateway, not by the Local Daemon.
	SetReady()

	// GetHost returns the Host on which the replica is hosted.
	GetHost() Host

	// SetHost sets the Host of the kernel.
	SetHost(Host)
}
