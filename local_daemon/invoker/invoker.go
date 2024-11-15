package invoker

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/proto"
	"time"

	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/types"
)

type StatucChangedHandler func(old jupyter.KernelStatus, new jupyter.KernelStatus)

type KernelInvoker interface {
	// InvokeWithContext starts a kernel with the given context.
	InvokeWithContext(context.Context, *proto.KernelReplicaSpec) (*jupyter.ConnectionInfo, error)

	// Status returns the status of the kernel.
	Status() (jupyter.KernelStatus, error)

	// Shutdown stops the kernel gracefully.
	Shutdown() error

	// Close stops the kernel immediately.
	Close() error

	// Wait waits for the kernel to exit.
	Wait() (jupyter.KernelStatus, error)

	// Expired returns true if the kernel has been stopped before the given timeout.
	// If the Wait() has been called, the kernel is considered expired.
	Expired(timeout time.Duration) bool

	// OnStatusChanged registers a callback function to be called when the kernel status changes.
	// The callback function is invocation specific and will be cleared after the kernel exits.
	OnStatusChanged(StatucChangedHandler)

	// GetReplicaAddress returns the address of the replica.
	GetReplicaAddress(spec *proto.KernelSpec, replicaId int32) string

	// KernelCreatedAt returns the time at which the KernelInvoker created the kernel.
	KernelCreatedAt() (time.Time, bool)

	// KernelCreated returns a bool indicating whether kernel the container has been created.
	KernelCreated() bool

	// TimeSinceKernelCreated returns the amount of time that has elapsed since the KernelInvoker created the kernel.
	TimeSinceKernelCreated() (time.Duration, bool)
}
