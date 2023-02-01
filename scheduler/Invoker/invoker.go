package invoker

import (
	"context"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type KernelInvoker interface {
	// InvokeWithContext starts a kernel with the given context.
	InvokeWithContext(context.Context, *gateway.KernelSpec) (*jupyter.ConnectionInfo, error)

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
}
