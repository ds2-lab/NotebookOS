package invoker

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"time"

	"github.com/scusemua/distributed-notebook/common/jupyter"
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

	// KernelCreatedAt returns the time at which the KernelInvoker created the kernel.
	KernelCreatedAt() (time.Time, bool)

	// KernelCreated returns a bool indicating whether kernel the container has been created.
	KernelCreated() bool

	// TimeSinceKernelCreated returns the amount of time that has elapsed since the KernelInvoker created the kernel.
	TimeSinceKernelCreated() (time.Duration, bool)

	WorkloadId() string

	// SetWorkloadId will panic if the CurrentContainerType of the target KernelInvoker is scheduling.StandardContainer.
	//
	// You can only mutate the WorkloadId field of a KernelInvoker struct if the CurrentContainerType of the target
	// KernelInvoker struct is scheduling.PrewarmContainer.
	SetWorkloadId(string)

	AssignedGpuDeviceIds() []int32

	// SetAssignedGpuDeviceIds will panic if the CurrentContainerType of the target KernelInvoker is
	// scheduling.StandardContainer.
	//
	// You can only mutate the AssignedGpuDeviceIds field of a KernelInvoker struct if the CurrentContainerType of the
	// target KernelInvoker struct is scheduling.PrewarmContainer.
	SetAssignedGpuDeviceIds([]int32)

	DebugPort() int

	// SetDebugPort will panic if the CurrentContainerType of the target KernelInvoker is scheduling.StandardContainer.
	//
	// You can only mutate the DebugPort field of a KernelInvoker struct if the CurrentContainerType of the target
	// KernelInvoker struct is scheduling.PrewarmContainer.
	SetDebugPort(int)

	KernelId() string

	// SetKernelId will panic if the CurrentContainerType of the target KernelInvoker is scheduling.StandardContainer.
	//
	// You can only mutate the KernelId field of a KernelInvoker struct if the CurrentContainerType of the target
	// KernelInvoker struct is scheduling.PrewarmContainer.
	SetKernelId(string)

	// CurrentContainerType is the current scheduling.ContainerType of the container created by the target
	// KernelInvoker.
	CurrentContainerType() scheduling.ContainerType

	// OriginalContainerType is the original scheduling.ContainerType of the container created by the target
	// DockerInvoker.
	//
	// OriginalContainerType can be used to determine if the container created by the target KernelInvoker was
	// originally a scheduling.PrewarmContainer that has since been promoted to a scheduling.StandardContainer.
	OriginalContainerType() scheduling.ContainerType

	// PromotePrewarmedContainer records within the target KernelInvoker that its container, which must originally have
	// been a scheduling.PrewarmContainer, is now a scheduling.StandardContainer.
	//
	// If the promotion is successful, then PromotePrewarmedContainer returns true.
	//
	// If the OriginalContainerType of the target KernelInvoker is KernelInvoker,
	// then PromotePrewarmedContainer returns false.
	PromotePrewarmedContainer() bool

	// ContainerIsPrewarm returns true if the CurrentContainerType of the target KernelInvoker is
	// scheduling.PrewarmContainer.
	ContainerIsPrewarm() bool
}
