package resource

import "github.com/shopspring/decimal"

// ManagerState defines a public interface for accessing (i.e., reading) but not mutating (i.e., writing)
// the current state of a ManagerState.
//
// ManagerState wraps several ComputeResourceState instances -- one for ComputeResource of each of the following types:
// idle, pending, committed, and spec. As such, ManagerState exposes a collection of several ComputeResourceState
// instances to provide a convenient type for reading all the relevant state of a AllocationManager.
type ManagerState interface {
	// IdleResources returns the idle ComputeResource managed by a AllocationManager.
	// Idle ComputeResource can overlap with pending ComputeResource. These are ComputeResource that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources() ComputeResourceState

	// PendingResources returns the pending ComputeResource managed by a AllocationManager.
	// Pending ComputeResource are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending ComputeResource indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending ComputeResource on a node is the amount of ComputeResource that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources() ComputeResourceState

	// CommittedResources returns the committed ComputeResource managed by a AllocationManager.
	// These are ComputeResource that are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources() ComputeResourceState

	// SpecResources returns the spec ComputeResource managed by a AllocationManager.
	// These are the total allocatable ComputeResource available on the Host.
	// Spec ComputeResource are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources() ComputeResourceState

	// String returns a string representation of the ManagerState suitable for logging.
	String() string
}

// ComputeResourceState defines a public interface for getting (i.e., reading) but not mutating (i.e., writing)
// the current state of a AllocationManager.
//
// ComputeResourceState encapsulates the ComputeResource for a single type of resource (i.e., idle, pending, committed, or spec).
// Meanwhile, ManagerState exposes a collection of several ComputeResourceState instances to provide a convenient
// type for reading all the relevant state of a AllocationManager.
type ComputeResourceState interface {
	// Status returns the Status of the ComputeResource encapsulated/made available for reading
	// by this ComputeResourceState instance.
	ResourceStatus() Status

	// Millicpus returns the gpus as a float64.
	// The units are millicpus, or 1/1000th of a CPU core.
	Millicpus() float64
	// MillicpusAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the number of cpus.
	// The units are millicpus, or 1/1000th of a CPU core.
	MillicpusAsDecimal() decimal.Decimal

	// MemoryMB returns the amount of memory as a float64.
	// The units are megabytes (MB).
	MemoryMB() float64
	// MemoryMbAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of memory.
	// The units are megabytes (MB).
	MemoryMbAsDecimal() decimal.Decimal

	// GPUs returns the gpus as a float64.
	// The units are vGPUs, where 1 vGPU = 1 GPU.
	GPUs() float64
	// GPUsAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the number of gpus.
	// The units are vGPUs, where 1 vGPU = 1 GPU.
	GPUsAsDecimal() decimal.Decimal

	// VRAM returns the amount of VRAM (in GB).
	VRAM() float64
	// VRAMAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of VRAM.
	// The units are gigabytes (GB).
	VRAMAsDecimal() decimal.Decimal

	// String returns a string representation of the ComputeResourceState suitable for logging.
	String() string

	// ComputeResourceSnapshot creates and returns a pointer to a new ComputeResourceSnapshot struct, thereby
	// capturing the current quantities of the ComputeResource encoded by the ComputeResourceState instance.
	ResourceSnapshot(snapshotId int32) *ComputeResourceSnapshot
}
