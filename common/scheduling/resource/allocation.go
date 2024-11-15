package resource

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"time"
)

const (
	// PendingAllocation indicates that a Allocation is "pending" rather than "committed".
	// This means that the ComputeResource are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these ComputeResource.
	PendingAllocation AllocationType = "pending"

	//CommittedAllocation indicates that a Allocation has been committed to the associated kernel replica.
	//That is, the GPUs, Millicpus, and Memory specified in the allocation are actively committed and bound to the
	//associated kernel replica. These ComputeResource are not available for use by other kernel replicas.
	CommittedAllocation AllocationType = "committed"

	// ResourceSnapshotMetadataKey is used as a key for the metadata dictionary of Jupyter messages
	// when including a snapshot of the AllocationManager's current resource quantities in the message.
	ResourceSnapshotMetadataKey string = "resource_snapshot"
)

// getKey creates and returns a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the AllocationManager.
func getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

// AllocationType differentiates between "pending" and "committed" resource allocations.
type AllocationType string

func (t AllocationType) String() string {
	return string(t)
}

// Allocation encapsulates an allocation of ComputeResource to a kernel replica.
// Each Allocation encapsulates an allocation of GPU, CPU, and Memory ComputeResource.
type Allocation struct {
	// AllocationId is the unique ID of the allocation.
	AllocationId string `json:"ID"`

	// GPUs is the number of GPUs in the Allocation.
	GPUs decimal.Decimal `json:"gpus"`

	// VramGB is the amount of VRAM (i.e., GPU memory) in GB.
	VramGB decimal.Decimal `json:"vram"`

	// Millicpus is the number of Millicpus in the Allocation, represented as 1/1000th cores.
	// That is, 1000 Millicpus is equal to 1 vCPU.
	Millicpus decimal.Decimal `json:"millicpus"`

	// MemoryMB is the amount of RAM in the Allocation in megabytes.
	MemoryMB decimal.Decimal `json:"memory_mb"`

	// ReplicaId is the SMR node ID of the replica to which the GPUs were allocated.
	ReplicaId int32 `json:"replica_id"`

	// KernelId is the ID of the kernel whose replica was allocated GPUs.
	KernelId string `json:"kernel_id"`

	// Timestamp is the time at which the ComputeResource were allocated to the replica.
	Timestamp time.Time `json:"timestamp"`

	// AllocationType indicates whether the Allocation is "pending" or "committed".
	//
	// "Pending" indicates that the ComputeResource are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these ComputeResource.
	//
	// "Committed" indicates that a Allocation has been committed to the associated kernel replica.
	// That is, the GPUs, Millicpus, and Memory specified in the allocation are actively committed and bound to the
	// associated kernel replica. These ComputeResource are not available for use by other kernel replicas.
	AllocationType AllocationType `json:"allocation_type"`

	// IsReservation indicates whether the ComputeResource were commited in anticipation of a leader election,
	// or if they are committed to a kernel that is actively training.
	IsReservation bool `json:"is_reservation"`

	// cachedAllocationKey is the cached return value of getKey(Allocation.ReplicaId, Allocation.KernelId).
	cachedAllocationKey string
}

// CloneAndReturnedAdjusted returns a copy of the target Allocation with its resource quantities
// adjusted to patch the given types.Spec.
//
// If the given types.Spec is nil, then the cloned/copied Allocation struct contains the same resource
// quantities as the original, target Allocation struct.
func (a *Allocation) CloneAndReturnedAdjusted(spec types.Spec) *Allocation {
	var (
		gpus decimal.Decimal
		vram decimal.Decimal
		mem  decimal.Decimal
		cpus decimal.Decimal
	)

	if spec == nil {
		gpus = a.GPUs.Copy()
		vram = a.VramGB.Copy()
		mem = a.MemoryMB.Copy()
		cpus = a.Millicpus.Copy()
	} else {
		gpus = decimal.NewFromFloat(spec.GPU())
		vram = decimal.NewFromFloat(spec.VRAM())
		mem = decimal.NewFromFloat(spec.MemoryMB())
		cpus = decimal.NewFromFloat(spec.CPU())
	}

	clonedResourceAllocation := &Allocation{
		AllocationId:        a.AllocationId,
		GPUs:                gpus,
		VramGB:              vram,
		Millicpus:           cpus,
		MemoryMB:            mem,
		ReplicaId:           a.ReplicaId,
		KernelId:            a.KernelId,
		Timestamp:           a.Timestamp,
		AllocationType:      a.AllocationType,
		IsReservation:       a.IsReservation,
		cachedAllocationKey: a.cachedAllocationKey,
	}

	return clonedResourceAllocation
}

// String returns a string representation of the Allocation suitable for logging.
func (a *Allocation) String() string {
	o, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(o)
}

// ToSpecString returns a string representation of the Allocation (suitable for logging) in the format
// of the String() methods of types.Spec implementations.
func (a *Allocation) ToSpecString() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %s, Memory: %s MB, GPUs: %s]",
		a.Millicpus.StringFixed(0), a.MemoryMB.StringFixed(4), a.GPUs.StringFixed(0))
}

// ToSpec converts the Allocation to a types.Spec instance with the same resource values as the
// Allocation's resource values.
//
// Specifically, a new types.DecimalSpec is created using copies of the Allocation's internal
// decimal.Decimal fields, and a pointer to this new types.DecimalSpec is returned.
//
// This is, in some sense, an alias for the ToDecimalSpec method, though ToSpec returns a types.Spec interface,
// whereas ToDecimalSpec returns a pointer to a types.DecimalSpec struct.
func (a *Allocation) ToSpec() types.Spec {
	return a.ToDecimalSpec()
}

// ToDecimalSpec converts the Allocation to a types.DecimalSpec struct with the same resource values as the
// Allocation's resource values and returns a pointer to it (the newly-created types.DecimalSpec).
func (a *Allocation) ToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		GPUs:      a.GPUs.Copy(),
		Millicpus: a.Millicpus.Copy(),
		MemoryMb:  a.MemoryMB.Copy(),
		VRam:      a.VramGB.Copy(),
	}
}

// IsNonZero returns true if any of the ComputeResource (cpu, gpu, memory) encapsulated by the Allocation are > 0.
func (a *Allocation) IsNonZero() bool {
	return a.GPUs.GreaterThan(decimal.Zero) || a.Millicpus.GreaterThan(decimal.Zero) || a.MemoryMB.GreaterThan(decimal.Zero)
}

// IsPending returns true if the Allocation is of type PendingAllocation.
// If the Allocation is instead of type CommittedAllocation, then IsPending returns false.
func (a *Allocation) IsPending() bool {
	return a.AllocationType == PendingAllocation
}

// IsCommitted returns true if the Allocation is of type CommittedAllocation.
// If the Allocation is instead of type PendingAllocation, then IsCommitted returns false.
func (a *Allocation) IsCommitted() bool {
	return a.AllocationType == CommittedAllocation
}

// AllocationBuilder is a utility struct whose purpose is to facilitate the creation of a
// new Allocation struct.
type AllocationBuilder struct {
	allocationId   string
	gpus           decimal.Decimal
	vramGb         decimal.Decimal
	millicpus      decimal.Decimal
	memoryMb       decimal.Decimal
	replicaId      int32
	kernelId       string
	allocationType AllocationType
}

// NewResourceAllocationBuilder creates a new AllocationBuilder and returns a pointer to it.
// The AllocationID of the Allocation being constructed is randomly generated at this point.
func NewResourceAllocationBuilder() *AllocationBuilder {
	return &AllocationBuilder{
		allocationId: uuid.NewString(),
	}
}

// WithIdOverride enables the specification of a specific ID to be used as the Allocation ID of the Allocation
// that is being created. This is entirely optional. If no ID is specified explicitly, then a random UUID is
// generated to be used as the Allocation ID of the Allocation that is under construction.
func (b *AllocationBuilder) WithIdOverride(id string) *AllocationBuilder {
	b.allocationId = id
	return b
}

// WithAllocationType enables the specification of the AllocationType of the Allocation that is being created.
func (b *AllocationBuilder) WithAllocationType(allocationType AllocationType) *AllocationBuilder {
	b.allocationType = allocationType
	return b
}

// WithKernelReplica enables the specification of the target of the Allocation (i.e., the kernel replica).
func (b *AllocationBuilder) WithKernelReplica(replicaId int32, kernelId string) *AllocationBuilder {
	b.kernelId = kernelId
	b.replicaId = replicaId
	return b
}

// WithGPUs enables the specification of the number of GPUs in the Allocation that is being constructed.
func (b *AllocationBuilder) WithGPUs(gpus float64) *AllocationBuilder {
	b.gpus = decimal.NewFromFloat(gpus)
	return b
}

// WithVRAM enables the specification of the amount of VRAM (in GB) in the Allocation that is being constructed.
func (b *AllocationBuilder) WithVRAM(vramGb float64) *AllocationBuilder {
	b.vramGb = decimal.NewFromFloat(vramGb)
	return b
}

// WithMillicpus enables the specification of the number of Millicpus (in millicpus, or 1/1000th of a core)
// in the Allocation that is being constructed.
func (b *AllocationBuilder) WithMillicpus(millicpus float64) *AllocationBuilder {
	b.millicpus = decimal.NewFromFloat(millicpus)
	return b
}

// WithMemoryMB enables the specification of the amount of memory (in megabytes)
// in the Allocation that is being constructed.
func (b *AllocationBuilder) WithMemoryMB(memoryMb float64) *AllocationBuilder {
	b.memoryMb = decimal.NewFromFloat(memoryMb)
	return b
}

// BuildResourceAllocation constructs the Allocation with the values specified to the AllocationBuilder.
func (b *AllocationBuilder) BuildResourceAllocation() *Allocation {
	return &Allocation{
		AllocationId:        b.allocationId,
		GPUs:                b.gpus,
		VramGB:              b.vramGb,
		Millicpus:           b.millicpus,
		MemoryMB:            b.memoryMb,
		ReplicaId:           b.replicaId,
		KernelId:            b.kernelId,
		AllocationType:      b.allocationType,
		Timestamp:           time.Now(),
		cachedAllocationKey: getKey(b.replicaId, b.kernelId),
	}
}
