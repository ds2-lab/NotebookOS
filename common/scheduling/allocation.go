package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"time"
)

const (
	// PendingAllocation indicates that an Allocation is "pending" rather than "committed".
	// This means that the HostResources are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these HostResources.
	PendingAllocation AllocationType = "pending"

	//CommittedAllocation indicates that an Allocation has been committed to the associated kernel replica.
	//That is, the GPUs, Millicpus, and Memory specified in the allocation are actively committed and bound to the
	//associated kernel replica. These HostResources are not available for use by other kernel replicas.
	CommittedAllocation AllocationType = "committed"

	// SnapshotMetadataKey is used as a key for the metadata dictionary of Jupyter messages
	// when including a snapshot of the AllocationManager's working resource quantities in the message.
	SnapshotMetadataKey string = "resource_snapshot"
)

// AllocationType differentiates between "pending" and "committed" resource allocations.
type AllocationType string

func (t AllocationType) String() string {
	return string(t)
}

type Allocation interface {
	CloneAndReturnedAdjusted(spec types.Spec) Allocation
	String() string
	ToSpecString() string
	ToSpec() types.Spec
	ToDecimalSpec() *types.DecimalSpec
	IsNonZero() bool
	IsPending() bool
	IsCommitted() bool
	GetKernelId() string
	GetReplicaId() int32
	SetGpuDeviceIds(deviceIds []int)
	ClearGpuDeviceIds()
	GetTimestamp() time.Time
	GetAllocationId() string
	GetGpus() float64
	GetVramGb() float64
	GetMemoryMb() float64
	GetMillicpus() float64
	SetGpus(decimal.Decimal)
	SetVramGb(decimal.Decimal)
	SetMemoryMb(decimal.Decimal)
	SetMillicpus(decimal.Decimal)
	GetGpuDeviceIds() []int
	GetAllocationType() AllocationType
	SetAllocationType(AllocationType)
	SetIsReservation(bool)

	// IsAReservation returns the Allocation's IsReservation value, which indicates whether the HostResources were
	// commited in anticipation of a leader election, or if they are committed to a kernel that is actively training.
	IsAReservation() bool
}
