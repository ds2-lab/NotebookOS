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
	SpecToString() string
	IsNonZero() bool
	IsPending() bool
	IsCommitted() bool
	GetKernelId() string
	GetHostId() string
	GetExecutionId() string
	SetExecutionId(executionId string)
	GetReplicaId() int32
	SetReplicaId(replicaId int32)
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

	// IsPreCommitted returns the Allocation's IsPreCommittedAllocation value, which indicates whether the
	// HostResources were commited in anticipation of a leader election, or if they are committed to a kernel that is
	// actively training.
	IsPreCommitted() bool

	// SetIsPreCommitted is used to set the value of the Allocation's IsPreCommittedAllocation flag.
	//
	// The IsPreCommittedAllocation indicates whether the HostResources were commited in anticipation of a leader
	// election, or if they are committed to a kernel that is actively training.
	SetIsPreCommitted(bool)

	// SetIsReservation is used to set the value of the Allocation's IsPreCommittedAllocation flag.
	//
	// The IsReservationAllocation indicates whether the HostResources were allocated (as either pending or committed,
	// depending upon the configured scheduling policy) in anticipation of a scheduling.KernelContainer being placed
	// onto the scheduling.Host. If true, then that means that the associated scheduling.KernelContainer has not yet
	// started running on the scheduling.Host (or that the notification that the scheduling.KernelContainer has started
	// running has not yet been received).
	SetIsReservation(isReservation bool)

	// IsReservation returns the value of the Allocation's IsReservationAllocation flag.
	//
	// The IsReservationAllocation indicates whether the HostResources were allocated (as either pending or committed,
	// depending upon the configured scheduling policy) in anticipation of a scheduling.KernelContainer being placed
	// onto the scheduling.Host. If true, then that means that the associated scheduling.KernelContainer has not yet
	// started running on the scheduling.Host (or that the notification that the scheduling.KernelContainer has started
	// running has not yet been received).
	IsReservation() bool
}
