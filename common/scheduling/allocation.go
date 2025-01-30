package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

type Allocation interface {
	CloneAndReturnedAdjusted(spec types.Spec) *resource.Allocation
	String() string
	ToSpecString() string
	ToSpec() types.Spec
	ToDecimalSpec() *types.DecimalSpec
	IsNonZero() bool
	IsPending() bool
	IsCommitted() bool
	GetKernelId() string
	GetTimestamp() time.Time
	GetAllocationId() string
	GetGpus() float64
	GetVramGb() float64
	GetMemoryMb() float64
	GetMillicpus() float64
	GetGpuDeviceIds() []int

	// IsAReservation returns the Allocation's IsReservation value, which indicates whether the HostResources were
	// commited in anticipation of a leader election, or if they are committed to a kernel that is actively training.
	IsAReservation() bool
}
