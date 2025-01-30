package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
)

// AllocationManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The AllocationManager allocates and deallocates HostResources to/from kernel replicas scheduled to run on the node.
//
// AllocationManager is a replacement for GpuManager.
//
// In general, AllocationManager elects to work with *types.DecimalSpec structs internally, rather than arbitrary
// types.Spec interface instances, as AllocationManager stores its own state in decimal.Decimal structs.
type AllocationManager interface {
	ResourcesSnapshot() *resource.ManagerSnapshot
	ProtoResourcesSnapshot() *proto.NodeResourcesSnapshot
	DebugSetIdleGPUs(value float64)
	RegisterMetricsManager(metricsManager *metrics.LocalDaemonPrometheusManager)
	SpecGPUs() decimal.Decimal
	SpecCPUs() decimal.Decimal
	SpecMemoryMB() decimal.Decimal
	SpecVRAM() decimal.Decimal
	SpecResources() *types.DecimalSpec
	IdleGPUs() decimal.Decimal
	IdleCPUs() decimal.Decimal
	IdleMemoryMB() decimal.Decimal
	IdleVRamGB() decimal.Decimal
	IdleResources() *types.DecimalSpec
	CommittedGPUs() decimal.Decimal
	CommittedCPUs() decimal.Decimal
	CommittedMemoryMB() decimal.Decimal
	CommittedVRamGB() decimal.Decimal
	CommittedResources() *types.DecimalSpec
	NumAvailableGpuDevices() int
	NumCommittedGpuDevices() int
	PendingGPUs() decimal.Decimal
	PendingCPUs() decimal.Decimal
	PendingMemoryMB() decimal.Decimal
	PendingVRAM() decimal.Decimal
	PendingResources() *types.DecimalSpec
	AdjustSpecGPUs(numGpus float64) error
	ReplicaHasPendingGPUs(replicaId int32, kernelId string) bool
	ReplicaHasCommittedResources(replicaId int32, kernelId string) bool
	ReplicaHasCommittedGPUs(replicaId int32, kernelId string) bool
	AssertAllocationIsPending(allocation *resource.Allocation) bool
	AssertAllocationIsCommitted(allocation *resource.Allocation) bool
	NumAllocations() int
	NumCommittedAllocations() int
	NumPendingAllocations() int
	GetAllocation(replicaId int32, kernelId string) (*resource.Allocation, bool)
	PromoteReservation(replicaId int32, kernelId string) error
	AdjustPendingResources(replicaId int32, kernelId string, updatedSpec types.Spec) error
	CommitResources(replicaId int32, kernelId string, resourceRequestArg types.Spec, isReservation bool) ([]int, error)
	ReleaseCommittedResources(replicaId int32, kernelId string) error
	KernelReplicaScheduled(replicaId int32, kernelId string, spec types.Spec) error
	ReplicaEvicted(replicaId int32, kernelId string) error
	HasSufficientIdleResourcesAvailable(spec types.Spec) bool
	GetGpuDeviceIdsAssignedToReplica(replicaId int32, kernelId string) ([]int, error)
	HasSufficientIdleResourcesAvailableWithError(spec types.Spec) (bool, error)
}
