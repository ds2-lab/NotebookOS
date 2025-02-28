package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"sync"
)

const (
	DefaultExecutionId = "N/A"
)

// AllocationManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The AllocationManager allocates and deallocates HostResources to/from kernel replicas scheduled to run on the node.
//
// AllocationManager is a replacement for GpuManager.
//
// In general, AllocationManager elects to work with *types.DecimalSpec structs internally, rather than arbitrary
// types.Spec interface instances, as AllocationManager stores its own state in decimal.Decimal structs.
//
// # Scheduling Terminology
//
//   - 'Spec': A Host's "spec" resources are the total resources it has available for allocation to KernelContainer
//     instances that are scheduled and placed onto the Host. The "spec" resources are (typically) a fixed quantity.
//     For instance, if the Host has 8 GPUs on it, then its SpecGPUs would be 8. This value does not change if GPUs are
//     allocated (exclusively or otherwise) to KernelContainer instances running on the Host. There will always be 8 GPUs
//     on the Host, regardless of whether the GPUs are allocated or not.
//
//   - 'Idle': "Idle" resources are resources that have not been exclusively committed to a particular KernelContainer.
//     Under certain scheduling policies, resources are only bound or committed (exclusively) to KernelContainer
//     instances running on a Host when the KernelReplica instance running within the KernelContainer begins to
//     actively train (or otherwise execute user-submitted code).
//
//   - 'Committed': "Committed" resources are those that have been bound or committed exclusively to a particular
//     KernelContainer instance that is running on the Host. Under some scheduling policies, resources are only
//     committed to KernelContainer instances when the KernelReplica instances running within those KernelContainer
//     instances are actively executing user-submitted code. Under other scheduling policies, resources are "committed"
//     to KernelContainer instances immediately, as soon as they are scheduled onto the Host (i.e., before they enter
//     the "running" state).
//
//   - 'Pending': "Pending" resources are the sum of all the resources required by all the idle KernelContainer
//     instances running on the Host. They are only relevant for scheduling policies that bind or commit resources to
//     KernelContainer instances while the associated KernelReplica instances are actively-training (rather than for
//     the entire lifetime of the KernelReplica). TransactionResources that are actively bound or committed to a KernelContainer
//     are not counted/included in the "pending" resource count(s) of/for a Host.
//
//   - 'Scheduled': a Scheduler has selected a Host to serve the KernelContainer of a particular KernelReplica.
//     This does not mean that the KernelContainer has actually started running on that Host yet, nor does it mean that
//     the KernelContainer will necessarily be placed onto the Host. It simply means that the Host has set aside
//     resources such that the particular KernelContainer could run on the Host. If for whatever reason, the scheduling
//     and creation of the KernelContainer is aborted, then the resources that were set aside for the KernelContainer
//     will be released/made available to other KernelContainer instances.
//
//   - "Placed": The KernelContainer that was previously "scheduled" onto the Host has actually started running on
//     the Host. This occurs after the Placer has sent a StartKernelReplica RPC (via the proto.LocalGatewayClient
//     interface). Once the KernelContainer begins running, notifications will propagate from the Local Scheduler to
//     the Cluster Gateway. When each respective component learns that the KernelContainer has actually begun running
//     on the Host, the AllocationManager will be informed of this, and the AllocationManager's records will be updated
//     accordingly.
//
//   - "Reserved": TransactionResources are "reserved" for a KernelContainer when a decision to schedule that KernelContainer on
//     a particular Host is made. So, when the KernelContainer is "scheduled", resources will be "reserved". When the
//     KernelContainer is "placed", the KernelContainer will (in theory) begin to run on the Host. Once the
//     KernelContainer enters the "running" state, the "reserved" resources are "promoted" from being a "reservation"
//     to simply being assigned to the KernelContainer. So, "reservation" specifically indicates that the resources have
//     been set aside for a KernelContainer that has not yet entered the "running" state (or for which the notification
//     that the KernelContainer has entered the "running" state has not yet reached the AllocationManager).
//
//   - "Pre-Allocated": TransactionResources that are "pre-allocated" to a KernelContainer are resources that have been exclusively
//     bound or committed to that KernelContainer in anticipation of the KernelContainer beginning to execute code. In
//     some cases, the Global Scheduler may explicitly select a replica to serve as the "primary replica" before forwarding
//     the user-submitted code (i.e., an "execute_request" message) to the KernelReplica. Alternatively, the configured
//     scheduling policy may specify that only one KernelReplica should exist per Kernel, in which case it is already
//     known which KernelReplica will serve as the "primary replica". In these cases, resources are "pre-allocated" or
//     "pre-committed" to a KernelContainer so that they are definitively available when the forwarded "execute_request"
//     message reaches the KernelReplica running within that KernelContainer.
//
//     Once the "smr_lead_task" notification that the KernelReplica running within the KernelContainer has actually
//     started training is received, the "pre-allocated"/"pre-committed" resources are "promoted" to simply being
//     "committed". (Note that "pre-allocated"/"pre-committed" resources are already included in a Host's "committed"
//     resource count, so this promotion is purely semantic.) There are situations in which the execution of the user
//     submitted code by the KernelReplica completes so quickly that the associated "execute_reply" message is received
//     before the "smr_lead_task" notification. In this case, the "pre-allocated"/"pre-committed" resources are never
//     semantically promoted to simply being "committed". When the "smr_lead_task" message is eventually received, it
//     is simply ignored. The determination that a given "smr_lead_task" message should or should not be ignored is
//     based upon a monotonically increasing counter that is assigned to each unique "execute_request" message sent to
//     a particular Kernel. This information is managed by the scheduling.ExecutionManager assigned to each Kernel.
type AllocationManager interface {
	// GetId returns the target AllocationManager's unique identifier, which is different from the associated Host's ID.
	GetId() string
	// GetNodeId returns the node ID or host ID of the scheduling.Host whose resources are managed by the target AllocationManager.
	GetNodeId() string
	ProtoResourcesSnapshot() *proto.NodeResourcesSnapshot
	DebugSetIdleGPUs(value float64)
	RegisterMetricsManager(metricsManager *metrics.ClusterMetricsProvider)
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
	// KernelHasCommittedResources returns true if any replica of the specified kernel has resources committed to it.
	KernelHasCommittedResources(kernelId string) bool
	// HasReservationForKernel returns true if the target Host has a reservation for the specified kernel.
	HasReservationForKernel(kernelId string) bool
	// ReplicaHasCommittedResources returns true if the specified replica has resources committed to it.
	ReplicaHasCommittedResources(replicaId int32, kernelId string) bool
	ReplicaHasCommittedGPUs(replicaId int32, kernelId string) bool
	AssertAllocationIsPending(allocation Allocation) bool
	AssertAllocationIsCommitted(allocation Allocation) bool
	NumAllocations() int
	NumCommittedAllocations() int
	NumPendingAllocations() int
	NumReservations() int

	// AdjustKernelResourceRequest when the ResourceSpec of a KernelContainer that is already scheduled on this
	// Host is updated or changed. This ensures that the Host's resource counts are up to date.
	AdjustKernelResourceRequest(updatedSpec types.Spec, oldSpec types.Spec, replicaId int32, kernelId string) error

	// AdjustKernelResourceRequestCoordinated when the ResourceSpec of a KernelContainer that is already scheduled on
	// this Host is updated or changed. This ensures that the Host's resource counts are up to date.
	//
	// This version runs in a coordination fashion and is used when updating the resources of multi-replica kernels.
	//
	// Returns a flag indicating whether the participant was even registered.
	AdjustKernelResourceRequestCoordinated(updatedSpec types.Spec, oldSpec types.Spec, container KernelContainer,
		schedulingMutex *sync.Mutex, tx CoordinatedTransaction) (bool, error)

	// GetReservation returns the scheduling.ResourceReservation associated with the specified kernel, if one exists.
	GetReservation(kernelId string) (Allocation, bool)
	GetAllocation(replicaId int32, kernelId string) (Allocation, bool)
	PromotePreCommitment(replicaId int32, kernelId string) error

	// AdjustPendingResources will attempt to adjust the pending resources assigned to a particular kernel.
	//
	// AdjustPendingResources is really only used by the Local Daemon.
	//
	// On success, nil is returned.
	//
	// If the specified kernel replica does not already have an associated pending resource allocation, then
	// an ErrAllocationNotFound error is returned.
	//
	// If the requested resource adjustment cannot be applied, then an ErrInvalidOperation error is returned.
	//
	// Note: if the rollback fails for any reason, then this will panic.
	AdjustPendingResources(replicaId int32, kernelId string, updatedSpec types.Spec) error
	SetUpdateIndex(updateIndex func(replicaId int32, kernelId string) error)
	SetUpdateSubscriptionRatio(updateSubscriptionRatio func() decimal.Decimal)

	// ReleaseReservation is to be called when a resource reservation should be released because the
	// scheduling of the associated replica of the associated kernel is being aborted.
	ReleaseReservation(spec *proto.KernelSpec) error

	// CommitResourcesToExistingContainer commits/binds HostResources to a particular kernel replica, such that the
	// HostResources are reserved for exclusive use by that kernel replica until the kernel replica releases them
	// (or another entity releases them on behalf of the kernel replica).
	//
	// Precondition: there must already be an Allocation of type PendingAllocation associated with the specified
	// kernel replica. If no such Allocation exists, then ErrInvalidAllocationRequest is returned.
	//
	// If the given types.Spec argument is non-nil, then the existing resource allocation associated with the specified
	// kernel will be adjusted (increased or decreased) according to the given spec. If the AllocationManager finds that
	// there are insufficient HostResources available to accommodate the requested adjustment, then an error is returned.
	//
	// If the given types.Spec argument is nil, then the pending resource allocation associated with the specified kernel
	// will simply be "promoted" to a "committed" resource request as-is, without adjusting any of the individual resource
	// values.
	//
	// nil is returned on success.
	//
	// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
	// The sync.Mutex is released before the function returns.
	CommitResourcesToExistingContainer(replicaId int32, kernelId string, executionId string,
		resourceRequestArg types.Spec, isReservation bool) ([]int, error)

	// ReleaseCommittedResources uncommits/unbinds HostResources from a particular kernel replica, such that the
	// HostResources are made available for use by other kernel replicas.
	//
	// ReleaseCommittedResources is intended only to be used when the replica for which committed resources are being
	// released is still going to be running on the host. If the replica is being evicted, then ReplicaEvicted should
	// be called instead.
	//
	// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
	// The sync.Mutex is released before the function returns.
	ReleaseCommittedResources(replicaId int32, kernelId string, executionId string) error
	// ContainerStartedRunningOnHost is to be called whenever a kernel replica is scheduled onto this scheduling.Host.
	// ContainerStartedRunningOnHost creates an Allocation of type PendingAllocation that is then associated with the
	// newly-scheduled kernel replica.
	//
	// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
	// The sync.Mutex is released before the function returns.
	ContainerStartedRunningOnHost(replicaId int32, kernelId string, spec types.Spec) error
	ReplicaEvicted(replicaId int32, kernelId string) error
	HasSufficientIdleResourcesAvailable(spec types.Spec) bool
	// GetGpuDeviceIdsAssignedToReplica returns the GPU device IDs assigned to the specified kernel replica.
	GetGpuDeviceIdsAssignedToReplica(replicaId int32, kernelId string) ([]int, error)
	HasSufficientIdleResourcesAvailableWithError(spec types.Spec) (bool, error)
	PreCommitResourcesToExistingContainer(replicaId int32, kernelId string, executionId string, resourceRequestArg types.Spec, gpuDeviceIds []int) ([]int, error)

	// CanServeContainerWithError returns nil if the target AllocationManager can serve the resource request.
	//
	// This method only checks against the AllocationManager's "spec" (i.e., the total HostResources available on the
	// AllocationManager, not taking into account current resource allocations).
	CanServeContainerWithError(resourceRequest types.Spec) (bool, error)

	// CanServeContainer returns a boolean indicating whether this AllocationManager could serve a kernel replica with
	// the given resource requirements / resource request. This method only checks against the AllocationManager's
	// "spec" (i.e., the total HostResources available on the AllocationManager, not taking into account current
	// resource allocations).
	//
	// CanServeContainer returns true when the AllocationManager could serve the hypothetical kernel and false when the
	// AllocationManager could not.
	CanServeContainer(resourceRequest types.Spec) bool

	// CanCommitResources returns a boolean indicating whether this AllocationManager could commit the specified resource
	// request to a kernel scheduled onto the AllocationManager right now. Commiting resource requires having sufficiently
	// many idle HostResources available.
	//
	// CanCommitResources returns true if the AllocationManager could commit/reserve the given HostResources right now.
	// Otherwise, CanCommitResources returns false.
	CanCommitResources(resourceRequest types.Spec) bool

	// CurrentResourcesToString returns all the current resource counts of the AllocationManager as a string and is
	// useful for printing. CurrentResourcesToString is very similar to GetResourceCountsAsString; the format of the
	// strings generated by the two methods just differs slightly.
	CurrentResourcesToString() string

	// GetResourceCountsAsString returns the current resource counts of the AllocationManager as a string and is
	// useful for printing. GetResourceCountsAsString is very similar to CurrentResourcesToString; the format of
	// the strings generated by the two methods just differs slightly.
	GetResourceCountsAsString() string

	// PlacedMemoryMB returns the total amount of scheduled memory, which is computed as the
	// sum of the AllocationManager's pending memory and the Host's committed memory, in megabytes.
	PlacedMemoryMB() decimal.Decimal

	// PlacedGPUs returns the total number of scheduled GPUs, which is computed as the
	// sum of the AllocationManager's pending GPUs and the Host's committed GPUs.
	PlacedGPUs() decimal.Decimal

	// PlacedVRAM returns the total amount of scheduled VRAM in GB, which is computed as the
	// sum of the AllocationManager's pending VRAM and the Host's committed VRAM.
	PlacedVRAM() decimal.Decimal

	// PlacedCPUs returns the total number of scheduled Millicpus, which is computed as the
	// sum of the AllocationManager's pending Millicpus and the Host's committed Millicpus.
	PlacedCPUs() decimal.Decimal

	// ReserveResources creates a new resource reservation for the specified replica of the specified kernel.
	//
	// The types.Spec argument encodes the amount of resources to reserve.
	ReserveResources(replicaId int32, kernelId string, spec types.Spec, usePendingResources bool) error
}

// UnitTestingAllocationManager is a wrapper around AllocationManager much like how UnitTestingHost is a wrapper
// around Host.
//
// UnitTestingAllocationManager exposes some additional methods that allow for the direct manipulation of the wrapped
// AllocationManager's resources. This is useful for unit testing and not much else.
type UnitTestingAllocationManager interface {
	AllocationManager

	// AddToPendingResources is only meant to be used during unit tests.
	AddToPendingResources(spec *types.DecimalSpec) error

	// AddToCommittedResources is only intended to be used during unit tests.
	AddToCommittedResources(spec *types.DecimalSpec) error

	// SubtractFromIdleResources is only intended to be used during unit tests.
	SubtractFromIdleResources(spec *types.DecimalSpec) error

	// SubtractFromCommittedResources is only intended to be used during unit tests.
	SubtractFromCommittedResources(spec *types.DecimalSpec) error

	// AddToIdleResources is only intended to be used during unit tests.
	AddToIdleResources(spec *types.DecimalSpec) error

	// SubtractFromPendingResources is only intended to be used during unit tests.
	SubtractFromPendingResources(spec *types.DecimalSpec) error

	// AddGpuDeviceIds makes the specified GPU device IDs available for allocation on the target
	// UnitTestingAllocationManager.
	AddGpuDeviceIds(gpuDeviceIds []int)
}
