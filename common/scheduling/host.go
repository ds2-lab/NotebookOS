package scheduling

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"time"
)

const (
	HostIndexCategoryMetadata types.HeapElementMetadataKey = "index_category"
	HostIndexKeyMetadata      types.HeapElementMetadataKey = "index_key"
)

// ErrorCallback defines a function to be called if a Host appears to be dead.
type ErrorCallback func(localDaemonId string, nodeName string, errorName string, errorMessage string) error

type PreemptionInfo interface {
	fmt.Stringer

	Penalty() float64
	Candidates() ContainerList
}

type Host interface {
	types.HeapElement

	proto.LocalGatewayClient

	// GetGrpcConnection returns the underlying grpc.ClientConn used to communicate with the remote Local Daemon.
	GetGrpcConnection() *grpc.ClientConn
	GetLocalGatewayClient() proto.LocalGatewayClient
	GetNodeName() string
	GetID() string
	IsExcludedFromScheduling() bool
	GetAddress() string
	ExcludeFromScheduling() bool
	Containers() hashmap.HashMap[string, KernelContainer]
	IncludeForScheduling() error
	IsBeingConsideredForScheduling() bool
	ConsiderForScheduling() bool
	SchedulerPoolType() SchedulerPoolType
	GetResourceSpec() types.Spec
	IsProperlyInitialized() bool

	SetSchedulerPoolType(schedulerPoolType SchedulerPoolType)
	SetIdx(types.HeapElementMetadataKey, int)
	GetIdx(types.HeapElementMetadataKey) int
	Compare(h2 interface{}) float64
	RecomputeSubscribedRatio() decimal.Decimal
	SubscribedRatio() float64
	SubscribedRatioAsDecimal() decimal.Decimal
	OversubscriptionFactor() decimal.Decimal
	ToVirtualDockerNode() *proto.VirtualDockerNode
	NumContainers() int
	NumReservations() int
	PlacedMemoryMB() decimal.Decimal
	PlacedGPUs() decimal.Decimal
	PlacedVRAM() decimal.Decimal
	PlacedCPUs() decimal.Decimal
	WillBecomeTooOversubscribed(resourceRequest types.Spec) bool

	// NumActiveSchedulingOperations returns the number of scheduling operations in which the target Host
	// is presently being considered.
	NumActiveSchedulingOperations() int32

	// CanServeContainerWithError returns nil if the target Host can serve the resource request.
	//
	// This method only checks against the Host's "spec" (i.e., the total HostResources available on the Host,
	// not taking into account current resource allocations).
	CanServeContainerWithError(resourceRequest types.Spec) (bool, error)

	// CanServeContainer returns a boolean indicating whether this Host could serve a kernel replica with the given
	// resource requirements / resource request. This method only checks against the Host's "spec" (i.e., the total
	// HostResources available on the Host, not taking into account current resource allocations).
	//
	// CanServeContainer returns true when the Host could serve the hypothetical kernel and false when the Host could not.
	CanServeContainer(resourceRequest types.Spec) bool

	// CanCommitResources returns a boolean indicating whether this Host could commit the specified resource request
	// to a kernel scheduled onto the Host right now. Commiting resource requires having sufficiently many idle HostResources
	// available.
	//
	// CanCommitResources returns true if the Host could commit/reserve the given HostResources right now.
	// Otherwise, CanCommitResources returns false.
	CanCommitResources(resourceRequest types.Spec) bool
	ReleaseReservation(spec *proto.KernelSpec) error

	// ReserveResources attempts to reserve the resources required by the specified kernel, returning
	// a boolean flag indicating whether the resource reservation was completed successfully.
	//
	// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
	ReserveResources(spec *proto.KernelSpec, usePendingResources bool) (bool, error)

	// ReserveResourcesForSpecificReplica attempts to reserve the resources required by the specified kernel replica,
	// returning a boolean flag indicating whether the resource reservation was completed successfully.
	//
	// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
	ReserveResourcesForSpecificReplica(replicaSpec *proto.KernelReplicaSpec, usePendingResources bool) (bool, error)

	// PreCommitResources pre-commits resources to the given KernelContainer.
	//
	// The specified KernelContainer must already be scheduled on the Host.
	//
	// This method is intended to be used when processing an "execute_request" that is about to be forwarded to
	// the Local Schedulers of the kernel replicas. The resources need to be pre-allocated to the KernelContainer
	// instances in case one of them wins.
	//
	// The resources will be released from the KernelContainer upon receiving an "execute_reply" indicating that a
	// particular KernelReplica yielded, or after the KernelContainer finishes executing the code in the event that
	// it wins its leader election.
	//
	// The executionId parameter is used to ensure that, if messages are received out-of-order, that we do not
	// pre-release resources when we shouldn't have.
	//
	// For example, let's say we submit EXECUTION_1 to a Kernel. We received the main execute_reply from the leader,
	// but there's a delay for the replies from the followers. In the meantime, we submit EXECUTION_2 to the Kernel.
	// EXECUTION_2 required we pre-allocate some resources again. Now if we receive the delayed replies to EXECUTION_1,
	// we may release the pre-committed resources for EXECUTION_2.
	//
	// By passing the executionId, which is stored with the pre-committed resource allocation, we can simply ignore
	// the de-allocation request if it is outdated.
	//
	// PreCommitResources is the inverse/counterpart to ReleasePreCommitedResources.
	PreCommitResources(container KernelContainer, executionId string) ([]int, error)

	// ReleasePreCommitedResources releases resources that were pre-committed to the given KernelContainer.
	//
	// ReleasePreCommitedResources is the inverse/counterpart to PreCommitResources.
	//
	// The executionId parameter is used to ensure that, if messages are received out-of-order, that we do not
	// pre-release resources when we shouldn't have.
	//
	// For example, let's say we submit EXECUTION_1 to a Kernel. We received the main execute_reply from the leader,
	// but there's a delay for the replies from the followers. In the meantime, we submit EXECUTION_2 to the Kernel.
	// EXECUTION_2 required we pre-allocate some resources again. Now if we receive the delayed replies to EXECUTION_1,
	// we may release the pre-committed resources for EXECUTION_2.
	//
	// By passing the executionId, which is stored with the pre-committed resource allocation, we can simply ignore
	// the de-allocation request if it is outdated.
	ReleasePreCommitedResources(container KernelContainer, executionId string) error

	// AdjustKernelResourceRequest when the ResourceSpec of a KernelContainer that is already scheduled on this
	// Host is updated or changed. This ensures that the Host's resource counts are up to date.
	AdjustKernelResourceRequest(updatedSpec types.Spec, oldSpec types.Spec, container KernelContainer) error

	// AdjustKernelResourceRequestCoordinated when the ResourceSpec of a KernelContainer that is already scheduled on
	// this Host is updated or changed. This ensures that the Host's resource counts are up to date.
	//
	// This version runs in a coordination fashion and is used when updating the resources of multi-replica kernels.
	AdjustKernelResourceRequestCoordinated(updatedSpec types.Spec, oldSpec types.Spec, container KernelContainer,
		tx CoordinatedTransaction) error

	Restore(restoreFrom Host, callback ErrorCallback) error
	Enabled() bool
	Enable(includeInScheduling bool) error
	Disable() error
	ContainerStoppedTraining(container KernelContainer) error
	ContainerStartedTraining(container KernelContainer) error
	ContainerRemoved(container KernelContainer) error
	// ContainerStartedRunningOnHost is to be called when a Container officially begins running on the target Host.
	ContainerStartedRunningOnHost(container KernelContainer) error
	ErrorCallback() ErrorCallback
	SetErrorCallback(callback ErrorCallback)
	Penalty(gpus float64) (float64, PreemptionInfo, error)
	HasAnyReplicaOfKernel(kernelId string) bool
	HasReservationForKernel(kernelId string) bool
	// HasResourcesCommittedToKernel returns true if the Host has resources committed to a replica of the specified kernel.
	HasResourcesCommittedToKernel(kernelId string) bool
	HasSpecificReplicaOfKernel(kernelId string, replicaId int32) bool
	GetAnyReplicaOfKernel(kernelId string) KernelContainer
	GetSpecificReplicaOfKernel(kernelId string, replicaId int32) KernelContainer
	String() string
	GetConnectionState() connectivity.State
	Stats() HostStatistics
	LastReschedule() types.StatFloat64Field
	TimeSinceLastSynchronizationWithRemote() time.Duration

	// GetReservation returns the scheduling.ResourceReservation associated with the specified kernel, if one exists.
	GetReservation(kernelId string) (Allocation, bool)
	GetMeta(key types.HeapElementMetadataKey) interface{}
	Priority(session UserSession) float64

	IdleGPUs() float64
	PendingGPUs() float64
	CommittedGPUs() float64
	IdleCPUs() float64
	PendingCPUs() float64
	CommittedCPUs() float64
	IdleMemoryMb() float64
	PendingMemoryMb() float64
	CommittedMemoryMb() float64
	IdleVRAM() float64
	PendingVRAM() float64
	CommittedVRAM() float64
	ResourceSpec() types.ValidatableResourceSpec
	CurrentResourcesToString() string
	IdleResources() *types.DecimalSpec
	PendingResources() *types.DecimalSpec
	CommittedResources() *types.DecimalSpec
	ScaleInPriority() float64
	IsContainedWithinIndex() bool
	SetContainedWithinIndex(bool)
	GetLastRemoteSync() time.Time
	GetCreatedAt() time.Time // GetCreatedAt returns the time at which the Host was created.

	// GetResourceCountsAsString returns the current resource counts of the Host as a string and is useful for printing.
	GetResourceCountsAsString() string
}

// UnitTestingHost is a wrapper around Host that exposes some additional methods that allow for the direct
// manipulation of the Host's resources. This is useful for unit testing and not much else.
//
// UnitTestingHost is a wrapper around Host much like how UnitTestingAllocationManager is a wrapper around AllocationManager.
type UnitTestingHost interface {
	Host

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

	// AllocationManager returns the AllocationManager that manages the resources of the target UnitTestingHost.
	AllocationManager() AllocationManager

	// AddGpuDeviceIds makes the specified GPU device IDs available for allocation on the target UnitTestingHost.
	AddGpuDeviceIds([]int)
}

type HostStatistics interface {
	// Priority returns the host's "priority", which is the benefit gained or lost in terms of GPU time per migration.
	Priority(session UserSession) float64

	// ScaleInPriority returns the host's "scheduling-in priority", or SIP, which is defined as a * the interactive
	// priority of a given task + b * the sum of the preemption priorities of the preemptible tasks
	ScaleInPriority() float64

	// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
	IdleGPUs() float64

	// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	// Pending GPUs are NOT actively bound to any
	PendingGPUs() float64

	// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
	CommittedGPUs() float64

	// IdleCPUs returns the number of Millicpus that the host has not allocated to any Containers.
	IdleCPUs() float64

	// PendingCPUs returns the number of Millicpus that are oversubscribed by Containers scheduled on the Host.
	// Pending Millicpus are NOT actively bound to any
	PendingCPUs() float64

	// CommittedCPUs returns the number of Millicpus that are actively bound to Containers scheduled on the Host.
	CommittedCPUs() float64

	// IdleMemoryMb returns the amount of memory, in megabytes (MB), that the host has not allocated to any Containers.
	IdleMemoryMb() float64

	// PendingMemoryMb returns the amount of memory, in megabytes (MB), that is oversubscribed by Containers scheduled on the Host.
	// Pending Memory are NOT actively bound to any
	PendingMemoryMb() float64

	// CommittedMemoryMb returns the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	CommittedMemoryMb() float64

	// IdleVRAM returns the amount of VRAM, in gigabytes (GB), that the host has not allocated to any Containers.
	IdleVRAM() float64

	// PendingVRAM returns the amount of memory, in gigabytes (GB), that is oversubscribed by Containers scheduled on the Host.
	// Pending Memory are NOT actively bound to any
	PendingVRAM() float64

	// CommittedVRAM returns the amount of memory, in gigabytes (GB), that is actively bound to Containers scheduled on the Host.
	CommittedVRAM() float64

	// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	LastReschedule() types.StatFloat64Field
}

type HostMeta interface {
	Value(key interface{}) interface{}
}
