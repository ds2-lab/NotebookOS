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

// ErrorCallback defines a function to be called if a Host appears to be dead.
type ErrorCallback func(localDaemonId string, nodeName string, errorName string, errorMessage string) error

type HostMetaKey string

type PreemptionInfo interface {
	fmt.Stringer

	Penalty() float64
	Candidates() ContainerList
}

type Host interface {
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
	GetLatestGpuInfo() *proto.GpuInfo
	SetSchedulerPoolType(schedulerPoolType SchedulerPoolType)
	SetIdx(idx int)
	Compare(h2 interface{}) float64
	RecomputeSubscribedRatio() decimal.Decimal
	LastResourcesSnapshot() types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]
	SubscribedRatio() float64
	SubscribedRatioAsDecimal() decimal.Decimal
	OversubscriptionFactor() decimal.Decimal
	ToVirtualDockerNode() *proto.VirtualDockerNode
	NumContainers() int
	NumReservations() int
	SynchronizeResourceInformation() error
	PlacedMemoryMB() decimal.Decimal
	PlacedGPUs() decimal.Decimal
	PlacedCPUs() decimal.Decimal
	WillBecomeTooOversubscribed(resourceRequest types.Spec) bool
	CanServeContainerWithError(resourceRequest types.Spec) (bool, error)
	CanServeContainer(resourceRequest types.Spec) bool
	CanCommitResources(resourceRequest types.Spec) bool
	ReleaseReservation(spec *proto.KernelSpec) error
	ReserveResources(spec *proto.KernelSpec) (bool, error)
	Restore(restoreFrom Host, callback ErrorCallback) error
	Enabled() bool
	Enable(includeInScheduling bool) error
	Disable() error
	ContainerStoppedTraining(container KernelContainer) error
	ContainerStartedTraining(container KernelContainer) error
	ContainerRemoved(container KernelContainer) error
	ContainerScheduled(container KernelContainer) error
	ErrorCallback() ErrorCallback
	SetErrorCallback(callback ErrorCallback)
	Penalty(gpus float64) (float64, PreemptionInfo, error)
	HasAnyReplicaOfKernel(kernelId string) bool
	HasSpecificReplicaOfKernel(kernelId string, replicaId int32) bool
	GetAnyReplicaOfKernel(kernelId string) KernelContainer
	GetSpecificReplicaOfKernel(kernelId string, replicaId int32) KernelContainer
	String() string
	GetConnectionState() connectivity.State
	Stats() HostStatistics
	LastReschedule() types.StatFloat64Field
	TimeSinceLastSynchronizationWithRemote() time.Duration
	SetMeta(key HostMetaKey, value interface{})
	GetMeta(key HostMetaKey) interface{}
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
	AddToPendingResources(spec *types.DecimalSpec) error
	AddToIdleResources(spec *types.DecimalSpec) error
	AddToCommittedResources(spec *types.DecimalSpec) error
	SubtractFromPendingResources(spec *types.DecimalSpec) error
	SubtractFromIdleResources(spec *types.DecimalSpec) error
	SubtractFromCommittedResources(spec *types.DecimalSpec) error
	IsContainedWithinIndex() bool
	SetContainedWithinIndex(bool)
	GetLastRemoteSync() time.Time
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
	// Pending MemoryMb are NOT actively bound to any
	PendingMemoryMb() float64

	// CommittedMemoryMb returns the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	CommittedMemoryMb() float64

	// IdleVRAM returns the amount of VRAM, in gigabytes (GB), that the host has not allocated to any Containers.
	IdleVRAM() float64

	// PendingVRAM returns the amount of memory, in gigabytes (GB), that is oversubscribed by Containers scheduled on the Host.
	// Pending MemoryMb are NOT actively bound to any
	PendingVRAM() float64

	// CommittedVRAM returns the amount of memory, in gigabytes (GB), that is actively bound to Containers scheduled on the Host.
	CommittedVRAM() float64

	// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	LastReschedule() types.StatFloat64Field
}

type HostMeta interface {
	Value(key interface{}) interface{}
}
