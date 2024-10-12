package scheduling

import (
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"google.golang.org/grpc"
	"time"
)

// AbstractHost is an extraction of the exported methods of the Host struct so that we can spoof Hosts in unit tests.
type AbstractHost interface {
	proto.LocalGatewayClient

	// GetID returns the ID of the AbstractHost.
	GetID() string
	// LastSnapshot returns the last types.HostResourceSnapshot to have been applied successfully to this Host.
	LastSnapshot() types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]
	ApplySnapshot(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error
	LockSynchronization()
	TryLockSynchronization() bool
	UnlockSynchronization()
	RangeOverContainers(rangeFunc func(s string, container *Container) (contd bool))
	// GetLastRemoteSync returns the time at which the AbstractHost last synchronized its resource counts with the actual
	// remote node that the AbstractHost represents.
	GetLastRemoteSync() time.Time
	// GetIsContainedWithinIndex returns a boolean indicating whether this Host is currently
	// contained within a valid ClusterIndex.
	GetIsContainedWithinIndex() bool
	SetIsContainedWithinIndex(bool)
	GetNodeName() string
	LockScheduling()
	TryLockScheduling() bool
	UnlockScheduling()
	LastResourcesSnapshot() types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]
	SubscribedRatio() float64
	ToVirtualDockerNode() *proto.VirtualDockerNode
	NumContainers() int
	SynchronizeResourceInformation() error
	PlacedMemoryMB() decimal.Decimal
	PlacedGPUs() decimal.Decimal
	PlacedCPUs() decimal.Decimal
	WillBecomeTooOversubscribed(resourceRequest types.Spec) bool
	CanServeContainer(resourceRequest types.Spec) bool
	CanCommitResources(resourceRequest types.Spec) bool
	ContainerScheduled(container *Container) error
	Restore(restored *Host, callback ErrorCallback) error
	Enabled() bool
	Enable() error
	Disable() error
	ContainerRemoved(container *Container) error
	ErrorCallback() ErrorCallback
	SetErrorCallback(callback ErrorCallback)
	Penalty(gpus float64) (float64, PreemptionInfo, error)
	HasAnyReplicaOfKernel(kernelId string) bool
	HasSpecificReplicaOfKernel(kernelId string, replicaId int32) bool
	GetAnyReplicaOfKernel(kernelId string) *Container
	GetSpecificReplicaOfKernel(kernelId string, replicaId int32) *Container
	SetIdx(idx int)
	String() string
	Conn() *grpc.ClientConn
	Stats() HostStatistics
	LastReschedule() types.StatFloat64Field
	TimeSinceLastSynchronizationWithRemote() time.Duration
	SetMeta(key HostMetaKey, value interface{})
	GetMeta(key HostMetaKey) interface{}
	Priority(session *Session) float64
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
	IdleResources() types.Spec
	PendingResources() types.Spec
	CommittedResources() types.Spec
	ScaleInPriority() float64
	AddToPendingResources(spec *types.DecimalSpec) error
	AddToIdleResources(spec *types.DecimalSpec) error
	AddToCommittedResources(spec *types.DecimalSpec) error
	SubtractFromPendingResources(spec *types.DecimalSpec) error
	SubtractFromIdleResources(spec *types.DecimalSpec) error
	SubtractFromCommittedResources(spec *types.DecimalSpec) error
}
