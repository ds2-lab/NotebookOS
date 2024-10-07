package scheduling

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrInvalidAllocationRequest indicates that a ResourceAllocation could not be created/satisfied due to an issue
	// with the request itself.
	//
	// The issue is not something of the nature that there are just insufficient resources available to satisfy the
	// request. Instead, ErrInvalidAllocationRequest indicates that the request itself was illegal or issued under
	// invalid circumstances, such as there being no existing ResourceAllocation of type PendingAllocation when
	// attempting to commit resources to a particular kernel replica. Alternatively, a kernel replica may be getting
	// evicted, but no existing ResourceAllocation is found for that particular kernel replica.
	ErrInvalidAllocationRequest = errors.New("the resource allocation could not be completed due to the request being invalid")

	// ErrInvalidOperation indicates that adding or subtracting the specified resources to/from the internal resource
	// counts of a resources struct would result in an invalid/illegal resource count within that resources struct,
	// such as a negative quantity for cpus, gpus, or memory.
	ErrInvalidOperation = errors.New("the requested resource operation would result in an invalid resource count")

	ErrIllegalGpuAdjustment     = errors.New("requested gpu adjustment is illegal")
	ErrAllocationNotFound       = errors.New("could not find the requested GPU allocation")
	ErrInvalidAllocationType    = errors.New("allocation for target kernel replica is not of expected/correct type")
	ErrNoPendingAllocationFound = errors.New("a pending allocation could not be found when allocating actual GPUs")
)

const (
	// PendingAllocation indicates that a ResourceAllocation is "pending" rather than "committed".
	// This means that the resources are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these resources.
	PendingAllocation AllocationType = "pending"

	//CommittedAllocation indicates that a ResourceAllocation has been committed to the associated kernel replica.
	//That is, the GPUs, Millicpus, and Memory specified in the allocation are actively committed and bound to the
	//associated kernel replica. These resources are not available for use by other kernel replicas.
	CommittedAllocation AllocationType = "committed"

	// ResourceSnapshotMetadataKey is used as a key for the metadata dictionary of Jupyter messages
	// when including a snapshot of the ResourceManager's current resource quantities in the message.
	ResourceSnapshotMetadataKey string = "resource_snapshot"
)

// getKey creates and returns a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the ResourceManager.
func getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

// AllocationType differentiates between "pending" and "committed" resource allocations.
type AllocationType string

func (t AllocationType) String() string {
	return string(t)
}

// ResourceAllocation encapsulates an allocation of resources to a kernel replica.
// Each ResourceAllocation encapsulates an allocation of GPU, CPU, and Memory resources.
type ResourceAllocation struct {
	// AllocationId is the unique ID of the allocation.
	AllocationId string `json:"ID"`

	// GPUs is the number of GPUs in the ResourceAllocation.
	GPUs decimal.Decimal `json:"gpus"`

	// Millicpus is the number of Millicpus in the ResourceAllocation, represented as 1/1000th cores.
	// That is, 1000 Millicpus is equal to 1 vCPU.
	Millicpus decimal.Decimal `json:"millicpus"`

	// MemoryMB is the amount of RAM in the ResourceAllocation in megabytes.
	MemoryMB decimal.Decimal `json:"memory_mb"`

	// ReplicaId is the SMR node ID of the replica to which the GPUs were allocated.
	ReplicaId int32 `json:"replica_id"`

	// KernelId is the ID of the kernel whose replica was allocated GPUs.
	KernelId string `json:"kernel_id"`

	// Timestamp is the time at which the resources were allocated to the replica.
	Timestamp time.Time `json:"timestamp"`

	// AllocationType indicates whether the ResourceAllocation is "pending" or "committed".
	//
	// "Pending" indicates that the resources are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these resources.
	//
	// "Committed" indicates that a ResourceAllocation has been committed to the associated kernel replica.
	// That is, the GPUs, Millicpus, and Memory specified in the allocation are actively committed and bound to the
	// associated kernel replica. These resources are not available for use by other kernel replicas.
	AllocationType AllocationType `json:"allocation_type"`

	// IsReservation indicates whether the resources were commited in anticipation of a leader election,
	// or if they are committed to a kernel that is actively training.
	IsReservation bool `json:"is_reservation"`

	// cachedAllocationKey is the cached return value of getKey(ResourceAllocation.ReplicaId, ResourceAllocation.KernelId).
	cachedAllocationKey string
}

// String returns a string representation of the ResourceAllocation suitable for logging.
func (a *ResourceAllocation) String() string {
	o, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(o)
}

// ToSpecString returns a string representation of the ResourceAllocation (suitable for logging) in the format
// of the String() methods of types.Spec implementations.
func (a *ResourceAllocation) ToSpecString() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %s, Memory: %s MB, GPUs: %s]",
		a.Millicpus.StringFixed(0), a.MemoryMB.StringFixed(4), a.GPUs.StringFixed(0))
}

// ToSpec converts the ResourceAllocation to a types.Spec instance with the same resource values as the
// ResourceAllocation's resource values.
//
// Specifically, a new types.DecimalSpec is created using copies of the ResourceAllocation's internal
// decimal.Decimal fields, and a pointer to this new types.DecimalSpec is returned.
//
// This is, in some sense, an alias for the ToDecimalSpec method, though ToSpec returns a types.Spec interface,
// whereas ToDecimalSpec returns a pointer to a types.DecimalSpec struct.
func (a *ResourceAllocation) ToSpec() types.Spec {
	return a.ToDecimalSpec()
}

// ToDecimalSpec converts the ResourceAllocation to a types.DecimalSpec struct with the same resource values as the
// ResourceAllocation's resource values and returns a pointer to it (the newly-created types.DecimalSpec).
func (a *ResourceAllocation) ToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		GPUs:      a.GPUs.Copy(),
		Millicpus: a.Millicpus.Copy(),
		MemoryMb:  a.MemoryMB.Copy(),
	}
}

// IsNonZero returns true if any of the resources (cpu, gpu, memory) encapsulated by the ResourceAllocation are > 0.
func (a *ResourceAllocation) IsNonZero() bool {
	return a.GPUs.GreaterThan(decimal.Zero) || a.Millicpus.GreaterThan(decimal.Zero) || a.MemoryMB.GreaterThan(decimal.Zero)
}

// IsPending returns true if the ResourceAllocation is of type PendingAllocation.
// If the ResourceAllocation is instead of type CommittedAllocation, then IsPending returns false.
func (a *ResourceAllocation) IsPending() bool {
	return a.AllocationType == PendingAllocation
}

// IsCommitted returns true if the ResourceAllocation is of type CommittedAllocation.
// If the ResourceAllocation is instead of type PendingAllocation, then IsCommitted returns false.
func (a *ResourceAllocation) IsCommitted() bool {
	return a.AllocationType == CommittedAllocation
}

// ResourceAllocationBuilder is a utility struct whose purpose is to facilitate the creation of a
// new ResourceAllocation struct.
type ResourceAllocationBuilder struct {
	allocationId   string
	gpus           decimal.Decimal
	millicpus      decimal.Decimal
	memoryMb       decimal.Decimal
	replicaId      int32
	kernelId       string
	allocationType AllocationType
}

// NewResourceAllocationBuilder creates a new ResourceAllocationBuilder and returns a pointer to it.
// The AllocationID of the ResourceAllocation being constructed is randomly generated at this point.
func NewResourceAllocationBuilder() *ResourceAllocationBuilder {
	return &ResourceAllocationBuilder{
		allocationId: uuid.NewString(),
	}
}

// WithIdOverride enables the specification of a specific ID to be used as the Allocation ID of the ResourceAllocation
// that is being created. This is entirely optional. If no ID is specified explicitly, then a random UUID is
// generated to be used as the Allocation ID of the ResourceAllocation that is under construction.
func (b *ResourceAllocationBuilder) WithIdOverride(id string) *ResourceAllocationBuilder {
	b.allocationId = id
	return b
}

// WithAllocationType enables the specification of the AllocationType of the ResourceAllocation that is being created.
func (b *ResourceAllocationBuilder) WithAllocationType(allocationType AllocationType) *ResourceAllocationBuilder {
	b.allocationType = allocationType
	return b
}

// WithKernelReplica enables the specification of the target of the ResourceAllocation (i.e., the kernel replica).
func (b *ResourceAllocationBuilder) WithKernelReplica(replicaId int32, kernelId string) *ResourceAllocationBuilder {
	b.kernelId = kernelId
	b.replicaId = replicaId
	return b
}

// WithGPUs enables the specification of the number of GPUs in the ResourceAllocation that is being constructed.
func (b *ResourceAllocationBuilder) WithGPUs(gpus float64) *ResourceAllocationBuilder {
	b.gpus = decimal.NewFromFloat(gpus)
	return b
}

// WithMillicpus enables the specification of the number of Millicpus (in millicpus, or 1/1000th of a core)
// in the ResourceAllocation that is being constructed.
func (b *ResourceAllocationBuilder) WithMillicpus(millicpus float64) *ResourceAllocationBuilder {
	b.millicpus = decimal.NewFromFloat(millicpus)
	return b
}

// WithMemoryMB enables the specification of the amount of memory (in megabytes)
// in the ResourceAllocation that is being constructed.
func (b *ResourceAllocationBuilder) WithMemoryMB(memoryMb float64) *ResourceAllocationBuilder {
	b.memoryMb = decimal.NewFromFloat(memoryMb)
	return b
}

// BuildResourceAllocation constructs the ResourceAllocation with the values specified to the ResourceAllocationBuilder.
func (b *ResourceAllocationBuilder) BuildResourceAllocation() *ResourceAllocation {
	return &ResourceAllocation{
		AllocationId:        b.allocationId,
		GPUs:                b.gpus,
		Millicpus:           b.millicpus,
		MemoryMB:            b.memoryMb,
		ReplicaId:           b.replicaId,
		KernelId:            b.kernelId,
		AllocationType:      b.allocationType,
		Timestamp:           time.Now(),
		cachedAllocationKey: getKey(b.replicaId, b.kernelId),
	}
}

// ResourceManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The ResourceManager allocates and deallocates resources to/from kernel replicas scheduled to run on the node.
//
// ResourceManager is a replacement for GpuManager.
//
// In general, ResourceManager elects to work with *types.DecimalSpec structs internally, rather than arbitrary
// types.Spec interface instances, as ResourceManager stores its own state in decimal.Decimal structs.
// TODO: Verify that all the cases in which the ResourceManager panics are legitimately panic-worthy, rather than scenarios
// that could arise during regular operation and should just be handled using the failure handler of whatever
// scheduling procedure we have in place.
type ResourceManager struct {
	mu sync.Mutex

	// ID is the unique ID of the ResourceManager. This is distinct from the NodeID.
	ID string

	// NodeID is the unique identifier of the node on which the ResourceManager exists.
	// This field is not populated immediately, as the LocalDaemon does not have an ID
	// when it is first created. Instead, the Cluster Gateway assigns an ID to the
	// LocalDaemon via the SetID gRPC call. The NodeID field of the ResourceManager
	// is assigned a value during the execution of the SetID RPC.
	NodeID string

	log logger.Logger // Logger.

	// resourceSnapshotCounter is an atomic, thread-safe counter used to associate a monotonically-increasing
	// identifier with each newly-created ResourceSnapshot and *proto.NodeResourcesSnapshot struct.
	//
	// That is, the *proto.NodeResourcesSnapshot structs created by the ResourceManager's ProtoResourcesSnapshot
	// method and the *ResourceWrapperSnapshot structs created by the ResourceManager's ResourcesSnapshot method share
	// the same "source" for their SnapshotId fields.
	//
	// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
	// *ResourceWrapperSnapshot structs and all *ResourceWrapperSnapshot structs originating from the same node.
	resourceSnapshotCounter atomic.Int32

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *ResourceAllocation.
	//
	// allocationIdMap contains ResourceAllocation structs of both types (CommittedAllocation and PendingAllocation).
	allocationKernelReplicaMap hashmap.HashMap[string, *ResourceAllocation]

	// resourcesWrapper encapsulates the state of all resources (idle, pending, committed, and spec) managed
	// by this ResourceManager.
	resourcesWrapper *resourcesWrapper

	// numPendingAllocations is the number of active ResourceAllocation instances of type PendingAllocation.
	numPendingAllocations types.StatInt32
	// numCommittedAllocations is the number of active ResourceAllocation instances of type CommittedAllocation.
	numCommittedAllocations types.StatInt32

	metricsManager *metrics.LocalDaemonPrometheusManager
}

// NewResourceManager creates a new ResourceManager struct and returns a pointer to it.
func NewResourceManager(resourceSpec types.Spec) *ResourceManager {
	manager := &ResourceManager{
		ID:                         uuid.NewString(),
		allocationKernelReplicaMap: hashmap.NewCornelkMap[string, *ResourceAllocation](128),
	}

	manager.resourcesWrapper = newResourcesWrapper(resourceSpec)

	manager.numPendingAllocations.Store(0)
	manager.numCommittedAllocations.Store(0)

	config.InitLogger(&manager.log, manager)

	manager.log.Debug("Resource Manager initialized: %v", manager.resourcesWrapper.String())

	return manager
}

// ResourcesSnapshot returns a *ResourceWrapperSnapshot encoding the current resource quantities
// tracked by the ResourceManager. The ResourceWrapperSnapshot struct is JSON-serializable.
// This method is intended to be used when the data will be transferred via JSON/ZMQ.
//
// Important note: the *proto.NodeResourcesSnapshot structs created by the ResourceManager's ProtoResourcesSnapshot
// method and the *ResourceWrapperSnapshot structs created by this method (i.e., the ResourceManager's ArbitraryResourceSnapshot
// method) share the same "source" for their SnapshotId fields.
//
// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
// *ResourceWrapperSnapshot structs and all *ResourceWrapperSnapshot structs originating from the same node.
//
// Similarly, while the IDs of all *ResourceWrapperSnapshot structs (or equivalently all *proto.NodeResourcesSnapshot
// structs) will be monotonically increasing, they may not increase by one from struct-to-struct (for structs of the
// same type). That is, if the Local Daemon produces 3 *ResourceWrapperSnapshot structs followed by 3
// *proto.NodeResourcesSnapshot structs followed by 1 *ResourceWrapperSnapshot struct, that last *ResourceWrapperSnapshot
// struct will have SnapshotID 6. SnapshotID 0, 1, and 2 are for the first three *ResourceWrapperSnapshot structs.
// IDs 3, 4, and 5 are for the three *proto.NodeResourcesSnapshot structs that followed, meaning that the last
// *ResourceWrapperSnapshot is ultimately assigned an SnapshotID of 6.
func (m *ResourceManager) ResourcesSnapshot() *ResourceWrapperSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotId := m.resourceSnapshotCounter.Add(1)
	snapshot := &ResourceWrapperSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          time.Now(),
		NodeId:             m.NodeID,
		ManagerId:          m.ID,
		IdleResources:      m.resourcesWrapper.idleResourcesSnapshot(snapshotId),
		PendingResources:   m.resourcesWrapper.pendingResourcesSnapshot(snapshotId),
		CommittedResources: m.resourcesWrapper.committedResourcesSnapshot(snapshotId),
		SpecResources:      m.resourcesWrapper.specResourcesSnapshot(snapshotId),
	}

	return snapshot
}

// ProtoResourcesSnapshot returns a *proto.NodeResourcesSnapshot encoding the current resource quantities
// tracked by the ResourceManager. This method is intended to be used when the data will be transferred via gRPC.
//
// Important note: the *ResourceWrapperSnapshot structs created by the ResourceManager's ArbitraryResourceSnapshot method and
// the *proto.NodeResourcesSnapshot structs created by this method (i.e., the ResourceManager's ProtoResourcesSnapshot
// method) share the same "source" for their SnapshotId fields.
//
// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
// *ResourceWrapperSnapshot structs and all *ResourceWrapperSnapshot structs originating from the same node.
//
// Similarly, while the IDs of all *ResourceWrapperSnapshot structs (or equivalently all *proto.NodeResourcesSnapshot
// structs) will be monotonically increasing, they may not increase by one from struct-to-struct (for structs of the
// same type). That is, if the Local Daemon produces 3 *ResourceWrapperSnapshot structs followed by 3
// *proto.NodeResourcesSnapshot structs followed by 1 *ResourceWrapperSnapshot struct, that last *ResourceWrapperSnapshot
// struct will have SnapshotID 6. SnapshotID 0, 1, and 2 are for the first three *ResourceWrapperSnapshot structs.
// IDs 3, 4, and 5 are for the three *proto.NodeResourcesSnapshot structs that followed, meaning that the last
// *ResourceWrapperSnapshot is ultimately assigned an SnapshotID of 6.
func (m *ResourceManager) ProtoResourcesSnapshot() *proto.NodeResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotId := m.resourceSnapshotCounter.Add(1)
	snapshot := &proto.NodeResourcesSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          timestamppb.Now(),
		NodeId:             m.NodeID,
		ManagerId:          m.ID,
		IdleResources:      m.resourcesWrapper.idleProtoResourcesSnapshot(snapshotId),
		PendingResources:   m.resourcesWrapper.pendingProtoResourcesSnapshot(snapshotId),
		CommittedResources: m.resourcesWrapper.committedProtoResourcesSnapshot(snapshotId),
		SpecResources:      m.resourcesWrapper.specProtoResourcesSnapshot(snapshotId),
	}

	return snapshot
}

// DebugSetIdleGPUs is a method used in unit tests to set the idle GPUs available within the ResourceManager
// to a specific value (typically zero).
func (m *ResourceManager) DebugSetIdleGPUs(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resourcesWrapper.idleResources.gpus = decimal.NewFromFloat(value)
}

// updatePrometheusResourceMetrics updates all the resource-related Prometheus metrics.
// updatePrometheusResourceMetrics is used as a callback by the GPU/Resource Manager.
func (m *ResourceManager) unsafeUpdatePrometheusResourceMetrics() {
	if m.metricsManager == nil {
		m.log.Warn("Cannot update Prometheus resource metrics; manager has not been registered yet.")
		return
	}

	// CPU resource metrics.
	m.metricsManager.IdleCpuGauge.
		Set(m.resourcesWrapper.idleResources.Millicpus())
	m.metricsManager.PendingCpuGauge.
		Set(m.resourcesWrapper.pendingResources.Millicpus())
	m.metricsManager.CommittedCpuGauge.
		Set(m.resourcesWrapper.committedResources.Millicpus())

	// Memory resource metrics.
	m.metricsManager.IdleMemoryGauge.
		Set(m.resourcesWrapper.idleResources.MemoryMB())
	m.metricsManager.PendingMemoryGauge.
		Set(m.resourcesWrapper.pendingResources.MemoryMB())
	m.metricsManager.CommittedMemoryGauge.
		Set(m.resourcesWrapper.committedResources.MemoryMB())

	// GPU resource metrics.
	m.metricsManager.IdleGpuGauge.
		Set(m.resourcesWrapper.idleResources.GPUs())
	m.metricsManager.PendingGpuGauge.
		Set(m.resourcesWrapper.pendingResources.GPUs())
	m.metricsManager.CommittedGpuGauge.
		Set(m.resourcesWrapper.committedResources.GPUs())
}

// RegisterMetricsManager is used to set the metricsManager field of the ResourceManager.
func (m *ResourceManager) RegisterMetricsManager(metricsManager *metrics.LocalDaemonPrometheusManager) {
	if m.metricsManager != nil {
		m.log.Warn("ResourceManager already has metrics manager assigned... will replace existing metrics manager.")
	}
	m.metricsManager = metricsManager
}

// SpecGPUs returns the total number of GPUs configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) SpecGPUs() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().GPUsAsDecimal().Copy()
}

// SpecCPUs returns the total number of Millicpus configured/present on this node in millicpus.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) SpecCPUs() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().MillicpusAsDecimal().Copy()
}

// SpecMemoryMB returns the total amount of memory in megabytes configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) SpecMemoryMB() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().MemoryMbAsDecimal().Copy()
}

// SpecResources returns a snapshot of the current quantities of spec resources available
// on this node at the time at which the SpecResources method is called.
func (m *ResourceManager) SpecResources() *types.DecimalSpec {
	return m.resourcesWrapper.specResources.ToDecimalSpec()
}

// IdleGPUs returns the number of GPUs that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) IdleGPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.IdleResources().GPUsAsDecimal().Copy()
}

// IdleCPUs returns the number of Millicpus that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) IdleCPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.IdleResources().MillicpusAsDecimal().Copy()
}

// IdleMemoryMB returns the amount of memory (in MB) that is uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) IdleMemoryMB() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.IdleResources().MemoryMbAsDecimal().Copy()
}

// IdleResources returns a snapshot of the current quantities of idle resources available
// on this node at the time at which the IdleResources method is called.
func (m *ResourceManager) IdleResources() *types.DecimalSpec {
	return m.resourcesWrapper.idleResources.ToDecimalSpec()
}

// CommittedGPUs returns the number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) CommittedGPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.CommittedResources().GPUsAsDecimal().Copy()
}

// CommittedCPUs returns the Millicpus, in millicpus, that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) CommittedCPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.CommittedResources().MillicpusAsDecimal().Copy()
}

// CommittedMemoryMB returns the amount of memory (in MB) that is actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) CommittedMemoryMB() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.CommittedResources().MemoryMbAsDecimal().Copy()
}

// CommittedResources returns a snapshot of the current quantities of committed resources available
// on this node at the time at which the CommittedResources method is called.
func (m *ResourceManager) CommittedResources() *types.DecimalSpec {
	return m.resourcesWrapper.committedResources.ToDecimalSpec()
}

// PendingGPUs returns the sum of the outstanding GPUs of all replicas scheduled onto this node.
// Pending GPUs are not allocated or committed to a particular replica yet.
// The time at which resources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) PendingGPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.PendingResources().GPUsAsDecimal().Copy()
}

// PendingCPUs returns the sum of the outstanding Millicpus of all replicas scheduled onto this node, in millicpus.
// Pending Millicpus are not allocated or committed to a particular replica yet.
// The time at which resources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) PendingCPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.PendingResources().MillicpusAsDecimal().Copy()
}

// PendingMemoryMB returns the sum of the outstanding memory of all replicas scheduled onto this node, in MB.
// Pending memory is not allocated or committed to a particular replica yet.
// The time at which resources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) PendingMemoryMB() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.PendingResources().MemoryMbAsDecimal().Copy()
}

// PendingResources returns a snapshot of the current quantities of pending resources available
// on this node at the time at which the PendingResources method is called.
func (m *ResourceManager) PendingResources() *types.DecimalSpec {
	return m.resourcesWrapper.pendingResources.ToDecimalSpec()
}

// AdjustSpecGPUs sets the available GPUs to the specified value.
//
// Spec GPUs cannot be adjusted to a value < the number of allocated GPUs.
//
// For example, if Spec GPUs is currently 8, and 5/8 GPUs are committed, then Spec GPUs cannot be adjusted
// to a value less than 5.
func (m *ResourceManager) AdjustSpecGPUs(numGpus float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	numGpusDecimal := decimal.NewFromFloat(numGpus)
	if numGpusDecimal.LessThan(m.resourcesWrapper.specResources.gpus) {
		return fmt.Errorf("%w: cannot set GPUs to value < number of committed GPUs (%s). Requested: %s", ErrIllegalGpuAdjustment, m.CommittedGPUs().StringFixed(0), numGpusDecimal.StringFixed(0))
	}

	oldSpecGPUs := m.SpecGPUs()
	m.resourcesWrapper.specResources.SetGpus(numGpusDecimal)
	m.log.Debug("Adjusted Spec GPUs from %s to %s.", oldSpecGPUs.StringFixed(0), numGpusDecimal.StringFixed(0))

	return nil
}

// ReplicaHasPendingGPUs returns true if the specified kernel replica has pending GPUs.
func (m *ResourceManager) ReplicaHasPendingGPUs(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// If it is a pending GPU allocation, then we may return true.
	if alloc.IsPending() {
		return alloc.GPUs.GreaterThan(decimal.Zero)
	}

	// It is an "actual" GPU allocation, not a pending GPU allocation, so return false.
	return false
}

// ReplicaHasCommittedResources returns true if the specified kernel replica has any resources committed to it.
func (m *ResourceManager) ReplicaHasCommittedResources(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	return alloc.IsNonZero()
}

// ReplicaHasCommittedGPUs returns true if the specified kernel replica has GPUs committed to it.
func (m *ResourceManager) ReplicaHasCommittedGPUs(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// It is an "actual" GPU allocation.
	return alloc.GPUs.GreaterThan(decimal.Zero)
}

// AssertAllocationIsPending returns true if the given *ResourceAllocation IS pending.
// If the given *ResourceAllocation is NOT pending, then this function will panic.
func (m *ResourceManager) AssertAllocationIsPending(allocation *ResourceAllocation) bool {
	if allocation.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %s", allocation.String()))
}

// AssertAllocationIsCommitted returns true if the given *ResourceAllocation is NOT pending.
// If the given *ResourceAllocation IS pending, then this function will panic.
func (m *ResourceManager) AssertAllocationIsCommitted(allocation *ResourceAllocation) bool {
	if allocation.IsCommitted() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation IS pending: %s", allocation.String()))
}

// NumAllocations returns the number of active ResourceAllocation instances of either AllocationType (i.e.,
// PendingAllocation or CommittedAllocation).
func (m *ResourceManager) NumAllocations() int {
	return m.allocationKernelReplicaMap.Len()
}

// NumCommittedAllocations returns the ResourceAllocation instances whose AllocationType is CommittedAllocation.
func (m *ResourceManager) NumCommittedAllocations() int {
	return m.numCommittedAllocations.LoadInt()
}

// NumPendingAllocations returns the ResourceAllocation instances whose AllocationType is PendingAllocation.
func (m *ResourceManager) NumPendingAllocations() int {
	return m.numPendingAllocations.LoadInt()
}

// PromoteReservation should be called when a kernel replica has won its leader election and begins executing code.
// This method simply records that the resources committed to the kernel are no longer "merely" a reservation.
// Instead, the resource allocation will indicate that they committed resources are being used by a kernel replica
// that is actively running user-submitted code.
//
// If there is no resource reservation (i.e., committed allocation whose IsReservation flag is set to true) for the
// specified kernel replica, then an error is returned. Likewise, if there is no committed allocation to begin with,
// then an error is returned (i.e., if there's no committed allocation whose IsReservation flag is either true or false).
func (m *ResourceManager) PromoteReservation(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *ResourceAllocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot promote reserved resources for replica %d of kernel %s: no existing resource allocation found for that kernel replica.",
			replicaId, kernelId)
	}

	if allocation.IsPending() {
		m.log.Error("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsReservation=true.",
			replicaId, kernelId, allocation.AllocationType.String(), CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, CommittedAllocation.String(), allocation.AllocationType.String())
	}

	if !allocation.IsReservation {
		m.log.Error("Found existing '%s' resource allocation for replica %d of kernel %s; "+
			"however, '%s' resource allocation is already not a reservation...",
			CommittedAllocation.String(), replicaId, kernelId, allocation.AllocationType.String())
		return fmt.Errorf("%w: expected '%s' allocation to be a reservation (it is not)",
			ErrInvalidAllocationType, CommittedAllocation.String())
	}

	allocation.IsReservation = false

	// Make sure everything is still hunky-dory.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// CommitResources commits/binds resources to a particular kernel replica, such that the resources are reserved for
// exclusive use by that kernel replica until the kernel replica releases them (or another entity releases them
// on behalf of the kernel replica).
//
// Precondition: there must already be a ResourceAllocation of type PendingAllocation associated with the specified
// kernel replica. If no such ResourceAllocation exists, then ErrInvalidAllocationRequest is returned.
//
// If the given types.Spec argument is non-nil, then the existing resource allocation associated with the specified
// kernel will be adjusted (increased or decreased) according to the given spec. If the ResourceManager finds that
// there are insufficient resources available to accommodate the requested adjustment, then an error is returned.
//
// If the given types.Spec argument is nil, then the pending resource allocation associated with the specified kernel
// will simply be "promoted" to a "committed" resource request as-is, without adjusting any of the individual resource
// values.
//
// nil is returned on success.
//
// This operation is performed atomically by acquiring the ResourceManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *ResourceManager) CommitResources(replicaId int32, kernelId string, adjustedResourceRequest types.Spec, isReservation bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *ResourceAllocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot commit resources to replica %d of kernel %s: no existing resource allocation "+
			"found for that kernel replica.", replicaId, kernelId)
		return fmt.Errorf("%w: no resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be committed.
	if allocation.IsCommitted() {
		m.log.Error("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsReservation=true.",
			replicaId, kernelId, allocation.AllocationType.String(), PendingResources.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, PendingResources.String(), allocation.AllocationType.String())
	}

	var requestedResources *types.DecimalSpec
	if adjustedResourceRequest != nil {
		m.log.Debug("Converting adjusted resource request to a decimal spec. Request: %s", adjustedResourceRequest.String())
		requestedResources = types.ToDecimalSpec(adjustedResourceRequest)
		m.log.Debug("Converted decimal spec: %s", requestedResources.String())
	} else {
		requestedResources = allocation.ToDecimalSpec()
		m.log.Debug("Pending allocation for kernel %s-%d pre-commitment: %s", kernelId, replicaId, allocation.ToSpec())
	}

	m.log.Debug("Attempting to commit the following resources to replica %d of kernel %s (isReservation=%v): %v",
		replicaId, kernelId, isReservation, requestedResources.String())

	// First, validate against this scheduling.Host's spec.
	if err := m.resourcesWrapper.specResources.ValidateWithError(requestedResources); err != nil {
		m.log.Error("Could not commit the following resources to replica %d of kernel %s due "+
			"to insufficient host spec: %s. Specific reason for commitment failure: %v.",
			replicaId, kernelId, requestedResources.String(), err)
		return err
	}

	// Next, validate against our actual idle resource capacity.
	if err := m.resourcesWrapper.idleResources.ValidateWithError(requestedResources); err != nil {
		m.log.Error("Could not commit resources to replica %d of kernel %s: %s. "+
			"Reason for commitment failure: %v.", replicaId, kernelId, requestedResources.String(), err)
		return err
	}

	// If we've gotten this far, then we have enough resources available to commit the requested resources
	// to the specified kernel replica. So, let's do that now. First, we'll decrement the idle resources.
	if err := m.resourcesWrapper.idleResources.Subtract(requestedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll decrement the pending resources. We decrement because the resources are no longer "pending".
	// Instead, they are actively bound/committed to the kernel replica.
	if err := m.resourcesWrapper.pendingResources.Subtract(requestedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll increment the committed resources.
	if err := m.resourcesWrapper.committedResources.Add(requestedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Finally, we'll update the ResourceAllocation struct associated with this request.
	// This involves updating the resource amounts stored in the ResourceAllocation as well as its AllocationType field.
	// The resource amounts may already match what was allocated, depending on if the adjustedResourceRequest parameter
	// was nil or not.
	//
	// Once updated, we'll remove it from the pending allocation maps and add it to the committed allocation maps.
	allocation.GPUs = requestedResources.GPUs.Copy()
	allocation.Millicpus = requestedResources.Millicpus.Copy()
	allocation.MemoryMB = requestedResources.MemoryMb.Copy()
	allocation.AllocationType = CommittedAllocation
	allocation.IsReservation = isReservation

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Decr()
	m.numCommittedAllocations.Incr()

	m.log.Debug("Successfully committed the following resources to replica %d of kernel %s (isReservation=%v): %v",
		replicaId, kernelId, isReservation, requestedResources.String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.resourcesWrapper)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// ReleaseCommittedResources uncommits/unbinds resources from a particular kernel replica, such that the resources are made
// available for use by other kernel replicas.
//
// This operation is performed atomically by acquiring the ResourceManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *ResourceManager) ReleaseCommittedResources(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *ResourceAllocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot release committed resources bound to replica %d of kernel %s: no existing resource "+
			"allocation found for that kernel replica.", replicaId, kernelId)
		return fmt.Errorf("%w: no pending resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be pending, since we're supposed to be releasing it right now.
	if allocation.IsPending() {
		// In some cases, this isn't really an error. We (almost) always try to release committed resources
		// when we receive an "execute_reply" message, as we commit/reserve resources for kernel replicas before
		// their leader election so that they're definitely available if they win.
		//
		// However, if we already knew that there were insufficient resources available prior to the leader election,
		// then we'll not have reserved any, and the call to ReleaseCommittedResources will "fail" (as there won't
		// be any committed resources to release). In this case, it's not an error.
		m.log.Debug("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsReservation=true.",
			replicaId, kernelId, allocation.AllocationType.String(), CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, CommittedAllocation.String(), allocation.AllocationType.String())
	}

	// Perform the resource count adjustments, as we've validated that everything is correct/as it should be.
	// We'll pass nil for the second argument as we don't need the *types.DecimalSpec anywhere else in
	// the ReleaseCommittedResources method.
	m.unsafeReleaseCommittedResources(allocation, nil)

	m.log.Debug("Attempting to release the following committed resources from replica %d of kernel %s: %v. Current committed resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourcesWrapper.committedResources.String())

	// Finally, we'll update the ResourceAllocation struct associated with this request.
	// This involves updating its AllocationType field to be PendingAllocation.
	//
	// We'll also adjust some internal counters that keep track of the number of pending and committed resource
	// allocations.
	m.unsafeDemoteCommittedAllocationToPendingAllocation(allocation)

	m.log.Debug("Successfully released the following (previously) committed resources to replica %d of kernel %s: %v. Updated committed resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourcesWrapper.committedResources.String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.resourcesWrapper)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// unsafeDemoteCommittedAllocationToPendingAllocation performs any necessary state adjustments to the given
// ResourceAllocation in order to demote it from a CommittedAllocation to a PendingAllocation.
//
// unsafeDemoteCommittedAllocationToPendingAllocation does NOT acquire the ResourceManager's mutex and thus must be
// called from a context in which said mutex is already held.
//
// unsafeDemoteCommittedAllocationToPendingAllocation also does not perform any checks to verify that the given
// ResourceAllocation is of the correct type (i.e., CommittedAllocation, at the time of being passed to this method).
//
// unsafeDemoteCommittedAllocationToPendingAllocation does not perform any resource count modification to the
// ResourceManager. This is expected to have already been performed prior to calling this method.
func (m *ResourceManager) unsafeDemoteCommittedAllocationToPendingAllocation(allocation *ResourceAllocation) {
	// Set the AllocationType of the ResourceAllocation to PendingAllocation.
	allocation.AllocationType = PendingAllocation

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()
	m.numCommittedAllocations.Decr()
}

// unsafeReleaseCommittedResources releases committed/bound resources from the kernel replica associated with
// the given ResourceAllocation.
//
// This function does NOT acquire the ResourceManager's mutex, nor does it perform any validation checks whatsoever.
// It is meant to be called from a context in which the ResourceManager's mutex is held and any appropriate
// checks are performed before the call to unsafeReleaseCommittedResources and after unsafeReleaseCommittedResources
// returns.
//
// The allocatedResources argument is optional. If it is passed as nil, then it will be assigned a value automatically
// by calling allocation.ToDecimalSpec(). If allocatedResources is non-nil, then it is necessarily expected to be
// the return value of allocation.ToDecimalSpec() (generated/called RIGHT before this function is called).
//
// If any of the resource modifications performed by this method return an error, then this method will panic.
//
// The only check that this method performs is whether the given *ResourceAllocation is nil.
// If the given *ResourceAllocation is nil, then this method will panic.
func (m *ResourceManager) unsafeReleaseCommittedResources(allocation *ResourceAllocation, allocatedResources *types.DecimalSpec) {
	if allocation == nil {
		panic("The provided ResourceAllocation cannot be nil.")
	}

	// If allocatedResources is nil, then call allocation.ToDecimalSpec() to populate allocatedResources with a value.
	if allocatedResources == nil {
		allocatedResources = allocation.ToDecimalSpec()
	}

	// If we've gotten this far, then we have enough resources available to commit the requested resources
	// to the specified kernel replica. So, let's do that now. First, we'll increment the idle resources.
	if err := m.resourcesWrapper.idleResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll increment the pending resources (since we're releasing committed resources).
	if err := m.resourcesWrapper.pendingResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll decrement the committed resources (since we're releasing committed resources).
	if err := m.resourcesWrapper.committedResources.Subtract(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}
}

// KernelReplicaScheduled is to be called whenever a kernel replica is scheduled onto this scheduling.Host.
// KernelReplicaScheduled creates a ResourceAllocation of type PendingAllocation that is then associated with the
// newly-scheduled kernel replica.
//
// This operation is performed atomically by acquiring the ResourceManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *ResourceManager) KernelReplicaScheduled(replicaId int32, kernelId string, spec types.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *ResourceAllocation
		allocationExists bool
	)

	// Verify that there does not already exist an allocation associated with the specified kernel replica.
	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); allocationExists {
		m.log.Error("Cannot subscribe pending resources to replica %d of kernel %s: found existing resource "+
			"allocation associated to that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: existing resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Construct the new ResourceAllocation using the resource quantities specified in the spec argument.
	builder := NewResourceAllocationBuilder().
		WithAllocationType(PendingAllocation).
		WithKernelReplica(replicaId, kernelId).
		WithMillicpus(spec.CPU()).
		WithMemoryMB(spec.MemoryMB()).
		WithGPUs(spec.GPU())
	allocation = builder.BuildResourceAllocation()

	m.log.Debug("Attempting to subscribe the following pending resources to replica %d of kernel %s: %v",
		replicaId, kernelId, spec.String())

	// Convert the given types.Spec argument to a *types.DecimalSpec struct.
	decimalSpec := types.ToDecimalSpec(spec)

	// First, validate against this scheduling.Host's spec.
	if err := m.resourcesWrapper.specResources.ValidateWithError(decimalSpec); err != nil {
		m.log.Error("Could not subscribe the following pending resources to replica %d of kernel %s due "+
			"to insufficient host spec: %s. Specific reason for subscription failure: %v.",
			replicaId, kernelId, decimalSpec.String(), err)
		return err
	}

	// If we've gotten this far, then we have enough resources available to subscribe the requested resources
	// to the specified kernel replica. So, let's do that now.
	if err := m.resourcesWrapper.pendingResources.Add(decimalSpec); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Store the allocation in the mapping.
	m.allocationKernelReplicaMap.Store(key, allocation)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()

	m.log.Debug("Successfully subscribed the following pending resources to replica %d of kernel %s: %v",
		replicaId, kernelId, decimalSpec.String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.resourcesWrapper)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// ReplicaEvicted is to be called whenever a kernel replica is stopped/evicted from this scheduling.Host.
// ReplicaEvicted releases any ResourceAllocation associated with the evicted/stopped kernel replica.
//
// If there are resources actively bound/committed to the kernel replica, then they are released.
// Likewise, any ResourceAllocation of type PendingAllocation is released/dissolved.
//
// This operation is performed atomically by acquiring the ResourceManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *ResourceManager) ReplicaEvicted(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *ResourceAllocation
		allocationExists bool
	)

	m.log.Debug("Attempting to evict replica %d of kernel %s.", replicaId, kernelId)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Error while evicting kernel replica within ResourceManager. "+
			"Could not find ResourceAllocation associated with replica %d of kernel %s...", replicaId, kernelId)
		return fmt.Errorf("%w: no resource allocation found for evicted replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	allocatedResources := allocation.ToDecimalSpec()

	// First, check if the allocation is of type CommittedAllocation.
	// If so, then we'll first release the committed resources before unsubscribing the pending resources.
	if allocation.IsCommitted() {
		// Perform the resource count adjustments associated with releasing committed resources.
		// We'll pass allocatedResources ourselves (non-nil), as we need the *types.DecimalSpec
		// later on in the ReplicaEvicted method.
		m.unsafeReleaseCommittedResources(allocation, allocatedResources)

		// Update the ResourceAllocation's AllocationType field, setting it to PendingAllocation, and adjust the
		// internal counters that keep track of the number of pending and committed resource allocations.
		m.unsafeDemoteCommittedAllocationToPendingAllocation(allocation)
	}

	// Next, unsubscribe the pending resources.
	if err := m.resourcesWrapper.pendingResources.Subtract(allocatedResources); err != nil {
		panic(err)
	}

	m.numPendingAllocations.Decr()

	// Delete the allocation, since the replica was evicted.
	m.allocationKernelReplicaMap.Delete(key)

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	m.log.Debug("Evicted replica %d of kernel %s, releasing the following pending resources: %v.",
		replicaId, kernelId, allocation.ToSpecString())
	m.log.Debug("After removal: %s.", m.resourcesWrapper.pendingResources.String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.resourcesWrapper)
	m.unsafeUpdatePrometheusResourceMetrics()

	return nil
}

// HasSufficientIdleResourcesAvailable returns true if there are sufficiently many idle resources available
// on the node such that the requested resources could be commited to a locally-running kernel replica.
func (m *ResourceManager) HasSufficientIdleResourcesAvailable(spec types.Spec) bool {
	return m.resourcesWrapper.idleResources.Validate(spec)
}

// HasSufficientIdleResourcesAvailableWithError returns true if there are sufficiently many idle resources available
// on the node such that the requested resources could be commited to a locally-running kernel replica.
//
// This method differs from HasSufficientIdleResourcesAvailable insofar as it returns an error encoding the resource(s)
// for which there are insufficient idle resources available.
func (m *ResourceManager) HasSufficientIdleResourcesAvailableWithError(spec types.Spec) (bool, error) {
	if err := m.resourcesWrapper.idleResources.ValidateWithError(spec); err != nil {
		return false, err
	}

	return true, nil
}

// InconsistentResourcesError is a custom error type used to indicate that some resource quantity within
// the ResourceManager is in an inconsistent or invalid/illegal state.
//
// A InconsistentResourcesError contains the information to describe exactly what is wrong, in terms of which
// quantity or quantities or involved, what the nature of the inconsistency or illegal state is, etc.
type InconsistentResourcesError struct {
	// ResourceKind indicates which kind of resource is in an inconsistent or invalid state.
	ResourceKind ResourceKind

	// ResourceStatus indicates which status of resource is in an inconsistent or invalid state.
	ResourceStatus ResourceStatus

	// ResourceInconsistency defines the various ways in which resources can be in an inconsistent or illegal state.
	// Examples include a resource being negative, a resource quantity being larger than the total available resources
	// of that kind on the node, and so on.
	ResourceInconsistency ResourceInconsistency

	// Quantity is the value of the inconsistent/invalid resource.
	Quantity decimal.Decimal

	// ReferenceQuantity is the value against which Quantity is being compared and, as a result, is in
	// an invalid or inconsistent state.
	//
	// For example, if the CPU resource is in an invalid or inconsistent state with the ResourceInconsistency
	// specified as ResourceQuantityGreaterThanSpec, then the ReferenceQuantity will be set to the appropriate
	// Quantity of the associated scheduling.Host instance's types.Spec.
	ReferenceQuantity decimal.Decimal

	// ReferenceQuantityIsMeaningful indicates that the value of ReferenceQuantity is meaningful, and not just
	// a default value used in cases where there is no ReferenceQuantity, such as when the Quantity is simply
	// a negative number.
	ReferenceQuantityIsMeaningful bool
}

// NewInconsistentResourcesError creates a new InconsistentResourcesError struct and returns a pointer to it.
//
// This function sets the ReferenceQuantityIsMeaningful field to false.
func NewInconsistentResourcesError(kind ResourceKind, inconsistency ResourceInconsistency, status ResourceStatus,
	quantity decimal.Decimal) *InconsistentResourcesError {

	return &InconsistentResourcesError{
		ResourceKind:                  kind,
		ResourceInconsistency:         inconsistency,
		Quantity:                      quantity,
		ResourceStatus:                status,
		ReferenceQuantity:             decimal.Zero.Copy(),
		ReferenceQuantityIsMeaningful: false,
	}
}

// NewInconsistentResourcesErrorWithResourceQuantity creates a new InconsistentResourcesError struct and
// returns a pointer to it.
//
// This function sets the ReferenceQuantityIsMeaningful field to true.
func NewInconsistentResourcesErrorWithResourceQuantity(kind ResourceKind, inconsistency ResourceInconsistency,
	status ResourceStatus, quantity decimal.Decimal, referenceQuantity decimal.Decimal) *InconsistentResourcesError {

	return &InconsistentResourcesError{
		ResourceKind:                  kind,
		ResourceInconsistency:         inconsistency,
		Quantity:                      quantity,
		ResourceStatus:                status,
		ReferenceQuantity:             referenceQuantity,
		ReferenceQuantityIsMeaningful: true,
	}
}

// AsError returns the InconsistentResourcesError as an error.
func (e *InconsistentResourcesError) AsError() error {
	return e
}

func (e *InconsistentResourcesError) Error() string {
	if e.ReferenceQuantityIsMeaningful {
		return fmt.Sprintf("resource \"%s\" is an inconsistent or invalid state: \"%s\" (quantity=%s, referenceQuantity=%s)",
			e.ResourceKind, e.ResourceInconsistency, e.Quantity, e.ReferenceQuantity)
	} else {
		return fmt.Sprintf("resource \"%s\" is an inconsistent or invalid state: \"%s\" (quantity=%s)",
			e.ResourceKind, e.ResourceInconsistency, e.Quantity)
	}
}

// unsafePerformConsistencyCheck validates that all the internal resource counters have valid values with respect
// to one another. For example, this function ensures that the pending, idle, and committed resource counts for
// cpu, memory, and gpus do not exceed the spec resource amounts, and that no values are negative.
//
// The resource quantities are checked in the following order: CPU, Memory, GPU.
// If any resource is found to be inconsistent, then a InconsistentResourcesError will be returned.
// The InconsistentResourcesError will be in reference to the first inconsistent quantity encountered when
// checking the resource quantities in the aforementioned order.
//
// Important: unsafePerformConsistencyCheck does not acquire the main mutex of the ResourceManager and thus
// must be called from a context in which the main mutex has already been acquired.
//
// If no resource quantities are inconsistent, then this method will return nil.
func (m *ResourceManager) unsafePerformConsistencyCheck() error {
	////////////////////////////////////////////
	// Check that everything is non-negative. //
	////////////////////////////////////////////

	// Idle resources.
	hasNegative, kind := m.resourcesWrapper.idleResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, IdleResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Pending resources.
	hasNegative, kind = m.resourcesWrapper.pendingResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, PendingResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Committed resources.
	hasNegative, kind = m.resourcesWrapper.committedResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, CommittedResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Spec resources.
	hasNegative, kind = m.resourcesWrapper.specResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, SpecResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	////////////////////////////////////////////////////////////////////////////////////////
	// Check that the idle and committed resources are no larger than the spec resources. //
	////////////////////////////////////////////////////////////////////////////////////////

	// Idle resources <= Spec resources.
	isOkay, offendingKind := m.resourcesWrapper.idleResources.LessThanOrEqual(m.resourcesWrapper.specResources)
	if !isOkay {
		return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, ResourceQuantityGreaterThanSpec,
			IdleResources, m.resourcesWrapper.idleResources.GetResource(offendingKind),
			m.resourcesWrapper.specResources.GetResource(offendingKind))
	}

	// Committed resources <= spec resources.
	isOkay, offendingKind = m.resourcesWrapper.committedResources.LessThanOrEqual(m.resourcesWrapper.specResources)
	if !isOkay {
		return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, ResourceQuantityGreaterThanSpec,
			CommittedResources, m.resourcesWrapper.committedResources.GetResource(offendingKind),
			m.resourcesWrapper.specResources.GetResource(offendingKind))
	}

	//
	// Some additional checks.
	//
	numKernelReplicasScheduledOnNode := m.allocationKernelReplicaMap.Len()
	if numKernelReplicasScheduledOnNode == 0 {
		// If there are no kernel replicas scheduled on this node, then our pending and committed
		// resource counts should be 0 and our idle resource count should be max (equal to spec).

		// First, check that our idle resources are equal to our spec resources.
		areEqual, offendingKind := m.resourcesWrapper.idleResources.EqualTo(m.resourcesWrapper.specResources)
		if !areEqual {
			return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, IdleSpecUnequal,
				IdleResources, m.resourcesWrapper.idleResources.GetResource(offendingKind),
				m.resourcesWrapper.specResources.GetResource(offendingKind))
		}

		// Next, check that our pending resources are equal to zero.
		isZero, offendingKind := m.resourcesWrapper.pendingResources.IsZero()
		if !isZero {
			return NewInconsistentResourcesError(offendingKind, PendingNonzero,
				PendingResources, m.resourcesWrapper.pendingResources.GetResource(offendingKind))
		}
	}

	return nil
}
