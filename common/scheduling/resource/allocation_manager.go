package resource

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"sync/atomic"
	"time"
)

// AllocationManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The AllocationManager allocates and deallocates HostResources to/from kernel replicas scheduled to run on the node.
//
// AllocationManager is a replacement for GpuManager.
//
// In general, AllocationManager elects to work with *types.DecimalSpec structs internally, rather than arbitrary
// types.Spec interface instances, as AllocationManager stores its own state in decimal.Decimal structs.
//
// # API Terminology
//
// For an overview of the scheduling-related terminology used in the API of this struct, please refer to the
// documentation of the scheduling.AllocationManager interface.
type AllocationManager struct {
	mu sync.Mutex

	// GetId is the unique identifier of the AllocationManager. This is distinct from the NodeId.
	Id string

	// NodeId is the unique identifier of the node on which the AllocationManager exists.
	// This field is not populated immediately, as the LocalDaemon does not have an ID
	// when it is first created. Instead, the Cluster Gateway assigns an ID to the
	// LocalDaemon via the SetID gRPC call. The NodeId field of the AllocationManager
	// is assigned a value during the execution of the SetID RPC.
	NodeId string

	log logger.Logger // Logger.

	// resourceSnapshotCounter is an atomic, thread-safe counter used to associate a monotonically-increasing
	// identifier with each newly-created ComputeResourceSnapshot and *proto.NodeResourcesSnapshot struct.
	//
	// That is, the *proto.NodeResourcesSnapshot structs created by the AllocationManager's ProtoResourcesSnapshot
	// method and the *ManagerSnapshot structs created by the AllocationManager's ResourcesSnapshot method share
	// the same "source" for their SnapshotId fields.
	//
	// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
	// *ManagerSnapshot structs and all *ManagerSnapshot structs originating from the same node.
	resourceSnapshotCounter atomic.Int32

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> scheduling.Allocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are scheduling.Allocation.
	//
	// allocationIdMap contains Allocation structs of both types (CommittedAllocation and PendingAllocation).
	allocationKernelReplicaMap hashmap.HashMap[string, scheduling.Allocation]

	// resourceManager encapsulates the state of all HostResources (idle, pending, committed, and spec) managed
	// by this AllocationManager.
	resourceManager *Manager

	// numPendingAllocations is the number of active Allocation instances of type scheduling.PendingAllocation.
	//
	// Allocation instances of type scheduling.PendingAllocation are created under scheduling policies that only
	// (exclusively) bind or commit resources to containers while the replicas within those containers are actively
	// executing user-submitted code.
	numPendingAllocations atomic.Int32

	// numCommittedAllocations is the number of active Allocation instances of type scheduling.CommittedAllocation.
	//
	// Allocation instances of type scheduling.CommittedAllocation are created when resources are exclusively bound or
	// committed to a scheduling.KernelContainer. When this occurs depends upon the scheduling policy. Under scheduling
	// policies for which the "container lifetime" is set to scheduling.SingleTrainingEvent, resources are bound to the
	// scheduling.KernelContainer for the entire duration of the scheduling.KernelContainer's lifetime. This is because
	// the scheduling.KernelContainer is created and exists only to execute a single code submission before terminating.
	//
	// Alternatively, under scheduling policies for which the "container lifetime" is set to scheduling.LongRunning, the
	// scheduling.KernelContainer persists beyond the scope of any single code execution. In this case, resources are
	// only exclusively bound or committed to a scheduling.KernelContainer immediately before it begins executing
	// user-submitted code, and they are released from the scheduling.KernelContainer once the execution completes.
	numCommittedAllocations atomic.Int32

	// numPreCommitments maintains a counter of the number of "pre-committed" allocations, which are a subset of the
	// allocations of type scheduling.CommittedAllocation. Resource pre-commitment occurs during the submission and
	// forwarding of a messaging.ShellExecuteRequest message (i.e., an "execute_request") message to the
	// scheduling.KernelReplica instances associated with a scheduling.Kernel. The resources are preemptively bound or
	// committed exclusively to one or more scheduling.KernelReplica instances in anticipation of those instances
	// beginning to train. (It is necessary for the resources to be available in order for the training to begin.)
	//
	// If the scheduling.KernelReplica is not selected as the "primary replica", as can be the case in multi-replica
	// scheduling policies, then the pre-committed resources will be released. Alternatively, if a
	// scheduling.KernelReplica with pre-committed resources is designated as the "primary replica" and begins
	// executing user-submitted code, then the "pre-committed" resources will be semantically updated to simply
	// being "committed". Since "pre-committed" resources are counted as a subset of "committed" resources, there will
	// be no change in the "committed" resource count of the AllocationManager or associated scheduling.Host.
	numPreCommitments atomic.Int32

	// numReservations maintains a counter of the number of resource allocations that are made prior to a
	// scheduling.KernelReplica actually beginning to run on a particular scheduling.Host. The resources are set aside
	// for the scheduling.KernelReplica ahead of time, so that they are definitely available when the
	// scheduling.KernelReplica is placed onto the scheduling.Host and begins running.
	numReservations atomic.Int32

	// availableGpuDevices is a queue.Fifo containing GPU device IDs.
	availableGpuDevices *queue.Fifo[int]

	metricsManager *metrics.LocalDaemonPrometheusManager
}

// NewAllocationManager creates a new AllocationManager struct and returns a pointer to it.
func NewAllocationManager(resourceSpec types.Spec) *AllocationManager {
	manager := &AllocationManager{
		Id:                         uuid.NewString(),
		allocationKernelReplicaMap: hashmap.NewCornelkMap[string, scheduling.Allocation](128),
		availableGpuDevices:        queue.NewFifo[int](int(resourceSpec.GPU())),
	}

	for i := 0; i < int(resourceSpec.GPU()); i++ {
		manager.availableGpuDevices.Enqueue(i)
	}

	manager.resourceManager = NewManager(resourceSpec)

	// Initialize all of these counters to 0.
	manager.numPendingAllocations.Store(0)
	manager.numCommittedAllocations.Store(0)
	manager.numPreCommitments.Store(0)
	manager.numReservations.Store(0)

	config.InitLogger(&manager.log, manager)

	manager.log.Debug("Resource Manager initialized: %v", manager.resourceManager.String())

	return manager
}

// GetId returns the target AllocationManager's unique identifier, which is the different from the associated
// scheduling.Host's ID.
func (m *AllocationManager) GetId() string {
	return m.Id
}

// GetNodeId returns the node ID or host ID of the scheduling.Host whose resources are managed by the target AllocationManager.
func (m *AllocationManager) GetNodeId() string {
	return m.NodeId
}

// ResourcesSnapshot returns a *ManagerSnapshot encoding the working resource quantities
// tracked by the AllocationManager. The ManagerSnapshot struct is JSON-serializable.
// This method is intended to be used when the data will be transferred via JSON/ZMQ.
//
// Important note: the *proto.NodeResourcesSnapshot structs created by the AllocationManager's ProtoResourcesSnapshot
// method and the *ManagerSnapshot structs created by this method (i.e., the AllocationManager's ArbitraryResourceSnapshot
// method) share the same "source" for their SnapshotId fields.
//
// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
// *ManagerSnapshot structs and all *ManagerSnapshot structs originating from the same node.
//
// Similarly, while the IDs of all *ManagerSnapshot structs (or equivalently all *proto.NodeResourcesSnapshot
// structs) will be monotonically increasing, they may not increase by one from struct-to-struct (for structs of the
// same type). That is, if the Local Daemon produces 3 *ManagerSnapshot structs followed by 3
// *proto.NodeResourcesSnapshot structs followed by 1 *ManagerSnapshot struct, that last *ManagerSnapshot
// struct will have SnapshotID 6. SnapshotID 0, 1, and 2 are for the first three *ManagerSnapshot structs.
// IDs 3, 4, and 5 are for the three *proto.NodeResourcesSnapshot structs that followed, meaning that the last
// *ManagerSnapshot is ultimately assigned an SnapshotID of 6.
func (m *AllocationManager) ResourcesSnapshot() *ManagerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	containers := make([]*proto.ReplicaInfo, 0, m.numPendingAllocations.Load()+m.numCommittedAllocations.Load())
	m.allocationKernelReplicaMap.Range(func(_ string, allocation scheduling.Allocation) (contd bool) {
		container := &proto.ReplicaInfo{
			KernelId:  allocation.GetKernelId(),
			ReplicaId: allocation.GetReplicaId(),
		}

		containers = append(containers, container)

		return true
	})

	snapshotId := m.resourceSnapshotCounter.Add(1)
	snapshot := &ManagerSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          time.Now(),
		NodeId:             m.NodeId,
		ManagerId:          m.Id,
		IdleResources:      m.resourceManager.idleResourcesSnapshot(snapshotId),
		PendingResources:   m.resourceManager.pendingResourcesSnapshot(snapshotId),
		CommittedResources: m.resourceManager.committedResourcesSnapshot(snapshotId),
		SpecResources:      m.resourceManager.specResourcesSnapshot(snapshotId),
		Containers:         containers,
	}

	return snapshot
}

// ProtoResourcesSnapshot returns a *proto.NodeResourcesSnapshot encoding the working resource quantities
// tracked by the AllocationManager. This method is intended to be used when the data will be transferred via gRPC.
//
// Important note: the *ManagerSnapshot structs created by the AllocationManager's ArbitraryResourceSnapshot method and
// the *proto.NodeResourcesSnapshot structs created by this method (i.e., the AllocationManager's ProtoResourcesSnapshot
// method) share the same "source" for their SnapshotId fields.
//
// Thus, the total ordering provided by the monotonically-increasing counter actually applies to all
// *ManagerSnapshot structs and all *ManagerSnapshot structs originating from the same node.
//
// Similarly, while the IDs of all *ManagerSnapshot structs (or equivalently all *proto.NodeResourcesSnapshot
// structs) will be monotonically increasing, they may not increase by one from struct-to-struct (for structs of the
// same type). That is, if the Local Daemon produces 3 *ManagerSnapshot structs followed by 3
// *proto.NodeResourcesSnapshot structs followed by 1 *ManagerSnapshot struct, that last *ManagerSnapshot
// struct will have SnapshotID 6. SnapshotID 0, 1, and 2 are for the first three *ManagerSnapshot structs.
// IDs 3, 4, and 5 are for the three *proto.NodeResourcesSnapshot structs that followed, meaning that the last
// *ManagerSnapshot is ultimately assigned an SnapshotID of 6.
func (m *AllocationManager) ProtoResourcesSnapshot() *proto.NodeResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshotId := m.resourceSnapshotCounter.Add(1)

	idleSnapshot := m.resourceManager.IdleProtoResourcesSnapshot(snapshotId)
	pendingSnapshot := m.resourceManager.PendingProtoResourcesSnapshot(snapshotId)
	committedSnapshot := m.resourceManager.CommittedProtoResourcesSnapshot(snapshotId)
	specSnapshot := m.resourceManager.SpecProtoResourcesSnapshot(snapshotId)

	snapshot := &proto.NodeResourcesSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          timestamppb.Now(),
		NodeId:             m.NodeId,
		ManagerId:          m.Id,
		IdleResources:      idleSnapshot,
		PendingResources:   pendingSnapshot,
		CommittedResources: committedSnapshot,
		SpecResources:      specSnapshot,
	}

	return snapshot
}

// RegisterMetricsManager is used to set the metricsManager field of the AllocationManager.
func (m *AllocationManager) RegisterMetricsManager(metricsManager *metrics.LocalDaemonPrometheusManager) {
	if m.metricsManager != nil {
		m.log.Warn("AllocationManager already has metrics manager assigned... will replace existing metrics manager.")
	}
	m.metricsManager = metricsManager
}

// SpecGPUs returns the total number of GPUs configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecGPUs() decimal.Decimal {
	return m.resourceManager.SpecResources().GPUsAsDecimal().Copy()
}

// SpecCPUs returns the total number of Millicpus configured/present on this node in millicpus.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecCPUs() decimal.Decimal {
	return m.resourceManager.SpecResources().MillicpusAsDecimal().Copy()
}

// SpecMemoryMB returns the total amount of memory in megabytes configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecMemoryMB() decimal.Decimal {
	return m.resourceManager.SpecResources().MemoryMbAsDecimal().Copy()
}

// SpecVRAM returns the amount of VRAM (in GB) that is configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecVRAM() decimal.Decimal {
	return m.resourceManager.SpecResources().VRAMAsDecimal().Copy()
}

// SpecResources returns a snapshot of the working quantities of spec HostResources available
// on this node at the time at which the SpecResources method is called.
func (m *AllocationManager) SpecResources() *types.DecimalSpec {
	return m.resourceManager.specResources.ToDecimalSpec()
}

// IdleGPUs returns the number of GPUs that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleGPUs() decimal.Decimal {
	return m.resourceManager.IdleResources().GPUsAsDecimal().Copy()
}

// IdleCPUs returns the number of Millicpus that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleCPUs() decimal.Decimal {
	return m.resourceManager.IdleResources().MillicpusAsDecimal().Copy()
}

// IdleMemoryMB returns the amount of memory (in MB) that is uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleMemoryMB() decimal.Decimal {
	return m.resourceManager.IdleResources().MemoryMbAsDecimal().Copy()
}

// IdleVRamGB returns the amount of VRAM (in GB) that is uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleVRamGB() decimal.Decimal {
	return m.resourceManager.IdleResources().VRAMAsDecimal().Copy()
}

// IdleResources returns a snapshot of the working quantities of idle HostResources available
// on this node at the time at which the IdleResources method is called.
func (m *AllocationManager) IdleResources() *types.DecimalSpec {
	return m.resourceManager.idleResources.ToDecimalSpec()
}

// CommittedGPUs returns the number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedGPUs() decimal.Decimal {
	return m.resourceManager.CommittedResources().GPUsAsDecimal().Copy()
}

// CommittedCPUs returns the Millicpus, in millicpus, that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedCPUs() decimal.Decimal {
	return m.resourceManager.CommittedResources().MillicpusAsDecimal().Copy()
}

// CommittedMemoryMB returns the amount of memory (in MB) that is actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedMemoryMB() decimal.Decimal {
	return m.resourceManager.CommittedResources().MemoryMbAsDecimal().Copy()
}

// CommittedVRamGB returns the amount of VRAM (in GB) that is actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedVRamGB() decimal.Decimal {
	return m.resourceManager.CommittedResources().VRAMAsDecimal().Copy()
}

// CommittedResources returns a snapshot of the working quantities of committed HostResources available
// on this node at the time at which the CommittedResources method is called.
func (m *AllocationManager) CommittedResources() *types.DecimalSpec {
	return m.resourceManager.CommittedResourcesSpec()
}

// NumAvailableGpuDevices returns the number of available (i.e., uncommitted/idle) GPU device IDs.
//
// NumAvailableGpuDevices is equal to the number of idle GPUs.
func (m *AllocationManager) NumAvailableGpuDevices() int {
	return m.availableGpuDevices.Len()
}

// NumCommittedGpuDevices returns the number of committed GPU device IDs.
//
// NumCommittedGpuDevices is equal to the number of committed GPUs.
func (m *AllocationManager) NumCommittedGpuDevices() int {
	return int(m.SpecResources().GPU()) - m.availableGpuDevices.Len()
}

// PendingGPUs returns the sum of the outstanding GPUs of all replicas scheduled onto this node.
// Pending GPUs are not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingGPUs() decimal.Decimal {
	return m.resourceManager.PendingResources().GPUsAsDecimal().Copy()
}

// PendingCPUs returns the sum of the outstanding Millicpus of all replicas scheduled onto this node, in millicpus.
// Pending Millicpus are not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingCPUs() decimal.Decimal {
	return m.resourceManager.PendingResources().MillicpusAsDecimal().Copy()
}

// PendingMemoryMB returns the sum of the outstanding memory of all replicas scheduled onto this node, in MB.
// Pending memory is not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingMemoryMB() decimal.Decimal {
	return m.resourceManager.PendingResources().MemoryMbAsDecimal().Copy()
}

// PendingVRAM returns the sum of the outstanding VRAM of all replicas scheduled onto this node, in GB.
// Pending VRAM is not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately.
// In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingVRAM() decimal.Decimal {
	return m.resourceManager.PendingResources().VRAMAsDecimal().Copy()
}

// PendingResources returns a snapshot of the working quantities of pending HostResources available
// on this node at the time at which the PendingResources method is called.
func (m *AllocationManager) PendingResources() *types.DecimalSpec {
	return m.resourceManager.PendingResourcesSpec()
}

// PlacedMemoryMB returns the total amount of scheduled memory, which is computed as the
// sum of the AllocationManager's pending memory and the Host's committed memory, in megabytes.
func (m *AllocationManager) PlacedMemoryMB() decimal.Decimal {
	return m.resourceManager.PendingResources().MemoryMbAsDecimal().
		Add(m.resourceManager.CommittedResources().MemoryMbAsDecimal())
}

// PlacedGPUs returns the total number of scheduled GPUs, which is computed as the
// sum of the AllocationManager's pending GPUs and the Host's committed GPUs.
func (m *AllocationManager) PlacedGPUs() decimal.Decimal {
	return m.resourceManager.PendingResources().GPUsAsDecimal().
		Add(m.resourceManager.CommittedResources().GPUsAsDecimal())
}

// PlacedVRAM returns the total amount of scheduled VRAM in GB, which is computed as the
// sum of the AllocationManager's pending VRAM and the Host's committed VRAM.
func (m *AllocationManager) PlacedVRAM() decimal.Decimal {
	return m.resourceManager.PendingResources().VRAMAsDecimal().
		Add(m.resourceManager.CommittedResources().VRAMAsDecimal())
}

// PlacedCPUs returns the total number of scheduled Millicpus, which is computed as the
// sum of the AllocationManager's pending Millicpus and the Host's committed Millicpus.
func (m *AllocationManager) PlacedCPUs() decimal.Decimal {
	return m.resourceManager.PendingResources().MillicpusAsDecimal().
		Add(m.resourceManager.CommittedResources().MillicpusAsDecimal())
}

// AdjustSpecGPUs sets the available GPUs to the specified value.
//
// Spec GPUs cannot be adjusted to a value < the number of allocated GPUs.
//
// For example, if Spec GPUs is currently 8, and 5/8 GPUs are committed, then Spec GPUs cannot be adjusted
// to a value less than 5.
func (m *AllocationManager) AdjustSpecGPUs(numGpus float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	numGpusDecimal := decimal.NewFromFloat(numGpus)
	if numGpusDecimal.LessThan(m.resourceManager.committedResources.gpus) {
		return fmt.Errorf("%w: cannot set GPUs to value < number of committed GPUs (%s). Requested: %s",
			ErrIllegalGpuAdjustment, m.CommittedGPUs().StringFixed(1), numGpusDecimal.StringFixed(1))
	}

	difference := m.SpecGPUs().Sub(numGpusDecimal)

	oldSpecGPUs := m.SpecGPUs()
	m.resourceManager.specResources.SetGpus(numGpusDecimal)
	m.log.Debug("Adjusted Spec GPUs from %s to %s.",
		oldSpecGPUs.StringFixed(1), numGpusDecimal.StringFixed(1))

	// If ORIGINAL - NEW > 0, then we're decreasing the total number of GPUs available.
	// So, we'll need to decrement the idle GPUs value.
	if difference.GreaterThan(decimal.Zero) {
		newIdleGPUs := m.IdleGPUs().Sub(difference)
		m.resourceManager.idleResources.SetGpus(newIdleGPUs)
	} else {
		// ORIGINAL - NEW < 0, so we're adding GPUs.
		// We'll call difference.Abs(), as difference is negative.
		// Alternatively, we could do idleGPUs - difference, since we'd be subtracting a negative and thus adding.
		newIdleGPUs := m.IdleGPUs().Add(difference.Abs())
		m.resourceManager.idleResources.SetGpus(newIdleGPUs)
	}

	return nil
}

// ReplicaHasPendingGPUs returns true if the specified kernel replica has pending GPUs.
func (m *AllocationManager) ReplicaHasPendingGPUs(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// If it is a pending GPU allocation, then we may return true.
	if alloc.IsPending() {
		return alloc.GetGpus() > 0
	}

	// It is an "actual" GPU allocation, not a pending GPU allocation, so return false.
	return false
}

// ReplicaHasCommittedResources returns true if the specified kernel replica has any HostResources committed to it.
func (m *AllocationManager) ReplicaHasCommittedResources(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// Commented out:
	//
	// Some sessions will not request any resources.
	// return alloc.IsNonZero()
	return alloc != nil
}

// ReplicaHasCommittedGPUs returns true if the specified kernel replica has GPUs committed to it.
func (m *AllocationManager) ReplicaHasCommittedGPUs(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// It is an "actual" GPU allocation.
	return alloc.GetGpus() > 0
}

// AssertAllocationIsPending returns true if the given scheduling.Allocation IS pending.
// If the given scheduling.Allocation is NOT pending, then this function will panic.
func (m *AllocationManager) AssertAllocationIsPending(allocation scheduling.Allocation) bool {
	if allocation.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %s", allocation.String()))
}

// AssertAllocationIsCommitted returns true if the given scheduling.Allocation is NOT pending.
// If the given scheduling.Allocation IS pending, then this function will panic.
func (m *AllocationManager) AssertAllocationIsCommitted(allocation scheduling.Allocation) bool {
	if allocation.IsCommitted() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation IS pending: %s", allocation.String()))
}

// NumAllocations returns the number of active Allocation instances of either AllocationType (i.e.,
// PendingAllocation or CommittedAllocation).
func (m *AllocationManager) NumAllocations() int {
	return m.allocationKernelReplicaMap.Len()
}

// NumCommittedAllocations returns the Allocation instances whose AllocationType is CommittedAllocation.
func (m *AllocationManager) NumCommittedAllocations() int {
	return int(m.numCommittedAllocations.Load())
}

// NumPendingAllocations returns the Allocation instances whose AllocationType is PendingAllocation.
func (m *AllocationManager) NumPendingAllocations() int {
	return int(m.numPendingAllocations.Load())
}

// GetAllocation returns the Allocation associated with the specific kernel replica, if one exists.
func (m *AllocationManager) GetAllocation(replicaId int32, kernelId string) (scheduling.Allocation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		return nil, false
	}

	return allocation, true
}

// PromoteReservation should be called when a kernel replica has won its leader election and begins executing code.
// This method simply records that the HostResources committed to the kernel are no longer "merely" a reservation.
// Instead, the resource allocation will indicate that they committed HostResources are being used by a kernel replica
// that is actively running user-submitted code.
//
// If there is no resource reservation (i.e., committed allocation whose IsPreCommittedAllocation flag is set to true) for the
// specified kernel replica, then an error is returned. Likewise, if there is no committed allocation to begin with,
// then an error is returned (i.e., if there's no committed allocation whose IsPreCommittedAllocation flag is either true or false).
func (m *AllocationManager) PromoteReservation(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot promote reserved HostResources for replica %d of kernel %s: no existing resource allocation found for that kernel replica.",
			replicaId, kernelId)
	}

	if allocation.IsPending() {
		m.log.Error("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsPreCommittedAllocation=true.",
			replicaId, kernelId, allocation.GetAllocationType().String(), scheduling.CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, scheduling.CommittedAllocation.String(), allocation.GetAllocationType().String())
	}

	if !allocation.IsPreCommitted() {
		m.log.Error("Found existing '%s' resource allocation for replica %d of kernel %s; "+
			"however, '%s' resource allocation is already not a reservation...",
			scheduling.CommittedAllocation.String(), replicaId, kernelId, allocation.GetAllocationType().String())
		return fmt.Errorf("%w: expected '%s' allocation to be a reservation (it is not)",
			ErrInvalidAllocationType, scheduling.CommittedAllocation.String())
	}

	allocation.SetIsPreCommitted(false)

	// Make sure everything is still hunky-dory.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// AdjustPendingResources will attempt to adjust the resources committed to a particular kernel.
//
// On success, nil is returned.
//
// If the specified kernel replica does not already have an associated pending resource allocation, then
// an ErrAllocationNotFound error is returned.
//
// If the requested resource adjustment cannot be applied, then an ErrInvalidOperation error is returned.
//
// Note: if the rollback fails for any reason, then this will panic.
func (m *AllocationManager) AdjustPendingResources(replicaId int32, kernelId string, updatedSpec types.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// To do this, we'll just release the existing pending resource request, attempt to reserve the new request,
	// and re-reserve the old request if the new request fails.
	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	// Verify that there already exists an allocation associated with the specified kernel replica.
	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot adjust pending resources of replica %d of kernel %s: could not found existing resource "+
			"allocation associated with that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: could not find existing pending resource allocation for replica %d of kernel %s",
			ErrAllocationNotFound, replicaId, kernelId)
	}

	if allocation.IsCommitted() {
		m.log.Error("Cannot adjust resources of replica %d of kernel %s, "+
			"as resources are already committed to that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: could not find existing pending resource allocation for replica %d of kernel %s",
			scheduling.ErrInvalidOperation, replicaId, kernelId)
	}

	// First, release the original amount of pending resources.
	originalAllocatedResources := allocation.ToDecimalSpec()
	err := m.unsafeUnsubscribePendingResources(originalAllocatedResources, key)
	if err != nil {
		m.log.Error("Failed to release original amount of pending resources during resource adjustment of replica %d of kernel %s because: %v",
			replicaId, kernelId, err)
		return err
	}

	decimalSpec := types.ToDecimalSpec(updatedSpec)
	adjustedAllocation := allocation.CloneAndReturnedAdjusted(decimalSpec)

	// Next, attempt to reserve the updated amount (which could be more or less than the original amount).
	err = m.unsafeAllocatePendingResources(decimalSpec, adjustedAllocation, key, replicaId, kernelId)
	if err != nil {
		m.log.Warn("Failed to allocate updated pending resources %s during resource adjustment of replica %d of kernel %s because: %v",
			decimalSpec.String(), replicaId, kernelId, err)

		// Rollback.
		rollbackErr := m.unsafeAllocatePendingResources(originalAllocatedResources, allocation, key, replicaId, kernelId)
		if rollbackErr != nil {
			m.log.Error("Failed to rollback pending resource allocation of %s for replica %d of kernel %s because: %v",
				originalAllocatedResources.String(), replicaId, kernelId, err)
			panic(err)
		}

		m.log.Debug("Successfully rolled back pending resource adjustment to %s for replica %d of kernel %s.",
			originalAllocatedResources, replicaId, kernelId)
		return err
	}

	m.log.Debug("Successfully adjusting pending resource request for replica %d of kernel \"%s\" from %v to %v.",
		replicaId, kernelId, originalAllocatedResources.String(), adjustedAllocation.ToSpecString())

	return nil
}

// CommitResources commits/binds HostResources to a particular kernel replica, such that the HostResources are reserved for
// exclusive use by that kernel replica until the kernel replica releases them (or another entity releases them
// on behalf of the kernel replica).
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
func (m *AllocationManager) CommitResources(replicaId int32, kernelId string, resourceRequestArg types.Spec, isPreCommitment bool) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot commit HostResources to replica %d of kernel %s: no existing resource allocation "+
			"found for that kernel replica.", replicaId, kernelId)
		return nil, fmt.Errorf("%w: no resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be committed.
	if allocation.IsCommitted() {
		m.log.Error("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsPreAllocation=true.",
			replicaId, kernelId, allocation.GetAllocationType().String(), PendingResources.String())
		return nil, fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, PendingResources.String(), allocation.GetAllocationType().String())
	}

	var requestedResources *types.DecimalSpec
	if resourceRequestArg != nil {
		m.log.Debug("Converting adjusted resource request to a decimal spec. Request: %s", resourceRequestArg.String())
		requestedResources = types.ToDecimalSpec(resourceRequestArg)
		m.log.Debug("Converted decimal spec: %s", requestedResources.String())
	} else {
		requestedResources = allocation.ToDecimalSpec()
		m.log.Debug("Pending allocation for kernel %s-%d pre-commitment: %s", kernelId, replicaId, allocation.ToSpec())
	}

	m.log.Debug("Attempting to commit the following HostResources to replica %d of kernel %s (isPreCommitment=%v): %v",
		replicaId, kernelId, isPreCommitment, requestedResources.String())

	// First, validate against this scheduling.Host's spec.
	if err := m.resourceManager.specResources.ValidateWithError(requestedResources); err != nil {
		m.log.Warn("Could not commit the following HostResources to replica %d of kernel %s due "+
			"to insufficient host spec: %s. Specific reason for commitment failure: %v.",
			replicaId, kernelId, requestedResources.String(), err)
		return nil, err
	}

	// Next, validate against our actual idle resource capacity.
	if err := m.resourceManager.idleResources.ValidateWithError(requestedResources); err != nil {
		m.log.Warn("Could not commit HostResources to replica %d of kernel %s: %s. "+
			"Reason for commitment failure: %v.", replicaId, kernelId, requestedResources.String(), err)
		return nil, err
	}

	m.log.Debug("Committing resources. Current resource counts: %s. Resources to be committed: %v.",
		m.resourceManager.GetResourceCountsAsString(), requestedResources.String())

	// If we've gotten this far, then we have enough HostResources available to commit the requested HostResources
	// to the specified kernel replica. So, let's do that now. First, we'll decrement the idle HostResources.
	if err := m.resourceManager.idleResources.Subtract(requestedResources); err != nil {
		return nil, err
	}

	// Next, we'll decrement the pending HostResources. We decrement because the HostResources are no longer "pending".
	// Instead, they are actively bound/committed to the kernel replica.
	if err := m.resourceManager.pendingResources.Subtract(requestedResources); err != nil {
		return nil, err
	}

	// Next, we'll increment the committed HostResources.
	if err := m.resourceManager.committedResources.Add(requestedResources); err != nil {
		return nil, err
	}

	// Finally, we'll update the Allocation struct associated with this request.
	// This involves updating the resource amounts stored in the Allocation as well as its AllocationType field.
	// The resource amounts may already match what was allocated, depending on if the resourceRequestArg parameter
	// was nil or not.
	//
	// Once updated, we'll remove it from the pending allocation maps and add it to the committed allocation maps.
	allocation.SetGpus(requestedResources.GPUs)
	allocation.SetMillicpus(requestedResources.Millicpus)
	allocation.SetMemoryMb(requestedResources.MemoryMb)
	allocation.SetVramGb(requestedResources.VRam)
	allocation.SetAllocationType(scheduling.CommittedAllocation)
	allocation.SetIsPreCommitted(isPreCommitment)

	gpuDeviceIds := make([]int, 0, int(allocation.GetGpus()))
	for len(gpuDeviceIds) < int(allocation.GetGpus()) {
		gpuDeviceId, ok := m.availableGpuDevices.Dequeue()

		if !ok {
			panic("Received no GPU device ID when one should have been available.")
		}

		m.log.Debug("Allocating GPU #%d to replica %d of kernel '%s'.", gpuDeviceId, replicaId, kernelId)
		gpuDeviceIds = append(gpuDeviceIds, gpuDeviceId)
	}

	allocation.SetGpuDeviceIds(gpuDeviceIds)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Add(-1)
	m.numCommittedAllocations.Add(1)

	m.log.Debug("Successfully committed the following HostResources to replica %d of kernel %s (isPreCommitment=%v): %v. GPUs reserved/allocated: %v.",
		replicaId, kernelId, isPreCommitment, requestedResources.String(), allocation.GetGpuDeviceIds())
	m.log.Debug("Updated resource counts: %s.", m.resourceManager.GetResourceCountsAsString())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return nil, err
	}

	return gpuDeviceIds, nil
}

// ReleaseCommittedResources uncommits/unbinds HostResources from a particular kernel replica, such that the HostResources are made
// available for use by other kernel replicas.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) ReleaseCommittedResources(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot release committed HostResources bound to replica %d of kernel %s: no existing resource "+
			"allocation found for that kernel replica.", replicaId, kernelId)
		return fmt.Errorf("%w: no pending resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be pending, since we're supposed to be releasing it right now.
	if allocation.IsPending() {
		// In some cases, this isn't really an error. We (almost) always try to release committed HostResources
		// when we receive an "execute_reply" message, as we commit/reserve HostResources for kernel replicas before
		// their leader election so that they're definitely available if they win.
		//
		// However, if we already knew that there were insufficient HostResources available prior to the leader election,
		// then we'll not have reserved any, and the call to ReleaseCommittedResources will "fail" (as there won't
		// be any committed HostResources to release). In this case, it's not an error.
		m.log.Debug("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsPreCommittedAllocation=true.",
			replicaId, kernelId, allocation.GetAllocationType().String(), scheduling.CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, scheduling.CommittedAllocation.String(), allocation.GetAllocationType().String())
	}

	// Perform the resource count adjustments, as we've validated that everything is correct/as it should be.
	// We'll pass nil for the second argument as we don't need the *types.DecimalSpec anywhere else in
	// the ReleaseCommittedResources method.
	m.unsafeReleaseCommittedResources(allocation, allocation.ToDecimalSpec())

	m.log.Debug("Attempting to release the following committed HostResources from replica %d of kernel %s: %v. Current resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourceManager.GetResourceCountsAsString())

	// Finally, we'll update the Allocation struct associated with this request.
	// This involves updating its AllocationType field to be PendingAllocation.
	//
	// We'll also adjust some internal counters that keep track of the number of pending and committed resource
	// allocations.
	m.unsafeDemoteCommittedAllocationToPendingAllocation(allocation)

	m.log.Debug("Successfully released the following (previously) committed HostResources to replica %d of kernel %s: %v. Updated resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourceManager.GetResourceCountsAsString())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// KernelReplicaScheduled is to be called whenever a kernel replica is scheduled onto this scheduling.Host.
// KernelReplicaScheduled creates an Allocation of type PendingAllocation that is then associated with the
// newly-scheduled kernel replica.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) KernelReplicaScheduled(replicaId int32, kernelId string, spec types.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	// Verify that there does not already exist an allocation associated with the specified kernel replica.
	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); allocationExists {
		m.log.Error("Cannot subscribe pending HostResources to replica %d of kernel %s: found existing resource "+
			"allocation associated to that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: existing resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Construct the new Allocation using the resource quantities specified in the spec argument.
	builder := NewResourceAllocationBuilder().
		WithAllocationType(scheduling.PendingAllocation).
		WithKernelReplica(replicaId, kernelId).
		WithMillicpus(spec.CPU()).
		WithMemoryMB(spec.MemoryMB()).
		WithGPUs(spec.GPU()).
		WithVRAM(spec.VRAM())
	allocation = builder.BuildResourceAllocation()

	m.log.Debug("Attempting to subscribe the following pending HostResources to replica %d of kernel %s: %v",
		replicaId, kernelId, spec.String())

	// Convert the given types.Spec argument to a *types.DecimalSpec struct.
	decimalSpec := types.ToDecimalSpec(spec)

	err := m.unsafeAllocatePendingResources(decimalSpec, allocation, key, replicaId, kernelId)
	if err != nil {
		m.log.Error("Failed to allocate pending resources %s to replica %d of kernel %s: %v",
			decimalSpec.String(), replicaId, kernelId, err)
		return err
	}

	m.log.Debug("Successfully subscribed the following pending HostResources to replica %d of kernel %s: %v",
		replicaId, kernelId, decimalSpec.String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err = m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

// ReplicaEvicted is to be called whenever a kernel replica is stopped/evicted from this scheduling.Host.
// ReplicaEvicted releases any Allocation associated with the evicted/stopped kernel replica.
//
// If there are HostResources actively bound/committed to the kernel replica, then they are released.
// Likewise, any Allocation of type PendingAllocation is released/dissolved.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) ReplicaEvicted(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	m.log.Debug("Attempting to evict replica %d of kernel %s.", replicaId, kernelId)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Error while evicting kernel replica within AllocationManager. "+
			"Could not find Allocation associated with replica %d of kernel %s...", replicaId, kernelId)
		return fmt.Errorf("%w: no resource allocation found for evicted replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	allocatedResources := allocation.ToDecimalSpec()

	// First, check if the allocation is of type CommittedAllocation.
	// If so, then we'll first release the committed HostResources before unsubscribing the pending HostResources.
	if allocation.IsCommitted() {
		m.log.Debug("Releasing resources committed to evicted replica %d of kernel %s now.", replicaId, kernelId)

		// Perform the resource count adjustments associated with releasing committed HostResources.
		// We'll pass allocatedResources ourselves (non-nil), as we need the *types.DecimalSpec
		// later on in the ReplicaEvicted method.
		m.unsafeReleaseCommittedResources(allocation, allocatedResources)

		// Update the Allocation's AllocationType field, setting it to PendingAllocation, and adjust the
		// internal counters that keep track of the number of pending and committed resource allocations.
		m.unsafeDemoteCommittedAllocationToPendingAllocation(allocation)
	}

	m.log.Debug("Releasing pending resources assigned to evicted replica %d of kernel %s now.", replicaId, kernelId)

	// Next, unsubscribe the pending HostResources.
	err := m.unsafeUnsubscribePendingResources(allocatedResources, key)
	if err != nil {
		m.log.Error("Failed to unsubscribe pending resources %s from replica %d of kernel %s because: %v",
			allocatedResources.String(), replicaId, kernelId, err)
		return err
	}

	m.log.Debug("Evicted replica %d of kernel %s, releasing the following pending HostResources: %v.",
		replicaId, kernelId, allocation.ToSpecString())
	m.log.Debug("Committed resources after removal: %s.", m.resourceManager.CommittedResources().String())
	m.log.Debug("Pending resources after removal: %s.", m.resourceManager.PendingResources().String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	return nil
}

// HasSufficientIdleResourcesAvailable returns true if there are sufficiently many idle HostResources available
// on the node such that the requested HostResources could be commited to a locally-running kernel replica.
func (m *AllocationManager) HasSufficientIdleResourcesAvailable(spec types.Spec) bool {
	return m.resourceManager.idleResources.Validate(spec)
}

func (m *AllocationManager) GetGpuDeviceIdsAssignedToReplica(replicaId int32, kernelId string) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot retrieve GPU device IDs committed to replica %d of kernel %s: no existing resource "+
			"allocation found for that kernel replica.", replicaId, kernelId)
		return nil, fmt.Errorf("%w: no resource allocation found for replica %d of kernel %s",
			ErrAllocationNotFound, replicaId, kernelId)
	}

	return allocation.GetGpuDeviceIds(), nil
}

// HasSufficientIdleResourcesAvailableWithError returns true if there are sufficiently many idle HostResources available
// on the node such that the requested HostResources could be commited to a locally-running kernel replica.
//
// This method differs from HasSufficientIdleResourcesAvailable insofar as it returns an error encoding the resource(s)
// for which there are insufficient idle HostResources available.
func (m *AllocationManager) HasSufficientIdleResourcesAvailableWithError(spec types.Spec) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.resourceManager.IdleResources().ValidateWithError(spec); err != nil {
		return false, err
	}

	return true, nil
}

// CanServeContainerWithError returns nil if the target AllocationManager can serve the resource request.
//
// This method only checks against the AllocationManager's "spec" (i.e., the total HostResources available on the
// AllocationManager, not taking into account current resource allocations).
func (m *AllocationManager) CanServeContainerWithError(resourceRequest types.Spec) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.resourceManager.SpecResources().ValidateWithError(resourceRequest)
	if err != nil {
		return false, err
	}

	return true, nil
}

// CanServeContainer returns a boolean indicating whether this AllocationManager could serve a kernel replica with the
// given resource requirements / resource request. This method only checks against the AllocationManager's "spec"
// (i.e., the total HostResources available on the AllocationManager, not taking into account current resource
// allocations).
//
// CanServeContainer returns true when the AllocationManager could serve the hypothetical kernel and false when the
// AllocationManager could not.
func (m *AllocationManager) CanServeContainer(resourceRequest types.Spec) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourceManager.SpecResources().Validate(resourceRequest)
}

// CanCommitResources returns a boolean indicating whether this AllocationManager could commit the specified resource
// request to a kernel scheduled onto the AllocationManager right now. Commiting resource requires having sufficiently
// many idle HostResources available.
//
// CanCommitResources returns true if the AllocationManager could commit/reserve the given HostResources right now.
// Otherwise, CanCommitResources returns false.
func (m *AllocationManager) CanCommitResources(resourceRequest types.Spec) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourceManager.IdleResources().Validate(types.ToDecimalSpec(resourceRequest))
}

// CurrentResourcesToString returns all the current resource counts of the AllocationManager as a string and is
// useful for printing. CurrentResourcesToString is very similar to GetResourceCountsAsString; the format of the
// strings generated by the two methods just differs slightly.
func (m *AllocationManager) CurrentResourcesToString() string {
	return m.resourceManager.String()
}

// GetResourceCountsAsString returns the current resource counts of the AllocationManager as a string and is
// useful for printing. GetResourceCountsAsString is very similar to CurrentResourcesToString; the format of
// the strings generated by the two methods just differs slightly.
func (m *AllocationManager) GetResourceCountsAsString() string {
	return m.resourceManager.GetResourceCountsAsString()
}

// DebugSetIdleGPUs is a method used in unit tests to set the idle GPUs available within the AllocationManager
// to a specific value (typically zero).
func (m *AllocationManager) DebugSetIdleGPUs(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resourceManager.idleResources.gpus = decimal.NewFromFloat(value)
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
// Important: unsafePerformConsistencyCheck does not acquire the main mutex of the AllocationManager and thus
// must be called from a context in which the main mutex has already been acquired.
//
// If no resource quantities are inconsistent, then this method will return nil.
func (m *AllocationManager) unsafePerformConsistencyCheck() error {
	////////////////////////////////////////////
	// Check that everything is non-negative. //
	////////////////////////////////////////////

	// Idle HostResources.
	hasNegative, kind := m.resourceManager.idleResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, IdleResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Pending HostResources.
	hasNegative, kind = m.resourceManager.pendingResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, PendingResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Committed HostResources.
	hasNegative, kind = m.resourceManager.committedResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, CommittedResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Spec HostResources.
	hasNegative, kind = m.resourceManager.specResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, SpecResources, m.resourceManager.idleResources.GetResource(kind))
	}

	////////////////////////////////////////////////////////////////////////////////////////
	// Check that the idle and committed HostResources are no larger than the spec HostResources. //
	////////////////////////////////////////////////////////////////////////////////////////

	// Idle HostResources <= Spec HostResources.
	isOkay, offendingKind := m.resourceManager.idleResources.LessThanOrEqual(m.resourceManager.specResources)
	if !isOkay {
		return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, ResourceQuantityGreaterThanSpec,
			IdleResources, m.resourceManager.idleResources.GetResource(offendingKind),
			m.resourceManager.specResources.GetResource(offendingKind))
	}

	// Committed HostResources <= spec HostResources.
	isOkay, offendingKind = m.resourceManager.committedResources.LessThanOrEqual(m.resourceManager.specResources)
	if !isOkay {
		return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, ResourceQuantityGreaterThanSpec,
			CommittedResources, m.resourceManager.committedResources.GetResource(offendingKind),
			m.resourceManager.specResources.GetResource(offendingKind))
	}

	//
	// Some additional checks.
	//
	numKernelReplicasScheduledOnNode := m.allocationKernelReplicaMap.Len()
	if numKernelReplicasScheduledOnNode == 0 {
		// If there are no kernel replicas scheduled on this node, then our pending and committed
		// resource counts should be 0 and our idle resource count should be max (equal to spec).

		// First, check that our idle HostResources are equal to our spec HostResources.
		areEqual, offendingKind := m.resourceManager.idleResources.EqualTo(m.resourceManager.specResources)
		if !areEqual {
			return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, IdleSpecUnequal,
				IdleResources, m.resourceManager.idleResources.GetResource(offendingKind),
				m.resourceManager.specResources.GetResource(offendingKind))
		}

		// Next, check that our pending HostResources are equal to zero.
		isZero, offendingKind := m.resourceManager.pendingResources.IsZero()
		if !isZero {
			return NewInconsistentResourcesError(offendingKind, PendingNonzero,
				PendingResources, m.resourceManager.pendingResources.GetResource(offendingKind))
		}
	}

	return nil
}

func (m *AllocationManager) unsafeUnsubscribePendingResources(allocatedResources *types.DecimalSpec, key string) error {
	m.log.Debug("Deallocating pending resources. Current resources: %v. Resources to be deallocated: %v",
		m.resourceManager.GetResourceCountsAsString(), allocatedResources.String())

	if err := m.resourceManager.pendingResources.Subtract(allocatedResources); err != nil {
		return err
	}

	m.log.Debug("Deallocated pending resources. Updated pending resources: %v",
		m.resourceManager.pendingResources.String())

	m.numPendingAllocations.Add(-1)

	// Delete the allocation, since the replica was evicted.
	m.allocationKernelReplicaMap.Delete(key)

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	return nil
}

func (m *AllocationManager) unsafeAllocatePendingResources(decimalSpec *types.DecimalSpec, allocation scheduling.Allocation, key string, replicaId int32, kernelId string) error {
	// First, validate against this scheduling.Host's spec.
	if err := m.resourceManager.specResources.ValidateWithError(decimalSpec); err != nil {
		m.log.Warn("Replica %d of kernel \"%s\" is requesting more resources [%v] than host has available [%v]. Specific reason for subscription failure: %v.",
			replicaId, kernelId, decimalSpec.String(), m.resourceManager.specResources.GetResourceCountsAsString(), err)

		// TODO: Should this return an error? Shouldn't we just prohibit scheduling/allocating more resources than we can possibly provide?
		return err
	}

	m.log.Debug("Allocating pending resources. Current resources: %s. Resources to be allocated: %v.",
		m.resourceManager.GetResourceCountsAsString(), decimalSpec.String())

	// If we've gotten this far, then we have enough HostResources available to subscribe the requested HostResources
	// to the specified kernel replica. So, let's do that now.
	if err := m.resourceManager.pendingResources.Add(decimalSpec); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		return err
	}

	m.log.Debug("Allocated pending resources. New resource counts: %s.",
		m.resourceManager.GetResourceCountsAsString())

	// Store the allocation in the mapping.
	m.allocationKernelReplicaMap.Store(key, allocation)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Add(1)

	return nil
}

// unsafeDemoteCommittedAllocationToPendingAllocation performs any necessary state adjustments to the given
// Allocation in order to demote it from a CommittedAllocation to a PendingAllocation.
//
// unsafeDemoteCommittedAllocationToPendingAllocation does NOT acquire the AllocationManager's mutex and thus must be
// called from a context in which said mutex is already held.
//
// unsafeDemoteCommittedAllocationToPendingAllocation also does not perform any checks to verify that the given
// Allocation is of the correct type (i.e., CommittedAllocation, at the time of being passed to this method).
//
// unsafeDemoteCommittedAllocationToPendingAllocation does not perform any resource count modification to the
// AllocationManager. This is expected to have already been performed prior to calling this method.
func (m *AllocationManager) unsafeDemoteCommittedAllocationToPendingAllocation(allocation scheduling.Allocation) {
	// Set the AllocationType of the Allocation to PendingAllocation.
	allocation.SetAllocationType(scheduling.PendingAllocation)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Add(1)
	m.numCommittedAllocations.Add(-1)
}

// unsafeReleaseCommittedResources releases committed/bound HostResources from the kernel replica associated with
// the given Allocation.
//
// This function does NOT acquire the AllocationManager's mutex, nor does it perform any validation checks whatsoever.
// It is meant to be called from a context in which the AllocationManager's mutex is held and any appropriate
// checks are performed before the call to unsafeReleaseCommittedResources and after unsafeReleaseCommittedResources
// returns.
//
// The allocatedResources argument is optional. If it is passed as nil, then it will be assigned a value automatically
// by calling allocation.ToDecimalSpec(). If allocatedResources is non-nil, then it is necessarily expected to be
// the return value of allocation.ToDecimalSpec() (generated/called RIGHT before this function is called).
//
// If any of the resource modifications performed by this method return an error, then this method will panic.
//
// The only check that this method performs is whether the given scheduling.Allocation is nil.
// If the given scheduling.Allocation is nil, then this method will panic.
func (m *AllocationManager) unsafeReleaseCommittedResources(allocation scheduling.Allocation, allocatedResources *types.DecimalSpec) {
	if allocation == nil {
		panic("The provided Allocation cannot be nil.")
	}

	// If allocatedResources is nil, then call allocation.ToDecimalSpec() to populate allocatedResources with a value.
	if allocatedResources == nil {
		allocatedResources = allocation.ToDecimalSpec()
	}

	m.log.Debug("Releasing committed resources. Current resource counts: %s. Resources to be deallocated: %v.",
		m.resourceManager.GetResourceCountsAsString(), allocatedResources.String())

	// If we've gotten this far, then we have enough HostResources available to commit the requested HostResources
	// to the specified kernel replica. So, let's do that now. First, we'll increment the idle HostResources.
	if err := m.resourceManager.idleResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll increment the pending HostResources (since we're releasing committed HostResources).
	if err := m.resourceManager.pendingResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll decrement the committed HostResources (since we're releasing committed HostResources).
	if err := m.resourceManager.committedResources.Subtract(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	for _, gpuDeviceId := range allocation.GetGpuDeviceIds() {
		m.availableGpuDevices.Enqueue(gpuDeviceId)
	}

	// Clear the GpuDeviceIds field of the allocation.
	allocation.ClearGpuDeviceIds()

	m.log.Debug("Released committed resources. Updated resource counts: %s.", m.resourceManager.GetResourceCountsAsString())
}

// UnitTestingAllocationManager is a wrapper around AllocationManager with a few additional methods to facilitate
// unit testing with scheduling.UnitTestingHost instances.
type unitTestingAllocationManager struct {
	*AllocationManager
}

// updatePrometheusResourceMetrics updates all the resource-related Prometheus metrics.
// updatePrometheusResourceMetrics is used as a callback by the GPU/Resource Manager.
func (m *AllocationManager) unsafeUpdatePrometheusResourceMetrics() {
	if m.metricsManager == nil {
		m.log.Warn("Cannot update Prometheus resource metrics; manager has not been registered yet.")
		return
	}

	// CPU resource metrics.
	m.metricsManager.IdleCpuGauge.
		Set(m.resourceManager.idleResources.Millicpus())
	m.metricsManager.PendingCpuGauge.
		Set(m.resourceManager.pendingResources.Millicpus())
	m.metricsManager.CommittedCpuGauge.
		Set(m.resourceManager.committedResources.Millicpus())

	// Memory resource metrics.
	m.metricsManager.IdleMemoryGauge.
		Set(m.resourceManager.idleResources.MemoryMB())
	m.metricsManager.PendingMemoryGauge.
		Set(m.resourceManager.pendingResources.MemoryMB())
	m.metricsManager.CommittedMemoryGauge.
		Set(m.resourceManager.committedResources.MemoryMB())

	// GPU resource metrics.
	m.metricsManager.IdleGpuGauge.
		Set(m.resourceManager.idleResources.GPUs())
	m.metricsManager.PendingGpuGauge.
		Set(m.resourceManager.pendingResources.GPUs())
	m.metricsManager.CommittedGpuGauge.
		Set(m.resourceManager.committedResources.GPUs())
}

func NewUnitTestingAllocationManager(manager scheduling.AllocationManager) scheduling.UnitTestingAllocationManager {
	if _, ok := manager.(*unitTestingAllocationManager); ok {
		panic(
			fmt.Sprintf(
				"Cannot wrap AllocationManager \"%s\" (NodeId=\"%s\") in a UnitTestingAllocationManager as it is already a UnitTestingAllocationManager.",
				manager.GetId(), manager.GetNodeId()))
	}

	if _, ok := manager.(*AllocationManager); !ok {
		panic(
			fmt.Sprintf(
				"Cannot wrap AllocationManager \"%s\" (NodeId=\"%s\") in a UnitTestingAllocationManager as it is of an unknown or unsupported concrete type.",
				manager.GetId(), manager.GetNodeId()))
	}

	return &unitTestingAllocationManager{
		AllocationManager: manager.(*AllocationManager),
	}
}

// AddToCommittedResources is only intended to be used during unit tests.
func (m *unitTestingAllocationManager) AddToCommittedResources(spec *types.DecimalSpec) error {
	m.log.Debug("Incrementing committed resources by [%v]. Current committed: %s.",
		spec.String(), m.resourceManager.CommittedResources().String())
	err := m.resourceManager.CommittedResources().Add(spec)

	if err != nil {
		m.log.Debug("Failed to increment committed resources by [%v]. Current committed: %s.",
			spec.String(), m.resourceManager.CommittedResources().String())
		return err
	}

	m.log.Debug("Successfully incremented committed resources by [%v]. Current committed: %s.",
		spec.String(), m.resourceManager.CommittedResources().String())
	return nil
}

// SubtractFromCommittedResources is only intended to be used during unit tests.
func (m *unitTestingAllocationManager) SubtractFromCommittedResources(spec *types.DecimalSpec) error {
	m.log.Debug("Decrementing committed resources by [%v]. Current committed: %s.",
		spec.String(), m.resourceManager.CommittedResources().String())
	err := m.resourceManager.CommittedResources().Subtract(spec)

	if err != nil {
		m.log.Debug("Failed to decrement committed resources by [%v]. Current committed: %s.",
			spec.String(), m.resourceManager.CommittedResources().String())
		return err
	}

	m.log.Debug("Successfully decremented committed resources by [%v]. Current committed: %s.",
		spec.String(), m.resourceManager.CommittedResources().String())
	return nil
}

// AddToIdleResources is only intended to be used during unit tests.
func (m *unitTestingAllocationManager) AddToIdleResources(spec *types.DecimalSpec) error {
	m.log.Debug("Incrementing idle resources by [%v]. Current idle: %s.",
		spec.String(), m.resourceManager.IdleResources().String())
	err := m.resourceManager.IdleResources().Add(spec)

	if err != nil {
		m.log.Debug("Failed to increment idle resources by [%v]. Current idle: %s.",
			spec.String(), m.resourceManager.IdleResources().String())
		return err
	}

	m.log.Debug("Successfully incremented idle resources by [%v]. Current idle: %s.",
		spec.String(), m.resourceManager.IdleResources().String())
	return nil
}

// SubtractFromIdleResources is only intended to be used during unit tests.
func (m *unitTestingAllocationManager) SubtractFromIdleResources(spec *types.DecimalSpec) error {
	m.log.Debug("Decrementing idle resources by [%v]. Current idle: %s.",
		spec.String(), m.resourceManager.IdleResources().String())
	err := m.resourceManager.IdleResources().Subtract(spec)

	if err != nil {
		m.log.Debug("Failed to decrement idle resources by [%v]. Current idle: %s.",
			spec.String(), m.resourceManager.IdleResources().String())
		return err
	}

	m.log.Debug("Successfully decremented idle resources by [%v]. Current idle: %s.",
		spec.String(), m.resourceManager.IdleResources().String())
	return nil
}

// AddToPendingResources is only meant to be used during unit tests.
func (m *unitTestingAllocationManager) AddToPendingResources(spec *types.DecimalSpec) error {
	m.log.Debug("Incrementing pending resources by [%v]. Current pending: %s.",
		spec.String(), m.resourceManager.PendingResources().String())
	err := m.resourceManager.PendingResources().Add(spec)

	if err != nil {
		m.log.Debug("Failed to increment pending resources by [%v]. Current pending: %s.",
			spec.String(), m.resourceManager.PendingResources().String())
		return err
	}

	m.log.Debug("Successfully incremented pending resources by [%v]. Current pending: %s.",
		spec.String(), m.resourceManager.PendingResources().String())
	return nil
}

func (m *unitTestingAllocationManager) SubtractFromPendingResources(spec *types.DecimalSpec) error {
	m.log.Debug("Decrementing pending resources by [%v]. Current pending: %s.",
		spec.String(), m.resourceManager.PendingResources().String())
	err := m.resourceManager.PendingResources().Subtract(spec)

	if err != nil {
		m.log.Debug("Failed to decrement pending resources by [%v]. Current pending: %s.",
			spec.String(), m.resourceManager.PendingResources().String())
		return err
	}

	m.log.Debug("Successfully decremented pending resources by [%v]. Current pending: %s.",
		spec.String(), m.resourceManager.PendingResources().String())
	return nil
}
