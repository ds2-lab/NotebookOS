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
// TODO: Verify that all the cases in which the AllocationManager panics are legitimately panic-worthy, rather than scenarios
// that could arise during regular operation and should just be handled using the failure handler of whatever
// scheduling procedure we have in place.
type AllocationManager struct {
	mu sync.Mutex

	// ID is the unique ID of the AllocationManager. This is distinct from the NodeID.
	ID string

	// NodeID is the unique identifier of the node on which the AllocationManager exists.
	// This field is not populated immediately, as the LocalDaemon does not have an ID
	// when it is first created. Instead, the Cluster Gateway assigns an ID to the
	// LocalDaemon via the SetID gRPC call. The NodeID field of the AllocationManager
	// is assigned a value during the execution of the SetID RPC.
	NodeID string

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

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *Allocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *Allocation.
	//
	// allocationIdMap contains Allocation structs of both types (CommittedAllocation and PendingAllocation).
	allocationKernelReplicaMap hashmap.HashMap[string, *Allocation]

	// resourcesWrapper encapsulates the state of all HostResources (idle, pending, committed, and spec) managed
	// by this AllocationManager.
	resourcesWrapper *Manager

	// numPendingAllocations is the number of active Allocation instances of type PendingAllocation.
	numPendingAllocations types.StatInt32
	// numCommittedAllocations is the number of active Allocation instances of type CommittedAllocation.
	numCommittedAllocations types.StatInt32

	// availableGpuDevices is a queue.FifoQueue containing GPU device IDs.
	availableGpuDevices *queue.FifoQueue

	metricsManager *metrics.LocalDaemonPrometheusManager
}

// NewAllocationManager creates a new AllocationManager struct and returns a pointer to it.
func NewAllocationManager(resourceSpec types.Spec) *AllocationManager {
	manager := &AllocationManager{
		ID:                         uuid.NewString(),
		allocationKernelReplicaMap: hashmap.NewCornelkMap[string, *Allocation](128),
		availableGpuDevices:        queue.NewQueue(int(resourceSpec.GPU())),
	}

	for i := 0; i < int(resourceSpec.GPU()); i++ {
		manager.availableGpuDevices.Enqueue(i)
	}

	manager.resourcesWrapper = NewManager(resourceSpec)

	manager.numPendingAllocations.Store(0)
	manager.numCommittedAllocations.Store(0)

	config.InitLogger(&manager.log, manager)

	manager.log.Debug("Resource Manager initialized: %v", manager.resourcesWrapper.String())

	return manager
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

	containers := make([]*proto.ReplicaInfo, 0, m.numPendingAllocations)
	m.allocationKernelReplicaMap.Range(func(_ string, allocation *Allocation) (contd bool) {
		container := &proto.ReplicaInfo{
			KernelId:  allocation.KernelId,
			ReplicaId: allocation.ReplicaId,
		}

		containers = append(containers, container)

		return true
	})

	snapshotId := m.resourceSnapshotCounter.Add(1)
	snapshot := &ManagerSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          time.Now(),
		NodeId:             m.NodeID,
		ManagerId:          m.ID,
		IdleResources:      m.resourcesWrapper.idleResourcesSnapshot(snapshotId),
		PendingResources:   m.resourcesWrapper.pendingResourcesSnapshot(snapshotId),
		CommittedResources: m.resourcesWrapper.committedResourcesSnapshot(snapshotId),
		SpecResources:      m.resourcesWrapper.specResourcesSnapshot(snapshotId),
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

	idleSnapshot := m.resourcesWrapper.IdleProtoResourcesSnapshot(snapshotId)
	pendingSnapshot := m.resourcesWrapper.PendingProtoResourcesSnapshot(snapshotId)
	committedSnapshot := m.resourcesWrapper.CommittedProtoResourcesSnapshot(snapshotId)
	specSnapshot := m.resourcesWrapper.SpecProtoResourcesSnapshot(snapshotId)

	snapshot := &proto.NodeResourcesSnapshot{
		SnapshotId:         snapshotId,
		Timestamp:          timestamppb.Now(),
		NodeId:             m.NodeID,
		ManagerId:          m.ID,
		IdleResources:      idleSnapshot,
		PendingResources:   pendingSnapshot,
		CommittedResources: committedSnapshot,
		SpecResources:      specSnapshot,
	}

	return snapshot
}

// DebugSetIdleGPUs is a method used in unit tests to set the idle GPUs available within the AllocationManager
// to a specific value (typically zero).
func (m *AllocationManager) DebugSetIdleGPUs(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resourcesWrapper.idleResources.gpus = decimal.NewFromFloat(value)
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
	return m.resourcesWrapper.SpecResources().GPUsAsDecimal().Copy()
}

// SpecCPUs returns the total number of Millicpus configured/present on this node in millicpus.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecCPUs() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().MillicpusAsDecimal().Copy()
}

// SpecMemoryMB returns the total amount of memory in megabytes configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecMemoryMB() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().MemoryMbAsDecimal().Copy()
}

// SpecVRAM returns the amount of VRAM (in GB) that is configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) SpecVRAM() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().VRAMAsDecimal().Copy()
}

// SpecResources returns a snapshot of the working quantities of spec HostResources available
// on this node at the time at which the SpecResources method is called.
func (m *AllocationManager) SpecResources() *types.DecimalSpec {
	return m.resourcesWrapper.specResources.ToDecimalSpec()
}

// IdleGPUs returns the number of GPUs that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleGPUs() decimal.Decimal {
	return m.resourcesWrapper.IdleResources().GPUsAsDecimal().Copy()
}

// IdleCPUs returns the number of Millicpus that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleCPUs() decimal.Decimal {
	return m.resourcesWrapper.IdleResources().MillicpusAsDecimal().Copy()
}

// IdleMemoryMB returns the amount of memory (in MB) that is uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleMemoryMB() decimal.Decimal {
	return m.resourcesWrapper.IdleResources().MemoryMbAsDecimal().Copy()
}

// IdleVRamGB returns the amount of VRAM (in GB) that is uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) IdleVRamGB() decimal.Decimal {
	return m.resourcesWrapper.IdleResources().VRAMAsDecimal().Copy()
}

// IdleResources returns a snapshot of the working quantities of idle HostResources available
// on this node at the time at which the IdleResources method is called.
func (m *AllocationManager) IdleResources() *types.DecimalSpec {
	return m.resourcesWrapper.idleResources.ToDecimalSpec()
}

// CommittedGPUs returns the number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedGPUs() decimal.Decimal {
	return m.resourcesWrapper.CommittedResources().GPUsAsDecimal().Copy()
}

// CommittedCPUs returns the Millicpus, in millicpus, that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedCPUs() decimal.Decimal {
	return m.resourcesWrapper.CommittedResources().MillicpusAsDecimal().Copy()
}

// CommittedMemoryMB returns the amount of memory (in MB) that is actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedMemoryMB() decimal.Decimal {
	return m.resourcesWrapper.CommittedResources().MemoryMbAsDecimal().Copy()
}

// CommittedVRamGB returns the amount of VRAM (in GB) that is actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) CommittedVRamGB() decimal.Decimal {
	return m.resourcesWrapper.CommittedResources().VRAMAsDecimal().Copy()
}

// CommittedResources returns a snapshot of the working quantities of committed HostResources available
// on this node at the time at which the CommittedResources method is called.
func (m *AllocationManager) CommittedResources() *types.DecimalSpec {
	return m.resourcesWrapper.committedResources.ToDecimalSpec()
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
	return m.resourcesWrapper.PendingResources().GPUsAsDecimal().Copy()
}

// PendingCPUs returns the sum of the outstanding Millicpus of all replicas scheduled onto this node, in millicpus.
// Pending Millicpus are not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingCPUs() decimal.Decimal {
	return m.resourcesWrapper.PendingResources().MillicpusAsDecimal().Copy()
}

// PendingMemoryMB returns the sum of the outstanding memory of all replicas scheduled onto this node, in MB.
// Pending memory is not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingMemoryMB() decimal.Decimal {
	return m.resourcesWrapper.PendingResources().MemoryMbAsDecimal().Copy()
}

// PendingVRAM returns the sum of the outstanding VRAM of all replicas scheduled onto this node, in GB.
// Pending VRAM is not allocated or committed to a particular replica yet.
// The time at which HostResources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately.
// In other cases, they're committed only when the replica is actively training.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *AllocationManager) PendingVRAM() decimal.Decimal {
	return m.resourcesWrapper.PendingResources().VRAMAsDecimal().Copy()
}

// PendingResources returns a snapshot of the working quantities of pending HostResources available
// on this node at the time at which the PendingResources method is called.
func (m *AllocationManager) PendingResources() *types.DecimalSpec {
	return m.resourcesWrapper.pendingResources.ToDecimalSpec()
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
	if numGpusDecimal.LessThan(m.resourcesWrapper.committedResources.gpus) {
		return fmt.Errorf("%w: cannot set GPUs to value < number of committed GPUs (%s). Requested: %s",
			ErrIllegalGpuAdjustment, m.CommittedGPUs().StringFixed(1), numGpusDecimal.StringFixed(1))
	}

	difference := m.SpecGPUs().Sub(numGpusDecimal)

	oldSpecGPUs := m.SpecGPUs()
	m.resourcesWrapper.specResources.SetGpus(numGpusDecimal)
	m.log.Debug("Adjusted Spec GPUs from %s to %s.",
		oldSpecGPUs.StringFixed(1), numGpusDecimal.StringFixed(1))

	// If ORIGINAL - NEW > 0, then we're decreasing the total number of GPUs available.
	// So, we'll need to decrement the idle GPUs value.
	if difference.GreaterThan(decimal.Zero) {
		newIdleGPUs := m.IdleGPUs().Sub(difference)
		m.resourcesWrapper.idleResources.SetGpus(newIdleGPUs)
	} else {
		// ORIGINAL - NEW < 0, so we're adding GPUs.
		// We'll call difference.Abs(), as difference is negative.
		// Alternatively, we could do idleGPUs - difference, since we'd be subtracting a negative and thus adding.
		newIdleGPUs := m.IdleGPUs().Add(difference.Abs())
		m.resourcesWrapper.idleResources.SetGpus(newIdleGPUs)
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
		return alloc.GPUs.GreaterThan(decimal.Zero)
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
	return alloc.GPUs.GreaterThan(decimal.Zero)
}

// AssertAllocationIsPending returns true if the given *Allocation IS pending.
// If the given *Allocation is NOT pending, then this function will panic.
func (m *AllocationManager) AssertAllocationIsPending(allocation *Allocation) bool {
	if allocation.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %s", allocation.String()))
}

// AssertAllocationIsCommitted returns true if the given *Allocation is NOT pending.
// If the given *Allocation IS pending, then this function will panic.
func (m *AllocationManager) AssertAllocationIsCommitted(allocation *Allocation) bool {
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
	return m.numCommittedAllocations.LoadInt()
}

// NumPendingAllocations returns the Allocation instances whose AllocationType is PendingAllocation.
func (m *AllocationManager) NumPendingAllocations() int {
	return m.numPendingAllocations.LoadInt()
}

// PromoteReservation should be called when a kernel replica has won its leader election and begins executing code.
// This method simply records that the HostResources committed to the kernel are no longer "merely" a reservation.
// Instead, the resource allocation will indicate that they committed HostResources are being used by a kernel replica
// that is actively running user-submitted code.
//
// If there is no resource reservation (i.e., committed allocation whose IsReservation flag is set to true) for the
// specified kernel replica, then an error is returned. Likewise, if there is no committed allocation to begin with,
// then an error is returned (i.e., if there's no committed allocation whose IsReservation flag is either true or false).
func (m *AllocationManager) PromoteReservation(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot promote reserved HostResources for replica %d of kernel %s: no existing resource allocation found for that kernel replica.",
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
		allocation       *Allocation
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
// Precondition: there must already be a Allocation of type PendingAllocation associated with the specified
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
func (m *AllocationManager) CommitResources(replicaId int32, kernelId string, resourceRequestArg types.Spec, isReservation bool) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *Allocation
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
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsReservation=true.",
			replicaId, kernelId, allocation.AllocationType.String(), PendingResources.String())
		return nil, fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, PendingResources.String(), allocation.AllocationType.String())
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

	m.log.Debug("Attempting to commit the following HostResources to replica %d of kernel %s (isReservation=%v): %v",
		replicaId, kernelId, isReservation, requestedResources.String())

	// First, validate against this scheduling.Host's spec.
	if err := m.resourcesWrapper.specResources.ValidateWithError(requestedResources); err != nil {
		m.log.Warn("Could not commit the following HostResources to replica %d of kernel %s due "+
			"to insufficient host spec: %s. Specific reason for commitment failure: %v.",
			replicaId, kernelId, requestedResources.String(), err)
		return nil, err
	}

	// Next, validate against our actual idle resource capacity.
	if err := m.resourcesWrapper.idleResources.ValidateWithError(requestedResources); err != nil {
		m.log.Warn("Could not commit HostResources to replica %d of kernel %s: %s. "+
			"Reason for commitment failure: %v.", replicaId, kernelId, requestedResources.String(), err)
		return nil, err
	}

	m.log.Debug("Committing resources. Current resource counts: %s. Resources to be committed: %v.",
		m.resourcesWrapper.GetResourceCountsAsString(), requestedResources.String())

	// If we've gotten this far, then we have enough HostResources available to commit the requested HostResources
	// to the specified kernel replica. So, let's do that now. First, we'll decrement the idle HostResources.
	if err := m.resourcesWrapper.idleResources.Subtract(requestedResources); err != nil {
		return nil, err
	}

	// Next, we'll decrement the pending HostResources. We decrement because the HostResources are no longer "pending".
	// Instead, they are actively bound/committed to the kernel replica.
	if err := m.resourcesWrapper.pendingResources.Subtract(requestedResources); err != nil {
		return nil, err
	}

	// Next, we'll increment the committed HostResources.
	if err := m.resourcesWrapper.committedResources.Add(requestedResources); err != nil {
		return nil, err
	}

	// Finally, we'll update the Allocation struct associated with this request.
	// This involves updating the resource amounts stored in the Allocation as well as its AllocationType field.
	// The resource amounts may already match what was allocated, depending on if the resourceRequestArg parameter
	// was nil or not.
	//
	// Once updated, we'll remove it from the pending allocation maps and add it to the committed allocation maps.
	allocation.GPUs = requestedResources.GPUs.Copy()
	allocation.Millicpus = requestedResources.Millicpus.Copy()
	allocation.MemoryMB = requestedResources.MemoryMb.Copy()
	allocation.AllocationType = CommittedAllocation
	allocation.IsReservation = isReservation

	gpuDeviceIds := make([]int, 0, int(allocation.GPUs.InexactFloat64()))
	for len(gpuDeviceIds) < int(allocation.GPUs.InexactFloat64()) {
		val := m.availableGpuDevices.Dequeue()

		if val == nil {
			panic("Received nil GPU device ID when one should have been available.")
		}

		gpuDeviceId, ok := val.(int)
		if !ok {
			panic("Received nil GPU device ID when one should have been available.")
		}

		gpuDeviceIds = append(gpuDeviceIds, gpuDeviceId)
	}

	allocation.GpuDeviceIds = gpuDeviceIds

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Decr()
	m.numCommittedAllocations.Incr()

	m.log.Debug("Successfully committed the following HostResources to replica %d of kernel %s (isReservation=%v): %v",
		replicaId, kernelId, isReservation, requestedResources.String())
	m.log.Debug("Updated resource counts: %s.", m.resourcesWrapper.GetResourceCountsAsString())

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
		allocation       *Allocation
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
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsReservation=true.",
			replicaId, kernelId, allocation.AllocationType.String(), CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, CommittedAllocation.String(), allocation.AllocationType.String())
	}

	// Perform the resource count adjustments, as we've validated that everything is correct/as it should be.
	// We'll pass nil for the second argument as we don't need the *types.DecimalSpec anywhere else in
	// the ReleaseCommittedResources method.
	m.unsafeReleaseCommittedResources(allocation, allocation.ToDecimalSpec())

	m.log.Debug("Attempting to release the following committed HostResources from replica %d of kernel %s: %v. Current resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourcesWrapper.GetResourceCountsAsString())

	// Finally, we'll update the Allocation struct associated with this request.
	// This involves updating its AllocationType field to be PendingAllocation.
	//
	// We'll also adjust some internal counters that keep track of the number of pending and committed resource
	// allocations.
	m.unsafeDemoteCommittedAllocationToPendingAllocation(allocation)

	m.log.Debug("Successfully released the following (previously) committed HostResources to replica %d of kernel %s: %v. Updated resource counts: %v.",
		replicaId, kernelId, allocation.ToSpecString(), m.resourcesWrapper.GetResourceCountsAsString())

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
// KernelReplicaScheduled creates a Allocation of type PendingAllocation that is then associated with the
// newly-scheduled kernel replica.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) KernelReplicaScheduled(replicaId int32, kernelId string, spec types.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       *Allocation
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
		WithAllocationType(PendingAllocation).
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
		allocation       *Allocation
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
	m.log.Debug("Committed resources after removal: %s.", m.resourcesWrapper.CommittedResources().String())
	m.log.Debug("Pending resources after removal: %s.", m.resourcesWrapper.PendingResources().String())

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	return nil
}

// HasSufficientIdleResourcesAvailable returns true if there are sufficiently many idle HostResources available
// on the node such that the requested HostResources could be commited to a locally-running kernel replica.
func (m *AllocationManager) HasSufficientIdleResourcesAvailable(spec types.Spec) bool {
	return m.resourcesWrapper.idleResources.Validate(spec)
}

// HasSufficientIdleResourcesAvailableWithError returns true if there are sufficiently many idle HostResources available
// on the node such that the requested HostResources could be commited to a locally-running kernel replica.
//
// This method differs from HasSufficientIdleResourcesAvailable insofar as it returns an error encoding the resource(s)
// for which there are insufficient idle HostResources available.
func (m *AllocationManager) HasSufficientIdleResourcesAvailableWithError(spec types.Spec) (bool, error) {
	if err := m.resourcesWrapper.idleResources.ValidateWithError(spec); err != nil {
		return false, err
	}

	return true, nil
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
	hasNegative, kind := m.resourcesWrapper.idleResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, IdleResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Pending HostResources.
	hasNegative, kind = m.resourcesWrapper.pendingResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, PendingResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Committed HostResources.
	hasNegative, kind = m.resourcesWrapper.committedResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, CommittedResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	// Spec HostResources.
	hasNegative, kind = m.resourcesWrapper.specResources.HasNegativeField()
	if hasNegative {
		return NewInconsistentResourcesError(kind, NegativeResourceQuantity, SpecResources, m.resourcesWrapper.idleResources.GetResource(kind))
	}

	////////////////////////////////////////////////////////////////////////////////////////
	// Check that the idle and committed HostResources are no larger than the spec HostResources. //
	////////////////////////////////////////////////////////////////////////////////////////

	// Idle HostResources <= Spec HostResources.
	isOkay, offendingKind := m.resourcesWrapper.idleResources.LessThanOrEqual(m.resourcesWrapper.specResources)
	if !isOkay {
		return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, ResourceQuantityGreaterThanSpec,
			IdleResources, m.resourcesWrapper.idleResources.GetResource(offendingKind),
			m.resourcesWrapper.specResources.GetResource(offendingKind))
	}

	// Committed HostResources <= spec HostResources.
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

		// First, check that our idle HostResources are equal to our spec HostResources.
		areEqual, offendingKind := m.resourcesWrapper.idleResources.EqualTo(m.resourcesWrapper.specResources)
		if !areEqual {
			return NewInconsistentResourcesErrorWithResourceQuantity(offendingKind, IdleSpecUnequal,
				IdleResources, m.resourcesWrapper.idleResources.GetResource(offendingKind),
				m.resourcesWrapper.specResources.GetResource(offendingKind))
		}

		// Next, check that our pending HostResources are equal to zero.
		isZero, offendingKind := m.resourcesWrapper.pendingResources.IsZero()
		if !isZero {
			return NewInconsistentResourcesError(offendingKind, PendingNonzero,
				PendingResources, m.resourcesWrapper.pendingResources.GetResource(offendingKind))
		}
	}

	return nil
}

func (m *AllocationManager) unsafeUnsubscribePendingResources(allocatedResources *types.DecimalSpec, key string) error {
	m.log.Debug("Deallocating pending resources. Current resources: %v. Resources to be deallocated: %v",
		m.resourcesWrapper.GetResourceCountsAsString(), allocatedResources.String())

	if err := m.resourcesWrapper.pendingResources.Subtract(allocatedResources); err != nil {
		return err
	}

	m.log.Debug("Deallocated pending resources. Updated pending resources: %v",
		m.resourcesWrapper.pendingResources.String())

	m.numPendingAllocations.Decr()

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

func (m *AllocationManager) unsafeAllocatePendingResources(decimalSpec *types.DecimalSpec, allocation *Allocation, key string, replicaId int32, kernelId string) error {
	// First, validate against this scheduling.Host's spec.
	if err := m.resourcesWrapper.specResources.ValidateWithError(decimalSpec); err != nil {
		m.log.Error("Could not subscribe the following pending HostResources to replica %d of kernel %s due "+
			"to insufficient host spec: %s. Specific reason for subscription failure: %v.",
			replicaId, kernelId, decimalSpec.String(), err)
		return err
	}

	m.log.Debug("Allocating pending resources. Current resources: %s. Resources to be allocated: %v.",
		m.resourcesWrapper.GetResourceCountsAsString(), decimalSpec.String())

	// If we've gotten this far, then we have enough HostResources available to subscribe the requested HostResources
	// to the specified kernel replica. So, let's do that now.
	if err := m.resourcesWrapper.pendingResources.Add(decimalSpec); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		return err
	}

	m.log.Debug("Allocated pending resources. New resource counts: %s.",
		m.resourcesWrapper.GetResourceCountsAsString())

	// Store the allocation in the mapping.
	m.allocationKernelReplicaMap.Store(key, allocation)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()

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
func (m *AllocationManager) unsafeDemoteCommittedAllocationToPendingAllocation(allocation *Allocation) {
	// Set the AllocationType of the Allocation to PendingAllocation.
	allocation.AllocationType = PendingAllocation

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()
	m.numCommittedAllocations.Decr()
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
// The only check that this method performs is whether the given *Allocation is nil.
// If the given *Allocation is nil, then this method will panic.
func (m *AllocationManager) unsafeReleaseCommittedResources(allocation *Allocation, allocatedResources *types.DecimalSpec) {
	if allocation == nil {
		panic("The provided Allocation cannot be nil.")
	}

	// If allocatedResources is nil, then call allocation.ToDecimalSpec() to populate allocatedResources with a value.
	if allocatedResources == nil {
		allocatedResources = allocation.ToDecimalSpec()
	}

	m.log.Debug("Releasing committed resources. Current resource counts: %s. Resources to be deallocated: %v.",
		m.resourcesWrapper.GetResourceCountsAsString(), allocatedResources.String())

	// If we've gotten this far, then we have enough HostResources available to commit the requested HostResources
	// to the specified kernel replica. So, let's do that now. First, we'll increment the idle HostResources.
	if err := m.resourcesWrapper.idleResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll increment the pending HostResources (since we're releasing committed HostResources).
	if err := m.resourcesWrapper.pendingResources.Add(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	// Next, we'll decrement the committed HostResources (since we're releasing committed HostResources).
	if err := m.resourcesWrapper.committedResources.Subtract(allocatedResources); err != nil {
		// For now, let's panic, as this shouldn't happen. If there is an error, then it indicates that there's a bug,
		// as we passed all the validation checks up above.
		panic(err)
	}

	for _, gpuDeviceId := range allocation.GpuDeviceIds {
		m.availableGpuDevices.Enqueue(gpuDeviceId)
	}

	// Clear the GpuDeviceIds field of the allocation.
	allocation.GpuDeviceIds = []int{}

	m.log.Debug("Released committed resources. Updated resource counts: %s.", m.resourcesWrapper.GetResourceCountsAsString())
}
