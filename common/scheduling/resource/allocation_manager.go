package resource

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ReplicaIdForReservation is used when constructing keys for reservation-based allocations.
	//
	// Reservations are handled in a replica-agnostic manner. If and when the replica is fully scheduled
	// and the reservation is promoted, the associated Allocation is then remapped using the replica's
	// actual replica ID.
	ReplicaIdForReservation int32 = 0

	// ReplicaIdForReplicaNotFound is used/returned for replicaId returns when there is no such replica
	ReplicaIdForReplicaNotFound int32 = -999
)

var (
	ErrSomeReplicaAlreadyPresent = errors.New("another replica of the specified kernel is already present")
	ErrContainerNotPresent       = errors.New("container is not present; cannot remove container for specified replica")
	ErrDifferentContainerPresent = errors.New("a different replica than the one specified is present")
	ErrMismatchedExecutionIds    = errors.New("cannot complete requested operation, as existing allocation is associated with a different execution")
)

// scheduledKernels maintains information about the scheduling.Kernel instances for which an associated
// scheduling.KernelContainer is scheduled on the scheduling.Host whose resources are managed by the
// AllocationManager associated with the scheduledKernels.
type scheduledKernels struct {

	// Kernels is a map from kernel ID to a kernelContainers struct which keeps track of the
	// scheduling.KernelContainer instances running on the associated scheduling.Host.
	//
	// The existence of an entry for a particular kernel ID in the Kernels map does NOT indicate that
	// there is at least one scheduling.KernelContainer for that scheduling.Kernel. It does indicate that
	// there was at least one scheduling.KernelContainer for that scheduling.Kernel running on the associated
	// scheduling.Host at some point in time; however, there may no longer be any scheduling.KernelContainer for
	// that scheduling.Kernel running on the associated scheduling.Host anymore.
	Kernels map[string]int32

	// NodeId is the unique identifier of the node on which the scheduledContainers exists.
	NodeId string

	mu sync.Mutex
}

// GetScheduledReplica returns the replica ID of the scheduling.KernelReplica that is scheduled for the
// specified scheduling.Kernel.
//
// If no scheduling.KernelReplica of the specified scheduling.Kernel is scheduled, then ReplicaIdForReplicaNotFound
// is returned.
func (sk *scheduledKernels) GetScheduledReplica(kernelId string) int32 {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if loaded {
		return scheduledReplica
	}

	return ReplicaIdForReplicaNotFound
}

// IsAnyReplicaScheduled returns true if any scheduling.KernelReplica of the specified scheduling.Kernel is
// scheduled on this scheduling.Host.
func (sk *scheduledKernels) IsAnyReplicaScheduled(kernelId string) bool {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	_, loaded := sk.Kernels[kernelId]
	return loaded
}

// IsAnySpecificScheduled returns true if the specified scheduling.KernelReplica of the specified scheduling.Kernel is
// scheduled on this scheduling.Host.
func (sk *scheduledKernels) IsAnySpecificScheduled(replicaId int32, kernelId string) bool {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if !loaded {
		return false
	}

	return scheduledReplica == replicaId
}

// ReservationCreated is called to record that a reservation was created for an unspecified replica of the specified kernel.
func (sk *scheduledKernels) ReservationCreated(kernelId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if loaded {
		return fmt.Errorf("%w: '%d'", ErrSomeReplicaAlreadyPresent, scheduledReplica)
	}

	sk.Kernels[kernelId] = ReplicaIdForReservation
	return nil
}

// ReservationReleased is called to record that a reservation was released for an unspecified replica of the specified kernel.
func (sk *scheduledKernels) ReservationReleased(kernelId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	_, loaded := sk.Kernels[kernelId]
	if !loaded {
		return fmt.Errorf("%w: unspecified replica for which a reservation was supposedly created",
			ErrContainerNotPresent)
	}

	delete(sk.Kernels, kernelId)
	return nil
}

// ReplicaStartedRunning is called when a replica of a kernel begins running after originally
// having a reservation created for it.
func (sk *scheduledKernels) ReplicaStartedRunning(replicaId int32, kernelId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if !loaded {
		return fmt.Errorf("%w: expected to find generic reference to container of kernel \"%s\"",
			ErrContainerNotPresent, kernelId)
	}

	if scheduledReplica != ReplicaIdForReservation {
		return fmt.Errorf("%w: expected to find generic reference to container of kernel \"%s\", instead found replica %d",
			ErrSomeReplicaAlreadyPresent, kernelId, scheduledReplica)
	}

	sk.Kernels[kernelId] = replicaId
	return nil
}

// ReplicaScheduled is called to record that the specified replica of the specified kernel has been scheduled.
func (sk *scheduledKernels) ReplicaScheduled(replicaId int32, kernelId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if loaded {
		return fmt.Errorf("%w: '%d'", ErrSomeReplicaAlreadyPresent, scheduledReplica)
	}

	sk.Kernels[kernelId] = replicaId
	return nil
}

// ReplicaRemoved is called to record that the specified replica of the specified kernel has been scheduled.
func (sk *scheduledKernels) ReplicaRemoved(replicaId int32, kernelId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	scheduledReplica, loaded := sk.Kernels[kernelId]
	if !loaded {
		return fmt.Errorf("%w: '%d'", ErrContainerNotPresent, replicaId)
	}

	if scheduledReplica != replicaId {
		return fmt.Errorf("%w: present='%d', specified='%d'",
			ErrDifferentContainerPresent, scheduledReplica, replicaId)
	}

	delete(sk.Kernels, kernelId)
	return nil
}

func newScheduledKernels(nodeId string) *scheduledKernels {
	return &scheduledKernels{
		NodeId:  nodeId,
		Kernels: make(map[string]int32),
	}
}

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
	log logger.Logger // Logger.

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> scheduling.Allocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are scheduling.Allocation.
	//
	// allocationIdMap contains Allocation structs of both types (CommittedAllocation and PendingAllocation).
	allocationKernelReplicaMap hashmap.HashMap[string, scheduling.Allocation]

	// kernelAllocationMap is a map from Kernel ID to the allocation associated with any replica of that kernel.
	kernelAllocationMap hashmap.HashMap[string, scheduling.Allocation]

	// schedulingPolicy is the configured scheduling.Policy in use by the cluster.
	schedulingPolicy scheduling.Policy

	scheduledKernels *scheduledKernels

	// resourceManager encapsulates the state of all HostResources (idle, pending, committed, and spec) managed
	// by this AllocationManager.
	resourceManager *Manager

	// availableGpuDevices is a queue.Fifo containing GPU device IDs.
	availableGpuDevices *queue.Fifo[int]

	metricsManager *metrics.LocalDaemonPrometheusManager

	updateIndex func(replicaId int32, kernelId string) error

	updateSubscriptionRatio func() decimal.Decimal

	// GetId is the unique identifier of the AllocationManager. This is distinct from the NodeId.
	Id string

	NodeName string

	// NodeId is the unique identifier of the node on which the AllocationManager exists.
	// This field is not populated immediately, as the LocalDaemon does not have an ID
	// when it is first created. Instead, the Cluster Gateway assigns an ID to the
	// LocalDaemon via the SetID gRPC call. The NodeId field of the AllocationManager
	// is assigned a value during the execution of the SetID RPC.
	NodeId string

	mu sync.Mutex

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

	// numFailedReservations is the number of times we attempted to reserve resources and failed to do so.
	numFailedReservations atomic.Int32

	// numFailedPreCommitments is the number of times we attempted to pre-commit resources and failed to do so.
	numFailedPreCommitments atomic.Int32
}

// NewAllocationManager creates a new AllocationManager struct and returns a pointer to it.
func NewAllocationManager(resourceSpec types.Spec, schedulingPolicy scheduling.Policy, nodeId string, nodeName string) *AllocationManager {
	manager := &AllocationManager{
		Id:                         uuid.NewString(),
		NodeName:                   nodeName,
		NodeId:                     nodeId,
		allocationKernelReplicaMap: hashmap.NewCornelkMap[string, scheduling.Allocation](128),
		kernelAllocationMap:        hashmap.NewCornelkMap[string, scheduling.Allocation](128),
		availableGpuDevices:        queue.NewFifo[int](int(resourceSpec.GPU())),
		schedulingPolicy:           schedulingPolicy,
		resourceManager:            NewManager(resourceSpec),
		scheduledKernels:           newScheduledKernels(nodeId),
		log:                        config.GetLogger(fmt.Sprintf("AllocationManager %s ", nodeName)),
	}

	for i := 0; i < int(resourceSpec.GPU()); i++ {
		manager.availableGpuDevices.Enqueue(i)
	}

	// Initialize all of these counters to 0.
	manager.numPendingAllocations.Store(0)
	manager.numCommittedAllocations.Store(0)
	manager.numPreCommitments.Store(0)
	manager.numReservations.Store(0)
	manager.numFailedReservations.Store(0)
	manager.numFailedPreCommitments.Store(0)

	manager.log.Debug("Resource Manager initialized: %v", manager.resourceManager.String())

	return manager
}

func (m *AllocationManager) SetUpdateIndex(updateIndex func(replicaId int32, kernelId string) error) {
	m.updateIndex = updateIndex
}

func (m *AllocationManager) SetUpdateSubscriptionRatio(updateSubscriptionRatio func() decimal.Decimal) {
	m.updateSubscriptionRatio = updateSubscriptionRatio
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

func (m *AllocationManager) GetNodeName() string {
	return m.NodeName
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
			ErrIllegalResourceAdjustment, m.CommittedGPUs().StringFixed(1), numGpusDecimal.StringFixed(1))
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
	return alloc != nil && alloc.IsCommitted()
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

// KernelHasCommittedResources returns true if any replica of the specified kernel has resources committed to it.
func (m *AllocationManager) KernelHasCommittedResources(kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	alloc, ok := m.kernelAllocationMap.Load(kernelId)
	if !ok {
		return false
	}

	return alloc != nil && alloc.IsCommitted()
}

// HasReservationForKernel returns true if the target Host has a reservation for the specified kernel.
func (m *AllocationManager) HasReservationForKernel(kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	alloc, ok := m.kernelAllocationMap.Load(kernelId)
	if !ok {
		return false
	}

	return alloc != nil && alloc.IsReservation()
}

// AdjustKernelResourceRequest when the ResourceSpec of a KernelContainer that is already scheduled on this
// Host is updated or changed. This ensures that the Host's resource counts are up to date.
func (m *AllocationManager) AdjustKernelResourceRequest(newSpec types.Spec, oldSpec types.Spec, container scheduling.KernelContainer) error {
	// Ensure that we're even allowed to do this (based on the scheduling policy).
	if !m.schedulingPolicy.SupportsDynamicResourceAdjustments() {
		return fmt.Errorf("%w (\"%s\")", scheduling.ErrDynamicResourceAdjustmentProhibited, m.schedulingPolicy.Name())
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	kernelId := container.KernelID()
	replicaId := container.ReplicaId()

	m.log.Debug("Attempting to adjust resource request for replica %d of kernel %s from [%v] to [%v].",
		replicaId, kernelId, oldSpec, newSpec)

	// Verify that the container is in fact scheduled on this Host.
	if !m.scheduledKernels.IsAnyReplicaScheduled(kernelId) {
		return fmt.Errorf("%w: replica %d of kernel %s",
			ErrContainerNotPresent, replicaId, kernelId)
	}

	// Retrieve the allocation, as we'll have to update it.
	key := getKey(replicaId, kernelId)
	allocation, loaded := m.allocationKernelReplicaMap.Load(key)
	if !loaded {
		m.log.Warn("Cannot adjust resource request for replica %d of kernel %s because no allocation exists...",
			replicaId, kernelId)
		return fmt.Errorf("%w: replica %d of kernel %s", ErrAllocationNotFound, replicaId, kernelId)
	}

	err := m.resourceManager.RunTransaction(func(state scheduling.TransactionState) {
		if allocation.IsPending() {
			state.PendingResources().Subtract(oldSpec)
			state.PendingResources().Add(newSpec)
		} else {
			m.log.Warn("Adjusting resource request of replica %d of kernel %s while resources are COMMITTED...",
				replicaId, kernelId)

			state.CommittedResources().Subtract(oldSpec)
			state.CommittedResources().Add(newSpec)
		}
	})

	if err != nil {
		m.log.Debug("Could not adjust resource request for replica %d of kernel %s from [%v] to [%v] because: %v",
			replicaId, kernelId, oldSpec, newSpec, err)
		return err
	}

	decimalSpec := types.ToDecimalSpec(newSpec)
	allocation.SetGpus(decimalSpec.GPUs)
	allocation.SetMillicpus(decimalSpec.Millicpus)
	allocation.SetMemoryMb(decimalSpec.MemoryMb)
	allocation.SetVramGb(decimalSpec.VRam)

	m.allocationKernelReplicaMap.Store(key, allocation)
	m.kernelAllocationMap.Store(kernelId, allocation)

	return nil
}

// AdjustKernelResourceRequestCoordinated when the ResourceSpec of a KernelContainer that is already scheduled on
// this Host is updated or changed. This ensures that the Host's resource counts are up to date.
//
// This version runs in a coordination fashion and is used when updating the resources of multi-replica kernels.
func (m *AllocationManager) AdjustKernelResourceRequestCoordinated(newSpec types.Spec, oldSpec types.Spec,
	container scheduling.KernelContainer, schedulingMutex *sync.Mutex, tx scheduling.CoordinatedTransaction) error {

	// Ensure that we're even allowed to do this (based on the scheduling policy).
	if !m.schedulingPolicy.SupportsDynamicResourceAdjustments() {
		return fmt.Errorf("%w (\"%s\")", scheduling.ErrDynamicResourceAdjustmentProhibited, m.schedulingPolicy.Name())
	}

	kernelId := container.KernelID()
	replicaId := container.ReplicaId()

	m.mu.Lock()
	targetReplicaIsScheduled := m.scheduledKernels.IsAnySpecificScheduled(replicaId, kernelId)

	// Verify that the container is in fact scheduled on this Host.
	if !targetReplicaIsScheduled {
		m.mu.Unlock()
		return fmt.Errorf("%w: replica %d of kernel %s",
			ErrContainerNotPresent, replicaId, kernelId)
	}

	// Retrieve the allocation, as we'll have to update it.
	key := getKey(replicaId, kernelId)
	allocation, loaded := m.allocationKernelReplicaMap.Load(key)
	if !loaded {
		m.log.Warn("Cannot adjust resource request for replica %d of kernel %s because no allocation exists...",
			replicaId, kernelId)
		m.mu.Unlock()
		return fmt.Errorf("%w: replica %d of kernel %s", ErrAllocationNotFound, replicaId, kernelId)
	}
	m.mu.Unlock()

	// Verify that the allocation is not committed.
	if allocation.IsCommitted() {
		return fmt.Errorf("%w: allocation for replica %d of kernel %s is committed; cannot dynamically adjust",
			ErrInvalidAllocationType, replicaId, kernelId)
	}

	txOperation := func(state scheduling.TransactionState) {
		state.PendingResources().Subtract(oldSpec)
		state.PendingResources().Add(newSpec)
	}

	m.log.Debug("Preparing to register replica %d of kernel %s from host %s for coordinated transaction %s.",
		replicaId, container.KernelID(), m.NodeName, tx.Id())

	// Register ourselves as a participant.
	// If we're the last participant to do so, then this will also initialize all the participants.
	err := tx.RegisterParticipant(replicaId, m.resourceManager.GetTransactionData, txOperation, schedulingMutex)
	if err != nil {
		m.log.Error("Received error upon registering for coordination transaction %s when updating spec of replica %d of kernel %s from [%s] to [%s]: %v",
			tx.Id(), replicaId, kernelId, oldSpec.String(), newSpec.String(), err)
		return err
	}

	// Ensure that we're all initialized. We may not have been the last to register.
	tx.WaitForParticipantsToBeInitialized()

	m.mu.Lock()
	defer m.mu.Unlock()

	err = tx.Run() // This will block until the other AllocationManager's also call Run.
	if err == nil {
		m.log.Debug("Successfully updated resource spec of replica %d of kernel %s.",
			replicaId, container.KernelID())

		decimalSpec := types.ToDecimalSpec(newSpec)
		allocation.SetGpus(decimalSpec.GPUs)
		allocation.SetMillicpus(decimalSpec.Millicpus)
		allocation.SetMemoryMb(decimalSpec.MemoryMb)
		allocation.SetVramGb(decimalSpec.VRam)

		m.allocationKernelReplicaMap.Store(key, allocation)
		m.kernelAllocationMap.Store(kernelId, allocation)

		if m.updateIndex != nil {
			err = m.updateIndex(replicaId, kernelId)
		}

		if m.updateSubscriptionRatio != nil {
			m.updateSubscriptionRatio()
		}

		return nil
	}

	err = tx.FailureReason()

	// If the error is nil, which really shouldn't happen, then we'll just assign a generic
	// error to the err variable before returning it.
	if err == nil {
		m.log.Warn("Transaction %s targeting all replicas of kernel %s has failed, but the failure reason is nil...",
			tx.Id(), kernelId)
		// err = fmt.Errorf("%w: failure reason unspecified", transaction.ErrTransactionFailed)

		err = transaction.NewErrTransactionFailed(fmt.Errorf("failure reason unspecified"),
			[]scheduling.ResourceKind{scheduling.UnknownResource}, []scheduling.ResourceStatus{scheduling.UnknownStatus})
	}

	m.log.Debug("Transaction %s targeting replica %d of kernel %s has failed because: %v",
		tx.Id(), replicaId, kernelId, err)

	return err
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

// GetReservation returns the scheduling.ResourceReservation associated with the specified kernel, if one exists.
func (m *AllocationManager) GetReservation(kernelId string) (scheduling.Allocation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getKey(ReplicaIdForReservation, kernelId)

	alloc, loaded := m.allocationKernelReplicaMap.Load(key)
	if !alloc.IsReservation() {
		return nil, false
	}

	return alloc, loaded
}

// NumReservations returns the number of active reservations on the scheduling.Host.
func (m *AllocationManager) NumReservations() int {
	return int(m.numReservations.Load())
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

// PromotePreCommitment should be called when a kernel replica has won its leader election and begins executing code.
// This method simply records that the HostResources committed to the kernel are no longer "merely" a reservation.
// Instead, the resource allocation will indicate that they committed HostResources are being used by a kernel replica
// that is actively running user-submitted code.
//
// If there is no resource reservation (i.e., committed allocation whose IsPreCommittedAllocation flag is set to true) for the
// specified kernel replica, then an error is returned. Likewise, if there is no committed allocation to begin with,
// then an error is returned (i.e., if there's no committed allocation whose IsPreCommittedAllocation flag is either true or false).
func (m *AllocationManager) PromotePreCommitment(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot promote reserved HostResources for replica %d of kernel %s: no existing resource allocation found for that kernel replica (under key \"%s\").",
			replicaId, kernelId, key)
	}

	if allocation.IsPending() {
		m.log.Warn("Found existing resource allocation for replica %d of kernel %s; "+
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
	m.numPreCommitments.Add(-1)

	return nil
}

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
		m.log.Error("Cannot adjust pending resources of replica %d of kernel %s: "+
			"no existing resource allocation associated with that kernel replica.", replicaId, kernelId)
		return fmt.Errorf("%w: replica %d of kernel %s", ErrAllocationNotFound, replicaId, kernelId)
	}

	if allocation.IsCommitted() {
		m.log.Error("Cannot adjust resources of replica %d of kernel %s, "+
			"as resources are already committed to that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: could not find existing pending resource allocation for replica %d of kernel %s",
			scheduling.ErrInvalidOperation, replicaId, kernelId)
	}

	// First, release the original amount of pending resources.
	originalAllocatedResources := allocation.ToDecimalSpec()
	err := m.releasePendingResources(originalAllocatedResources, replicaId, kernelId)
	if err != nil {
		m.log.Error("Failed to release original amount of pending resources during resource adjustment of replica %d of kernel %s because: %v",
			replicaId, kernelId, err)
		return err
	}

	decimalSpec := types.ToDecimalSpec(updatedSpec)
	adjustedAllocation := allocation.CloneAndReturnedAdjusted(decimalSpec)

	// Next, attempt to reserve the updated amount (which could be more or less than the original amount).
	err = m.allocatePendingResources(decimalSpec, adjustedAllocation, key, replicaId, kernelId)
	if err != nil {
		m.log.Warn("Failed to allocate updated pending resources %s during resource adjustment of replica %d of kernel %s because: %v",
			decimalSpec.String(), replicaId, kernelId, err)

		// Rollback.
		rollbackErr := m.allocatePendingResources(originalAllocatedResources, allocation, key, replicaId, kernelId)
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

func (m *AllocationManager) PreCommitResourcesToExistingContainer(replicaId int32, kernelId string, executionId string,
	resourceRequestArg types.Spec, gpuDeviceIds []int) ([]int, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.scheduledKernels.IsAnyReplicaScheduled(kernelId) {
		m.log.Warn("Cannot pre-commit resources to replica %d of kernel %s, as no replicas of kernel %s exist on host %s.",
			replicaId, kernelId, kernelId, m.NodeName)
		return nil, fmt.Errorf("%w: no replicas of kernel %s are scheduled (including specified replica %d)",
			ErrContainerNotPresent, kernelId, replicaId)
	}

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Cannot commit HostResources to replica %d of kernel %s: no existing resource allocation "+
			"found for that kernel replica (under key \"%s\").", replicaId, kernelId, key)
		return nil, fmt.Errorf("%w: no resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Check if there's already an existing committed allocation for the specified kernel replica.
	if allocation.IsCommitted() {
		// Check if the existing committed allocation for the specified kernel replica is a pre-commit.
		// If it isn't, then that's very problematic.
		if !allocation.IsPreCommitted() {
			m.log.Error("Found existing (non-pre) committed allocation for replica %d of kernel %s with resources "+
				"%v while trying to commit resources %v for execution \"%s\"...",
				replicaId, kernelId, allocation.ToSpecString(), resourceRequestArg.String(), executionId)

			return allocation.GetGpuDeviceIds(), fmt.Errorf("%w: replica %d of kernel %s",
				scheduling.ErrResourcesAlreadyCommitted, replicaId, kernelId)
		}

		// If the allocation exists, is a pre-commit, and the execution IDs match, then we're OK. Just return now.
		if allocation.GetExecutionId() == executionId {
			m.log.Debug("TransactionResources are already pre-commited to replica %d of kernel \"%s\" for execution \"%s\". "+
				"No need to pre-commit them.", replicaId, kernelId, executionId)

			return allocation.GetGpuDeviceIds(), nil
		}

		// The allocation exists, it's a pre-commit, but the execution IDs do NOT match.
		// So, there were some resources that were already pre-committed, but they were for a different execution.
		// We'll assume that the existing pre-committed resources are now old/out-of-date.
		// We'll replace the existing allocation with the new one that is being requested now.
		previousExecutionId := allocation.GetExecutionId()

		m.log.Warn("TransactionResources were already pre-committed to replica %d of kernel \"%s\" for execution \"%s\".",
			replicaId, kernelId, previousExecutionId)
		m.log.Warn("However, we're supposed to pre-commit resources [%v] to replica %d of kernel \"%s\" for a new "+
			"execution with ID=\"%s\".", resourceRequestArg.String(), replicaId, kernelId, executionId)
		m.log.Warn("Will replace pre-commitment for execution \"%s\" with pre-commitment for execution \"%s\" "+
			"(for replica %d of kernel \"%s\")", previousExecutionId, executionId, replicaId, kernelId)

		// Release the old pre-committed resources (for the previous execution ID).
		// We hold the schedulingMutex right now, so this is all occurring atomically.
		// We'll create the new pre-committed resource reservation down below.
		err := m.releaseCommittedResources(allocation, nil, true)
		if err != nil {
			m.log.Error("Failed to release pre-committed resources for replica %d of kernel \"%s\" during "+
				"replacement of pre-commitment for execution \"%s\" with pre-commitment for execution \"%s\": %v",
				replicaId, kernelId, previousExecutionId, executionId)
			return nil, err
		}
	}

	// Ensure this is set to false in case there's an issue with our attempt to pre-commit.
	allocation.SetIsPreCommitted(false)

	decimalSpec := types.ToDecimalSpec(resourceRequestArg)

	err := m.allocateCommittedResources(replicaId, kernelId, decimalSpec, allocation, true,
		true, executionId, key, gpuDeviceIds)
	if err != nil {
		m.log.Warn("Failed to pre-commit resources [%v] to replica %d of kernel %s: %v",
			decimalSpec.String(), replicaId, kernelId, err)
		return nil, err
	}

	return allocation.GetGpuDeviceIds(), nil
}

// CommitResourcesToExistingContainer commits/binds HostResources to a particular kernel replica, such that the HostResources are reserved for
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
func (m *AllocationManager) CommitResourcesToExistingContainer(replicaId int32, kernelId string, executionId string,
	resourceRequestArg types.Spec, isPreCommitment bool) ([]int, error) {
	if isPreCommitment {
		m.log.Warn("Request to pre-commit resources to replica %d of kernel %s is using CommitResourcesToExistingContainer instead of PreCommitResourcesToExistingContainer. Please update the API call.",
			replicaId, kernelId)
		return m.PreCommitResourcesToExistingContainer(replicaId, kernelId, executionId, resourceRequestArg, nil)
	}

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
			"found for that kernel replica (under key \"%s\").", replicaId, kernelId, key)
		return nil, fmt.Errorf("%w: no resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be committed.
	if allocation.IsCommitted() {
		m.log.Warn("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsPreAllocation=true.",
			replicaId, kernelId, allocation.GetAllocationType().String(), scheduling.PendingResources.String())
		return nil, fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, scheduling.PendingResources.String(), allocation.GetAllocationType().String())
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

	// Next, execute an (atomic) transaction which modifies resources of all relevant statuses/types.
	err := m.allocateCommittedResources(replicaId, kernelId, requestedResources, allocation, true,
		isPreCommitment, executionId, key, nil)
	if err != nil {
		return nil, err
	}

	return allocation.GetGpuDeviceIds(), nil
}

// ReleaseCommittedResources uncommits/unbinds HostResources from a particular kernel replica, such that the
// HostResources are made available for use by other kernel replicas.
//
// ReleaseCommittedResources is intended only to be used when the replica for which committed resources are being
// released is still going to be running on the host. If the replica is being evicted, then ReplicaEvicted should
// be called instead.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) ReleaseCommittedResources(replicaId int32, kernelId string, executionId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		// It's OK for there to be an error in some cases -- if we receive the "execute_reply" from the leader
		// first, and we pre-committed resources to all replicas, then we release the pre-commitments at that
		// point. So, if we attempt to release the same pre-commitment when we receive "execute_reply" from
		// the follower replicas, then that release will fail (because it will have already occurred).
		m.log.Warn("Cannot release committed HostResources bound to replica %d of kernel %s: no existing resource "+
			"allocation found for that kernel replica. (under key \"%s\").", replicaId, kernelId, key)
		return fmt.Errorf("%w: no committed resource allocation found for replica %d of kernel %s",
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
		m.log.Warn("Found existing resource allocation for replica %d of kernel %s; "+
			"however, resource allocation is of type '%s'. Expected an allocation of type '%s' with IsPreCommittedAllocation=true.",
			replicaId, kernelId, allocation.GetAllocationType().String(), scheduling.CommittedAllocation.String())
		return fmt.Errorf("%w: expected '%s', found '%s'",
			ErrInvalidAllocationType, scheduling.CommittedAllocation.String(), allocation.GetAllocationType().String())
	}

	prevExecutionId := allocation.GetExecutionId()

	// If this allocation is actually associated with a particular "execute_request" message, and the execution ID
	// we received is valid (i.e., it isn't equal to scheduling.DefaultExecutionId), then let's make sure that we're
	// deallocating resources for the correct "execute_request" message.
	if prevExecutionId != scheduling.DefaultExecutionId && executionId != scheduling.DefaultExecutionId && allocation.GetExecutionId() != executionId {
		m.log.Warn("Committed allocation of [%v] for replica %d of kernel %s is associated with execution \"%s\"; however, de-commit request is for execution \"%s\". Ignoring.",
			allocation.ToSpecString(), replicaId, kernelId, prevExecutionId, executionId)

		return fmt.Errorf("%w: \"%s\" (requested execution = \"%s\")",
			ErrMismatchedExecutionIds, prevExecutionId, executionId)
	}

	// Perform the resource count adjustments, as we've validated that everything is correct/as it should be.
	// We'll pass nil for the second argument as we don't need the *types.DecimalSpec anywhere else in
	// the ReleaseCommittedResources method.
	err := m.releaseCommittedResources(allocation, allocation.ToDecimalSpec(), true)
	if err != nil {
		m.log.Error(
			utils.RedStyle.Render(
				"Failed to release resources committed to replica %d of kernel %s: %v"), replicaId, kernelId, err)

		panic(err)
	}

	// Set the AllocationType of the Allocation to PendingAllocation.
	allocation.SetAllocationType(scheduling.PendingAllocation)

	return nil
}

// ReleaseReservation is to be called when a resource reservation should be released because the
// scheduling of the associated replica of the associated kernel is being aborted.
//
// ReleaseReservation is the inverse of ReserveResources.
func (m *AllocationManager) ReleaseReservation(spec *proto.KernelSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	kernelId := spec.Id

	// First, verify that there is in fact a reservation for the specified kernel.
	if scheduledReplica := m.scheduledKernels.GetScheduledReplica(kernelId); scheduledReplica == ReplicaIdForReplicaNotFound {
		m.log.Warn("Cannot release reservation for replica of kernel %s: no reservation found.", kernelId)
		return fmt.Errorf("%w: \"%s\"", ErrReservationNotFound, kernelId)
	}

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	// Verify that there does not already exist an allocation associated with the specified kernel replica.
	key = getKey(ReplicaIdForReservation, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Warn("Cannot release reservation for replica of kernel %s: no reservation found. (under key \"%s\").",
			ReplicaIdForReservation, kernelId, key)
		return fmt.Errorf("%w: \"%s\"", ErrReservationNotFound, kernelId)
	}

	if !allocation.IsReservation() {
		panic(fmt.Sprintf("Expected allocation for kernel \"%s\" to be an allocation.", kernelId))
	}

	var err error
	if allocation.IsCommitted() {
		err = m.releaseCommittedResources(allocation, allocation.ToDecimalSpec(), false)
	} else {
		err = m.releasePendingResources(allocation.ToDecimalSpec(), ReplicaIdForReservation, kernelId)
	}

	if err != nil {
		m.log.Error(
			utils.RedStyle.Render(
				"Failed to release resources reserved for replica of kernel %s from host %s: %v"),
			kernelId, m.NodeName, err)

		panic(err)
	}

	err = m.scheduledKernels.ReservationReleased(kernelId)
	if err != nil {
		m.log.Error(
			utils.RedStyle.Render(
				"Failed to release resources reserved for replica of kernel %s from host %s: %v"),
			kernelId, m.NodeName, err)

		panic(err)
	}

	m.numReservations.Add(-1)

	return nil
}

// ReserveResources creates a NEW Allocation for the specified replica of the specified kernel.
//
// The Allocation may be of either type -- scheduling.PendingAllocation or scheduling.CommittedAllocation --
// depending upon the value of the 'usePending' parameter.
//
// The new Allocation will be created with the additional piece of metadata indicating that it is a reservation,
// rather than an Allocation for an actively-running scheduling.KernelContainer.
//
// The types.Spec argument encodes the amount of resources to reserve.
//
// ReserveResources is the inverse of ReleaseReservation.
func (m *AllocationManager) ReserveResources(replicaId int32, kernelId string, spec types.Spec, usePending bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify that there are no other replicas of the specified kernel scheduled on this host.
	if scheduledReplica := m.scheduledKernels.GetScheduledReplica(kernelId); scheduledReplica != ReplicaIdForReplicaNotFound {
		numFailedReservations := m.numFailedReservations.Add(1)
		m.log.Debug("Cannot reserve resources for replica of kernel %s: found existing resource "+
			"allocation associated to that another replica of that kernel (replica %d) [numFailedReservations=%d].",
			kernelId, scheduledReplica, numFailedReservations)
		return fmt.Errorf("%w: existing resource allocation found for another replica of kernel %s (%d)",
			ErrInvalidAllocationRequest, kernelId, scheduledReplica)
	}

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
		allocationType   scheduling.AllocationType
	)

	if usePending {
		allocationType = scheduling.PendingAllocation
	} else {
		allocationType = scheduling.CommittedAllocation
	}

	// If a specific replica was specified, then we'll check for existing allocations for that specific
	// replica as well as for an unspecified replica.
	if replicaId >= 1 {
		// Verify that there does not already exist an allocation associated with the specified kernel replica.
		key = getKey(replicaId, kernelId)
		if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); allocationExists {
			numFailedReservations := m.numFailedReservations.Add(1)
			m.log.Debug("Cannot create reservation for replica %d of kernel %s: found existing resource "+
				"allocation associated with specific replica %d [numFailedReservations=%d]: %s",
				replicaId, kernelId, replicaId, numFailedReservations, allocation.String())
			return fmt.Errorf("%w: existing resource allocation found for replica %d of kernel %s",
				ErrInvalidAllocationRequest, replicaId, kernelId)
		}
	}

	// Verify that there does not already exist an allocation associated with an unspecified kernel replica.
	key = getKey(ReplicaIdForReservation, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); allocationExists {
		numFailedReservations := m.numFailedReservations.Add(1)
		m.log.Debug("Cannot create reservation for new replica of kernel %s: found existing resource "+
			"allocation associated with unspecified replica [numFailedReservations=%d]: %s",
			kernelId, numFailedReservations, allocation.String())
		return fmt.Errorf("%w: existing resource allocation found for unspecified replica of kernel %s",
			ErrInvalidAllocationRequest, kernelId)
	}

	// Construct the new Allocation using the resource quantities specified in the spec argument.
	allocation = NewResourceAllocationBuilder().
		WithAllocationType(allocationType).
		WithKernelReplica(ReplicaIdForReservation, kernelId).
		WithMillicpus(spec.CPU()).
		WithMemoryMB(spec.MemoryMB()).
		WithGPUs(spec.GPU()).
		WithVRAM(spec.VRAM()).
		WithHostId(m.NodeId).
		WithHostName(m.NodeName).
		WithExecutionId(scheduling.DefaultExecutionId).
		IsAReservation().
		IsNotAPreCommitment().
		BuildResourceAllocation()

	var (
		// Convert the given types.Spec argument to a *types.DecimalSpec struct.
		requestedResources = types.ToDecimalSpec(spec)
		err                error
		status             scheduling.ResourceStatus
	)

	if usePending {
		status = scheduling.PendingResources

		m.log.Debug("Attempting to create %v reservation for replica of kernel %s: %v",
			status, kernelId, spec.String())

		err = m.allocatePendingResources(requestedResources, allocation, key, ReplicaIdForReservation, kernelId)
	} else {
		status = scheduling.CommittedResources

		m.log.Debug("Attempting to create %v reservation for replica of kernel %s: %v",
			status, kernelId, spec.String())

		err = m.allocateCommittedResources(ReplicaIdForReservation, kernelId, requestedResources, allocation, false,
			false, scheduling.DefaultExecutionId, key, nil)
	}

	if err != nil {
		numFailedReservations := m.numFailedReservations.Add(1)
		m.log.Debug("Failed to allocate %s resources %s to replica of kernel %s [numFailedReservations=%d]: %v",
			status.String(), requestedResources.String(), kernelId, numFailedReservations, err)
		return err
	}

	numReservations := m.numReservations.Add(1)
	err = m.scheduledKernels.ReservationCreated(kernelId)
	if err != nil {
		panic(err)
	}

	m.log.Debug("Successfully created %s reservation for replica of kernel %s [numSuccessfulReservations=%d]: %v",
		status.String(), kernelId, numReservations, requestedResources.String())
	return nil
}

// ContainerStartedRunningOnHost is to be called whenever a kernel replica is scheduled onto this scheduling.Host.
// ContainerStartedRunningOnHost creates an Allocation of type PendingAllocation that is then associated with the
// newly-scheduled kernel replica.
//
// This operation is performed atomically by acquiring the AllocationManager::mu sync.Mutex.
// The sync.Mutex is released before the function returns.
func (m *AllocationManager) ContainerStartedRunningOnHost(replicaId int32, kernelId string, spec types.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify that there are no other replicas of the specified kernel scheduled on this host.
	scheduledReplica := m.scheduledKernels.GetScheduledReplica(kernelId)
	if scheduledReplica != ReplicaIdForReplicaNotFound && scheduledReplica != ReplicaIdForReservation {
		numFailedReservations := m.numFailedReservations.Add(1)
		m.log.Debug("Cannot reserve resources for replica of kernel %s: found existing resource "+
			"allocation associated to that another replica of that kernel (replica %d) [numFailedReservations=%d].",
			kernelId, scheduledReplica, numFailedReservations)
		return fmt.Errorf("%w: existing resource allocation found for another replica of kernel %s (%d)",
			ErrInvalidAllocationRequest, kernelId, scheduledReplica)
	}

	var (
		key              string
		allocation       scheduling.Allocation
		allocationExists bool
	)

	// Verify that there does not already exist an allocation or reservation associated with the specified kernel replica.
	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); allocationExists {
		// If the existing allocation is a reservation, then just "promote" the reservation to a standard
		// allocation, decrement the "number of reservations" counter, and return nil (i.e., no error).
		if allocation.IsReservation() {
			err := m.scheduledKernels.ReplicaStartedRunning(replicaId, kernelId)
			if err != nil {
				m.log.Error("Failed to record that replica %d of kernel %s started running: %v",
					replicaId, kernelId, err)
				return err
			}

			allocation.SetIsReservation(false)

			m.log.Debug("Updated existing allocation for replica %d of kernel %s stored under key \"%s\". Changed reservation flag to false.",
				replicaId, kernelId, key)

			m.allocationKernelReplicaMap.Store(key, allocation)
			m.kernelAllocationMap.Store(kernelId, allocation)
			m.numReservations.Add(-1)
			return nil
		}

		m.log.Error("Cannot subscribe pending resources to replica %d of kernel %s: found existing, non-reservation "+
			"allocation associated with that kernel replica: %s", replicaId, kernelId, allocation.String())
		return fmt.Errorf("%w: existing resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Verify that there does not already exist a reservation associated with an unspecified kernel replica.
	if replicaId != ReplicaIdForReservation {
		genericKey := getKey(ReplicaIdForReservation, kernelId)
		if allocation, allocationExists = m.allocationKernelReplicaMap.Load(genericKey); allocationExists {
			// If the existing allocation is a reservation, then just "promote" the reservation to a standard
			// allocation, decrement the "number of reservations" counter, and return nil (i.e., no error).
			if !allocation.IsReservation() {
				panic(fmt.Sprintf("Reservation for unspecified replica of kernel %s is not a reservation...", kernelId))
			}

			err := m.scheduledKernels.ReplicaStartedRunning(replicaId, kernelId)
			if err != nil {
				m.log.Error("Failed to record that replica %d of kernel %s started running: %v",
					replicaId, kernelId, err)
				return err
			}

			m.log.Debug("Promoting reservation for replica %d of kernel %s to 'regular' allocation.",
				replicaId, kernelId)

			m.allocationKernelReplicaMap.Delete(genericKey)
			m.kernelAllocationMap.Delete(kernelId)
			genericKey = getKey(replicaId, kernelId)

			allocation.SetIsReservation(false)
			allocation.SetReplicaId(replicaId)

			m.log.Debug("Updated existing allocation for previously-unknown replica %d of kernel %s stored under key \"%s\". Changed reservation flag to false.",
				replicaId, kernelId, genericKey)

			m.allocationKernelReplicaMap.Store(genericKey, allocation)
			m.kernelAllocationMap.Store(kernelId, allocation)
			m.numReservations.Add(-1)
			return nil
		}
	}

	// Construct the new Allocation using the resource quantities specified in the spec argument.
	allocation = NewResourceAllocationBuilder().
		WithAllocationType(scheduling.PendingAllocation).
		WithKernelReplica(replicaId, kernelId).
		WithMillicpus(spec.CPU()).
		WithMemoryMB(spec.MemoryMB()).
		WithGPUs(spec.GPU()).
		WithVRAM(spec.VRAM()).
		WithHostId(m.NodeId).
		WithHostName(m.NodeName).
		WithExecutionId(scheduling.DefaultExecutionId).
		IsNotAReservation().
		IsNotAPreCommitment().
		BuildResourceAllocation()

	m.log.Debug("Attempting to subscribe the following pending HostResources to replica %d of kernel %s: %v",
		replicaId, kernelId, spec.String())

	// Convert the given types.Spec argument to a *types.DecimalSpec struct.
	decimalSpec := types.ToDecimalSpec(spec)

	err := m.allocatePendingResources(decimalSpec, allocation, key, replicaId, kernelId)
	if err != nil {
		m.log.Error("Failed to allocate pending resources %s to replica %d of kernel %s: %v",
			decimalSpec.String(), replicaId, kernelId, err)
		return err
	}

	err = m.scheduledKernels.ReplicaScheduled(replicaId, kernelId)
	if err != nil {
		m.log.Error("Error while recording that replica %d of kernel %s was scheduled onto host %s: %v",
			replicaId, kernelId, m.NodeName, err)
		panic(err) // We just checked up above that no other replica was scheduled, so this should never happen.
	}

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	m.log.Debug("Successfully subscribed the following pending HostResources to replica %d of kernel %s: %v",
		replicaId, kernelId, decimalSpec.String())

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

	m.log.Debug("Attempting to evict replica %d of kernel %s from host %s now.", replicaId, kernelId, m.NodeName)

	key = getKey(replicaId, kernelId)
	if allocation, allocationExists = m.allocationKernelReplicaMap.Load(key); !allocationExists {
		m.log.Error("Error while evicting kernel replica within AllocationManager. "+
			"Could not find Allocation associated with replica %d of kernel %s... (under key \"%s\").",
			replicaId, kernelId, key)
		return fmt.Errorf("%w: no resource allocation found for evicted replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	allocatedResources := allocation.ToDecimalSpec()

	// First, check if the allocation is of type CommittedAllocation.
	// If so, then we'll first release the committed HostResources before unsubscribing the pending HostResources.
	if allocation.IsCommitted() {
		m.log.Debug("Releasing resources committed to evicted replica %d of kernel %s on host %s now.",
			replicaId, kernelId, m.NodeName)

		// Perform the resource count adjustments associated with releasing committed HostResources.
		// We'll pass allocatedResources ourselves (non-nil), as we need the *types.DecimalSpec
		// later on in the ReplicaEvicted method.
		err := m.releaseCommittedResources(allocation, allocatedResources, false)
		if err != nil {
			m.log.Error(
				utils.RedStyle.Render(
					"Failed to release resources committed to replica %d of kernel %s on host %s: %v"),
				replicaId, kernelId, m.NodeName, err)

			panic(err)
		}
	} else {
		m.log.Debug("Releasing pending resources assigned to evicted replica %d of kernel %s on host %s now.",
			replicaId, kernelId, m.NodeName)

		// Unsubscribe the pending HostResources.
		err := m.releasePendingResources(allocatedResources, replicaId, kernelId)
		if err != nil {
			m.log.Error(
				utils.RedStyle.Render(
					"Failed to unsubscribe pending resources %s from replica %d of kernel %s on host %s because: %v"),
				allocatedResources.String(), replicaId, kernelId, m.NodeName, err)
			panic(err)
		}
	}

	err := m.scheduledKernels.ReplicaRemoved(replicaId, kernelId)
	if err != nil {
		m.log.Error(
			utils.RedStyle.Render(
				"Failed to evict replica %d of kernel %s from host %s because: %v"),
			replicaId, kernelId, m.NodeName, err)
		panic(err)
	}

	m.log.Debug("Evicted replica %d of kernel %s, releasing the following pending HostResources: %v.",
		replicaId, kernelId, allocation.ToSpecString())
	m.log.Debug("Committed resources after removal: %s.", m.resourceManager.CommittedResources().String())
	m.log.Debug("Pending resources after removal: %s.", m.resourceManager.PendingResources().String())

	return nil
}

// HasSufficientIdleResourcesAvailable returns true if there are sufficiently many idle HostResources available
// on the node such that the requested HostResources could be commited to a locally-running kernel replica.
func (m *AllocationManager) HasSufficientIdleResourcesAvailable(spec types.Spec) bool {
	return m.resourceManager.idleResources.Validate(spec)
}

// GetGpuDeviceIdsAssignedToReplica returns the GPU device IDs assigned to the specified kernel replica.
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
			"allocation found for that kernel replica. (under key \"%s\").", replicaId, kernelId, key)
		return nil, fmt.Errorf("%w: replica %d of kernel %s", ErrAllocationNotFound, replicaId, kernelId)
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

// reservationShouldUsePendingResources returns a boolean flag indicating whether a reservation -- which is not
// indicated to be for a ready-to-train kernel replica -- should be created using pending or committed resources.
//
// The basis for what is returned by reservationShouldUsePendingResources is the scheduling.ResourceBindingMode
// of the configured scheduling.Policy.
func (m *AllocationManager) reservationShouldUsePendingResources() bool {
	return m.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart
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
		return scheduling.NewInconsistentResourcesError(kind, scheduling.NegativeResourceQuantity,
			scheduling.IdleResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Pending HostResources.
	hasNegative, kind = m.resourceManager.pendingResources.HasNegativeField()
	if hasNegative {
		return scheduling.NewInconsistentResourcesError(kind, scheduling.NegativeResourceQuantity,
			scheduling.PendingResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Committed HostResources.
	hasNegative, kind = m.resourceManager.committedResources.HasNegativeField()
	if hasNegative {
		return scheduling.NewInconsistentResourcesError(kind, scheduling.NegativeResourceQuantity,
			scheduling.CommittedResources, m.resourceManager.idleResources.GetResource(kind))
	}

	// Spec HostResources.
	hasNegative, kind = m.resourceManager.specResources.HasNegativeField()
	if hasNegative {
		return scheduling.NewInconsistentResourcesError(kind, scheduling.NegativeResourceQuantity,
			scheduling.SpecResources, m.resourceManager.idleResources.GetResource(kind))
	}

	////////////////////////////////////////////////////////////////////////////////////////
	// Check that the idle and committed HostResources are no larger than the spec HostResources. //
	////////////////////////////////////////////////////////////////////////////////////////

	// Idle HostResources <= Spec HostResources.
	isOkay, offendingKind := m.resourceManager.idleResources.LessThanOrEqual(m.resourceManager.specResources)
	if !isOkay {
		return scheduling.NewInconsistentResourcesErrorWithResourceQuantity(offendingKind,
			scheduling.QuantityGreaterThanSpec, scheduling.IdleResources,
			m.resourceManager.idleResources.GetResource(offendingKind),
			m.resourceManager.specResources.GetResource(offendingKind))
	}

	// Committed HostResources <= spec HostResources.
	isOkay, offendingKind = m.resourceManager.committedResources.LessThanOrEqual(m.resourceManager.specResources)
	if !isOkay {
		return scheduling.NewInconsistentResourcesErrorWithResourceQuantity(offendingKind,
			scheduling.QuantityGreaterThanSpec, scheduling.CommittedResources,
			m.resourceManager.committedResources.GetResource(offendingKind),
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
		var areEqual bool
		areEqual, offendingKind = m.resourceManager.idleResources.EqualTo(m.resourceManager.specResources)
		if !areEqual {
			return scheduling.NewInconsistentResourcesErrorWithResourceQuantity(offendingKind,
				scheduling.IdleSpecUnequal, scheduling.IdleResources,
				m.resourceManager.idleResources.GetResource(offendingKind),
				m.resourceManager.specResources.GetResource(offendingKind))
		}

		// Next, check that our pending HostResources are equal to zero.
		isZero, offendingKind := m.resourceManager.pendingResources.IsZero()
		if !isZero {
			return scheduling.NewInconsistentResourcesError(offendingKind,
				scheduling.PendingNonzero, scheduling.PendingResources,
				m.resourceManager.pendingResources.GetResource(offendingKind))
		}
	}

	return nil
}

func (m *AllocationManager) releasePendingResources(allocatedResources *types.DecimalSpec, replicaId int32, kernelId string) error {
	m.log.Debug("Deallocating pending resources. Current resources: %v. TransactionResources to be deallocated: %v",
		m.resourceManager.GetResourceCountsAsString(), allocatedResources.String())

	if err := m.resourceManager.pendingResources.Subtract(allocatedResources); err != nil {
		return err
	}

	m.log.Debug("Deallocated pending resources. Updated pending resources: %v",
		m.resourceManager.pendingResources.String())

	m.numPendingAllocations.Add(-1)

	// Delete the allocation, since the replica was evicted.
	m.allocationKernelReplicaMap.Delete(getKey(replicaId, kernelId))
	m.kernelAllocationMap.Delete(kernelId)

	if m.updateSubscriptionRatio != nil {
		m.updateSubscriptionRatio()
	}

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		panic(err)
	}

	if m.updateIndex != nil {
		err = m.updateIndex(replicaId, kernelId)
		if err != nil {
			m.log.Error("Failed to update index for host %s", m.NodeName)
			return err
		}
	}

	// Update Prometheus metrics.
	m.unsafeUpdatePrometheusResourceMetrics()

	return nil
}

// allocatePendingResources is used to create and apply an Allocation of type scheduling.PendingAllocation.
//
// allocatePendingResources is not thread safe (with respect to the AllocationManager).
func (m *AllocationManager) allocatePendingResources(spec *types.DecimalSpec,
	allocation scheduling.Allocation, key string, replicaId int32, kernelId string) error {

	m.log.Debug("Allocating pending resources. Current resources: %s. TransactionResources to be allocated: %v.",
		m.resourceManager.GetResourceCountsAsString(), spec.String())

	// First, validate against this scheduling.Host's spec.
	if err := m.resourceManager.specResources.ValidateWithError(spec); err != nil {
		m.log.Error("Replica %d of kernel \"%s\" is requesting more resources [%v] than host has available [%v]. Specific reason for subscription failure: %v.",
			replicaId, kernelId, spec.String(), m.resourceManager.specResources.GetResourceCountsAsString(), err)
		return err
	}

	// If we've gotten this far, then we have enough HostResources available to subscribe the requested HostResources
	// to the specified kernel replica. So, let's do that now.
	if err := m.resourceManager.pendingResources.Add(spec); err != nil {
		return err
	}

	m.log.Debug("Allocated pending resources. New resource counts: %s.",
		m.resourceManager.GetResourceCountsAsString())

	// Store the allocation in the mapping.
	m.allocationKernelReplicaMap.Store(key, allocation)
	m.kernelAllocationMap.Store(kernelId, allocation)

	m.log.Debug("Stored new pending allocation for replica %d of kernel %s stored under key \"%s\".",
		replicaId, kernelId, key)

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Add(1)

	if m.updateSubscriptionRatio != nil {
		m.updateSubscriptionRatio()
	}

	if m.updateIndex != nil {
		err := m.updateIndex(replicaId, kernelId)
		if err != nil {
			m.log.Error("Failed to update index for host %s", m.NodeName)
			return err
		}
	}

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err := m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		panic(err)
	}

	// Update Prometheus metrics.
	m.unsafeUpdatePrometheusResourceMetrics()

	return nil
}

// allocateCommittedResources is used to create and apply an Allocation of type scheduling.CommittedAllocation.
//
// NOTE: The provided Allocation parameter may or may not have its resource counts up-to-date (i.e., consistent with
// the specified types.DecimalSpec parameter) prior to the commitment of resources through the underlying Manager.
//
// allocateCommittedResources is not thread safe (with respect to the AllocationManager).
func (m *AllocationManager) allocateCommittedResources(replicaId int32, kernelId string, resourceRequest *types.DecimalSpec,
	allocation scheduling.Allocation, decrementPending bool, isPreCommitment bool, executionId string, key string, gpuDeviceIds []int) error {

	m.log.Debug("Allocating committed resources. Current resources: %s. TransactionResources to be allocated: %v. DecrPending=%v. IsPreCommit=%v.",
		m.resourceManager.GetResourceCountsAsString(), resourceRequest.String(), decrementPending, isPreCommitment)

	// First, validate against this scheduling.Host's spec.
	if err := m.resourceManager.specResources.ValidateWithError(resourceRequest); err != nil {
		m.log.Error("Replica %d of kernel \"%s\" is requesting more resources [%v] than host has available [%v]. "+
			"Specific reason for subscription failure: %v.", replicaId, kernelId, resourceRequest.String(),
			m.resourceManager.specResources.GetResourceCountsAsString(), err)
		return err
	}

	err := m.resourceManager.RunTransaction(func(state scheduling.TransactionState) {
		state.CommittedResources().Add(resourceRequest)

		if decrementPending {
			state.PendingResources().Subtract(resourceRequest)
		}

		state.IdleResources().Subtract(resourceRequest)
	})

	if err != nil {
		m.log.Warn("Could not commit resources [%v] to replica %d of kernel %s because: %v.",
			resourceRequest.String(), replicaId, kernelId, err)

		// This error should be of type transaction.ErrTransactionFailed.
		// If it's not, then we'll just return it directly...
		var txFailedError transaction.ErrTransactionFailed
		if !errors.As(err, &txFailedError) {
			m.log.Warn("Could not commit resources [%v] to replica %d of kernel %s for unexpected reason: %v",
				resourceRequest.String(), replicaId, kernelId, err)
			return err
		}

		// If the reason is ErrNegativeResourceCount, then we'll return an InsufficientResourcesError.
		if errors.Is(txFailedError.Reason, transaction.ErrNegativeResourceCount) {
			return scheduling.NewInsufficientResourcesError(m.IdleResources(), resourceRequest, txFailedError.OffendingKinds)
		}

		return err
	}

	m.log.Debug(
		utils.LightBlueStyle.Render(
			"Allocated committed resources [%v] to replica %d of kernel %s. New resource counts: %s."),
		resourceRequest.String(), replicaId, kernelId, m.resourceManager.GetResourceCountsAsString())

	if decrementPending {
		// Update the pending/committed allocation counters.
		m.numPendingAllocations.Add(-1)
	}

	m.numCommittedAllocations.Add(1)

	// Finally, we'll update the Allocation struct associated with this request.
	// This involves updating the resource amounts stored in the Allocation as well as its AllocationType field.
	// The resource amounts may already match what was allocated, depending on if the resourceRequestArg parameter
	// was nil or not.
	//
	// Once updated, we'll remove it from the pending allocation maps and add it to the committed allocation maps.
	allocation.SetGpus(resourceRequest.GPUs)
	allocation.SetMillicpus(resourceRequest.Millicpus)
	allocation.SetMemoryMb(resourceRequest.MemoryMb)
	allocation.SetVramGb(resourceRequest.VRam)
	allocation.SetAllocationType(scheduling.CommittedAllocation)
	allocation.SetIsPreCommitted(isPreCommitment)
	allocation.SetExecutionId(executionId)

	if int(allocation.GetGpus()) > m.availableGpuDevices.Len() {
		panic(fmt.Sprintf("Require %d GPU device ID(s) for replica %d of kernel %s, but only %d is/are available. Committed GPUs: %.0f.",
			int(allocation.GetGpus()), replicaId, kernelId, m.availableGpuDevices.Len(), m.resourceManager.CommittedResources().GPUs()))
	}

	// Validate that the specified GPU device IDs are available, or allocate GPU device IDs ourselves if
	// the caller did not specify any GPU device IDs.
	gpuDeviceIds = m.commitGpuDeviceIds(allocation, gpuDeviceIds)

	allocation.SetGpuDeviceIds(gpuDeviceIds)

	m.log.Debug("Updated GPU device IDs: %v", allocation.GetGpuDeviceIds())

	// Store the allocation in the mapping.
	m.allocationKernelReplicaMap.Store(key, allocation)
	m.kernelAllocationMap.Store(kernelId, allocation)

	m.log.Debug("Stored committed allocation for replica %d of kernel %s stored under key \"%s\".",
		replicaId, kernelId, key)

	if m.updateSubscriptionRatio != nil {
		m.updateSubscriptionRatio()
	}

	// Sanity Check. Make sure everything is OK with respect to our internal state/bookkeeping.
	err = m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		return err
	}

	if m.updateIndex != nil {
		err := m.updateIndex(replicaId, kernelId)
		if err != nil {
			m.log.Error("Failed to update index for host %s", m.NodeName)
			return err
		}
	}

	if isPreCommitment {
		m.numPreCommitments.Add(1)
	}

	// Update Prometheus metrics.
	// m.resourceMetricsCallback(m.Manager)
	m.unsafeUpdatePrometheusResourceMetrics()

	m.log.Debug("Successfully committed resources to replica %d of kernel %s (isPreCommitment=%v): %v. GPUs IDs: %v.",
		replicaId, kernelId, isPreCommitment, resourceRequest.String(), allocation.GetGpuDeviceIds())
	m.log.Debug("Updated resource counts: %s.", m.resourceManager.GetResourceCountsAsString())

	return nil
}

// commitGpuDeviceIds validates that the specified GPU device IDs are available if the caller specifies them.
//
// If the caller did not specify any GPU device IDs, then commitGpuDeviceIds will allocate the GPU device IDs itself.
func (m *AllocationManager) commitGpuDeviceIds(allocation scheduling.Allocation, gpuDeviceIds []int) []int {
	numGpuDeviceIdsRequired := int(allocation.GetGpus())

	if gpuDeviceIds == nil || len(gpuDeviceIds) == 0 {
		gpuDeviceIds = make([]int, 0, numGpuDeviceIdsRequired)

		m.log.Debug("Allocating %d/%d remaining, available GPU device IDs.",
			int(allocation.GetGpus()), m.availableGpuDevices.Len())

		for len(gpuDeviceIds) < int(allocation.GetGpus()) {
			gpuDeviceId, ok := m.availableGpuDevices.Dequeue()

			if !ok {
				panic("Received no GPU device ID when one should have been available.")
			}

			m.log.Debug("Allocating GPU #%d to replica %d of kernel '%s'.",
				gpuDeviceId, allocation.GetReplicaId(), allocation.GetKernelId())

			gpuDeviceIds = append(gpuDeviceIds, gpuDeviceId)
		}

		return gpuDeviceIds
	}

	if len(gpuDeviceIds) != numGpuDeviceIdsRequired {
		m.log.Error("Caller specified %d GPU device ID(s) for replica %d of kernel %s; however, %d GPU device ID(s) are required.",
			len(gpuDeviceIds), allocation.GetReplicaId(), allocation.GetKernelId(), numGpuDeviceIdsRequired)

		panic("Mismatch between required number of GPU device IDs and specified number of GPU device IDs.")
	}

	m.log.Debug("Caller specified %d GPU device ID(s) to be allocated to replica %d of kernel %s: %v",
		len(gpuDeviceIds), allocation.GetReplicaId(), allocation.GetKernelId(), gpuDeviceIds)

	availableGpuDevices := m.availableGpuDevices.ToSlice()
	setDifference, isSubset := utils.SetDifferenceIfSubset(availableGpuDevices, gpuDeviceIds)
	if !isSubset {
		m.log.Error("1 or more specified GPU device IDs are unavailable. Available: %v. Specified: %v.",
			availableGpuDevices, gpuDeviceIds)

		panic("One or more specified GPU device IDs are unavailable.")
	}

	// Recreate the GPU device ID queue from the set difference.
	m.availableGpuDevices = queue.NewFifoFromSlice(setDifference)

	return gpuDeviceIds
}

// releaseCommittedResources releases committed/bound HostResources from the kernel replica associated with
// the given Allocation.
//
// This function does NOT acquire the AllocationManager's mutex, nor does it perform any validation checks whatsoever.
// It is meant to be called from a context in which the AllocationManager's mutex is held and any appropriate
// checks are performed before the call to releaseCommittedResources and after releaseCommittedResources
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
func (m *AllocationManager) releaseCommittedResources(allocation scheduling.Allocation, spec *types.DecimalSpec, incrementPending bool) error {
	if allocation == nil {
		panic("The provided Allocation cannot be nil.")
	}

	// If spec is nil, then call allocation.ToDecimalSpec() to populate spec with a value.
	if spec == nil {
		spec = allocation.ToDecimalSpec()
	}

	m.log.Debug("Releasing committed resources. Current resource counts: %s. Resources to be deallocated: %v.",
		m.resourceManager.GetResourceCountsAsString(), spec.String())

	deviceIdsToReturn := allocation.GetGpuDeviceIds()
	numToReturn := len(deviceIdsToReturn)
	m.log.Debug("Returning %d GPU device ID(s): %v. Current device ID pool size: %d.",
		numToReturn, deviceIdsToReturn, m.availableGpuDevices.Len())
	for _, gpuDeviceId := range deviceIdsToReturn {
		m.availableGpuDevices.Enqueue(gpuDeviceId)
	}
	m.log.Debug("Returned %d GPU device ID(s). GPU device ID pool %d → %d.",
		numToReturn, m.availableGpuDevices.Len()-numToReturn, m.availableGpuDevices.Len())

	// Clear the GpuDeviceIds field of the allocation.
	allocation.ClearGpuDeviceIds()

	err := m.resourceManager.RunTransaction(func(state scheduling.TransactionState) {
		state.CommittedResources().Subtract(spec)

		if incrementPending {
			state.PendingResources().Add(spec)
		}

		state.IdleResources().Add(spec)
	})

	if err != nil {
		m.log.Error(
			utils.RedStyle.Render("Failed to release committed resources [%v] from replica %d of kernel %s: %v"),
			spec.String(), allocation.GetReplicaId(), allocation.GetKernelId(), err)
		panic(err)
	}

	// Make sure everything is OK with respect to our internal state/bookkeeping.
	err = m.unsafePerformConsistencyCheck()
	if err != nil {
		m.log.Error("Discovered an inconsistency: %v", err)
		panic(err)
	}

	m.numCommittedAllocations.Add(-1)
	if incrementPending {
		m.numPendingAllocations.Add(1)
	}

	if m.updateIndex != nil {
		err := m.updateIndex(allocation.GetReplicaId(), allocation.GetKernelId())
		if err != nil {
			m.log.Error("Failed to update index for host %s", m.NodeName)
			return err
		}
	}

	if m.updateSubscriptionRatio != nil {
		m.updateSubscriptionRatio()
	}

	if allocation.IsPreCommitted() {
		m.numPreCommitments.Add(-1)
		allocation.SetIsPreCommitted(false)
	}

	// Update Prometheus metrics.
	m.unsafeUpdatePrometheusResourceMetrics()

	m.log.Debug("Released committed resources. Updated resource counts: %s.", m.resourceManager.GetResourceCountsAsString())
	return nil
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

// UnitTestingAllocationManager is a wrapper around AllocationManager with a few additional methods to facilitate
// unit testing with scheduling.UnitTestingHost instances.
type unitTestingAllocationManager struct {
	*AllocationManager
}

func NewUnitTestingAllocationManager(manager scheduling.AllocationManager) scheduling.UnitTestingAllocationManager {
	if _, ok := manager.(*unitTestingAllocationManager); ok {
		panic(
			fmt.Sprintf(
				"Cannot wrap AllocationManager \"%s\" (HostId=\"%s\") in a UnitTestingAllocationManager as it is already a UnitTestingAllocationManager.",
				manager.GetId(), manager.GetNodeId()))
	}

	if _, ok := manager.(*AllocationManager); !ok {
		panic(
			fmt.Sprintf(
				"Cannot wrap AllocationManager \"%s\" (HostId=\"%s\") in a UnitTestingAllocationManager as it is of an unknown or unsupported concrete type.",
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

// AddGpuDeviceIds makes the specified GPU device IDs available for allocation on the target UnitTestingHost.
func (m *unitTestingAllocationManager) AddGpuDeviceIds(gpuDeviceIds []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, gpuDeviceId := range gpuDeviceIds {
		m.log.Debug("Artificially adding GPU device ID: %d", gpuDeviceId)
		m.availableGpuDevices.Enqueue(gpuDeviceId)
	}
}
