package daemon

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"sync"
	"time"
)

const (
	// PendingAllocation indicates that a ResourceAllocation is "pending" rather than "committed".
	// This means that the resources are not "actually" allocated to the associated kernel replica.
	// The kernel replica is merely scheduled locally, but it has not bound to these resources.
	PendingAllocation AllocationType = "pending"

	//CommittedAllocation indicates that a ResourceAllocation has been committed to the associated kernel replica.
	//That is, the GPUs, CPUs, and Memory specified in the allocation are actively committed and bound to the
	//associated kernel replica. These resources are not available for use by other kernel replicas.
	CommittedAllocation AllocationType = "committed"

	// IdleResources can overlap with pending resources. These are resources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources ResourceType = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending resources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending resources on a node is the amount of resources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources ResourceType = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources ResourceType = "committed"

	// SpecResources are the total allocatable resources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources ResourceType = "spec"
)

// ResourceStateWrapper defines a public interface for accessing (i.e., reading) but not mutating (i.e., writing)
// the current state of a ResourceStateWrapper.
//
// ResourceStateWrapper wraps several ResourceState instances -- one for resources of each of the following types:
// idle, pending, committed, and spec. As such, ResourceStateWrapper exposes a collection of several ResourceState
// instances to provide a convenient type for reading all the relevant state of a ResourceManager.
type ResourceStateWrapper interface {
	// IdleResources returns the idle resources managed by a ResourceManager.
	// Idle resources can overlap with pending resources. These are resources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources() ResourceState

	// PendingResources returns the pending resources managed by a ResourceManager.
	// Pending resources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending resources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending resources on a node is the amount of resources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources() ResourceState

	// CommittedResources returns the committed resources managed by a ResourceManager.
	// These are resources that are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources() ResourceState

	// SpecResources returns the spec resources managed by a ResourceManager.
	// These are the total allocatable resources available on the Host.
	// Spec resources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources() ResourceState

	// String returns a string representation of the ResourceStateWrapper suitable for logging.
	String() string
}

// ResourceState defines a public interface for getting (i.e., reading) but not mutating (i.e., writing)
// the current state of a ResourceManager.
//
// ResourceState encapsulates the resources for a single type of resource (i.e., idle, pending, committed, or spec).
// Meanwhile, ResourceStateWrapper exposes a collection of several ResourceState instances to provide a convenient
// type for reading all the relevant state of a ResourceManager.
type ResourceState interface {
	// ResourceType returns the ResourceType of the resources encapsulated/made available for reading
	// by this ResourceState instance.
	ResourceType() ResourceType

	// CPUs returns the gpus as a float64.
	// The units are millicpus, or 1/1000th of a CPU core.
	CPUs() float64
	// CPUsAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the number of cpus.
	// The units are millicpus, or 1/1000th of a CPU core.
	CPUsAsDecimal() decimal.Decimal

	// MemoryMB returns the amount of memory as a float64.
	// The units are megabytes (MB).
	MemoryMB() float64
	// MemoryMbAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of memory.
	// The units are megabytes (MB).
	MemoryMbAsDecimal() decimal.Decimal

	// GPUs returns the gpus as a float64.
	// The units are vGPUs, where 1 vGPU = 1 GPU.
	GPUs() float64
	// GPUsAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the number of gpus.
	// The units are vGPUs, where 1 vGPU = 1 GPU.
	GPUsAsDecimal() decimal.Decimal

	// String returns a string representation of the ResourceState suitable for logging.
	String() string
}

// resourceMetricsCallback is a callback function that is supposed to be triggered whenever resources
// are allocated or deallocated so that the associated Prometheus metrics can be updated accordingly.
type resourceMetricsCallback func(resources ResourceStateWrapper)

// ResourceType differentiates between idle, pending, committed, and spec resources.
type ResourceType string

func (t ResourceType) String() string {
	return string(t)
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
	AllocationId string `json:"id"`

	// GPUs is the number of GPUs in the ResourceAllocation.
	GPUs decimal.Decimal `json:"gpus"`

	// Millicpus is the number of CPUs in the ResourceAllocation, represented as 1/1000th cores.
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
	// That is, the GPUs, CPUs, and Memory specified in the allocation are actively committed and bound to the
	// associated kernel replica. These resources are not available for use by other kernel replicas.
	AllocationType AllocationType `json:"allocation_type"`
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
func NewResourceAllocationBuilder(allocationType AllocationType) *ResourceAllocationBuilder {
	return &ResourceAllocationBuilder{
		allocationId:   uuid.NewString(),
		allocationType: allocationType,
	}
}

// WithKernelReplica enables the specification of the target of the ResourceAllocation (i.e., the kernel replica).
func (b *ResourceAllocationBuilder) WithKernelReplica(kernelId string, replicaId int32) *ResourceAllocationBuilder {
	b.kernelId = kernelId
	b.replicaId = replicaId
	return b
}

// WithGPUs enables the specification of the number of GPUs in the ResourceAllocation that is being constructed.
func (b *ResourceAllocationBuilder) WithGPUs(gpus float64) *ResourceAllocationBuilder {
	b.gpus = decimal.NewFromFloat(gpus)
	return b
}

// WithMillicpus enables the specification of the number of CPUs (in millicpus, or 1/1000th of a core)
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
		AllocationId:   b.allocationId,
		GPUs:           b.gpus,
		Millicpus:      b.millicpus,
		MemoryMB:       b.memoryMb,
		ReplicaId:      b.replicaId,
		KernelId:       b.kernelId,
		AllocationType: b.allocationType,
		Timestamp:      time.Now(),
	}
}

// resources is a struct used by the ResourceManager to track its total idle, pending, committed, and spec resources
// of each type (CPU, GPU, and Memory).
type resources struct {
	mu sync.Mutex // Enables atomic access to each individual field.

	resourceType ResourceType    // resourceType is the ResourceType represented/encoded by this resources struct.
	millicpus    decimal.Decimal // millicpus is CPU in 1/1000th of CPU core.
	gpus         decimal.Decimal // gpus is the number of GPUs.
	memoryMB     decimal.Decimal // memoryMB is the amount of memory in MB.
}

func (r *resources) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Sprintf("[%s resources: millicpus=%s,gpus=%s,memoryMB=%s]",
		r.resourceType.String(), r.millicpus.StringFixed(0),
		r.gpus.StringFixed(0), r.memoryMB.StringFixed(4))
}

func (r *resources) ResourceType() ResourceType {
	return r.resourceType
}

func (r *resources) MemoryMB() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.memoryMB.InexactFloat64()
}

func (r *resources) MemoryMbAsDecimal() decimal.Decimal {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.memoryMB.Copy()
}

// SetMemoryMB sets the amount of memory to a copy of the specified decimal.Decimal value.
func (r *resources) SetMemoryMB(memoryMB decimal.Decimal) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.memoryMB = memoryMB
}

func (r *resources) GPUs() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.gpus.InexactFloat64()
}

func (r *resources) GPUsAsDecimal() decimal.Decimal {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.gpus.Copy()
}

// SetGpus sets the number of GPUs to a copy of the specified decimal.Decimal value.
func (r *resources) SetGpus(gpus decimal.Decimal) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.gpus = gpus.Copy()
}

func (r *resources) Millicpus() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.millicpus.InexactFloat64()
}

func (r *resources) MillicpusAsDecimal() decimal.Decimal {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.millicpus.Copy()
}

// SetMillicpus sets the number of CPUs to a copy of the specified decimal.Decimal value.
func (r *resources) SetMillicpus(millicpus decimal.Decimal) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.millicpus = millicpus
}

// resourcesWrapper is a wrapper around several resources structs, each of which corresponds to idle, pending,
// committed, or spec resources.
type resourcesWrapper struct {
	mu sync.Mutex

	idleResources      *resources
	pendingResources   *resources
	committedResources *resources
	specResources      *resources
}

func (r *resourcesWrapper) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Sprintf("resourcesWrapper{%s, %s, %s, %s}",
		r.idleResources.String(), r.pendingResources.String(), r.committedResources.String(), r.specResources.String())
}

func (r *resourcesWrapper) IdleResources() *resources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources
}

func (r *resourcesWrapper) PendingResources() *resources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources
}

func (r *resourcesWrapper) CommittedResources() *resources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources
}

func (r *resourcesWrapper) SpecResources() *resources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources
}

// ResourceManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The ResourceManager allocates and deallocates resources to/from kernel replicas scheduled to run on the node.
//
// ResourceManager is a replacement for ResourceManager.
type ResourceManager struct {
	mu sync.Mutex

	id  string        // Unique ID of the Resource Manager.
	log logger.Logger // Logger.

	// allocationIdMap is a map from AllocationID -> *ResourceAllocation.
	// That is, allocationIdMap is a mapping in which the keys are strings -- the allocation ID --
	// and values are the associated *ResourceAllocation (i.e., the *ResourceAllocation whose ID is the key).
	//
	// allocationIdMap contains ResourceAllocation structs of type CommittedAllocation.
	allocationIdMap hashmap.HashMap[string, *ResourceAllocation]

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *ResourceAllocation.
	//
	// allocationKernelReplicaMap contains ResourceAllocation structs of type CommittedAllocation.
	allocationKernelReplicaMap hashmap.HashMap[string, *ResourceAllocation]

	// pendingAllocIdMap is a map from AllocationID -> *ResourceAllocation.
	// That is, pendingAllocIdMap is a mapping in which keys are strings -- the allocation ID --
	// and values are the associated *ResourceAllocation (i.e., the *ResourceAllocation whose ID is the key).
	//
	// pendingAllocIdMap contains ResourceAllocation structs of type PendingAllocation.
	pendingAllocIdMap hashmap.HashMap[string, *ResourceAllocation]

	// pendingAllocKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, pendingAllocKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *ResourceAllocation.
	//
	// pendingAllocKernelReplicaMap contains ResourceAllocation structs of type PendingAllocation.
	pendingAllocKernelReplicaMap hashmap.HashMap[string, *ResourceAllocation]

	// resourcesWrapper encapsulates the state of all resources (idle, pending, committed, and spec) managed
	// by this ResourceManager.
	resourcesWrapper *resourcesWrapper

	// resourceMetricsCallback is a callback function that is supposed to be triggered whenever resources
	// are allocated or deallocated so that the associated Prometheus metrics can be updated accordingly.
	resourceMetricsCallback resourceMetricsCallback
}

// NewResourceManager creates a new ResourceManager struct and returns a pointer to it.
func NewResourceManager(resourceSpec types.Spec, resourceMetricsCallback resourceMetricsCallback) *ResourceManager {
	manager := &ResourceManager{
		id:                           uuid.NewString(),
		allocationIdMap:              hashmap.NewCornelkMap[string, *ResourceAllocation](64),
		allocationKernelReplicaMap:   hashmap.NewCornelkMap[string, *ResourceAllocation](64),
		pendingAllocIdMap:            hashmap.NewCornelkMap[string, *ResourceAllocation](64),
		pendingAllocKernelReplicaMap: hashmap.NewCornelkMap[string, *ResourceAllocation](64),
		resourceMetricsCallback:      resourceMetricsCallback,
	}

	manager.resourcesWrapper = &resourcesWrapper{
		idleResources: &resources{
			resourceType: IdleResources,
			millicpus:    decimal.NewFromFloat(resourceSpec.CPU()),
			memoryMB:     decimal.NewFromFloat(resourceSpec.MemoryMB()),
			gpus:         decimal.NewFromFloat(resourceSpec.GPU()),
		},
		pendingResources: &resources{
			resourceType: PendingResources,
			millicpus:    decimal.Zero.Copy(),
			memoryMB:     decimal.Zero.Copy(),
			gpus:         decimal.Zero.Copy(),
		},
		committedResources: &resources{
			resourceType: CommittedResources,
			millicpus:    decimal.Zero.Copy(),
			memoryMB:     decimal.Zero.Copy(),
			gpus:         decimal.Zero.Copy(),
		},
		specResources: &resources{
			resourceType: SpecResources,
			millicpus:    decimal.NewFromFloat(resourceSpec.CPU()),
			memoryMB:     decimal.NewFromFloat(resourceSpec.MemoryMB()),
			gpus:         decimal.NewFromFloat(resourceSpec.GPU()),
		},
	}

	config.InitLogger(&manager.log, manager)

	manager.log.Debug("Resource Manager initialized: %v", manager.resourcesWrapper.String())

	return manager
}

// SpecGPUs returns the total number of GPUs configured/present on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) SpecGPUs() decimal.Decimal {
	return m.resourcesWrapper.SpecResources().GPUsAsDecimal().Copy()
}

// SpecCPUs returns the total number of CPUs configured/present on this node in millicpus.
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

// IdleGPUs returns the number of GPUs that are uncommitted and therefore available on this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) IdleGPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.IdleResources().GPUsAsDecimal().Copy()
}

// IdleCPUs returns the number of CPUs that are uncommitted and therefore available on this node.
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

// CommittedGPUs returns the number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
//
// This returns a copy of the decimal.Decimal used internally.
func (m *ResourceManager) CommittedGPUs() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.resourcesWrapper.CommittedResources().GPUsAsDecimal().Copy()
}

// CommittedCPUs returns the CPUs, in millicpus, that are actively committed and allocated to replicas that are scheduled onto this node.
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

// PendingCPUs returns the sum of the outstanding CPUs of all replicas scheduled onto this node, in millicpus.
// Pending CPUs are not allocated or committed to a particular replica yet.
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

	key := m.getKey(replicaId, kernelId)
	alloc, ok := m.pendingAllocKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// If it is a pending GPU allocation, then we may return true.
	if alloc.IsPending() {
		return alloc.GPUs.GreaterThan(ZeroDecimal)
	}

	// It is an "actual" GPU allocation, not a pending GPU allocation, so return false.
	return false
}

// ReplicaHasCommittedResources returns true if the specified kernel replica has any resources committed to it.
func (m *ResourceManager) ReplicaHasCommittedResources(replicaId int32, kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getKey(replicaId, kernelId)
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

	key := m.getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
	if !ok {
		return false
	}

	// It is an "actual" GPU allocation.
	return alloc.GPUs.GreaterThan(ZeroDecimal)
}

// getKey creates and returns a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the GPU Manager.
func (m *ResourceManager) getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

// assertPending returns true if the given *ResourceAllocation IS pending.
// If the given *ResourceAllocation is NOT pending, then this panics.
func (m *ResourceManager) assertPending(allocation *ResourceAllocation) bool {
	if allocation.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %v", allocation))
}

// assertNotPending returns true if the given *ResourceAllocation is NOT pending.
// If the given *ResourceAllocation IS pending, then this panics.
func (m *ResourceManager) assertNotPending(allocation *ResourceAllocation) bool {
	if !allocation.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation IS pending: %v", allocation))
}

// NumAllocations returns the number of active allocations.
func (m *ResourceManager) NumAllocations() int {
	return m.allocationIdMap.Len()
}

// NumPendingAllocations returns the number of pending allocations.
func (m *ResourceManager) NumPendingAllocations() int {
	return m.pendingAllocIdMap.Len()
}

// CommitResources commits/binds resources to a particular kernel replica, such that the resources are reserved for
// exclusive use by that kernel replica until the kernel replica releases them (or another entity releases them
// on behalf of the kernel replica).
func (m *ResourceManager) CommitResources() error {
	// TODO: Implement me.
	panic("Not implemented")
}

// UncommitResources uncommits/unbinds resources from a particular kernel replica, such that the resources are made
// available for use by other kernel replicas.
func (m *ResourceManager) UncommitResources() error {
	// TODO: Implement me.
	panic("Not implemented")
}

// ReplicaScheduled is to be called whenever a kernel replica is scheduled onto this scheduling.Host.
// ReplicaScheduled creates a ResourceAllocation of type PendingAllocation that is then associated with the
// newly-scheduled kernel replica.
func (m *ResourceManager) ReplicaScheduled() error {
	// TODO: Implement me.
	panic("Not implemented")
}

// ReplicaEvicted is to be called whenever a kernel replica is stopped/evicted from this scheduling.Host.
// ReplicaEvicted releases any ResourceAllocation associated with the evicted/stopped kernel replica.
//
// If there are resources actively bound/committed to the kernel replica, then they are released.
// Likewise, any ResourceAllocation of type PendingAllocation is released/dissolved.
func (m *ResourceManager) ReplicaEvicted() error {
	// TODO: Implement me.
	panic("Not implemented")
}
