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

func (r *resources) SetGpus(gpus decimal.Decimal) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.gpus = gpus
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
// ResourceManager is a replacement for GpuManager.
type ResourceManager struct {
	mu sync.Mutex

	id  string        // Unique ID of the Resource Manager.
	log logger.Logger // Logger.

	// allocationIdMap is a map from AllocationID -> *ResourceAllocation.
	// That is, allocationIdMap is a mapping in which the keys are strings -- the allocation ID --
	// and values are the associated *ResourceAllocation (i.e., the *ResourceAllocation whose ID is the key).
	allocationIdMap hashmap.HashMap[string, *ResourceAllocation]

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *gpuAllocation.
	allocationKernelReplicaMap hashmap.HashMap[string, *ResourceAllocation]

	// pendingAllocIdMap is a map from AllocationID -> *ResourceAllocation.
	// That is, pendingAllocIdMap is a mapping in which keys are strings -- the allocation ID --
	// and values are the associated *gpuAllocation (i.e., the *gpuAllocation whose ID is the key).
	pendingAllocIdMap hashmap.HashMap[string, *ResourceAllocation]

	// pendingAllocKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, pendingAllocKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *gpuAllocation.
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
