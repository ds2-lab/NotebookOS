package daemon

import (
	"encoding/json"
	"errors"
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

var (
	// ErrInvalidAllocationRequest indicates that a ResourceAllocation could not be created/satisfied due to an issue
	// with the request itself.
	//
	// The issue is not something of the nature that there are just insufficient resources available to satisfy the
	// request. Instead, ErrInvalidAllocationRequest indicates that the request itself was illegal or issued under
	// invalid circumstances, such as there being no existing ResourceAllocation of type PendingAllocation when
	// attempting to commit resources to a particular kernel replica.
	ErrInvalidAllocationRequest = errors.New("the resource allocation could not be completed due to the request being invalid")

	// ErrInsufficientMemory indicates that there was insufficient memory resources available to validate/support/serve
	// the given resource request/types.Spec.
	ErrInsufficientMemory = errors.New("insufficient memory resources available")

	// ErrInsufficientCPUs indicates that there was insufficient CPU resources available to validate/support/serve
	// the given resource request/types.Spec.
	ErrInsufficientCPUs = errors.New("insufficient CPU resources available")

	// ErrInsufficientGPUs indicates that there was insufficient GPU resources available to validate/support/serve
	// the given resource request/types.Spec.
	ErrInsufficientGPUs = errors.New("insufficient GPU resources available")
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

// getKey creates and returns a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the ResourceManager.
func getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

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

	// Millicpus returns the gpus as a float64.
	// The units are millicpus, or 1/1000th of a CPU core.
	Millicpus() float64
	// MillicpusAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the number of cpus.
	// The units are millicpus, or 1/1000th of a CPU core.
	MillicpusAsDecimal() decimal.Decimal

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

	// cachedAllocationKey is the cached return value of getKey(ResourceAllocation.ReplicaId, ResourceAllocation.KernelId).
	cachedAllocationKey string
}

// assertIsPending returns true if the target *ResourceAllocation has AllocationType equal to PendingAllocation.
// If the target *ResourceAllocation has CommittedAllocation equal to PendingAllocation, then this function will panic.
func (a *ResourceAllocation) assertIsPending() bool {
	if a.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %s", a.String()))
}

// assertIsCommitted returns true if the target *ResourceAllocation has AllocationType equal to CommittedAllocation
// If the target *ResourceAllocation has AllocationType equal to PendingAllocation, then this function will panic.
func (a *ResourceAllocation) assertIsCommitted() bool {
	if !a.IsPending() {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation IS pending: %s", a.String()))
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
	return fmt.Sprintf("ResourceSpec[CPUs: %s, Memory: %s MB, GPUs: %s]",
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

// resources is a struct used by the ResourceManager to track its total idle, pending, committed, and spec resources
// of each type (CPU, GPU, and Memory).
type resources struct {
	sync.Mutex // Enables atomic access to each individual field.

	resourceType ResourceType    // resourceType is the ResourceType represented/encoded by this struct.
	millicpus    decimal.Decimal // millicpus is CPU in 1/1000th of CPU core.
	gpus         decimal.Decimal // gpus is the number of GPUs.
	memoryMB     decimal.Decimal // memoryMB is the amount of memory in MB.
}

func (res *resources) String() string {
	res.Lock()
	defer res.Unlock()

	return fmt.Sprintf("[%s resources: millicpus=%s,gpus=%s,memoryMB=%s]",
		res.resourceType.String(), res.millicpus.StringFixed(0),
		res.gpus.StringFixed(0), res.memoryMB.StringFixed(4))
}

func (res *resources) ResourceType() ResourceType {
	return res.resourceType
}

func (res *resources) MemoryMB() float64 {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.InexactFloat64()
}

func (res *resources) MemoryMbAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.Copy()
}

// SetMemoryMB sets the amount of memory to a copy of the specified decimal.Decimal value.
func (res *resources) SetMemoryMB(memoryMB decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.memoryMB = memoryMB
}

func (res *resources) GPUs() float64 {
	res.Lock()
	defer res.Unlock()

	return res.gpus.InexactFloat64()
}

func (res *resources) GPUsAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.gpus.Copy()
}

// SetGpus sets the number of GPUs to a copy of the specified decimal.Decimal value.
func (res *resources) SetGpus(gpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.gpus = gpus.Copy()
}

func (res *resources) Millicpus() float64 {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.InexactFloat64()
}

func (res *resources) MillicpusAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.Copy()
}

// SetMillicpus sets the number of CPUs to a copy of the specified decimal.Decimal value.
func (res *resources) SetMillicpus(millicpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.millicpus = millicpus
}

// Validate returns true if each of the resources' cpu, gpu, and memory are greater than or equal to the respective
// resource of the given types.DecimalSpec.
func (res *resources) Validate(spec *types.DecimalSpec) bool {
	res.Lock()
	defer res.Unlock()

	return res.gpus.GreaterThanOrEqual(spec.GPUs) &&
		res.millicpus.GreaterThanOrEqual(spec.Millicpus) &&
		res.memoryMB.GreaterThanOrEqual(spec.MemoryMb)
}

// ValidateWithError returns nil if each of the resources' cpu, gpu, and memory are greater than or equal to the
// respective resource of the given types.DecimalSpec. That is, if the given types.DecimalSpec is validated, so to
// speak, then ValidateWithError will return nil.
//
// If the specified types.DecimalSpec is NOT validated, then an error is returned.
// This error indicates which of the resources' cpu, gpu, and/or memory were insufficient to validate the given spec.
func (res *resources) ValidateWithError(spec *types.DecimalSpec) error {
	res.Lock()
	defer res.Unlock()

	sufficientGPUsAvailable := res.gpus.GreaterThanOrEqual(spec.GPUs)
	sufficientCPUsAvailable := res.millicpus.GreaterThanOrEqual(spec.Millicpus)
	sufficientMemoryAvailable := res.memoryMB.GreaterThanOrEqual(spec.MemoryMb)

	errs := make([]error, 0)
	if !sufficientGPUsAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientGPUs, res.gpus.StringFixed(0), spec.GPUs.StringFixed(0))
		errs = append(errs, err)
	}

	if !sufficientCPUsAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientCPUs, res.millicpus.StringFixed(0), spec.Millicpus.StringFixed(0))
		errs = append(errs, err)
	}

	if !sufficientMemoryAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientMemory, res.memoryMB.StringFixed(0), spec.MemoryMb.StringFixed(0))
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	} else {
		return nil
	}
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

func (r *resourcesWrapper) IdleResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources
}

func (r *resourcesWrapper) PendingResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources
}

func (r *resourcesWrapper) CommittedResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources
}

func (r *resourcesWrapper) SpecResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources
}

// ResourceManager is responsible for keeping track of resource allocations on behalf of the Local Daemon.
// The ResourceManager allocates and deallocates resources to/from kernel replicas scheduled to run on the node.
//
// ResourceManager is a replacement for GpuManager.
//
// In general, ResourceManager elects to work with *types.DecimalSpec structs internally, rather than arbitrary
// types.Spec interface instances, as ResourceManager stores its own state in decimal.Decimal structs.
type ResourceManager struct {
	mu sync.Mutex

	id  string        // Unique ID of the Resource Manager.
	log logger.Logger // Logger.

	// allocationKernelReplicaMap is a map from "<KernelID>-<ReplicaID>" -> *ResourceAllocation.
	// That is, allocationKernelReplicaMap is a mapping in which keys are strings of the form
	// "<KernelID>-<ReplicaID>" and values are *ResourceAllocation.
	//
	// allocationIdMap contains ResourceAllocation structs of both types (CommittedAllocation and PendingAllocation).
	allocationKernelReplicaMap hashmap.HashMap[string, *ResourceAllocation]

	// resourcesWrapper encapsulates the state of all resources (idle, pending, committed, and spec) managed
	// by this ResourceManager.
	resourcesWrapper *resourcesWrapper

	// resourceMetricsCallback is a callback function that is supposed to be triggered whenever resources
	// are allocated or deallocated so that the associated Prometheus metrics can be updated accordingly.
	resourceMetricsCallback resourceMetricsCallback

	// numPendingAllocations is the number of active ResourceAllocation instances of type PendingAllocation.
	numPendingAllocations types.StatInt32
	// numCommittedAllocations is the number of active ResourceAllocation instances of type CommittedAllocation.
	numCommittedAllocations types.StatInt32
}

// NewResourceManager creates a new ResourceManager struct and returns a pointer to it.
func NewResourceManager(resourceSpec types.Spec, resourceMetricsCallback resourceMetricsCallback) *ResourceManager {
	manager := &ResourceManager{
		id:                         uuid.NewString(),
		allocationKernelReplicaMap: hashmap.NewCornelkMap[string, *ResourceAllocation](128),
		resourceMetricsCallback:    resourceMetricsCallback,
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

	manager.numPendingAllocations.Store(0)
	manager.numCommittedAllocations.Store(0)

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

	key := getKey(replicaId, kernelId)
	alloc, ok := m.allocationKernelReplicaMap.Load(key)
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
	return alloc.GPUs.GreaterThan(ZeroDecimal)
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
func (m *ResourceManager) CommitResources(replicaId int32, kernelId string, adjustedResourceRequest types.Spec) error {
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
		return fmt.Errorf("%w: no pending resource allocation found for replica %d of kernel %s",
			ErrInvalidAllocationRequest, replicaId, kernelId)
	}

	// Sanity check, essentially. It should not already be committed.
	allocation.assertIsPending()

	var requestedResources *types.DecimalSpec
	if adjustedResourceRequest == nil {
		requestedResources = types.ToDecimalSpec(adjustedResourceRequest)
	} else {
		requestedResources = allocation.ToDecimalSpec()
	}

	m.log.Debug("Attempting to commit the following resources to replica %d of kernel %s: %v",
		replicaId, kernelId, requestedResources.String())

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
	m.resourcesWrapper.idleResources.SetMillicpus(m.resourcesWrapper.idleResources.millicpus.Sub(requestedResources.Millicpus))
	m.resourcesWrapper.idleResources.SetMemoryMB(m.resourcesWrapper.idleResources.memoryMB.Sub(requestedResources.MemoryMb))
	m.resourcesWrapper.idleResources.SetGpus(m.resourcesWrapper.idleResources.gpus.Sub(requestedResources.GPUs))

	// Next, we'll decrement the pending resources.
	m.resourcesWrapper.pendingResources.SetMillicpus(m.resourcesWrapper.pendingResources.millicpus.Sub(requestedResources.Millicpus))
	m.resourcesWrapper.pendingResources.SetMemoryMB(m.resourcesWrapper.pendingResources.memoryMB.Sub(requestedResources.MemoryMb))
	m.resourcesWrapper.pendingResources.SetGpus(m.resourcesWrapper.pendingResources.gpus.Sub(requestedResources.GPUs))

	// Next, we'll increment the committed resources.
	m.resourcesWrapper.committedResources.SetMillicpus(m.resourcesWrapper.committedResources.millicpus.Add(requestedResources.Millicpus))
	m.resourcesWrapper.committedResources.SetMemoryMB(m.resourcesWrapper.committedResources.memoryMB.Add(requestedResources.MemoryMb))
	m.resourcesWrapper.committedResources.SetGpus(m.resourcesWrapper.committedResources.gpus.Add(requestedResources.GPUs))

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

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Decr()
	m.numCommittedAllocations.Incr()

	m.log.Debug("Successfully committed the following resources to replica %d of kernel %s: %v",
		replicaId, kernelId, requestedResources.String())

	// Update Prometheus metrics.
	m.resourceMetricsCallback(m.resourcesWrapper)

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
	allocation.assertIsCommitted()
	allocatedResources := allocation.ToDecimalSpec()

	m.log.Debug("Attempting to release the following committed resources from replica %d of kernel %s: %v",
		replicaId, kernelId, allocatedResources.String())

	// If we've gotten this far, then we have enough resources available to commit the requested resources
	// to the specified kernel replica. So, let's do that now. First, we'll increment the idle resources.
	m.resourcesWrapper.idleResources.SetMillicpus(m.resourcesWrapper.idleResources.millicpus.Add(allocatedResources.Millicpus))
	m.resourcesWrapper.idleResources.SetMemoryMB(m.resourcesWrapper.idleResources.memoryMB.Add(allocatedResources.MemoryMb))
	m.resourcesWrapper.idleResources.SetGpus(m.resourcesWrapper.idleResources.gpus.Add(allocatedResources.GPUs))

	// Next, we'll increment the pending resources (since we're releasing committed resources).
	m.resourcesWrapper.pendingResources.SetMillicpus(m.resourcesWrapper.pendingResources.millicpus.Add(allocatedResources.Millicpus))
	m.resourcesWrapper.pendingResources.SetMemoryMB(m.resourcesWrapper.pendingResources.memoryMB.Add(allocatedResources.MemoryMb))
	m.resourcesWrapper.pendingResources.SetGpus(m.resourcesWrapper.pendingResources.gpus.Add(allocatedResources.GPUs))

	// Next, we'll decrement the committed resources (since we're releasing committed resources).
	m.resourcesWrapper.committedResources.SetMillicpus(m.resourcesWrapper.committedResources.millicpus.Sub(allocatedResources.Millicpus))
	m.resourcesWrapper.committedResources.SetMemoryMB(m.resourcesWrapper.committedResources.memoryMB.Sub(allocatedResources.MemoryMb))
	m.resourcesWrapper.committedResources.SetGpus(m.resourcesWrapper.committedResources.gpus.Sub(allocatedResources.GPUs))

	// Finally, we'll update the ResourceAllocation struct associated with this request.
	// This involves updating its AllocationType field to be PendingAllocation.
	//
	// Once updated, we'll remove it from the pending allocation maps and add it to the committed allocation maps.
	allocation.AllocationType = PendingAllocation

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()
	m.numCommittedAllocations.Decr()

	m.log.Debug("Successfully released the following (previously) committed resources to replica %d of kernel %s: %v",
		replicaId, kernelId, allocatedResources.String())

	// Update Prometheus metrics.
	m.resourceMetricsCallback(m.resourcesWrapper)

	return nil
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
		WithMillicpus(spec.MemoryMB()).
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
	m.resourcesWrapper.pendingResources.SetMillicpus(m.resourcesWrapper.pendingResources.millicpus.Add(decimalSpec.Millicpus))
	m.resourcesWrapper.pendingResources.SetMemoryMB(m.resourcesWrapper.pendingResources.memoryMB.Add(decimalSpec.MemoryMb))
	m.resourcesWrapper.pendingResources.SetGpus(m.resourcesWrapper.pendingResources.gpus.Add(decimalSpec.GPUs))

	// Update the pending/committed allocation counters.
	m.numPendingAllocations.Incr()

	m.log.Debug("Successfully subscribed the following pending resources to replica %d of kernel %s: %v",
		replicaId, kernelId, decimalSpec.String())

	// Update Prometheus metrics.
	m.resourceMetricsCallback(m.resourcesWrapper)

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

	m.numPendingAllocations.Decr()

	// TODO: Implement me.
	panic("Not implemented")
}
