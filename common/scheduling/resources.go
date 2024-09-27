package scheduling

import (
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"log"
	"sync"
)

const (
	// IdleResources can overlap with pending resources. These are resources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources ResourceStatus = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending resources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending resources on a node is the amount of resources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources ResourceStatus = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources ResourceStatus = "committed"

	// SpecResources are the total allocatable resources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources ResourceStatus = "spec"

	// NoResource is a sort of default value for ResourceKind.
	NoResource ResourceKind = "N/A"
	CPU        ResourceKind = "CPU"
	GPU        ResourceKind = "GPU"
	Memory     ResourceKind = "Memory"

	// NegativeResourceQuantity indicates that the inconsistent/invalid resource is
	// inconsistent/invalid because its quantity is negative.
	NegativeResourceQuantity ResourceInconsistency = "negative_quantity"

	// ResourceQuantityGreaterThanSpec indicates that the inconsistent/invalid resource
	// is inconsistent/invalid because its quantity is greater than that of the scheduling.Host
	// instances types.Spec quantity.
	ResourceQuantityGreaterThanSpec ResourceInconsistency = "quantity_greater_than_spec"

	// IdleSpecUnequal indicates that our IdleResources and SpecResources are unequal despite having no kernel
	// replicas scheduled locally on the node. (When the ndoe is empty, all our resources should be idle.)
	IdleSpecUnequal ResourceInconsistency = "idle_and_spec_resources_unequal"

	// PendingNonzero indicates that our PendingResources are non-zero despite having no replicas scheduled locally.
	PendingNonzero ResourceInconsistency = "pending_nonzero"
)

var (
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

// ResourceKind can be one of CPU, GPU, or Memory
type ResourceKind string

// ResourceInconsistency defines the various ways in which resources can be in an inconsistent or illegal state.
// Examples include a resource being negative, a resource quantity being larger than the total available resources
// of that kind on the node, and so on.
type ResourceInconsistency string

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
	// ResourceStatus returns the ResourceStatus of the resources encapsulated/made available for reading
	// by this ResourceState instance.
	ResourceStatus() ResourceStatus

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

	// ResourceSnapshot creates and returns a pointer to a new ResourceSnapshot struct, thereby
	// capturing the current quantities of the resources encoded by the ResourceState instance.
	ResourceSnapshot() *ResourceSnapshot
}

// ResourceStatus differentiates between idle, pending, committed, and spec resources.
type ResourceStatus string

func (t ResourceStatus) String() string {
	return string(t)
}

// ResourceSnapshot is a snapshot of a resources struct with exported
// fields so that it can be marshalled and unmarshalled to JSON.
type ResourceSnapshot struct {
	ResourceStatus ResourceStatus  `json:"resource_status"` // resourceStatus is the ResourceStatus represented/encoded by this struct.
	Millicpus      decimal.Decimal `json:"millicpus"`       // millicpus is CPU in 1/1000th of CPU core.
	Gpus           decimal.Decimal `json:"gpus"`            // gpus is the number of GPUs.
	MemoryMB       decimal.Decimal `json:"memoryMB"`        // memoryMB is the amount of memory in MB.
}

// String returns a string representation of the target ResourceSnapshot struct that is suitable for logging.
func (s *ResourceSnapshot) String() string {
	return fmt.Sprintf("ResourceSnapshot[Status=%s,Millicpus=%s,MemoryMB=%s,GPUs=%s",
		s.ResourceStatus.String(), s.Millicpus.StringFixed(0), s.MemoryMB.StringFixed(4), s.Gpus.StringFixed(0))
}

// resources is a struct used by the ResourceManager to track its total idle, pending, committed, and spec resources
// of each type (CPU, GPU, and Memory).
type resources struct {
	sync.Mutex // Enables atomic access to each individual field.

	resourceStatus ResourceStatus  // resourceStatus is the ResourceStatus represented/encoded by this struct.
	millicpus      decimal.Decimal // millicpus is CPU in 1/1000th of CPU core.
	gpus           decimal.Decimal // gpus is the number of GPUs.
	memoryMB       decimal.Decimal // memoryMB is the amount of memory in MB.
}

// ResourceSnapshot constructs and returns a pointer to a new ResourceSnapshot struct.
//
// This method is thread-safe to ensure that the quantities of each resource are all captured atomically.
func (res *resources) ResourceSnapshot() *ResourceSnapshot {
	res.Lock()
	defer res.Unlock()

	snapshot := &ResourceSnapshot{
		ResourceStatus: res.resourceStatus,
		Millicpus:      res.millicpus,
		Gpus:           res.gpus,
		MemoryMB:       res.memoryMB,
	}

	return snapshot
}

// ToDecimalSpec returns a pointer to a types.DecimalSpec struct that encapsulates a snapshot of
// the current quantities of resources encoded/maintained by the target resources struct.
//
// This method is thread-safe to ensure that the quantity of each individual resource type cannot
// be modified during the time that the new types.DecimalSpec struct is being constructed.
func (res *resources) ToDecimalSpec() *types.DecimalSpec {
	res.Lock()
	defer res.Unlock()

	return &types.DecimalSpec{
		GPUs:      res.gpus.Copy(),
		Millicpus: res.millicpus.Copy(),
		MemoryMb:  res.memoryMB.Copy(),
	}
}

// LessThan returns true if each field of the target 'resources' struct is strictly less than the corresponding field
// of the other 'resources' struct.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not less than the corresponding field of the other 'resources'
// struct, then false is returned.
//
// The ResourceKind are checked in the following order: CPU, Memory, GPU.
// The ResourceKind of the first offending quantity will be returned, along with false, based on that order.
func (res *resources) LessThan(other *resources) (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	other.Lock()
	defer other.Unlock()

	if !res.millicpus.LessThan(other.millicpus) {
		return false, CPU
	}

	if !res.memoryMB.LessThan(other.memoryMB) {
		return false, Memory
	}

	if !res.gpus.LessThan(other.gpus) {
		return false, GPU
	}

	return true, NoResource
}

// LessThanOrEqual returns true if each field of the target 'resources' struct is less than or equal to the
// corresponding field of the other 'resources' struct.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not less than or equal to the corresponding field of the
// other 'resources' struct, then false is returned.
func (res *resources) LessThanOrEqual(other *resources) (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	other.Lock()
	defer other.Unlock()

	if !res.millicpus.LessThanOrEqual(other.millicpus) {
		return false, CPU
	}

	if !res.memoryMB.LessThanOrEqual(other.memoryMB) {
		return false, Memory
	}

	if !res.gpus.LessThanOrEqual(other.gpus) {
		return false, GPU
	}

	return true, NoResource
}

// GreaterThan returns true if each field of the target 'resources' struct is strictly greater than to the
// corresponding field of the other 'resources' struct.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not strictly greater than the corresponding field of the
// other 'resources' struct, then false is returned.
func (res *resources) GreaterThan(other *resources) (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	other.Lock()
	defer other.Unlock()

	if !res.millicpus.GreaterThan(other.millicpus) {
		return false, CPU
	}

	if !res.memoryMB.GreaterThan(other.memoryMB) {
		return false, Memory
	}

	if !res.gpus.GreaterThan(other.gpus) {
		return false, GPU
	}

	return true, NoResource
}

// GreaterThanOrEqual returns true if each field of the target 'resources' struct is greater than or equal to the
// corresponding field of the other 'resources' struct.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not greater than or equal to the corresponding field of the
// other 'resources' struct, then false is returned.
func (res *resources) GreaterThanOrEqual(other *resources) (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	other.Lock()
	defer other.Unlock()

	if !res.millicpus.GreaterThanOrEqual(other.millicpus) {
		return false, CPU
	}

	if !res.memoryMB.GreaterThanOrEqual(other.memoryMB) {
		return false, Memory
	}

	if !res.gpus.GreaterThanOrEqual(other.gpus) {
		return false, GPU
	}

	return true, NoResource
}

// EqualTo returns true if each field of the target 'resources' struct is exactly equal to the corresponding field of
// the other 'resources' struct.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not equal to the corresponding field of the other 'resources'
// struct, then false is returned.
func (res *resources) EqualTo(other *resources) (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	other.Lock()
	defer other.Unlock()

	if !res.millicpus.Equals(other.millicpus) {
		return false, CPU
	}

	if !res.memoryMB.Equals(other.memoryMB) {
		return false, Memory
	}

	if !res.gpus.Equals(other.gpus) {
		return false, GPU
	}

	return true, NoResource
}

// IsZero returns true if each field of the target 'resources' struct is exactly equal to 0.
//
// This method locks both 'resources' instances, beginning with the target instance.
//
// If any field of the target 'resources' struct is not equal to 0, then false is returned.
func (res *resources) IsZero() (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	if !res.millicpus.Equals(decimal.Zero) {
		return false, CPU
	}

	if !res.memoryMB.Equals(decimal.Zero) {
		return false, Memory
	}

	if !res.gpus.Equals(decimal.Zero) {
		return false, GPU
	}

	return true, NoResource
}

// GetResource returns a copy of the decimal.Decimal corresponding with the specified ResourceKind.
//
// This method is thread-safe.
//
// If kind is equal to NoResource, then this method will panic.
func (res *resources) GetResource(kind ResourceKind) decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	if kind == CPU {
		return res.millicpus.Copy()
	}

	if kind == Memory {
		return res.memoryMB.Copy()
	}

	if kind == GPU {
		return res.gpus.Copy()
	}

	panic(fmt.Sprintf("Invalid ResourceKind specified: \"%s\"", kind))
}

// HasNegativeField returns true if millicpus, gpus, or memoryMB is negative.
// It also returns the ResourceKind of the negative field.
//
// This method is thread-safe.
//
// The resources are checked in the following order: CPU, Memory, GPU.
// This method will return true and the associated ResourceKind for the first negative ResourceKind encountered.
//
// If no resources are negative, then this method returns false and NoResource.
func (res *resources) HasNegativeField() (bool, ResourceKind) {
	res.Lock()
	defer res.Unlock()

	if res.millicpus.IsNegative() {
		return true, CPU
	}

	if res.memoryMB.IsNegative() {
		return true, Memory
	}

	if res.gpus.IsNegative() {
		return true, GPU
	}

	return false, NoResource
}

func (res *resources) String() string {
	res.Lock()
	defer res.Unlock()

	return fmt.Sprintf("[%s resources: millicpus=%s,gpus=%s,memoryMB=%s]",
		res.resourceStatus.String(), res.millicpus.StringFixed(0),
		res.gpus.StringFixed(0), res.memoryMB.StringFixed(4))
}

func (res *resources) ResourceStatus() ResourceStatus {
	return res.resourceStatus
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

// Add adds the resources encapsulated in the given types.DecimalSpec to the resources' internal resource counts.
//
// If performing this operation were to result in any of the resources' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the resources' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *resources) Add(spec *types.DecimalSpec) error {
	res.Lock()
	defer res.Unlock()

	updatedCPUs := res.millicpus.Add(spec.Millicpus)
	if updatedCPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s CPUs would be set to %s millicpus after addition (current=%s,addend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedCPUs.String(),
			res.millicpus.StringFixed(0), spec.Millicpus.StringFixed(0))
	}

	updatedMemory := res.memoryMB.Add(spec.MemoryMb)
	if updatedMemory.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s memory would be equal to %s megabytes after addition (current=%s,addend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedMemory.String(),
			res.memoryMB.StringFixed(4), spec.MemoryMb.StringFixed(4))
	}

	updatedGPUs := res.gpus.Add(spec.GPUs)
	if updatedGPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s GPUs would be set to %s GPUs after addition (current=%s,addend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedGPUs.String(),
			res.gpus.StringFixed(0), spec.GPUs.StringFixed(0))
	}

	// If we've gotten to this point, then all the updated resource counts are valid, at least with respect
	// to not being negative. Persist the changes and return nil, indicating that the addition operation was successful.
	res.gpus = updatedGPUs
	res.millicpus = updatedCPUs
	res.memoryMB = updatedMemory

	return nil
}

// Subtract subtracts the resources encapsulated in the given types.DecimalSpec from the resources' own internal counts.
//
// If performing this operation were to result in any of the resources' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the resources' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *resources) Subtract(spec *types.DecimalSpec) error {
	res.Lock()
	defer res.Unlock()

	updatedCPUs := res.millicpus.Sub(spec.Millicpus)
	if updatedCPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s CPUs would be set to %s millicpus after subtraction (current=%s,subtrahend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedCPUs.String(),
			res.millicpus.StringFixed(0), spec.Millicpus.StringFixed(0))
	}

	updatedMemory := res.memoryMB.Sub(spec.MemoryMb)
	if updatedMemory.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s memory would be equal to %s megabytes after subtraction (current=%s,subtrahend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedMemory.String(),
			res.memoryMB.StringFixed(4), spec.MemoryMb.StringFixed(4))
	}

	updatedGPUs := res.gpus.Sub(spec.GPUs)
	if updatedGPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s GPUs would be set to %s GPUs after subtraction (current=%s,subtrahend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedGPUs.String(),
			res.gpus.StringFixed(0), spec.GPUs.StringFixed(0))
	}

	// If we've gotten to this point, then all the updated resource counts are valid, at least with respect
	// to not being negative. Persist the changes and return nil, indicating that the subtract operation was successful.
	res.gpus = updatedGPUs
	res.millicpus = updatedCPUs
	res.memoryMB = updatedMemory

	return nil

}

// Validate returns true if each of the resources' cpu, gpu, and memory are greater than or equal to the respective
// resource of the given types.DecimalSpec.
func (res *resources) Validate(spec types.Spec) bool {
	res.Lock()
	defer res.Unlock()

	// Convert the given types.Spec to a *types.DecimalSpec.
	var decimalSpec *types.DecimalSpec
	if specAsDecimalSpec, ok := spec.(*types.DecimalSpec); ok {
		// If the parameter is already a *types.DecimalSpec, then no actual conversion needs to be performed.
		decimalSpec = specAsDecimalSpec
	} else {
		decimalSpec = types.ToDecimalSpec(spec)
	}

	return res.gpus.GreaterThanOrEqual(decimalSpec.GPUs) &&
		res.millicpus.GreaterThanOrEqual(decimalSpec.Millicpus) &&
		res.memoryMB.GreaterThanOrEqual(decimalSpec.MemoryMb)
}

// ValidateWithError returns nil if each of the resources' cpu, gpu, and memory are greater than or equal to the
// respective resource of the given types.DecimalSpec. That is, if the given types.DecimalSpec is validated, so to
// speak, then ValidateWithError will return nil.
//
// If the specified types.DecimalSpec is NOT validated, then an error is returned.
// This error indicates which of the resources' cpu, gpu, and/or memory were insufficient to validate the given spec.
func (res *resources) ValidateWithError(spec types.Spec) error {
	res.Lock()
	defer res.Unlock()

	// Convert the given types.Spec to a *types.DecimalSpec.
	var decimalSpec *types.DecimalSpec
	if specAsDecimalSpec, ok := spec.(*types.DecimalSpec); ok {
		// If the parameter is already a *types.DecimalSpec, then no actual conversion needs to be performed.
		decimalSpec = specAsDecimalSpec
	} else {
		decimalSpec = types.ToDecimalSpec(spec)
	}

	sufficientGPUsAvailable := res.gpus.GreaterThanOrEqual(decimalSpec.GPUs)
	sufficientCPUsAvailable := res.millicpus.GreaterThanOrEqual(decimalSpec.Millicpus)
	sufficientMemoryAvailable := res.memoryMB.GreaterThanOrEqual(decimalSpec.MemoryMb)

	errs := make([]error, 0)
	if !sufficientGPUsAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientGPUs, res.gpus.StringFixed(0), decimalSpec.GPUs.StringFixed(0))
		errs = append(errs, err)
	}

	if !sufficientCPUsAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientCPUs, res.millicpus.StringFixed(0), decimalSpec.Millicpus.StringFixed(0))
		errs = append(errs, err)
	}

	if !sufficientMemoryAvailable {
		err := fmt.Errorf("%w: available=%s,required=%s",
			ErrInsufficientMemory, res.memoryMB.StringFixed(0), decimalSpec.MemoryMb.StringFixed(0))
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

// String returns a string representation of the resourcesWrapper that is suitable for logging.
func (r *resourcesWrapper) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Sprintf("resourcesWrapper{%s, %s, %s, %s}",
		r.idleResources.String(), r.pendingResources.String(), r.committedResources.String(), r.specResources.String())
}

// IdleResources returns a ResourceState that is responsible for encoding the current idle resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) IdleResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources
}

// PendingResources returns a ResourceState that is responsible for encoding the current pending resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) PendingResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources
}

// CommittedResources returns a ResourceState that is responsible for encoding the current committed resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) CommittedResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources
}

// SpecResources returns a ResourceState that is responsible for encoding the current spec resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) SpecResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources
}

// IdleResourcesSnapshot returns a *ResourceSnapshot struct capturing the current idle resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) IdleResourcesSnapshot() *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources.ResourceSnapshot()
}

// PendingResourcesSnapshot returns a *ResourceSnapshot struct capturing the current pending resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) PendingResourcesSnapshot() *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources.ResourceSnapshot()
}

// CommittedResourcesSnapshot returns a *ResourceSnapshot struct capturing the current committed resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) CommittedResourcesSnapshot() *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources.ResourceSnapshot()
}

// SpecResourcesSnapshot returns a *ResourceSnapshot struct capturing the current spec resources
// of the target resourcesWrapper.
func (r *resourcesWrapper) SpecResourcesSnapshot() *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources.ResourceSnapshot()
}

// ResourceSnapshot returns a pointer to a ResourceSnapshot created for the specified "status" of resources
// (i.e., "idle", "pending", "committed", or "spec").
func (r *resourcesWrapper) ResourceSnapshot(status ResourceStatus) *ResourceSnapshot {
	switch status {
	case IdleResources:
		{
			return r.IdleResourcesSnapshot()
		}
	case PendingResources:
		{
			return r.PendingResourcesSnapshot()
		}
	case CommittedResources:
		{
			return r.CommittedResourcesSnapshot()
		}
	case SpecResources:
		{
			return r.SpecResourcesSnapshot()
		}
	default:
		{
			log.Fatalf("Unknown or unexpected ResourceStatus specified: \"%s\"", status)
			return nil
		}
	}
}
