package scheduling

import (
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"log"
	"reflect"
	"sync"
	"time"
)

const (
	// IdleResources can overlap with pending Resources. These are Resources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources ResourceStatus = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending Resources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending Resources on a node is the amount of Resources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources ResourceStatus = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources ResourceStatus = "committed"

	// SpecResources are the total allocatable Resources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources ResourceStatus = "spec"

	// NoResource is a sort of default value for ResourceKind.
	NoResource ResourceKind = "N/A"
	CPU        ResourceKind = "CPU"
	GPU        ResourceKind = "GPU"
	VRAM       ResourceKind = "VRAM"
	Memory     ResourceKind = "Memory"

	// NegativeResourceQuantity indicates that the inconsistent/invalid resource is
	// inconsistent/invalid because its quantity is negative.
	NegativeResourceQuantity ResourceInconsistency = "negative_quantity"

	// ResourceQuantityGreaterThanSpec indicates that the inconsistent/invalid resource
	// is inconsistent/invalid because its quantity is greater than that of the scheduling.Host
	// instances types.Spec quantity.
	ResourceQuantityGreaterThanSpec ResourceInconsistency = "quantity_greater_than_spec"

	// IdleSpecUnequal indicates that our IdleResources and SpecResources are unequal despite having no kernel
	// replicas scheduled locally on the node. (When the ndoe is empty, all our Resources should be idle.)
	IdleSpecUnequal ResourceInconsistency = "idle_and_spec_resources_unequal"

	// PendingNonzero indicates that our PendingResources are non-zero despite having no replicas scheduled locally.
	PendingNonzero ResourceInconsistency = "pending_nonzero"
)

var (
	// ErrInsufficientMemory indicates that there was insufficient memory Resources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientMemory = errors.New("insufficient memory Resources available")

	// ErrInsufficientCPUs indicates that there was insufficient CPU Resources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientCPUs = errors.New("insufficient CPU Resources available")

	// ErrInsufficientGPUs indicates that there was insufficient GPU Resources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientGPUs = errors.New("insufficient GPU Resources available")

	// ErrInvalidSnapshot is a general error message indicating that the application of a snapshot has failed.
	ErrInvalidSnapshot = errors.New("the specified snapshot could not be applied")

	// ErrIncompatibleResourceStatus is a specific reason for why the application of a snapshot may fail.
	// If the source and target ResourceStatus values do not match, then the snapshot will be rejected.
	ErrIncompatibleResourceStatus = errors.New("source and target ResourceStatus values are not the same")
)

// InsufficientResourcesError is a custom error type that is used to indicate that Resources could not be
// allocated because there are insufficient Resources available for one or more Resources (CPU, GPU, or RAM).
type InsufficientResourcesError struct {
	// AvailableResources are the Resources that were available on the node at the time that the
	// failed allocation was attempted.
	AvailableResources types.Spec
	// RequestedResources are the Resources that were requested, and that could not be fulfilled in their entirety.
	RequestedResources types.Spec
	// OffendingResourceKinds is a slice containing each ResourceKind for which there were insufficient
	// Resources available (and thus that particular ResourceKind contributed to the inability of the node
	// to fulfill the resource request).
	OffendingResourceKinds []ResourceKind
}

// NewInsufficientResourcesError constructs a new InsufficientResourcesError struct and returns a pointer to it.
func NewInsufficientResourcesError(avail types.Spec, req types.Spec, kinds []ResourceKind) *InsufficientResourcesError {
	return &InsufficientResourcesError{
		AvailableResources:     avail,
		RequestedResources:     req,
		OffendingResourceKinds: kinds,
	}
}

func (e InsufficientResourcesError) Error() string {
	return e.String()
}

func (e InsufficientResourcesError) Is(other error) bool {
	var insufficientResourcesError *InsufficientResourcesError
	if !errors.As(other, &insufficientResourcesError) {
		return false
	}

	return true
}

func (e InsufficientResourcesError) String() string {
	return fmt.Sprintf("InsufficientResourcesError[Available=%s,Requested=%s]",
		e.AvailableResources.String(), e.RequestedResources.String())
}

// ResourceKind can be one of CPU, GPU, or Memory
type ResourceKind string

// ResourceInconsistency defines the various ways in which Resources can be in an inconsistent or illegal state.
// Examples include a resource being negative, a resource quantity being larger than the total available Resources
// of that kind on the node, and so on.
type ResourceInconsistency string

// ResourceStateWrapper defines a public interface for accessing (i.e., reading) but not mutating (i.e., writing)
// the current state of a ResourceStateWrapper.
//
// ResourceStateWrapper wraps several ResourceState instances -- one for Resources of each of the following types:
// idle, pending, committed, and spec. As such, ResourceStateWrapper exposes a collection of several ResourceState
// instances to provide a convenient type for reading all the relevant state of a ResourceManager.
type ResourceStateWrapper interface {
	// IdleResources returns the idle Resources managed by a ResourceManager.
	// Idle Resources can overlap with pending Resources. These are Resources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources() ResourceState

	// PendingResources returns the pending Resources managed by a ResourceManager.
	// Pending Resources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending Resources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending Resources on a node is the amount of Resources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources() ResourceState

	// CommittedResources returns the committed Resources managed by a ResourceManager.
	// These are Resources that are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources() ResourceState

	// SpecResources returns the spec Resources managed by a ResourceManager.
	// These are the total allocatable Resources available on the Host.
	// Spec Resources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources() ResourceState

	// String returns a string representation of the ResourceStateWrapper suitable for logging.
	String() string
}

// ResourceState defines a public interface for getting (i.e., reading) but not mutating (i.e., writing)
// the current state of a ResourceManager.
//
// ResourceState encapsulates the Resources for a single type of resource (i.e., idle, pending, committed, or spec).
// Meanwhile, ResourceStateWrapper exposes a collection of several ResourceState instances to provide a convenient
// type for reading all the relevant state of a ResourceManager.
type ResourceState interface {
	// ResourceStatus returns the ResourceStatus of the Resources encapsulated/made available for reading
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

	// VRAM returns the amount of VRAM (in GB).
	VRAM() float64
	// VRAMAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of VRAM.
	// The units are gigabytes (GB).
	VRAMAsDecimal() decimal.Decimal

	// String returns a string representation of the ResourceState suitable for logging.
	String() string

	// ResourceSnapshot creates and returns a pointer to a new ResourceSnapshot struct, thereby
	// capturing the current quantities of the Resources encoded by the ResourceState instance.
	ResourceSnapshot(snapshotId int32) *ResourceSnapshot
}

// ResourceStatus differentiates between idle, pending, committed, and spec Resources.
type ResourceStatus string

func (t ResourceStatus) String() string {
	return string(t)
}

// ResourceSnapshot is a snapshot of a Resources struct with exported
// fields so that it can be marshalled and unmarshalled to JSON.
type ResourceSnapshot struct {
	ResourceStatus ResourceStatus  `json:"resource_status"` // resourceStatus is the ResourceStatus represented/encoded by this struct.
	Millicpus      decimal.Decimal `json:"millicpus"`       // millicpus is CPU in 1/1000th of CPU core.
	Gpus           decimal.Decimal `json:"gpus"`            // gpus is the number of GPUs.
	VRamGB         decimal.Decimal `json:"vram"`            // VRamGB is the amount of VRAM (GPU memory) in GBs.
	MemoryMB       decimal.Decimal `json:"memoryMB"`        // memoryMB is the amount of memory in MB.

	// SnapshotId uniquely identifies the HostResourceSnapshot in which this ResourceSnapshot struct will be included.
	// Specifically, the SnapshotId and defines a total order amongst all HostResourceSnapshot instances that originate
	// from the same node. Each newly-created HostResourceSnapshot is assigned an ID from a monotonically-increasing
	// counter by the ResourceManager from the associated Host.
	SnapshotId int32 `json:"snapshot_id"`
}

func (s *ResourceSnapshot) GetSnapshotId() int32 {
	return s.SnapshotId
}

func (s *ResourceSnapshot) GetResourceStatus() string {
	return s.ResourceStatus.String()
}

func (s *ResourceSnapshot) GetMillicpus() int32 {
	return int32(s.Millicpus.InexactFloat64())
}

func (s *ResourceSnapshot) GetMemoryMb() float32 {
	return float32(s.MemoryMB.InexactFloat64())
}

func (s *ResourceSnapshot) GetGpus() int32 {
	return int32(s.Gpus.InexactFloat64())
}

func (s *ResourceSnapshot) GetVramGb() float32 {
	return float32(s.VRamGB.InexactFloat64())
}

// String returns a string representation of the target ResourceSnapshot struct that is suitable for logging.
func (s *ResourceSnapshot) String() string {
	return fmt.Sprintf("ResourceSnapshot[Status=%s,Millicpus=%s,MemoryMB=%s,GPUs=%s,VRAM=%s",
		s.ResourceStatus.String(), s.Millicpus.StringFixed(0), s.MemoryMB.StringFixed(4),
		s.Gpus.StringFixed(0), s.VRamGB.StringFixed(4))
}

func (s *ResourceSnapshot) ToProtoResourcesSnapshot() *proto.ResourcesSnapshot {
	return &proto.ResourcesSnapshot{
		ResourceStatus: s.ResourceStatus.String(),
		Millicpus:      int32(s.Millicpus.InexactFloat64()),
		Gpus:           int32(s.Gpus.InexactFloat64()),
		VramGb:         float32(s.VRamGB.InexactFloat64()),
		MemoryMb:       float32(s.MemoryMB.InexactFloat64()),
		SnapshotId:     s.SnapshotId,
	}
}

// Resources is a struct used by the ResourceManager to track its total idle, pending, committed, and spec Resources
// of each type (CPU, GPU, and Memory).
type Resources struct {
	sync.Mutex // Enables atomic access to each individual field.

	// lastAppliedSnapshotId is the ID of the last snapshot that was applied to this Resources struct.
	lastAppliedSnapshotId int32

	resourceStatus ResourceStatus  // resourceStatus is the ResourceStatus represented/encoded by this struct.
	millicpus      decimal.Decimal // millicpus is CPU in 1/1000th of CPU core.
	gpus           decimal.Decimal // gpus is the number of GPUs.
	memoryMB       decimal.Decimal // memoryMB is the amount of memory in MB.
	vramGB         decimal.Decimal // vram is the amount of GPU memory in GB.
}

// ApplySnapshotToResources atomically overwrites its resource quantities with the quantities encoded
// in the given ArbitraryResourceSnapshot instance.
//
// ApplySnapshotToResources returns nil on success. The only failure possible is that the ArbitraryResourceSnapshot
// encodes Resources of a different "status" than the target Resources struct. For example, if the target
// Resources struct encodes "idle" Resources, whereas the given ArbitraryResourceSnapshot instance encodes
// "pending" Resources, then an error will be returned, and none of the resource quantities in the target
// Resources struct will be overwritten.
func ApplySnapshotToResources[T types.ArbitraryResourceSnapshot](res *Resources, snapshot T) error {
	res.Lock()
	defer res.Unlock()

	// Ensure that the snapshot corresponds to Resources of the same status as the target Resources struct.
	// If it doesn't, then we'll reject the snapshot.
	if res.resourceStatus.String() != snapshot.GetResourceStatus() {
		return fmt.Errorf("%w: %w", ErrInvalidSnapshot, ErrIncompatibleResourceStatus)
	}

	// Ensure that the snapshot being applied is not old. If it is old, then we'll reject it.
	if res.lastAppliedSnapshotId > snapshot.GetSnapshotId() {
		return fmt.Errorf("%w: %w (last applied ID=%d, given ID=%d)",
			ErrInvalidSnapshot, ErrOldSnapshot, res.lastAppliedSnapshotId, snapshot.GetSnapshotId())
	}

	res.millicpus = decimal.NewFromFloat(float64(snapshot.GetMillicpus()))
	res.memoryMB = decimal.NewFromFloat(float64(snapshot.GetMemoryMb()))
	res.gpus = decimal.NewFromFloat(float64(snapshot.GetGpus()))
	res.vramGB = decimal.NewFromFloat(float64(snapshot.GetVramGb()))
	res.lastAppliedSnapshotId = snapshot.GetSnapshotId()

	return nil
}

// ResourceSnapshot constructs and returns a pointer to a new ResourceSnapshot struct.
//
// This method is thread-safe to ensure that the quantities of each resource are all captured atomically.
func (res *Resources) ResourceSnapshot(snapshotId int32) *ResourceSnapshot {
	res.Lock()
	defer res.Unlock()

	snapshot := &ResourceSnapshot{
		ResourceStatus: res.resourceStatus,
		Millicpus:      res.millicpus,
		Gpus:           res.gpus,
		MemoryMB:       res.memoryMB,
		VRamGB:         res.vramGB,
		SnapshotId:     snapshotId,
	}

	return snapshot
}

// protoResourceSnapshot constructs and returns a pointer to a new protoResourceSnapshot struct.
//
// This method is thread-safe to ensure that the quantities of each resource are all captured atomically.
func (res *Resources) protoResourceSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	res.Lock()
	defer res.Unlock()

	snapshot := &proto.ResourcesSnapshot{
		ResourceStatus: res.resourceStatus.String(),
		Millicpus:      int32(res.millicpus.InexactFloat64()),
		Gpus:           int32(res.gpus.InexactFloat64()),
		VramGb:         float32(res.vramGB.InexactFloat64()),
		MemoryMb:       float32(res.memoryMB.InexactFloat64()),
		SnapshotId:     snapshotId,
	}

	return snapshot
}

// ToDecimalSpec returns a pointer to a types.DecimalSpec struct that encapsulates a snapshot of
// the current quantities of Resources encoded/maintained by the target Resources struct.
//
// This method is thread-safe to ensure that the quantity of each individual resource type cannot
// be modified during the time that the new types.DecimalSpec struct is being constructed.
func (res *Resources) ToDecimalSpec() *types.DecimalSpec {
	res.Lock()
	defer res.Unlock()

	return res.unsafeToDecimalSpec()
}

// unsafeToDecimalSpec returns a pointer to a types.DecimalSpec struct that encapsulates a snapshot of
// the current quantities of Resources encoded/maintained by the target Resources struct.
//
// This method is not thread-safe and should be called only by the ToDecimalSpec method, unless
// the Resources' lock is already held.
func (res *Resources) unsafeToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		GPUs:      res.gpus.Copy(),
		Millicpus: res.millicpus.Copy(),
		MemoryMb:  res.memoryMB.Copy(),
		VRam:      res.vramGB.Copy(),
	}
}

// LessThan returns true if each field of the target 'Resources' struct is strictly less than the corresponding field
// of the other 'Resources' struct.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not less than the corresponding field of the other 'Resources'
// struct, then false is returned.
//
// The ResourceKind are checked in the following order: CPU, Memory, GPU.
// The ResourceKind of the first offending quantity will be returned, along with false, based on that order.
func (res *Resources) LessThan(other *Resources) (bool, ResourceKind) {
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

	if !res.vramGB.LessThan(other.vramGB) {
		return false, VRAM
	}

	return true, NoResource
}

// LessThanOrEqual returns true if each field of the target 'Resources' struct is less than or equal to the
// corresponding field of the other 'Resources' struct.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not less than or equal to the corresponding field of the
// other 'Resources' struct, then false is returned.
func (res *Resources) LessThanOrEqual(other *Resources) (bool, ResourceKind) {
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

	if !res.vramGB.LessThanOrEqual(other.vramGB) {
		return false, VRAM
	}

	return true, NoResource
}

// GreaterThan returns true if each field of the target 'Resources' struct is strictly greater than to the
// corresponding field of the other 'Resources' struct.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not strictly greater than the corresponding field of the
// other 'Resources' struct, then false is returned.
func (res *Resources) GreaterThan(other *Resources) (bool, ResourceKind) {
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

	if !res.vramGB.GreaterThan(other.vramGB) {
		return false, VRAM
	}

	return true, NoResource
}

// GreaterThanOrEqual returns true if each field of the target 'Resources' struct is greater than or equal to the
// corresponding field of the other 'Resources' struct.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not greater than or equal to the corresponding field of the
// other 'Resources' struct, then false is returned.
func (res *Resources) GreaterThanOrEqual(other *Resources) (bool, ResourceKind) {
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

	if !res.vramGB.GreaterThanOrEqual(other.vramGB) {
		return false, VRAM
	}

	return true, NoResource
}

// EqualTo returns true if each field of the target 'Resources' struct is exactly equal to the corresponding field of
// the other 'Resources' struct.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not equal to the corresponding field of the other 'Resources'
// struct, then false is returned.
func (res *Resources) EqualTo(other *Resources) (bool, ResourceKind) {
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

	if !res.vramGB.Equals(other.vramGB) {
		return false, VRAM
	}

	return true, NoResource
}

// IsZero returns true if each field of the target 'Resources' struct is exactly equal to 0.
//
// This method locks both 'Resources' instances, beginning with the target instance.
//
// If any field of the target 'Resources' struct is not equal to 0, then false is returned.
func (res *Resources) IsZero() (bool, ResourceKind) {
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

	if !res.vramGB.Equals(decimal.Zero) {
		return false, VRAM
	}

	return true, NoResource
}

// GetResource returns a copy of the decimal.Decimal corresponding with the specified ResourceKind.
//
// This method is thread-safe.
//
// If kind is equal to NoResource, then this method will panic.
func (res *Resources) GetResource(kind ResourceKind) decimal.Decimal {
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

	if kind == VRAM {
		return res.vramGB.Copy()
	}

	panic(fmt.Sprintf("Invalid ResourceKind specified: \"%s\"", kind))
}

// HasNegativeField returns true if millicpus, gpus, or memoryMB is negative.
// It also returns the ResourceKind of the negative field.
//
// This method is thread-safe.
//
// The Resources are checked in the following order: CPU, Memory, GPU.
// This method will return true and the associated ResourceKind for the first negative ResourceKind encountered.
//
// If no Resources are negative, then this method returns false and NoResource.
func (res *Resources) HasNegativeField() (bool, ResourceKind) {
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

	if res.vramGB.IsNegative() {
		return true, VRAM
	}

	return false, NoResource
}

func (res *Resources) String() string {
	res.Lock()
	defer res.Unlock()

	return fmt.Sprintf("[%s Resources: millicpus=%s,gpus=%s,vram=%sGB,memory=%sMB]",
		res.resourceStatus.String(), res.millicpus.StringFixed(0),
		res.gpus.StringFixed(0), res.vramGB.StringFixed(4), res.memoryMB.StringFixed(4))
}

func (res *Resources) ResourceStatus() ResourceStatus {
	return res.resourceStatus
}

func (res *Resources) MemoryMB() float64 {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.InexactFloat64()
}

func (res *Resources) MemoryMbAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.Copy()
}

// SetMemoryMB sets the amount of memory to a copy of the specified decimal.Decimal value.
func (res *Resources) SetMemoryMB(memoryMB decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.memoryMB = memoryMB
}

// VRAM returns the amount of VRAM (in GB).
func (res *Resources) VRAM() float64 {
	res.Lock()
	defer res.Unlock()

	return res.vramGB.InexactFloat64()
}

// VRAMAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of VRAM.
// The units are gigabytes (GB).
func (res *Resources) VRAMAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.vramGB.Copy()
}

func (res *Resources) GPUs() float64 {
	res.Lock()
	defer res.Unlock()

	return res.gpus.InexactFloat64()
}

func (res *Resources) GPUsAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.gpus.Copy()
}

// SetGpus sets the number of GPUs to a copy of the specified decimal.Decimal value.
func (res *Resources) SetGpus(gpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.gpus = gpus.Copy()
}

func (res *Resources) Millicpus() float64 {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.InexactFloat64()
}

func (res *Resources) MillicpusAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.Copy()
}

// SetMillicpus sets the number of Millicpus to a copy of the specified decimal.Decimal value.
func (res *Resources) SetMillicpus(millicpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.millicpus = millicpus
}

// Add adds the Resources encapsulated in the given types.DecimalSpec to the Resources' internal resource counts.
//
// If performing this operation were to result in any of the Resources' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the Resources' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *Resources) Add(spec *types.DecimalSpec) error {
	res.Lock()
	defer res.Unlock()

	updatedCPUs := res.millicpus.Add(spec.Millicpus)
	if updatedCPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s Millicpus would be set to %s millicpus after addition (current=%s,addend=%s)",
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

	updatedVRAM := res.vramGB.Add(spec.VRam)
	if updatedVRAM.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s VRAM would be set to %s GB after subtraction (current=%s,subtrahend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedVRAM.String(),
			res.vramGB.StringFixed(0), spec.VRam.StringFixed(0))
	}

	// If we've gotten to this point, then all the updated resource counts are valid, at least with respect
	// to not being negative. Persist the changes and return nil, indicating that the addition operation was successful.
	res.gpus = updatedGPUs
	res.millicpus = updatedCPUs
	res.memoryMB = updatedMemory
	res.vramGB = updatedVRAM

	return nil
}

// Subtract subtracts the Resources encapsulated in the given types.DecimalSpec from the Resources' own internal counts.
//
// If performing this operation were to result in any of the Resources' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the Resources' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *Resources) Subtract(spec *types.DecimalSpec) error {
	res.Lock()
	defer res.Unlock()

	updatedCPUs := res.millicpus.Sub(spec.Millicpus)
	if updatedCPUs.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s Millicpus would be set to %s millicpus after subtraction (current=%s,subtrahend=%s)",
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

	updatedVRAM := res.vramGB.Sub(spec.VRam)
	if updatedVRAM.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: %s VRAM would be set to %s GB after subtraction (current=%s,subtrahend=%s)",
			ErrInvalidOperation, res.resourceStatus.String(), updatedVRAM.String(),
			res.vramGB.StringFixed(0), spec.VRam.StringFixed(0))
	}

	// If we've gotten to this point, then all the updated resource counts are valid, at least with respect
	// to not being negative. Persist the changes and return nil, indicating that the subtract operation was successful.
	res.gpus = updatedGPUs
	res.millicpus = updatedCPUs
	res.memoryMB = updatedMemory
	res.vramGB = updatedVRAM

	return nil

}

// Validate returns true if each of the Resources' cpu, gpu, and memory are greater than or equal to the respective
// resource of the given types.DecimalSpec.
func (res *Resources) Validate(spec types.Spec) bool {
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
		res.memoryMB.GreaterThanOrEqual(decimalSpec.MemoryMb) &&
		res.vramGB.GreaterThanOrEqual(decimalSpec.VRam)
}

// ValidateWithError returns nil if each of the Resources' cpu, gpu, and memory are greater than or equal to the
// respective resource of the given types.DecimalSpec. That is, if the given types.DecimalSpec is validated, so to
// speak, then ValidateWithError will return nil.
//
// If the specified types.DecimalSpec is NOT validated, then an error is returned.
// This error indicates which of the Resources' cpu, gpu, and/or memory were insufficient to validate the given spec.
func (res *Resources) ValidateWithError(spec types.Spec) error {
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
	sufficientVRamAvailable := res.vramGB.GreaterThanOrEqual(decimalSpec.VRam)

	offendingKinds := make([]ResourceKind, 0)
	if !sufficientGPUsAvailable {
		offendingKinds = append(offendingKinds, GPU)
	}

	if !sufficientCPUsAvailable {
		offendingKinds = append(offendingKinds, CPU)
	}

	if !sufficientMemoryAvailable {
		offendingKinds = append(offendingKinds, Memory)
	}

	if !sufficientVRamAvailable {
		offendingKinds = append(offendingKinds, VRAM)
	}

	if len(offendingKinds) > 0 {
		return NewInsufficientResourcesError(res.unsafeToDecimalSpec(), spec, offendingKinds)
	} else {
		return nil
	}
}

// ResourcesWrapper is a wrapper around several Resources structs, each of which corresponds to idle, pending,
// committed, or spec Resources.
type ResourcesWrapper struct {
	mu sync.Mutex

	// lastAppliedSnapshotId is the ID of the last snapshot that was applied to this ResourcesWrapper.
	lastAppliedSnapshotId int32

	idleResources      *Resources
	pendingResources   *Resources
	committedResources *Resources
	specResources      *Resources
}

// NewResourcesWrapper creates a new ResourcesWrapper struct from the given types.Spec and returns
// a pointer to it (the new ResourcesWrapper struct).
//
// The given types.Spec is used to initialize the spec and idle resource quantities of the new ResourcesWrapper struct.
func NewResourcesWrapper(spec types.Spec) *ResourcesWrapper {
	resourceSpec := types.ToDecimalSpec(spec)

	return &ResourcesWrapper{
		// Snapshot IDs begin at 0, so -1 will always be less than the first snapshot to be applied.
		lastAppliedSnapshotId: -1,
		idleResources: &Resources{
			resourceStatus: IdleResources,
			millicpus:      resourceSpec.Millicpus.Copy(),
			memoryMB:       resourceSpec.MemoryMb.Copy(),
			gpus:           resourceSpec.GPUs.Copy(),
			vramGB:         resourceSpec.VRam.Copy(),
		},
		pendingResources: &Resources{
			resourceStatus: PendingResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
			vramGB:         decimal.Zero.Copy(),
		},
		committedResources: &Resources{
			resourceStatus: CommittedResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
			vramGB:         decimal.Zero.Copy(),
		},
		specResources: &Resources{
			resourceStatus: SpecResources,
			millicpus:      resourceSpec.Millicpus.Copy(),
			memoryMB:       resourceSpec.MemoryMb.Copy(),
			gpus:           resourceSpec.GPUs.Copy(),
			vramGB:         resourceSpec.VRam.Copy(),
		},
	}
}

// ApplySnapshotToResourceWrapper atomically overwrites the target resourceWrapper's resource quantities with
// the resource quantities encoded by the given HostResourceSnapshot instance.
//
// ApplySnapshotToResourceWrapper returns nil on success.
//
// If the given HostResourceSnapshot's SnapshotId is less than the resourceWrapper's lastAppliedSnapshotId,
// then an error will be returned.
func ApplySnapshotToResourceWrapper[T types.ArbitraryResourceSnapshot](r *ResourcesWrapper, snapshot types.HostResourceSnapshot[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure that the snapshot being applied is not old. If it is old, then we'll reject it.
	if r.lastAppliedSnapshotId > snapshot.GetSnapshotId() {
		return fmt.Errorf("%w: %w (last applied ID=%d, given ID=%d)",
			ErrInvalidSnapshot, ErrOldSnapshot, r.lastAppliedSnapshotId, snapshot.GetSnapshotId())
	}

	var err error
	if err = ApplySnapshotToResources(r.idleResources, snapshot.GetIdleResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.pendingResources, snapshot.GetPendingResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.committedResources, snapshot.GetCommittedResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.specResources, snapshot.GetSpecResources()); err != nil {
		return err
	}

	return nil
}

// String returns a string representation of the ResourcesWrapper that is suitable for logging.
func (r *ResourcesWrapper) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Sprintf("ResourcesWrapper{%s, %s, %s, %s}",
		r.idleResources.String(), r.pendingResources.String(), r.committedResources.String(), r.specResources.String())
}

// IdleResources returns a ResourceState that is responsible for encoding the current idle Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) IdleResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources
}

// PendingResources returns a ResourceState that is responsible for encoding the current pending Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) PendingResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources
}

// CommittedResources returns a ResourceState that is responsible for encoding the current committed Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) CommittedResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources
}

// SpecResources returns a ResourceState that is responsible for encoding the current spec Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) SpecResources() ResourceState {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources
}

// idleResourcesSnapshot returns a *ResourceSnapshot struct capturing the current idle Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) idleResourcesSnapshot(snapshotId int32) *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources.ResourceSnapshot(snapshotId)
}

// pendingResourcesSnapshot returns a *ResourceSnapshot struct capturing the current pending Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) pendingResourcesSnapshot(snapshotId int32) *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources.ResourceSnapshot(snapshotId)
}

// committedResourcesSnapshot returns a *ResourceSnapshot struct capturing the current committed Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) committedResourcesSnapshot(snapshotId int32) *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources.ResourceSnapshot(snapshotId)
}

// specResourcesSnapshot returns a *ResourceSnapshot struct capturing the current spec Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) specResourcesSnapshot(snapshotId int32) *ResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources.ResourceSnapshot(snapshotId)
}

// IdleProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current idle Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) IdleProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources.protoResourceSnapshot(snapshotId)
}

// PendingProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current pending Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) PendingProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources.protoResourceSnapshot(snapshotId)
}

// CommittedProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current committed Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) CommittedProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources.protoResourceSnapshot(snapshotId)
}

// SpecProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current spec Resources
// of the target ResourcesWrapper.
func (r *ResourcesWrapper) SpecProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources.protoResourceSnapshot(snapshotId)
}

// ResourceSnapshot returns a pointer to a ResourceSnapshot created for the specified "status" of Resources
// (i.e., "idle", "pending", "committed", or "spec").
func (r *ResourcesWrapper) ResourceSnapshot(status ResourceStatus, snapshotId int32) *ResourceSnapshot {
	switch status {
	case IdleResources:
		{
			return r.idleResourcesSnapshot(snapshotId)
		}
	case PendingResources:
		{
			return r.pendingResourcesSnapshot(snapshotId)
		}
	case CommittedResources:
		{
			return r.committedResourcesSnapshot(snapshotId)
		}
	case SpecResources:
		{
			return r.specResourcesSnapshot(snapshotId)
		}
	default:
		{
			log.Fatalf("Unknown or unexpected ResourceStatus specified: \"%s\"", status)
			return nil
		}
	}
}

// ResourceWrapperSnapshot encapsulates a JSON-compatible snapshot of the resource quantities of the ResourceManager.
type ResourceWrapperSnapshot struct {
	// SnapshotId uniquely identifies the ResourceWrapperSnapshot and defines a total order amongst all ResourceWrapperSnapshot
	// structs originating from the same node. Each newly-created ResourceWrapperSnapshot is assigned an ID from a
	// monotonically-increasing counter by the ResourceManager.
	SnapshotId int32 `json:"snapshot_id"`

	// NodeId is the ID of the node from which the snapshot originates.
	NodeId string `json:"host_id"`

	// ManagerId is the unique ID of the ResourceManager struct from which the ResourceWrapperSnapshot was constructed.
	ManagerId string `json:"manager_id"`

	// Timestamp is the time at which the ResourceWrapperSnapshot was taken/created.
	Timestamp time.Time `json:"timestamp"`

	IdleResources      *ResourceSnapshot `json:"idle_resources"`
	PendingResources   *ResourceSnapshot `json:"pending_resources"`
	CommittedResources *ResourceSnapshot `json:"committed_resources"`
	SpecResources      *ResourceSnapshot `json:"spec_resources"`

	// Containers are the Containers presently running on the Host.
	Containers []types.ContainerInfo
}

// MetadataResourceWrapperSnapshot is a simpel wrapper around a ResourceWrapperSnapshot struct so that
// we can deserialize a ResourceWrapperSnapshot struct from the metadata frame of a JupyterMessage, which
// is typically a map[string]interface{}.
type MetadataResourceWrapperSnapshot struct {
	ResourceWrapperSnapshot *ResourceWrapperSnapshot `json:"resource_snapshot"`
}

func (s *ResourceWrapperSnapshot) GetContainers() []types.ContainerInfo {
	return s.Containers
}

////////////////////////////////////////////////////
// HostResourceSnapshot interface implementation. //
////////////////////////////////////////////////////

// Compare compares the object with specified object.
// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
func (s *ResourceWrapperSnapshot) Compare(obj interface{}) float64 {
	if obj == nil {
		log.Fatalf("Cannot compare target ResourceWrapperSnapshot with nil.")
	}

	other, ok := obj.(types.ArbitraryResourceSnapshot)
	if !ok {
		log.Fatalf("Cannot compare target ResourceWrapperSnapshot with specified object of type '%s'.",
			reflect.ValueOf(obj).Type().String())
	}

	if s.GetSnapshotId() < other.GetSnapshotId() {
		return -1
	} else if s.GetSnapshotId() == other.GetSnapshotId() {
		return 0
	} else {
		return 1
	}
}

// String returns a string representation of the ResourceWrapperSnapshot suitable for logging/printing.
func (s *ResourceWrapperSnapshot) String() string {
	idle := "nil"
	if s.IdleResources != nil {
		idle = s.IdleResources.String()
	}

	pending := "nil"
	if s.PendingResources != nil {
		pending = s.PendingResources.String()
	}

	committed := "nil"
	if s.CommittedResources != nil {
		committed = s.CommittedResources.String()
	}

	spec := "nil"
	if s.SpecResources != nil {
		spec = s.SpecResources.String()
	}

	return fmt.Sprintf("ResourceWrapperSnapshot[SnapshotID=%d, NodeID=%s, ManagerID=%s, Timestamp=%v, Idle=%s, Pending=%s, Committed=%s, Spec=%s]",
		s.SnapshotId, s.NodeId, s.ManagerId, s.Timestamp, idle, pending, committed, spec)
}

// GetSnapshotId is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetSnapshotId() int32 {
	return s.SnapshotId
}

// GetNodeId is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetNodeId() string {
	return s.NodeId
}

// GetManagerId is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetManagerId() string {
	return s.ManagerId
}

// GetGoTimestamp is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetGoTimestamp() time.Time {
	return s.Timestamp
}

// GetIdleResources is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetIdleResources() types.ArbitraryResourceSnapshot {
	return s.IdleResources
}

// GetPendingResources is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetPendingResources() types.ArbitraryResourceSnapshot {
	return s.PendingResources
}

// GetCommittedResources is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetCommittedResources() types.ArbitraryResourceSnapshot {
	return s.CommittedResources
}

// GetSpecResources is part of the HostResourceSnapshot interface implementation.
func (s *ResourceWrapperSnapshot) GetSpecResources() types.ArbitraryResourceSnapshot {
	return s.SpecResources
}

// ProtoNodeResourcesSnapshotWrapper is a wrapper around the proto.NodeResourcesSnapshot struct that
// provides a way for a proto.NodeResourcesSnapshot struct to satisfy the types.HostResourceSnapshot interface.
type ProtoNodeResourcesSnapshotWrapper struct {
	*proto.NodeResourcesSnapshotWithContainers
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetIdleResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.IdleResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetPendingResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.PendingResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetCommittedResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.CommittedResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetSpecResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.SpecResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetContainers() []types.ContainerInfo {
	containerInfo := make([]types.ContainerInfo, 0, len(w.Containers))
	for _, container := range w.Containers {
		containerInfo = append(containerInfo, container)
	}
	return containerInfo
}

func (w *ProtoNodeResourcesSnapshotWrapper) String() string {
	return w.NodeResourcesSnapshotWithContainers.String()
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetSnapshotId() int32 {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.SnapshotId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetNodeId() string {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.NodeId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetManagerId() string {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.ManagerId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetGoTimestamp() time.Time {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.GetGoTimestamp()
}

func (w *ProtoNodeResourcesSnapshotWrapper) Compare(obj interface{}) float64 {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.Compare(obj)
}
