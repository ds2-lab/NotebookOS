package resource

import (
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"sync"
)

// Status differentiates between idle, pending, committed, and spec ComputeResource.
type Status string

func (t Status) String() string {
	return string(t)
}

// ComputeResource is a struct used by the AllocationManager to track its total idle, pending, committed, and spec ComputeResource
// of each type (CPU, GPU, and Memory).
type ComputeResource struct {
	sync.Mutex // Enables atomic access to each individual field.

	// lastAppliedSnapshotId is the ID of the last snapshot that was applied to this ComputeResource struct.
	lastAppliedSnapshotId int32

	resourceStatus Status          // resourceStatus is the ResourceStatus represented/encoded by this struct.
	millicpus      decimal.Decimal // millicpus is CPU in 1/1000th of CPU core.
	gpus           decimal.Decimal // gpus is the number of GPUs.
	memoryMB       decimal.Decimal // memoryMB is the amount of memory in MB.
	vramGB         decimal.Decimal // vram is the amount of GPU memory in GB.
}

// ApplySnapshotToResources atomically overwrites its resource quantities with the quantities encoded
// in the given ArbitraryResourceSnapshot instance.
//
// ApplySnapshotToResources returns nil on success. The only failure possible is that the ArbitraryResourceSnapshot
// encodes ComputeResource of a different "status" than the target ComputeResource struct. For example, if the target
// ComputeResource struct encodes "idle" ComputeResource, whereas the given ArbitraryResourceSnapshot instance encodes
// "pending" ComputeResource, then an error will be returned, and none of the resource quantities in the target
// ComputeResource struct will be overwritten.
func ApplySnapshotToResources[T types.ArbitraryResourceSnapshot](res *ComputeResource, snapshot T) error {
	res.Lock()
	defer res.Unlock()

	// Ensure that the snapshot corresponds to ComputeResource of the same status as the target ComputeResource struct.
	// If it doesn't, then we'll reject the snapshot.
	if res.resourceStatus.String() != snapshot.GetResourceStatus() {
		return fmt.Errorf("%w: %w", ErrInvalidSnapshot, ErrIncompatibleResourceStatus)
	}

	// Ensure that the snapshot being applied is not old. If it is old, then we'll reject it.
	if res.lastAppliedSnapshotId > snapshot.GetSnapshotId() {
		return fmt.Errorf("%w: %w (last applied ID=%d, given ID=%d)",
			ErrInvalidSnapshot, scheduling.ErrOldSnapshot, res.lastAppliedSnapshotId, snapshot.GetSnapshotId())
	}

	res.millicpus = decimal.NewFromFloat(float64(snapshot.GetMillicpus()))
	res.memoryMB = decimal.NewFromFloat(float64(snapshot.GetMemoryMb()))
	res.gpus = decimal.NewFromFloat(float64(snapshot.GetGpus()))
	res.vramGB = decimal.NewFromFloat(float64(snapshot.GetVramGb()))
	res.lastAppliedSnapshotId = snapshot.GetSnapshotId()

	return nil
}

// ResourceSnapshot constructs and returns a pointer to a new ComputeResourceSnapshot struct.
//
// This method is thread-safe to ensure that the quantities of each resource are all captured atomically.
func (res *ComputeResource) ResourceSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	res.Lock()
	defer res.Unlock()

	snapshot := &ComputeResourceSnapshot{
		ResourceStatus: res.resourceStatus,
		Millicpus:      res.millicpus,
		Gpus:           res.gpus,
		MemoryMB:       res.memoryMB,
		VRamGB:         res.vramGB,
		SnapshotId:     snapshotId,
	}

	return snapshot
}

// ProtoSnapshot constructs and returns a pointer to a new ProtoSnapshot struct.
//
// This method is thread-safe to ensure that the quantities of each resource are all captured atomically.
func (res *ComputeResource) ProtoSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
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
// the current quantities of ComputeResource encoded/maintained by the target ComputeResource struct.
//
// This method is thread-safe to ensure that the quantity of each individual resource type cannot
// be modified during the time that the new types.DecimalSpec struct is being constructed.
func (res *ComputeResource) ToDecimalSpec() *types.DecimalSpec {
	res.Lock()
	defer res.Unlock()

	return res.unsafeToDecimalSpec()
}

// unsafeToDecimalSpec returns a pointer to a types.DecimalSpec struct that encapsulates a snapshot of
// the current quantities of ComputeResource encoded/maintained by the target ComputeResource struct.
//
// This method is not thread-safe and should be called only by the ToDecimalSpec method, unless
// the ComputeResource' lock is already held.
func (res *ComputeResource) unsafeToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		GPUs:      res.gpus.Copy(),
		Millicpus: res.millicpus.Copy(),
		MemoryMb:  res.memoryMB.Copy(),
		VRam:      res.vramGB.Copy(),
	}
}

// LessThan returns true if each field of the target 'ComputeResource' struct is strictly less than the corresponding field
// of the other 'ComputeResource' struct.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not less than the corresponding field of the other 'ComputeResource'
// struct, then false is returned.
//
// The Kind are checked in the following order: CPU, Memory, GPU.
// The Kind of the first offending quantity will be returned, along with false, based on that order.
func (res *ComputeResource) LessThan(other *ComputeResource) (bool, Kind) {
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

// LessThanOrEqual returns true if each field of the target 'ComputeResource' struct is less than or equal to the
// corresponding field of the other 'ComputeResource' struct.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not less than or equal to the corresponding field of the
// other 'ComputeResource' struct, then false is returned.
func (res *ComputeResource) LessThanOrEqual(other *ComputeResource) (bool, Kind) {
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

// GreaterThan returns true if each field of the target 'ComputeResource' struct is strictly greater than to the
// corresponding field of the other 'ComputeResource' struct.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not strictly greater than the corresponding field of the
// other 'ComputeResource' struct, then false is returned.
func (res *ComputeResource) GreaterThan(other *ComputeResource) (bool, Kind) {
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

// GreaterThanOrEqual returns true if each field of the target 'ComputeResource' struct is greater than or equal to the
// corresponding field of the other 'ComputeResource' struct.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not greater than or equal to the corresponding field of the
// other 'ComputeResource' struct, then false is returned.
func (res *ComputeResource) GreaterThanOrEqual(other *ComputeResource) (bool, Kind) {
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

// EqualTo returns true if each field of the target 'ComputeResource' struct is exactly equal to the corresponding field of
// the other 'ComputeResource' struct.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not equal to the corresponding field of the other 'ComputeResource'
// struct, then false is returned.
func (res *ComputeResource) EqualTo(other *ComputeResource) (bool, Kind) {
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

// IsZero returns true if each field of the target 'ComputeResource' struct is exactly equal to 0.
//
// This method locks both 'ComputeResource' instances, beginning with the target instance.
//
// If any field of the target 'ComputeResource' struct is not equal to 0, then false is returned.
func (res *ComputeResource) IsZero() (bool, Kind) {
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

// GetResource returns a copy of the decimal.Decimal corresponding with the specified Kind.
//
// This method is thread-safe.
//
// If kind is equal to NoResource, then this method will panic.
func (res *ComputeResource) GetResource(kind Kind) decimal.Decimal {
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

	panic(fmt.Sprintf("Invalid Kind specified: \"%s\"", kind))
}

// HasNegativeField returns true if millicpus, gpus, or memoryMB is negative.
// It also returns the Kind of the negative field.
//
// This method is thread-safe.
//
// The ComputeResource are checked in the following order: CPU, Memory, GPU.
// This method will return true and the associated Kind for the first negative Kind encountered.
//
// If no ComputeResource are negative, then this method returns false and NoResource.
func (res *ComputeResource) HasNegativeField() (bool, Kind) {
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

func (res *ComputeResource) String() string {
	res.Lock()
	defer res.Unlock()

	return fmt.Sprintf("[%s ComputeResource: millicpus=%s,gpus=%s,vram=%sGB,memory=%sMB]",
		res.resourceStatus.String(), res.millicpus.StringFixed(0),
		res.gpus.StringFixed(0), res.vramGB.StringFixed(4), res.memoryMB.StringFixed(4))
}

func (res *ComputeResource) ResourceStatus() Status {
	return res.resourceStatus
}

func (res *ComputeResource) MemoryMB() float64 {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.InexactFloat64()
}

func (res *ComputeResource) MemoryMbAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.memoryMB.Copy()
}

// SetMemoryMB sets the amount of memory to a copy of the specified decimal.Decimal value.
func (res *ComputeResource) SetMemoryMB(memoryMB decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.memoryMB = memoryMB
}

// VRAM returns the amount of VRAM (in GB).
func (res *ComputeResource) VRAM() float64 {
	res.Lock()
	defer res.Unlock()

	return res.vramGB.InexactFloat64()
}

// VRAMAsDecimal returns a copy of the decimal.Decimal that precisely & accurately encodes the amount of VRAM.
// The units are gigabytes (GB).
func (res *ComputeResource) VRAMAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.vramGB.Copy()
}

func (res *ComputeResource) GPUs() float64 {
	res.Lock()
	defer res.Unlock()

	return res.gpus.InexactFloat64()
}

func (res *ComputeResource) GPUsAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.gpus.Copy()
}

// SetGpus sets the number of GPUs to a copy of the specified decimal.Decimal value.
func (res *ComputeResource) SetGpus(gpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.gpus = gpus.Copy()
}

func (res *ComputeResource) Millicpus() float64 {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.InexactFloat64()
}

func (res *ComputeResource) MillicpusAsDecimal() decimal.Decimal {
	res.Lock()
	defer res.Unlock()

	return res.millicpus.Copy()
}

// SetMillicpus sets the number of Millicpus to a copy of the specified decimal.Decimal value.
func (res *ComputeResource) SetMillicpus(millicpus decimal.Decimal) {
	res.Lock()
	defer res.Unlock()

	res.millicpus = millicpus
}

// Add adds the ComputeResource encapsulated in the given types.DecimalSpec to the ComputeResource' internal resource counts.
//
// If performing this operation were to result in any of the ComputeResource' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the ComputeResource' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *ComputeResource) Add(spec *types.DecimalSpec) error {
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

// Subtract subtracts the ComputeResource encapsulated in the given types.DecimalSpec from the ComputeResource' own internal counts.
//
// If performing this operation were to result in any of the ComputeResource' internal counts becoming negative, then
// an error is returned and no changes are made whatsoever.
//
// This operation is performed atomically. It should not be called from a context in which the ComputeResource' mutex is
// already held/acquired, as this will lead to a deadlock.
func (res *ComputeResource) Subtract(spec *types.DecimalSpec) error {
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

// Validate returns true if each of the ComputeResource' cpu, gpu, and memory are greater than or equal to the respective
// resource of the given types.DecimalSpec.
func (res *ComputeResource) Validate(spec types.Spec) bool {
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

// ValidateWithError returns nil if each of the ComputeResource' cpu, gpu, and memory are greater than or equal to the
// respective resource of the given types.DecimalSpec. That is, if the given types.DecimalSpec is validated, so to
// speak, then ValidateWithError will return nil.
//
// If the specified types.DecimalSpec is NOT validated, then an error is returned.
// This error indicates which of the ComputeResource' cpu, gpu, and/or memory were insufficient to validate the given spec.
func (res *ComputeResource) ValidateWithError(spec types.Spec) error {
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

	offendingKinds := make([]Kind, 0)
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
