package transaction

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/shopspring/decimal"
)

type Resources struct {
	initial   *types.DecimalSpec
	working   *types.DecimalSpec
	isMutable bool
}

func NewResources(initial *types.DecimalSpec, isMutable bool) *Resources {
	return &Resources{
		initial:   initial,
		working:   initial.CloneDecimalSpec(),
		isMutable: isMutable,
	}
}

func (t *Resources) Clone() scheduling.TransactionResources {
	clone := *t
	return &clone
}

// Sanitize attempts to round all of its working resource values to either the min or the max, if any corresponding
// quantities are within epsilon of the quantity in min or max.
func (t *Resources) Sanitize(min types.Spec, max types.Spec) {
	if min != nil {
		minDecimal := types.ToDecimalSpec(min)
		t.working.Millicpus = utils.TryRoundToDecimal(t.working.Millicpus, minDecimal.Millicpus)
		t.working.MemoryMb = utils.TryRoundToDecimal(t.working.MemoryMb, minDecimal.MemoryMb)
		t.working.GPUs = utils.TryRoundToDecimal(t.working.GPUs, minDecimal.GPUs)
		t.working.VRam = utils.TryRoundToDecimal(t.working.VRam, minDecimal.VRam)
	}

	if max != nil {
		maxDecimal := types.ToDecimalSpec(max)
		t.working.Millicpus = utils.TryRoundToDecimal(t.working.Millicpus, maxDecimal.Millicpus)
		t.working.MemoryMb = utils.TryRoundToDecimal(t.working.MemoryMb, maxDecimal.MemoryMb)
		t.working.GPUs = utils.TryRoundToDecimal(t.working.GPUs, maxDecimal.GPUs)
		t.working.VRam = utils.TryRoundToDecimal(t.working.VRam, maxDecimal.VRam)
	}
}

// Initial returns (a copy of) the initial resources before any operation operations are/were performed.
func (t *Resources) Initial() types.Spec {
	return t.initial.Clone()
}

// IsMutable returns a flag indicating whether the target *Resources is mutable.
func (t *Resources) IsMutable() bool {
	return t.isMutable
}

// hasNegativeWorkingField returns true if there is at least one field less than 0 in the Resources's
// working *types.DecimalSpec.
//
// If there is at least one negative resource Kind, then that Kind is also returned.
func (t *Resources) hasNegativeWorkingField() (bool, []scheduling.ResourceKind) {
	var (
		hasNegativeField bool
		offendingKinds   []scheduling.ResourceKind
	)

	if t.working.Millicpus.LessThan(decimal.Zero) {
		hasNegativeField = true
		offendingKinds = []scheduling.ResourceKind{scheduling.CPU}
	}

	if t.working.MemoryMb.LessThan(decimal.Zero) {
		hasNegativeField = true

		if offendingKinds == nil {
			offendingKinds = make([]scheduling.ResourceKind, 0)
		}

		offendingKinds = append(offendingKinds, scheduling.Memory)
	}

	if t.working.GPUs.LessThan(decimal.Zero) {
		hasNegativeField = true

		if offendingKinds == nil {
			offendingKinds = make([]scheduling.ResourceKind, 0)
		}

		offendingKinds = append(offendingKinds, scheduling.GPU)
	}

	if t.working.VRam.LessThan(decimal.Zero) {
		hasNegativeField = true

		if offendingKinds == nil {
			offendingKinds = make([]scheduling.ResourceKind, 0)
		}

		offendingKinds = append(offendingKinds, scheduling.VRAM)
	}

	return hasNegativeField, offendingKinds
}

// Working returns the current/working resource quantities.
func (t *Resources) Working() types.Spec {
	return t.working.CloneDecimalSpec()
}

// Add adds the given types.Spec to the working/current resources quantities.
func (t *Resources) Add(spec types.Spec) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	t.working = t.working.AddDecimal(spec)
}

// Subtract subtracts the given types.Spec from the working/current resources quantities.
func (t *Resources) Subtract(spec types.Spec) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	t.working = t.working.Subtract(spec)
}

// Equals returns a flag indicating whether the current transactional state is equal to the given interface{}.
//
// The given interface must be a types.Spec or another Resource struct, or this will return false.
func (t *Resources) Equals(other interface{}) bool {
	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	var (
		spec       types.Spec
		res        Resources
		resPointer *Resources
		ok         bool
	)

	if spec, ok = other.(types.Spec); ok {
		if t.working == nil {
			t.working = t.initial.CloneDecimalSpec()
		}

		return t.working.Equals(spec)
	}

	if res, ok = other.(Resources); ok {
		resSpec := res.Working()

		return t.working.Equals(resSpec)
	}

	if resPointer, ok = other.(*Resources); ok {
		resSpec := resPointer.Working()

		return t.working.Equals(resSpec)
	}

	return false
}

// GreaterThan returns a flag indicating whether the current transactional state is greater than the given types.Spec.
func (t *Resources) GreaterThan(spec types.Spec) (bool, scheduling.ResourceKind) {
	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	decimalSpec := types.ToDecimalSpec(spec)

	if !t.working.Millicpus.GreaterThan(decimalSpec.Millicpus) {
		return false, scheduling.CPU
	}

	if !t.working.MemoryMb.GreaterThan(decimalSpec.MemoryMb) {
		return false, scheduling.Memory
	}

	if !t.working.GPUs.GreaterThan(decimalSpec.GPUs) {
		return false, scheduling.GPU
	}

	if !t.working.VRam.GreaterThan(decimalSpec.VRam) {
		return false, scheduling.VRAM
	}

	return true, scheduling.NoResource
}

// LessThanOrEqual returns a flag indicating whether the current transactional state is <= than the given types.Spec.
func (t *Resources) LessThanOrEqual(spec types.Spec) (bool, scheduling.ResourceKind) {
	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	decimalSpec := types.ToDecimalSpec(spec)

	if !t.working.Millicpus.LessThanOrEqual(decimalSpec.Millicpus) {
		return false, scheduling.CPU
	}

	if !t.working.MemoryMb.LessThanOrEqual(decimalSpec.MemoryMb) {
		return false, scheduling.Memory
	}

	if !t.working.GPUs.LessThanOrEqual(decimalSpec.GPUs) {
		return false, scheduling.GPU
	}

	if !t.working.VRam.LessThanOrEqual(decimalSpec.VRam) {
		return false, scheduling.VRAM
	}

	return true, scheduling.NoResource
}
