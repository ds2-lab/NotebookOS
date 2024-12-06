package resource

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"time"
)

var (
	ErrNilTransaction                = errors.New("could not run transaction because transaction was nil")
	ErrImmutableResourceModification = errors.New("transaction failed because an attempt to modify an immutable resource was made")
)

type TransactionState interface {
	IdleResources() TransactionResources
	PendingResources() TransactionResources
	CommittedResources() TransactionResources
	SpecResources() TransactionResources

	// Validate checks that the transaction state is in a valid state. Validate error returns nil if so.
	Validate() error
}

type transactionState struct {
	idleResources      *transactionResources
	pendingResources   *transactionResources
	committedResources *transactionResources
	specResources      *transactionResources
}

func NewTransactionState() TransactionState {
	return &transactionState{}
}

// getQuantityOfResourceKind returns the (working) field corresponding to the specified Kind of the specified
// *transactionResources struct.
func getQuantityOfResourceKind(res *transactionResources, kind Kind) decimal.Decimal {
	switch kind {
	case CPU:
		{
			return res.working.Millicpus
		}
	case Memory:
		{
			return res.working.MemoryMb
		}
	case GPU:
		{
			return res.working.GPUs
		}
	case VRAM:
		{
			return res.working.VRam
		}
	default:
		{
			panic(fmt.Sprintf("invalid/unsupported resource kind: \"%s\"", kind.String()))
		}
	}
}

func (t *transactionState) IdleResources() TransactionResources {
	return t.idleResources
}

func (t *transactionState) PendingResources() TransactionResources {
	return t.pendingResources
}

func (t *transactionState) CommittedResources() TransactionResources {
	return t.committedResources
}

func (t *transactionState) SpecResources() TransactionResources {
	return t.specResources
}

// Validate checks that the transaction state is in a valid state. Validate error returns nil if so.
func (t *transactionState) Validate() error {
	if hasNegativeField, kind := t.idleResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrInvalidOperation, kind.String(), getQuantityOfResourceKind(t.idleResources, kind))
	}

	if hasNegativeField, kind := t.pendingResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrInvalidOperation, kind.String(), getQuantityOfResourceKind(t.pendingResources, kind))
	}

	if hasNegativeField, kind := t.committedResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrInvalidOperation, kind.String(), getQuantityOfResourceKind(t.committedResources, kind))
	}

	if hasNegativeField, kind := t.specResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrInvalidOperation, kind.String(), getQuantityOfResourceKind(t.specResources, kind))
	}

	if isLessThanOrEqual, offendingKind := t.committedResources.lessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: committed %s (%s) would exceed spec %s (%s)",
			ErrInvalidOperation, offendingKind.String(), getQuantityOfResourceKind(t.committedResources, offendingKind).StringFixed(4),
			offendingKind.String(), getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	if isLessThanOrEqual, offendingKind := t.idleResources.lessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: idle %s (%s) would exceed spec %s (%s)",
			ErrInvalidOperation, offendingKind.String(), getQuantityOfResourceKind(t.idleResources, offendingKind).StringFixed(4),
			offendingKind.String(), getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	return nil
}

type TransactionResources interface {
	// Add adds the given types.Spec to the working/current resources quantities.
	Add(types.Spec)

	// Subtract subtracts the given types.Spec from the working/current resources quantities.
	Subtract(types.Spec)

	// Equals returns a flag indicating whether the current transactional state is equal to the given types.Spec.
	Equals(types.Spec) bool

	// Initial returns (a copy of) the initial resources before any transaction operations are/were performed.
	Initial() types.Spec

	// Working returns the current/working resource quantities.
	Working() types.Spec

	// IsMutable returns a flag indicating whether the target TransactionResources is mutable.
	IsMutable() bool

	// Sanitize attempts to round all of its working resource values to either the min or the max, if any corresponding
	// quantities are within epsilon of the quantity in min or max.
	Sanitize(min types.Spec, max types.Spec)
}

type transactionResources struct {
	isMutable bool

	// The initial state of the transaction.
	initial *types.DecimalSpec
	// The current "working" state of the transaction.
	working *types.DecimalSpec
}

// Sanitize attempts to round all of its working resource values to either the min or the max, if any corresponding
// quantities are within epsilon of the quantity in min or max.
func (t *transactionResources) Sanitize(min types.Spec, max types.Spec) {
	if min != nil {
		minDecimal := types.ToDecimalSpec(min)
		t.working.Millicpus = TryRoundToDecimal(t.working.Millicpus, minDecimal.Millicpus)
		t.working.MemoryMb = TryRoundToDecimal(t.working.MemoryMb, minDecimal.MemoryMb)
		t.working.GPUs = TryRoundToDecimal(t.working.GPUs, minDecimal.GPUs)
		t.working.VRam = TryRoundToDecimal(t.working.VRam, minDecimal.VRam)
	}

	if max != nil {
		maxDecimal := types.ToDecimalSpec(max)
		t.working.Millicpus = TryRoundToDecimal(t.working.Millicpus, maxDecimal.Millicpus)
		t.working.MemoryMb = TryRoundToDecimal(t.working.MemoryMb, maxDecimal.MemoryMb)
		t.working.GPUs = TryRoundToDecimal(t.working.GPUs, maxDecimal.GPUs)
		t.working.VRam = TryRoundToDecimal(t.working.VRam, maxDecimal.VRam)
	}
}

// Initial returns (a copy of) the initial resources before any transaction operations are/were performed.
func (t *transactionResources) Initial() types.Spec {
	return t.initial.Clone()
}

// IsMutable returns a flag indicating whether the target *transactionResources is mutable.
func (t *transactionResources) IsMutable() bool {
	return t.isMutable
}

// hasNegativeField returns true if there is at least one field less than 0 in the transactionResources's
// working *types.DecimalSpec.
//
// If there is at least one negative resource Kind, then that Kind is also returned.
func (t *transactionResources) hasNegativeField() (bool, Kind) {
	if t.working.Millicpus.LessThan(decimal.Zero) {
		return true, CPU
	}

	if t.working.MemoryMb.LessThan(decimal.Zero) {
		return true, Memory
	}

	if t.working.GPUs.LessThan(decimal.Zero) {
		return true, GPU
	}

	if t.working.VRam.LessThan(decimal.Zero) {
		return true, VRAM
	}

	return false, NoResource
}

// Working returns the current/working resource quantities.
func (t *transactionResources) Working() types.Spec {
	return t.working.Clone()
}

// Add adds the given types.Spec to the working/current resources quantities.
func (t *transactionResources) Add(spec types.Spec) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	t.working = t.working.AddDecimal(spec)
}

// Subtract subtracts the given types.Spec from the working/current resources quantities.
func (t *transactionResources) Subtract(spec types.Spec) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	t.working = t.working.Subtract(spec)
}

// Equals returns a flag indicating whether the current transactional state is equal to the given types.Spec.
func (t *transactionResources) Equals(spec types.Spec) bool {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	return t.working.Equals(spec)
}

// greaterThan returns a flag indicating whether the current transactional state is greater than the given types.Spec.
func (t *transactionResources) greaterThan(spec types.Spec) (bool, Kind) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	decimalSpec := types.ToDecimalSpec(spec)

	if !t.working.Millicpus.GreaterThan(decimalSpec.Millicpus) {
		return false, CPU
	}

	if !t.working.MemoryMb.GreaterThan(decimalSpec.MemoryMb) {
		return false, Memory
	}

	if !t.working.GPUs.GreaterThan(decimalSpec.GPUs) {
		return false, GPU
	}

	if !t.working.VRam.GreaterThan(decimalSpec.VRam) {
		return false, VRAM
	}

	return true, NoResource
}

// lessThanOrEqual returns a flag indicating whether the current transactional state is <= than the given types.Spec.
func (t *transactionResources) lessThanOrEqual(spec types.Spec) (bool, Kind) {
	if !t.isMutable {
		panic(ErrImmutableResourceModification)
	}

	if t.working == nil {
		t.working = t.initial.CloneDecimalSpec()
	}

	decimalSpec := types.ToDecimalSpec(spec)

	if !t.working.Millicpus.LessThanOrEqual(decimalSpec.Millicpus) {
		return false, CPU
	}

	if !t.working.MemoryMb.LessThanOrEqual(decimalSpec.MemoryMb) {
		return false, Memory
	}

	if !t.working.GPUs.LessThanOrEqual(decimalSpec.GPUs) {
		return false, GPU
	}

	if !t.working.VRam.LessThanOrEqual(decimalSpec.VRam) {
		return false, VRAM
	}

	return true, NoResource
}

type Transaction func(m TransactionState)

type TransactionRunner struct {
	transaction Transaction
	state       TransactionState
	resultChan  chan interface{}

	log logger.Logger
}

func NewTransactionRunner(transaction Transaction, state TransactionState, resultChan chan interface{}, name string) *TransactionRunner {
	runner := &TransactionRunner{
		transaction: transaction,
		state:       state,
		resultChan:  resultChan,
	}

	if name != "" {
		config.InitLogger(&runner.log, name)
	} else {
		config.InitLogger(&runner.log, runner)
	}

	return runner
}

func (r *TransactionRunner) State() TransactionState {
	return r.state
}

func (r *TransactionRunner) runTransaction() {
	st := time.Now()

	defer func() {
		if err := recover(); err != nil {
			r.log.Warn("Transaction failed after %v: %v", time.Since(st), err)
			r.resultChan <- err
		} else {
			r.log.Debug("Transaction succeeded. Time elapsed: %v.", time.Since(st))
			r.resultChan <- struct{}{}
		}
	}()

	if r.transaction == nil {
		return
	}

	r.transaction(r.state)

	zeroSpec := types.NewDecimalSpec(0, 0, 0, 0)

	// Clamp committed resources between 0 and the Host's spec resources.
	r.state.IdleResources().Sanitize(zeroSpec, r.state.SpecResources().Initial())

	// There isn't really an upper limit here (not one that we can compute right now).
	r.state.PendingResources().Sanitize(zeroSpec, nil)

	// Clamp committed resources between 0 and the Host's spec resources.
	r.state.CommittedResources().Sanitize(zeroSpec, r.state.SpecResources().Initial())

	if err := r.state.Validate(); err != nil {
		panic(err)
	}
}
