package transaction

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/shopspring/decimal"
)

type State struct {
	ParticipantId int32

	idleResources      *Resources
	pendingResources   *Resources
	committedResources *Resources
	specResources      *Resources
}

func NewState(idleResources *Resources, pendingResources *Resources, committedResources *Resources, specResources *Resources) *State {
	return &State{
		idleResources:      idleResources,
		pendingResources:   pendingResources,
		committedResources: committedResources,
		specResources:      specResources,
	}
}

func (t *State) Clone() scheduling.TransactionState {
	return &State{
		ParticipantId:      t.ParticipantId,
		idleResources:      t.idleResources.Clone().(*Resources),
		pendingResources:   t.pendingResources.Clone().(*Resources),
		committedResources: t.committedResources.Clone().(*Resources),
		specResources:      t.specResources.Clone().(*Resources),
	}
}

func (t *State) SetParticipantId(id int32) {
	t.ParticipantId = id
}

func (t *State) GetParticipantId() int32 {
	return t.ParticipantId
}

func (t *State) IdleResources() scheduling.TransactionResources {
	return t.idleResources
}

func (t *State) PendingResources() scheduling.TransactionResources {
	return t.pendingResources
}

func (t *State) CommittedResources() scheduling.TransactionResources {
	return t.committedResources
}

func (t *State) SpecResources() scheduling.TransactionResources {
	return t.specResources
}

// Validate checks that the operation state is in a valid state. Validate error returns nil if so.
func (t *State) Validate() (scheduling.ResourceKind, error) {
	if hasNegativeField, kind := t.idleResources.hasNegativeWorkingField(); hasNegativeField {
		reason := fmt.Errorf("%w (%s %s = %s)", ErrNegativeResourceCount, scheduling.IdleResources.String(),
			kind.String(), getQuantityOfResourceKind(t.idleResources, kind))
		return kind, NewErrTransactionFailed(reason, kind, scheduling.IdleResources)
	}

	if hasNegativeField, kind := t.pendingResources.hasNegativeWorkingField(); hasNegativeField {
		//return kind, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.PendingResources.String(), kind.String(), getQuantityOfResourceKind(t.pendingResources, kind))

		reason := fmt.Errorf("%w (%s %s = %s)", ErrNegativeResourceCount, scheduling.PendingResources.String(),
			kind.String(), getQuantityOfResourceKind(t.pendingResources, kind))
		return kind, NewErrTransactionFailed(reason, kind, scheduling.PendingResources)
	}

	if hasNegativeField, kind := t.committedResources.hasNegativeWorkingField(); hasNegativeField {
		//return kind, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.CommittedResources.String(), kind.String(), getQuantityOfResourceKind(t.committedResources, kind))

		reason := fmt.Errorf("%w (%s %s = %s)", ErrNegativeResourceCount, scheduling.CommittedResources.String(),
			kind.String(), getQuantityOfResourceKind(t.committedResources, kind))
		return kind, NewErrTransactionFailed(reason, kind, scheduling.CommittedResources)
	}

	if hasNegativeField, kind := t.specResources.hasNegativeWorkingField(); hasNegativeField {
		//return kind, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.SpecResources.String(), kind.String(), getQuantityOfResourceKind(t.specResources, kind))

		reason := fmt.Errorf("%w (%s %s = %s)", ErrNegativeResourceCount, scheduling.SpecResources.String(),
			kind.String(), getQuantityOfResourceKind(t.specResources, kind))
		return kind, NewErrTransactionFailed(reason, kind, scheduling.SpecResources)
	}

	if isLessThanOrEqual, offendingKind := t.committedResources.LessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		//return offendingKind, fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
		//	ErrTransactionFailed, scheduling.CommittedResources.String(), offendingKind.String(),
		//	getQuantityOfResourceKind(t.committedResources, offendingKind).StringFixed(4),
		//	offendingKind.String(), scheduling.SpecResources.String(),
		//	getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))

		inconsistentResourcesError := scheduling.NewInconsistentResourcesErrorWithResourceQuantity(
			offendingKind, scheduling.QuantityGreaterThanSpec, scheduling.CommittedResources,
			getQuantityOfResourceKind(t.committedResources, offendingKind),
			getQuantityOfResourceKind(t.specResources, offendingKind))

		//reason := fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
		//	ErrTransactionFailed, scheduling.CommittedResources.String(), offendingKind.String(),
		//	getQuantityOfResourceKind(t.committedResources, offendingKind).StringFixed(4),
		//	offendingKind.String(), scheduling.SpecResources.String(),
		//	getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
		return offendingKind, NewErrTransactionFailed(inconsistentResourcesError, offendingKind, scheduling.SpecResources)
	}

	if isLessThanOrEqual, offendingKind := t.idleResources.LessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		//return offendingKind, fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
		//	ErrTransactionFailed, scheduling.IdleResources.String(), offendingKind.String(),
		//	getQuantityOfResourceKind(t.idleResources, offendingKind).StringFixed(4),
		//	offendingKind.String(), scheduling.SpecResources.String(),
		//	getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))

		//reason := fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
		//	ErrTransactionFailed, scheduling.IdleResources.String(), offendingKind.String(),
		//	getQuantityOfResourceKind(t.idleResources, offendingKind).StringFixed(4),
		//	offendingKind.String(), scheduling.SpecResources.String(),
		//	getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))

		inconsistentResourcesError := scheduling.NewInconsistentResourcesErrorWithResourceQuantity(
			offendingKind, scheduling.IdleSpecUnequal, scheduling.IdleResources,
			getQuantityOfResourceKind(t.idleResources, offendingKind),
			getQuantityOfResourceKind(t.specResources, offendingKind))
		return offendingKind, NewErrTransactionFailed(inconsistentResourcesError, offendingKind, scheduling.IdleResources)
	}

	idleSpec := t.idleResources.working
	committedSpec := t.committedResources.working
	combinedSpec := idleSpec.Add(committedSpec)

	if !combinedSpec.Equals(t.specResources.working) {
		//return scheduling.NoResource, fmt.Errorf("%w: idle resources [%s] + committed resources [%s] should equal spec resources [%s]; instead, they equal [%s]",
		//	ErrTransactionFailed, idleSpec.String(), committedSpec.String(), combinedSpec.String(), t.specResources.working.String())

		inconsistentResourcesError := scheduling.NewInconsistentResourcesErrorWithResourceQuantity(
			scheduling.UnknownResource, scheduling.IdleCommittedSumDoesNotEqualSpec, scheduling.IdleResources,
			decimal.Zero, decimal.Zero)
		return scheduling.UnknownResource, NewErrTransactionFailed(inconsistentResourcesError, scheduling.UnknownResource, scheduling.IdleResources)
	}

	return scheduling.NoResource, nil
}
