package transaction

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/shopspring/decimal"
)

type State struct {
	idleResources      *Resources
	pendingResources   *Resources
	committedResources *Resources
	specResources      *Resources
	ParticipantId      int32
}

func NewState(idleResources *Resources, pendingResources *Resources, committedResources *Resources, specResources *Resources) *State {
	state := &State{
		idleResources:      idleResources,
		pendingResources:   pendingResources,
		committedResources: committedResources,
		specResources:      specResources,
	}

	//config.InitLogger(&state.log, state)

	return state
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
func (t *State) Validate() ([]scheduling.ResourceKind, error) {
	var (
		negativeResourceErrors []error
		offendingKinds         []scheduling.ResourceKind
		offendingStatuses      []scheduling.ResourceStatus
	)

	// Create the above arrays.
	initNegResourceState := func() {
		negativeResourceErrors = make([]error, 0, 1)
		offendingKinds = make([]scheduling.ResourceKind, 0, 1)
		offendingStatuses = make([]scheduling.ResourceStatus, 0, 1)
	}

	// Append values to the above arrays.
	updateNegResourceState := func(reason error, kinds []scheduling.ResourceKind, status scheduling.ResourceStatus) {
		negativeResourceErrors = append(negativeResourceErrors, reason)
		offendingKinds = append(offendingKinds, kinds...)
		offendingStatuses = append(offendingStatuses, scheduling.IdleResources)
	}

	getNegResourceReason := func(kinds []scheduling.ResourceKind, status scheduling.ResourceStatus, res *Resources) error {
		explanation := "("
		for idx, kind := range kinds {
			explanation = explanation + fmt.Sprintf("%s %s = %s", scheduling.IdleResources.String(),
				kind.String(), getQuantityOfResourceKind(res, kind))

			if idx == len(kinds)-1 {
				explanation += ")"
			} else {
				explanation += ", "
			}
		}

		return fmt.Errorf("%w %s", ErrNegativeResourceCount, explanation)
	}

	if hasNegativeField, kinds := t.idleResources.hasNegativeWorkingField(); hasNegativeField {
		initNegResourceState()

		reason := getNegResourceReason(kinds, scheduling.IdleResources, t.idleResources)
		updateNegResourceState(reason, kinds, scheduling.IdleResources)

		// kinds, NewErrTransactionFailed(reason, kinds, scheduling.IdleResources)
	}

	if hasNegativeField, kinds := t.pendingResources.hasNegativeWorkingField(); hasNegativeField {
		//return kinds, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.PendingResources.String(), kinds.String(), getQuantityOfResourceKind(t.pendingResources, kinds))

		if negativeResourceErrors == nil {
			initNegResourceState()
		}

		reason := getNegResourceReason(kinds, scheduling.PendingResources, t.pendingResources)
		updateNegResourceState(reason, kinds, scheduling.PendingResources)
	}

	if hasNegativeField, kinds := t.committedResources.hasNegativeWorkingField(); hasNegativeField {
		//return kinds, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.CommittedResources.String(), kinds.String(), getQuantityOfResourceKind(t.committedResources, kinds))

		if negativeResourceErrors == nil {
			initNegResourceState()
		}

		reason := getNegResourceReason(kinds, scheduling.CommittedResources, t.committedResources)
		updateNegResourceState(reason, kinds, scheduling.CommittedResources)
	}

	if hasNegativeField, kinds := t.specResources.hasNegativeWorkingField(); hasNegativeField {
		//return kinds, fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
		//	scheduling.SpecResources.String(), kinds.String(), getQuantityOfResourceKind(t.specResources, kinds))

		if negativeResourceErrors == nil {
			initNegResourceState()
		}

		reason := getNegResourceReason(kinds, scheduling.SpecResources, t.specResources)
		updateNegResourceState(reason, kinds, scheduling.SpecResources)
	}

	if negativeResourceErrors != nil && len(negativeResourceErrors) > 0 {
		//t.log.Warn("Number of 'negative resource' errors: %d", len(negativeResourceErrors))
		//t.log.Warn("Offending kinds: %v", offendingKinds)
		//t.log.Warn("Offending statuses: %v", offendingStatuses)

		var err error
		for _, negativeResourceError := range negativeResourceErrors {
			if err == nil {
				err = negativeResourceError
			} else {
				err = errors.Join(err, negativeResourceError)
			}
		}

		return offendingKinds, NewErrTransactionFailed(err, offendingKinds, offendingStatuses)
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

		return []scheduling.ResourceKind{offendingKind},
			NewErrTransactionFailed(inconsistentResourcesError, []scheduling.ResourceKind{offendingKind},
				[]scheduling.ResourceStatus{scheduling.SpecResources})
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
		return []scheduling.ResourceKind{offendingKind},
			NewErrTransactionFailed(inconsistentResourcesError, []scheduling.ResourceKind{offendingKind},
				[]scheduling.ResourceStatus{scheduling.IdleResources})
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
		return []scheduling.ResourceKind{scheduling.UnknownResource},
			NewErrTransactionFailed(inconsistentResourcesError, []scheduling.ResourceKind{scheduling.UnknownResource},
				[]scheduling.ResourceStatus{scheduling.IdleResources, scheduling.CommittedResources})
	}

	return []scheduling.ResourceKind{scheduling.NoResource}, nil
}
