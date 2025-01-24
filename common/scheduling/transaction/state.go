package transaction

import (
	"fmt"
)

type State struct {
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

func (t *State) IdleResources() *Resources {
	return t.idleResources
}

func (t *State) PendingResources() *Resources {
	return t.pendingResources
}

func (t *State) CommittedResources() *Resources {
	return t.committedResources
}

func (t *State) SpecResources() *Resources {
	return t.specResources
}

// Validate checks that the operation state is in a valid state. Validate error returns nil if so.
func (t *State) Validate() error {
	if hasNegativeField, kind := t.idleResources.hasNegativeWorkingField(); hasNegativeField {
		return fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
			IdleResources.String(), kind.String(), getQuantityOfResourceKind(t.idleResources, kind))
	}

	if hasNegativeField, kind := t.pendingResources.hasNegativeWorkingField(); hasNegativeField {
		return fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
			PendingResources.String(), kind.String(), getQuantityOfResourceKind(t.pendingResources, kind))
	}

	if hasNegativeField, kind := t.committedResources.hasNegativeWorkingField(); hasNegativeField {
		return fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
			CommittedResources.String(), kind.String(), getQuantityOfResourceKind(t.committedResources, kind))
	}

	if hasNegativeField, kind := t.specResources.hasNegativeWorkingField(); hasNegativeField {
		return fmt.Errorf("%w: %w (%s %s = %s)", ErrTransactionFailed, ErrNegativeResourceCount,
			SpecResources.String(), kind.String(), getQuantityOfResourceKind(t.specResources, kind))
	}

	if isLessThanOrEqual, offendingKind := t.committedResources.LessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
			ErrTransactionFailed, CommittedResources.String(), offendingKind.String(),
			getQuantityOfResourceKind(t.committedResources, offendingKind).StringFixed(4),
			offendingKind.String(), SpecResources.String(),
			getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	if isLessThanOrEqual, offendingKind := t.idleResources.LessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: %s %s (%s) would exceed %s %s (%s)",
			ErrTransactionFailed, IdleResources.String(), offendingKind.String(),
			getQuantityOfResourceKind(t.idleResources, offendingKind).StringFixed(4),
			offendingKind.String(), SpecResources.String(),
			getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	idleSpec := t.idleResources.working
	committedSpec := t.committedResources.working
	combinedSpec := idleSpec.Add(committedSpec)

	if !combinedSpec.Equals(t.specResources.working) {
		return fmt.Errorf("%w: idle resources [%s] + committed resources [%s] should equal spec resources [%s]; instead, they equal [%s]",
			ErrTransactionFailed, idleSpec.String(), committedSpec.String(), combinedSpec.String(), t.specResources.working.String())
	}

	return nil
}
