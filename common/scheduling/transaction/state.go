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
	if hasNegativeField, kind := t.idleResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrTransactionFailed, kind.String(), getQuantityOfResourceKind(t.idleResources, kind))
	}

	if hasNegativeField, kind := t.pendingResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrTransactionFailed, kind.String(), getQuantityOfResourceKind(t.pendingResources, kind))
	}

	if hasNegativeField, kind := t.committedResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrTransactionFailed, kind.String(), getQuantityOfResourceKind(t.committedResources, kind))
	}

	if hasNegativeField, kind := t.specResources.hasNegativeField(); hasNegativeField {
		return fmt.Errorf("%w: %s would become negative (%s)", ErrTransactionFailed, kind.String(), getQuantityOfResourceKind(t.specResources, kind))
	}

	if isLessThanOrEqual, offendingKind := t.committedResources.lessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: committed %s (%s) would exceed spec %s (%s)",
			ErrTransactionFailed, offendingKind.String(), getQuantityOfResourceKind(t.committedResources, offendingKind).StringFixed(4),
			offendingKind.String(), getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	if isLessThanOrEqual, offendingKind := t.idleResources.lessThanOrEqual(t.specResources.initial); !isLessThanOrEqual {
		return fmt.Errorf("%w: idle %s (%s) would exceed spec %s (%s)",
			ErrTransactionFailed, offendingKind.String(), getQuantityOfResourceKind(t.idleResources, offendingKind).StringFixed(4),
			offendingKind.String(), getQuantityOfResourceKind(t.specResources, offendingKind).StringFixed(4))
	}

	return nil
}
