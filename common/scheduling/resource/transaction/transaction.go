package transaction

import (
	"github.com/scusemua/distributed-notebook/common/types"
)

type Operation func(m *State)

type Transaction struct {
	operation Operation
	state     *State
}

func New(operation Operation, initialState *State) *Transaction {
	if operation == nil || initialState == nil {
		return nil
	}

	return &Transaction{
		operation: operation,
		state:     initialState,
	}
}

func (t *Transaction) Run() (*State, error) {
	if t.operation == nil {
		return nil, ErrNilTransactionOperation
	}

	if t.state == nil {
		return nil, ErrNilInitialState
	}

	t.operation(t.state)

	zeroSpec := types.NewDecimalSpec(0, 0, 0, 0)

	// Clamp committed resources between 0 and the Host's spec resources.
	t.state.IdleResources().Sanitize(zeroSpec, t.state.SpecResources().Initial())

	// There isn't really an upper limit here (not one that we can compute right now).
	t.state.PendingResources().Sanitize(zeroSpec, nil)

	// Clamp committed resources between 0 and the Host's spec resources.
	t.state.CommittedResources().Sanitize(zeroSpec, t.state.SpecResources().Initial())

	if err := t.state.Validate(); err != nil {
		return nil, err
	}

	return t.state, nil
}
