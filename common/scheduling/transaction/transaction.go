package transaction

import (
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
	"sync/atomic"
)

type Operation func(state *State)

type Transaction struct {
	mu sync.Mutex

	operation Operation
	state     *State

	complete  atomic.Bool
	succeeded atomic.Bool
}

func New(operation Operation, initialState *State) *Transaction {
	if operation == nil || initialState == nil {
		return nil
	}

	tx := &Transaction{
		operation: operation,
		state:     initialState,
	}

	tx.complete.Store(false)
	tx.succeeded.Store(false)

	return tx
}

// validateInputs ensures that the Transaction has a valid Operation and State assigned to it before running.
func (t *Transaction) validateInputs() error {
	if t.operation == nil {
		return ErrNilTransactionOperation
	}

	if t.state == nil {
		return ErrNilInitialState
	}

	return nil
}

// run actually executes the Transaction.
func (t *Transaction) run() {
	t.operation(t.state)

	zeroSpec := types.NewDecimalSpec(0, 0, 0, 0)

	// Clamp committed resources between 0 and the Host's spec resources.
	t.state.IdleResources().Sanitize(zeroSpec, t.state.SpecResources().Initial())

	// There isn't really an upper limit here (not one that we can compute right now).
	t.state.PendingResources().Sanitize(zeroSpec, nil)

	// Clamp committed resources between 0 and the Host's spec resources.
	t.state.CommittedResources().Sanitize(zeroSpec, t.state.SpecResources().Initial())
}

// validateState checks that the Transaction's state is valid.
//
// This is NOT thread-safe.
func (t *Transaction) validateState() error {
	return t.state.Validate()
}

// setFinished is used internally to mark the Transaction as complete and to record whether it succeeded.
//
// This is NOT thread-safe.
func (t *Transaction) setFinished(success bool) {
	t.succeeded.Store(success)
	t.complete.Store(true)
}

// Run executes the Transaction and validates the state after.
func (t *Transaction) Run() (*State, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.validateInputs(); err != nil {
		return nil, err
	}

	t.run()

	if err := t.validateState(); err != nil {
		t.setFinished(false)
		return nil, err
	}

	t.setFinished(true)
	return t.state, nil
}
