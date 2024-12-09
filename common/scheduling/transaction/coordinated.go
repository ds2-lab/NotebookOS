package transaction

import (
	"errors"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidParticipantId         = errors.New("invalid participant id")

	ErrTransactionRegistrationError = errors.New("failed to register coordinated transaction participant")
	ErrTransactionAlreadyStarted    = errors.New("cannot register participant as transaction has already started")
)

// CommitTransactionResult defines a function that is used to commit the result of a transaction.
type CommitTransactionResult func(state *State)

// CoordinatedParticipant represents a participant in a coordinated transaction.
//
// Each CoordinatedParticipant is typically associated with a specific replica of a kernel.
type CoordinatedParticipant struct {
	// commit defines how the committed state should be used/applied if the transaction succeeds for the CoordinatedParticipant.
	commit CommitTransactionResult

	// tx defines the operations that should be applied to the CoordinatedParticipant's resources during the transaction.
	tx *Transaction
}

// validateState checks if the result of the transaction is valid.
//
// validateState returns nil if the state is valid and an error if not. The error explains why the state is invalid.
func (p *CoordinatedParticipant) validateState() error {
	return p.tx.validateState()
}

// commitResult calls the commit function of the target CoordinatedParticipant, passing the current
// state of the target CoordinatedParticipant's Transaction field.
func (p *CoordinatedParticipant) commitResult() {
	p.commit(p.tx.state)
}

// run executes the transaction for the target CoordinatedParticipant.
func (p *CoordinatedParticipant) run(wg *sync.WaitGroup) {
	defer wg.Done()
	p.tx.run()
}

// CoordinatedTransaction encapsulates a coordinated Transaction that should be run and applied to multiple
// entities (typically replicas of a kernel) at once.
//
// The CoordinatedTransaction will either succeed for all replicas or fail for all replicas.
type CoordinatedTransaction struct {
	mu sync.Mutex

	log logger.Logger

	// participants is a map from node/participant ID to the associated CoordinatedParticipant struct.
	participants map[int32]*CoordinatedParticipant

	// expectedNumParticipants is the number of participants that are expected to register.
	// Once expectedNumParticipants participants register, the transaction will automatically start.
	expectedNumParticipants int

	// complete indicates whether the CoordinatedTransaction has finished (either successfully or not)
	complete atomic.Bool
	// succeeded indicates whether the CoordinatedTransaction completed successfully
	succeeded atomic.Bool
	// started indicates whether the CoordinatedTransaction has been started
	started atomic.Bool

	// failureReason will hold the error returned by the first participant to fail.
	// It will be nil if the CoordinatedTransaction has not been started or if the CoordinatedTransaction succeeded.
	failureReason error

	// doneGroup is used to track how many of the CoordinatedParticipants have finished running their own
	// personal transactions.
	doneGroup *sync.WaitGroup
}

func NewCoordinatedTransaction(numParticipants int) *CoordinatedTransaction {
	coordinatedTransaction := &CoordinatedTransaction{
		participants:            make(map[int32]*CoordinatedParticipant, numParticipants),
		expectedNumParticipants: numParticipants,
	}

	var doneGroup sync.WaitGroup
	doneGroup.Add(1)

	coordinatedTransaction.doneGroup = &doneGroup

	coordinatedTransaction.complete.Store(false)
	coordinatedTransaction.succeeded.Store(false)
	coordinatedTransaction.started.Store(false)

	config.InitLogger(&coordinatedTransaction.log, coordinatedTransaction)

	return coordinatedTransaction
}

// IsComplete returns a flag indicating whether the transaction is over.
func (t *CoordinatedTransaction) IsComplete() bool {
	return t.complete.Load()
}

// Succeeded returns a flag indicating whether the transaction succeeded.
//
// If the CoordinatedTransaction is not setFinished yet, then Succeeded will return false.
func (t *CoordinatedTransaction) Succeeded() bool {
	if !t.complete.Load() {
		return false
	}

	return t.succeeded.Load()
}

// Wait blocks until the CoordinatedTransaction completes and returns whether it was successful.
func (t *CoordinatedTransaction) Wait() bool {
	t.doneGroup.Wait()

	return t.succeeded.Load()
}

// RegisterParticipant is used to register a "participant" of the CoordinatedTransaction.
//
// If the CoordinatedTransaction has already started, then X will return an error.
func (t *CoordinatedTransaction) RegisterParticipant(id int32, state *State, operation Operation, commit CommitTransactionResult) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started.Load() {
		return errors.Join(ErrTransactionRegistrationError, ErrTransactionAlreadyStarted)
	}

	tx := New(operation, state)
	if err := tx.validateInputs(); err != nil {
		return errors.Join(ErrTransactionRegistrationError, err)
	}

	t.participants[id] = &CoordinatedParticipant{
		tx:     tx,
		commit: commit,
	}

	if len(t.participants) == t.expectedNumParticipants {
		return t.run()
	}

	return nil
}

// runIndividual runs an individual transaction.
func (t *CoordinatedTransaction) runIndividual(tx *Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	tx.run()
}

// NumExpectedParticipants returns the number of participants that are expected to register.
//
// Once this many participants register, the transaction will automatically start.
func (t *CoordinatedTransaction) NumExpectedParticipants() int {
	return t.expectedNumParticipants
}

// NumRegisteredParticipants returns the number of participants that have already registered.
func (t *CoordinatedTransaction) NumRegisteredParticipants() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.participants)
}

func (t *CoordinatedTransaction) FailureReason() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.failureReason
}

// recordFinished records that the CoordinatedTransaction finished.
func (t *CoordinatedTransaction) recordFinished(succeeded bool, failureReason error) {
	if succeeded && failureReason != nil {
		panic("transaction cannot succeed with a non-nil failure reason")
	}

	t.succeeded.Store(succeeded)
	t.complete.Store(true)
	t.failureReason = failureReason
	t.doneGroup.Done()
}

// run runs the transaction. run is called automatically when the last participant registers.
func (t *CoordinatedTransaction) run() error {
	if len(t.participants) == 0 {
		return ErrNilInitialState
	}

	t.started.Store(true)

	var wg sync.WaitGroup
	wg.Add(len(t.participants))

	// Inputs were validated during registration.
	for _, participant := range t.participants {
		go participant.run(&wg)
	}

	wg.Wait()

	for id, participant := range t.participants {
		err := participant.validateState()
		if err != nil {
			t.log.Warn("Participant %d failed. Aborting transaction. Reason: %v.", id, err)
			t.recordFinished(false, err)
			return err
		}
	}

	for _, participant := range t.participants {
		participant.commitResult()
	}

	t.recordFinished(true, nil)
	return nil
}
