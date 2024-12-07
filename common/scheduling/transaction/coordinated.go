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

type CommitTransactionResult func(state *State)

type CoordinatedParticipant struct {
	commit CommitTransactionResult
	tx     *Transaction
}

func (p *CoordinatedParticipant) validateState() error {
	return p.tx.validateState()
}

func (p *CoordinatedParticipant) commitResult() {
	p.commit(p.tx.state)
}

func (p *CoordinatedParticipant) run(wg *sync.WaitGroup) {
	defer wg.Done()
	p.tx.run()
}

type CoordinatedTransaction struct {
	mu sync.Mutex

	operation Operation

	log logger.Logger

	participants map[int32]*CoordinatedParticipant

	expectedNumParticipants int

	complete  atomic.Bool
	succeeded atomic.Bool
	started   atomic.Bool

	failureReason error

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
	if t.operation == nil {
		return ErrNilTransactionOperation
	}

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
