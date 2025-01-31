package transaction

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrInvalidParticipantId         = errors.New("invalid participant id")
	// ErrAbortFailed                  = errors.New("failed to abort coordinated transaction as it already started")

	ErrTransactionRegistrationError = errors.New("failed to register coordinated transaction participant")
	ErrTransactionAlreadyStarted    = errors.New("cannot register participant as transaction has already started")
	ErrTransactionAborted           = errors.New("transaction was manually aborted")
	ErrMissingParticipants          = errors.New("transaction cannot run as one or more participants are missing")
)

// CommitTransactionResult defines a function that is used to commit the result of a transaction.
type CommitTransactionResult func(state *State)

// GetInitialStateForTransaction is a function that returns the initial state and the commit function for a transaction
// participant. Participants pass this function when registering.
//
// The expectation is that any necessary mutexes will already be held before a GetInitialStateForTransaction
// function is called.
type GetInitialStateForTransaction func() (*State, CommitTransactionResult)

// CoordinatedParticipant represents a participant in a coordinated transaction.
//
// Each CoordinatedParticipant is typically associated with a specific replica of a kernel.
type CoordinatedParticipant struct {
	// id is the SMR node ID of the kernel replica represented by the CoordinatedParticipant
	id int32

	// commit defines how the committed state should be used/applied if the transaction succeeds for the CoordinatedParticipant.
	commit CommitTransactionResult

	// tx defines the operations that should be applied to the CoordinatedParticipant's resources during the transaction.
	tx *Transaction

	// Operation is the operation that is executed by the CoordinatedParticipant during the CoordinatedTransaction.
	operation Operation

	// getInitialState obtains the initial state for the CoordinatedTransaction.
	// getInitialState is called after the mu is acquired.
	getInitialState GetInitialStateForTransaction

	initialState *State

	// mu is the CoordinatedParticipant's mutex. The CoordinatedTransaction will acquire the mutexes of all
	// the CoordinatedParticipant structs that are involved at the very beginning of the transaction.
	//
	// It is the caller's responsibility to unlock the mu once the transaction has finished.
	// The CoordinatedTransaction will not unlock the mu.
	mu *sync.Mutex

	log logger.Logger
}

// tryLock attempts to acquire the CoordinatedParticipant's mu (a sync.Mutex).
func (p *CoordinatedParticipant) tryLock() bool {
	return p.mu.TryLock()
}

// initialize initializes the CoordinatedParticipant, getting its initial State from the CoordinatedParticipant's
// getInitialState field (of type GetInitialStateForTransaction).
//
// initialize creates the internal, single-participant *Transaction as well.
//
// IMPORTANT: initialize should only be called once ALL mutexes (of ALL CoordinatedParticipant entities involved
// in the CoordinatedTransaction) have been acquired.
func (p *CoordinatedParticipant) initialize(txId string) error {
	if p.operation == nil {
		return ErrNilTransactionOperation
	}

	// Get the initial state for the transaction.
	p.initialState, p.commit = p.getInitialState()

	if p.initialState == nil {
		return ErrNilInitialState
	}

	p.initialState.ParticipantId = p.id

	p.tx = New(p.operation, p.initialState)
	if p.tx == nil {
		return fmt.Errorf("unexpectedly failed to initialize transaction")
	}

	if err := p.tx.validateInputs(); err != nil {
		return errors.Join(ErrTransactionRegistrationError, err)
	}

	config.InitLogger(&p.log, fmt.Sprintf("CoordTx-%s-%d ", txId, p.id))

	return nil
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

	id string

	kernelId string

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
	// shouldAbort is true when the CoordinatedTransaction runs, it will automatically fail no matter what
	shouldAbort atomic.Bool
	// aborted is set to true if the CoordinatedTransaction was (successfully) aborted
	aborted atomic.Bool

	// failureReason will hold the error returned by the first participant to fail.
	// It will be nil if the CoordinatedTransaction has not been started or if the CoordinatedTransaction succeeded.
	failureReason error

	// doneGroup is used to track how many of the CoordinatedParticipants have finished running their own
	// personal transactions.
	doneGroup *sync.WaitGroup
}

func NewCoordinatedTransaction(numParticipants int, kernelId string) *CoordinatedTransaction {
	coordinatedTransaction := &CoordinatedTransaction{
		id:                      uuid.NewString(),
		kernelId:                kernelId,
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

func (t *CoordinatedTransaction) Id() string {
	return t.id
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

func (t *CoordinatedTransaction) Started() bool {
	return t.started.Load()
}

// Wait blocks until the CoordinatedTransaction completes and returns whether it was successful.
func (t *CoordinatedTransaction) Wait() bool {
	if t.shouldAbort.Load() {
		t.log.Debug("Transaction %s targeting kernel %s was aborted. Immediately returning from Wait().",
			t.id, t.kernelId)
		return false
	}

	t.doneGroup.Wait()

	return t.succeeded.Load()
}

// Abort attempts to abort the transaction.
func (t *CoordinatedTransaction) Abort() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.shouldAbort.Store(true)
}

// RegisterParticipant is used to register a "participant" of the CoordinatedTransaction.
//
// If the CoordinatedTransaction has already started, then X will return an error.
//
// The expectation is that any necessary mutexes will already be held before the initial state function is called.
//
// The given mutex will be locked by the CoordinatedTransaction, but it is the caller's responsibility to unlock it.
func (t *CoordinatedTransaction) RegisterParticipant(id int32, getInitialState GetInitialStateForTransaction, operation Operation, mu *sync.Mutex) error {
	if getInitialState == nil {
		return errors.Join(ErrTransactionRegistrationError, ErrNilInitialStateFunction)
	}

	if mu == nil {
		return errors.Join(ErrTransactionRegistrationError, ErrNilParticipantMutex)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started.Load() {
		return errors.Join(ErrTransactionRegistrationError, ErrTransactionAlreadyStarted)
	}

	if t.shouldAbort.Load() {
		return errors.Join(ErrTransactionRegistrationError, ErrTransactionAborted)
	}

	t.participants[id] = &CoordinatedParticipant{
		id:              id,
		getInitialState: getInitialState,
		operation:       operation,
		mu:              mu,
	}

	if len(t.participants) == t.expectedNumParticipants {
		t.log.Debug("Registered participant %d/%d (with ID=%d) for tx %s targeting kernel %s. Beginning transaction now.",
			len(t.participants), t.expectedNumParticipants, id, t.id, t.kernelId)

		_ = t.run()
	}

	return nil
}

// NumExpectedParticipants returns the number of participants that are expected to register.
//
// Once this many participants register, the transaction will automatically start.
func (t *CoordinatedTransaction) NumExpectedParticipants() int {
	return t.expectedNumParticipants
}

// NumRegisteredParticipants returns the number of participants that have already registered.
func (t *CoordinatedTransaction) NumRegisteredParticipants() int {
	//t.mu.Lock()
	//defer t.mu.Unlock()

	return len(t.participants)
}

func (t *CoordinatedTransaction) FailureReason() error {
	//t.mu.Lock()
	//defer t.mu.Unlock()

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

// initializeAndLockParticipants acquires the mutexes of all scheduling.Host instances involved in the CoordinatedTransaction.
//
// IMPORTANT: initializeAndLockParticipants is called with the CoordinatedTransaction's mu already locked.
func (t *CoordinatedTransaction) initializeAndLockParticipants() error {
	if len(t.participants) != t.expectedNumParticipants {
		return fmt.Errorf("%w: expected %d participants, have only %d registered",
			ErrMissingParticipants, t.expectedNumParticipants, len(t.participants))
	}

	// Keep track of the mutexes that we've already locked successfully.
	lockedMutexes := make([]*sync.Mutex, 0, len(t.participants))

	releaseLocks := func() {
		if len(lockedMutexes) == 0 {
			return
		}

		// Release all locked mutexes.
		for _, mu := range lockedMutexes {
			mu.Unlock()
		}

		// Recreate the slice of locked mutexes.
		lockedMutexes = make([]*sync.Mutex, 0, len(t.participants))
	}

	// Keep trying until we've locked all the locks.
	numTries := 1
	for {
		// Iterate over all the participants, attempting to lock each one.
		for _, participant := range t.participants {
			// Try to lock the participant's mutex.
			locked := participant.tryLock()

			// Check if we succeeded in locking the participant's mutex.
			if locked {
				// We succeeded. Append the mutex to the slice of locked mutexes and continue.
				lockedMutexes = append(lockedMutexes, participant.mu)
				continue
			}

			t.log.Debug("Failed to lock participant %d's mutex for tx %s targeting kernel %s.",
				participant.id, t.id, t.kernelId)

			// We failed to lock the mutex. Release all acquired locks and break out of the (inner) for-loop.
			releaseLocks()
			break
		}

		// If we failed to lock a mutex, then the length of lockedMutexes will be 0, and we'll loop again.
		//
		// If we succeeded in locking all mutexes, then the length of lockedMutexes will be equal to
		// the value of t.expectedNumParticipants. In this case, we can simply return.
		if len(lockedMutexes) == t.expectedNumParticipants {
			t.log.Debug("Locked all %d mutexes for tx %s targeting kernel %s",
				len(lockedMutexes), t.id, t.kernelId)

			// We locked all the locks. Break out of the loop.
			break
		}

		t.log.Debug("Only locked %d mutexes for tx %s targeting kernel %s on attempt %d...",
			len(lockedMutexes), t.id, t.kernelId)

		numTries += 1

		// Sleep for a random interval between 5 - 10 milliseconds before retrying.
		time.Sleep(time.Millisecond*time.Duration(rand.Int64N(5)) + (time.Millisecond * 5))
	}

	// Now that the locks have been acquired, we initialize all the participants.
	for _, participant := range t.participants {
		err := participant.initialize(t.id)
		if err != nil {
			t.log.Error("Failed to initialize participant %d of tx %s targeting kernel %s: %v",
				participant.id, t.id, t.kernelId, err)
			return err
		}

		t.log.Debug("Successfully initialized participant %d of tx %s targeting kernel %s",
			participant.id, t.id, t.kernelId)
	}

	return nil
}

// run runs the transaction. run is called automatically when the last participant registers.
//
// IMPORTANT: run is called with the CoordinatedTransaction's mu already locked.
func (t *CoordinatedTransaction) run() error {
	if len(t.participants) == 0 {
		return ErrNilInitialState
	}

	err := t.initializeAndLockParticipants()
	if err != nil {
		t.recordFinished(false, err)
		return err
	}

	t.started.Store(true)

	var wg sync.WaitGroup
	wg.Add(len(t.participants))

	// Inputs were validated during registration.
	for _, participant := range t.participants {
		t.log.Debug("Running participant %d in tx %s targeting kernel %s",
			participant.id, t.id, t.kernelId)
		go participant.run(&wg)
	}

	wg.Wait()

	for id, participant := range t.participants {
		err = participant.validateState()
		if err != nil {
			t.log.Warn("Participant %d failed in tx %s targeting kernel %s. Aborting transaction. Reason: %v.",
				id, t.id, t.kernelId, err)
			t.recordFinished(false, err)
			return err
		}
	}

	if t.shouldAbort.Load() {
		t.log.Warn("Aborting transaction %s targeting kernel %s.", t.id, t.kernelId)
		t.recordFinished(false, ErrTransactionAborted)
		return nil
	}

	t.log.Debug("Transaction %s targeting kernel %s has succeeded.", t.id, t.kernelId)

	for _, participant := range t.participants {
		participant.commitResult()
	}
	t.recordFinished(true, nil)

	return nil
}

// runIndividual runs an individual transaction.
func (t *CoordinatedTransaction) runIndividual(tx *Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	tx.run()
}
