package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
)

type TransactionOperation func(state TransactionState)

// CommitTransactionResult defines a function that is used to commit the result of a transaction.
type CommitTransactionResult func(state TransactionState)

// GetInitialStateForTransaction is a function that returns the initial state and the commit function for a transaction
// participant. Participants pass this function when registering.
//
// The expectation is that any necessary mutexes will already be held before a GetInitialStateForTransaction
// function is called.
type GetInitialStateForTransaction func() (TransactionState, CommitTransactionResult)

// CoordinatedTransaction encapsulates a coordinated Transaction that should be run and applied to multiple
// entities (typically replicas of a kernel) at once.
//
// The CoordinatedTransaction will either succeed for all replicas or fail for all replicas.
type CoordinatedTransaction interface {
	Id() string
	IsComplete() bool
	Succeeded() bool
	Started() bool
	Wait() bool
	Abort()
	RegisterParticipant(id int32, getInitialState GetInitialStateForTransaction, operation TransactionOperation, mu *sync.Mutex) error
	NumExpectedParticipants() int
	NumRegisteredParticipants() int
	FailureReason() error

	// WaitForParticipantsToBeInitialized blocks until the target CoordinatedTransaction's
	// CoordinatedParticipant instances have all registered and been initialized.
	WaitForParticipantsToBeInitialized()

	// Run will run the target CoordinatedTransaction if the target CoordinatedTransaction is ready.
	// The CoordinatedTransaction is ready when all CoordinatedParticipants have registered, and when all
	// possible driver goroutines have called TryRun.
	//
	// If the target CoordinatedTransaction is NOT ready, then TryRun will block until the target CoordinatedTransaction
	// has executed (and either failed or succeeded).
	//
	// This motivation for this is that the initialization of CoordinatedParticipants acquires all the Host-level
	// mutexes, whereas calling TryRun ensures that the AllocationManager locks have been acquired. The order in
	// which these locks are acquired is important in order to avoid deadlocks.
	//
	// The parameter sync.Mutex is an optional mutex that will be locked AFTER all the CoordinatedTransaction's
	// CoordinatedParticipant instances have been locked.
	Run() error

	// ParticipantsInitialized returns true if all the CoordinatedParticipant instances have been initialized.
	ParticipantsInitialized() bool

	// IsReady returns true if the target CoordinatedTransaction has registered all CoordinatedParticipant instances.
	//
	// IsReady will only return true if the target CoordinatedParticipant has not yet run.
	//
	// isReady is thread safe.
	//IsReady() bool
}

type TransactionState interface {
	IdleResources() TransactionResources
	PendingResources() TransactionResources
	CommittedResources() TransactionResources
	SpecResources() TransactionResources
	Clone() TransactionState

	// Validate checks if the Transaction can be committed or if it results in an invalid state.
	// If the state is invalid, then the offending ResourceKind is returned, if applicable.
	Validate() ([]ResourceKind, error)
	SetParticipantId(id int32)
	GetParticipantId() int32
}

type TransactionResources interface {
	Clone() TransactionResources
	Sanitize(min types.Spec, max types.Spec)
	Initial() types.Spec
	IsMutable() bool
	Working() types.Spec
	Add(spec types.Spec)
	Subtract(spec types.Spec)
	Equals(other interface{}) bool
	GreaterThan(spec types.Spec) (bool, ResourceKind)
	LessThanOrEqual(spec types.Spec) (bool, ResourceKind)
}
