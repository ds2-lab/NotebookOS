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
}

type TransactionState interface {
	IdleResources() TransactionResources
	PendingResources() TransactionResources
	CommittedResources() TransactionResources
	SpecResources() TransactionResources
	Validate() error
	SetParticipantId(id int32)
	GetParticipantId() int32
}

type TransactionResources interface {
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
