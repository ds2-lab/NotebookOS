package transaction_test

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
	"time"
)

type resourceContainer struct {
	Id        int32
	Idle      *types.DecimalSpec
	Pending   *types.DecimalSpec
	Committed *types.DecimalSpec
	Spec      *types.DecimalSpec
}

func (rc *resourceContainer) getCommit() func(state scheduling.TransactionState) {
	return func(state scheduling.TransactionState) {
		rc.Idle = types.ToDecimalSpec(state.IdleResources().Working())
		rc.Pending = types.ToDecimalSpec(state.PendingResources().Working())
		rc.Committed = types.ToDecimalSpec(state.CommittedResources().Working())
		rc.Spec = types.ToDecimalSpec(state.SpecResources().Working())
	}
}

func (rc *resourceContainer) getStateForTransaction() scheduling.TransactionState {
	idle := transaction.NewResources(rc.Idle, true)
	pending := transaction.NewResources(rc.Pending, true)
	committed := transaction.NewResources(rc.Committed, true)
	spec := transaction.NewResources(rc.Spec, false)

	return transaction.NewState(idle, pending, committed, spec)
}

func (rc *resourceContainer) GetResourceCountsAsString() string {
	return fmt.Sprintf("IDLE [%s], PENDING [%s], COMMITTED [%s]",
		rc.Idle.String(), rc.Pending.String(), rc.Committed.String())
}

func newResourceContainer(id int32, spec types.Spec) *resourceContainer {
	decimalSpec := types.ToDecimalSpec(spec)
	return &resourceContainer{
		Id:        id,
		Idle:      decimalSpec,
		Pending:   types.ZeroDecimalSpec,
		Committed: types.ZeroDecimalSpec,
		Spec:      decimalSpec,
	}
}

var _ = Describe("Coordinated", func() {
	spec1 := types.NewDecimalSpec(10, 10, 10, 10)

	It("Will correctly commit a valid coordinated transaction", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())
		Expect(coordinatedTransaction).ToNot(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)

			state.PendingResources().Add(spec1)

			state.PendingResources().Add(spec1)

			state.PendingResources().Subtract(spec1)
			state.IdleResources().Subtract(spec1)
			state.CommittedResources().Add(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, tx, &mu3)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeTrue())
		Expect(coordinatedTransaction.Succeeded()).To(BeTrue())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())
		Expect(coordinatedTransaction.FailureReason()).To(BeNil())
	})

	It("Will correctly commit a valid coordinated transaction even if it cannot acquire all the locks right away", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())
		Expect(coordinatedTransaction).ToNot(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)

			state.PendingResources().Add(spec1)

			state.PendingResources().Add(spec1)

			state.PendingResources().Subtract(spec1)
			state.IdleResources().Subtract(spec1)
			state.CommittedResources().Add(spec1)

			fmt.Printf("Participant %d has executed the tx and is waiting for commit\n", state.GetParticipantId())
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		mu3.Lock()

		go func() {
			err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
				state := container3.getStateForTransaction()
				commit := container3.getCommit()

				return state, commit
			}, tx, &mu3)
			Expect(err).To(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))
		}()

		Expect(coordinatedTransaction.Started()).To(BeFalse())

		time.Sleep(time.Second * 1)

		Expect(coordinatedTransaction.Started()).To(BeFalse())

		mu3.Unlock()

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeTrue())
		Expect(coordinatedTransaction.Succeeded()).To(BeTrue())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())
		Expect(coordinatedTransaction.FailureReason()).To(BeNil())
	})

	It("Will correctly reject an invalid coordinated transaction", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())
		Expect(coordinatedTransaction).ToNot(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Subtract(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, tx, &mu3)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeFalse())
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())
		Expect(coordinatedTransaction.FailureReason()).ToNot(BeNil())

		var txFailedError transaction.ErrTransactionFailed
		Expect(errors.As(coordinatedTransaction.FailureReason(), &txFailedError)).To(BeTrue())
		Expect(errors.Is(txFailedError.Reason, transaction.ErrNegativeResourceCount)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the transaction has already started", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, tx, &mu3)
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeTrue())
		Expect(coordinatedTransaction.Succeeded()).To(BeTrue())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, tx, &mu3)
		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, transaction.ErrTransactionAlreadyStarted)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the participant's initial state is nil", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			commit := container3.getCommit()

			return nil, commit
		}, tx, &mu3)

		Expect(coordinatedTransaction.Wait()).To(BeFalse())
		Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrNilInitialState)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the participant's transaction operation is nil", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, nil, &mu3)

		Expect(coordinatedTransaction.Wait()).To(BeFalse())
		Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrNilTransactionOperation)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the participant's mutex is nil", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)
		}

		var mu1, mu2 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container3.getStateForTransaction()
			commit := container3.getCommit()

			return state, commit
		}, nil, nil)
		Expect(errors.Is(err, transaction.ErrNilParticipantMutex)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the participant's 'get initial state' function is nil", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3, uuid.NewString())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state scheduling.TransactionState) {
			state.PendingResources().Add(spec1)
		}

		var mu1, mu2, mu3 sync.Mutex

		err := coordinatedTransaction.RegisterParticipant(container1.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container1.getStateForTransaction()
			commit := container1.getCommit()

			return state, commit
		}, tx, &mu1)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, func() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
			state := container2.getStateForTransaction()
			commit := container2.getCommit()

			return state, commit
		}, tx, &mu2)
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, nil, tx, &mu3)
		Expect(errors.Is(err, transaction.ErrNilInitialStateFunction)).To(BeTrue())
	})
})
