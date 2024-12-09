package transaction_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
)

type resourceContainer struct {
	Id        int32
	Idle      *types.DecimalSpec
	Pending   *types.DecimalSpec
	Committed *types.DecimalSpec
	Spec      *types.DecimalSpec
}

func (rc *resourceContainer) getCommit() func(state *transaction.State) {
	return func(state *transaction.State) {
		rc.Idle = types.ToDecimalSpec(state.IdleResources().Working())
		rc.Pending = types.ToDecimalSpec(state.PendingResources().Working())
		rc.Committed = types.ToDecimalSpec(state.CommittedResources().Working())
		rc.Spec = types.ToDecimalSpec(state.SpecResources().Working())
	}
}

func (rc *resourceContainer) getStateForTransaction() *transaction.State {
	idle := transaction.NewResources(rc.Idle, true)
	pending := transaction.NewResources(rc.Pending, true)
	committed := transaction.NewResources(rc.Committed, true)
	spec := transaction.NewResources(rc.Spec, false)

	return transaction.NewState(idle, pending, committed, spec)
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
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3)
		Expect(coordinatedTransaction).ToNot(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state *transaction.State) {
			state.PendingResources().Add(spec1)
		}

		err := coordinatedTransaction.RegisterParticipant(container1.Id, container1.getStateForTransaction(), tx, container1.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, container2.getStateForTransaction(), tx, container2.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, container3.getStateForTransaction(), tx, container3.getCommit())
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

	It("Will correctly reject an invalid coordinated transaction", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3)
		Expect(coordinatedTransaction).ToNot(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state *transaction.State) {
			state.PendingResources().Subtract(spec1)
		}

		err := coordinatedTransaction.RegisterParticipant(container1.Id, container1.getStateForTransaction(), tx, container1.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, container2.getStateForTransaction(), tx, container2.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeFalse())
		Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, container3.getStateForTransaction(), tx, container3.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeFalse())
		Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())
		Expect(coordinatedTransaction.FailureReason()).ToNot(BeNil())
		Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrTransactionFailed)).To(BeTrue())
		Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrNegativeResourceCount)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the transaction has already started", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3)

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state *transaction.State) {
			state.PendingResources().Add(spec1)
		}

		err := coordinatedTransaction.RegisterParticipant(container1.Id, container1.getStateForTransaction(), tx, container1.getCommit())
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, container2.getStateForTransaction(), tx, container2.getCommit())
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, container3.getStateForTransaction(), tx, container3.getCommit())
		Expect(err).To(BeNil())
		Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
		Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(3))

		succeeded := coordinatedTransaction.Wait()
		Expect(succeeded).To(BeTrue())
		Expect(coordinatedTransaction.Succeeded()).To(BeTrue())
		Expect(coordinatedTransaction.Started()).To(BeTrue())
		Expect(coordinatedTransaction.IsComplete()).To(BeTrue())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, container3.getStateForTransaction(), tx, container3.getCommit())
		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, transaction.ErrTransactionAlreadyStarted)).To(BeTrue())
	})

	It("Will return an error from CoordinatedTransaction::RegisterParticipant if the participant's transaction was not initialized correctly", func() {
		coordinatedTransaction := transaction.NewCoordinatedTransaction(3)

		container1 := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))
		container2 := newResourceContainer(2, types.NewDecimalSpec(100, 100, 100, 100))
		container3 := newResourceContainer(3, types.NewDecimalSpec(100, 100, 100, 100))

		tx := func(state *transaction.State) {
			state.PendingResources().Add(spec1)
		}

		err := coordinatedTransaction.RegisterParticipant(container1.Id, container1.getStateForTransaction(), tx, container1.getCommit())
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container2.Id, container2.getStateForTransaction(), tx, container2.getCommit())
		Expect(err).To(BeNil())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, nil, tx, container3.getCommit())
		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, transaction.ErrNilInitialState)).To(BeTrue())

		err = coordinatedTransaction.RegisterParticipant(container3.Id, container3.getStateForTransaction(), nil, container3.getCommit())
		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, transaction.ErrNilTransactionOperation)).To(BeTrue())
	})
})
