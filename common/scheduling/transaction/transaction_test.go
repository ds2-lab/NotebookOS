package transaction_test

import (
	"errors"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
)

var _ = Describe("Transaction Tests", func() {
	It("Should commit participants that would not result in invalid resource counts", func() {
		operation := func(s *transaction.State) {
			s.PendingResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			s.PendingResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

			s.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))
			s.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
		}

		container := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))

		idle := container.Idle.CloneDecimalSpec()
		pending := container.Pending.CloneDecimalSpec()
		committed := container.Committed.CloneDecimalSpec()

		fmt.Printf("Pre-operation: %s\n", container.GetResourceCountsAsString())

		tx := transaction.New(operation, container.getStateForTransaction())
		state, err := tx.Run()
		Expect(err).To(BeNil())
		Expect(state).ToNot(BeNil())

		commit := container.getCommit()
		commit(state)

		fmt.Printf("Post-operation: %s\n", container.GetResourceCountsAsString())

		Expect(idle.Equals(container.Idle)).To(BeFalse())
		Expect(pending.Equals(container.Pending)).To(BeTrue())
		Expect(committed.Equals(container.Committed)).To(BeFalse())
	})

	It("Should reject participants that would result in invalid resource counts", func() {
		operation := func(state *transaction.State) {
			state.PendingResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			state.PendingResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

			state.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))
			state.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

			state.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			state.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))

			// Will result in a negative count
			state.PendingResources().Subtract(types.NewDecimalSpec(1000, 0, 0, 0))
		}

		container := newResourceContainer(1, types.NewDecimalSpec(100, 100, 100, 100))

		idle := container.Idle.CloneDecimalSpec()
		pending := container.Pending.CloneDecimalSpec()
		committed := container.Committed.CloneDecimalSpec()

		fmt.Printf("Pre-operation: %s\n", container.GetResourceCountsAsString())

		tx := transaction.New(operation, container.getStateForTransaction())
		Expect(tx).ToNot(BeNil())

		_, err := tx.Run()

		fmt.Printf("Post-operation: %s\n", container.GetResourceCountsAsString())

		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())
		Expect(errors.Is(err, transaction.ErrNegativeResourceCount)).To(BeTrue())

		Expect(idle.Equals(container.Idle)).To(BeTrue())
		Expect(pending.Equals(container.Pending)).To(BeTrue())
		Expect(committed.Equals(container.Committed)).To(BeTrue())
	})
})
