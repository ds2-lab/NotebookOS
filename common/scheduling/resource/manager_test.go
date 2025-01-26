package resource_test

import (
	"errors"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
)

var _ = Describe("Manager Tests", func() {
	Context("Transactions", func() {
		It("Should commit participants that would not result in invalid resource counts", func() {
			transaction := func(s *transaction.State) {
				s.PendingResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
				s.PendingResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

				s.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))
				s.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			}

			manager := resource.NewManager(types.NewDecimalSpec(200, 200, 200, 200))

			idle := manager.IdleResources().ToDecimalSpec()
			pending := manager.PendingResources().ToDecimalSpec()
			committed := manager.CommittedResources().ToDecimalSpec()

			fmt.Printf("Pre-operation: %s\n", manager.GetResourceCountsAsString())

			err := manager.RunTransaction(transaction)
			Expect(err).To(BeNil())

			fmt.Printf("Post-operation: %s\n", manager.GetResourceCountsAsString())

			Expect(idle.Equals(manager.IdleResources().ToDecimalSpec())).To(BeFalse())
			Expect(pending.Equals(manager.PendingResources().ToDecimalSpec())).To(BeTrue())
			Expect(committed.Equals(manager.CommittedResources().ToDecimalSpec())).To(BeFalse())
		})

		It("Should reject participants that would result in invalid resource counts", func() {
			tx := func(s *transaction.State) {
				s.IdleResources().Add(types.NewDecimalSpec(25, 25, 25, 25))

				s.PendingResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))
				s.PendingResources().Add(types.NewDecimalSpec(25, 25, 25, 25))

				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
				s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			}

			manager := resource.NewManager(types.NewDecimalSpec(100, 100, 100, 100))

			idle := manager.IdleResources().ToDecimalSpec()
			pending := manager.PendingResources().ToDecimalSpec()
			committed := manager.CommittedResources().ToDecimalSpec()

			fmt.Printf("Pre-operation: %s\n", manager.GetResourceCountsAsString())

			err := manager.RunTransaction(tx)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, transaction.ErrTransactionFailed)).To(BeTrue())

			fmt.Printf("Post-operation: %s\n", manager.GetResourceCountsAsString())

			Expect(idle.Equals(manager.IdleResources().ToDecimalSpec())).To(BeTrue())
			Expect(pending.Equals(manager.PendingResources().ToDecimalSpec())).To(BeTrue())
			Expect(committed.Equals(manager.CommittedResources().ToDecimalSpec())).To(BeTrue())
		})
	})

	Context("Coordinated Transactions", func() {
		baseSpec := types.NewDecimalSpec(100, 100, 100, 100)
		deltaSpec := types.NewDecimalSpec(25, 25, 25, 25)

		It("Will correctly commit a valid coordinated transaction", func() {
			coordinatedTransaction := transaction.NewCoordinatedTransaction(3)
			Expect(coordinatedTransaction).ToNot(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			tx := func(state *transaction.State) {
				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Subtract(deltaSpec)
				state.IdleResources().Subtract(deltaSpec)
				state.CommittedResources().Add(deltaSpec)
			}

			var mu1, mu2, mu3 sync.Mutex

			manager1 := resource.NewManager(baseSpec)
			initialState1, commit1 := manager1.GetTransactionData()

			err := coordinatedTransaction.RegisterParticipant(1, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState1, commit1
			}, tx, &mu1)
			Expect(err).To(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			manager2 := resource.NewManager(baseSpec)
			initialState2, commit2 := manager2.GetTransactionData()
			err = coordinatedTransaction.RegisterParticipant(1, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState2, commit2
			}, tx, &mu2)
			Expect(err).To(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			manager3 := resource.NewManager(baseSpec)
			initialState3, commit3 := manager3.GetTransactionData()
			err = coordinatedTransaction.RegisterParticipant(3, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState3, commit3
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

		It("Will correctly reject a coordinated transaction that would result in an invalid resource state", func() {
			coordinatedTransaction := transaction.NewCoordinatedTransaction(3)
			Expect(coordinatedTransaction).ToNot(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(0))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			tx := func(state *transaction.State) {
				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Add(deltaSpec)

				state.PendingResources().Subtract(deltaSpec)
				state.IdleResources().Subtract(deltaSpec)
				state.CommittedResources().Add(deltaSpec)

				// Will result in a negative state.
				state.CommittedResources().Subtract(types.NewDecimalSpec(1500, 1500, 1500, 1500))
			}

			var mu1, mu2, mu3 sync.Mutex

			manager1 := resource.NewManager(baseSpec)
			initialState1, commit1 := manager1.GetTransactionData()
			err := coordinatedTransaction.RegisterParticipant(1, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState1, commit1
			}, tx, &mu1)
			Expect(err).To(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(1))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			manager2 := resource.NewManager(baseSpec)
			initialState2, commit2 := manager2.GetTransactionData()
			err = coordinatedTransaction.RegisterParticipant(1, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState2, commit2
			}, tx, &mu2)
			Expect(err).To(BeNil())
			Expect(coordinatedTransaction.NumExpectedParticipants()).To(Equal(3))
			Expect(coordinatedTransaction.NumRegisteredParticipants()).To(Equal(2))
			Expect(coordinatedTransaction.Succeeded()).To(BeFalse())
			Expect(coordinatedTransaction.Started()).To(BeFalse())
			Expect(coordinatedTransaction.IsComplete()).To(BeFalse())

			manager3 := resource.NewManager(baseSpec)
			initialState3, commit3 := manager3.GetTransactionData()
			err = coordinatedTransaction.RegisterParticipant(3, func() (*transaction.State, transaction.CommitTransactionResult) {
				return initialState3, commit3
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
			Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrTransactionFailed)).To(BeTrue())
			Expect(errors.Is(coordinatedTransaction.FailureReason(), transaction.ErrNegativeResourceCount)).To(BeTrue())
		})
	})
})
