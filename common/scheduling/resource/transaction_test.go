package resource_test

import (
	"errors"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/types"
)

var _ = Describe("Transaction Tests", func() {
	It("Should commit transactions that would not result in invalid resource counts", func() {
		transaction := func(s resource.TransactionState) {
			s.IdleResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))

			s.PendingResources().Subtract(types.NewDecimalSpec(25, 25, 25, 25))
			s.PendingResources().Add(types.NewDecimalSpec(25, 25, 25, 25))

			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
			s.CommittedResources().Add(types.NewDecimalSpec(25, 25, 25, 25))
		}

		manager := resource.NewManager(types.NewDecimalSpec(200, 200, 200, 200))

		idle := manager.IdleResources().ToDecimalSpec()
		pending := manager.PendingResources().ToDecimalSpec()
		committed := manager.CommittedResources().ToDecimalSpec()

		fmt.Printf("Pre-transaction: %s\n", manager.GetResourceCountsAsString())

		err := manager.RunTransaction(transaction)
		Expect(err).To(BeNil())

		fmt.Printf("Post-transaction: %s\n", manager.GetResourceCountsAsString())

		Expect(idle.Equals(manager.IdleResources().ToDecimalSpec())).To(BeFalse())
		Expect(pending.Equals(manager.PendingResources().ToDecimalSpec())).To(BeTrue())
		Expect(committed.Equals(manager.CommittedResources().ToDecimalSpec())).To(BeFalse())
	})

	It("Should reject transactions that would result in invalid resource counts", func() {
		transaction := func(s resource.TransactionState) {
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

		fmt.Printf("Pre-transaction: %s\n", manager.GetResourceCountsAsString())

		err := manager.RunTransaction(transaction)
		Expect(err).ToNot(BeNil())
		Expect(errors.Is(err, resource.ErrInvalidOperation)).To(BeTrue())

		fmt.Printf("Post-transaction: %s\n", manager.GetResourceCountsAsString())

		Expect(idle.Equals(manager.IdleResources().ToDecimalSpec())).To(BeTrue())
		Expect(pending.Equals(manager.PendingResources().ToDecimalSpec())).To(BeTrue())
		Expect(committed.Equals(manager.CommittedResources().ToDecimalSpec())).To(BeTrue())
	})
})
