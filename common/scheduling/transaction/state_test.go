package transaction_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
)

var _ = Describe("State", func() {
	Context("State validation", func() {
		It("Will correctly identify a valid state as being valid", func() {
			idle := transaction.NewResources(types.NewDecimalSpec(100, 100, 100, 100), true)
			pending := transaction.NewResources(types.NewDecimalSpec(100, 100, 100, 100), true)
			committed := transaction.NewResources(types.NewDecimalSpec(100, 100, 100, 100), true)
			spec := transaction.NewResources(types.NewDecimalSpec(100, 100, 100, 100), true)

			state := transaction.NewState(idle, pending, committed, spec)
			Expect(state).ToNot(BeNil())
		})
	})
})
