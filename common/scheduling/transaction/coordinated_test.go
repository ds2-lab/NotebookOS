package transaction_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
)

var _ = Describe("Coordinated", func() {
	It("Will correctly commit a valid coordinated transaction", func() {
		coordinated := transaction.NewCoordinatedTransaction(3)
		Expect(coordinated).ToNot(BeNil())
		Expect(coordinated.FailureReason()
	})
})
