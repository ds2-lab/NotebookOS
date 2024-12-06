package resource_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
)

var _ = Describe("Transaction", func() {
	It("Should correctly commit resources", func() {
		transaction := func(m resource.TransactionState) {

		}

		resultChan := make(chan interface{}, 1)

		runner := resource.NewTransactionRunner(transaction, nil, resultChan, "")

		Expect(runner).ToNot(BeNil())
	})
})
