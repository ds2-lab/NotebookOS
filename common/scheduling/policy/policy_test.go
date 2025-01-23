package policy_test

import (
	"github.com/Scusemua/go-utils/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"log"
	"os"
)

func validateOptions(options *scheduling.SchedulerOptions) {
	flags, err := config.ValidateOptions(options)
	if errors.Is(err, config.ErrPrintUsage) {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
}

var _ = Describe("Policy", func() {
	var options *scheduling.SchedulerOptions

	Context("FCFS Batch Scheduling Policy", func() {
		BeforeEach(func() {

		})

		It("Should return the expected values", func() {
			fcfs, err := policy.NewFcfsBatchSchedulingPolicy()
			Expect(err).To(BeNil())
		})
	})

	Context("Autoscaling FCFS Batch Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})

	Context("Dynamic v3 Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})

	Context("Dynamic v4 Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})

	Context("Gandiva Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})

	Context("Reservation Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})

	Context("Static Scheduling Policy", func() {
		It("Should return the expected values", func() {

		})
	})
})
