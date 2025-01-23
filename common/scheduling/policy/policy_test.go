package policy_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
)

var _ = Describe("Policy", func() {
	var options *scheduling.SchedulerOptions

	Context("FCFS Batch Scheduling Policy", func() {
		BeforeEach(func() {
			options = scheduling.DefaultFcfsSchedulerOptions
		})

		It("Should return the expected values", func() {
			fcfs, err := policy.NewFcfsBatchSchedulingPolicy(options)
			Expect(err).To(BeNil())
			Expect(fcfs).ToNot(BeNil())
			Expect(fcfs.PolicyKey()).To(Equal(scheduling.FcfsBatch))
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
		BeforeEach(func() {
			options = scheduling.DefaultStaticSchedulerOptions
		})

		It("Should return the expected values", func() {
			static, err := policy.NewStaticPolicy(options)
			Expect(err).To(BeNil())
			Expect(static).ToNot(BeNil())
			Expect(static.PolicyKey()).To(Equal(scheduling.Static))
		})
	})
})
