package policy_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
)

var _ = Describe("Policy", func() {
	var options scheduling.SchedulerOptions

	It("Should panic when trying to instantiate different policy than what is specified in config", func() {
		options = *scheduling.DefaultFcfsSchedulerOptions

		instantiatePolicy := func() {
			_, _ = policy.NewStaticPolicy(&options)
		}

		Expect(instantiatePolicy).To(Panic())
	})

	Context("FCFS Batch Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultFcfsSchedulerOptions
		})

		It("Should return the expected values", func() {
			fcfs, err := policy.NewFcfsBatchSchedulingPolicy(&options)
			Expect(err).To(BeNil())
			Expect(fcfs).ToNot(BeNil())
			Expect(fcfs.PolicyKey()).To(Equal(scheduling.FcfsBatch))
		})
	})

	Context("Dynamic v3 Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultStaticSchedulerOptions
			options.SchedulingPolicy = scheduling.DynamicV3.String()
		})

		It("Should return the expected values", func() {
			dynamicV3, err := policy.NewDynamicV3Policy(&options)
			Expect(err).To(BeNil())
			Expect(dynamicV3).ToNot(BeNil())
			Expect(dynamicV3.PolicyKey()).To(Equal(scheduling.DynamicV3))
			Expect(dynamicV3.NumReplicas()).To(Equal(3))
		})
	})

	Context("Dynamic v4 Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultStaticSchedulerOptions
			options.SchedulingPolicy = scheduling.DynamicV4.String()
		})

		It("Should return the expected values", func() {
			dynamicV4, err := policy.NewDynamicV4Policy(&options)
			Expect(err).To(BeNil())
			Expect(dynamicV4).ToNot(BeNil())
			Expect(dynamicV4.PolicyKey()).To(Equal(scheduling.DynamicV4))
			Expect(dynamicV4.NumReplicas()).To(Equal(3))
		})
	})

	Context("Gandiva Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultFcfsSchedulerOptions
			options.SchedulingPolicy = scheduling.Gandiva.String()
		})

		It("Should return the expected values", func() {
			gandiva, err := policy.NewGandivaPolicy(&options)
			Expect(err).To(BeNil())
			Expect(gandiva).ToNot(BeNil())
			Expect(gandiva.PolicyKey()).To(Equal(scheduling.Gandiva))
			Expect(gandiva.NumReplicas()).To(Equal(1))
		})
	})

	Context("Reservation Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultFcfsSchedulerOptions
			options.SchedulingPolicy = scheduling.Reservation.String()
		})

		It("Should return the expected values", func() {
			reservation, err := policy.NewReservationPolicy(&options)
			Expect(err).To(BeNil())
			Expect(reservation).ToNot(BeNil())
			Expect(reservation.PolicyKey()).To(Equal(scheduling.Reservation))
			Expect(reservation.NumReplicas()).To(Equal(1))
		})
	})

	Context("Static Scheduling Policy", func() {
		BeforeEach(func() {
			options = *scheduling.DefaultStaticSchedulerOptions
		})

		It("Should return the expected values", func() {
			static, err := policy.NewStaticPolicy(&options)
			Expect(err).To(BeNil())
			Expect(static).ToNot(BeNil())
			Expect(static.PolicyKey()).To(Equal(scheduling.Static))
			Expect(static.NumReplicas()).To(Equal(3))
		})
	})
})
