package daemon_test

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/local_daemon/daemon"
)

var _ = Describe("GPU Manager Tests", func() {
	config.LogLevel = logger.LOG_LEVEL_ALL

	var manager *daemon.GpuManager
	var numActualGPUs int64

	var numActualGPUsDecimal decimal.Decimal
	var tooManyGPUsDecimal decimal.Decimal
	var zeroDecimal = decimal.NewFromFloat(0.0)

	var kernel1 = "kernel1"

	BeforeEach(func() {
		numActualGPUs = 8
		manager = daemon.NewGpuManager(numActualGPUs)
		numActualGPUsDecimal = decimal.NewFromInt(numActualGPUs)
		tooManyGPUsDecimal = decimal.NewFromInt(numActualGPUs * 2)
	})

	Context("errors", func() {
		It("should return an error if actual GPUs are allocated before being put into the 'pending' state first", func() {
			Expect(manager.IdleGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(zeroDecimal)).To(BeTrue())

			err := manager.AllocateGPUs(numActualGPUsDecimal, 1, kernel1)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(daemon.ErrNoPendingAllocationFound))
		})

		It("should return an error when allocating too many GPUs", func() {
			Expect(manager.IdleGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(zeroDecimal)).To(BeTrue())

			err := manager.AllocateGPUs(tooManyGPUsDecimal, 1, kernel1)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(daemon.ErrInsufficientGPUs))
		})

		It("should return an error when deallocating GPUs that were never allocated in the first place", func() {
			Expect(manager.IdleGPUs()).To(Equal(numActualGPUsDecimal))
			Expect(manager.SpecGPUs()).To(Equal(numActualGPUsDecimal))
			Expect(manager.PendingGPUs()).To(Equal(zeroDecimal))
			Expect(manager.CommittedGPUs()).To(Equal(zeroDecimal))

			err := manager.ReleaseAllocatedGPUs(1, kernel1)
			Expect(err).ToNot(BeNil())
			Expect(err).To(Equal(daemon.ErrAllocationNotFound))
		})
	})

	Context("actual GPU allocations", func() {
		It("should allocate and deallocate resources correctly", func() {
			Expect(manager.IdleGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(zeroDecimal)).To(BeTrue())

			//
			// Pending Allocation
			//

			err := manager.AllocatePendingGPUs(numActualGPUsDecimal, 1, kernel1)

			Expect(err).To(BeNil())
			Expect(manager.NumPendingAllocations()).To(Equal(1))
			Expect(manager.NumAllocations()).To(Equal(0))

			GinkgoWriter.Printf("After creating pending GPU allocation:\n")
			GinkgoWriter.Printf("manager.CommittedGPUs(): %s\n", manager.CommittedGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.PendingGPUs(): %s\n", manager.PendingGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.SpecGPUs(): %s\n", manager.SpecGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.IdleGPUs(): %s\n", manager.IdleGPUs().StringFixed(2))

			Expect(manager.IdleGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(zeroDecimal)).To(BeTrue())

			//
			// Actual Allocation
			//

			err = manager.AllocateGPUs(numActualGPUsDecimal, 1, kernel1)

			Expect(err).To(BeNil())
			Expect(manager.NumPendingAllocations()).To(Equal(0))
			Expect(manager.NumAllocations()).To(Equal(1))

			GinkgoWriter.Printf("After allocating GPUs:\n")
			GinkgoWriter.Printf("manager.CommittedGPUs(): %s\n", manager.CommittedGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.PendingGPUs(): %s\n", manager.PendingGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.SpecGPUs(): %s\n", manager.SpecGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.IdleGPUs(): %s\n", manager.IdleGPUs().StringFixed(2))

			Expect(manager.IdleGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())

			//
			// Release Allocation
			//

			err = manager.ReleaseAllocatedGPUs(1, kernel1)

			Expect(err).To(BeNil())
			Expect(manager.NumPendingAllocations()).To(Equal(0))
			Expect(manager.NumAllocations()).To(Equal(0))

			GinkgoWriter.Printf("After releasing allocated GPUs:\n")
			GinkgoWriter.Printf("manager.CommittedGPUs(): %s\n", manager.CommittedGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.PendingGPUs(): %s\n", manager.PendingGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.SpecGPUs(): %s\n", manager.SpecGPUs().StringFixed(2))
			GinkgoWriter.Printf("manager.IdleGPUs(): %s\n", manager.IdleGPUs().StringFixed(2))

			Expect(manager.IdleGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.SpecGPUs().Equals(numActualGPUsDecimal)).To(BeTrue())
			Expect(manager.PendingGPUs().Equals(zeroDecimal)).To(BeTrue())
			Expect(manager.CommittedGPUs().Equals(zeroDecimal)).To(BeTrue())
		})
	})
})
