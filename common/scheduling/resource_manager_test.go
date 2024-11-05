package scheduling_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

var _ = Describe("ResourceManager", func() {
	var resourceManager *scheduling.ResourceManager
	resourceManagerSpec := types.NewDecimalSpec(8000, 64000, 8, 32)

	BeforeEach(func() {
		resourceManager = scheduling.NewResourceManager(resourceManagerSpec)
	})

	It("Will correctly handle the scheduling of a pending resource request", func() {
		kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

		err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
		Expect(err).To(BeNil())

		Expect(resourceManager.SpecResources().Equals(resourceManagerSpec)).To(BeTrue())

		Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
		Expect(resourceManager.NumAllocations()).To(Equal(1))
		Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

		Expect(resourceManager.PendingResources().Equals(kernel1Spec))
		Expect(resourceManager.IdleResources().Equals(resourceManagerSpec)).To(BeTrue())
		Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
	})

	It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
		kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

		err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
		Expect(err).To(BeNil())

		err = resourceManager.CommitResources(1, "Kernel1", kernel1Spec, false)
		Expect(err).To(BeNil())

		Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
		Expect(resourceManager.NumAllocations()).To(Equal(1))
		Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

		Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
		Expect(resourceManager.IdleResources().Equals(resourceManagerSpec.Subtract(kernel1Spec))).To(BeTrue())
		Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())
	})
})
