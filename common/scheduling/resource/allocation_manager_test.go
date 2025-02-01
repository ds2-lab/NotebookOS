package resource_test

import (
	"errors"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/configuration/samples"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"gopkg.in/yaml.v3"
)

var _ = Describe("AllocationManager Standard Tests", func() {
	var (
		resourceManager  *resource.AllocationManager
		schedulingPolicy scheduling.Policy
		opts             *domain.ClusterGatewayOptions
	)

	hostSpec := types.NewDecimalSpec(8000, 64000, 8, 32)

	Context("Static Scheduling", func() {
		BeforeEach(func() {
			err := yaml.Unmarshal([]byte(samples.GatewayStaticYaml), &opts)
			Expect(err).To(BeNil())

			schedulingPolicy, err = policy.NewStaticPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			resourceManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, uuid.NewString())
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(4))

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(3))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok := errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = resourceManager.KernelReplicaScheduled(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel7",
				scheduling.DefaultExecutionId, kernel7spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(4))

			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.CPU)).
				To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.Memory)).
				To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.GPU)).
				To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.VRAM)).
				To(BeTrue())

			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel7spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(0))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then successfully scheduling a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(resourceManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := resourceManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(updatedResourceManagerSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly fail to adjust its spec GPUs when doing so would decrease them below the number of committed GPUs", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalGpuAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrInvalidOperation)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.AdjustPendingResources(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrAllocationNotFound)).To(BeTrue())
		})
	})

	Context("FCFS Batch Scheduling", func() {
		BeforeEach(func() {
			err := yaml.Unmarshal([]byte(samples.GatewayFcfsYaml), &opts)
			Expect(err).To(BeNil())

			schedulingPolicy, err = policy.NewFcfsBatchSchedulingPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			resourceManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, uuid.NewString())
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(4))

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(3))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok := errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = resourceManager.KernelReplicaScheduled(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel7",
				scheduling.DefaultExecutionId, kernel7spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(4))

			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.CPU)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.Memory)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.GPU)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.VRAM)).To(BeTrue())

			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel7spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(0))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then successfully scheduling a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(resourceManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := resourceManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(updatedResourceManagerSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly fail to adjust its spec GPUs when doing so would decrease them below the number of committed GPUs", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalGpuAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrInvalidOperation)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.AdjustPendingResources(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrAllocationNotFound)).To(BeTrue())
		})
	})

	Context("Reservation-Based Scheduling", func() {
		BeforeEach(func() {
			err := yaml.Unmarshal([]byte(samples.GatewayReservationYaml), &opts)
			Expect(err).To(BeNil())

			schedulingPolicy, err = policy.NewReservationPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			resourceManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, uuid.NewString())
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(4))

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(3))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok := errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = resourceManager.KernelReplicaScheduled(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = resourceManager.KernelReplicaScheduled(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel7",
				scheduling.DefaultExecutionId, kernel7spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(4))

			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.CPU)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.Memory)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.GPU)).To(BeTrue())
			Expect(distNbTesting.ContainsOffendingResourceKind(insufficientResourcesError.OffendingResourceKinds, scheduling.VRAM)).To(BeTrue())

			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel7spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(resourceManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(0))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := resourceManager.ReplicaEvicted(1, "Kernel1")
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then successfully scheduling a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(resourceManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := resourceManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(resourceManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(updatedResourceManagerSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())
		})

		It("Will correctly fail to adjust its spec GPUs when doing so would decrease them below the number of committed GPUs", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = resourceManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalGpuAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1Spec))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			err = resourceManager.ReleaseCommittedResources(1, "Kernel1", scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1SpecV1)
			Expect(err).To(BeNil())
			err = resourceManager.KernelReplicaScheduled(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(resourceManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := resourceManager.KernelReplicaScheduled(1, "Kernel1", kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = resourceManager.CommitResourcesToExistingContainer(1, "Kernel1",
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))

			Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
			Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(resourceManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = resourceManager.AdjustPendingResources(1, "Kernel1", kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrInvalidOperation)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := resourceManager.AdjustPendingResources(1, "Kernel1", kernel1Spec)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrAllocationNotFound)).To(BeTrue())
		})
	})
})
