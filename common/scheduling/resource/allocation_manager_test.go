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
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
	"time"
)

var _ = Describe("AllocationManager Standard Tests", func() {
	var (
		allocationManager *resource.AllocationManager
		schedulingPolicy  scheduling.Policy
		opts              *domain.ClusterGatewayOptions
	)

	hostSpec := types.NewDecimalSpec(8000, 64000, 8, 32)

	kernel1Id := "Kernel1"
	hostId := uuid.NewString()
	hostName := "TestNode"

	Context("Allocation Tests", func() {
		BeforeEach(func() {
			err := yaml.Unmarshal([]byte(samples.GatewayStaticYaml), &opts)
			Expect(err).To(BeNil())

			schedulingPolicy, err = policy.NewStaticPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			allocationManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, hostId, hostName)
		})

		It("Will correctly enable the specification of an allocation's ID and GPU device IDs", func() {
			builder := resource.NewResourceAllocationBuilder()

			allocation := builder.WithIdOverride("allocation id").BuildResourceAllocation()

			Expect(allocation.AllocationId).To(Equal("allocation id"))

			allocation = builder.WithGpuDeviceId(1).BuildResourceAllocation()

			Expect(len(allocation.GpuDeviceIds)).To(Equal(1))
			Expect(allocation.GpuDeviceIds[0]).To(Equal(1))

			allocation = builder.WithGpuDeviceIds([]int{1, 2, 3}).BuildResourceAllocation()

			Expect(len(allocation.GpuDeviceIds)).To(Equal(3))
			Expect(allocation.GpuDeviceIds[0]).To(Equal(1))
			Expect(allocation.GpuDeviceIds[1]).To(Equal(2))
			Expect(allocation.GpuDeviceIds[2]).To(Equal(3))
		})

		It("Will correctly enable the specification of an allocation's reservation and pre-commit statuses", func() {
			builder := resource.NewResourceAllocationBuilder()

			allocation := builder.
				IsAReservation().
				IsNotAPreCommitment().
				BuildResourceAllocation()

			Expect(allocation.IsReservationAllocation).To(BeTrue())
			Expect(allocation.IsReservation()).To(BeTrue())

			Expect(allocation.IsPreCommittedAllocation).To(BeFalse())
			Expect(allocation.IsPreCommitted()).To(BeFalse())

			allocation = builder.
				IsAPreCommitment().
				IsNotAReservation().
				BuildResourceAllocation()

			Expect(allocation.IsReservationAllocation).To(BeFalse())
			Expect(allocation.IsReservation()).To(BeFalse())

			Expect(allocation.IsPreCommittedAllocation).To(BeTrue())
			Expect(allocation.IsPreCommitted()).To(BeTrue())
		})

		It("Will correctly return the replica ID of an allocation", func() {
			kernel1Spec := types.NewDecimalSpec(0, 0, 0, 0)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())

			Expect(allocation.IsReservation()).To(BeFalse())
			allocation.SetIsReservation(true)
			Expect(allocation.IsReservation()).To(BeTrue())
		})

		It("Will correctly identify when an allocation is a reservation", func() {
			kernel1Spec := types.NewDecimalSpec(0, 0, 0, 0)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))

			allocation.SetReplicaId(int32(2))
			Expect(allocation.GetReplicaId()).To(Equal(int32(2)))
		})

		It("Will correctly identify when an allocation is 'zero'", func() {
			kernel1Spec := types.NewDecimalSpec(0, 0, 0, 0)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsNonZero()).To(BeFalse())
		})

		It("Will correctly return a cloned allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			clonedAllocation := allocation.CloneAndReturnedAdjusted(kernel1Spec)

			Expect(allocation.GetKernelId()).To(Equal(clonedAllocation.GetKernelId()))
			Expect(allocation.GetReplicaId()).To(Equal(clonedAllocation.GetReplicaId()))
			Expect(allocation.GetMillicpus()).To(Equal(clonedAllocation.GetMillicpus()))
			Expect(allocation.GetMemoryMb()).To(Equal(clonedAllocation.GetMemoryMb()))
			Expect(allocation.GetGpus()).To(Equal(clonedAllocation.GetGpus()))
			Expect(allocation.GetVramGb()).To(Equal(clonedAllocation.GetVramGb()))
			Expect(allocation.ToDecimalSpec().Equals(clonedAllocation.ToDecimalSpec())).To(BeTrue())
			Expect(allocation.IsCommitted()).To(Equal(clonedAllocation.IsCommitted()))
			Expect(allocation.IsPending()).To(Equal(clonedAllocation.IsPending()))
		})

		It("Will correctly return a cloned and adjusted allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			kernel2Spec := types.NewDecimalSpec(14000, 26000, 5, 12)

			clonedAllocation := allocation.CloneAndReturnedAdjusted(kernel2Spec)

			Expect(clonedAllocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(clonedAllocation.GetKernelId()))
			Expect(allocation.GetReplicaId()).To(Equal(clonedAllocation.GetReplicaId()))
			Expect(clonedAllocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(clonedAllocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(clonedAllocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(clonedAllocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(clonedAllocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(clonedAllocation.ToSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(Equal(clonedAllocation.IsCommitted()))
			Expect(allocation.IsPending()).To(Equal(clonedAllocation.IsPending()))
		})
	})

	Context("Static Scheduling", func() {
		BeforeEach(func() {
			err := yaml.Unmarshal([]byte(samples.GatewayStaticYaml), &opts)
			Expect(err).To(BeNil())

			schedulingPolicy, err = policy.NewStaticPolicy(&opts.SchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			allocationManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, hostId, hostName)
		})

		It("Will correctly report the spec resources", func() {
			Expect(allocationManager.SpecCPUs().Equal(hostSpec.Millicpus)).To(BeTrue())
			Expect(allocationManager.SpecMemoryMB().Equal(hostSpec.MemoryMb)).To(BeTrue())
			Expect(allocationManager.SpecGPUs().Equal(hostSpec.GPUs)).To(BeTrue())
			Expect(allocationManager.SpecVRAM().Equal(hostSpec.VRam)).To(BeTrue())
		})

		It("Will correctly report the node ID and node name", func() {
			Expect(allocationManager.GetNodeName()).To(Equal(hostName))
			Expect(allocationManager.GetNodeId()).To(Equal(hostId))
			Expect(len(allocationManager.GetId()) > 0).To(BeTrue())
		})

		It("Will correctly report when it can and cannot commit resources to containers", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			kernel2Spec := types.NewDecimalSpec(4000, 16000, 4, 8)

			Expect(allocationManager.CanCommitResources(kernel2Spec)).To(BeTrue())

			kernel3Spec := types.NewDecimalSpec(4000, 16000, 8, 8)

			Expect(allocationManager.CanCommitResources(kernel3Spec)).To(BeTrue())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			Expect(allocationManager.CanCommitResources(kernel2Spec)).To(BeTrue())

			Expect(allocationManager.CanCommitResources(kernel3Spec)).To(BeFalse())
		})

		It("Will correctly report when it can and cannot serve containers", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			kernel2Spec := types.NewDecimalSpec(4000, 16000, 4, 8)

			Expect(allocationManager.CanServeContainer(kernel2Spec)).To(BeTrue())

			kernel3Spec := types.NewDecimalSpec(4000, 16000, 18, 8)

			Expect(allocationManager.CanServeContainer(kernel3Spec)).To(BeFalse())
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			_, err := allocationManager.GetGpuDeviceIdsAssignedToReplica(1, kernel1Id)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrAllocationNotFound)).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.IdleCPUs().Equals(hostSpec.Millicpus)).To(BeTrue())
			Expect(allocationManager.IdleMemoryMB().Equals(hostSpec.MemoryMb)).To(BeTrue())
			Expect(allocationManager.IdleGPUs().Equals(hostSpec.GPUs)).To(BeTrue())
			Expect(allocationManager.IdleVRamGB().Equals(hostSpec.VRam)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(allocationManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(allocationManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())
			Expect(allocationManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(allocationManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())

			gpuDeviceIds, err := allocationManager.GetGpuDeviceIdsAssignedToReplica(1, kernel1Id)
			Expect(err).To(BeNil())
			Expect(gpuDeviceIds).ToNot(BeNil())
			Expect(len(gpuDeviceIds) == 0).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsNonZero()).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			gpuDeviceIds, err := allocationManager.GetGpuDeviceIdsAssignedToReplica(1, kernel1Id)
			Expect(err).To(BeNil())
			Expect(gpuDeviceIds).ToNot(BeNil())
			Expect(len(gpuDeviceIds) == 2).To(BeTrue())
		})

		It("Will correctly pre-commit resources to an existing container", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			gpuDeviceIDs := []int{0, 1}
			execId := uuid.NewString()

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsPreCommitted()).To(BeFalse())

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			ids, err := allocationManager.PreCommitResourcesToExistingContainer(1, kernel1Id, execId, kernel1Spec, gpuDeviceIDs)
			Expect(ids).ToNot(BeNil())
			Expect(len(ids) == len(gpuDeviceIDs)).To(BeTrue())
			Expect(ids).To(Equal(gpuDeviceIDs))
			Expect(err).To(BeNil())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsPreCommitted()).To(BeTrue())

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())
		})

		It("Will correctly return an error when trying to pre-commit resources to a non-existant container", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			gpuDeviceIDs := []int{0, 1}
			execId := uuid.NewString()

			ids, err := allocationManager.PreCommitResourcesToExistingContainer(1, kernel1Id, execId, kernel1Spec, gpuDeviceIDs)
			Expect(ids).To(BeNil())
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrContainerNotPresent)).To(BeTrue())
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(4))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(3))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel7",
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
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustKernelResourceRequest(kernel1SpecV2, kernel1SpecV1, int32(1), kernel1Id)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica with pending resources", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			err = allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(0))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocationManager.CommittedCPUs().Equals(kernel1Spec.Millicpus)).To(BeTrue())
			Expect(allocationManager.CommittedMemoryMB().Equals(kernel1Spec.MemoryMb)).To(BeTrue())
			Expect(allocationManager.CommittedGPUs().Equals(kernel1Spec.GPUs)).To(BeTrue())
			Expect(allocationManager.CommittedVRamGB().Equals(kernel1Spec.VRam)).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(allocationManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := allocationManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalResourceAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrResourcesAlreadyCommitted)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.AdjustPendingResources(1, kernel1Id, kernel1Spec)
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

			allocationManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, hostId, hostName)
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.GetHostId()).To(Equal(hostId))
			Expect(allocation.GetHostName()).To(Equal(hostName))
			Expect(len(allocation.GetAllocationId()) > 0).To(BeTrue())
			Expect(time.Since(allocation.GetTimestamp()) < time.Second*2).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(4))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(3))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel7",
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
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica with committed resources", func() {

		})

		It("Will correctly handle evicting a kernel replica with pending resources", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			err = allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(0))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(allocationManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := allocationManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalResourceAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrResourcesAlreadyCommitted)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.AdjustPendingResources(1, kernel1Id, kernel1Spec)
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

			allocationManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, hostId, hostName)
		})

		It("Will correctly handle the scheduling of a single pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
		})

		It("Will correctly handle the scheduling of multiple pending resource request", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3250, 12345, 5, 3)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())
		})

		It("Will correctly handle the promotion of a pending resource allocation to a committed allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())
		})

		It("Will fail to promote a pending allocation to a committed allocation for a non-existent pending allocation", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			allocatedGpuResourceIds, err := allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
			Expect(allocatedGpuResourceIds).To(BeNil())
		})

		It("Will correctly handle scheduling multiple committed resources", func() {
			By("Correctly handling the scheduling of the first pending resources")

			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			allocation, ok := allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the second pending resources")

			kernel2Spec := types.NewDecimalSpec(3000, 12000, 2, 8)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the first committed resources")

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources(kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, kernel1Id)
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal(kernel1Id))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel1Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel1Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel1Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel1Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the second committed resources")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel2",
				scheduling.DefaultExecutionId, kernel2Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			kernel1And2Spec := kernel1Spec.Add(kernel2Spec)

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1And2Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1And2Spec)).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(4))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(4))

			Expect(allocationManager.KernelHasCommittedResources("Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel2")).To(BeTrue())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel2")).To(BeFalse())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel2")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel2"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel2Spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel2Spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel2Spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel2Spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel2Spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsPending()).To(BeFalse())

			By("Correctly handling the scheduling of the third pending resources")

			kernel3spec := types.NewDecimalSpec(2000, 0, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel3", kernel3spec)

			Expect(err).To(BeNil())
			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(3))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(2))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the third committed resources due to lack of available CPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel3",
				scheduling.DefaultExecutionId, kernel3spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			GinkgoWriter.Printf("Error: %v\n", err)

			var insufficientResourcesError scheduling.InsufficientResourcesError
			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.CPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel3spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel3")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel3")).To(BeFalse()) // Spec 0 GPUs

			allocation, ok = allocationManager.GetAllocation(1, "Kernel3")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel3"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel3spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel3spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel3spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel3spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel3spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fourth pending resources")

			kernel4spec := types.NewDecimalSpec(0, 0, 6, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel4", kernel4spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly rejecting the scheduling of the fourth committed resources due to lack of available GPU")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel4",
				scheduling.DefaultExecutionId, kernel4spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel4spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			Expect(allocationManager.KernelHasCommittedResources("Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedGPUs(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, "Kernel4")).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, "Kernel4")).To(BeTrue())

			allocation, ok = allocationManager.GetAllocation(1, "Kernel4")
			Expect(ok).To(BeTrue())
			Expect(allocation).ToNot(BeNil())
			Expect(allocation.GetKernelId()).To(Equal("Kernel4"))
			Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
			Expect(allocation.GetMillicpus()).To(Equal(kernel4spec.CPU()))
			Expect(allocation.GetMemoryMb()).To(Equal(kernel4spec.MemoryMB()))
			Expect(allocation.GetGpus()).To(Equal(kernel4spec.GPU()))
			Expect(allocation.GetVramGb()).To(Equal(kernel4spec.VRAM()))
			Expect(allocation.ToDecimalSpec().Equals(kernel4spec)).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsPending()).To(BeTrue())

			By("Correctly handling the scheduling of the fifth pending resources")

			kernel5spec := types.NewDecimalSpec(0, 64000, 0, 0)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel5", kernel5spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the fifth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel5",
				scheduling.DefaultExecutionId, kernel5spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.Memory))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel5spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the sixth pending resources")

			kernel6spec := types.NewDecimalSpec(0, 0, 0, 32)
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel6", kernel6spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the sixth committed resources due to lack of available memory")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel6",
				scheduling.DefaultExecutionId, kernel6spec, false)
			Expect(err).ToNot(BeNil())
			Expect(allocatedGpuResourceIds).To(BeNil())

			ok = errors.As(err, &insufficientResourcesError)
			Expect(ok).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())

			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.VRAM))
			Expect(insufficientResourcesError.RequestedResources).To(Equal(kernel6spec))
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))

			By("Correctly handling the scheduling of the seventh pending resources")

			kernel7spec := hostSpec.Clone()
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel7", kernel7spec)
			Expect(err).To(BeNil())

			By("Correctly rejecting the scheduling of the seventh committed resources due to lack of availability for all resource types")

			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, "Kernel7",
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
			Expect(insufficientResourcesError.AvailableResources).To(Equal(allocationManager.IdleResources()))
		})

		It("Will correctly adjust a lone pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle evicting a kernel replica with pending resources", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeTrue())

			err = allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(0))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasCommittedResources(1, kernel1Id)).To(BeFalse())
			Expect(allocationManager.ReplicaHasPendingGPUs(1, kernel1Id)).To(BeFalse())
		})

		It("Will correctly return an error when trying to evict a non-existent kernel replica", func() {
			err := allocationManager.ReplicaEvicted(1, kernel1Id)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly handle deallocating committed resources from a kernel replica", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(6))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec)).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
			Expect(allocationManager.NumCommittedGpuDevices()).To(Equal(0))
			Expect(allocationManager.NumAvailableGpuDevices()).To(Equal(8))
		})

		// Commented-out:
		//
		// For now, we allow scheduling replicas with pending requests that are too big.
		//
		It("Will fail to allocate pending resources for a request it cannot satisfy", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 10, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).ToNot(BeNil())

			var insufficientResourcesError scheduling.InsufficientResourcesError
			Expect(errors.As(err, &insufficientResourcesError)).To(BeTrue())
			Expect(insufficientResourcesError).ToNot(BeNil())
			Expect(len(insufficientResourcesError.OffendingResourceKinds)).To(Equal(1))
			Expect(insufficientResourcesError.OffendingResourceKinds[0]).To(Equal(scheduling.GPU))
			Expect(hostSpec.Equals(insufficientResourcesError.AvailableResources)).To(BeTrue())
			Expect(kernel1Spec.Equals(insufficientResourcesError.RequestedResources)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(10)
			Expect(err).To(BeNil())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(10)

			Expect(allocationManager.SpecResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly handle adjusting its spec GPUs and then failing to schedule a kernel", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 5, 8)
			err := allocationManager.AdjustSpecGPUs(4)
			Expect(err).To(BeNil())

			updatedResourceManagerSpec := hostSpec.CloneDecimalSpec()
			updatedResourceManagerSpec.UpdateSpecGPUs(4)
			Expect(allocationManager.IdleResources().Equals(updatedResourceManagerSpec)).To(BeTrue())

			err = allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
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

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			err = allocationManager.AdjustSpecGPUs(1)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrIllegalResourceAdjustment)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a non-existent kernel replica", func() {
			err := allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationRequest)).To(BeTrue())
		})

		It("Will correctly return an error when trying to release committed resources from a kernel replica that has only pending resources allocated to it", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1Spec))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			err = allocationManager.ReleaseCommittedResources(1, kernel1Id, scheduling.DefaultExecutionId)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrInvalidAllocationType)).To(BeTrue())
		})

		It("Will correctly adjust a lone pending resource reservation to a smaller reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(2000, 8000, 1, 4)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(1))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will correctly adjust a pending resource reservation to a larger reservation", func() {
			kernel1SpecV1 := types.NewDecimalSpec(4000, 16000, 2, 8)
			kernel2Spec := types.NewDecimalSpec(3000, 1532, 3, 18)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1SpecV1)
			Expect(err).To(BeNil())
			err = allocationManager.ContainerStartedRunningOnHost(1, "Kernel2", kernel2Spec)
			Expect(err).To(BeNil())

			Expect(allocationManager.SpecResources().Equals(hostSpec)).To(BeTrue())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV1.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).To(BeNil())

			Expect(allocationManager.NumPendingAllocations()).To(Equal(2))
			Expect(allocationManager.NumAllocations()).To(Equal(2))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

			Expect(allocationManager.PendingResources().Equals(kernel1SpecV2.Add(kernel2Spec)))
			Expect(allocationManager.IdleResources().Equals(hostSpec)).To(BeTrue())
			Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())
		})

		It("Will fail to adjust a resource request that is already committed", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)

			err := allocationManager.ContainerStartedRunningOnHost(1, kernel1Id, kernel1Spec)
			Expect(err).To(BeNil())

			var allocatedGpuResourceIds []int
			allocatedGpuResourceIds, err = allocationManager.CommitResourcesToExistingContainer(1, kernel1Id,
				scheduling.DefaultExecutionId, kernel1Spec, false)
			Expect(err).To(BeNil())
			Expect(allocatedGpuResourceIds).ToNot(BeNil())
			Expect(len(allocatedGpuResourceIds)).To(Equal(2))

			Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
			Expect(allocationManager.NumAllocations()).To(Equal(1))
			Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

			Expect(allocationManager.PendingResources().IsZero()).To(BeTrue())
			Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(kernel1Spec))).To(BeTrue())
			Expect(allocationManager.CommittedResources().Equals(kernel1Spec)).To(BeTrue())

			kernel1SpecV2 := types.NewDecimalSpec(8000, 32000, 4, 16)

			err = allocationManager.AdjustPendingResources(1, kernel1Id, kernel1SpecV2)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, scheduling.ErrResourcesAlreadyCommitted)).To(BeTrue())
		})

		It("Will fail to adjust a resource request that does not exist", func() {
			kernel1Spec := types.NewDecimalSpec(4000, 16000, 2, 8)
			err := allocationManager.AdjustPendingResources(1, kernel1Id, kernel1Spec)
			Expect(err).ToNot(BeNil())
			Expect(errors.Is(err, resource.ErrAllocationNotFound)).To(BeTrue())
		})
	})
})
