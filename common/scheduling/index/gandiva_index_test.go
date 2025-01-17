package index_test

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/types"
	"go.uber.org/mock/gomock"
)

var _ = Describe("GandivaIndex Tests", func() {
	var (
		mockCtrl    *gomock.Controller
		mockCluster *mock_scheduling.MockCluster
	)

	hostSpec := types.NewDecimalSpec(64000, 128000, 8, 40)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockCluster = mock_scheduling.NewMockCluster(mockCtrl)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	createHost := func(idx int) scheduling.Host {
		localGatewayClient := mock_proto.NewMockLocalGatewayClient(mockCtrl)

		hostId := fmt.Sprintf("Host%d", idx)
		nodeName := fmt.Sprintf("Host%d", idx)

		localGatewayClient.EXPECT().SetID(
			gomock.Any(),
			&proto.HostId{
				Id: hostId,
			},
		).Return(&proto.HostId{
			Id:       hostId,
			NodeName: nodeName,
		}, nil)

		localGatewayClient.EXPECT().GetActualGpuInfo(
			gomock.Any(),
			&proto.Void{},
		).Return(&proto.GpuInfo{
			SpecGPUs:              int32(hostSpec.GPU()),
			IdleGPUs:              int32(hostSpec.GPU()),
			CommittedGPUs:         0,
			PendingGPUs:           0,
			NumPendingAllocations: 0,
			NumAllocations:        0,
			GpuSchedulerID:        uuid.NewString(),
			LocalDaemonID:         hostId,
		}, nil)

		host, err := entity.NewHost(hostId, "0.0.0.0", scheduling.MillicpusPerHost,
			scheduling.MemoryMbPerHost, scheduling.VramPerHostGb, 3, mockCluster, mockCluster,
			nil, localGatewayClient, scheduling.BindResourcesWhenContainerScheduled,
			func(_ string, _ string, _ string, _ string) error { return nil })

		Expect(host).ToNot(BeNil())
		Expect(err).To(BeNil())

		return host
	}

	Context("Adding and Removing Hosts", func() {
		Context("Empty Hosts", func() {
			It("Will handle a single add operation correctly", func() {
				gandivaIndex := index.NewGandivaIndex(int32(hostSpec.GPU()))
				Expect(gandivaIndex).ToNot(BeNil())

				host1 := createHost(1)
				gandivaIndex.Add(host1)
				Expect(gandivaIndex.Len()).To(Equal(1))

				ret, _ := gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(1))

				By("Succeeding during consecutive calls to Seek")

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(1))

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(1))
			})

			It("Will handle an add followed by a remove correctly", func() {
				gandivaIndex := index.NewGandivaIndex(int32(hostSpec.GPU()))
				Expect(gandivaIndex).ToNot(BeNil())

				host1 := createHost(1)
				gandivaIndex.Add(host1)
				Expect(gandivaIndex.Len()).To(Equal(1))

				ret, _ := gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(1))

				gandivaIndex.Remove(host1)
				Expect(gandivaIndex.Len()).To(Equal(0))

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).To(BeNil())
			})

			It("Will handle multiple add and remove operations correctly", func() {
				gandivaIndex := index.NewGandivaIndex(int32(hostSpec.GPU()))
				Expect(gandivaIndex).ToNot(BeNil())

				host1 := createHost(1)
				gandivaIndex.Add(host1)
				Expect(gandivaIndex.Len()).To(Equal(1))

				meta := host1.GetMeta(index.HostMetaGandivaIndexPos)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				ret, _ := gandivaIndex.Seek([]interface{}{})
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(1))

				host2 := createHost(2)
				gandivaIndex.Add(host2)
				Expect(gandivaIndex.Len()).To(Equal(2))

				meta = host2.GetMeta(index.HostMetaGandivaIndexPos)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(1)))

				ret, _ = gandivaIndex.Seek([]interface{}{})
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(2))

				By("Succeeding during consecutive calls to Seek")

				ret, _ = gandivaIndex.Seek([]interface{}{})
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(2))

				ret, _ = gandivaIndex.Seek([]interface{}{})
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(2))

				By("Correctly removing one of the two hosts")

				gandivaIndex.Remove(host1)
				Expect(gandivaIndex.Len()).To(Equal(1))

				ret, _ = gandivaIndex.Seek([]interface{}{})
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(gandivaIndex.Len()).To(Equal(1))

				By("Correctly removing the final host")

				meta = host2.GetMeta(index.HostMetaGandivaIndexPos)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				fmt.Printf("Removing final host.")
				gandivaIndex.Remove(host2)
				Expect(gandivaIndex.Len()).To(Equal(0))

				ret, _ = gandivaIndex.Seek([]interface{}{})
				Expect(ret).To(BeNil())
			})
		})

		Context("Non-Empty Hosts", func() {
			var (
				gandivaIndex *index.GandivaIndex
				host1        scheduling.Host
				host2        scheduling.Host
				host3        scheduling.Host
			)

			BeforeEach(func() {
				gandivaIndex = index.NewGandivaIndex(int32(hostSpec.GPU()))
				Expect(gandivaIndex).ToNot(BeNil())

				host1 = createHost(1)
				host2 = createHost(2)
				host3 = createHost(3)

				err := host1.AddToCommittedResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())
				err = host1.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())

				err = host2.AddToCommittedResources(types.NewDecimalSpec(128, 256, 4, 2))
				Expect(err).To(BeNil())
				err = host2.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 4, 2))
				Expect(err).To(BeNil())

				err = host3.AddToCommittedResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())
				err = host3.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())

				gandivaIndex.Add(host1)
				gandivaIndex.Add(host2)
				gandivaIndex.Add(host3)
			})

			It("Will correctly handle multiple add and remove operations", func() {
				By("Correctly handling the addition of 3 hosts")
				Expect(gandivaIndex.Len()).To(Equal(3))

				By("Correctly returning the least-loaded host")

				ret, _ := gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(3))

				By("Correctly handling the removal of the least-loaded host")

				gandivaIndex.Remove(host1)
				Expect(gandivaIndex.Len()).To(Equal(2))

				By("Correctly returning the 'new' least-loaded host")

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(gandivaIndex.Len()).To(Equal(2))

				By("Correctly handling the removal of the least-loaded host")

				gandivaIndex.Remove(host2)
				Expect(gandivaIndex.Len()).To(Equal(1))

				By("Correctly returning the 'new' least-loaded host")

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host3))
				Expect(gandivaIndex.Len()).To(Equal(1))

				By("Correctly handling the removal of final remaining host")

				gandivaIndex.Remove(host3)
				Expect(gandivaIndex.Len()).To(Equal(0))

				By("Correctly returning no hosts because the index is empty")

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).To(BeNil())
				Expect(gandivaIndex.Len()).To(Equal(0))
			})

			It("Will correctly update its order when the resources of hosts change", func() {
				By("Correctly handling the addition of 3 hosts")
				Expect(gandivaIndex.Len()).To(Equal(3))

				// Host1 and Host2 will have equal resources, but "Host1" < "Host2" (i.e., compare the IDs), so
				// Host1 should still be returned upon seeking.
				By("Correctly sorting itself such that host1 is returned upon seeking, due to it having a 'lower' ID")

				err := host1.AddToCommittedResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())
				err = host1.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())

				gandivaIndex.Update(host1)

				ret, _ := gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host2 is returned upon seeking")

				err = host1.AddToCommittedResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())
				err = host1.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())

				gandivaIndex.Update(host1)

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(gandivaIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host1 is returned upon seeking")

				err = host2.AddToCommittedResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())
				err = host2.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())

				gandivaIndex.Update(host1)

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(gandivaIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host3 is returned upon seeking")

				err = host3.SubtractFromCommittedResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())
				err = host3.AddToIdleResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())

				gandivaIndex.Update(host1)

				ret, _ = gandivaIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host3))
				Expect(gandivaIndex.Len()).To(Equal(3))
			})
		})
	})
})
