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

var (
	emptyBlacklist = make([]interface{}, 0)
)

var _ = Describe("MultiIndex Tests", func() {
	var (
		mockCtrl    *gomock.Controller
		mockCluster *mock_scheduling.MockCluster
		mockPolicy  *mock_scheduling.MockPolicy
	)

	hostSpec := types.NewDecimalSpec(64000, 128000, 8, 40)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockCluster = mock_scheduling.NewMockCluster(mockCtrl)
		mockPolicy = mock_scheduling.NewMockPolicy(mockCtrl)

		mockPolicy.EXPECT().ResourceScalingPolicy().AnyTimes().Return(scheduling.BindResourcesWhenContainerScheduled)
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
			SpecGPUs:              int32(hostSpec.GPU() + 1),
			IdleGPUs:              int32(hostSpec.GPU() + 1),
			CommittedGPUs:         0,
			PendingGPUs:           0,
			NumPendingAllocations: 0,
			NumAllocations:        0,
			GpuSchedulerID:        uuid.NewString(),
			LocalDaemonID:         hostId,
		}, nil)

		host, err := entity.NewHost(hostId, "0.0.0.0", scheduling.MillicpusPerHost,
			scheduling.MemoryMbPerHost, scheduling.VramPerHostGb, 3, mockCluster, mockCluster,
			nil, localGatewayClient, mockPolicy,
			func(_ string, _ string, _ string, _ string) error { return nil })

		Expect(host).ToNot(BeNil())
		Expect(err).To(BeNil())

		return host
	}

	Context("Multi-Index of LeastLoadedIndex HostPools", func() {
		Context("Adding and Removing Hosts", func() {
			Context("Empty Hosts", func() {
				It("Will handle a single add operation correctly", func() {
					multiIndex := index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 := createHost(1)
					multiIndex.Add(host1)
					Expect(multiIndex.Len()).To(Equal(1))

					ret, _, err := multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))

					By("Succeeding during consecutive calls to Seek")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))
				})

				It("Will handle an add followed by a remove correctly", func() {
					multiIndex := index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 := createHost(1)
					multiIndex.Add(host1)
					Expect(multiIndex.Len()).To(Equal(1))

					ret, _, err := multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))

					multiIndex.Remove(host1)
					Expect(multiIndex.Len()).To(Equal(0))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(ret).To(BeNil())
				})

				It("Will handle multiple add and remove operations correctly", func() {
					multiIndex := index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 := createHost(1)
					multiIndex.Add(host1)
					Expect(multiIndex.Len()).To(Equal(1))

					meta := host1.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).To(BeNil())

					ret, _, err := multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))

					meta = host1.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).ToNot(BeNil())
					Expect(meta.(int32)).To(Equal(int32(0)))

					host2 := createHost(2)
					multiIndex.Add(host2)
					Expect(multiIndex.Len()).To(Equal(2))

					meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).To(BeNil())

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(2))

					meta = host1.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).ToNot(BeNil())
					Expect(meta.(int32)).To(Equal(int32(0)))

					meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).To(BeNil())

					By("Succeeding during consecutive calls to Seek")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(2))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(2))

					By("Correctly removing one of the two hosts")

					multiIndex.Remove(host1)
					Expect(multiIndex.Len()).To(Equal(1))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host2))
					Expect(multiIndex.Len()).To(Equal(1))

					By("Correctly removing the final host")

					meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
					Expect(meta).ToNot(BeNil())
					Expect(meta.(int32)).To(Equal(int32(0)))

					fmt.Printf("Removing final host.")
					multiIndex.Remove(host2)
					Expect(multiIndex.Len()).To(Equal(0))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(ret).To(BeNil())
				})
			})

			Context("Non-Empty Hosts", func() {
				var (
					multiIndex *index.MultiIndex[*index.LeastLoadedIndex]
					host1      scheduling.Host
					host2      scheduling.Host
					host3      scheduling.Host
				)

				BeforeEach(func() {
					multiIndex = index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

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

					multiIndex.Add(host1)
					multiIndex.Add(host2)
					multiIndex.Add(host3)

					Expect(multiIndex.Len()).To(Equal(3))
					Expect(multiIndex.NumFreeHosts()).To(Equal(3))
				})

				It("Will correctly handle multiple add and remove operations", func() {
					By("Correctly handling the addition of 3 hosts")
					Expect(multiIndex.Len()).To(Equal(3))

					By("Correctly returning the least-loaded host")

					ret, _, err := multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(3))

					By("Correctly handling the removal of the least-loaded host")

					multiIndex.Remove(host1)
					Expect(multiIndex.Len()).To(Equal(2))

					By("Correctly returning the 'new' least-loaded host")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host2))
					Expect(multiIndex.Len()).To(Equal(2))

					By("Correctly handling the removal of the least-loaded host")

					multiIndex.Remove(host2)
					Expect(multiIndex.Len()).To(Equal(1))

					By("Correctly returning the 'new' least-loaded host")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host3))
					Expect(multiIndex.Len()).To(Equal(1))

					By("Correctly handling the removal of final remaining host")

					multiIndex.Remove(host3)
					Expect(multiIndex.Len()).To(Equal(0))

					By("Correctly returning no hosts because the index is empty")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(ret).To(BeNil())
					Expect(multiIndex.Len()).To(Equal(0))
				})
			})
		})
	})
})
