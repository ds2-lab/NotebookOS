package index_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/types"
	"go.uber.org/mock/gomock"
)

var (
	emptyBlacklist = make([]interface{}, 0)
)

var _ = Describe("MultiIndex Tests", func() {
	var (
		mockCtrl      *gomock.Controller
		mockCluster   *mock_scheduling.MockCluster
		mockScheduler *mock_scheduling.MockScheduler
		mockPolicy    *mock_scheduling.MockPolicy
	)

	hostSpec := types.NewDecimalSpec(64000, 128000, 8, 40)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockCluster = mock_scheduling.NewMockCluster(mockCtrl)
		mockScheduler = mock_scheduling.NewMockScheduler(mockCtrl)
		mockCluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)
		mockScheduler.EXPECT().Policy().AnyTimes().Return(mockPolicy)
		mockPolicy = mock_scheduling.NewMockPolicy(mockCtrl)

		mockPolicy.EXPECT().ResourceBindingMode().AnyTimes().Return(scheduling.BindResourcesWhenContainerScheduled)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Multi-Index of LeastLoadedIndex HostPools", func() {
		Context("Adding and Removing Hosts", func() {
			Context("Empty Hosts", func() {
				It("Will handle a single add operation correctly", func() {
					multiIndex := index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
					multiIndex.AddHost(host1)
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

					host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
					multiIndex.AddHost(host1)
					Expect(multiIndex.Len()).To(Equal(1))

					ret, _, err := multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host1))
					Expect(multiIndex.Len()).To(Equal(1))

					multiIndex.RemoveHost(host1)
					Expect(multiIndex.Len()).To(Equal(0))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(ret).To(BeNil())
				})

				It("Will handle multiple add and remove operations correctly", func() {
					multiIndex := index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
					multiIndex.AddHost(host1)
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

					host2 := createHost(2, mockCtrl, mockCluster, hostSpec)
					multiIndex.AddHost(host2)
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

					multiIndex.RemoveHost(host1)
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
					multiIndex.RemoveHost(host2)
					Expect(multiIndex.Len()).To(Equal(0))

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{4})
					Expect(ret).To(BeNil())
				})
			})

			Context("Non-Empty Hosts", func() {
				var (
					multiIndex *index.MultiIndex[*index.LeastLoadedIndex]
					host1      scheduling.UnitTestingHost
					host2      scheduling.UnitTestingHost
					host3      scheduling.UnitTestingHost
				)

				BeforeEach(func() {
					multiIndex = index.NewMultiIndex[*index.LeastLoadedIndex](int32(hostSpec.GPU()+1), index.NewLeastLoadedIndexWrapper)
					Expect(multiIndex).ToNot(BeNil())

					host1 = createHost(1, mockCtrl, mockCluster, hostSpec)
					host2 = createHost(2, mockCtrl, mockCluster, hostSpec)
					host3 = createHost(3, mockCtrl, mockCluster, hostSpec)

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

					multiIndex.AddHost(host1)
					multiIndex.AddHost(host2)
					multiIndex.AddHost(host3)

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

					multiIndex.RemoveHost(host1)
					Expect(multiIndex.Len()).To(Equal(2))

					By("Correctly returning the 'new' least-loaded host")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host2))
					Expect(multiIndex.Len()).To(Equal(2))

					By("Correctly handling the removal of the least-loaded host")

					multiIndex.RemoveHost(host2)
					Expect(multiIndex.Len()).To(Equal(1))

					By("Correctly returning the 'new' least-loaded host")

					ret, _, err = multiIndex.Seek(emptyBlacklist, []float64{2})
					Expect(err).To(BeNil())
					Expect(ret).ToNot(BeNil())
					Expect(ret).To(Equal(host3))
					Expect(multiIndex.Len()).To(Equal(1))

					By("Correctly handling the removal of final remaining host")

					multiIndex.RemoveHost(host3)
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
