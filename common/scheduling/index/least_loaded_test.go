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

var _ = Describe("LeastLoadedIndex Tests", func() {
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
		mockPolicy = mock_scheduling.NewMockPolicy(mockCtrl)
		mockScheduler = mock_scheduling.NewMockScheduler(mockCtrl)

		mockScheduler.EXPECT().Policy().AnyTimes().Return(mockPolicy)
		mockCluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)

		mockPolicy.EXPECT().ResourceBindingMode().AnyTimes().Return(scheduling.BindResourcesWhenContainerScheduled)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Adding and Removing Hosts", func() {
		Context("Empty Hosts", func() {
			It("Will handle a single add operation correctly", func() {
				leastLoadedIndex := index.NewLeastLoadedIndex()
				Expect(leastLoadedIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				leastLoadedIndex.AddHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				ret, _, err := leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(err).To(BeNil())
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				By("Succeeding during consecutive calls to Seek")

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(1))
			})

			It("Will handle an add followed by a remove correctly", func() {
				leastLoadedIndex := index.NewLeastLoadedIndex()
				Expect(leastLoadedIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				leastLoadedIndex.AddHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				ret, _, err := leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				leastLoadedIndex.RemoveHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(0))

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).To(BeNil())
			})

			It("Will handle multiple add and remove operations correctly", func() {
				leastLoadedIndex := index.NewLeastLoadedIndex()
				Expect(leastLoadedIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				leastLoadedIndex.AddHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				meta := host1.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				ret, _, err := leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				host2 := createHost(2, mockCtrl, mockCluster, hostSpec)
				leastLoadedIndex.AddHost(host2)
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(1)))

				ret, _, err = leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				By("Succeeding during consecutive calls to Seek")

				ret, _, err = leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				ret, _, err = leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				By("Correctly removing one of the two hosts")

				leastLoadedIndex.RemoveHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				ret, _, err = leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				By("Correctly removing the final host")

				meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				fmt.Printf("Removing final host.")
				leastLoadedIndex.RemoveHost(host2)
				Expect(leastLoadedIndex.Len()).To(Equal(0))

				ret, _, err = leastLoadedIndex.Seek([]interface{}{})
				Expect(err).To(BeNil())
				Expect(ret).To(BeNil())
			})
		})

		Context("Non-Empty Hosts", func() {
			var (
				leastLoadedIndex *index.LeastLoadedIndex
				host1            scheduling.UnitTestingHost
				host2            scheduling.UnitTestingHost
				host3            scheduling.UnitTestingHost
			)

			BeforeEach(func() {
				leastLoadedIndex = index.NewLeastLoadedIndex()
				Expect(leastLoadedIndex).ToNot(BeNil())

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

				leastLoadedIndex.AddHost(host1)
				leastLoadedIndex.AddHost(host2)
				leastLoadedIndex.AddHost(host3)
			})

			It("Will correctly handle multiple add and remove operations", func() {
				By("Correctly handling the addition of 3 hosts")
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				By("Correctly returning the least-loaded host")

				ret, _, err := leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				By("Correctly handling the removal of the least-loaded host")

				leastLoadedIndex.RemoveHost(host1)
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				By("Correctly returning the 'new' least-loaded host")

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(leastLoadedIndex.Len()).To(Equal(2))

				By("Correctly handling the removal of the least-loaded host")

				leastLoadedIndex.RemoveHost(host2)
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				By("Correctly returning the 'new' least-loaded host")

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host3))
				Expect(leastLoadedIndex.Len()).To(Equal(1))

				By("Correctly handling the removal of final remaining host")

				leastLoadedIndex.RemoveHost(host3)
				Expect(leastLoadedIndex.Len()).To(Equal(0))

				By("Correctly returning no hosts because the index is empty")

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).To(BeNil())
				Expect(leastLoadedIndex.Len()).To(Equal(0))
			})

			It("Will correctly update its order when the resources of hosts change", func() {
				By("Correctly handling the addition of 3 hosts")
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				// Host1 and Host2 will have equal resources, but "Host1" < "Host2" (i.e., compare the IDs), so
				// Host1 should still be returned upon seeking.
				By("Correctly sorting itself such that host1 is returned upon seeking, due to it having a 'lower' ID")

				err := host1.AddToCommittedResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())
				err = host1.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 2, 2))
				Expect(err).To(BeNil())

				leastLoadedIndex.Update(host1)

				ret, _, err := leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host2 is returned upon seeking")

				err = host1.AddToCommittedResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())
				err = host1.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())

				leastLoadedIndex.Update(host1)

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host1 is returned upon seeking")

				err = host2.AddToCommittedResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())
				err = host2.SubtractFromIdleResources(types.NewDecimalSpec(128, 256, 1, 2))
				Expect(err).To(BeNil())

				leastLoadedIndex.Update(host1)

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(leastLoadedIndex.Len()).To(Equal(3))

				By("Correctly sorting itself such that host3 is returned upon seeking")

				err = host3.SubtractFromCommittedResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())
				err = host3.AddToIdleResources(types.NewDecimalSpec(128, 256, 6, 2))
				Expect(err).To(BeNil())

				leastLoadedIndex.Update(host1)

				ret, _, err = leastLoadedIndex.Seek(make([]interface{}, 0))
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host3))
				Expect(leastLoadedIndex.Len()).To(Equal(3))
			})
		})
	})
})
