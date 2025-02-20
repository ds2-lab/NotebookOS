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
	"slices"
)

var _ = Describe("Static Index Tests", func() {
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

	It("Will be instantiated correctly", func() {
		staticIndex := index.NewStaticMultiIndex(int32(hostSpec.GPU()))
		Expect(staticIndex).ToNot(BeNil())

		expectedHostPoolIds := []int32{1, 2, 4, 8}

		Expect(staticIndex.NumPools).To(Equal(int32(4)))
		Expect(staticIndex.GetNumPools()).To(Equal(4))

		actualHostPoolIds := staticIndex.HostPoolIDs()
		Expect(actualHostPoolIds).ToNot(BeNil())
		Expect(len(actualHostPoolIds)).To(Equal(len(expectedHostPoolIds)))

		Expect(expectedHostPoolIds).To(Equal(actualHostPoolIds))
		for _, expectedId := range expectedHostPoolIds {
			Expect(slices.Contains(actualHostPoolIds, expectedId)).To(BeTrue())
		}

		for _, expectedId := range expectedHostPoolIds {
			GinkgoWriter.Printf("Loading HostPool %d\n", expectedId)
			pool, loaded := staticIndex.GetHostPoolByIndex(expectedId)
			Expect(loaded).To(BeTrue())
			Expect(pool).ToNot(BeNil())
			Expect(pool.Len()).To(Equal(0))

			Expect(staticIndex.NumHostsInPoolByIndex(expectedId)).To(Equal(0))
		}
	})

	Context("Adding and Removing Hosts", func() {
		Context("Empty Hosts", func() {
			It("Will handle a single add operation correctly", func() {
				staticIndex := index.NewStaticMultiIndex(int32(hostSpec.GPU()))
				Expect(staticIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				staticIndex.AddHost(host1)
				Expect(staticIndex.Len()).To(Equal(1))

				ret, _, err := staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(1))

				By("Succeeding during consecutive calls to Seek")

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(1))

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(1))
			})

			It("Will handle an add followed by a remove correctly", func() {
				staticIndex := index.NewStaticMultiIndex(int32(hostSpec.GPU()))
				Expect(staticIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				staticIndex.AddHost(host1)
				Expect(staticIndex.Len()).To(Equal(1))

				ret, _, err := staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(1))

				staticIndex.RemoveHost(host1)
				Expect(staticIndex.Len()).To(Equal(0))

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(ret).To(BeNil())
			})

			It("Will handle multiple add and remove operations correctly", func() {
				staticIndex := index.NewStaticMultiIndex(int32(hostSpec.GPU()))
				Expect(staticIndex).ToNot(BeNil())

				host1 := createHost(1, mockCtrl, mockCluster, hostSpec)
				staticIndex.AddHost(host1)
				Expect(staticIndex.Len()).To(Equal(1))

				meta := host1.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).To(BeNil())

				ret, _, err := staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(1))

				meta = host1.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				host2 := createHost(2, mockCtrl, mockCluster, hostSpec)
				staticIndex.AddHost(host2)
				Expect(staticIndex.Len()).To(Equal(2))

				meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).To(BeNil())

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(2))

				meta = host1.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).To(BeNil())

				By("Succeeding during consecutive calls to Seek")

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(2))

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(2))

				By("Correctly removing one of the two hosts")

				staticIndex.RemoveHost(host1)
				Expect(staticIndex.Len()).To(Equal(1))

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(staticIndex.Len()).To(Equal(1))

				By("Correctly removing the final host")

				meta = host2.GetMeta(index.LeastLoadedIndexMetadataKey)
				Expect(meta).ToNot(BeNil())
				Expect(meta.(int32)).To(Equal(int32(0)))

				fmt.Printf("Removing final host.")
				staticIndex.RemoveHost(host2)
				Expect(staticIndex.Len()).To(Equal(0))

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{4})
				Expect(ret).To(BeNil())
			})
		})

		It("Will correctly compute values GetStaticMultiIndexBucket", func() {
			gpusPerHost := int32(8)

			Expect(index.GetStaticMultiIndexBucket(0, gpusPerHost)).To(Equal(int32(8)))
			Expect(index.GetStaticMultiIndexBucket(1, gpusPerHost)).To(Equal(int32(8)))
			Expect(index.GetStaticMultiIndexBucket(2, gpusPerHost)).To(Equal(int32(4)))
			Expect(index.GetStaticMultiIndexBucket(3, gpusPerHost)).To(Equal(int32(2)))
			Expect(index.GetStaticMultiIndexBucket(4, gpusPerHost)).To(Equal(int32(2)))
			Expect(index.GetStaticMultiIndexBucket(5, gpusPerHost)).To(Equal(int32(1)))
			Expect(index.GetStaticMultiIndexBucket(6, gpusPerHost)).To(Equal(int32(1)))
			Expect(index.GetStaticMultiIndexBucket(7, gpusPerHost)).To(Equal(int32(1)))
			Expect(index.GetStaticMultiIndexBucket(8, gpusPerHost)).To(Equal(int32(1)))
		})

		Context("Non-Empty Hosts", func() {
			var (
				staticIndex *index.StaticMultiIndex
				host1       scheduling.UnitTestingHost
				host2       scheduling.UnitTestingHost
				host3       scheduling.UnitTestingHost
			)

			BeforeEach(func() {
				staticIndex = index.NewStaticMultiIndex(int32(hostSpec.GPU()))
				Expect(staticIndex).ToNot(BeNil())

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

				staticIndex.AddHost(host1)
				staticIndex.AddHost(host2)
				staticIndex.AddHost(host3)

				Expect(staticIndex.Len()).To(Equal(3))
				Expect(staticIndex.NumFreeHosts()).To(Equal(3))
			})

			It("Will correctly handle multiple add and remove operations", func() {
				By("Correctly handling the addition of 3 hosts")
				Expect(staticIndex.Len()).To(Equal(3))

				By("Correctly returning the least-loaded host")

				ret, _, err := staticIndex.Seek(emptyBlacklist, []float64{2})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host1))
				Expect(staticIndex.Len()).To(Equal(3))

				By("Correctly handling the removal of the least-loaded host")

				staticIndex.RemoveHost(host1)
				Expect(staticIndex.Len()).To(Equal(2))

				By("Correctly returning the 'new' least-loaded host")

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{2})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host2))
				Expect(staticIndex.Len()).To(Equal(2))

				By("Correctly handling the removal of the least-loaded host")

				staticIndex.RemoveHost(host2)
				Expect(staticIndex.Len()).To(Equal(1))

				By("Correctly returning the 'new' least-loaded host")

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{2})
				Expect(err).To(BeNil())
				Expect(ret).ToNot(BeNil())
				Expect(ret).To(Equal(host3))
				Expect(staticIndex.Len()).To(Equal(1))

				By("Correctly handling the removal of final remaining host")

				staticIndex.RemoveHost(host3)
				Expect(staticIndex.Len()).To(Equal(0))

				By("Correctly returning no hosts because the index is empty")

				ret, _, err = staticIndex.Seek(emptyBlacklist, []float64{2})
				Expect(ret).To(BeNil())
				Expect(staticIndex.Len()).To(Equal(0))
			})
		})
	})
})
