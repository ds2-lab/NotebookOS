package test_device

import (
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/scheduler/device"
	"github.com/zhangjyr/distributed-notebook/scheduler/mock_device"
	"k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Return the first n device IDs.
// Panics if n is greater than the size of the Devices map.
func firstNDeviceIDs(d device.Devices, n int) []string {
	if n > d.Size() {
		panic("requested too many devices")
	}

	deviceIDs := make([]string, 0, n)
	for deviceID := range d {
		deviceIDs = append(deviceIDs, deviceID)
	}

	return deviceIDs
}

var _ = Describe("Allocator Tests", func() {
	var (
		allocator    device.VirtualGpuAllocator
		allDevices   device.Devices
		opts         *device.VirtualGpuPluginServerOptions
		mockCtrl     *gomock.Controller
		mockPodCache *mock_device.MockPodCache
		nodeName     string = "TestNode"
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockPodCache = mock_device.NewMockPodCache(mockCtrl)

		opts = &device.VirtualGpuPluginServerOptions{
			NumVirtualGPUs:   64,
			DevicePluginPath: "/var/lib/kubelet/device-plugins/",
		}

		allocator = device.NewVirtualGpuAllocatorForTesting(opts, nodeName, mockPodCache)
		allDevices = allocator.GetDevices()
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	It("should allocate resources using the public Allocate DevicePlugin API", func() {
		req := &v1beta1.AllocateRequest{
			ContainerRequests: []*v1beta1.ContainerAllocateRequest{
				{
					DevicesIDs: firstNDeviceIDs(allDevices, 16),
				},
			},
		}

		resp, err := allocator.Allocate(req)

		Expect(err).To(BeNil())
		Expect(resp).ToNot(BeNil())
	})
})
