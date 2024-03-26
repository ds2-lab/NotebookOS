package test_device

import (
	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/scheduler/device"
	"github.com/zhangjyr/distributed-notebook/scheduler/mock_device"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Return the first n device IDs.
// Panics if n is greater than the size of the Devices map.
func firstNDeviceIDs(d device.Devices, n int) []string {
	if n > d.Size() {
		panic("requested too many devices")
	}

	deviceIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		deviceID := d.GetByIndex(i).ID
		deviceIDs = append(deviceIDs, deviceID)
	}

	return deviceIDs
}

func spoofPods(n int, podsWithVGPUs int, numVGPUs int) (device.StringSet, []corev1.Pod, map[string]*corev1.Pod) {
	activePodIDs := sets.New[string]()
	activePods := make([]corev1.Pod, 0, n)
	activePodsMap := make(map[string]*corev1.Pod)

	cpu, err := resource.ParseQuantity("100m")
	if err != nil {
		panic(err)
	}

	mem, err := resource.ParseQuantity("100Mi")
	if err != nil {
		panic(err)
	}

	vgpu, err := resource.ParseQuantity(fmt.Sprintf("%d", numVGPUs))
	if err != nil {
		panic(err)
	}

	noVgpu, err := resource.ParseQuantity(fmt.Sprintf("%d", 0))
	if err != nil {
		panic(err)
	}

	for i := 0; i < podsWithVGPUs; i++ {
		pod := spoofPod(i+1, cpu, mem, vgpu)
		activePodIDs.Insert(string(pod.UID))
		activePods = append(activePods, pod)
		activePodsMap[string(pod.UID)] = &pod
	}

	for i := podsWithVGPUs; i < n; i++ {
		pod := spoofPod(i+1, cpu, mem, noVgpu)
		activePodIDs.Insert(string(pod.UID))
		activePods = append(activePods, pod)
		activePodsMap[string(pod.UID)] = &pod
	}

	return activePodIDs, activePods, activePodsMap
}

func spoofPod(id int, cpu resource.Quantity, mem resource.Quantity, vgpu resource.Quantity) corev1.Pod {
	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("pod-%d", id),
			Namespace: "default",
			UID:       types.UID(uuid.NewString()),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "nginx",
					Image:           "nginx",
					ImagePullPolicy: "Always",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"cpu":                    cpu,
							"mem":                    mem,
							device.VDeviceAnnotation: vgpu,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	return pod
}

// Return the Pods from the slice that use GPUs.
func getGpuPods(pods []corev1.Pod) []corev1.Pod {
	gpuPods := make([]corev1.Pod, 0)

	for _, pod := range pods {
		if device.PodRequiresVirtualGPUs(&pod) {
			gpuPods = append(gpuPods, pod)
		}
	}

	return gpuPods
}

var _ = Describe("Allocator Tests", func() {
	config.LogLevel = logger.LOG_LEVEL_ALL

	var (
		allocator      device.VirtualGpuAllocator
		allDevices     device.Devices
		opts           *device.VirtualGpuPluginServerOptions
		mockCtrl       *gomock.Controller
		mockPodCache   *mock_device.MockPodCache
		nodeName       string = "TestNode"
		numSpoofedPods int
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockPodCache = mock_device.NewMockPodCache(mockCtrl)

		numSpoofedPods = 4
		activePodIDs, activePods, _ := spoofPods(numSpoofedPods, 1, 4)

		mockPodCache.EXPECT().GetActivePodIDs().Return(activePodIDs).Times(1)
		// mockPodCache.EXPECT().GetActivePods().Return(activePodsMap).Times(1)
		mockPodCache.EXPECT().GetPodsRunningOnNode(nodeName, string(corev1.PodPending)).Return(activePods, nil).Times(2)

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
		requestSize := 4
		totalVirtualGPUs := 64

		req := &v1beta1.AllocateRequest{
			ContainerRequests: []*v1beta1.ContainerAllocateRequest{
				{
					DevicesIDs: firstNDeviceIDs(allDevices, requestSize),
				},
			},
		}

		Expect(allocator.NumVirtualGPUs()).To(Equal(totalVirtualGPUs))
		Expect(allocator.NumFreeVirtualGPUs()).To(Equal(totalVirtualGPUs))
		Expect(allocator.NumAllocatedVirtualGPUs()).To(BeZero())

		allPods, err := mockPodCache.GetPodsRunningOnNode(nodeName, string(corev1.PodPending))
		for idx, pod := range allPods {
			GinkgoWriter.Printf("Pods %d/%d '%s': %v\n\n", idx+1, len(allPods), pod.Name, pod)
		}
		Expect(len(allPods)).To(Equal(numSpoofedPods))
		Expect(err).To(BeNil())

		resp, err := allocator.Allocate(req)

		Expect(err).To(BeNil())
		Expect(resp).ToNot(BeNil())
		Expect(allocator.NumVirtualGPUs()).To(Equal(totalVirtualGPUs))
		Expect(allocator.NumFreeVirtualGPUs()).To(Equal(totalVirtualGPUs - requestSize))
		Expect(allocator.NumAllocatedVirtualGPUs()).To(Equal(requestSize))
		Expect(allocator.NumAllocations()).To(Equal(1))

		gpuPods := getGpuPods(allPods)
		Expect(len(gpuPods)).To(Equal(1))
		gpuPod := gpuPods[0]
		allocation, err := allocator.GetAllocationForPod(string(gpuPod.UID))
		Expect(err).To(BeNil())
		Expect(allocation).ToNot(BeNil())
		Expect(allocation.DeviceIDs).ToNot(BeNil())
		Expect(len(allocation.DeviceIDs)).To(Equal(requestSize))
		for idx, deviceId := range allocation.DeviceIDs {
			// The first `requestSize` Devices to be allocated should have IDs in the order that they were generated by the ResourceManager.
			prefix := fmt.Sprintf("Virtual-GPU-%d", idx)
			Expect(deviceId[0:len(prefix)]).To(Equal(prefix))
		}
	})
})
