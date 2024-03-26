package device

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var (
	ErrAllocationNotFound = errors.New("could not find an allocation associated with the specified pod")
)

type virtualGpuAllocatorImpl struct {
	sync.Mutex

	log logger.Logger

	kubeClient kubernetes.Interface
	nodeName   string

	opts *VirtualGpuPluginServerOptions

	resourceManager ResourceManager
	stopChan        chan interface{}

	podCache PodCache

	// Mapping from PodID to its allocation.
	allocations map[string]*gateway.VirtualGpuAllocation
}

// Creates a new virtualGpuAllocator using an out-of-cluster config for its Kubernetes client.
func NewVirtualGpuAllocatorForTesting(opts *VirtualGpuPluginServerOptions, nodeName string, podCache PodCache) VirtualGpuAllocator {
	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	} else {
		panic("Cannot find Kubernetes config!")
	}

	// use the current context in kubeconfig
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	return newVirtualGpuAllocatorImpl(opts, nodeName, podCache, kubeConfig)
}

func NewVirtualGpuAllocator(opts *VirtualGpuPluginServerOptions, nodeName string, podCache PodCache) VirtualGpuAllocator {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	return newVirtualGpuAllocatorImpl(opts, nodeName, podCache, kubeConfig)
}

func newVirtualGpuAllocatorImpl(opts *VirtualGpuPluginServerOptions, nodeName string, podCache PodCache, kubeConfig *rest.Config) VirtualGpuAllocator {
	allocator := &virtualGpuAllocatorImpl{
		opts:        opts,
		stopChan:    make(chan interface{}),
		allocations: make(map[string]*gateway.VirtualGpuAllocation),
		nodeName:    nodeName,
		podCache:    podCache,
	}
	config.InitLogger(&allocator.log, allocator)

	allocator.resourceManager = NewResourceManager(allocator.ResourceName(), opts.NumVirtualGPUs)

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(err.Error())
	}

	allocator.kubeClient = clientset

	return allocator
}

// Return the map of allocations, which is Pod UID -> allocation.
func (v *virtualGpuAllocatorImpl) getAllocations() map[string]*gateway.VirtualGpuAllocation {
	return v.allocations
}

// Return the Devices that are managed by this allocator and its underlying resource manager.
func (v *virtualGpuAllocatorImpl) GetDevices() Devices {
	return v.resourceManager.Devices()
}

func (v *virtualGpuAllocatorImpl) ResourceName() string {
	return VDeviceAnnotation
}

func (v *virtualGpuAllocatorImpl) apiDevices() []*pluginapi.Device {
	return v.resourceManager.Devices().GetPluginDevices()
}

// Return the total number of vGPUs.
func (v *virtualGpuAllocatorImpl) NumVirtualGPUs() int {
	return v.resourceManager.NumDevices()
}

// Return the number of vGPUs that are presently allocated.
func (v *virtualGpuAllocatorImpl) NumAllocatedVirtualGPUs() int {
	return v.resourceManager.NumAllocatedDevices()
}

// Return the number of vGPUs that are presently free/not allocated.
func (v *virtualGpuAllocatorImpl) NumFreeVirtualGPUs() int {
	return v.resourceManager.NumFreeDevices()
}

func (v *virtualGpuAllocatorImpl) stop() {
	close(v.stopChan)
	v.stopChan = nil
}

// Set the total number of vGPUs to a new value.
// This will return an error if the specified value is less than the number of currently-allocated vGPUs.
func (v *virtualGpuAllocatorImpl) SetTotalVirtualGPUs(value int32) error {
	v.Lock()
	defer v.Unlock()

	if value < int32(v.NumAllocatedVirtualGPUs()) {
		return ErrInvalidResourceAdjustment
	}

	// TODO(Ben): Modify the total number of virtual GPUs.
	return v.resourceManager.SetTotalNumDevices(value)
}

// Allocate is called during container creation so that the Device
// Plugin can run device specific operations and instruct Kubelet
// of the steps to make the Device available in the container
func (v *virtualGpuAllocatorImpl) Allocate(req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	v.Lock()
	defer v.Unlock()

	// Requests are always sent one-at-a-time.
	var request *pluginapi.ContainerAllocateRequest = req.ContainerRequests[0]

	v.log.Debug("virtualGpuPluginServerImpl::Allocate called. %d vGPU(s) requested.", len(request.DevicesIDs))
	klog.V(2).Infof("virtualGpuPluginServerImpl::Allocate called. %d vGPU(s) requested.", len(request.DevicesIDs))

	v.clearTerminatedPods()
	var candidatePod *corev1.Pod

	candidatePods, err := v.getCandidatePodsForAllocation()
	if err != nil {
		errorMessage := fmt.Sprintf("Failed to retrieve candidate pods for allocation because: %v", err)
		v.log.Error(errorMessage)
		klog.Error(errorMessage)
		return nil, fmt.Errorf(errorMessage)
	}

	var numVirtualGPUsRequested int32 = int32(len(request.DevicesIDs))
	for _, pod := range candidatePods {
		if _, ok := v.allocations[string(pod.UID)]; ok {
			v.log.Debug("Pod %s has already been allocated GPUs. Continuing our search.", string(pod.UID))
			klog.V(2).Infof("Pod %s has already been allocated GPUs. Continuing our search.", string(pod.UID))
			continue
		}

		if GetVirtualGpuRequirementsOfPod(pod) == numVirtualGPUsRequested {
			v.log.Debug("Found candidate Pod %s(%s) with requested vGPUs = %d.", string(pod.UID), pod.Name, numVirtualGPUsRequested)
			klog.V(2).Infof("Found candidate Pod %s(%s) with requested vGPUs = %d.", string(pod.UID), pod.Name, numVirtualGPUsRequested)
			candidatePod = pod
			break
		}
	}

	if candidatePod != nil {
		// Allocate resources to the Pod.
		resp, err := v.doAllocate(numVirtualGPUsRequested, candidatePod)

		if err == nil {
			responses := &pluginapi.AllocateResponse{
				ContainerResponses: []*pluginapi.ContainerAllocateResponse{resp},
			}
			v.log.Info("Returning the following value from virtualGpuPluginServerImpl::Allocate: %v", responses)
			klog.V(2).Infof("Returning the following value from virtualGpuPluginServerImpl::Allocate: %v", responses)
			return responses, nil
		} else {
			errorMessage := fmt.Sprintf("failed to allocate vGPUs to pod %s(%s) because: %v", string(candidatePod.UID), candidatePod.Name, err)
			v.log.Error(errorMessage)
			klog.Error(errorMessage)
			return nil, fmt.Errorf(errorMessage)
		}
	} else {
		errorMessage := fmt.Sprintf("could not find candidate Pod for request for %d vGPUs, allocation failed.", len(request.DevicesIDs))
		v.log.Error(errorMessage)
		klog.Error(errorMessage)
		return nil, fmt.Errorf(errorMessage)
	}
}

// This actually performs the allocation of GPUs to a particular pod.
func (v *virtualGpuAllocatorImpl) doAllocate(vgpusRequired int32, candidatePod *corev1.Pod) (*pluginapi.ContainerAllocateResponse, error) {
	v.log.Debug("Allocating %d vGPU(s) to Pod %s(%s) now.", vgpusRequired, candidatePod.UID, candidatePod.Name)
	klog.V(2).Infof("Allocating %d vGPU(s) to Pod %s(%s) now.", vgpusRequired, candidatePod.UID, candidatePod.Name)
	allocatedDeviceIDs, deviceSpecs, err := v.resourceManager.AllocateDevices(int(vgpusRequired))

	if err != nil {
		v.log.Error("Failed to allocate %d vGPU(s) to Pod %s(%s) because: %v", vgpusRequired, candidatePod.UID, candidatePod.Name, err)
		klog.Errorf("Failed to allocate %d vGPU(s) to Pod %s(%s) because: %v", vgpusRequired, candidatePod.UID, candidatePod.Name, err)
		return nil, err
	}

	// Store the allocation.
	allocation := &gateway.VirtualGpuAllocation{
		DeviceIDs: allocatedDeviceIDs,
	}
	v.allocations[string(candidatePod.UID)] = allocation

	// TODO(Ben): Add more in-depth logic for allocation here. This works, but doesn't hook into the new, more detailed architecture that I just setup.
	response := &pluginapi.ContainerAllocateResponse{
		Devices: deviceSpecs,
	}

	v.log.Debug("Successfully allocated %d vGPU(s) to Pod %s(%s).", vgpusRequired, candidatePod.UID, candidatePod.Name)
	klog.V(2).Infof("Successfully allocated %d vGPU(s) to Pod %s(%s).", vgpusRequired, candidatePod.UID, candidatePod.Name)

	return response, nil
}

// Return the number of individual allocations.
func (v *virtualGpuAllocatorImpl) NumAllocations() int {
	return len(v.allocations)
}

// Return the ResourceManager used by the Allocator.
func (v *virtualGpuAllocatorImpl) ResourceManager() ResourceManager {
	return v.resourceManager
}

// Return an allocation for a particular pod identified by its UID.
// Returns an `ErrAllocationNotFound` error if no allocation is found.
func (v *virtualGpuAllocatorImpl) GetAllocationForPod(podUID string) (*gateway.VirtualGpuAllocation, error) {
	allocation, ok := v.allocations[podUID]
	if !ok {
		return nil, ErrAllocationNotFound
	}

	return allocation, nil
}

// This returns GPU resources that were allocated to Pods that have since been terminated.
func (v *virtualGpuAllocatorImpl) clearTerminatedPods() {
	v.log.Debug("Clearing terminated Pods now.")

	activePodIDs := v.podCache.GetActivePodIDs()

	lastActivePodIDs := sets.New[string]()
	for podId := range v.allocations {
		lastActivePodIDs.Insert(podId)
	}

	v.log.Debug("Previous number of active Pods: %d. Current: %d.", len(activePodIDs), len(lastActivePodIDs))

	toBeRemoved := lastActivePodIDs.Difference(activePodIDs)

	var numFreedDevices int
	for podId := range toBeRemoved {
		allocation, ok := v.allocations[podId]
		if !ok {
			v.log.Error("No allocation found for now-removed Pod '%s'", podId)
			klog.Errorf("No allocation found for now-removed Pod '%s'", podId)
			continue
		}

		for _, deviceID := range allocation.GetDeviceIDs() {
			err := v.resourceManager.FreeDevice(deviceID)
			if err != nil {
				v.log.Error("Failed to free vGPU %s: %v", deviceID, err)
				klog.Errorf("Failed to free vGPU %s: %v", deviceID, err)
			} else {
				numFreedDevices += 1
			}
		}

		delete(v.allocations, podId)
	}

	v.log.Debug("Freed %d vGPU(s) from old, terminated Pods.", numFreedDevices)
	klog.V(2).Infof("Freed %d vGPU(s) from old, terminated Pods.", numFreedDevices)
}

// Return the Pods that may be the target of an allocation request (that was just received).
func (v *virtualGpuAllocatorImpl) getCandidatePodsForAllocation() ([]*corev1.Pod, error) {
	v.log.Debug("Getting candidate Pods for new allocation request now...")

	candidatePods := []*corev1.Pod{}
	allPods, err := v.podCache.GetPodsRunningOnNode(v.nodeName, string(corev1.PodPending))
	if err != nil {
		klog.Errorf("Failed to get Pods running on node %s because: %v", v.nodeName, err)
		return candidatePods, err
	}

	v.log.Debug("Found %d pod(s) running on node %s.", len(allPods), v.nodeName)
	klog.V(1).Infof("Found %d pod(s) running on node %s.", len(allPods), v.nodeName)
	for _, pod := range allPods {
		current := pod
		if PodRequiresVirtualGPUs(&current) && !PodHasVirtualGPUsAllocated(&current) {
			candidatePods = append(candidatePods, &current)
		} else {
			v.log.Debug("Pod %s(%s) does not require vGPUs.", pod.UID, pod.Name)
			klog.V(1).Infof("Pod %s(%s) does not require vGPUs.", pod.UID, pod.Name)
		}
	}

	for _, pod := range candidatePods {
		klog.V(3).Infof("candidate pod %s in ns %s with creation-timestamp %d is found.",
			pod.Name,
			pod.Namespace,
			GetCreationTimeOfPod(pod))
	}

	v.log.Debug("Found %d pod(s) for new allocation request.", len(candidatePods))

	return sortPodsByCreationTime(candidatePods), nil
}
