package device

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type virtualGpuAllocator struct {
	sync.Mutex

	log logger.Logger

	kubeClient kubernetes.Interface
	nodeName   string

	opts *VirtualGpuPluginServerOptions

	resourceManager ResourceManager
	stopChan        chan interface{}

	// Mapping from PodID to its allocation.
	allocations map[string]*gateway.VirtualGpuAllocation
}

func newVirtualGpuAllocator(opts *VirtualGpuPluginServerOptions, nodeName string) *virtualGpuAllocator {
	allocator := &virtualGpuAllocator{
		opts:        opts,
		stopChan:    make(chan interface{}),
		allocations: make(map[string]*gateway.VirtualGpuAllocation),
		nodeName:    nodeName,
	}
	config.InitLogger(&allocator.log, allocator)

	allocator.resourceManager = NewResourceManager(allocator.ResourceName(), opts.NumVirtualGPUs)

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(err.Error())
	}

	allocator.kubeClient = clientset

	return allocator
}

// Return the map of allocations, which is Pod UID -> allocation.
func (v *virtualGpuAllocator) getAllocations() map[string]*gateway.VirtualGpuAllocation {
	return v.allocations
}

func (v *virtualGpuAllocator) ResourceName() string {
	return VDeviceAnnotation
}

func (v *virtualGpuAllocator) apiDevices() []*pluginapi.Device {
	return v.resourceManager.Devices().GetPluginDevices()
}

// Return the total number of vGPUs.
func (v *virtualGpuAllocator) numVirtualGPUs() int {
	return v.resourceManager.NumDevices()
}

// Return the number of vGPUs that are presently allocated.
func (v *virtualGpuAllocator) numAllocatedVirtualGPUs() int {
	return v.resourceManager.NumAllocatedDevices()
}

// Return the number of vGPUs that are presently free/not allocated.
func (v *virtualGpuAllocator) numFreeVirtualGPUs() int {
	return v.resourceManager.NumFreeDevices()
}

func (v *virtualGpuAllocator) stop() {
	close(v.stopChan)
	v.stopChan = nil
}

// Set the total number of vGPUs to a new value.
// This will return an error if the specified value is less than the number of currently-allocated vGPUs.
func (v *virtualGpuAllocator) setTotalVirtualGPUs(value int32) error {
	v.Lock()
	defer v.Unlock()

	if value < int32(v.numAllocatedVirtualGPUs()) {
		return ErrInvalidResourceAdjustment
	}

	// TODO(Ben): Modify the total number of virtual GPUs.
	return v.resourceManager.SetTotalNumDevices(value)
}

// Allocate is called during container creation so that the Device
// Plugin can run device specific operations and instruct Kubelet
// of the steps to make the Device available in the container
func (v *virtualGpuAllocator) allocate(ctx context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	v.Lock()
	defer v.Unlock()

	v.log.Info("virtualGpuPluginServerImpl::Allocate called. Request: %v", req)
	klog.V(2).Infof("%+v allocation request for vcore", req)

	// Requests are always sent one-at-a-time.
	var request *pluginapi.ContainerAllocateRequest = req.ContainerRequests[0]

	v.clearTerminatedPods()
	var candidatePod *corev1.Pod

	candidatePods, err := getCandidatePodsForAllocation(v.kubeClient, v.nodeName)
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

		if getVirtualGpuRequirementsOfPod(pod) == numVirtualGPUsRequested {
			v.log.Debug("Found candidate Pod %s(%s) with requested vGPUs = %d.", string(pod.UID), pod.Name, numVirtualGPUsRequested)
			klog.V(2).Infof("Found candidate Pod %s(%s) with requested vGPUs = %d.", string(pod.UID), pod.Name, numVirtualGPUsRequested)
			candidatePod = pod
			break
		}
	}

	if candidatePod != nil {
		// Allocate resources to the Pod.
		resp, err := v.doAllocate(numVirtualGPUsRequested, candidatePod)

		if err != nil {
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
		errorMessage := fmt.Sprintf("could not find candidate Pod for request %v, allocation failed.", request)
		v.log.Error(errorMessage)
		klog.Error(errorMessage)
		return nil, fmt.Errorf(errorMessage)
	}
}

// This actually performs the allocation of GPUs to a particular pod.
func (v *virtualGpuAllocator) doAllocate(vgpusRequired int32, candidatePod *corev1.Pod) (*pluginapi.ContainerAllocateResponse, error) {
	allocatedDeviceIDs, deviceSpecs, err := v.resourceManager.AllocateDevices(int(vgpusRequired))

	if err != nil {
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

	return response, nil
}

// This returns GPU resources that were allocated to Pods that have since been terminated.
func (v *virtualGpuAllocator) clearTerminatedPods() {
	activePodIDs := podCache.GetActivePodIDs()

	lastActivePodIDs := sets.NewString()
	for podId, _ := range v.allocations {
		lastActivePodIDs.Insert(podId)
	}

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
func getCandidatePodsForAllocation(kubeClient kubernetes.Interface, nodeName string) ([]*v1.Pod, error) {
	candidatePods := []*v1.Pod{}
	allPods, err := getPodsRunningOnNode(kubeClient, nodeName, string(v1.PodPending))
	if err != nil {
		klog.Errorf("Failed to get Pods running on node %s because: %v", nodeName, err)
		return candidatePods, err
	}

	for _, pod := range allPods {
		current := pod
		if podRequiresVirtualGPUs(&current) && !podHasVirtualGPUsAllocated(&current) {
			candidatePods = append(candidatePods, &current)
		}
	}

	for _, pod := range candidatePods {
		klog.V(3).Infof("candidate pod %s in ns %s with creation-timestamp %d is found.",
			pod.Name,
			pod.Namespace,
			getCreationTimeOfPod(pod))
	}

	return sortPodsByCreationTime(candidatePods), nil
}

// Return the Pods running on the specified node.
// Optionally return only the Pods in a particular phase by passing a pod phase via the `podPhase` parameter.
// If you do not want to restrict the Pods to any particular phase, then pass the empty string for the `podPhase` parameter.
func getPodsRunningOnNode(kubeClient kubernetes.Interface, nodeName string, podPhase string) ([]corev1.Pod, error) {
	if nodeName == "" {
		nodeName, _ = os.Hostname()
	}

	var pods []corev1.Pod
	var selector fields.Selector

	if podPhase == "" {
		selector = fields.SelectorFromSet(fields.Set{
			"spec.nodeName": nodeName,
		})
	} else {
		selector = fields.SelectorFromSet(fields.Set{
			"spec.nodeName": nodeName,
			"status.phase":  podPhase,
		})
	}

	deadline := time.Minute
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), deadline)
	defer deadlineCancel()
	err := wait.PollUntilContextTimeout(deadlineCtx, time.Second, deadline, true, func(ctx context.Context) (bool, error) {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			return false, err
		} else {
			pods = podList.Items
		}
		return true, nil
	})

	if err != nil {
		return pods, fmt.Errorf("failed to retrieve list of Pods running on node %s because: %v", nodeName, err)
	}

	return pods, nil
}
