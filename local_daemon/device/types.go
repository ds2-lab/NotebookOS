package device

import (
	"errors"
	"github.com/zhangjyr/distributed-notebook/common/proto"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	informersCore "k8s.io/client-go/informers/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	VDeviceAnnotation  = "ds2-lab.github.io/deflated-gpu"
	VirtualGPUAssigned = "ds2-lab.github.io/gpu-assigned"

	ClusterNameAnnotation = "clusterName"

	KubeletSocket = "kubelet.sock"
)

var (
	ErrSocketDeleted = errors.New("the DevicePlugin Socket has been deleted")
)

// Performs allocations on behalf of a VirtualGpuPluginServer.
type Allocator interface {
	// Allocate allows the plugin to replace the server Allocate(). Plugin can return
	// UseDefaultAllocateMethod if the default server allocation is anyhow preferred
	// for the particular allocation request.
	Allocate(*pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error)

	// Return the ResourceManager used by the Allocator.
	ResourceManager() ResourceManager

	// Set the total number of vGPUs to a new value.
	// This will return an error if the specified value is less than the number of currently-allocated vGPUs.
	SetTotalVirtualGPUs(int32) error

	NumVirtualGPUs() int          // Return the total number of vGPUs.
	NumAllocatedVirtualGPUs() int // Return the number of vGPUs that are presently allocated.
	NumFreeVirtualGPUs() int      // Return the number of vGPUs that are presently free/not allocated.
	NumAllocations() int          // Return the number of individual allocations.

	// Return an allocation for a particular pod identified by its UID.
	// Returns an `ErrAllocationNotFound` error if no allocation is found.
	GetAllocationForPod(string) (*proto.VirtualGpuAllocation, error)
}

type PreferredAllocator interface {
	// GetPreferredAllocation defines the list of devices preferred for allocating next.
	GetPreferredAllocation(*pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error)
}

type VirtualGpuAllocator interface {
	Allocator

	// Return the Devices that are managed by this allocator and its underlying resource manager.
	GetDevices() Devices
}

// Implements the DevicePlugin interface.
// https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/#device-plugin-implementation
type VirtualGpuPluginServer interface {
	Run() error           // Start the gRPC server and register with the kubelet. This function should be called within its own goroutine.
	Stop()                // Stop the gRPC server.
	SocketName() string   // Returns just the name of the device plugin socket.
	SocketFile() string   // Returns fully-qualified path to device plugin socket file.
	ResourceName() string // The name of the resource made available by this plugin.

	SetTotalVirtualGPUs(int32) error // Set the total number of vGPUs to a new value. This will return an error if the specified value is less than the number of currently-allocated vGPUs.
	NumVirtualGPUs() int             // Return the total number of vGPUs.
	NumAllocatedVirtualGPUs() int    // Return the number of vGPUs that are presently allocated.
	NumFreeVirtualGPUs() int         // Return the number of vGPUs that are presently free/not allocated.

	GetAllocations() map[string]*proto.VirtualGpuAllocation // Return the map of allocations, which is Pod UID -> allocation.
}

// Implements the PodResourcesLister interface.
// https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/#monitoring-device-plugin-resources
type VirtualGpuListerServer interface {
	Run() error           // Start the gRPC server and register with the kubelet. This function should be called within its own goroutine.
	Stop()                // Stop the gRPC server.
	SocketName() string   // Returns just the name of the device plugin socket.
	SocketFile() string   // Returns fully-qualified path to device plugin socket file.
	ResourceName() string // The name of the resource made available by this plugin.
}

// Performs low-level device allocations on behalf of an Allocator.
type ResourceManager interface {
	// Return the name of the resource managed by this instance of ResourceManager.
	Resource() string

	// Return all of the devices.
	Devices() Devices

	// Return the total number of devices.
	NumDevices() int

	// The number of currently-allocated devices.
	NumAllocatedDevices() int

	// The number of free, unallocated devices.
	NumFreeDevices() int

	// Allocate a specific Device identified by the device's ID.
	// Returns ErrDeviceNotFound if the specified device cannot be found.
	// Return ErrDeviceAlreadyAllocated if the specified device is already marked as allocated.
	// Otherwise, return nil.
	AllocateSpecificDevice(string) error

	// Allocate n devices.
	// The allocation is performed all at once.
	// If there is an insufficient number of devices available to fulfil the entire request, then no devices are allocated and an error is returned.
	// If the allocation is performed successfully, then a slice containing the allocated devices is returned, along with a nil error.
	AllocateDevices(int) ([]string, []*pluginapi.DeviceSpec, error)

	// Returns ErrDeviceNotFound if the specified device cannot be found.
	// Return ErrDeviceAlreadyAllocated if the specified device is already marked as free.
	// Otherwise, return nil.
	FreeDevice(string) error

	// Modify the total number of resources that are available.
	// This will return an error if this value is less than the number of allocated devices.
	SetTotalNumDevices(int32) error
}

// Using type alias so we can mock this.
// See https://github.com/golang/mock/issues/621#issuecomment-1094351718 for details.
type StringSet = sets.Set[string]

// Serves as a WatchDog of the Pods on this node that consume vGPUs.
type PodCache interface {
	GetActivePodIDs() StringSet                 // Get the IDs of all active (i.e., non-terminated) Pods on the Node. This only returns Pods that consume vGPUs.
	GetActivePods() map[string]*corev1.Pod      // Get all active (i.e., non-terminated) Pods on the Node. This only returns Pods that consume vGPUs.
	GetPod(string, string) (*corev1.Pod, error) // Given a namespace and the Pod's name, return the Kubernetes Pod.
	Informer() informersCore.PodInformer        // Return the Kubernetes Informer used by the PodCache.
	StopChan() chan struct{}                    // Return the channel used to stop the PodCache.

	// Return the Pods running on the specified node.
	// Optionally return only the Pods in a particular phase by passing a pod phase via the `podPhase` parameter.
	// If you do not want to restrict the Pods to any particular phase, then pass the empty string for the `podPhase` parameter.
	GetPodsRunningOnNode(nodeName string, podPhase string) ([]corev1.Pod, error)
}
