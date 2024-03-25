package device

import (
	"errors"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
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

type Allocator interface {
	// Allocate allows the plugin to replace the server Allocate(). Plugin can return
	// UseDefaultAllocateMethod if the default server allocation is anyhow preferred
	// for the particular allocation request.
	Allocate(*pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error)
}

type PreferredAllocator interface {
	// GetPreferredAllocation defines the list of devices preferred for allocating next.
	GetPreferredAllocation(*pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error)
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

	GetAllocations() map[string]*gateway.VirtualGpuAllocation // Return the map of allocations, which is Pod UID -> allocation.
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
