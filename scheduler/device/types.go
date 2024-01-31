package device

import "errors"

const (
	VDeviceAnnotation = "ds2-lab.github.io/deflated-gpu"
	GPUAssigned       = "ds2-lab.github.io/gpu-assigned"

	ClusterNameAnnotation = "clusterName"

	KubeletSocket = "kubelet.sock"
)

var (
	ErrSocketDeleted = errors.New("DevicePlugin Socket deleted.")
)

// Implements the DevicePlugin interface.
// https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/#device-plugin-implementation
type VirtualGpuPluginServer interface {
	Run() error           // Start the gRPC server and register with the kubelet. This function should be called within its own goroutine.
	Stop()                // Stop the gRPC server.
	SocketName() string   // Returns just the name of the device plugin socket.
	SocketFile() string   // Returns fully-qualified path to device plugin socket file.
	ResourceName() string // The name of the resource made available by this plugin.
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
	Resource() string
	Devices() Devices
}
