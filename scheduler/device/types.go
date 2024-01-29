package device

import "errors"

const (
	VDeviceAnnotation = "ds2-lab.github.io/vgpu-device"
	GPUAssigned       = "ds2-lab.github.io/gpu-assigned"

	ClusterNameAnnotation = "clusterName"
)

var (
	ErrSocketDeleted = errors.New("DevicePlugin Socket deleted.")
)

type VirtualGpuResourceServer interface {
	Run() error           // Start the gRPC server and register with the kubelet. This function should be called within its own goroutine.
	Stop()                // Stop the gRPC server.
	SocketName() string   // Returns just the name of the device plugin socket.
	SocketFile() string   // Returns fully-qualified path to device plugin socket file.
	ResourceName() string // The name of the resource made available by this plugin.
	// RegisterWithKubelet() error // Register the plugin with the kubelet.
}
