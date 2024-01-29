package device

const (
	VDeviceAnnotation = "ds2-lab.github.io/vgpu-device"
	GPUAssigned       = "ds2-lab.github.io/gpu-assigned"

	ClusterNameAnnotation = "clusterName"

	KubeletSocket = "kubelet.sock"
)

type VirtualGpuResourceServer interface {
	Run() error
	Stop()
	SocketName() string
	ResourceName() string
	RegisterWithKubelet() error // Register the plugin with the kubelet.
}
