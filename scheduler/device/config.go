package device

type VirtualGpuPluginServerOptions struct {
	DevicePluginPath string `name:"device-plugin-path" description:"The path to the socket used by the kubelet to receive our DevicePlugin registration."`
	NumVirtualGPUs   int    `name:"num-virtual-gpus-per-node" description:"The number of virtual GPUs to be made available on each Kubernetes node."`
	// DevicePluginRpcPort string `name:"device-plugin-port" description:"The port that the gRPC server for the DevicePlugin interface listens on."`
}
