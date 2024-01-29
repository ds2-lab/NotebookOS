package device

type VirtualGpuResourceServerOptions struct {
	DevicePluginPath string `name:"device-plugin-path" description:"The path to the socket used by the kubelet to receive our DevicePlugin registration."`
	// DevicePluginRpcPort string `name:"device-plugin-port" description:"The port that the gRPC server for the DevicePlugin interface listens on."`
}
