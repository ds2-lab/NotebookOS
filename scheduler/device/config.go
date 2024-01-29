package device

type VirtualGpuResourceServerConfig struct {
	DevicePluginPath string `name:"device-plugin-path" description:"The path to the socket used by the kubelet to receive our DevicePlugin registration."`
}
