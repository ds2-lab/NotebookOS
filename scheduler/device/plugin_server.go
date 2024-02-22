package device

import (
	"context"
	"net"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	vgpuSocketName = "vgpu.sock"

	deviceListAsVolumeMountsHostPath          = "/dev/null"
	deviceListAsVolumeMountsContainerPathRoot = "/var/run/vgpu-devices"

	deviceListEnvvar = "VISIBLE_VGPU_DEVICES"
)

type virtualGpuPluginServerImpl struct {
	srv             *grpc.Server
	socketFile      string // Fully-qualified path.
	opts            *VirtualGpuPluginServerOptions
	log             logger.Logger
	resourceManager ResourceManager
	stop            chan interface{}

	VirtualGPUs int
}

func NewVirtualGpuPluginServer(opts *VirtualGpuPluginServerOptions) VirtualGpuPluginServer {
	socketFile := filepath.Join(opts.DevicePluginPath, vgpuSocketName)

	server := &virtualGpuPluginServerImpl{
		srv:        grpc.NewServer(),
		socketFile: socketFile,
		opts:       opts,
		stop:       make(chan interface{}),
	}

	config.InitLogger(&server.log, server)

	server.resourceManager = NewResourceManager(server.ResourceName(), opts.NumVirtualGPUs)

	return server
}

// Return the options for this DevicePlugin that will be passed to the Kubelet during registration.
func (v *virtualGpuPluginServerImpl) getDevicePluginOptions() *pluginapi.DevicePluginOptions {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired:                false,
		GetPreferredAllocationAvailable: false,
	}
}

func (v *virtualGpuPluginServerImpl) apiDevices() []*pluginapi.Device {
	return v.resourceManager.Devices().GetPluginDevices()
}

func (v *virtualGpuPluginServerImpl) SocketName() string {
	return path.Base(v.socketFile)
}

func (v *virtualGpuPluginServerImpl) SocketFile() string {
	return v.socketFile
}

func (v *virtualGpuPluginServerImpl) ResourceName() string {
	return VDeviceAnnotation
}

func (v *virtualGpuPluginServerImpl) Stop() {
	v.log.Warn("Stopping Virtual GPU resource server.")
	klog.Warning("Stopping Virtual GPU resource server.")
	v.srv.Stop()
	close(v.stop)
	v.stop = nil

	v.log.Warn("Virtual GPU resource server has stopped.")
	klog.Warning("Virtual GPU resource server has stopped.")
}

// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuPluginServerImpl) Run() error {
	err := syscall.Unlink(v.socketFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	v.log.Info("Listening on Unix Socket at \"%s\"", v.socketFile)
	klog.V(2).Infof("Listening on Unix Socket at \"%s\"", v.socketFile)
	l, err := net.Listen("unix", v.socketFile)
	if err != nil {
		return err
	}

	pluginapi.RegisterDevicePluginServer(v.srv, v)

	v.log.Info("Server %s is ready at %s", VDeviceAnnotation, v.socketFile)
	klog.V(2).Infof("Server %s is ready at %s", VDeviceAnnotation, v.socketFile)

	go func() {
		v.log.Info("Starting gRPC server for '%s'", v.ResourceName())
		klog.V(2).Infof("Starting gRPC server for '%s'", v.ResourceName())
		if err := v.srv.Serve(l); err != nil {
			klog.Errorf("Unable to start the gRPC DevicePlugin server: %+v", err)
		}
	}()

	// Wait for the server to start before registering with the kubelet.
	if err = waitForDevicePluginServer(v.socketFile, 30*time.Second); err != nil {
		klog.Errorf("Failed to detect gRPC DevicePlugin server start-up: %+v", err)
		return err
	}

	v.log.Info("Server %s has started. Registering with kubelet now.", VDeviceAnnotation)
	klog.V(2).Infof("Server %s has started. Registering with kubelet now.", VDeviceAnnotation)

	// Register this DevicePlugin with the Kubelet.
	v.registerWithKubelet()

	v.log.Info("Server %s has successfully registering with the kubelet.", VDeviceAnnotation)
	klog.V(2).Infof("Server %s has successfully registered with the kubelet.", VDeviceAnnotation)

	// We're already being called from a go-routine, so it is safe to call this.
	return v.watchDevicePluginSocket()
}

// Register this DevicePlugin with the Kubelet.
func (v *virtualGpuPluginServerImpl) registerWithKubelet() error {
	ctx := context.Background()

	kubeSocketFile := filepath.Join(v.opts.DevicePluginPath, KubeletSocket)
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	}

	conn, err := grpc.DialContext(ctx, kubeSocketFile, dialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	req := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     v.SocketName(),
		ResourceName: v.ResourceName(),
		Options:      v.getDevicePluginOptions(),
	}

	v.log.Debug("Register to kubelet with endpoint %s", req.Endpoint)
	klog.V(2).Infof("Register to kubelet with endpoint %s", req.Endpoint)
	_, err = client.Register(context.Background(), req)
	if err != nil {
		return errors.Wrap(err, "Cannot register to kubelet service")
	}

	return nil
}

// Per the Kubernetes documentation: a device v is expected to detect kubelet restarts and re-register itself with the new kubelet instance.
// A new kubelet instance deletes all the existing Unix sockets under /var/lib/kubelet/device-plugins when it starts.
// A device v can monitor the deletion of its Unix socket and re-register itself upon such an event.
//
// Thus, in this function, we monitor the deletion of our Unix socket and re-register ourselves if we detect such an event.
// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuPluginServerImpl) watchDevicePluginSocket() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		v.log.Error("Failed to create file system watcher for file \"%s\"", v.opts.DevicePluginPath)
		panic(errors.Wrapf(err, "Failed to create file system watcher for file \"%s\"", v.opts.DevicePluginPath))
	}
	defer watcher.Close()

	err = watcher.Add(v.opts.DevicePluginPath)
	if err != nil {
		v.log.Error("Failed to add file \"%s\" to file system watcher", v.opts.DevicePluginPath)
		watcher.Close()
		panic(errors.Wrapf(err, "Failed to add file \"%s\" to file system watcher", v.opts.DevicePluginPath))
	}

	for {
		select {
		case event := <-watcher.Events:
			if event.Name == v.socketFile && event.Op&fsnotify.Create == fsnotify.Create {
				time.Sleep(time.Second)
				v.log.Warn("Socket %s deleted, restarting.", v.socketFile)
				return ErrSocketDeleted // errors.Errorf("Socket deleted, restarting.", v.socketFile)
			}
		case err := <-watcher.Errors:
			v.log.Error("FileWatcher error: %s", err)
		}
	}
}

/** DevicePlugin implementation. */

// Allocate is called during container creation so that the Device
// Plugin can run device specific operations and instruct Kubelet
// of the steps to make the Device available in the container
func (v *virtualGpuPluginServerImpl) Allocate(ctx context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	v.log.Info("virtualGpuPluginServerImpl::Allocate called. Request: %v", req)
	klog.V(2).Infof("%+v allocation request for vcore", req)

	responses := pluginapi.AllocateResponse{}

	// for _, req := range req.ContainerRequests {
	// 	for _, id := range req.DevicesIDs {
	// 		v.log.Info("Checking if we have device %s now.", id)
	// 		klog.V(2).Infof("Checking if we have device %s now.", id)

	// 		if !v.resourceManager.Devices().Contains(id) {
	// 			v.log.Error("We do NOT have a vGPU device with ID=%s.", id)
	// 			klog.V(2).Infof("We do NOT have a vGPU device with ID=%s.", id)
	// 			return nil, fmt.Errorf("invalid allocation request for '%s': unknown device: %s", v.resourceManager.Resource(), id)
	// 		}
	// 	}

	// 	response, err := v.getAllocateResponse(req.DevicesIDs)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to get allocate response: %v", err)
	// 	}

	// 	responses.ContainerResponses = append(responses.ContainerResponses, response)
	// }

	for range req.ContainerRequests {
		responses.ContainerResponses = append(responses.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: []*pluginapi.DeviceSpec{
				{
					// We use "/dev/fuse" for these virtual devices.
					ContainerPath: "/dev/fuse",
					HostPath:      "/dev/fuse",
					Permissions:   "mrw",
				},
			},
		})
	}

	v.log.Info("Returning the following value from virtualGpuPluginServerImpl::Allocate: %v", responses)
	klog.V(2).Infof("Returning the following value from virtualGpuPluginServerImpl::Allocate: %v", responses)
	return &responses, nil
}

// func (v *virtualGpuPluginServerImpl) getAllocateResponse(requestIds []string) (*pluginapi.ContainerAllocateResponse, error) {
// 	deviceIDs := v.resourceManager.Devices().Subset(requestIds).GetIndices()

// 	response := pluginapi.ContainerAllocateResponse{}

// 	// All Dummy values. Don't mean anything. Virtual GPUs don't really exist.
// 	response.Envs = v.apiEnvs(deviceListEnvvar, []string{deviceListAsVolumeMountsContainerPathRoot})
// 	response.Mounts = v.apiMounts(deviceIDs)
// 	response.Devices = v.apiDeviceSpecs(requestIds)

// 	return &response, nil
// }

// getAllocateResponseForCDI returns the allocate response for the specified device IDs.
// This response contains the annotations required to trigger CDI injection in the container engine or nvidia-container-runtime.
// func (v *virtualGpuPluginServerImpl) getAllocateResponseForCDI(responseID string, deviceIDs []string) (pluginapi.ContainerAllocateResponse, error) {
// 	response := pluginapi.ContainerAllocateResponse{}
// 	return response, nil
// }

// func (v *virtualGpuPluginServerImpl) apiDeviceSpecs(ids []string) []*pluginapi.DeviceSpec {
// 	var specs []*pluginapi.DeviceSpec
// 	for _, id := range ids {
// 		spec := &pluginapi.DeviceSpec{
// 			ContainerPath: id, // Dummy values. Don't mean anything. Virtual GPUs don't really exist.
// 			HostPath:      id, // Dummy values. Don't mean anything. Virtual GPUs don't really exist.
// 			Permissions:   "rw",
// 		}
// 		specs = append(specs, spec)
// 	}

// 	return specs
// }

// func (v *virtualGpuPluginServerImpl) apiMounts(deviceIndices []int) []*pluginapi.Mount {
// 	var mounts []*pluginapi.Mount

// 	for _, idx := range deviceIndices {
// 		mount := &pluginapi.Mount{
// 			HostPath:      deviceListAsVolumeMountsHostPath,                                                 // Dummy values. Don't mean anything. Virtual GPUs don't really exist.
// 			ContainerPath: filepath.Join(deviceListAsVolumeMountsContainerPathRoot, fmt.Sprintf("%d", idx)), // Dummy values. Don't mean anything. Virtual GPUs don't really exist.
// 		}
// 		mounts = append(mounts, mount)
// 	}

// 	return mounts
// }

// func (v *virtualGpuPluginServerImpl) apiEnvs(envvar string, deviceIDs []string) map[string]string {
// 	return map[string]string{
// 		envvar: strings.Join(deviceIDs, ","),
// 	}
// }

// ListAndWatch returns a stream of List of Devices
// Whenever a Device state change or a Device disappears, ListAndWatch
// returns the new list
func (v *virtualGpuPluginServerImpl) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	v.log.Info("virtualGpuPluginServerImpl::ListAndWatch called.")
	klog.V(2).Infof("ListAndWatch request for vcore")

	if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: v.apiDevices()}); err != nil {
		return err
	}

	// We don't send unhealthy state.
	for {
		time.Sleep(time.Second)
	}

	klog.V(2).Info("ListAndWatch exiting.")
	return nil
}

// GetDevicePluginOptions returns options to be communicated with Device Manager.
func (v *virtualGpuPluginServerImpl) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	v.log.Info("virtualGpuPluginServerImpl::GetDevicePluginOptions called.")
	klog.V(2).Infof("GetDevicePluginOptions request for vcore")

	return v.getDevicePluginOptions(), nil
}

// NOTE: We do not implement this. It is an optional part of the DevicePlugin interface.
//
// PreStartContainer is called, if indicated by Device Plugin during registration phase,
// before each container start. Device v can run device specific operations
// such as resetting the device before making devices available to the container.
func (v *virtualGpuPluginServerImpl) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	v.log.Info("virtualGpuPluginServerImpl::PreStartContainer called. Request: %v", req)
	klog.V(2).Infof("PreStartContainer request for vcore")
	panic("Not implemented.")
}

// NOTE: We do not implement this. It is an optional part of the DevicePlugin interface.
//
// GetPreferredAllocation returns a preferred set of devices to allocate
// from a list of available ones. The resulting preferred allocation is not
// guaranteed to be the allocation ultimately performed by the
// devicemanager. It is only designed to help the devicemanager make a more
// informed allocation decision when possible.
func (v *virtualGpuPluginServerImpl) GetPreferredAllocation(ctx context.Context, req *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	v.log.Info("virtualGpuPluginServerImpl::GetPreferredAllocation called. Request: %v", req)
	klog.V(2).Infof("PreStartContainer request for vcore")
	panic("Not implemented.")
}
