package device

import (
	"context"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"net"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	vgpuSocketName = "vgpu.sock"
)

var (
	ErrNotEnabled                = errors.New("the virtual gpu plugin server is not enabled")
	ErrInvalidResourceAdjustment = errors.New("the number of virtual GPUs cannot be decreased below the number of already-allocated virtual GPUs")
)

type virtualGpuPluginServerImpl struct {
	srv        *grpc.Server
	socketFile string // Fully-qualified path.
	opts       *domain.VirtualGpuPluginServerOptions
	log        logger.Logger
	podCache   PodCache

	stopChan                   chan interface{}
	totalNumVirtualGpusChanged chan interface{}

	enabled bool

	allocator *virtualGpuAllocatorImpl
}

func NewVirtualGpuPluginServer(opts *domain.VirtualGpuPluginServerOptions, nodeName string, disabled bool) VirtualGpuPluginServer {
	socketFile := filepath.Join(opts.DevicePluginPath, vgpuSocketName)

	server := &virtualGpuPluginServerImpl{
		srv:                        grpc.NewServer(),
		socketFile:                 socketFile,
		opts:                       opts,
		totalNumVirtualGpusChanged: make(chan interface{}),
		stopChan:                   make(chan interface{}),
		enabled:                    !disabled,
	}

	if !disabled {
		podCache := NewPodCache(nodeName)
		if podCache == nil {
			panic("Failed to create PodCache.")
		}
		server.podCache = podCache
		server.allocator = NewVirtualGpuAllocator(opts, nodeName, podCache, server.totalNumVirtualGpusChanged).(*virtualGpuAllocatorImpl)
	}

	config.InitLogger(&server.log, server)

	return server
}

// Return the map of allocations, which is Pod UID -> allocation.
func (v *virtualGpuPluginServerImpl) GetAllocations() map[string]*proto.VirtualGpuAllocation {
	return v.allocator.getAllocations()
}

// Return the options for this DevicePlugin that will be passed to the Kubelet during registration.
func (v *virtualGpuPluginServerImpl) getDevicePluginOptions() *pluginapi.DevicePluginOptions {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired:                false,
		GetPreferredAllocationAvailable: false,
	}
}

func (v *virtualGpuPluginServerImpl) apiDevices() []*pluginapi.Device {
	return v.allocator.apiDevices()
}

// Return the total number of vGPUs.
func (v *virtualGpuPluginServerImpl) NumVirtualGPUs() int {
	return v.allocator.NumVirtualGPUs()
}

// Return the number of vGPUs that are presently allocated.
func (v *virtualGpuPluginServerImpl) NumAllocatedVirtualGPUs() int {
	return v.allocator.NumAllocatedVirtualGPUs()
}

// Return the number of vGPUs that are presently free/not allocated.
func (v *virtualGpuPluginServerImpl) NumFreeVirtualGPUs() int {
	return v.allocator.NumFreeVirtualGPUs()
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
	v.podCache.StopChan() <- struct{}{} // Stop the Pod WatchDog.
	v.stopChan <- struct{}{}
	v.srv.Stop()
	v.allocator.stop()

	v.log.Warn("Virtual GPU resource server has stopped.")
	klog.Warning("Virtual GPU resource server has stopped.")
}

// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuPluginServerImpl) Run() error {
	if !v.enabled {
		return ErrNotEnabled
	}

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

// Set the total number of vGPUs to a new value.
// This will return an error if the specified value is less than the number of currently-allocated vGPUs.
func (v *virtualGpuPluginServerImpl) SetTotalVirtualGPUs(value int32) error {
	return v.allocator.SetTotalVirtualGPUs(value)
}

/** DevicePlugin implementation. */

// Allocate is called during container creation so that the Device
// Plugin can run device specific operations and instruct Kubelet
// of the steps to make the Device available in the container
func (v *virtualGpuPluginServerImpl) Allocate(ctx context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	return v.allocator.Allocate(req)
}

// ListAndWatch returns a stream of List of Devices
// Whenever a Device state change or a Device disappears, ListAndWatch
// returns the new list
func (v *virtualGpuPluginServerImpl) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	v.log.Info("virtualGpuPluginServerImpl::ListAndWatch called.")
	klog.V(2).Infof("ListAndWatch request for vcore")

	v.log.Debug("Sending first/initial 'ListAndWatchResponse' now. Number of vGPUs: %d.", v.NumVirtualGPUs())
	klog.V(3).Infof("Sending first/initial 'ListAndWatchResponse' now. Number of vGPUs: %d.", v.NumVirtualGPUs())
	if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: v.apiDevices()}); err != nil {
		return err
	} else {
		v.log.Debug("Successfully sent first/initial 'ListAndWatchResponse': %v", v.NumVirtualGPUs())
		klog.V(3).Info("Successfully sent sent first/initial 'ListAndWatchResponse'")
	}

	// We don't send unhealthy state.
	var running = true
	for running {
		select {
		case <-v.totalNumVirtualGpusChanged:
			{
				v.log.Debug("Total number of vGPUs changed (%d). Sending 'ListAndWatchResponse' now", v.NumVirtualGPUs())
				klog.V(2).Infof("Total number of vGPUs changed (%d). Sending 'ListAndWatchResponse' now", v.NumVirtualGPUs())
				if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: v.apiDevices()}); err != nil {
					return err
				} else {
					v.log.Debug("Successfully sent 'ListAndWatchResponse'")
					klog.V(2).Info("Successfully sent 'ListAndWatchResponse'")
				}
			}
		case <-v.allocator.stopChan:
			{
				v.log.Warn("Received 'STOP' notification in ListAndWatch.")
				klog.Warning("Received 'STOP' notification in ListAndWatch.")
				running = false
				break
			}
		}
	}

	v.log.Debug("ListAndWatch exiting.")
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
