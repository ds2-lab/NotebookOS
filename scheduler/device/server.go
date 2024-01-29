package device

import (
	"context"
	"net"
	"os"
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
)

type virtualGpuResourceServerImpl struct {
	srv        *grpc.Server
	socketFile string // Fully-qualified path.
	socketName string // Just the name of the socket.
	opts       *VirtualGpuResourceServerOptions
	log        logger.Logger
}

func NewVirtualGpuResourceServer(opts *VirtualGpuResourceServerOptions) VirtualGpuResourceServer {
	socketFile := filepath.Join(opts.DevicePluginPath, vgpuSocketName)

	server := &virtualGpuResourceServerImpl{
		srv:        grpc.NewServer(),
		socketFile: socketFile,
		socketName: vgpuSocketName,
		opts:       opts,
	}

	config.InitLogger(&server.log, server)

	return server
}

// Return the options for this DevicePlugin that will be passed to the Kubelet during registration.
func (v *virtualGpuResourceServerImpl) getDevicePluginOptions() *pluginapi.DevicePluginOptions {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired:                false,
		GetPreferredAllocationAvailable: false,
	}
}

func (v *virtualGpuResourceServerImpl) SocketName() string {
	return v.socketName
}

func (v *virtualGpuResourceServerImpl) SocketFile() string {
	return v.socketFile
}

func (v *virtualGpuResourceServerImpl) ResourceName() string {
	return VDeviceAnnotation
}

func (v *virtualGpuResourceServerImpl) Stop() {
	v.srv.Stop()
	v.log.Warn("Virtual GPU resource server has stopped.")
}

// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuResourceServerImpl) Run() error {
	pluginapi.RegisterDevicePluginServer(v.srv, v)

	err := syscall.Unlink(v.socketFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	l, err := net.Listen("unix", v.socketFile)
	if err != nil {
		return err
	}

	v.log.Info("Server %s is ready at %s", VDeviceAnnotation, v.socketFile)
	klog.V(2).Infof("Server %s is ready at %s", VDeviceAnnotation, v.socketFile)

	go func() {
		if err := v.srv.Serve(l); err != nil {
			klog.Errorf("Unable to start the gRPC DevicePlugin server: %+v", err)
		}
	}()

	// Wait for the server to start before registering with the kubelet.
	if err = waitForDevicePluginServer(v.socketFile, 30*time.Second); err != nil {
		klog.Errorf("Failed to detect gRPC DevicePlugin server start-up: %+v", err)
		return err
	}

	// Register this DevicePlugin with the Kubelet.
	v.registerWithKubelet()

	// We're already being called from a go-routine, so it is safe to call this.
	return v.watchDevicePluginSocket()
}

// Register this DevicePlugin with the Kubelet.
func (v *virtualGpuResourceServerImpl) registerWithKubelet() error {
	ctx := context.Background()

	kubeSocketFile := filepath.Join(v.opts.DevicePluginPath, pluginapi.KubeletSocketWindows)
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
		Endpoint:     v.socketFile,
		ResourceName: v.ResourceName(),
		Options:      v.getDevicePluginOptions(),
	}

	klog.V(2).Infof("Register to kubelet with endpoint %s", req.Endpoint)
	_, err = client.Register(context.Background(), req)
	if err != nil {
		return errors.Wrap(err, "Cannot register to kubelet service")
	}

	return nil
}

// Per the Kubernetes documentation: a device plugin is expected to detect kubelet restarts and re-register itself with the new kubelet instance.
// A new kubelet instance deletes all the existing Unix sockets under /var/lib/kubelet/device-plugins when it starts.
// A device plugin can monitor the deletion of its Unix socket and re-register itself upon such an event.
//
// Thus, in this function, we monitor the deletion of our Unix socket and re-register ourselves if we detect such an event.
// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuResourceServerImpl) watchDevicePluginSocket() error {
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
func (v *virtualGpuResourceServerImpl) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	klog.V(2).Infof("%+v allocation request for vcore", reqs)
	panic("Not implemented.")
}

// ListAndWatch returns a stream of List of Devices
// Whenever a Device state change or a Device disappears, ListAndWatch
// returns the new list
func (v *virtualGpuResourceServerImpl) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	klog.V(2).Infof("ListAndWatch request for vcore")
	panic("Not implemented.")
}

// GetDevicePluginOptions returns options to be communicated with Device Manager.
func (v *virtualGpuResourceServerImpl) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	klog.V(2).Infof("GetDevicePluginOptions request for vcore")
	panic("Not implemented.")
}

// NOTE: We do not implement this. It is an optional part of the DevicePlugin interface.
//
// PreStartContainer is called, if indicated by Device Plugin during registration phase,
// before each container start. Device plugin can run device specific operations
// such as resetting the device before making devices available to the container.
func (v *virtualGpuResourceServerImpl) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
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
func (v *virtualGpuResourceServerImpl) GetPreferredAllocation(ctx context.Context, req *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	klog.V(2).Infof("PreStartContainer request for vcore")
	panic("Not implemented.")
}
