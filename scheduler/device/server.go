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
	vcoreSocketName = "vcore.sock"
)

type virtualGpuResourceServerImpl struct {
	srv        *grpc.Server // This server is not "owned" by us. It is owned by the Scheduler/Local Daemon.
	socketFile string
	opts       *VirtualGpuResourceServerConfig
	log        logger.Logger
}

func newVirtualGpuResourceServerImpl(opts *VirtualGpuResourceServerConfig, srv *grpc.Server) VirtualGpuResourceServer {
	socketFile := filepath.Join(opts.DevicePluginPath, vcoreSocketName)

	server := &virtualGpuResourceServerImpl{
		srv:        srv,
		socketFile: socketFile,
		opts:       opts,
	}

	config.InitLogger(&server.log, server)

	return server
}

func (v *virtualGpuResourceServerImpl) SocketName() string {
	return v.socketFile
}

func (v *virtualGpuResourceServerImpl) ResourceName() string {
	return VDeviceAnnotation
}

func (v *virtualGpuResourceServerImpl) Stop() {
	// Do nothing. The gRPC server isn't owned by us.
}

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

	return v.srv.Serve(l)
}

func (v *virtualGpuResourceServerImpl) RegisterWithKubelet() error {
	socketFile := filepath.Join(v.opts.DevicePluginPath, KubeletSocket)
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()}

	conn, err := grpc.Dial(socketFile, dialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)

	req := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(v.SocketName()),
		ResourceName: v.ResourceName(),
		Options: &pluginapi.DevicePluginOptions{
			PreStartRequired:                false,
			GetPreferredAllocationAvailable: false,
		},
	}

	klog.V(2).Infof("Register to kubelet with endpoint %s", req.Endpoint)
	_, err = client.Register(context.Background(), req)
	if err != nil {
		return err
	}

	return nil
}

// Per the Kubernetes documentation: a device plugin is expected to detect kubelet restarts and re-register itself with the new kubelet instance.
// A new kubelet instance deletes all the existing Unix sockets under /var/lib/kubelet/device-plugins when it starts.
// A device plugin can monitor the deletion of its Unix socket and re-register itself upon such an event.
//
// Thus, in this function, we monitor the deletion of our Unix socket and re-register ourselves if we detect such an event.
// NOTE: This function should be called within its own goroutine.
func (v *virtualGpuResourceServerImpl) WatchDevicePluginSocket() error {
	devicePluginSocket := filepath.Join(v.opts.DevicePluginPath, KubeletSocket)
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
			if event.Name == devicePluginSocket && event.Op&fsnotify.Create == fsnotify.Create {
				time.Sleep(time.Second)
				v.log.Warn("Socket %s deleted, restarting.", devicePluginSocket)
				// TODO(Ben): Implement restarting and recreating the gRPC server.
				panic("Have not yet implemented restarting on kubelet restarts.")
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
