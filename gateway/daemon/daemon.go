package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/hashicorp/yamux"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

const (
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"
)

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")
	ErrNoHandler        = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in daemon")
	ErrNotSupported     = status.Errorf(codes.Unimplemented, "not supported in daemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")

	// Internal errors
	ErrHeaderNotFound        = errors.New("message header not found")
	ErrKernelNotFound        = errors.New("kernel not found")
	ErrKernelNotReady        = errors.New("kernel not ready")
	ErrInvalidJupyterMessage = errors.New("invalid jupter message")
	ErrKernelIDRequired      = errors.New("kernel id frame is required for kernel_info_request")
)

type GatewayDaemonConfig func(*GatewayDaemon)

// GatewayDaemon serves distributed notebook gateway for three roles:
// 1. A jupyter remote kernel gateway.
// 2. A global scheduler that coordinate host schedulers.
// 3. Implemented net.Listener interface to bi-directional gRPC calls.
type GatewayDaemon struct {
	id string

	gateway.UnimplementedClusterGatewayServer
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	ClusterOptions    core.CoreOptions

	// cluster provisioning related members
	listener net.Listener
	cluster  core.Cluster
	placer   core.Placer

	// kernel members
	transport string
	ip        string
	kernels   hashmap.HashMap[string, *client.DistributedKernelClient]
	log       logger.Logger

	// lifetime
	closed  int32
	cleaned chan struct{}
}

func New(opts *jupyter.ConnectionInfo, configs ...GatewayDaemonConfig) *GatewayDaemon {
	daemon := &GatewayDaemon{
		connectionOptions: opts,
		transport:         "tcp",
		ip:                opts.IP,
		kernels:           hashmap.NewCornelkMap[string, *client.DistributedKernelClient](1000),
		cleaned:           make(chan struct{}),
	}
	for _, config := range configs {
		config(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon)
	daemon.cluster = core.NewCluster()
	daemon.placer = core.NewRandomPlacer(daemon.cluster, &daemon.ClusterOptions)

	if daemon.ip == "" {
		ip, err := utils.GetIP()
		if err != nil {
			daemon.log.Warn("No ip set because of missing configuration and failed to get ip: %v", err)
		} else {
			daemon.ip = ip
		}
	}

	return daemon
}

// Listen listens on the TCP network address addr and returns a net.Listener that intercepts incoming connections.
func (d *GatewayDaemon) Listen(transport string, addr string) (net.Listener, error) {
	// Initialize listener
	lis, err := net.Listen(transport, addr)
	if err != nil {
		return nil, err
	}

	d.listener = lis
	return d, nil
}

// net.Listener implementation
func (d *GatewayDaemon) Accept() (net.Conn, error) {
	// Inspired by https://github.com/dustin-decker/grpc-firewall-bypass
	incoming, err := d.listener.Accept()
	if err != nil {
		return nil, err
	}

	// Initialize yamux session for bi-directional gRPC calls
	session, err := yamux.Client(incoming, yamux.DefaultConfig())
	if err != nil {
		return nil, err
	}

	// Dial to create a gRPC connection with existing mux connection
	var mux net.Conn
	gConn, err := grpc.Dial(":0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			mux, err = session.Open()
			return mux, err
		}))
	if err != nil {
		return nil, err
	}

	// Create a host scheduler client and register it.
	host, err := NewHostScheduler(incoming.RemoteAddr().String(), gConn)
	if err == nil {
		d.cluster.GetHostManager().Store(host.ID(), host)
	} else if err == errRestoreRequired {
		// Restore host scheduler.
		registered, loaded := d.cluster.GetHostManager().LoadOrStore(host.ID(), host)
		if loaded {
			registered.Restore(host)
		} else {
			d.log.Warn("Host scheduler requested for restoration but not found: %s", host.ID())
			// TODO: Notify scheduler to restore?
		}
	} else {
		d.log.Warn("Failed to create host scheduler client: %s", err.Error())
		mux.Close()
		return nil, err
	}

	d.log.Info("Incomming host scheduler %v connected", host)
	return mux, nil
}

// Close are compatible with GatewayDaemon.Close().

func (d *GatewayDaemon) Addr() net.Addr {
	return d.listener.Addr()
}

func (d *GatewayDaemon) SetID(ctx context.Context, hostId *gateway.HostId) (*gateway.HostId, error) {
	d.id = hostId.Id
	return hostId, nil
}

// StartKernel launches a new kernel.
func (d *GatewayDaemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	// Try to find existing kernel by session id first. The kernel that associated with the session id will not be clear during restart.
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		// Initialize kernel with new context.
		kernel = client.NewDistributedKernel(context.Background(), in, d.router)
		_, err := kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			kernel.Close()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		_, err = kernel.InitializeIOForwarder()
		if err != nil {
			kernel.Close()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	} else {
		d.log.Info("Restarting %v...", kernel)
		kernel.BindSession(in.Session)
	}

	hosts := d.placer.FindHosts(nil)

	var created sync.WaitGroup
	created.Add(len(hosts))
	for i, host := range hosts {
		// Launch replicas in parallel.
		go func(replicaId int, host core.Host) {
			var err error
			defer func() {
				created.Done()
				if err != nil {
					d.log.Warn("Failed to start replica(%s:%d): %v", kernel.KernelSpec().Id, replicaId, err)
				}
			}()

			var replicaConnInfo *gateway.KernelConnectionInfo
			replicaSpec := &gateway.KernelReplicaSpec{
				Kernel:      in,
				ReplicaId:   int32(replicaId),
				NumReplicas: int32(len(hosts)),
			}
			replicaConnInfo, err = d.placer.Place(host, replicaSpec)
			if err != nil {
				return
			}

			// Initialize kernel client
			replica := client.NewKernelClient(context.Background(), replicaSpec, replicaConnInfo.ConnectionInfo(), d.router)
			err = replica.Validate()
			if err != nil {
				d.closeKernel(host, kernel, replica, replicaId, "validation error")
				return
			}

			err = kernel.AddReplica(replica, host)
			if err != nil {
				d.closeKernel(host, kernel, replica, replicaId, "failed adding to the kernel")
				return
			}

		}(i+1, host)
	}

	// TODO: Handle replica creation error and ensure enough number of replicas are created.

	// Wait for all replicas to be created.
	created.Wait()

	if kernel.Size() == 0 {
		return nil, status.Errorf(codes.Internal, "Failed to start kernel")
	}

	// TODO: Handle kernel response.
	d.kernels.Store(kernel.KernelSpec().Id, kernel)
	for _, sess := range kernel.Sessions() {
		d.kernels.Store(sess, kernel)
	}

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(kernel.Socket(jupyter.ShellMessage).Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(kernel.Socket(jupyter.IOMessage).Port),
		SignatureScheme: kernel.KernelSpec().SignatureScheme,
		Key:             kernel.KernelSpec().Key,
	}
	d.log.Info("Kernel(%s) started: %v", kernel.ID(), info)
	return info, nil
}

func (d *GatewayDaemon) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	return nil, ErrNotSupported
}

// KernelStatus returns the status of a kernel.
func (d *GatewayDaemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.Status(), nil)
}

// KillKernel kills a kernel.
func (d *GatewayDaemon) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	return d.StopKernel(ctx, in)
}

// StopKernel stops a kernel.
func (d *GatewayDaemon) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	d.log.Info("Stopping %v, will restart %v", kernel, *in.Restart)
	ret = &gateway.Void{}
	go func() {
		err = d.errorf(kernel.Shutdown(d.placer.Reclaim, *in.Restart))
		if err != nil {
			d.log.Warn("Failed to close kernel: %s", err.Error())
			return
		}

		// Clear session records.
		for _, sess := range kernel.Sessions() {
			d.kernels.Delete(sess)
		}
		d.log.Debug("Cleaned sessions %v after replicas stopped.", kernel.Sessions())
		if *in.Restart {
			// Keep kernel records if restart is requested.
			kernel.ClearSessions()
		} else {
			d.kernels.Delete(kernel.ID())
			d.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}
	}()
	return
}

// WaitKernel waits for a kernel to exit.
func (d *GatewayDaemon) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return d.statusErrorf(jupyter.KernelStatusExited, nil)
	}

	return d.statusErrorf(kernel.WaitClosed(), nil)
}

func (d *GatewayDaemon) Start() error {
	d.log.Info("Starting router...")

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Clean up
	d.cleanUp()

	return err
}

func (d *GatewayDaemon) Close() error {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		// Closed already
		return nil
	}

	// Close the router
	d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned

	// Close the listener
	if d.listener != nil {
		d.listener.Close()
	}

	return nil
}

// RouterProvider implementations.
func (d *GatewayDaemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.ControlMessage, msg)
}

func (d *GatewayDaemon) kernelShellHandler(typ jupyter.MessageType, msg *zmq4.Msg) error {
	return d.ShellHandler(d.router, msg)
}

func (d *GatewayDaemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	kernelId, header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok && header.MsgType == ShellKernelInfoRequest {
		// Register kernel on ShellKernelInfoRequest
		if kernelId == "" {
			return ErrKernelIDRequired
		}

		kernel, ok = d.kernels.Load(kernelId)
		if !ok {
			return ErrKernelNotFound
		}

		kernel.BindSession(header.Session)
		d.kernels.Store(header.Session, kernel)
	}
	if kernel == nil {
		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	if err := d.forwardRequest(kernel, jupyter.ShellMessage, msg); err != nil {
		return err
	}

	return nil
}

func (d *GatewayDaemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *GatewayDaemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *GatewayDaemon) idFromMsg(msg *zmq4.Msg) (id string, sessId bool, err error) {
	kernelId, _, frames := d.router.ExtractKernelFrames(msg.Frames)
	if kernelId != "" {
		return kernelId, false, nil
	}

	header, err := d.headerFromFrames(frames)
	if err != nil {
		return "", false, err
	}

	return header.Session, true, nil
}

func (d *GatewayDaemon) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
	var header jupyter.MessageHeader
	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if len(frames) < 6 {
		return nil, ErrInvalidJupyterMessage
	}
	err := json.Unmarshal(frames[2], &header)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

func (d *GatewayDaemon) headerFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, err error) {
	kernelId, _, frames := d.router.ExtractKernelFrames(msg.Frames)

	header, err = d.headerFromFrames(frames)

	return kernelId, header, err
}

func (d *GatewayDaemon) sessionFromMsg(msg *zmq4.Msg) (*client.DistributedKernelClient, error) {
	id, _, err := d.idFromMsg(msg)
	if err != nil {
		return nil, err
	}

	session, ok := d.kernels.Load(id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	if session.Status() != jupyter.KernelStatusRunning {
		return session, ErrKernelNotReady
	}

	return session, nil
}

func (d *GatewayDaemon) forwardRequest(session *client.DistributedKernelClient, typ jupyter.MessageType, msg *zmq4.Msg) (err error) {
	if session == nil {
		session, err = d.sessionFromMsg(msg)
		if err != nil {
			return err
		}
	}

	d.log.Debug("Forwarding %v request to all kernels: %s: %v", typ, session.ID(), msg)
	return session.RequestWithHandler(typ, msg, d.getKernelResponseForwarder(typ))
}

func (d *GatewayDaemon) getKernelResponseForwarder(typ jupyter.MessageType) core.KernelMessageHandler {
	return func(router router.RouterInfo, msg *zmq4.Msg) error {
		socket := router.Socket(typ)
		if socket == nil {
			d.log.Warn("Unable to forward %v response: socket unavailable", typ)
			return nil
		}
		d.log.Debug("Forwarding %v response from %v: %v", socket, router, msg)
		return socket.Send(*msg)
	}
}

func (d *GatewayDaemon) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *GatewayDaemon) statusErrorf(status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *GatewayDaemon) cleanUp() {
	// Clear nothing for now:
	// Hosts and kernels may contact other gateways to restore status.
	close(d.cleaned)
}

func (d *GatewayDaemon) closeKernel(host core.Host, session *client.DistributedKernelClient, kernel *client.KernelClient, replicaId int, reason string) {
	defer kernel.Close()

	if err := d.placer.Reclaim(host, session); err != nil {
		d.log.Warn("Failed to close kernel(%s:%d) after %s, failure: %v", kernel.ID(), replicaId, reason, err)
	}
}
