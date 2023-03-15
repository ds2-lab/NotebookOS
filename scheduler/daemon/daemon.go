package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"

	"github.com/zhangjyr/distributed-notebook/scheduler/invoker"
)

const (
	ShellKernelInfoRequest = "kernel_info_request"
	ShellShutdownRequest   = "shutdown_request"
)

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")
	ErrNoHandler        = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in SchedulerDaemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")

	// Internal errors
	ErrHeaderNotFound        = errors.New("message header not found")
	ErrKernelNotFound        = errors.New("kernel not found")
	ErrKernelNotReady        = errors.New("kernel not ready")
	ErrInvalidJupyterMessage = errors.New("invalid jupter message")
	ErrKernelIDRequired      = errors.New("kernel id frame is required for kernel_info_request")

	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute
)

type SchedulerDaemonConfig func(*SchedulerDaemon)

type SchedulerDaemonOptions struct {
	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer bool `name:"direct" usage:"True if the scheduler serves jupyter notebook directly."`
}

type SchedulerDaemon struct {
	// Options
	id string

	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// Options
	connectionOptions *jupyter.ConnectionInfo
	Options           SchedulerDaemonOptions

	// members
	transport string
	ip        string
	kernels   hashmap.HashMap[string, *client.KernelClient]
	log       logger.Logger

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

func New(opts *jupyter.ConnectionInfo, configs ...SchedulerDaemonConfig) *SchedulerDaemon {
	daemon := &SchedulerDaemon{
		connectionOptions: opts,
		transport:         "tcp",
		ip:                opts.IP,
		kernels:           hashmap.NewCornelkMap[string, *client.KernelClient](1000),
		closed:            make(chan struct{}),
		cleaned:           make(chan struct{}),
	}
	for _, config := range configs {
		config(daemon)
	}
	config.InitLogger(&daemon.log, daemon)
	daemon.router = router.New(context.Background(), daemon.connectionOptions, daemon)

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

// SetID sets the SchedulerDaemon id by the gateway.
func (d *SchedulerDaemon) SetID(ctx context.Context, in *gateway.HostId) (*gateway.HostId, error) {
	// If id has been set(e.g., restored after restart), return the original id.
	if d.id != "" {
		return &gateway.HostId{Id: d.id}, nil
	}

	d.id = in.Id
	return in, nil
}

// StartKernel starts a single kernel.
func (d *SchedulerDaemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	return d.StartKernelReplica(ctx, &gateway.KernelReplicaSpec{
		ReplicaId: 1,
		Replicas:  nil,
		Kernel:    in,
	})
}

// StartKernel launches a new kernel.
func (d *SchedulerDaemon) StartKernelReplica(ctx context.Context, in *gateway.KernelReplicaSpec) (*gateway.KernelConnectionInfo, error) {
	invoker := invoker.NewDockerInvoker(d.connectionOptions)
	connInfo, err := invoker.InvokeWithContext(ctx, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Initialize kernel client with new context.
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, invoker)
	kernel := client.NewKernelClient(kernelCtx, in, connInfo)
	shell := d.router.Socket(jupyter.ShellMessage)
	if d.Options.DirectServer {
		var err error
		shell, err = kernel.InitializeShellForwarder(d.kernelShellHandler)
		if err != nil {
			d.closeKernel(kernel, "failed initializing shell forwarder")
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	iopub, err := kernel.InitializeIOForwarder()
	if err != nil {
		d.closeKernel(kernel, "failed initializing io forwarder")
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if err := kernel.Validate(); err != nil {
		d.closeKernel(kernel, "validation error")
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// TODO: Handle kernel response.

	// Register kernel.
	d.kernels.Store(kernel.ID(), kernel)
	// Register all sessions already associated with the kernel. Usually, there will be only one session used by the KernelManager(manager.py)
	for _, session := range kernel.Sessions() {
		d.kernels.Store(session, kernel)
	}

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(shell.Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(iopub.Port),
		SignatureScheme: connInfo.SignatureScheme,
		Key:             connInfo.Key,
	}
	d.log.Info("Kernel %s started: %v", in.ID(), info)
	return info, nil
}

// KernelStatus returns the status of a kernel.
func (d *SchedulerDaemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Warn("Kernel %s not found on query status", in.Id)
		return nil, ErrKernelNotFound
	}

	status, err := d.getInvoker(kernel).Status()
	return d.statusErrorf(kernel, status, err)
}

// KillKernel kills a kernel.
func (d *SchedulerDaemon) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	ret = &gateway.Void{}
	err = d.errorf(d.getInvoker(kernel).Close())
	return
}

// StopKernel stops a kernel.
func (d *SchedulerDaemon) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	ret = &gateway.Void{}
	err = d.errorf(d.getInvoker(kernel).Shutdown())
	return
}

// WaitKernel waits for a kernel to exit.
func (d *SchedulerDaemon) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	status, err := d.getInvoker(kernel).Wait()
	return d.statusErrorf(kernel, status, err)
}

func (d *SchedulerDaemon) SetClose(ctx context.Context, in *gateway.Void) (*gateway.Void, error) {
	d.Close()
	return &gateway.Void{}, nil
}

func (d *SchedulerDaemon) Start() error {
	d.log.Info("Starting router...")

	// Start cleaning routine.
	go d.cleanUp()

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Shutdown helper routines (e.g., cleanUp())
	close(d.closed)
	return err
}

func (d *SchedulerDaemon) Close() error {
	// Close the router
	d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned
	return nil
}

// RouterProvider implementations.
func (d *SchedulerDaemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	// Kernel ID is not available in the control message.
	_, header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok {
		return ErrKernelNotFound
	}

	if err := d.forwardRequest(context.Background(), kernel, jupyter.ControlMessage, msg, nil); err != nil {
		return err
	}

	// Handle ShutdownRequest
	if header.MsgType == ShellShutdownRequest {
		go func() {
			status, err := d.getInvoker(kernel).Wait() // Wait() will detect the kernel status and the cleanup() will clean kernel automatically.
			d.statusErrorf(kernel, status, err)
		}()
	}

	return nil
}

func (d *SchedulerDaemon) kernelShellHandler(info core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	return d.ShellHandler(info, msg)
}

func (d *SchedulerDaemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
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

		d.log.Debug("Binding %v with session %s ", kernel, header.Session)
		d.kernels.Store(header.Session, kernel)
		kernel.BindSession(header.Session)
	}
	if kernel == nil {
		return ErrKernelNotFound
	}

	// Check availability
	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := d.forwardRequest(ctx, kernel, jupyter.ShellMessage, msg, cancel); err != nil {
		return err
	}

	return nil
}

func (d *SchedulerDaemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(context.Background(), nil, jupyter.StdinMessage, msg, nil)
}

func (d *SchedulerDaemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(context.Background(), nil, jupyter.HBMessage, msg, nil)
}

// idFromMsg extracts the kernel id or session id from the ZMQ message.
func (d *SchedulerDaemon) idFromMsg(msg *zmq4.Msg) (id string, sessId bool, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)
	if kernelId != "" {
		return kernelId, false, nil
	}

	header, err := d.headerFromFrames(msg.Frames[offset:])
	if err != nil {
		return "", false, err
	}

	return header.Session, true, nil
}

func (d *SchedulerDaemon) headerFromFrames(frames [][]byte) (*jupyter.MessageHeader, error) {
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

func (d *SchedulerDaemon) headerFromMsg(msg *zmq4.Msg) (kernelId string, header *jupyter.MessageHeader, err error) {
	kernelId, _, offset := d.router.ExtractDestFrame(msg.Frames)

	header, err = d.headerFromFrames(msg.Frames[offset:])

	return kernelId, header, err
}

func (d *SchedulerDaemon) kernelFromMsg(msg *zmq4.Msg) (kernel *client.KernelClient, err error) {
	id, _, err := d.idFromMsg(msg)
	if err != nil {
		return nil, err
	}

	kernel, ok := d.kernels.Load(id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, ErrKernelNotReady
	}

	return kernel, nil
}

func (d *SchedulerDaemon) forwardRequest(ctx context.Context, kernel *client.KernelClient, typ jupyter.MessageType, msg *zmq4.Msg, done func()) (err error) {
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	d.log.Debug("Forwarding %v request(%p) to %v: %v", typ, msg, kernel, msg)
	if done == nil {
		done = func() {}
	}
	return kernel.RequestWithHandler(ctx, typ, msg, d.kernelResponseForwarder, done)
}

func (d *SchedulerDaemon) kernelResponseForwarder(from core.KernelInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	socket := from.Socket(typ)
	if socket == nil {
		socket = d.router.Socket(typ)
	}
	if socket == nil {
		d.log.Warn("Unable to forward %v response: socket unavailable", typ)
		return nil
	}
	d.log.Debug("Forwarding %v response from %v: %v", socket, from, msg)
	return socket.Send(*msg)
}

func (d *SchedulerDaemon) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *SchedulerDaemon) statusErrorf(kernel *client.KernelClient, status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}

	if status >= jupyter.KernelStatusExited {
		d.kernels.Delete(kernel.ID())
		for _, session := range kernel.Sessions() {
			d.kernels.Delete(session)
		}
		d.log.Debug("Cleaned kernel %s and associated sessions %v after kernel stopped.", kernel.ID(), kernel.Sessions())
		kernel.Close()
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *SchedulerDaemon) getInvoker(kernel *client.KernelClient) invoker.KernelInvoker {
	return kernel.Context().Value(ctxKernelInvoker).(invoker.KernelInvoker)
}

func (d *SchedulerDaemon) closeKernel(kernel *client.KernelClient, reason string) {
	if err := d.getInvoker(kernel).Close(); err != nil {
		d.log.Warn("Failed to close %v after %s, failure: %v", kernel, reason, err)
	}
	kernel.Close()
}

func (d *SchedulerDaemon) cleanUp() {
	timer := time.NewTimer(cleanUpInterval)

	for {
		select {
		case <-d.closed:
			// Router is closed, clean up all kernels.
			d.kernels.Range(d.clearHandler)
			// Signal that the clean up is done.
			close(d.cleaned)
			return
		case <-timer.C:
			// Clean up expired kernels.
			d.kernels.Range(d.gcHandler)
			// Reset the timer.
			timer.Reset(cleanUpInterval)
		}
	}
}

func (d *SchedulerDaemon) clearHandler(_ string, kernel *client.KernelClient) (contd bool) {
	d.getInvoker(kernel).Close()
	return true
}

func (d *SchedulerDaemon) gcHandler(kernelId string, kernel *client.KernelClient) (contd bool) {
	if d.getInvoker(kernel).Expired(cleanUpInterval) {
		d.kernels.Delete(kernelId)
		if kernelId == kernel.ID() {
			d.log.Debug("Cleaned kernel %s due to expiration.", kernelId)
		} else {
			d.log.Debug("Cleaned session %s of kernel %s due to expiration.", kernelId, kernel.ID())
		}
	}
	return true
}
