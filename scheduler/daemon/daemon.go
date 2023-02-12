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
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in daemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")

	// Internal errors
	ErrHeaderNotFound        = errors.New("message header not found")
	ErrKernelNotFound        = errors.New("kernel not found")
	ErrKernelNotReady        = errors.New("kernel not ready")
	ErrInvalidJupyterMessage = errors.New("invalid jupter message")

	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute
)

type Daemon struct {
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// members
	opts      *jupyter.ConnectionInfo
	transport string
	ip        string
	kernels   hashmap.HashMap[string, *client.KernelClient]
	log       logger.Logger

	// TODO: fix me
	lastKernel *client.KernelClient

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

func New(opts *jupyter.ConnectionInfo) *Daemon {
	daemon := &Daemon{
		opts:      opts,
		transport: "tcp",
		ip:        opts.IP,
		kernels:   hashmap.NewCornelkMap[string, *client.KernelClient](1000),
		closed:    make(chan struct{}),
		cleaned:   make(chan struct{}),
	}
	daemon.router = router.New(context.Background(), opts, daemon)
	config.InitLogger(&daemon.log, daemon)

	return daemon
}

// func (d *daemon) Id() jupyter.NodeId {
// 	return d.id
// }

// StartKernel launches a new kernel.
func (d *Daemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	invoker := invoker.NewDockerInvoker(d.opts)
	kernelConnInfo, err := invoker.InvokeWithContext(ctx, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Initialize kernel client
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, invoker)
	kernel := client.NewKernelClient(in.Id, kernelCtx, kernelConnInfo)
	iopub, err := kernel.InitializeIOForwarder()
	if err != nil {
		kernel.Close()
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if err := kernel.Validate(); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	// TODO: Handle kernel response.
	d.kernels.Store(in.Id, kernel)
	d.lastKernel = kernel

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(d.router.Socket(jupyter.ShellMessage).Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(iopub.Port),
		SignatureScheme: kernelConnInfo.SignatureScheme,
		Key:             kernelConnInfo.Key,
	}
	d.log.Info("Kernel %s started: %v", in.Id, info)
	return info, nil
}

// KernelStatus returns the status of a kernel.
func (d *Daemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		d.log.Warn("Kernel %s not found on query status", in.Id)
		return nil, ErrKernelNotFound
	}

	return d.statusErrorf(d.getInvoker(kernel).Status())
}

// KillKernel kills a kernel.
func (d *Daemon) KillKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	ret = &gateway.Void{}
	err = d.errorf(d.getInvoker(kernel).Close())
	return
}

// StopKernel stops a kernel.
func (d *Daemon) StopKernel(ctx context.Context, in *gateway.KernelId) (ret *gateway.Void, err error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	ret = &gateway.Void{}
	err = d.errorf(d.getInvoker(kernel).Shutdown())
	return
}

// WaitKernel waits for a kernel to exit.
func (d *Daemon) WaitKernel(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
		return nil, ErrKernelNotFound
	}

	return d.statusErrorf(d.getInvoker(kernel).Wait())
}

func (d *Daemon) Close() {
	// Close the router
	d.router.Close()

	// Wait for the kernels to be cleaned up
	<-d.cleaned
}

// RouterProvider implementations.
func (d *Daemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.ControlMessage, msg)
}

func (d *Daemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	header, err := d.headerFromMsg(msg)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok && header.MsgType == ShellKernelInfoRequest {
		// Register session on ShellKernelInfoRequest
		d.log.Debug("Binding session %s", header.Session)
		kernel = d.lastKernel
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

	// Handle ShutdownRequest
	if header.MsgType == ShellShutdownRequest {
		go d.getInvoker(kernel).Wait() // Wait() will detect the kernel status and the cleanup() will clean kernel automatically.
	}
	return nil
}

func (d *Daemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.StdinMessage, msg)
}

func (d *Daemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardRequest(nil, jupyter.HBMessage, msg)
}

func (d *Daemon) Start() error {
	d.log.Info("Starting router...")

	// Start cleaning routine.
	go d.cleanUp()

	// Start the router. The call will return on error or router.Close() is called.
	err := d.router.Start()

	// Shutdown helper routines (e.g., cleanUp())
	close(d.closed)
	return err
}

func (d *Daemon) headerFromMsg(msg *zmq4.Msg) (*jupyter.MessageHeader, error) {
	frames := d.skipIdentities(msg.Frames)
	if len(frames) == 0 {
		return nil, ErrHeaderNotFound
	}

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

func (d *Daemon) kernelFromMsg(msg *zmq4.Msg) (*client.KernelClient, error) {
	header, err := d.headerFromMsg(msg)
	if err != nil {
		return nil, err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok {
		return nil, ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return kernel, ErrKernelNotReady
	}

	return kernel, nil
}

func (d *Daemon) forwardRequest(kernel *client.KernelClient, typ jupyter.MessageType, msg *zmq4.Msg) (err error) {
	if kernel == nil {
		kernel, err = d.kernelFromMsg(msg)
		if err != nil {
			return err
		}
	}

	d.log.Debug("Forwarding %v request to kernel: %s: %v", typ, kernel.ID(), msg)
	return kernel.RequestWithHandler(typ, msg, d.getKernelResponseForwarder(typ))
}

func (d *Daemon) getKernelResponseForwarder(typ jupyter.MessageType) client.MessageHandler {
	return func(kernel client.ClientInfo, msg *zmq4.Msg) error {
		d.log.Debug("Forwarding %v response from kernel %s: %v", typ, kernel.ID(), msg)
		return d.router.Socket(typ).Send(*msg)
	}
}

func (d *Daemon) skipIdentities(frames [][]byte) [][]byte {
	i := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for string(frames[i]) != "<IDS|MSG>" {
		i++
	}
	return frames[i:]
}

func (d *Daemon) errorf(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, err.Error())
}

func (d *Daemon) statusErrorf(status jupyter.KernelStatus, err error) (*gateway.KernelStatus, error) {
	if err != nil {
		return nil, d.errorf(err)
	}
	return &gateway.KernelStatus{Status: int32(status)}, nil
}

func (d *Daemon) getInvoker(kernel *client.KernelClient) invoker.KernelInvoker {
	return kernel.Context().Value(ctxKernelInvoker).(invoker.KernelInvoker)
}

func (d *Daemon) cleanUp() {
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

func (d *Daemon) clearHandler(_ string, kernel *client.KernelClient) (contd bool) {
	d.getInvoker(kernel).Close()
	return true
}

func (d *Daemon) gcHandler(kernelId string, kernel *client.KernelClient) (contd bool) {
	if d.getInvoker(kernel).Expired(cleanUpInterval) {
		d.kernels.Delete(kernelId)
		if kernelId == kernel.ID() {
			d.log.Debug("Cleaned kernel %s due to expiration.", kernelId)
		} else {
			d.log.Debug("Cleaned entry %s of kernel %s due to expiration.", kernelId, kernel.ID())
		}
	}
	return true
}
