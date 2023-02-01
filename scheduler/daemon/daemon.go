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

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")
	ErrNoHandler        = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in daemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")

	// Internal errors
	ErrHeaderNotFound     = errors.New("message header not found")
	ErrKernelNotFound     = errors.New("kernel not found")
	ErrKernelNotReady     = errors.New("kernel not ready")
	ErrSocketNotAvailable = errors.New("socket not available")

	// Context keys
	ctxKernelInvoker = utils.ContextKey("invoker")

	cleanUpInterval = time.Minute
)

type Daemon struct {
	gateway.UnimplementedLocalGatewayServer
	router *router.Router

	// members
	transport string
	ip        string
	kernels   hashmap.HashMap[string, *client.KernelClient]
	log       logger.Logger

	// lifetime
	closed  chan struct{}
	cleaned chan struct{}
}

func New(opts *jupyter.ConnectionInfo) *Daemon {
	daemon := &Daemon{
		transport: "tcp",
		ip:        opts.IP,
		kernels:   hashmap.NewCornelkMap[string, *client.KernelClient](1000),
		closed:    make(chan struct{}),
		cleaned:   make(chan struct{}),
	}
	daemon.router = router.New(context.Background(), opts, daemon)
	config.InitLogger(&daemon.log, daemon)

	go daemon.start()
	go daemon.cleanUp()

	return daemon
}

// func (d *daemon) Id() jupyter.NodeId {
// 	return d.id
// }

// StartKernel launches a new kernel.
func (d *Daemon) StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error) {
	invoker := &invoker.LocalInvoker{}
	kernelConnInfo, err := invoker.InvokeWithContext(ctx, in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Initialize kernel client
	kernelCtx := context.WithValue(context.Background(), ctxKernelInvoker, invoker)
	kernel := client.NewKernelClient(in.Id, kernelCtx, kernelConnInfo)
	d.kernels.Store(in.Id, kernel)

	info := &gateway.KernelConnectionInfo{
		Ip:              d.ip,
		Transport:       d.transport,
		ControlPort:     int32(d.router.Socket(jupyter.ControlMessage).Port),
		ShellPort:       int32(d.router.Socket(jupyter.ShellMessage).Port),
		StdinPort:       int32(d.router.Socket(jupyter.StdinMessage).Port),
		HbPort:          int32(d.router.Socket(jupyter.HBMessage).Port),
		IopubPort:       int32(kernelConnInfo.IOPubPort),
		SignatureScheme: kernelConnInfo.SignatureScheme,
		Key:             kernelConnInfo.Key,
	}
	return info, nil
}

// KernelStatus returns the status of a kernel.
func (d *Daemon) GetKernelStatus(ctx context.Context, in *gateway.KernelId) (*gateway.KernelStatus, error) {
	kernel, ok := d.kernels.Load(in.Id)
	if !ok {
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
	d.router.Close()
	// Wait for the kernels to be cleaned up
	<-d.cleaned
}

// RouterProvider implementations.
func (d *Daemon) ControlHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardHandler(info, jupyter.ControlMessage, msg)
}

func (d *Daemon) ShellHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardHandler(info, jupyter.ShellMessage, msg)
}

func (d *Daemon) StdinHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardHandler(info, jupyter.StdinMessage, msg)
}

func (d *Daemon) HBHandler(info router.RouterInfo, msg *zmq4.Msg) error {
	return d.forwardHandler(info, jupyter.HBMessage, msg)
}

func (d *Daemon) start() {
	d.router.Start()
	close(d.closed)
}

func (d *Daemon) forwardHandler(info router.RouterInfo, typ jupyter.MessageType, msg *zmq4.Msg) error {
	frames := d.skipIdentities(msg.Frames)
	if len(frames) == 0 {
		return ErrHeaderNotFound
	}

	var header jupyter.MessageHeader
	err := json.Unmarshal(frames[0], &header)
	if err != nil {
		return err
	}

	kernel, ok := d.kernels.Load(header.Session)
	if !ok {
		return ErrKernelNotFound
	}

	if kernel.Status() != jupyter.KernelStatusRunning {
		return ErrKernelNotReady
	}

	dest := kernel.Socket(typ)
	if dest == nil {
		return ErrSocketNotAvailable
	}

	return dest.Send(*msg)
}

func (d *Daemon) skipIdentities(frames [][]byte) [][]byte {
	i := 0
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
			d.kernels.Range(d.clearHandler)
			close(d.cleaned)
			return
		case <-timer.C:
			d.kernels.Range(d.gcHandler)
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
	}
	return true
}
