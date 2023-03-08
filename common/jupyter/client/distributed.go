package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	protocol "github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	SessionIDUndecided = "undecided"
)

var (
	ctxKernelHost = utils.ContextKey("host")
)

type ReplicaRemover func(core.Host, core.MetaSession) error

type DistributedKernelClient struct {
	*server.BaseServer
	*SessionManager
	router router.RouterInfo
	server *server.AbstractServer

	id     string
	status types.KernelStatus

	spec     *gateway.KernelSpec
	replicas []core.Kernel
	size     int

	log     logger.Logger
	mu      sync.RWMutex
	cleaned chan struct{}
}

func NewDistributedKernel(ctx context.Context, spec *gateway.KernelSpec, router router.RouterInfo) *DistributedKernelClient {
	kernel := &DistributedKernelClient{
		id:     spec.Id,
		router: router,
		server: server.New(ctx, &types.ConnectionInfo{Transport: "tcp"}, func(s *server.AbstractServer) {
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx)}
			s.Sockets.IO = &types.Socket{Socket: zmq4.NewPub(s.Ctx)}
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:   types.KernelStatusInitializing,
		spec:     spec,
		replicas: make([]core.Kernel, 0, 3),
		cleaned:  make(chan struct{}),
	}
	kernel.BaseServer = kernel.server.Server()
	kernel.SessionManager = NewSessionManager(spec.Session)
	kernel.log = kernel.server.Log
	return kernel
}

// ResetID resets the kernel ID.
func (c *DistributedKernelClient) ResetID(id string) {
	c.id = id
	c.log.Info("Reset kernel ID to %s", id)
	if colorLog, ok := c.log.(*logger.ColorLogger); ok {
		colorLog.Prefix = fmt.Sprintf("kernel(%s) ", id)
	}
}

// String returns a string representation of the client.
func (c *DistributedKernelClient) String() string {
	return fmt.Sprintf("kernel(%s)", c.id)
}

// Socket implements the router.RouteInfo interface.
func (c *DistributedKernelClient) Socket(typ types.MessageType) *types.Socket {
	socket := c.BaseServer.Socket(typ)
	if socket != nil {
		return socket
	}

	return c.router.Socket(typ)
}

// MetaSession implementations.
func (c *DistributedKernelClient) ID() string {
	return c.id
}

func (s *DistributedKernelClient) Spec() protocol.Spec {
	return s.spec.Resource
}

func (s *DistributedKernelClient) KernelSpec() *gateway.KernelSpec {
	return s.spec
}

// Status returns the kernel status.
func (c *DistributedKernelClient) Status() types.KernelStatus {
	return c.status
}

// BindSession binds a session ID to the client.
func (c *DistributedKernelClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	var restarted bool
	if c.status == types.KernelStatusExited {
		restarted = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(types.KernelStatusExited), int32(types.KernelStatusInitializing))
	}
	if restarted {
		c.log.Info("Restarted for binding kernel session %s", sess)
	} else {
		c.log.Info("Binded session %s", sess)
	}
}

// Size returns the number of replicas in the kernel.
func (c *DistributedKernelClient) Size() int {
	return c.size
}

// Replicas returns the replicas in the kernel.
func (c *DistributedKernelClient) Replicas() []core.Kernel {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Make a copy of references.
	ret := make([]core.Kernel, 0, c.size)
	for i := 0; i < c.size; i++ {
		if c.replicas[i] != nil {
			ret = append(ret, c.replicas[i])
		}
	}
	return ret
}

// AddReplica adds a replica peer to the kernel.
func (c *DistributedKernelClient) AddReplica(k *KernelClient, host core.Host) error {
	// IOForwarder is initialized, link the kernel to feed the IOPub.
	err := k.initializeIOPub(c.handleMsg)
	if err != nil {
		return err
	}

	// Safe to append the kernel now.
	k.SetContext(context.WithValue(k.Context(), ctxKernelHost, host))

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.size == len(c.replicas) {
		c.replicas = append(c.replicas, k)
	} else {
		// Fill the empty slot.
		for i := 0; i < len(c.replicas); i++ {
			if c.replicas[i] == nil {
				c.replicas[i] = k
				break
			}
		}
	}
	c.size++
	// Once a replica is available, the kernel is ready.
	atomic.CompareAndSwapInt32((*int32)(&c.status),
		int32(types.KernelStatusInitializing), int32(types.KernelStatusRunning))
	return nil
}

// RemoveReplica removes a kernel peer from the kernel.
func (c *DistributedKernelClient) RemoveReplica(k *KernelClient, remover ReplicaRemover) (core.Host, error) {
	host, err := c.stopReplicaLocked(k, remover)
	if err != nil {
		return host, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.size--
	for i := 0; i < len(c.replicas); i++ {
		if c.replicas[i] == k {
			k.Close()
			c.replicas[i] = nil
			break
		}
	}

	return host, nil
}

// Validate validates the session.
func (c *DistributedKernelClient) Validate() error {
	if c.status >= types.KernelStatusRunning {
		return nil
	}

	return types.ErrKernelNotReady
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *DistributedKernelClient) InitializeShellForwarder(handler types.MessageHandler) (*types.Socket, error) {
	shell := c.server.Sockets.Shell
	if err := c.server.Listen(shell); err != nil {
		return nil, err
	}

	go c.server.Serve(shell, func(typ types.MessageType, msg *zmq4.Msg) error {
		msg.Frames = c.BaseServer.AddKernelFrame(msg.Frames, c.KernelSpec().Id)
		return handler(typ, msg)
	})

	return shell, nil
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *DistributedKernelClient) InitializeIOForwarder() (*types.Socket, error) {
	if err := c.server.Listen(c.server.Sockets.IO); err != nil {
		return nil, err
	}

	return c.server.Sockets.IO, nil
}

// RequestWithHandler sends a request and handles the response.
func (c *DistributedKernelClient) RequestWithHandler(typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, replicas ...core.Kernel) error {
	// Boardcast to all replicas if no replicas are specified.
	if len(replicas) == 0 {
		replicas = c.replicas
	}

	preprocessor := func(_ router.RouterInfo, msg *zmq4.Msg) error {
		// Kernel frame is automatically removed.
		return handler(c, msg)
	}

	if len(replicas) == 1 {
		return replicas[0].(*KernelClient).requestWithHandler(typ, msg, true, preprocessor)
	}

	resq := make(chan *zmq4.Msg, len(replicas))
	forwarder := func(_ router.RouterInfo, msg *zmq4.Msg) error {
		resq <- msg
		return nil
	}
	for _, kernel := range replicas {
		go func(kernel core.Kernel) {
			err := kernel.(*KernelClient).requestWithHandler(typ, msg, true, forwarder)
			if err != nil {
				c.log.Warn("Failed to send request to %v: %v", kernel, err)
			}
		}(kernel)
	}

	go func() {
		// Just wait for the first response.
		preprocessor(c, <-resq)
	}()
	return nil
}

// Shutdown releases all replicas and closes the session.
func (c *DistributedKernelClient) Shutdown(remover ReplicaRemover, restart bool) error {
	c.mu.RLock()

	if c.status == types.KernelStatusExited {
		c.mu.RUnlock()
		return nil
	}

	// Stop the replicas first.
	var stopped sync.WaitGroup
	stopped.Add(c.size)

	// In RLock, don't change anything in c.replicas.
	for _, kernel := range c.replicas {
		if kernel == nil {
			continue
		}

		go func(kernel core.Kernel) {
			defer stopped.Done()

			if host, err := c.stopReplicaLocked(kernel.(*KernelClient), remover); err != nil {
				c.log.Warn("Failed to stop %v on host %v: %v", kernel, host, err)
				return
			}
		}(kernel)
	}
	c.mu.RUnlock()
	stopped.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearReplicasLocked()

	// If restart is true, we are done without actually close the kernel.
	// The sockets will be reused because the jupyter server will not try to connect a new iopub during restart.
	if restart {
		return nil
	}

	return c.closeLocked()
}

// Close closes the session and all replicas.
func (c *DistributedKernelClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == types.KernelStatusExited {
		return nil
	}

	return c.closeLocked()
}

// WaitClosed waits for the replicas to be cleaned.
func (c *DistributedKernelClient) WaitClosed() types.KernelStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// If shutdown is initiated, cleaned should have been closed.
	select {
	case <-c.cleaned:
		return c.status
	default:
	}

	// Or shutdown is initiated from somewhere else (e.g. zmq command), query the status.
	c.queryCloseLocked()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearReplicasLocked()

	return c.status
}

func (c *DistributedKernelClient) stopReplicaLocked(k *KernelClient, remover ReplicaRemover) (core.Host, error) {
	host := k.Context().Value(ctxKernelHost).(core.Host)
	if err := remover(host, c); err != nil {
		return host, err
	}

	host.WaitKernel(context.Background(), &gateway.KernelId{Id: k.ID()})
	return host, nil
}

func (c *DistributedKernelClient) clearReplicasLocked() {
	for i, kernel := range c.replicas {
		if kernel != nil {
			kernel.Close()
			c.replicas[i] = nil
		}
	}
	c.size = 0
	c.replicas = c.replicas[:0]
	c.status = types.KernelStatusExited
}

func (c *DistributedKernelClient) closeLocked() error {
	c.BaseServer.Close()

	c.clearReplicasLocked()
	for _, socket := range c.server.Sockets.All {
		if socket != nil {
			socket.Close()
		}
	}

	close(c.cleaned)

	return nil
}

func (c *DistributedKernelClient) queryCloseLocked() error {
	// Stop the replicas first.
	var stopped sync.WaitGroup
	stopped.Add(c.size)
	for _, kernel := range c.replicas {
		if kernel == nil {
			continue
		}

		go func(kernel core.Kernel) {
			defer stopped.Done()

			host := kernel.Context().Value(ctxKernelHost).(core.Host)
			host.WaitKernel(context.Background(), &gateway.KernelId{Id: kernel.ID()})
		}(kernel)
	}
	stopped.Wait()

	return nil
}

func (c *DistributedKernelClient) handleMsg(typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.IOMessage:
		return c.server.Sockets.IO.Send(*msg)
	}

	return ErrHandlerNotImplemented
}
