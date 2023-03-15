package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
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

	ErrReplicaNotFound = fmt.Errorf("replica not found")
)

type ReplicaRemover func(core.Host, core.MetaSession) error

// ReplicaKernelInfo offers hybrid information that reflects the replica source of messages.
type ReplicaKernelInfo struct {
	kernel  core.KernelInfo
	replica core.KernelInfo
}

func (r *ReplicaKernelInfo) ID() string {
	return r.kernel.ID()
}

func (r *ReplicaKernelInfo) String() string {
	return r.replica.String()
}

func (r *ReplicaKernelInfo) Socket(typ types.MessageType) *types.Socket {
	return r.kernel.Socket(typ)
}

type DistributedKernelClient struct {
	*server.BaseServer
	*SessionManager
	server *server.AbstractServer

	id             string
	status         types.KernelStatus
	busyStatus     *AggregateKernelStatus
	lastBStatusMsg *zmq4.Msg

	spec     *gateway.KernelSpec
	replicas []core.Kernel
	size     int

	log     logger.Logger
	mu      sync.RWMutex
	cleaned chan struct{}
}

func NewDistributedKernel(ctx context.Context, spec *gateway.KernelSpec, numReplicas int) *DistributedKernelClient {
	kernel := &DistributedKernelClient{
		id: spec.Id,
		server: server.New(ctx, &types.ConnectionInfo{Transport: "tcp"}, func(s *server.AbstractServer) {
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx)}
			s.Sockets.IO = &types.Socket{Socket: zmq4.NewPub(s.Ctx)}
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:   types.KernelStatusInitializing,
		spec:     spec,
		replicas: make([]core.Kernel, numReplicas),
		cleaned:  make(chan struct{}),
	}
	kernel.BaseServer = kernel.server.Server()
	kernel.SessionManager = NewSessionManager(spec.Session)
	kernel.busyStatus = NewAggregateKernelStatus(kernel, numReplicas)
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

	// On adding replica, we keep the position of the replica in the kernel aligned with the replica ID.
	// The replica ID starts with 1.
	if int(k.ReplicaID()) > cap(c.replicas) {
		newArr := make([]core.Kernel, cap(c.replicas)*2)
		copy(newArr, c.replicas)
		c.replicas = newArr[:k.ReplicaID()]
	} else if int(k.ReplicaID()) > len(c.replicas) {
		c.replicas = c.replicas[:k.ReplicaID()]
	}
	if c.replicas[k.ReplicaID()-1] == nil {
		// New added replica, or no size change for the replacement.
		c.size++
	}
	c.replicas[k.ReplicaID()-1] = k
	// Once a replica is available, the kernel is ready.
	if atomic.CompareAndSwapInt32((*int32)(&c.status), int32(types.KernelStatusInitializing), int32(types.KernelStatusRunning)) {
		c.busyStatus.Collect(context.Background(), 1, len(c.replicas), types.MessageKernelStatusStarting, c.pubIOMessage)
	}
	return nil
}

// RemoveReplica removes a kernel peer from the kernel.
func (c *DistributedKernelClient) RemoveReplica(k *KernelClient, remover ReplicaRemover) (core.Host, error) {
	host, err := c.stopReplicaLocked(k, remover)
	if err != nil {
		return host, err
	}

	c.mu.Lock()
	defer k.Close()
	defer c.mu.Unlock()

	// The replica ID starts with 1.
	if int(k.ReplicaID()) > len(c.replicas) || c.replicas[k.ReplicaID()-1] == k {
		return host, ErrReplicaNotFound
	}

	c.replicas[k.ReplicaID()-1] = nil
	c.size--
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
func (c *DistributedKernelClient) InitializeShellForwarder(handler core.KernelMessageHandler) (*types.Socket, error) {
	shell := c.server.Sockets.Shell
	if err := c.server.Listen(shell); err != nil {
		return nil, err
	}

	go c.server.Serve(c, shell, func(srv types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
		msg.Frames, _ = c.BaseServer.AddDestFrame(msg.Frames, c.KernelSpec().Id, server.JOffsetAutoDetect)
		return handler(srv.(*DistributedKernelClient), typ, msg)
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

// RequestWithHandler sends a request to all replicas and handles the response.
func (c *DistributedKernelClient) RequestWithHandler(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func()) error {
	return c.RequestWithHandlerAndReplicas(ctx, typ, msg, handler, done)
}

// RequestWithHandlerAndReplicas sends a request to specified replicas and handles the response.
func (c *DistributedKernelClient) RequestWithHandlerAndReplicas(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func(), replicas ...core.Kernel) error {
	// Boardcast to all replicas if no replicas are specified.
	if len(replicas) == 0 {
		replicas = c.replicas
	}

	once := sync.Once{}
	var replicaCtx context.Context
	var cancel context.CancelFunc
	if typ == types.ShellMessage {
		replicaCtx, cancel = context.WithCancel(ctx)
	} else {
		replicaCtx, cancel = context.WithTimeout(ctx, server.DefaultRequestTimeout)
	}
	forwarder := func(replica core.KernelInfo, typ types.MessageType, msg *zmq4.Msg) (err error) {
		execErr, yield := c.handleShellError(replica.(*KernelClient), msg)
		if yield {
			c.log.Debug("Discard %v response from %v: %v", typ, replica, execErr)
			return
		}

		// Handler will only be called once.
		forwarded := false
		once.Do(func() {
			cancel()
			err = handler(&ReplicaKernelInfo{kernel: c, replica: replica}, typ, msg)
			forwarded = true
			done()
		})
		if !forwarded {
			c.log.Debug("Discard %s response from %v", typ, replica)
		}
		return
	}

	// Send the request to all replicas.
	statusCtx, _ := context.WithTimeout(context.Background(), server.DefaultRequestTimeout)
	c.busyStatus.Collect(statusCtx, c.size, len(c.replicas), types.MessageKernelStatusBusy, c.pubIOMessage)
	if len(replicas) == 1 {
		return replicas[0].(*KernelClient).requestWithHandler(replicaCtx, typ, msg, forwarder, c.getWaitResponseOption, nil)
	}
	for _, kernel := range replicas {
		if kernel == nil {
			continue
		}

		go func(kernel core.Kernel) {
			err := kernel.(*KernelClient).requestWithHandler(replicaCtx, typ, msg, forwarder, c.getWaitResponseOption, nil)
			if err != nil {
				c.log.Warn("Failed to send request to %v: %v", kernel, err)
			}
		}(kernel)
	}
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
	for _, replica := range c.replicas {
		if replica == nil {
			continue
		}

		go func(replica core.Kernel) {
			defer stopped.Done()

			if host, err := c.stopReplicaLocked(replica.(*KernelClient), remover); err != nil {
				c.log.Warn("Failed to stop %v on host %v: %v", replica, host, err)
				return
			}
		}(replica)
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
	for _, replica := range c.replicas {
		if replica == nil {
			continue
		}

		go func(replica core.Kernel) {
			defer stopped.Done()

			host := replica.Context().Value(ctxKernelHost).(core.Host)
			host.WaitKernel(context.Background(), &gateway.KernelId{Id: replica.ID()})
		}(replica)
	}
	stopped.Wait()

	return nil
}

func (c *DistributedKernelClient) getWaitResponseOption(key string) interface{} {
	switch key {
	case server.WROptionRemoveDestFrame:
		return true
	}

	return nil
}

func (c *DistributedKernelClient) handleMsg(replica types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.IOMessage:
		topic, jFrames := replica.(*KernelClient).extractIOTopicFrame(msg.Frames)
		switch topic {
		case types.IOTopicStatus:
			return c.handleIOKernelStatus(replica.(*KernelClient), jFrames, msg)
		default:
			c.log.Debug("Forwarding %v message from replica %d: %v", typ, replica.(*KernelClient).ReplicaID(), msg)
			return c.server.Sockets.IO.Send(*msg)
		}
	}

	return ErrHandlerNotImplemented
}

func (c *DistributedKernelClient) handleIOKernelStatus(replica *KernelClient, frames [][]byte, msg *zmq4.Msg) error {
	status, err := replica.handleIOKernelStatus(frames, msg)
	if err != nil {
		return err
	}

	c.log.Debug("reducing replica %d status %s to match %s", replica.ReplicaID(), status, c.busyStatus.expectingStatus)
	c.busyStatus.Reduce(replica.ReplicaID(), status, msg, c.pubIOMessage)
	return nil
}

func (c *DistributedKernelClient) handleShellError(replica *KernelClient, msg *zmq4.Msg) (err error, yield bool) {
	jFrame, _ := c.SkipIdentities(msg.Frames)
	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if len(jFrame) < 5 {
		return types.ErrInvalidJupyterMessage, false
	}

	var msgErr types.MessageError
	err = json.Unmarshal(jFrame[5], &msgErr)
	if err != nil {
		return err, false
	}

	if msgErr.Status == types.MessageStatusOK {
		return nil, false
	}

	return errors.New(msgErr.ErrValue), msgErr.ErrName == types.MessageErrYieldExecution
}

func (c *DistributedKernelClient) pubIOMessage(msg *zmq4.Msg, status string, how string) error {
	c.log.Debug("Publishing %v status(%s:%s): %v", types.IOMessage, status, how, msg)
	c.lastBStatusMsg = msg
	err := c.server.Sockets.IO.Send(*msg)

	// Initiate idle status collection.
	if status == types.MessageKernelStatusBusy {
		c.busyStatus.Collect(context.Background(), c.size, len(c.replicas), types.MessageKernelStatusIdle, c.pubIOMessage)
		// Fill matched status that has been received before collecting.
		for _, replica := range c.replicas {
			if replica == nil {
				continue
			}

			status, msg := replica.(*KernelClient).BusyStatus()
			if status == types.MessageKernelStatusIdle {
				c.busyStatus.Reduce(replica.(*KernelClient).ReplicaID(), types.MessageKernelStatusIdle, msg, c.pubIOMessage)
			}
		}
	}

	return err
}
