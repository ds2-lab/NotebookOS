package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	heartbeatInterval = time.Second
)

// KernelClient offers a simple interface to communicate with a kernel.
// All sockets except IOPub are connected on dialing.
type KernelClient struct {
	*server.BaseServer
	*SessionManager
	router router.RouterInfo
	client *server.AbstractServer

	id        string
	replicaId int32
	status    types.KernelStatus
	shell     *types.Socket
	iopub     *types.Socket

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelClient creates a new KernelClient.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(ctx context.Context, spec *gateway.KernelReplicaSpec, info *types.ConnectionInfo, router router.RouterInfo) *KernelClient {
	client := &KernelClient{
		id:        spec.Kernel.Id,
		replicaId: spec.ReplicaId,
		router:    router,
		client: server.New(ctx, info, func(s *server.AbstractServer) {
			// We do not set handlers of the sockets here. So no server routine will be started on dialing.
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.StdinPort}
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.HBPort}
			// IOPub is lazily initialized for different subclasses.
			if spec.ReplicaId == 0 {
				config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Kernel.Id))
			} else {
				config.InitLogger(&s.Log, fmt.Sprintf("Replica %s:%d ", spec.Kernel.Id, spec.ReplicaId))
			}
		}),
		status: types.KernelStatusInitializing,
	}
	client.BaseServer = client.client.Server()
	client.SessionManager = NewSessionManager(spec.Kernel.Session)
	client.log = client.client.Log

	return client
}

// ID returns the kernel ID.
func (c *KernelClient) ID() string {
	return c.id
}

// String returns a string representation of the client.
func (c *KernelClient) String() string {
	if c.replicaId == 0 {
		return fmt.Sprintf("kernel(%s)", c.id)
	} else {
		return fmt.Sprintf("replica(%s:%d)", c.id, c.replicaId)
	}
}

// Socket implements router.RouterInfo interface.
func (c *KernelClient) Socket(typ types.MessageType) *types.Socket {
	switch typ {
	case types.IOMessage:
		return c.iopub
	case types.ShellMessage:
		if c.shell != nil {
			return c.shell
		} else {
			break
		}
	}

	return c.router.Socket(typ)
}

// ConnectionInfo returns the connection info.
func (c *KernelClient) ConnectionInfo() *types.ConnectionInfo {
	return c.client.Meta
}

// Status returns the kernel status.
func (c *KernelClient) Status() types.KernelStatus {
	return c.status
}

// BindSession binds a session ID to the client.
func (c *KernelClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	c.log.Info("Binded session %s", sess)
}

// Validate validates the kernel connections. If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
func (c *KernelClient) Validate() error {
	if c.status >= types.KernelStatusRunning {
		return nil
	}
	timer := time.NewTimer(0)

	for {
		select {
		case <-c.client.Ctx.Done():
			c.status = types.KernelStatusExited
			return types.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()
			// Wait for heartbeat connection to
			if err := c.dial(nil, c.client.Sockets.HB); err != nil {
				c.client.Log.Warn("Failed to dial heartbeat (%v:%v), retrying...", c.client.Sockets.HB.Addr(), err)
				timer.Reset(heartbeatInterval)
				c.mu.Unlock()
				break // break select
			}
			c.client.Log.Debug("Heartbeat connected")

			// Dial all other sockets. If IO socket is set previously and has not been dialed
			// because kernel is not ready, dial it now.
			if err := c.dial(c.client.Sockets.All[types.HBMessage+1:]...); err != nil {
				c.client.Log.Warn(err.Error())
				c.Close()
				c.status = types.KernelStatusExited
				c.mu.Unlock()
				return err
			}

			c.status = types.KernelStatusRunning
			c.mu.Unlock()
			return nil
		}
	}
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *KernelClient) InitializeShellForwarder(handler types.MessageHandler) (*types.Socket, error) {
	shell := &types.Socket{
		Socket: zmq4.NewRouter(c.client.Ctx),
		Type:   types.ShellMessage,
	}

	if err := c.client.Listen(shell); err != nil {
		return nil, err
	}

	c.shell = shell
	go c.client.Serve(shell, func(typ types.MessageType, msg *zmq4.Msg) error {
		msg.Frames = c.BaseServer.AddKernelFrame(msg.Frames, c.id)
		return handler(typ, msg)
	})

	return shell, nil
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *KernelClient) InitializeIOForwarder() (*types.Socket, error) {
	iopub := &types.Socket{
		Socket: zmq4.NewPub(c.client.Ctx),
		Type:   types.IOMessage,
	}

	if err := c.client.Listen(iopub); err != nil {
		return nil, err
	}

	// Though named IOPub, it is a sub socket for a client.
	// Subscribe to all messages.
	// Dial our self if the client is running and serving heartbeat.
	// Try dial, ignore failure.
	if err := c.initializeIOPub(c.handleMsg); err != nil {
		iopub.Close()
		return nil, err
	}

	c.iopub = iopub
	return iopub, nil
}

// RequestWithHandler sends a request and handles the response.
func (c *KernelClient) RequestWithHandler(typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler) error {
	return c.requestWithHandler(typ, msg, c.shell != nil, handler)
}

func (c *KernelClient) requestWithHandler(typ types.MessageType, msg *zmq4.Msg, removeKernelFrame bool, handler core.KernelMessageHandler) error {
	if c.status < types.KernelStatusRunning {
		return types.ErrKernelNotReady
	}

	socket := c.client.Sockets.All[typ]
	if socket == nil {
		return types.ErrSocketNotAvailable
	}

	if err := socket.Send(*msg); err != nil {
		return err
	}

	// Add timeout if necessary.
	go c.client.ServeOnce(context.Background(), socket, msg, removeKernelFrame, func(typ types.MessageType, msg *zmq4.Msg) error {
		// Kernel frame is automatically removed.
		return handler(c, msg)
	})
	return nil
}

// Close closes the zmq sockets.
func (c *KernelClient) Close() error {
	c.BaseServer.Close()
	for _, socket := range c.client.Sockets.All {
		if socket != nil {
			socket.Close()
		}
	}
	if c.iopub != nil {
		c.iopub.Close()
		c.iopub = nil
	}
	return nil
}

func (c *KernelClient) initializeIOPub(handler types.MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Handler is set, so server routing will be started on dialing.
	c.client.Sockets.IO = &types.Socket{
		Socket:  zmq4.NewSub(c.client.Ctx), // Sub socket for client.
		Port:    c.client.Meta.IOPubPort,
		Type:    types.IOMessage,
		Handler: handler,
	}
	c.client.Sockets.IO.SetOption(zmq4.OptionSubscribe, "")
	c.client.Sockets.All[types.IOMessage] = c.client.Sockets.IO

	if c.status == types.KernelStatusRunning {
		if err := c.dial(c.client.Sockets.IO); err != nil {
			return err
		}
	}
	return nil
}

// dial connects to specified sockets
func (c *KernelClient) dial(sockets ...*types.Socket) error {
	// Start listening on all specified sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.client.Meta.Transport, c.client.Meta.IP)
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		err := socket.Socket.Dial(fmt.Sprintf(address, socket.Port))
		if err != nil {
			return fmt.Errorf("could not connect to kernel socket(port:%d): %w", socket.Port, err)
		}
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, socket := range sockets {
		if socket != nil && socket.Handler != nil {
			go c.client.Serve(socket, socket.Handler)
		}
	}

	return nil
}

func (c *KernelClient) handleMsg(typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.IOMessage:
		if c.iopub != nil {
			c.client.Log.Debug("Forwarding %v message: %v", typ, msg)
			return c.iopub.Socket.Send(*msg)
		} else {
			return ErrIOPubNotStarted
		}
	}

	return ErrHandlerNotImplemented
}
