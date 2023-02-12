package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
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
	server  *server.AbstractServer
	id      string
	status  types.KernelStatus
	iopub   *types.Socket
	session string

	mu sync.Mutex
}

// NewKernelClient creates a new KernelClient.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(id string, ctx context.Context, info *types.ConnectionInfo) *KernelClient {
	client := &KernelClient{
		id: id,
		server: server.New(ctx, info, func(s *server.AbstractServer) {
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.StdinPort}
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.HBPort}
			// IOPub is lazily initialized for different subclasses.
		}),
		status: types.KernelStatusInitializing,
	}
	client.BaseServer = client.server.Server()
	client.server.Log = config.GetLogger(fmt.Sprintf("Kernel %s ", id))

	return client
}

// ID returns the kernel ID.
func (c *KernelClient) ID() string {
	return c.id
}

// Status returns the kernel status.
func (c *KernelClient) Status() types.KernelStatus {
	return c.status
}

// Session returns the associated session ID.
func (c *KernelClient) Session() string {
	return c.session
}

// BindSession binds a session ID to the client.
func (c *KernelClient) BindSession(sess string) {
	c.session = sess
	c.server.Log.Info("Binded session %s", sess)
}

// Validate validates the kernel connections. If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
func (c *KernelClient) Validate() error {
	timer := time.NewTimer(0)

	for {
		select {
		case <-c.server.Ctx.Done():
			c.status = types.KernelStatusExited
			return types.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()
			// Wait for heartbeat connection to
			if err := c.dial(c.server.Sockets.HB); err != nil {
				c.server.Log.Warn("Failed to dial heartbeat (%v:%v), retrying...", c.server.Sockets.HB.Addr(), err)
				timer.Reset(heartbeatInterval)
				c.mu.Unlock()
				break // break select
			}
			c.server.Log.Debug("Heartbeat connected")

			if err := c.dial(c.server.Sockets.All[types.HBMessage+1:]...); err != nil {
				c.server.Log.Warn(err.Error())
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
func (c *KernelClient) InitializeIOForwarder() (*types.Socket, error) {
	iopub, err := c.listenIO()
	if err != nil {
		return nil, err
	}
	c.iopub = iopub

	c.mu.Lock()
	defer c.mu.Unlock()

	c.server.Sockets.IOPub = &types.Socket{Socket: zmq4.NewSub(c.server.Ctx), Port: c.server.Meta.IOPubPort} // Though named IOPub, it is a sub socket for a client.
	c.server.Sockets.IOPub.SetOption(zmq4.OptionSubscribe, "")                                               // Subscribe to all messages.
	c.server.Sockets.All[types.IOPubMessage] = c.server.Sockets.IOPub
	// Dial our self if the client is running and serving heartbeat.
	if c.status == types.KernelStatusRunning {
		// Try dial, ignore failure.
		if err := c.dial(c.server.Sockets.IOPub); err != nil {
			return nil, err
		}
	}

	return iopub, nil
}

// RequestWithHandler sends a request and handles the response.
func (c *KernelClient) RequestWithHandler(typ types.MessageType, msg *zmq4.Msg, handler MessageHandler) error {
	if c.status < types.KernelStatusRunning {
		return types.ErrKernelReady
	}

	socket := c.server.Sockets.All[typ]
	if socket == nil {
		return types.ErrSocketNotAvailable
	}

	if err := socket.Send(*msg); err != nil {
		return err
	}

	go c.server.ServeOnce(typ, socket, func(typ types.MessageType, msg *zmq4.Msg) error {
		return handler(c, msg)
	})
	return nil
}

// Close closes the zmq sockets.
func (c *KernelClient) Close() {
	c.BaseServer.Close()
	for _, socket := range c.server.Sockets.All {
		if socket != nil {
			socket.Close()
		}
	}
	if c.iopub != nil {
		c.iopub.Close()
	}
}

// dial connects to specified sockets
func (c *KernelClient) dial(sockets ...*types.Socket) error {
	// Start listening on all specified sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.server.Meta.Transport, c.server.Meta.IP)
	tbserved := make([]types.MessageType, 0, len(sockets))
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		err := socket.Socket.Dial(fmt.Sprintf(address, socket.Port))
		if err != nil {
			return fmt.Errorf("could not connect to kernel socket(port:%d): %w", socket.Port, err)
		}

		// IOPub is the only socket that needs to be served for a client.
		if socket == c.server.Sockets.IOPub {
			tbserved = append(tbserved, types.IOPubMessage)
		}
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, typ := range tbserved {
		go c.server.Serve(typ, c.server.Sockets.All[typ], c.handleMsg)
	}

	return nil
}

func (c *KernelClient) listenIO() (*types.Socket, error) {
	iopub := zmq4.NewPub(c.server.Ctx)
	err := iopub.Listen("tcp://:0")
	if err != nil {
		return nil, err
	}

	return &types.Socket{Socket: iopub, Port: iopub.Addr().(*net.TCPAddr).Port}, nil
}

func (c *KernelClient) handleMsg(typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.IOPubMessage:
		if c.iopub != nil {
			c.server.Log.Debug("Forwarding %v message: %v", typ, msg)
			return c.iopub.Socket.Send(*msg)
		} else {
			return ErrIOPubNotStarted
		}
	}

	return ErrHandlerNotImplemented
}
