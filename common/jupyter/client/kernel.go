package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
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
	client *server.AbstractServer

	id             string
	replicaId      int32
	status         types.KernelStatus
	busyStatus     string
	lastBStatusMsg *zmq4.Msg
	shell          *types.Socket
	iopub          *types.Socket

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelClient creates a new KernelClient.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(ctx context.Context, spec *gateway.KernelReplicaSpec, info *types.ConnectionInfo) *KernelClient {
	client := &KernelClient{
		id:        spec.Kernel.Id,
		replicaId: spec.ReplicaId,
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

// ReplicaID returns the replica ID.
func (c *KernelClient) ReplicaID() int32 {
	return c.replicaId
}

// Address returns the address of the kernel.
func (c *KernelClient) Address() string {
	return c.client.Meta.IP
}

// String returns a string representation of the client.
func (c *KernelClient) String() string {
	if c.replicaId == 0 {
		return fmt.Sprintf("kernel(%s)", c.id)
	} else {
		return fmt.Sprintf("replica(%s:%d)", c.id, c.replicaId)
	}
}

// Socket returns the serve socket the kernel is listening on.
func (c *KernelClient) Socket(typ types.MessageType) *types.Socket {
	switch typ {
	case types.IOMessage:
		return c.iopub
	case types.ShellMessage:
		return c.shell
	}

	return nil
}

// ConnectionInfo returns the connection info.
func (c *KernelClient) ConnectionInfo() *types.ConnectionInfo {
	return c.client.Meta
}

// Status returns the kernel status.
func (c *KernelClient) Status() types.KernelStatus {
	return c.status
}

// BusyStatus returns the kernel busy status.
func (c *KernelClient) BusyStatus() (string, *zmq4.Msg) {
	return c.busyStatus, c.lastBStatusMsg
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
func (c *KernelClient) InitializeShellForwarder(handler core.KernelMessageHandler) (*types.Socket, error) {
	shell := &types.Socket{
		Socket: zmq4.NewRouter(c.client.Ctx),
		Type:   types.ShellMessage,
	}

	if err := c.client.Listen(shell); err != nil {
		return nil, err
	}

	c.shell = shell
	go c.client.Serve(c, shell, func(srv types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
		msg.Frames, _ = c.BaseServer.AddDestFrame(msg.Frames, c.id, server.JOffsetAutoDetect)
		return handler(c, typ, msg)
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
func (c *KernelClient) RequestWithHandler(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func()) error {
	return c.requestWithHandler(ctx, typ, msg, handler, c.getWaitResponseOption, done)
}

func (c *KernelClient) requestWithHandler(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, getOption server.WaitResponseOptionGetter, done func()) error {
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
	go c.client.WaitResponse(ctx, c, socket, msg, c, func(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
		// Kernel frame is automatically removed.
		err := handler(server.(*KernelClient), typ, msg)
		if done != nil {
			done()
		}
		return err
	}, getOption)
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
			go c.client.Serve(c, socket, socket.Handler)
		}
	}

	return nil
}

func (c *KernelClient) handleMsg(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.IOMessage:
		if c.iopub != nil {
			topic, jFrames := c.extractIOTopicFrame(msg.Frames)
			if topic == types.IOTopicStatus {
				c.handleIOKernelStatus(jFrames, msg)
			}
			c.client.Log.Debug("Forwarding %v message: %v", typ, msg)
			return c.iopub.Socket.Send(*msg)
		} else {
			return ErrIOPubNotStarted
		}
	}

	return ErrHandlerNotImplemented
}

func (c *KernelClient) getWaitResponseOption(key string) interface{} {
	switch key {
	case server.WROptionRemoveDestFrame:
		return c.shell != nil
	}

	return nil
}

func (c *KernelClient) extractIOTopicFrame(frames [][]byte) (topic string, jFrames [][]byte) {
	_, jOffset := c.SkipIdentities(frames)
	if jOffset == 0 {
		return "", frames[jOffset:]
	}

	topic = string(frames[jOffset-1])
	if types.IOTopicStatusRecognizer.MatchString(topic) {
		return types.IOTopicStatus, frames[jOffset:]
	}

	return topic, frames[jOffset:]
}

func (c *KernelClient) handleIOKernelStatus(frames [][]byte, msg *zmq4.Msg) (string, error) {
	var status types.MessageKernelStatus
	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if len(frames) < 5 {
		return "", types.ErrInvalidJupyterMessage
	}
	err := json.Unmarshal(frames[5], &status)
	if err != nil {
		return "", err
	}

	c.busyStatus = status.Status
	c.lastBStatusMsg = msg
	return status.Status, nil
}
