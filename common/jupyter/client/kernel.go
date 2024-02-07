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
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	protocol "github.com/zhangjyr/distributed-notebook/common/types"
)

var (
	heartbeatInterval = time.Second
)

type SMRReadyCallback func(*KernelClient)

// KernelClient offers a simple interface to communicate with a kernel.
// All sockets except IOPub are connected on dialing.
//
// Each replica of a particular Distributed Kernel will have a corresponding KernelClient.
// These KernelClients are then wrapped/managed by a DistributedKernelClient, which is only
// used by the Gateway.
//
// Used by both the Gateway and Local Daemon components.
type KernelClient struct {
	*server.BaseServer
	*SessionManager
	client *server.AbstractServer

	id                    string
	replicaId             int32
	persistentId          string
	spec                  *gateway.KernelSpec
	status                types.KernelStatus
	busyStatus            string
	lastBStatusMsg        *zmq4.Msg
	iobroker              *MessageBroker[core.Kernel, *zmq4.Msg, types.JupyterFrames]
	shell                 *types.Socket // Listener.
	iopub                 *types.Socket // Listener.
	addSourceKernelFrames bool          // If true, then the SUB-type ZMQ socket, which is used as part of the Jupyter IOPub Socket, will set its subscription option to the KernelClient's kernel ID.
	shellListenPort       int           // Port that the KernelClient::shell socket listens on.
	iopubListenPort       int           // Port that the KernelClient::iopub socket listens on.
	kernelPodName         string        // Name of the Pod housing the associated distributed kernel replica container.

	smrReadyCallback SMRReadyCallback

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelClient creates a new KernelClient.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(ctx context.Context, spec *gateway.KernelReplicaSpec, info *types.ConnectionInfo, addSourceKernelFrames bool, shellListenPort int, iopubListenPort int, kernelPodName string, smrReadyCallback SMRReadyCallback) *KernelClient {
	client := &KernelClient{
		id:                    spec.Kernel.Id,
		replicaId:             spec.ReplicaId,
		spec:                  spec.Kernel,
		addSourceKernelFrames: addSourceKernelFrames,
		shellListenPort:       shellListenPort,
		iopubListenPort:       iopubListenPort,
		kernelPodName:         kernelPodName,
		smrReadyCallback:      smrReadyCallback,
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

	client.log.Debug("Created new Kernel Client with spec %v, connection info %v.", spec, info)

	return client
}

func (c *KernelClient) KernelPodName() string {
	return c.kernelPodName
}

func (c *KernelClient) ShellListenPort() int {
	return c.shellListenPort
}

func (c *KernelClient) IOPubListenPort() int {
	return c.iopubListenPort
}

// ID returns the kernel ID.
func (c *KernelClient) ID() string {
	return c.id
}

func (c *KernelClient) RequestDestID() string {
	return c.id
}

func (c *KernelClient) SourceKernelID() string {
	return c.id
}

// ReplicaID returns the replica ID.
func (c *KernelClient) ReplicaID() int32 {
	return c.replicaId
}

// ReplicaID returns the replica ID.
func (c *KernelClient) SetReplicaID(replicaId int32) {
	c.replicaId = replicaId
}

// PersistentID returns the persistent ID.
func (c *KernelClient) PersistentID() string {
	return c.persistentId
}

// Spec returns the resource spec
func (c *KernelClient) Spec() protocol.Spec {
	return c.spec.Resource
}

// KernelSpec returns the kernel spec.
func (c *KernelClient) KernelSpec() *gateway.KernelSpec {
	return c.spec
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
		Port:   c.shellListenPort,
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
// Returns Pub socket, Sub socket, error.
func (c *KernelClient) InitializeIOForwarder() (*types.Socket, error) {
	iopub := &types.Socket{
		Socket: zmq4.NewPub(c.client.Ctx),
		Port:   c.iopubListenPort, // c.client.Meta.IOSubPort,
		Type:   types.IOMessage,
	}

	c.log.Debug("Created ZeroMQ PUB socket with port %d.", iopub.Port)

	if err := c.client.Listen(iopub); err != nil {
		return nil, err
	}

	c.iopub = iopub
	c.iobroker = NewMessageBroker[core.Kernel](c.extractIOTopicFrame)
	c.iobroker.Subscribe(MessageBrokerAllTopics, c.forwardIOMessage) // Default to forward all messages.
	c.iobroker.Subscribe(types.IOTopicStatus, c.handleIOKernelStatus)
	c.iobroker.Subscribe(types.IOTopicSMRReady, c.handleIOKernelSMRReady)
	return iopub, nil
}

// AddIOHandler adds a handler for a specific IOPub topic.
// The handler should return ErrStopPropagation to avoid msg being forwarded to the client.
func (c *KernelClient) AddIOHandler(topic string, handler MessageBrokerHandler[core.Kernel, types.JupyterFrames, *zmq4.Msg]) error {
	if c.iobroker == nil {
		return ErrIOPubNotStarted
	}
	c.iobroker.Subscribe(topic, handler)
	return nil
}

// RequestWithHandler sends a request and handles the response.
func (c *KernelClient) RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func()) error {
	c.log.Debug("%s %v request(%p): %v", prompt, typ, msg, msg)
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

	// Add timeout if necessary.
	c.client.Request(ctx, c, socket, msg, c, c, func(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) (err error) {
		// Kernel frame is automatically removed.
		if handler != nil {
			err = handler(server.(*KernelClient), typ, msg)
		}
		return err
	}, done, getOption)
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

// Initialize the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
// If the provided types.MessageHandler parameter is nil, then we will use the default handler.
// (The default handler is KernelClient::InitializeIOSub.)
//
// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
func (c *KernelClient) InitializeIOSub(handler types.MessageHandler, subscriptionTopic string) (*types.Socket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Default to KernelClient::handleMsg if the provided handler is null.
	if handler == nil {
		c.log.Debug("Creating ZeroMQ SUB socket: default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
		handler = c.handleMsg
	} else {
		c.log.Debug("Creating ZeroMQ SUB socket: non-default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
	}

	// Handler is set, so server routing will be started on dialing.
	c.client.Sockets.IO = &types.Socket{
		Socket:  zmq4.NewSub(c.client.Ctx), // Sub socket for client.
		Port:    c.client.Meta.IOPubPort,
		Type:    types.IOMessage,
		Handler: handler,
	}

	c.client.Sockets.IO.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
	c.client.Sockets.All[types.IOMessage] = c.client.Sockets.IO

	if c.status == types.KernelStatusRunning {
		if err := c.dial(c.client.Sockets.IO); err != nil {
			return nil, err
		}
	}

	c.log.Debug("ZeroMQ SUB socket has port %d", c.client.Sockets.IO.Port)

	return c.client.Sockets.IO, nil
}

// dial connects to specified sockets
func (c *KernelClient) dial(sockets ...*types.Socket) error {
	// Start listening on all specified sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.client.Meta.Transport, c.client.Meta.IP)
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		c.log.Debug("Dialing %s socket at %s now...", socket.Type.String(), fmt.Sprintf(address, socket.Port))

		err := socket.Socket.Dial(fmt.Sprintf(address, socket.Port))
		if err != nil {
			return fmt.Errorf("could not connect to kernel socket(port:%d): %w", socket.Port, err)
		}
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, socket := range sockets {
		if socket != nil && socket.Handler != nil {
			c.log.Debug("Beginning to serve socket %v.", socket.Type.String())
			go c.client.Serve(c, socket, socket.Handler)
		}
	}

	return nil
}

func (c *KernelClient) handleMsgWithKernelSourceFrame(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
	// c.log.Debug("Received message of type %v: \"%v\"", typ.String(), msg)
	switch typ {
	case types.IOMessage:
		// msg.Frames = c.RemoveSourceKernelFrame(msg.Frames, -1)

		if c.iopub != nil {
			return c.iobroker.Publish(c, msg)
		} else {
			return ErrIOPubNotStarted
		}
	}

	return ErrHandlerNotImplemented
}

func (c *KernelClient) handleMsg(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
	// c.log.Debug("Received message of type %v: \"%v\"", typ.String(), msg)
	switch typ {
	case types.IOMessage:
		if c.iopub != nil {
			return c.iobroker.Publish(c, msg)
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
		// case server.WROptionRemoveSourceKernelFrame:
		// 	return c.removeSourceKernelFramesFromMessages
	}

	return nil
}

func (c *KernelClient) extractIOTopicFrame(msg *zmq4.Msg) (topic string, jFrames types.JupyterFrames) {
	_, jOffset := c.SkipIdentities(msg.Frames)
	jFrames = msg.Frames[jOffset:]
	if jOffset == 0 {
		return "", jFrames
	}

	rawTopic := msg.Frames[jOffset-1]
	matches := types.IOTopicStatusRecognizer.FindSubmatch(rawTopic)
	if len(matches) > 0 {
		return string(matches[2]), jFrames
	}

	return string(rawTopic), jFrames
}

func (c *KernelClient) forwardIOMessage(kernel core.Kernel, _ types.JupyterFrames, msg *zmq4.Msg) error {
	// c.client.Log.Debug("Forwarding %v message for Kernel \"%s\".", types.IOMessage, c.id)
	// if c.addSourceKernelFrames {
	// 	c.client.Log.Debug("Adding \"Source Kernel\" frame to %v message that we're forwarding.", types.IOMessage)
	// 	msg.Frames = c.AddSourceKernelFrame(msg.Frames, c.SourceKernelID(), server.JOffsetAutoDetect)
	// }

	// c.client.Log.Debug("%v message to be forwarded (for kernel \"%s\"): %v", types.IOMessage, c.id, msg)

	return kernel.Socket(types.IOMessage).Send(*msg)
}

func (c *KernelClient) handleIOKernelStatus(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var status types.MessageKernelStatus
	if err := frames.Validate(); err != nil {
		return err
	}
	err := frames.DecodeContent(&status)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel Status for Kernel %v, Status %v.", kernel.ID(), status.Status)

	c.busyStatus = status.Status
	c.lastBStatusMsg = msg
	// Return nil so that we forward the message to the client.
	return nil
}

func (c *KernelClient) handleIOKernelSMRReady(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var ready types.MessageSMRReady
	if err := frames.Validate(); err != nil {
		return err
	}
	err := frames.DecodeContent(&ready)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel SMR Ready for Kernel %v, PersistentID %v.", kernel.ID(), ready.PersistentID)

	c.persistentId = ready.PersistentID
	c.log.Debug("Persistent ID confirmed: %v", c.persistentId)

	if c.smrReadyCallback != nil {
		c.smrReadyCallback(c)
	}

	return types.ErrStopPropagation
}
