package client

import (
	"context"
	"errors"
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

	ErrResourceSpecAlreadySet = errors.New("kernel already has a resource spec set")
)

type SMRNodeReadyNotificationCallback func(*KernelClient)
type SMRNodeUpdatedNotificationCallback func(*types.MessageSMRNodeUpdated) // For node-added or node-removed notifications.

// KernelReplicaClient offers a simple interface to communicate with a kernel replica.
type KernelReplicaClient interface {
	core.Kernel
	server.Server

	// InitializeIOForwarder initializes the IOPub serving.
	InitializeShellForwarder(handler core.KernelMessageHandler) (*types.Socket, error)

	// InitializeIOForwarder initializes the IOPub serving.
	// Returns Pub socket, Sub socket, error.
	InitializeIOForwarder() (*types.Socket, error)

	// Initialize the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
	// If the provided types.MessageHandler parameter is nil, then we will use the default handler. (The default handler is KernelClient::InitializeIOSub.)
	// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
	InitializeIOSub(handler types.MessageHandler, subscriptionTopic string) (*types.Socket, error)

	// Validate validates the kernel connections.
	// If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
	Validate() error

	ShellListenPort() int

	IOPubListenPort() int

	RequestDestID() string

	// Return the name of the Kubernetes Pod hosting the replica.
	PodName() string

	SourceKernelID() string

	// ReplicaID returns the replica ID.
	ReplicaID() int32

	// AddIOHandler adds a handler for a specific IOPub topic.
	// The handler should return ErrStopPropagation to avoid msg being forwarded to the client.
	AddIOHandler(topic string, handler KernelMessageBrokerHandler) error

	// ID returns the kernel ID.
	ID() string

	// Session returns the associated session ID.
	Sessions() []string

	// Set the ResourceSpec of the kernel.
	SetResourceSpec(spec *gateway.ResourceSpec)

	SetReplicaID(replicaId int32)

	// PersistentID returns the persistent ID.
	PersistentID() string

	// Address returns the address of the kernel.
	Address() string

	// String returns a string representation of the client.
	String() string

	// Returns true if the replica has registered and joined its SMR cluster.
	// Only used by the Cluster Gateway, not by the Local Daemon.
	IsReady() bool

	// Designate the replica as ready.
	// Only used by the Cluster Gateway, not by the Local Daemon.
	SetReady()

	// Socket returns the serve socket the kernel is listening on.
	Socket(typ types.MessageType) *types.Socket

	// ConnectionInfo returns the connection info.
	ConnectionInfo() *types.ConnectionInfo

	// Status returns the kernel status.
	Status() types.KernelStatus

	// BusyStatus returns the kernel busy status.
	BusyStatus() (string, *zmq4.Msg)

	// BindSession binds a session ID to the client.
	BindSession(sess string)

	// RequestWithHandler sends a request and handles the response.
	RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func(), timeout time.Duration) error

	// Take note that we should yield the next execution request.
	YieldNextExecutionRequest()

	// Call after successfully yielding the next execution request.
	YieldedNextExecutionRequest()

	// Return true if we're supposed to yield the next execution request.
	SupposedToYieldNextExecutionRequest() bool

	// Return the ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
	HostId() string
}

// Implementation of the KernelReplicaClient interface.
//
// All sockets except IOPub are connected on dialing.
//
// Each replica of a particular Distributed Kernel will have a corresponding KernelClient.
// These KernelClients are then wrapped/managed by a distributedKernelClientImpl, which is only
// used by the Gateway.
//
// Used by both the Gateway and Local Daemon components.
type KernelClient struct {
	*server.BaseServer
	SessionManager
	client *server.AbstractServer

	id                        string
	replicaId                 int32
	persistentId              string
	spec                      *gateway.KernelSpec
	status                    types.KernelStatus
	busyStatus                string
	lastBStatusMsg            *zmq4.Msg
	iobroker                  *MessageBroker[core.Kernel, *zmq4.Msg, types.JupyterFrames]
	shell                     *types.Socket // Listener.
	iopub                     *types.Socket // Listener.
	addSourceKernelFrames     bool          // If true, then the SUB-type ZMQ socket, which is used as part of the Jupyter IOPub Socket, will set its subscription option to the KernelClient's kernel ID.
	shellListenPort           int           // Port that the KernelClient::shell socket listens on.
	iopubListenPort           int           // Port that the KernelClient::iopub socket listens on.
	kernelPodName             string        // Name of the Pod housing the associated distributed kernel replica container.
	kubernetesNodeName        string        // Name of the node that the Pod is running on.
	ready                     bool          // True if the replica has registered and joined its SMR cluster. Only used by the Cluster Gateway, not by the Local Daemon.
	yieldNextExecutionRequest bool          // If true, then we will yield the next 'execute_request'.
	hostId                    string        // The ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).

	smrNodeReadyCallback SMRNodeReadyNotificationCallback
	smrNodeAddedCallback SMRNodeUpdatedNotificationCallback
	// smrNodeRemovedCallback SMRNodeUpdatedNotificationCallback

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelClient creates a new KernelClient.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(ctx context.Context, spec *gateway.KernelReplicaSpec, info *types.ConnectionInfo, addSourceKernelFrames bool, shellListenPort int, iopubListenPort int, kernelPodName string, kubernetesNodeName string, smrNodeReadyCallback SMRNodeReadyNotificationCallback, smrNodeAddedCallback SMRNodeUpdatedNotificationCallback, persistentId string, hostId string, shouldSendAcks bool) *KernelClient {
	client := &KernelClient{
		id:                        spec.Kernel.Id,
		persistentId:              persistentId,
		replicaId:                 spec.ReplicaId,
		spec:                      spec.Kernel,
		addSourceKernelFrames:     addSourceKernelFrames,
		shellListenPort:           shellListenPort,
		iopubListenPort:           iopubListenPort,
		kernelPodName:             kernelPodName,
		kubernetesNodeName:        kubernetesNodeName,
		smrNodeReadyCallback:      smrNodeReadyCallback,
		smrNodeAddedCallback:      smrNodeAddedCallback,
		yieldNextExecutionRequest: false,
		hostId:                    hostId,
		// smrNodeRemovedCallback: smrNodeRemovedCallback,
		client: server.New(ctx, info, shouldSendAcks, func(s *server.AbstractServer) {
			// We do not set handlers of the sockets here. So no server routine will be started on dialing.
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.StdinPort}
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.HBPort}
			// s.Sockets.Ack = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.AckPort}
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

// Return the name of the Kubernetes Pod hosting the replica.
func (c *KernelClient) PodName() string {
	return c.kernelPodName
}

// Name of the node that the Pod is running on.
func (c *KernelClient) NodeName() string {
	return c.kubernetesNodeName
}

func (c *KernelClient) ShellListenPort() int {
	return c.shellListenPort
}

func (c *KernelClient) IOPubListenPort() int {
	return c.iopubListenPort
}

// Take note that we should yield the next execution request.
func (c *KernelClient) YieldNextExecutionRequest() {
	c.yieldNextExecutionRequest = true
}

// Call after successfully yielding the next execution request.
func (c *KernelClient) YieldedNextExecutionRequest() {
	c.yieldNextExecutionRequest = false
}

func (c *KernelClient) SupposedToYieldNextExecutionRequest() bool {
	return c.yieldNextExecutionRequest
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

func (c *KernelClient) SetReplicaID(replicaId int32) {
	c.replicaId = replicaId
}

// Set the value of the persistentId field.
// This will panic if the persistentId has already been set to something other than the empty string.
func (c *KernelClient) SetPersistentID(persistentId string) {
	if c.persistentId != "" {
		panic(fmt.Sprintf("Cannot set persistent ID of kernel %s. Persistent ID already set to value: '%s'", c.id, c.persistentId))
	}
	c.persistentId = persistentId
}

// PersistentID returns the persistent ID.
func (c *KernelClient) PersistentID() string {
	return c.persistentId
}

// Spec returns the resource spec
func (c *KernelClient) ResourceSpec() *gateway.ResourceSpec {
	return c.spec.GetResourceSpec()
}

func (c *KernelClient) SetResourceSpec(spec *gateway.ResourceSpec) {
	c.spec.ResourceSpec = spec
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

// Returns true if the replica has registered and joined its SMR cluster.
// Only used by the Cluster Gateway, not by the Local Daemon.
func (c *KernelClient) IsReady() bool {
	return c.ready
}

// Return the ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
func (c *KernelClient) HostId() string {
	return c.hostId
}

// Designate the replica as ready.
// Only used by the Cluster Gateway, not by the Local Daemon.
func (c *KernelClient) SetReady() {
	c.ready = true
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
	c.log.Info("Binded session %s to kernel client", sess)
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
	go c.client.Serve(c, shell, c, func(srv types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
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
	c.iobroker.Subscribe(types.IOTopicSMRNodeAdded, c.handleIOKernelSMRNodeAdded)
	// c.iobroker.Subscribe(types.IOTopicSMRNodeRemoved, c.handleIOKernelSMRNodeRemoved)
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
func (c *KernelClient) RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func(), timeout time.Duration) error {
	// c.log.Debug("%s %v request(%p): %v", prompt, typ, msg, msg)
	return c.requestWithHandler(ctx, typ, msg, handler, c.getWaitResponseOption, done, timeout)
}

func (c *KernelClient) requestWithHandler(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, getOption server.WaitResponseOptionGetter, done func(), timeout time.Duration) error {
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
	}, done, getOption, timeout, true)
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

		c.log.Debug("Successfully dialed %s socket at %s.", socket.Type.String(), fmt.Sprintf(address, socket.Port))
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, socket := range sockets {
		if socket != nil && socket.Handler != nil {
			c.log.Debug("Beginning to serve socket %v.", socket.Type.String())
			go c.client.Serve(c, socket, c, socket.Handler)
		}
	}

	return nil
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

	// c.log.Debug("Handling IO Kernel Status for Kernel %v, Status %v.", kernel.ID(), status.Status)

	c.busyStatus = status.Status
	c.lastBStatusMsg = msg
	// Return nil so that we forward the message to the client.
	return nil
}

// func (c *KernelClient) handleIOKernelSMRNodeRemoved(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
// 	c.log.Debug("Handling IO Kernel SMR Node-Removed message...")
// 	var node_removed_message types.MessageSMRNodeUpdated
// 	if err := frames.Validate(); err != nil {
// 		return err
// 	}
// 	err := frames.DecodeContent(&node_removed_message)
// 	if err != nil {
// 		return err
// 	}

// 	c.log.Debug("Handling IO Kernel SMR Node-Removed message for replica %d of kernel %s.", node_removed_message.NodeID, node_removed_message.KernelId)

// 	if c.smrNodeRemovedCallback != nil {
// 		c.smrNodeRemovedCallback(&node_removed_message)
// 	}

// 	return types.ErrStopPropagation
// }

func (c *KernelClient) handleIOKernelSMRNodeAdded(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var node_added_message types.MessageSMRNodeUpdated
	if err := frames.Validate(); err != nil {
		return err
	}
	err := frames.DecodeContent(&node_added_message)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel SMR Node-Added message for replica %d of kernel %s.", node_added_message.NodeID, node_added_message.KernelId)

	if c.smrNodeAddedCallback != nil {
		c.smrNodeAddedCallback(&node_added_message)
	}

	return types.ErrStopPropagation
}

func (c *KernelClient) handleIOKernelSMRReady(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var node_ready_message types.MessageSMRReady
	if err := frames.Validate(); err != nil {
		return err
	}
	err := frames.DecodeContent(&node_ready_message)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel SMR Ready for Kernel %v, PersistentID %v.", kernel.ID(), node_ready_message.PersistentID)

	c.persistentId = node_ready_message.PersistentID
	c.log.Debug("Persistent ID confirmed: %v", c.persistentId)

	if c.smrNodeReadyCallback != nil {
		c.smrNodeReadyCallback(c)
	}

	return types.ErrStopPropagation
}
