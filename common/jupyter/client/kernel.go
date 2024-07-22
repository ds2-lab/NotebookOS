package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	heartbeatInterval = time.Second

	ErrResourceSpecAlreadySet = errors.New("kernel already has a resource spec set")
)

type SMRNodeReadyNotificationCallback func(KernelReplicaClient)
type SMRNodeUpdatedNotificationCallback func(*types.MessageSMRNodeUpdated) // For node-added or node-removed notifications.

// KernelReplicaClient offers a simple interface to communicate with a kernel replica.
type KernelReplicaClient interface {
	core.Kernel
	server.Server
	server.SourceKernel
	server.Sender

	// InitializeIOForwarder initializes the IOPub serving.
	InitializeShellForwarder(handler core.KernelMessageHandler) (*types.Socket, error)

	// InitializeIOForwarder initializes the IOPub serving.
	// Returns Pub socket, Sub socket, error.
	InitializeIOForwarder() (*types.Socket, error)

	// Initialize the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
	// If the provided types.MessageHandler parameter is nil, then we will use the default handler. (The default handler is kernelReplicaClientImpl::InitializeIOSub.)
	// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
	InitializeIOSub(handler types.MessageHandler, subscriptionTopic string) (*types.Socket, error)

	ShellListenPort() int

	IOPubListenPort() int

	// Get the Host on which the replica is hosted.
	GetHost() core.Host

	// Set the Host of the kernel.
	SetHost(core.Host)

	RequestDestID() string

	// Return the name of the Kubernetes Pod hosting the replica.
	PodName() string

	SourceKernelID() string

	// ReplicaID returns the replica ID.
	ReplicaID() int32

	NodeName() string

	SetPersistentID(string)

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

	// Status returns the kernel status.
	Status() types.KernelStatus

	// BusyStatus returns the kernel busy status.
	BusyStatus() (string, *zmq4.Msg)

	// BindSession binds a session ID to the client.
	BindSession(sess string)

	// Take note that we should yield the next execution request.
	YieldNextExecutionRequest()

	// Call after successfully yielding the next execution request.
	YieldedNextExecutionRequest()

	// Return true if we're supposed to yield the next execution request.
	SupposedToYieldNextExecutionRequest() bool

	// Return the ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
	HostId() string
}

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
type ConnectionRevalidationFailedCallback func(replica KernelReplicaClient, msg *zmq4.Msg, err error)

// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
type ResubmissionAfterSuccessfulRevalidationFailedCallback func(replica KernelReplicaClient, msg *zmq4.Msg, err error)

// Implementation of the KernelReplicaClient interface.
//
// All sockets except IOPub are connected on dialing.
//
// Each replica of a particular Distributed Kernel will have a corresponding kernelReplicaClientImpl.
// These KernelClients are then wrapped/managed by a distributedKernelClientImpl, which is only
// used by the Gateway.
//
// Used by both the Gateway and Local Daemon components.
type kernelReplicaClientImpl struct {
	*server.BaseServer
	SessionManager
	client *server.AbstractServer

	// destMutex                 sync.Mutex
	id                        string
	replicaId                 int32
	persistentId              string
	spec                      *gateway.KernelSpec
	status                    types.KernelStatus
	busyStatus                string
	lastBStatusMsg            *zmq4.Msg
	iobroker                  *MessageBroker[core.Kernel, *zmq4.Msg, types.JupyterFrames]
	shell                     *types.Socket         // Listener.
	iopub                     *types.Socket         // Listener.
	addSourceKernelFrames     bool                  // If true, then the SUB-type ZMQ socket, which is used as part of the Jupyter IOPub Socket, will set its subscription option to the kernelReplicaClientImpl's kernel ID.
	shellListenPort           int                   // Port that the kernelReplicaClientImpl::shell socket listens on.
	iopubListenPort           int                   // Port that the kernelReplicaClientImpl::iopub socket listens on.
	kernelPodName             string                // Name of the Pod housing the associated distributed kernel replica container.
	kubernetesNodeName        string                // Name of the node that the Pod is running on.
	ready                     bool                  // True if the replica has registered and joined its SMR cluster. Only used by the Cluster Gateway, not by the Local Daemon.
	yieldNextExecutionRequest bool                  // If true, then we will yield the next 'execute_request'.
	hostId                    string                // The ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
	host                      core.Host             // The host that the kernel replica is running on.
	connectionInfo            *types.ConnectionInfo // Connection information of the remote kernel (or kernel client) whom we're connecting to.

	// Callback for when we try to forward a message to a kernel replica, don't get back any ACKs, and then fail to reconnect.
	connectionRevalidationFailedCallback ConnectionRevalidationFailedCallback

	// If we successfully reconnect to a kernel and then fail to send the message again, then we call this.
	resubmissionAfterSuccessfulRevalidationFailedCallback ResubmissionAfterSuccessfulRevalidationFailedCallback

	smrNodeReadyCallback SMRNodeReadyNotificationCallback
	smrNodeAddedCallback SMRNodeUpdatedNotificationCallback
	// smrNodeRemovedCallback SMRNodeUpdatedNotificationCallback

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelClient creates a new kernelReplicaClientImpl.
// The client will intialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
func NewKernelClient(ctx context.Context, spec *gateway.KernelReplicaSpec, info *types.ConnectionInfo, addSourceKernelFrames bool, shellListenPort int, iopubListenPort int, kernelPodName string, kubernetesNodeName string, smrNodeReadyCallback SMRNodeReadyNotificationCallback, smrNodeAddedCallback SMRNodeUpdatedNotificationCallback, persistentId string, hostId string, host core.Host, shouldAckMessages bool, connectionRevalidationFailedCallback ConnectionRevalidationFailedCallback, resubmissionAfterSuccessfulRevalidationFailedCallback ResubmissionAfterSuccessfulRevalidationFailedCallback) KernelReplicaClient {
	client := &kernelReplicaClientImpl{
		id:                                   spec.Kernel.Id,
		persistentId:                         persistentId,
		replicaId:                            spec.ReplicaId,
		spec:                                 spec.Kernel,
		addSourceKernelFrames:                addSourceKernelFrames,
		shellListenPort:                      shellListenPort,
		iopubListenPort:                      iopubListenPort,
		kernelPodName:                        kernelPodName,
		kubernetesNodeName:                   kubernetesNodeName,
		smrNodeReadyCallback:                 smrNodeReadyCallback,
		smrNodeAddedCallback:                 smrNodeAddedCallback,
		yieldNextExecutionRequest:            false,
		host:                                 host,
		hostId:                               hostId,
		connectionInfo:                       info,
		connectionRevalidationFailedCallback: connectionRevalidationFailedCallback,
		resubmissionAfterSuccessfulRevalidationFailedCallback: resubmissionAfterSuccessfulRevalidationFailedCallback,
		client: server.New(ctx, info, func(s *server.AbstractServer) {
			// We do not set handlers of the sockets here. So no server routine will be started on dialing.
			s.Sockets.Control = types.NewSocket(zmq4.NewDealer(s.Ctx), info.ControlPort, types.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", spec.Kernel.Id))
			s.Sockets.Shell = types.NewSocket(zmq4.NewDealer(s.Ctx), info.ShellPort, types.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", spec.Kernel.Id))
			s.Sockets.Stdin = types.NewSocket(zmq4.NewDealer(s.Ctx), info.StdinPort, types.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", spec.Kernel.Id))
			s.Sockets.HB = types.NewSocket(zmq4.NewDealer(s.Ctx), info.HBPort, types.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", spec.Kernel.Id))
			s.ReconnectOnAckFailure = true
			s.PrependId = false
			s.Name = fmt.Sprintf("kernelReplicaClientImpl-%s", spec.Kernel.Id)
			s.ShouldAckMessages = shouldAckMessages
			// s.Sockets.Ack = types.NewSocket(Socket: zmq4.NewReq(s.Ctx), Port: info.AckPort}
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

// Recreate and return the kernel's control socket.
// This reuses the handler on the existing/previous control socket.
func (c *kernelReplicaClientImpl) recreateControlSocket() *types.Socket {
	handler, err := c.closeSocket(types.ControlMessage)
	if err != nil {
		c.log.Error("Could not find control socket on Client... Handler of new socket will be nil.")
	}

	new_socket := types.NewSocketWithHandler(zmq4.NewDealer(c.client.Ctx), c.connectionInfo.ControlPort, types.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", c.id), handler)
	c.client.Sockets.Control = new_socket
	c.client.Sockets.All[types.ControlMessage] = new_socket
	return new_socket
}

// Recreate and return the kernel's shell socket.
// This reuses the handler on the existing/previous shell socket.
func (c *kernelReplicaClientImpl) recreateShellSocket() *types.Socket {
	handler, err := c.closeSocket(types.ShellMessage)
	if err != nil {
		c.log.Error("Could not find shell socket on Client... Handler of new socket will be nil.")
	}

	new_socket := types.NewSocketWithHandler(zmq4.NewDealer(c.client.Ctx), c.connectionInfo.ShellPort, types.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", c.id), handler)
	c.client.Sockets.Shell = new_socket
	c.client.Sockets.All[types.ShellMessage] = new_socket
	return new_socket
}

// Recreate and return the kernel's stdin socket.
// This reuses the handler on the existing/previous stdin socket.
func (c *kernelReplicaClientImpl) recreateStdinSocket() *types.Socket {
	handler, err := c.closeSocket(types.StdinMessage)
	if err != nil {
		c.log.Error("Could not find stdin socket on Client... Handler of new socket will be nil.")
	}

	new_socket := types.NewSocketWithHandler(zmq4.NewDealer(c.client.Ctx), c.connectionInfo.StdinPort, types.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", c.id), handler)
	c.client.Sockets.Stdin = new_socket
	c.client.Sockets.All[types.StdinMessage] = new_socket
	return new_socket
}

// Recreate and return the kernel's heartbeat socket.
// This reuses the handler on the existing/previous heartbeat socket.
func (c *kernelReplicaClientImpl) recreateHeartbeatSocket() *types.Socket {
	handler, err := c.closeSocket(types.HBMessage)
	if err != nil {
		c.log.Error("Could not find heartbeat socket on Client... Handler of new socket will be nil.")
	}

	new_socket := types.NewSocketWithHandler(zmq4.NewDealer(c.client.Ctx), c.connectionInfo.HBPort, types.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", c.id), handler)
	c.client.Sockets.HB = new_socket
	c.client.Sockets.All[types.HBMessage] = new_socket
	return new_socket
}

// Close the socket of the specified type, returning the handler set for that socket.
func (c *kernelReplicaClientImpl) closeSocket(typ types.MessageType) (types.MessageHandler, error) {
	if c.client != nil && c.client.Sockets.All[typ] != nil {
		oldSocket := c.client.Sockets.All[typ]
		handler := oldSocket.Handler

		if atomic.LoadInt32(&oldSocket.Serving) == 1 {
			c.log.Debug("Sending 'stop-serving' notification to %s socket.", typ.String())
			oldSocket.StopServingChan <- struct{}{}
			c.log.Debug("Sent 'stop-serving' notification to %s socket.", typ.String())
		}

		err := oldSocket.Close()
		if err != nil {
			// Print the error, but that's all. We're recreating the socket anyway.
			c.log.Warn("Error while closing %s socket: %v", typ.String(), err)
		}

		return handler, nil
	} else {
		return nil, types.ErrSocketNotAvailable
	}
}

// This is called when the replica ID is set/changed, so that the logger's prefix reflects the replica ID.
func (c *kernelReplicaClientImpl) updateLogPrefix() {
	if c.client.Log == nil {
		return
	}

	c.client.Log.(*logger.ColorLogger).Prefix = fmt.Sprintf("Replica %s:%d ", c.id, c.replicaId)
}

// Return the name of the Kubernetes Pod hosting the replica.
func (c *kernelReplicaClientImpl) PodName() string {
	return c.kernelPodName
}

// Name of the node that the Pod is running on.
func (c *kernelReplicaClientImpl) NodeName() string {
	return c.kubernetesNodeName
}

func (c *kernelReplicaClientImpl) ShellListenPort() int {
	return c.shellListenPort
}

func (c *kernelReplicaClientImpl) IOPubListenPort() int {
	return c.iopubListenPort
}

// Take note that we should yield the next execution request.
func (c *kernelReplicaClientImpl) YieldNextExecutionRequest() {
	c.yieldNextExecutionRequest = true
}

// Call after successfully yielding the next execution request.
func (c *kernelReplicaClientImpl) YieldedNextExecutionRequest() {
	c.yieldNextExecutionRequest = false
}

func (c *kernelReplicaClientImpl) SupposedToYieldNextExecutionRequest() bool {
	return c.yieldNextExecutionRequest
}

// ID returns the kernel ID.
func (c *kernelReplicaClientImpl) ID() string {
	return c.id
}

func (c *kernelReplicaClientImpl) RequestDestID() string {
	return c.id
}

func (c *kernelReplicaClientImpl) SourceKernelID() string {
	return c.id
}

// ReplicaID returns the replica ID.
func (c *kernelReplicaClientImpl) ReplicaID() int32 {
	return c.replicaId
}

func (c *kernelReplicaClientImpl) SetReplicaID(replicaId int32) {
	c.replicaId = replicaId

	c.updateLogPrefix()
}

// Set the value of the persistentId field.
// This will panic if the persistentId has already been set to something other than the empty string.
func (c *kernelReplicaClientImpl) SetPersistentID(persistentId string) {
	if c.persistentId != "" {
		panic(fmt.Sprintf("Cannot set persistent ID of kernel %s. Persistent ID already set to value: '%s'", c.id, c.persistentId))
	}
	c.persistentId = persistentId
}

// PersistentID returns the persistent ID.
func (c *kernelReplicaClientImpl) PersistentID() string {
	return c.persistentId
}

// Spec returns the resource spec
func (c *kernelReplicaClientImpl) ResourceSpec() *gateway.ResourceSpec {
	return c.spec.GetResourceSpec()
}

func (c *kernelReplicaClientImpl) SetResourceSpec(spec *gateway.ResourceSpec) {
	c.spec.ResourceSpec = spec
}

// KernelSpec returns the kernel spec.
func (c *kernelReplicaClientImpl) KernelSpec() *gateway.KernelSpec {
	return c.spec
}

// Address returns the address of the kernel.
func (c *kernelReplicaClientImpl) Address() string {
	return c.client.Meta.IP
}

// String returns a string representation of the client.
func (c *kernelReplicaClientImpl) String() string {
	if c.replicaId == 0 {
		return fmt.Sprintf("kernel(%s)", c.id)
	} else {
		return fmt.Sprintf("replica(%s:%d)", c.id, c.replicaId)
	}
}

// Returns true if the replica has registered and joined its SMR cluster.
// Only used by the Cluster Gateway, not by the Local Daemon.
func (c *kernelReplicaClientImpl) IsReady() bool {
	return c.ready
}

// Return the ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
func (c *kernelReplicaClientImpl) HostId() string {
	return c.hostId
}

// Designate the replica as ready.
// Only used by the Cluster Gateway, not by the Local Daemon.
func (c *kernelReplicaClientImpl) SetReady() {
	c.log.Debug("Kernel %s-%d has been designated as ready.", c.id, c.replicaId)
	c.ready = true
}

// Socket returns the serve socket the kernel is listening on.
func (c *kernelReplicaClientImpl) Socket(typ types.MessageType) *types.Socket {
	switch typ {
	case types.IOMessage:
		return c.iopub
	case types.ShellMessage:
		return c.shell
	}

	return nil
}

// ConnectionInfo returns the connection info.
func (c *kernelReplicaClientImpl) ConnectionInfo() *types.ConnectionInfo {
	return c.client.Meta
}

// Status returns the kernel status.
func (c *kernelReplicaClientImpl) Status() types.KernelStatus {
	return c.status
}

// BusyStatus returns the kernel busy status.
func (c *kernelReplicaClientImpl) BusyStatus() (string, *zmq4.Msg) {
	return c.busyStatus, c.lastBStatusMsg
}

// BindSession binds a session ID to the client.
func (c *kernelReplicaClientImpl) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	c.log.Info("Binded session %s to kernel client", sess)
}

// Recreate and redial a particular socket.
func (c *kernelReplicaClientImpl) ReconnectSocket(typ types.MessageType) error {
	var socket *types.Socket

	c.log.Debug("Recreating %s socket.", typ.String())
	switch typ {
	case types.ControlMessage:
		{
			socket = c.recreateControlSocket()
		}
	case types.ShellMessage:
		{
			socket = c.recreateShellSocket()
		}
	case types.HBMessage:
		{
			socket = c.recreateStdinSocket()
		}
	case types.StdinMessage:
		{
			socket = c.recreateHeartbeatSocket()
		}
	}

	timer := time.NewTimer(0)

	for {
		select {
		case <-c.client.Ctx.Done():
			c.log.Debug("Could not reconnect %s socket; kernel has exited.", typ.String())
			c.status = types.KernelStatusExited
			return types.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()

			c.log.Debug("Attempting to reconnect on %s socket.", typ.String())
			if err := c.dial(socket); err != nil {
				c.client.Log.Error("Failed to reconnect %v socket: %v", typ, err)
				c.Close()
				c.status = types.KernelStatusExited
				c.mu.Unlock()
				return err
			}

			c.mu.Unlock()
			return nil
		}
	}
}

// Validate validates the kernel connections. If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
func (c *kernelReplicaClientImpl) Validate() error {
	if c.status >= types.KernelStatusRunning { /* If we're already connected, then just return, unless `forceReconnect` is true */
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
			// Wait for heartbeat connection.
			if err := c.dial(nil, c.client.Sockets.HB); err != nil {
				c.client.Log.Warn("Failed to dial heartbeat (%v:%v), retrying...", c.client.Sockets.HB.Addr(), err)
				timer.Reset(heartbeatInterval)
				c.mu.Unlock()
				break // break select
			}
			c.client.Log.Debug("Heartbeat connected")

			// Dial all other sockets.
			// If IO socket is set previously and has not been dialed because kernel is not ready, then dial it now.
			if err := c.dial(c.client.Sockets.All[types.HBMessage+1:]...); err != nil {
				c.client.Log.Error("Failed to dial at least one socket: %v", err)
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

func (c *kernelReplicaClientImpl) InitializeShellForwarder(handler core.KernelMessageHandler) (*types.Socket, error) {
	c.log.Debug("Initializing shell forwarder for kernel client.")

	shell := types.NewSocket(zmq4.NewRouter(c.client.Ctx), c.shellListenPort, types.ShellMessage, fmt.Sprintf("K-Router-ShellForwrder[%s]", c.id))
	if err := c.client.Listen(shell); err != nil {
		return nil, err
	}

	c.shell = shell
	go c.client.Serve(c, shell, c, func(srv types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
		msg.Frames, _ = c.BaseServer.AddDestFrame(msg.Frames, c.id, jupyter.JOffsetAutoDetect)
		return handler(c, typ, msg)
	}, true /* Kernel clients should ACK messages that they're forwarding. */)

	return shell, nil
}

// func (c *kernelReplicaClientImpl) Unlock() {
// 	c.destMutex.Unlock()
// }

// func (c *kernelReplicaClientImpl) Lock() {
// 	c.destMutex.Lock()
// }

// InitializeIOForwarder initializes the IOPub serving.
// Returns Pub socket, Sub socket, error.
func (c *kernelReplicaClientImpl) InitializeIOForwarder() (*types.Socket, error) {
	iopub := types.NewSocket(zmq4.NewPub(c.client.Ctx), c.iopubListenPort /* c.client.Meta.IOSubPort */, types.IOMessage, fmt.Sprintf("K-Pub-IOForwrder[%s]", c.id))

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
func (c *kernelReplicaClientImpl) AddIOHandler(topic string, handler MessageBrokerHandler[core.Kernel, types.JupyterFrames, *zmq4.Msg]) error {
	if c.iobroker == nil {
		return ErrIOPubNotStarted
	}
	c.iobroker.Subscribe(topic, handler)
	return nil
}

// RequestWithHandler sends a request and handles the response.
func (c *kernelReplicaClientImpl) RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, done func()) error {
	// c.log.Debug("%s %v request(%p): %v", prompt, typ, msg, msg)
	return c.requestWithHandler(ctx, typ, msg, handler, c.getWaitResponseOption, done)
}

func (c *kernelReplicaClientImpl) requestWithHandler(ctx context.Context, typ types.MessageType, msg *zmq4.Msg, handler core.KernelMessageHandler, getOption server.WaitResponseOptionGetter, done func()) error {
	if c.status < types.KernelStatusRunning {
		return types.ErrKernelNotReady
	}

	socket := c.client.Sockets.All[typ]
	if socket == nil {
		return types.ErrSocketNotAvailable
	}

	requiresACK := (typ == types.ShellMessage) || (typ == types.ControlMessage)

	sendRequest := func() error {
		return c.client.Request(ctx, c, socket, msg, c, c, func(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) (err error) {
			// Kernel frame is automatically removed.
			if handler != nil {
				err = handler(server.(*kernelReplicaClientImpl), typ, msg)
			}
			return err
		}, done, getOption, requiresACK)
	}

	// Add timeout if necessary.
	err := sendRequest()

	if err != nil {
		c.log.Warn("Failed to send request because: %v", err)

		// If the error is that we didn't receive any ACKs, then we'll try to reconnect to the kernel.
		// If we reconnect successfully, then we'll try to send it again.
		// If we still fail to send the message at that point, then we'll just give up (for now).
		if errors.Is(err, jupyter.ErrNoAck) {
			c.log.Warn("Connectivity with remote kernel client may have been lost. Will attempt to reconnect and resubmit %v message.", typ)

			if reconnectionError := c.ReconnectSocket(typ); reconnectionError != nil {
				c.log.Error("Failed to reconnect to remote kernel client because: %v", reconnectionError)
				c.connectionRevalidationFailedCallback(c, msg, reconnectionError)
				return errors.Join(err, reconnectionError)
			}

			c.log.Debug("Successfully reconnected with remote kernel client. Will attempt to resubmit %v message now.", typ)

			secondAttemptErr := sendRequest()
			if secondAttemptErr != nil {
				c.log.Error("Failed to resubmit %v message after successfully reconnecting: %v", typ, secondAttemptErr)
				c.resubmissionAfterSuccessfulRevalidationFailedCallback(c, msg, secondAttemptErr)
				return errors.Join(err, secondAttemptErr)
			}
		}
	}

	return err
}

// Close closes the zmq sockets.
func (c *kernelReplicaClientImpl) Close() error {
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

// Get the Host on which the replica is hosted.
func (c *kernelReplicaClientImpl) GetHost() core.Host {
	return c.host
}

// Set the Host of the kernel.
func (c *kernelReplicaClientImpl) SetHost(host core.Host) {
	c.host = host
}

// Initialize the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
// If the provided types.MessageHandler parameter is nil, then we will use the default handler.
// (The default handler is kernelReplicaClientImpl::InitializeIOSub.)
//
// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
func (c *kernelReplicaClientImpl) InitializeIOSub(handler types.MessageHandler, subscriptionTopic string) (*types.Socket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Default to kernelReplicaClientImpl::handleMsg if the provided handler is null.
	if handler == nil {
		c.log.Debug("Creating ZeroMQ SUB socket: default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
		handler = c.handleMsg
	} else {
		c.log.Debug("Creating ZeroMQ SUB socket: non-default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
	}

	// Handler is set, so server routing will be started on dialing.
	c.client.Sockets.IO = types.NewSocketWithHandler(zmq4.NewSub(c.client.Ctx), c.client.Meta.IOPubPort /* sub socket for client */, types.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s]", c.id), handler)

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
func (c *kernelReplicaClientImpl) dial(sockets ...*types.Socket) error {
	// Start listening on all specified sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.client.Meta.Transport, c.client.Meta.IP)
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		addressWithPort := fmt.Sprintf(address, socket.Port)
		c.log.Debug("Dialing %s socket at %s now...", socket.Type.String(), addressWithPort)

		err := socket.Socket.Dial(addressWithPort)
		if err != nil {
			return fmt.Errorf("could not connect to kernel %v socket at address %s: %w", socket.Type.String(), addressWithPort, err)
		}

		c.log.Debug("Successfully dialed %s socket at %s.", socket.Type.String(), addressWithPort)
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, socket := range sockets {
		if socket != nil && socket.Handler != nil {
			c.log.Debug("Beginning to serve socket %v.", socket.Type.String())
			go c.client.Serve(c, socket, c, socket.Handler, c.client.ShouldAckMessages)
		} else if socket != nil {
			c.log.Debug("Not serving socket %v.", socket.Type.String())
		}
	}

	return nil
}

func (c *kernelReplicaClientImpl) handleMsg(server types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
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

func (c *kernelReplicaClientImpl) getWaitResponseOption(key string) interface{} {
	switch key {
	case jupyter.WROptionRemoveDestFrame:
		return c.shell != nil
		// case server.WROptionRemoveSourceKernelFrame:
		// 	return c.removeSourceKernelFramesFromMessages
	}

	return nil
}

func (c *kernelReplicaClientImpl) extractIOTopicFrame(msg *zmq4.Msg) (topic string, jFrames types.JupyterFrames) {
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

func (c *kernelReplicaClientImpl) forwardIOMessage(kernel core.Kernel, _ types.JupyterFrames, msg *zmq4.Msg) error {
	// c.client.Log.Debug("Forwarding %v message for Kernel \"%s\".", types.IOMessage, c.id)
	// if c.addSourceKernelFrames {
	// 	c.client.Log.Debug("Adding \"Source Kernel\" frame to %v message that we're forwarding.", types.IOMessage)
	// 	msg.Frames = c.AddSourceKernelFrame(msg.Frames, c.SourceKernelID(), server.JOffsetAutoDetect)
	// }

	// c.client.Log.Debug("%v message to be forwarded (for kernel \"%s\"): %v", types.IOMessage, c.id, msg)

	return kernel.Socket(types.IOMessage).Send(*msg)
}

func (c *kernelReplicaClientImpl) handleIOKernelStatus(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var status types.MessageKernelStatus
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling IO kernel status: %v", err)
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

// func (c *kernelReplicaClientImpl) handleIOKernelSMRNodeRemoved(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
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

func (c *kernelReplicaClientImpl) handleIOKernelSMRNodeAdded(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var node_added_message types.MessageSMRNodeUpdated
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling kernel SMR node added: %v", err)
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

func (c *kernelReplicaClientImpl) handleIOKernelSMRReady(kernel core.Kernel, frames types.JupyterFrames, msg *zmq4.Msg) error {
	var node_ready_message types.MessageSMRReady
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling kernel SMR ready: %v", err)
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
