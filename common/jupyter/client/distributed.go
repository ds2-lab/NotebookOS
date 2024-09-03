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
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	SessionIDUndecided = "undecided"
)

var (
	CtxKernelHost   = utils.ContextKey("host")
	KernelInfoReply = "kernel_info_reply"

	ErrReplicaNotFound      = fmt.Errorf("replica not found")
	ErrReplicaAlreadyExists = errors.New("cannot replace existing replica, as node IDs cannot be reused")
	ErrNoActiveExecution    = errors.New("no active execution associated with kernel who sent 'YIELD' notification")
)

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c DistributedKernelClient) error

// ReplicaRemover is a function that removes a replica from a kernel.
// If noop is specified, it is the caller's responsibility to stop the replica.
type ReplicaRemover func(host scheduling.Host, session scheduling.MetaSession, noop bool) error

// ReplicaKernelInfo offers hybrid information that reflects the replica source of messages.
type ReplicaKernelInfo struct {
	scheduling.KernelInfo
	replica scheduling.KernelInfo
}

func (r *ReplicaKernelInfo) String() string {
	return r.replica.String()
}

type DistributedKernelClient interface {
	SessionManager

	// InitializeShellForwarder initializes the IOPub serving.
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types.Socket, error)

	// InitializeIOForwarder initializes the IOPub serving.
	// Returns Pub socket, Sub socket, error.
	InitializeIOForwarder() (*types.Socket, error)

	GetReplicaByID(id int32) (scheduling.KernelReplica, error)

	// Size returns the number of replicas in the kernel.
	Size() int

	// Close closes the session and all replicas.
	Close() error

	// RemoveReplicaByID removes a kernel peer from the kernel by replica ID.
	RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (scheduling.Host, error)

	// Validate validates the kernel connections.
	// If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
	Validate() error

	// IsReplicaReady returns true if the replica with the specified ID is ready.
	IsReplicaReady(replicaId int32) (bool, error)

	AggregateBusyStatus() string

	// AddReplica adds a replica peer to the kernel.
	AddReplica(r scheduling.KernelReplica, host scheduling.Host) error

	// NumActiveAddOperations returns the number of active migrations of the associated kernel's replicas.
	NumActiveAddOperations() int

	// Shutdown releases all replicas and closes the session.
	Shutdown(remover ReplicaRemover, restart bool) error

	// WaitClosed waits for the replicas to be cleaned.
	WaitClosed() types.KernelStatus

	KernelSpec() *gateway.KernelSpec

	AddOperationCompleted()

	ShellListenPort() int

	IOPubListenPort() int

	// PodName returns the name of the Kubernetes Pod hosting the replica.
	PodName(id int32) (string, error)

	AddOperationStarted()

	// Replicas returns the replicas in the kernel.
	Replicas() []scheduling.KernelReplica

	SourceKernelID() string

	// PrepareNewReplica determines the replica ID for the new replica and returns the KernelReplicaSpec required to start the replica.
	//
	// Pass -1 for smrNodeId to automatically select the next node ID.
	PrepareNewReplica(persistentId string, smrNodeId int32) *gateway.KernelReplicaSpec

	// ID returns the kernel ID.
	ID() string

	// Sessions returns the associated session ID.
	Sessions() []string

	ResourceSpec() *gateway.ResourceSpec

	// PersistentID returns the persistent ID.
	PersistentID() string

	// String returns a string representation of the client.
	String() string

	// IsReady returns true if ALL replicas associated with this distributed kernel client are ready.
	IsReady() bool

	SetActiveExecution(activeExecution *ActiveExecution)

	ActiveExecution() *ActiveExecution

	// Socket returns the serve socket the kernel is listening on.
	Socket(typ types.MessageType) *types.Socket

	// ConnectionInfo returns the connection info.
	ConnectionInfo() *types.ConnectionInfo

	// Status returns the kernel status.
	Status() types.KernelStatus

	// BindSession binds a session ID to the client.
	BindSession(sess string)

	// RequestWithHandler sends a request and handles the response.
	RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error

	// GetReadyReplica returns a replica that has already joined its SMR cluster and everything.
	// GetReadyReplica returns nil if there are no ready replicas.
	GetReadyReplica() scheduling.KernelReplica

	ExecutionFailedCallback() ExecutionFailedCallback
}

// BaseDistributedKernelClient is a client of a Distributed Jupyter Kernel that is used by the Gateway daemon.
// It wraps individual KernelReplicaClient instances -- one for each replica of the kernel.
type BaseDistributedKernelClient struct {
	*server.BaseServer
	SessionManager
	server *server.AbstractServer

	// destMutex sync.Mutex

	id             string
	status         types.KernelStatus
	busyStatus     *AggregateKernelStatus
	lastBStatusMsg *types.JupyterMessage

	spec *gateway.KernelSpec
	// replicas []scheduling.KernelReplica
	replicas map[int32]scheduling.KernelReplica
	// size     int

	persistentId    string
	connectionInfo  *types.ConnectionInfo
	shellListenPort int // Port that the kernelReplicaClientImpl::shell socket listens on.
	iopubListenPort int // Port that the kernelReplicaClientImpl::iopub socket listens on.

	numActiveAddOperations int // Number of active migrations of the associated kernel's replicas.

	nextNodeId int32

	// The current execution request that is being processed by the kernels.
	activeExecution *ActiveExecution

	// Callback for when execution fails (such as all replicas proposing 'YIELD').
	executionFailedCallback ExecutionFailedCallback

	log     logger.Logger
	mu      sync.RWMutex
	closing int32
	cleaned chan struct{}
}

func NewDistributedKernel(ctx context.Context, spec *gateway.KernelSpec, numReplicas int, connectionInfo *types.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string, executionFailedCallback ExecutionFailedCallback) *BaseDistributedKernelClient {
	kernel := &BaseDistributedKernelClient{
		id: spec.Id, persistentId: persistentId,
		server: server.New(ctx, &types.ConnectionInfo{Transport: "tcp"}, func(s *server.AbstractServer) {
			s.Sockets.Shell = types.NewSocket(zmq4.NewRouter(s.Ctx), shellListenPort, types.ShellMessage, fmt.Sprintf("DK-Router-Shell[%s]", spec.Id))
			s.Sockets.IO = types.NewSocket(zmq4.NewPub(s.Ctx), iopubListenPort, types.IOMessage, fmt.Sprintf("DK-Pub-IO[%s]", spec.Id)) // connectionInfo.IOSubPort}
			s.PrependId = true
			/* The DistributedKernelClient lives on the Gateway. The Shell forwarder only receives messages from the frontend, which should not be ACK'd. */
			s.ShouldAckMessages = false
			s.ReconnectOnAckFailure = false
			s.Name = fmt.Sprintf("DistrKernelClient-%s", spec.Id)
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:                  types.KernelStatusInitializing,
		spec:                    spec,
		replicas:                make(map[int32]scheduling.KernelReplica, numReplicas), // make([]scheduling.KernelReplica, numReplicas),
		cleaned:                 make(chan struct{}),
		connectionInfo:          connectionInfo,
		shellListenPort:         shellListenPort,
		iopubListenPort:         iopubListenPort,
		numActiveAddOperations:  0,
		executionFailedCallback: executionFailedCallback,
		nextNodeId:              int32(numReplicas + 1),
	}
	kernel.BaseServer = kernel.server.Server()
	kernel.SessionManager = NewSessionManager(spec.Session)
	kernel.busyStatus = NewAggregateKernelStatus(kernel, numReplicas)
	kernel.log = kernel.server.Log
	return kernel
}

func (c *BaseDistributedKernelClient) ShellListenPort() int {
	return c.shellListenPort
}

func (c *BaseDistributedKernelClient) IOPubListenPort() int {
	return c.iopubListenPort
}

func (c *BaseDistributedKernelClient) ActiveExecution() *ActiveExecution {
	return c.activeExecution
}

func (c *BaseDistributedKernelClient) headerFromFrames(frames [][]byte) (*types.MessageHeader, error) {
	jFrames := types.JupyterFrames(frames)
	if err := jFrames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while extracting header: %v", err)
		return nil, err
	}

	var header types.MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		c.log.Error("Failed to decode message header \"%v\" from message frames: %v", string(jFrames[types.JupyterFrameHeader]), err)
		return nil, err
	}

	return &header, nil
}

func (c *BaseDistributedKernelClient) ExecutionFailedCallback() ExecutionFailedCallback {
	return c.executionFailedCallback
}

func (c *BaseDistributedKernelClient) SetActiveExecution(activeExecution *ActiveExecution) {
	c.activeExecution = activeExecution
}

// ResetID resets the kernel ID.
func (c *BaseDistributedKernelClient) ResetID(id string) {
	c.id = id
	c.log.Info("Reset kernel ID to %s", id)
	if colorLog, ok := c.log.(*logger.ColorLogger); ok {
		colorLog.Prefix = fmt.Sprintf("kernel(%s) ", id)
	}
}

func (c *BaseDistributedKernelClient) PersistentID() string {
	return c.persistentId
}

// String returns a string representation of the client.
func (c *BaseDistributedKernelClient) String() string {
	return fmt.Sprintf("kernel(%s)", c.id)
}

// MetaSession implementations.
func (c *BaseDistributedKernelClient) ID() string {
	return c.id
}

func (c *BaseDistributedKernelClient) SourceKernelID() string {
	return c.id
}

func (s *BaseDistributedKernelClient) ResourceSpec() *gateway.ResourceSpec {
	return s.spec.GetResourceSpec()
}

func (s *BaseDistributedKernelClient) KernelSpec() *gateway.KernelSpec {
	return s.spec
}

// ConnectionInfo returns the connection info.
func (c *BaseDistributedKernelClient) ConnectionInfo() *types.ConnectionInfo {
	return c.server.Meta
}

// Status returns the kernel status.
func (c *BaseDistributedKernelClient) Status() types.KernelStatus {
	return c.status
}

func (c *BaseDistributedKernelClient) AggregateBusyStatus() string {
	return c.busyStatus.status
}

// BindSession binds a session ID to the client.
func (c *BaseDistributedKernelClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	var restarted bool
	if c.status == types.KernelStatusExited {
		// restarted = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(types.KernelStatusExited), int32(types.KernelStatusInitializing))
		restarted = c.setStatus(types.KernelStatusExited, types.KernelStatusInitializing)
	}
	if restarted {
		c.log.Info("Restarted for binding kernel session %s", sess)
	} else {
		c.log.Info("Binded session %s to distributed kernel client.", sess)
	}
}

// Atomically swap the kernel's status from a particular old status value to a particular new status value.
//
// If the status is swapped, then returns true. Otherwise, returns false.
func (c *BaseDistributedKernelClient) setStatus(oldStatus types.KernelStatus, newStatus types.KernelStatus) (swapped bool) {
	swapped = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(oldStatus), int32(newStatus))

	if swapped {
		c.log.Debug("Swapped kernel status from %s to %s.", oldStatus.String(), newStatus.String())
	} else {
		c.log.Debug("Attempt to swap kernel status from %s to %s rejected.", oldStatus.String(), newStatus.String())
	}

	return
}

// Size returns the number of replicas in the kernel.
func (c *BaseDistributedKernelClient) Size() int {
	return len(c.replicas) // c.size
}

// Return the number of active migrations of the associated kernel's replicas.
func (c *BaseDistributedKernelClient) NumActiveAddOperations() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.numActiveAddOperations
}

// func (c *BaseDistributedKernelClient) Unlock() {
// 	c.destMutex.Unlock()
// }

// func (c *BaseDistributedKernelClient) Lock() {
// 	c.destMutex.Lock()
// }

func (c *BaseDistributedKernelClient) AddOperationStarted() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.numActiveAddOperations += 1
}

func (c *BaseDistributedKernelClient) AddOperationCompleted() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.numActiveAddOperations -= 1

	if c.numActiveAddOperations < 0 {
		panic("Number of active migration operations cannot fall below 0.")
	}
}

// Replicas returns the replicas in the kernel.
func (c *BaseDistributedKernelClient) Replicas() []scheduling.KernelReplica {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Make a copy of references.
	ret := make([]scheduling.KernelReplica, 0, len(c.replicas))
	for _, replica := range c.replicas {
		ret = append(ret, replica)
	}
	// for i := 0; i < c.size; i++ {
	// 	if c.replicas[i] != nil {
	// 		ret = append(ret, c.replicas[i])
	// 	}
	// }

	return ret
}

func (c *BaseDistributedKernelClient) PodName(id int32) (string, error) {
	replica, err := c.GetReplicaByID(id)

	if err != nil {
		c.log.Debug("Could not find replica with id %d", id)
		return "", err
	}

	return replica.PodName(), nil
}

// PrepareNewReplica determines the replica ID for the new replica and returns the KernelReplicaSpec required to start the replica.
//
// Pass -1 for smrNodeId to automatically select the next node ID.
func (c *BaseDistributedKernelClient) PrepareNewReplica(persistentId string, smrNodeId int32) *gateway.KernelReplicaSpec {
	c.mu.Lock()
	defer c.mu.Unlock()

	if smrNodeId == -1 {
		smrNodeId = c.nextNodeId
		c.nextNodeId++
	}

	spec := &gateway.KernelReplicaSpec{
		Kernel:       c.spec,
		NumReplicas:  int32(len(c.replicas)) + 1,
		Join:         true,
		PersistentId: &persistentId,
		ReplicaId:    smrNodeId,
	}

	// Find the first empty slot.
	// for i := 0; i < len(c.replicas); i++ {
	// 	if c.replicas[i] == nil {
	// 		spec.ReplicaId = int32(i + 1)
	// 		break
	// 	}
	// }
	// if spec.ReplicaId == 0 {
	// 	spec.ReplicaId = int32(len(c.replicas) + 1)
	// }

	// If we follows add one and remove one rule, replicas are expected to have no holes in
	// replica list (i.e. no nil in the list) at this point. So we spec.Replicas is leaved
	// empty, and leave invokers to deterministically fill the list.

	return spec
}

// AddReplica adds a replica peer to the kernel.
func (c *BaseDistributedKernelClient) AddReplica(r scheduling.KernelReplica, host scheduling.Host) error {
	// IOForwarder is initialized, link the kernel to feed the IOPub.
	// _, err := r.InitializeIOSub(c.handleMsg, c.id)
	_, err := r.InitializeIOSub(c.handleMsg, "")
	if err != nil {
		return err
	}

	// Safe to append the kernel now.
	r.SetContext(context.WithValue(r.Context(), CtxKernelHost, host))

	c.mu.Lock()
	c.replicas[r.ReplicaID()] = r
	c.mu.Unlock()

	// On adding replica, we keep the position of the replica in the kernel aligned with the replica ID.
	// The replica ID starts with 1.
	// if int(r.ReplicaID()) > cap(c.replicas) {
	// 	newArr := make([]scheduling.KernelReplica, cap(c.replicas)*2)
	// 	copy(newArr, c.replicas)
	// 	c.replicas = newArr[:r.ReplicaID()]
	// } else if int(r.ReplicaID()) > len(c.replicas) {
	// 	c.replicas = c.replicas[:r.ReplicaID()]
	// }
	// if c.replicas[r.ReplicaID()-1] == nil {
	// 	// New added replica, or no size change for the replacement.
	// 	c.size++
	// }
	// c.replicas[r.ReplicaID()-1] = r
	// Once a replica is available, the kernel is ready.
	if statusChanged := c.setStatus(types.KernelStatusInitializing, types.KernelStatusRunning); statusChanged {
		// if atomic.CompareAndSwapInt32((*int32)(&c.status), int32(types.KernelStatusInitializing), int32(types.KernelStatusRunning)) {
		// Update signature scheme and key.
		c.server.Meta.SignatureScheme = r.ConnectionInfo().SignatureScheme
		c.server.Meta.Key = r.ConnectionInfo().Key

		// Collect the status of all replicas.
		c.busyStatus.Collect(context.Background(), 1, len(c.replicas), types.MessageKernelStatusStarting, c.pubIOMessage)
	}
	return nil
}

// RemoveReplica removes a kernel peer from the kernel.
func (c *BaseDistributedKernelClient) RemoveReplica(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (scheduling.Host, error) {
	host, err := c.stopReplicaLocked(r, remover, noop)
	if err != nil {
		return host, err
	}

	c.mu.Lock()
	defer r.Close()
	defer c.mu.Unlock()

	if c.replicas[r.ReplicaID()] != r {
		// This is bad and should never happen.
		c.log.Error("Replica stored under ID key %d has ID %d.", r.ReplicaID(), c.replicas[r.ReplicaID()].ID())
		return host, ErrReplicaNotFound
	}

	delete(c.replicas, r.ReplicaID())

	// The replica ID starts with 1.
	// if int(r.ReplicaID()) > len(c.replicas) || c.replicas[r.ReplicaID()-1] != r {
	// 	return host, ErrReplicaNotFound
	// }

	// c.replicas[r.ReplicaID()-1] = nil
	// c.size--
	return host, nil
}

func (c *BaseDistributedKernelClient) GetReplicaByID(id int32) (scheduling.KernelReplica, error) {
	c.mu.RLock()
	// if id <= int32(len(c.replicas)) {
	// 	replica = c.replicas[id-1]
	// }
	replica, ok := c.replicas[id]
	c.mu.RUnlock()

	if replica == nil || !ok {
		valid_ids := make([]int32, 0, len(c.replicas))
		for valid_id := range c.replicas {
			valid_ids = append(valid_ids, valid_id)
		}
		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, valid_ids)
		return nil, ErrReplicaNotFound
	}

	return replica, nil
}

// RemoveReplicaByID removes a kernel peer from the kernel by replica ID.
func (c *BaseDistributedKernelClient) RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.mu.RLock()
	// var replica scheduling.KernelReplica
	// if id <= int32(len(c.replicas)) {
	// 	replica = c.replicas[id-1]
	// }
	replica, ok := c.replicas[id]
	c.mu.RUnlock()

	if replica == nil || !ok {
		valid_ids := make([]int32, 0, len(c.replicas))
		for valid_id := range c.replicas {
			valid_ids = append(valid_ids, valid_id)
		}
		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, valid_ids)
		return nil, ErrReplicaNotFound
	}

	return c.RemoveReplica(replica, remover, noop)
}

// Validate validates the session.
func (c *BaseDistributedKernelClient) Validate() error {
	if c.status >= types.KernelStatusRunning {
		return nil
	}

	return types.ErrKernelNotReady
}

// InitializeShellForwarder initializes the Shell serving.
func (c *BaseDistributedKernelClient) InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types.Socket, error) {
	c.log.Debug("Initializing shell forwarder for distributed kernel client.")

	shell := c.server.Sockets.Shell
	if err := c.server.Listen(shell); err != nil {
		return nil, err
	}

	go c.server.Serve(c, shell, func(srv types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
		msg.AddDestinationId(c.KernelSpec().Id)
		// msg.Frames, _ = types.AddDestFrame(msg.Frames, c.KernelSpec().Id, jupyter.JOffsetAutoDetect)
		c.log.Debug("Received shell message via DistributedShellForwarder. Message: %v", msg)
		return handler(srv.(*BaseDistributedKernelClient), typ, msg)
	})

	return shell, nil
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *BaseDistributedKernelClient) InitializeIOForwarder() (*types.Socket, error) {
	if err := c.server.Listen(c.server.Sockets.IO); err != nil {
		return nil, err
	}

	return c.server.Sockets.IO, nil
}

// Return a replica that has already joined its SMR cluster and everything.
// Returns nil if there are no ready replicas.
func (c *BaseDistributedKernelClient) GetReadyReplica() scheduling.KernelReplica {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, replica := range c.replicas {
		if replica.IsReady() {
			return replica
		}
	}

	return nil
}

// Return true if ALL replicas associated with this distributed kernel client are ready.
func (c *BaseDistributedKernelClient) IsReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, replica := range c.replicas {
		if !replica.IsReady() {
			return false
		}
	}

	return true
}

// Return true if the replica with the specified ID is ready.
func (c *BaseDistributedKernelClient) IsReplicaReady(replicaId int32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	replica, ok := c.replicas[replicaId]
	if !ok {
		return false, ErrReplicaNotFound
	}

	return replica.IsReady(), nil
}

// RequestWithHandler sends a request to all replicas and handles the response.
func (c *BaseDistributedKernelClient) RequestWithHandler(ctx context.Context, prompt string, typ types.MessageType, msg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error {
	return c.RequestWithHandlerAndReplicas(ctx, typ, msg, handler, done)
}

// Process a response to a shell message. This is called before the handler that was passed when issuing the request.
// Return true if the message is a 'yield' message (indicating that the replica yielded an execution).
func (c *BaseDistributedKernelClient) preprocessShellResponse(replica scheduling.KernelInfo, msg *types.JupyterMessage) (err error, yielded bool) {
	// var (
	// 	header             *types.MessageHeader
	// 	jupyterMessageType string
	// )

	replicaClient := replica.(*kernelReplicaClientImpl)
	replicaId := replicaClient.replicaId

	// _, header, _, err = types.HeaderFromMsg(msg)
	// if err != nil {
	// 	c.log.Error("Failed to extract header from ZMQ message sent by replica %d of kernel %s: %v", replicaId, c.id, err)
	// 	return err, false
	// }

	// jupyterMessageType = header.MsgType
	// if jupyterMessageType != KernelInfoReply {
	// 	c.log.Debug("Received shell '%v' response from kernel %s: %v", jupyterMessageType, c.id, msg)
	// } else { // We don't print the message if it is just a KernelInfoReply.
	// 	c.log.Debug("Received shell '%v' response from kernel %s.", jupyterMessageType, c.id)
	// }

	jFrames, _ := types.SkipIdentitiesFrame(msg.Frames)
	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if len(jFrames) < 5 {
		c.log.Error("Received invalid Jupyter message from replica %d of kernel %s (detected in extractShellError)", replicaId, c.id)
		return types.ErrInvalidJupyterMessage, false
	}

	if len(jFrames[types.JupyterFrameContent]) == 0 {
		c.log.Warn("Received shell '%v' response with empty content.", msg.JupyterMessageType())
		return nil, false
	}

	var msgErr types.MessageError
	err = json.Unmarshal(jFrames[types.JupyterFrameContent], &msgErr)
	if err != nil {
		c.log.Error("Failed to unmarshal shell message received from replica %d of kernel %s because: %v", replicaId, c.id, err)
		return err, false
	}

	if msgErr.Status == types.MessageStatusOK {
		if msg.JupyterMessageType() == "execute_reply" {
			return c.executionFinished(replicaId), false
		}
		return nil, false
	}

	err = errors.New(msgErr.ErrValue)

	if msgErr.ErrName == types.MessageErrYieldExecution {
		yielded = true
		errors.Join()
		err2 := c.handleExecutionYieldedNotification(replica.(*kernelReplicaClientImpl), msgErr)
		return errors.Join(err, err2), true
	}

	return err, false
}

func (c *BaseDistributedKernelClient) executionFinished(replicaId int32) error {
	if c.activeExecution != nil {
		c.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d.", c.activeExecution.executionId, c.id, replicaId)
		c.activeExecution.ReceivedLeadProposal(replicaId)
		c.activeExecution.SetExecuted()
		return nil
	} else {
		return ErrNoActiveExecution
	}
}

// RequestWithHandlerAndReplicas sends a request to specified replicas and handles the response.
func (c *BaseDistributedKernelClient) RequestWithHandlerAndReplicas(ctx context.Context, typ types.MessageType, jMsg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func(), replicas ...scheduling.KernelReplica) error {
	// Boardcast to all replicas if no replicas are specified.
	if len(replicas) == 0 {
		for _, replica := range c.replicas {
			replicas = append(replicas, replica)
		}
	}

	once := sync.Once{}
	var replicaCtx context.Context
	var cancel context.CancelFunc
	if typ == types.ShellMessage {
		replicaCtx, cancel = context.WithCancel(ctx)
	} else {
		replicaCtx, cancel = context.WithTimeout(ctx, jupyter.DefaultRequestTimeout)
	}
	// defer cancel()
	forwarder := func(replica scheduling.KernelInfo, typ types.MessageType, msg *types.JupyterMessage) (err error) {
		c.log.Debug(utils.BlueStyle.Render("Received %s response from %v"), typ.String(), replica)

		if typ == types.ShellMessage {
			// "Preprocess" the response, which involves checking if it a YIELD notification, and handling a situation in which ALL replicas have proposed 'YIELD'.
			_, yielded := c.preprocessShellResponse(replica, msg)
			if yielded {
				return nil
			}
		}

		// TODO: Remove this eventually once all bugs are fixed.
		// These next two if-statements are used to ensure that the handler for 'ping_reply' responses is always called.
		// They're used to test connectivity with kernels, so we always want them to be called.
		// _, header, _, err := types.HeaderFromMsg(msg)
		// if err != nil {
		// 	panic(err)
		// }
		// if header.MsgType == "ping_reply" {
		if msg.JupyterMessageType() == "ping_reply" {
			if handler == nil {
				panic("Handler is nil")
			}

			return handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, typ, msg)
		}

		// Handler will only be called once.
		forwarded := false
		once.Do(func() {
			cancel()
			if handler != nil {
				err = handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, typ, msg)
			}
			forwarded = true
		})
		if !forwarded {
			c.log.Debug("Discard %v \"%s\" response from %v", typ, msg.JupyterMessageType() /* header.MsgType */, replica)
		}
		return err
	}

	// Send the request to all replicas.
	statusCtx, statusCancel := context.WithTimeout(context.Background(), jupyter.DefaultRequestTimeout)
	defer statusCancel()
	c.busyStatus.Collect(statusCtx, len(c.replicas), len(c.replicas), types.MessageKernelStatusBusy, c.pubIOMessage)
	if len(replicas) == 1 {
		return replicas[0].(*kernelReplicaClientImpl).requestWithHandler(replicaCtx, typ, jMsg, forwarder, c.getWaitResponseOption, done)
	}

	// Add the dest frame here, as there can be a race condition where multiple replicas will add the dest frame at the same time, leading to multiple dest frames.
	_, reqId, jOffset := types.ExtractDestFrame(jMsg.Frames)
	if reqId == "" {
		c.log.Debug("Adding destination '%s' to frames at offset %d now. Old frames: %v.", c.id, jOffset, types.JupyterFrames(jMsg.Frames).String())
		// jMsg.Frames, _ = types.AddDestFrame(jMsg.Frames, c.id, jOffset)
		jMsg.AddDestinationId(c.id)
		c.log.Debug("Added destination '%s' to frames at offset %d. New frames: %v.", c.id, jOffset, types.JupyterFrames(jMsg.Frames).String())
	}

	var wg sync.WaitGroup
	for _, kernel := range replicas {
		if kernel == nil {
			continue
		}

		wg.Add(1)
		go func(kernel scheduling.Kernel) {
			// TODO: If the ACKs fail on this and we reconnect and retry, the wg.Done may be called too many times.
			// Need to fix this. Either make the timeout bigger, or... do something else. Maybe we don't need the pending request
			// to be cleared after the context ends; we just do it on ACK timeout.
			kernel.(*kernelReplicaClientImpl).requestWithHandler(replicaCtx, typ, jMsg, forwarder, c.getWaitResponseOption, wg.Done)
		}(kernel)
	}
	if done != nil {
		go func() {
			wg.Wait()
			done()
		}()
	}
	return nil
}

// Shutdown releases all replicas and closes the session.
func (c *BaseDistributedKernelClient) Shutdown(remover ReplicaRemover, restart bool) error {
	c.log.Debug("Shutting down Kernel %s.", c.id)
	c.mu.RLock()

	if c.status == types.KernelStatusExited {
		c.mu.RUnlock()
		c.log.Warn("Kernel %s has already exited; there's no need to shutdown.", c.id)
		return nil
	}

	// Stop the replicas first.
	var stopped sync.WaitGroup
	stopped.Add(len(c.replicas))

	// In RLock, don't change anything in c.replicas.
	for _, replica := range c.replicas {
		if replica == nil {
			continue
		}

		go func(replica scheduling.Kernel) {
			defer stopped.Done()

			if host, err := c.stopReplicaLocked(replica.(*kernelReplicaClientImpl), remover, false); err != nil {
				c.log.Warn("Failed to stop %v on host %v: %v", replica, host, err)
				return
			} else {
				c.log.Debug("Successfully stopped replica %v on host %s.", replica, host)
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
func (c *BaseDistributedKernelClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == types.KernelStatusExited {
		return nil
	}

	return c.closeLocked()
}

// WaitClosed waits for the replicas to be cleaned.
func (c *BaseDistributedKernelClient) WaitClosed() types.KernelStatus {
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

func (c *BaseDistributedKernelClient) stopReplicaLocked(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.log.Debug("Stopping replica %d of kernel %s now.", r.ReplicaID(), r.ID())
	host := r.Context().Value(CtxKernelHost).(scheduling.Host)
	if err := remover(host, c, noop); err != nil {
		return host, err
	}

	// c.log.Debug("Waiting for replica %d of kernel %s to stop and return its status.", r.ReplicaID(), r.ID())
	// host.WaitKernel(context.Background(), &gateway.KernelId{Id: r.ID()})
	return host, nil
}

func (c *BaseDistributedKernelClient) clearReplicasLocked() {
	c.log.Debug("Clearing replicas now.", c.id)
	toRemove := make([]int32, len(c.replicas))
	for id, kernel := range c.replicas {
		if kernel != nil {
			c.log.Debug("Closing replica %d now.", kernel.ReplicaID())
			err := kernel.Close()

			if err != nil {
				// TODO(Ben): Handle this more cleanly.
				c.log.Error("Error while trying to close replica %d: %v", kernel.ReplicaID(), err)
			} else {
				c.log.Debug("Successfully closed replica %d.", kernel.ReplicaID())
			}
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		delete(c.replicas, id)
	}

	c.status = types.KernelStatusExited
}

func (c *BaseDistributedKernelClient) closeLocked() error {
	if !atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		<-c.cleaned
		return nil
	}

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

func (c *BaseDistributedKernelClient) queryCloseLocked() error {
	// Stop the replicas first.
	var stopped sync.WaitGroup
	stopped.Add(len(c.replicas))
	for _, replica := range c.replicas {
		if replica == nil {
			continue
		}

		go func(replica scheduling.Kernel) {
			defer stopped.Done()

			host := replica.Context().Value(CtxKernelHost).(scheduling.Host)
			host.WaitKernel(context.Background(), &gateway.KernelId{Id: replica.ID()})
		}(replica)
	}
	stopped.Wait()

	return nil
}

func (c *BaseDistributedKernelClient) getWaitResponseOption(key string) interface{} {
	switch key {
	case jupyter.WROptionRemoveDestFrame:
		return true
	}

	return nil
}

func (c *BaseDistributedKernelClient) handleMsg(replica types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
	// c.log.Debug("DistrKernClient %v handling %v message: %v", c.id, typ.String(), msg)
	switch typ {
	case types.IOMessage:
		// Remove the source kernel frame.
		// msg.Frames = c.RemoveSourceKernelFrame(msg.Frames, -1)

		topic, jFrames := replica.(*kernelReplicaClientImpl).extractIOTopicFrame(msg)
		switch topic {
		case types.IOTopicStatus:
			return c.handleIOKernelStatus(replica.(*kernelReplicaClientImpl), jFrames, msg)
		default:
			return c.server.Sockets.IO.Send(*msg.Msg)
		}
	}

	return ErrHandlerNotImplemented
}

func (c *BaseDistributedKernelClient) handleIOKernelStatus(replica *kernelReplicaClientImpl, frames types.JupyterFrames, msg *types.JupyterMessage) error {
	err := replica.handleIOKernelStatus(replica, frames, msg)
	if err != nil {
		return err
	}

	status, msg := replica.BusyStatus()
	// c.log.Debug("reducing replica %d status %s to match %s", replica.ReplicaID(), status, c.busyStatus.expectingStatus)
	c.busyStatus.Reduce(replica.ReplicaID(), status, msg, c.pubIOMessage)
	return nil
}

func (c *BaseDistributedKernelClient) handleExecutionYieldedNotification(replica *kernelReplicaClientImpl, msgErr types.MessageError) error {
	yieldError := errors.New(msgErr.ErrValue)
	c.log.Debug("Received 'YIELD' proposal from %v: %v", replica, yieldError)

	if c.activeExecution != nil {
		replicaId := replica.replicaId

		err := c.activeExecution.ReceivedYieldProposal(replicaId)
		if errors.Is(err, ErrExecutionFailedAllYielded) {
			return c.handleFailedExecutionAllYielded()
		}

		return err
	} else {
		c.log.Error("Received 'YIELD' proposal from %v, but we have no active execution...", replica)
		return ErrNoActiveExecution
	}
}

// Handle a failed execution in which all replicas proposed 'YIELD'.
//
// Note that 'final' means that it was the last replica whose message we received; all replicas proposed 'YIELD',however.
func (c *BaseDistributedKernelClient) handleFailedExecutionAllYielded() error {
	c.log.Warn("Kernel %s failed to execute code; all replicas proposed 'YIELD'.", c.id)
	return c.executionFailedCallback(c)
}

func (c *BaseDistributedKernelClient) pubIOMessage(msg *types.JupyterMessage, status string, how string) error {
	// c.log.Debug("Publishing %v status(%s:%s): %v", types.IOMessage, status, how, msg)
	c.lastBStatusMsg = msg
	err := c.server.Sockets.IO.Send(*msg.Msg)

	// Initiate idle status collection.
	if status == types.MessageKernelStatusBusy {
		c.busyStatus.Collect(context.Background(), len(c.replicas), len(c.replicas), types.MessageKernelStatusIdle, c.pubIOMessage)
		// Fill matched status that has been received before collecting.
		for _, replica := range c.replicas {
			if replica == nil {
				continue
			}

			status, msg := replica.(*kernelReplicaClientImpl).BusyStatus()
			if status == types.MessageKernelStatusIdle {
				c.busyStatus.Reduce(replica.(*kernelReplicaClientImpl).ReplicaID(), types.MessageKernelStatusIdle, msg, c.pubIOMessage)
			}
		}
	}

	return err
}
