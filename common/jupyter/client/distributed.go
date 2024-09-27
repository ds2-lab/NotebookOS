package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	commonTypes "github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

//const (
//	SessionIDUndecided = "undecided"
//)

var (
	CtxKernelHost        = utils.ContextKey("host")
	ErrNoActiveExecution = errors.New("no active execution associated with kernel who sent 'YIELD' notification")

	//KernelInfoReply         = "kernel_info_reply"
	//ErrReplicaAlreadyExists = errors.New("cannot replace existing replica, as node IDs cannot be reused")
)

type SessionManager interface {
	Sessions() []string        // Session returns the associated session ID.
	BindSession(sess string)   // BindSession binds a session ID to the client.
	UnbindSession(sess string) // UnbindSession unbinds a session ID from the client.
	ClearSessions()            // ClearSessions clears all sessions.
}

// ExecutionLatencyCallback is provided by the Cluster Gateway to each DistributedKernelClient.
// When a DistributedKernelClient receives a notification that a kernel has started execution user-submitted code,
// the DistributedKernelClient will check if its ActiveExecution struct has the original "sent-at" timestamp
// of the original "execute_request". If it does, then it can calculate the latency between submission and when
// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
// so that a relevant Prometheus metric can be updated.
type ExecutionLatencyCallback func(latency time.Duration, workloadId string)

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c *DistributedKernelClient) error

// ReplicaRemover is a function that removes a replica from a kernel.
// If noop is specified, it is the caller's responsibility to stop the replica.
type ReplicaRemover func(host *scheduling.Host, session *scheduling.Session, noop bool) error

// ReplicaKernelInfo offers hybrid information that reflects the replica source of messages.
type ReplicaKernelInfo struct {
	scheduling.KernelInfo
	replica scheduling.KernelInfo
}

func (r *ReplicaKernelInfo) String() string {
	return r.replica.String()
}

// DistributedKernelClient is a client of a Distributed Jupyter Kernel that is used by the Gateway daemon.
// It wraps individual KernelReplicaClient instances -- one for each replica of the kernel.
type DistributedKernelClient struct {
	*server.BaseServer
	SessionManager
	server *server.AbstractServer

	// destMutex sync.Mutex

	id             string
	status         types.KernelStatus
	busyStatus     *AggregateKernelStatus
	lastBStatusMsg *types.JupyterMessage

	spec *proto.KernelSpec
	// replicas []scheduling.KernelReplica
	replicas map[int32]scheduling.KernelReplica
	// size     int

	persistentId    string
	connectionInfo  *types.ConnectionInfo
	shellListenPort int // Port that the KernelReplicaClient::shell socket listens on.
	iopubListenPort int // Port that the KernelReplicaClient::iopub socket listens on.

	numActiveAddOperations int // Number of active migrations of the associated kernel's replicas.

	nextNodeId int32

	// activeExecution is the current execution request that is being processed by the kernels.
	activeExecution *scheduling.ActiveExecution

	// activeExecutionQueue is a queue of ActiveExecution structs corresponding to
	// submitted/enqueued execution operations.
	activeExecutionQueue scheduling.ActiveExecutionQueue

	// Callback for when execution fails (such as all replicas proposing 'YIELD').
	executionFailedCallback ExecutionFailedCallback

	// ExecutionLatencyCallback is provided by the Cluster Gateway to each DistributedKernelClient.
	// When a DistributedKernelClient receives a notification that a kernel has started execution user-submitted code,
	// the DistributedKernelClient will check if its ActiveExecution struct has the original "sent-at" timestamp
	// of the original "execute_request". If it does, then it can calculate the latency between submission and when
	// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
	// so that a relevant Prometheus metric can be updated.
	executionLatencyCallback ExecutionLatencyCallback

	// messagingMetricsProvider is an interface that enables the recording of metrics observed by the DistributedKernelClient.
	messagingMetricsProvider metrics.MessagingMetricsProvider

	session *scheduling.Session

	log     logger.Logger
	mu      sync.RWMutex
	closing int32
	cleaned chan struct{}
}

func NewDistributedKernel(ctx context.Context, spec *proto.KernelSpec, numReplicas int, hostId string,
	connectionInfo *types.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string,
	executionFailedCallback ExecutionFailedCallback, executionLatencyCallback ExecutionLatencyCallback,
	messagingMetricsProvider metrics.MessagingMetricsProvider) *DistributedKernelClient {

	kernel := &DistributedKernelClient{
		id:                       spec.Id,
		persistentId:             persistentId,
		messagingMetricsProvider: messagingMetricsProvider,
		server: server.New(ctx, &types.ConnectionInfo{Transport: "tcp"}, metrics.ClusterGateway, func(s *server.AbstractServer) {
			s.Sockets.Shell = types.NewSocket(zmq4.NewRouter(s.Ctx), shellListenPort, types.ShellMessage, fmt.Sprintf("DK-Router-Shell[%s]", spec.Id))
			s.Sockets.IO = types.NewSocket(zmq4.NewPub(s.Ctx), iopubListenPort, types.IOMessage, fmt.Sprintf("DK-Pub-IO[%s]", spec.Id)) // connectionInfo.IOSubPort}
			s.PrependId = true
			/* The DistributedKernelClient lives on the Gateway. The Shell forwarder only receives messages from the frontend, which should not be ACK'd. */
			s.ShouldAckMessages = false
			s.ReconnectOnAckFailure = false
			s.ComponentId = hostId
			s.Name = fmt.Sprintf("DistrKernelClient-%s", spec.Id)
			s.MessagingMetricsProvider = messagingMetricsProvider
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:                   types.KernelStatusInitializing,
		spec:                     spec,
		replicas:                 make(map[int32]scheduling.KernelReplica, numReplicas), // make([]scheduling.KernelReplica, numReplicas),
		cleaned:                  make(chan struct{}),
		connectionInfo:           connectionInfo,
		shellListenPort:          shellListenPort,
		iopubListenPort:          iopubListenPort,
		numActiveAddOperations:   0,
		executionFailedCallback:  executionFailedCallback,
		activeExecutionQueue:     make(scheduling.ActiveExecutionQueue, 0, 16),
		executionLatencyCallback: executionLatencyCallback,
		nextNodeId:               int32(numReplicas + 1),
	}
	kernel.BaseServer = kernel.server.Server()
	kernel.SessionManager = NewSessionManager(spec.Session)
	kernel.busyStatus = NewAggregateKernelStatus(kernel, numReplicas)
	kernel.log = kernel.server.Log
	return kernel
}

// SetSession sets/updates the scheduling.Session associated with the DistributedKernelClient.
func (c *DistributedKernelClient) SetSession(session *scheduling.Session) {
	c.session = session
}

// GetSession returns the scheduling.Session associated with the DistributedKernelClient.
func (c *DistributedKernelClient) GetSession() *scheduling.Session {
	return c.session
}

// GetContainers returns a slice containing all the scheduling.Container associated with the scheduling.Session
// (i.e., the scheduling.Session that itself is associated with the DistributedKernelClient).
func (c *DistributedKernelClient) GetContainers() []*scheduling.Container {
	c.mu.Lock()
	defer c.mu.Unlock()

	containers := make([]*scheduling.Container, len(c.replicas))
	for _, replica := range c.replicas {
		containers = append(containers, replica.Container())
	}

	return containers
}

func (c *DistributedKernelClient) ShellListenPort() int {
	return c.shellListenPort
}

func (c *DistributedKernelClient) IOPubListenPort() int {
	return c.iopubListenPort
}

func (c *DistributedKernelClient) ActiveExecution() *scheduling.ActiveExecution {
	return c.activeExecution
}

func (c *DistributedKernelClient) headerFromFrames(frames [][]byte) (*types.MessageHeader, error) {
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

func (c *DistributedKernelClient) ExecutionFailedCallback() ExecutionFailedCallback {
	return c.executionFailedCallback
}

// SetActiveExecution attempts to set the ActiveExecution of the DistributedKernelClient.
// SetActiveExecution will replace the current ActiveExecution with the given ActiveExecution,
// bypassing all other ActiveExecution instances that are already enqueued.
func (c *DistributedKernelClient) SetActiveExecution(activeExecution *scheduling.ActiveExecution) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.activeExecution = activeExecution
}

// ExecutionComplete sets the current ActiveExecution to nil, or possibly to the next ActiveExecution in the queue,
// if the queue is non-empty. This also calls TrainingStopped on the KernelReplicaClient that was performing the
// training.
//
// If there are any ActiveExecution structs enqueued, then the next struct is popped off the queue and
// assigned as the current ActiveExecution for the DistributedKernelClient. If this occurs, then
// ExecutionComplete returns true.
//
// If there are no ActiveExecution structs enqueued, then ExecutionComplete returns false.
func (c *DistributedKernelClient) ExecutionComplete() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.activeExecution.ActiveReplica.TrainingStopped()

	if len(c.activeExecutionQueue) > 0 {
		c.activeExecution = c.activeExecutionQueue.Dequeue()
		c.log.Debug("Popped next ActiveExecution off of queue and assigned it as current ActiveExecution: %v",
			c.activeExecution)

		return true, err
	} else {
		c.activeExecution = nil

		return false, err
	}
}

// EnqueueActiveExecution will set the ActiveExecution of the DistributedKernelClient to the
// given ActiveExecution if the DistributedKernelClient currently has no ActiveExecution. In this case,
// EnqueueActiveExecution will return false.
//
// If the DistributedKernelClient already has an ActiveExecution, then the ActiveExecution passed
// as an argument to the SetActiveExecution method will instead be enqueued. In this case,
// EnqueueActiveExecution will return true.
func (c *DistributedKernelClient) EnqueueActiveExecution(activeExecution *scheduling.ActiveExecution) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If there is already an active execution, then enqueue the new one.
	if c.activeExecution != nil {
		c.log.Debug("Found non-nil active execution. Enqueuing new active execution.")
		c.activeExecutionQueue = append(c.activeExecutionQueue, activeExecution)
		return true
	}

	// There's no active execution currently, so just set the value.
	c.activeExecution = activeExecution
	return false
}

// ResetID resets the kernel ID.
func (c *DistributedKernelClient) ResetID(id string) {
	c.id = id
	c.log.Info("Reset kernel ID to %s", id)
	if colorLog, ok := c.log.(*logger.ColorLogger); ok {
		colorLog.Prefix = fmt.Sprintf("kernel(%s) ", id)
	}
}

func (c *DistributedKernelClient) PersistentID() string {
	return c.persistentId
}

// String returns a string representation of the client.
func (c *DistributedKernelClient) String() string {
	return fmt.Sprintf("kernel(%s)", c.id)
}

// MetaSession implementations.

func (c *DistributedKernelClient) ID() string {
	return c.id
}

func (c *DistributedKernelClient) SourceKernelID() string {
	return c.id
}

func (c *DistributedKernelClient) ResourceSpec() *commonTypes.DecimalSpec {
	return c.spec.DecimalSpecFromKernelSpec()
}

func (c *DistributedKernelClient) KernelSpec() *proto.KernelSpec {
	return c.spec
}

// ConnectionInfo returns the connection info.
func (c *DistributedKernelClient) ConnectionInfo() *types.ConnectionInfo {
	return c.server.Meta
}

// Status returns the kernel status.
func (c *DistributedKernelClient) Status() types.KernelStatus {
	return c.status
}

func (c *DistributedKernelClient) AggregateBusyStatus() string {
	return c.busyStatus.status
}

// BindSession binds a session ID to the client.
func (c *DistributedKernelClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	var restarted bool
	if c.status == types.KernelStatusExited {
		// restarted = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(types.KernelStatusExited), int32(types.KernelStatusInitializing))
		restarted = c.setStatus(types.KernelStatusExited, types.KernelStatusInitializing)
	}
	if restarted {
		c.log.Info("Restarted for binding kernel session %s", sess)
	} else {
		c.log.Info("Bound session %s to distributed kernel client.", sess)
	}
}

// Atomically swap the kernel's status from a particular old status value to a particular new status value.
//
// If the status is swapped, then returns true. Otherwise, returns false.
func (c *DistributedKernelClient) setStatus(oldStatus types.KernelStatus, newStatus types.KernelStatus) (swapped bool) {
	swapped = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(oldStatus), int32(newStatus))

	if swapped {
		c.log.Debug("Swapped kernel status from %s to %s.", oldStatus.String(), newStatus.String())
	} else {
		c.log.Debug("Attempt to swap kernel status from %s to %s rejected.", oldStatus.String(), newStatus.String())
	}

	return
}

// Size returns the number of replicas in the kernel.
func (c *DistributedKernelClient) Size() int {
	return len(c.replicas) // c.size
}

// NumActiveAddOperations returns the number of active migrations of the associated kernel's replicas.
func (c *DistributedKernelClient) NumActiveAddOperations() int {
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

func (c *DistributedKernelClient) AddOperationStarted() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.numActiveAddOperations += 1
}

func (c *DistributedKernelClient) AddOperationCompleted() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.numActiveAddOperations -= 1

	if c.numActiveAddOperations < 0 {
		panic("Number of active migration operations cannot fall below 0.")
	}
}

// Replicas returns the replicas in the kernel.
func (c *DistributedKernelClient) Replicas() []scheduling.KernelReplica {
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

func (c *DistributedKernelClient) PodName(id int32) (string, error) {
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
func (c *DistributedKernelClient) PrepareNewReplica(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec {
	c.mu.Lock()
	defer c.mu.Unlock()

	if smrNodeId == -1 {
		smrNodeId = c.nextNodeId
		c.nextNodeId++
	}

	spec := &proto.KernelReplicaSpec{
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
func (c *DistributedKernelClient) AddReplica(r scheduling.KernelReplica, host *scheduling.Host) error {
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
		// Update signature scheme and key.
		c.server.Meta.SignatureScheme = r.ConnectionInfo().SignatureScheme
		c.server.Meta.Key = r.ConnectionInfo().Key

		// Collect the status of all replicas.
		c.busyStatus.Collect(context.Background(), 1, len(c.replicas), types.MessageKernelStatusStarting, c.pubIOMessage)
	}
	return nil
}

// RemoveReplica removes a kernel peer from the kernel.
func (c *DistributedKernelClient) RemoveReplica(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (*scheduling.Host, error) {
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
		return host, scheduling.ErrReplicaNotFound
	}

	delete(c.replicas, r.ReplicaID())

	// The replica ID starts with 1.
	// if int(r.ReplicaID()) > len(c.replicas) || c.replicas[r.ReplicaID()-1] != r {
	// 	return host, scheduling.ErrReplicaNotFound
	// }

	// c.replicas[r.ReplicaID()-1] = nil
	// c.size--

	if r.(*KernelReplicaClient).IsTraining() {
		err := r.(*KernelReplicaClient).TrainingStopped()
		if err != nil {
			c.log.Error("Error whilst stopping training on replica %d (during removal process): %v",
				r.ReplicaID(), err)
		}
	}

	//container := r.Container()
	//
	//// If the Container is actively-training, then we need to call TrainingStopped
	//// before removing it so that the resources are all returned appropriately.
	//if container.IsTraining() {
	//	err := container.TrainingStopped()
	//	if err != nil {
	//		c.log.Error("Failed to stop training on scheduling.Container %s-%d during replica removal because: %v", r.ID(), r.ReplicaID(), err)
	//	}
	//}

	err = host.ContainerRemoved(r.Container())
	if err != nil {
		c.log.Error("Failed to remove scheduling.Container %s-%d from Host %s because: %v", r.ID(), r.ReplicaID(), host.ID, err)
	}

	r.Container().SetHost(nil) // Set the Host to nil...
	return host, nil
}

func (c *DistributedKernelClient) GetReplicaByID(id int32) (scheduling.KernelReplica, error) {
	c.mu.RLock()
	// if id <= int32(len(c.replicas)) {
	// 	replica = c.replicas[id-1]
	// }
	replica, ok := c.replicas[id]
	c.mu.RUnlock()

	if replica == nil || !ok {
		validIds := make([]int32, 0, len(c.replicas))
		for validId := range c.replicas {
			validIds = append(validIds, validId)
		}
		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, validIds)
		return nil, scheduling.ErrReplicaNotFound
	}

	return replica, nil
}

// RemoveReplicaByID removes a kernel peer from the kernel by replica ID.
func (c *DistributedKernelClient) RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (*scheduling.Host, error) {
	c.mu.RLock()
	// var replica scheduling.KernelReplica
	// if id <= int32(len(c.replicas)) {
	// 	replica = c.replicas[id-1]
	// }
	replica, ok := c.replicas[id]
	c.mu.RUnlock()

	if replica == nil || !ok {
		validIds := make([]int32, 0, len(c.replicas))
		for validId := range c.replicas {
			validIds = append(validIds, validId)
		}
		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, validIds)
		return nil, scheduling.ErrReplicaNotFound
	}

	return c.RemoveReplica(replica, remover, noop)
}

// Validate validates the session.
func (c *DistributedKernelClient) Validate() error {
	if c.status >= types.KernelStatusRunning {
		return nil
	}

	return types.ErrKernelNotReady
}

// InitializeShellForwarder initializes the Shell serving.
func (c *DistributedKernelClient) InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types.Socket, error) {
	c.log.Debug("Initializing shell forwarder for distributed kernel client.")

	shell := c.server.Sockets.Shell
	if err := c.server.Listen(shell); err != nil {
		return nil, err
	}

	go c.server.Serve(c, shell, func(srv types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
		msg.AddDestinationId(c.KernelSpec().Id)
		// msg.Frames, _ = types.AddDestFrame(msg.Frames, c.KernelSpec().Id, jupyter.JOffsetAutoDetect)
		c.log.Debug("Received shell message via DistributedShellForwarder. Message: %v", msg)
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

// GetReadyReplica returns a replica that has already joined its SMR cluster and everything.
// GetReadyReplica returns nil if there are no ready replicas.
func (c *DistributedKernelClient) GetReadyReplica() scheduling.KernelReplica {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, replica := range c.replicas {
		if replica.IsReady() {
			return replica
		}
	}

	return nil
}

// IsReady returns true if ALL replicas associated with this distributed kernel client are ready.
func (c *DistributedKernelClient) IsReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, replica := range c.replicas {
		if !replica.IsReady() {
			return false
		}
	}

	return true
}

// IsReplicaReady returns true if the replica with the specified ID is ready.
func (c *DistributedKernelClient) IsReplicaReady(replicaId int32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	replica, ok := c.replicas[replicaId]
	if !ok {
		return false, scheduling.ErrReplicaNotFound
	}

	return replica.IsReady(), nil
}

// RequestWithHandler sends a request to all replicas and handles the response.
func (c *DistributedKernelClient) RequestWithHandler(ctx context.Context, _ string, typ types.MessageType, msg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error {
	return c.RequestWithHandlerAndReplicas(ctx, typ, msg, handler, done)
}

// Process a response to a shell message. This is called before the handler that was passed when issuing the request.
// Return true if the message is a 'yield' message (indicating that the replica yielded an execution).
func (c *DistributedKernelClient) preprocessShellResponse(replica scheduling.KernelInfo, msg *types.JupyterMessage) (err error, yielded bool) {
	// var (
	// 	header             *types.MessageHeader
	// 	jupyterMessageType string
	// )

	replicaClient := replica.(*KernelReplicaClient)
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
		err2 := c.handleExecutionYieldedNotification(replica.(*KernelReplicaClient), msgErr)
		return errors.Join(err, err2), true
	}

	return err, false
}

func (c *DistributedKernelClient) executionFinished(replicaId int32) error {
	if c.activeExecution != nil {
		if c.activeExecution.HasValidOriginalSentTimestamp() {
			latency := time.Since(c.activeExecution.OriginalSentTimestamp())
			c.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d. Total time elapsed since submission: %v.", c.activeExecution.ExecutionId(), c.id, replicaId, latency)
		} else {
			c.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d.", c.activeExecution.ExecutionId(), c.id, replicaId)
		}

		err := c.activeExecution.ReceivedLeadProposal(replicaId)
		if err != nil {
			return err
		}

		c.activeExecution.SetExecuted()
		return nil
	} else {
		return ErrNoActiveExecution
	}
}

// RequestWithHandlerAndReplicas sends a request to specified replicas and handles the response.
func (c *DistributedKernelClient) RequestWithHandlerAndReplicas(ctx context.Context, typ types.MessageType, jMsg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func(), replicas ...scheduling.KernelReplica) error {
	// Broadcast to all replicas if no replicas are specified.
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
		replicaCtx, cancel = context.WithTimeout(ctx, types.DefaultRequestTimeout)
	}
	// defer cancel()
	forwarder := func(replica scheduling.KernelInfo, typ types.MessageType, msg *types.JupyterMessage) (err error) {
		c.log.Debug(utils.BlueStyle.Render("Received %s response from %v"), typ.String(), replica)

		if typ == types.ShellMessage {
			// "Preprocess" the response, which involves checking if it is a YIELD notification, and handling a situation in which ALL replicas have proposed 'YIELD'.
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
	statusCtx, statusCancel := context.WithTimeout(context.Background(), types.DefaultRequestTimeout)
	defer statusCancel()
	c.busyStatus.Collect(statusCtx, len(c.replicas), len(c.replicas), types.MessageKernelStatusBusy, c.pubIOMessage)
	if len(replicas) == 1 {
		return replicas[0].(*KernelReplicaClient).requestWithHandler(replicaCtx, typ, jMsg, forwarder, c.getWaitResponseOption, done)
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
			if err := kernel.(*KernelReplicaClient).requestWithHandler(replicaCtx, typ, jMsg, forwarder, c.getWaitResponseOption, wg.Done); err != nil {
				c.log.Debug("Error while issuing %s '%s' request to kernel %s: %v", typ, jMsg.JupyterMessageType(), c.id, err)
			}
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
func (c *DistributedKernelClient) Shutdown(remover ReplicaRemover, restart bool) error {
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

			if host, err := c.stopReplicaLocked(replica.(*KernelReplicaClient), remover, false); err != nil {
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
	err := c.queryCloseLocked()
	if err != nil {
		c.log.Error("Error during query-close-locked: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearReplicasLocked()

	return c.status
}

func (c *DistributedKernelClient) stopReplicaLocked(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (*scheduling.Host, error) {
	c.log.Debug("Stopping replica %d of kernel %s now.", r.ReplicaID(), r.ID())
	host := r.Context().Value(CtxKernelHost).(*scheduling.Host)
	if err := remover(host, c.session, noop); err != nil {
		return host, err
	}

	// c.log.Debug("Waiting for replica %d of kernel %s to stop and return its status.", r.ReplicaID(), r.ID())
	// host.WaitKernel(context.Background(), &gateway.KernelId{Id: r.ID()})
	return host, nil
}

func (c *DistributedKernelClient) clearReplicasLocked() {
	c.log.Debug("Clearing replicas now.")
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

func (c *DistributedKernelClient) closeLocked() error {
	if !atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		<-c.cleaned
		return nil
	}

	c.BaseServer.Close()

	c.clearReplicasLocked()
	for _, socket := range c.server.Sockets.All {
		if socket != nil {
			_ = socket.Close()
		}
	}

	close(c.cleaned)

	return nil
}

func (c *DistributedKernelClient) queryCloseLocked() error {
	// Stop the replicas first.
	var stopped sync.WaitGroup
	stopped.Add(len(c.replicas))
	for _, replica := range c.replicas {
		if replica == nil {
			continue
		}

		go func(kernelReplica scheduling.KernelReplica) {
			defer stopped.Done()

			host := kernelReplica.Context().Value(CtxKernelHost).(*scheduling.Host)
			if _, err := host.WaitKernel(context.Background(), &proto.KernelId{Id: kernelReplica.ID()}); err != nil {
				c.log.Error("Error while waiting on replica %d of kernel %s to stop: %v",
					kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			}
		}(replica)
	}
	stopped.Wait()

	return nil
}

func (c *DistributedKernelClient) getWaitResponseOption(key string) interface{} {
	switch key {
	case jupyter.WROptionRemoveDestFrame:
		return true
	}

	return nil
}

// handleSmrLeadTaskMessage handles an types.MessageTypeSMRLeadTask IO Pub message.
// TODO: This logic is sort of buried away in a very non-obvious place...
func (c *DistributedKernelClient) handleSmrLeadTaskMessage(kernelReplica *KernelReplicaClient, msg *types.JupyterMessage) error {
	c.log.Debug("Received \"%s\" message from %v: %s", types.MessageTypeSMRLeadTask, kernelReplica.String(), msg.String())

	metadata, err := msg.DecodeMetadata()
	if err != nil {
		c.log.Error("Failed to decode metadata frame of \"%s\" message: %v", msg.JupyterMessageType(), err)
		return err // TODO: Should I panic here?
	}

	var (
		snapshot commonTypes.HostResourceSnapshot[*scheduling.ResourceSnapshot]
	)
	if val, loaded := metadata[scheduling.ResourceSnapshotMetadataKey]; !loaded {
		c.log.Error("Metadata frame of \"%s\" message did not contain an \"%s\" entry...",
			msg.JupyterMessageType(), scheduling.ResourceSnapshotMetadataKey)
		return err // TODO: Should I panic here?
	} else {
		snapshot = val.(commonTypes.HostResourceSnapshot[*scheduling.ResourceSnapshot])
	}

	if err = KernelStartedTraining(kernelReplica, snapshot); err != nil {
		c.log.Error("Failed to start training for kernel replica %s-%d: %v", c.id, kernelReplica.ReplicaID(), err)
		panic(err)
	}

	if c.activeExecution == nil {
		log.Fatalf("Kernel %s has started training; however, its active execution is nil...", c.id)
	}

	c.activeExecution.ActiveReplica = kernelReplica
	if c.activeExecution.HasValidOriginalSentTimestamp() {
		var (
			leadMessage types.MessageSMRLeadTask
			latency     time.Duration
		)

		frames := types.JupyterFrames(msg.Frames[msg.Offset:])
		if err := frames.DecodeContent(&leadMessage); err != nil {
			c.log.Error("Failed to decode content of SMR Lead ZMQ message: %v", err)

			// Since we can't get the exact value, we'll approximate it using the current timestamp.
			latency = time.Since(c.activeExecution.OriginalSentTimestamp())
		} else {
			startedProcessingAt := time.UnixMilli(leadMessage.UnixMilliseconds)

			// Difference between when the code began executing in the kernel and when the
			// associated "execute_request" message was actually sent.
			latency = startedProcessingAt.Sub(c.activeExecution.OriginalSentTimestamp())
		}

		c.log.Debug("Time elapsed between submission and starting to execute user's code: %v", latency)

		if !c.activeExecution.HasValidWorkloadId() {
			c.log.Warn("ActiveExecution had \"sent-at\" timestamp, but no workload ID...")
		}

		// Record metrics in Prometheus.
		c.executionLatencyCallback(latency, c.activeExecution.WorkloadId())
	} else {
		c.log.Warn("ActiveExecution did not have original \"send\" timestamp available.")
	}

	c.log.Debug("Session \"%s\" has successfully started training on replica %d.", c.id, kernelReplica.ReplicaID())

	return nil
}

func (c *DistributedKernelClient) handleMsg(replica types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
	switch typ {
	case types.IOMessage:
		topic, jFrames := replica.(*KernelReplicaClient).extractIOTopicFrame(msg)
		switch topic {
		case types.IOTopicStatus:
			{
				return c.handleIOKernelStatus(replica.(*KernelReplicaClient), jFrames, msg)
			}
		case types.MessageTypeSMRLeadTask:
			{
				return c.handleSmrLeadTaskMessage(replica.(*KernelReplicaClient), msg)
			}
		default:
			{
				return c.server.Sockets.IO.Send(*msg.Msg)
			}
		}
	default:
		{
		}
	}

	return ErrHandlerNotImplemented
}

func (c *DistributedKernelClient) handleIOKernelStatus(replica *KernelReplicaClient, frames types.JupyterFrames, msg *types.JupyterMessage) error {
	err := replica.handleIOKernelStatus(replica, frames, msg)
	if err != nil {
		return err
	}

	status, msg := replica.BusyStatus()
	// c.log.Debug("reducing replica %d status %s to match %s", replica.ReplicaID(), status, c.busyStatus.expectingStatus)
	c.busyStatus.Reduce(replica.ReplicaID(), status, msg, c.pubIOMessage)
	return nil
}

func (c *DistributedKernelClient) handleExecutionYieldedNotification(replica *KernelReplicaClient, msgErr types.MessageError) error {
	yieldError := errors.New(msgErr.ErrValue)
	c.log.Debug("Received 'YIELD' proposal from %v: %v", replica, yieldError)

	if c.activeExecution != nil {
		replicaId := replica.replicaId

		err := c.activeExecution.ReceivedYieldProposal(replicaId)
		if errors.Is(err, scheduling.ErrExecutionFailedAllYielded) {
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
func (c *DistributedKernelClient) handleFailedExecutionAllYielded() error {
	c.log.Warn("Kernel %s failed to execute code; all replicas proposed 'YIELD'.", c.id)
	return c.executionFailedCallback(c)
}

func (c *DistributedKernelClient) pubIOMessage(msg *types.JupyterMessage, status string, _ string) error {
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

			status, msg := replica.(*KernelReplicaClient).BusyStatus()
			if status == types.MessageKernelStatusIdle {
				c.busyStatus.Reduce(replica.(*KernelReplicaClient).ReplicaID(), types.MessageKernelStatusIdle, msg, c.pubIOMessage)
			}
		}
	}

	return err
}
