package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
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

type CodeExecutionQueue []*scheduling.ActiveExecution

// Enqueue adds an element to the end of the queue
func (q *CodeExecutionQueue) Enqueue(item *scheduling.ActiveExecution) {
	*q = append(*q, item)
}

// Dequeue removes and returns the element from the front of the queue
func (q *CodeExecutionQueue) Dequeue() *scheduling.ActiveExecution {
	if len(*q) == 0 {
		return nil // Queue is empty
	}
	item := (*q)[0]
	*q = (*q)[1:]
	return item
}

// ReplicaKernelInfo offers hybrid information that reflects the replica source of messages.
type ReplicaKernelInfo struct {
	scheduling.KernelInfo
	replica scheduling.KernelReplicaInfo
}

func (r *ReplicaKernelInfo) ReplicaID() int32 {
	return r.replica.ReplicaID()
}

func (r *ReplicaKernelInfo) String() string {
	return r.replica.String()
}

type DistributedClientProvider interface {
	NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec, numReplicas int, hostId string,
		connectionInfo *jupyter.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string,
		debugMode bool, executionFailedCallback scheduling.ExecutionFailedCallback,
		executionLatencyCallback scheduling.ExecutionLatencyCallback, messagingMetricsProvider metrics.MessagingMetricsProvider) scheduling.Kernel
}

// DistributedKernelClient is a client of a Distributed Jupyter Kernel that is used by the Gateway daemon.
// It wraps individual KernelReplicaClient instances -- one for each replica of the kernel.
type DistributedKernelClient struct {
	*server.BaseServer
	scheduling.SessionManager
	server *server.AbstractServer

	id             string
	status         jupyter.KernelStatus
	busyStatus     *AggregateKernelStatus
	lastBStatusMsg *messaging.JupyterMessage

	spec *proto.KernelSpec
	// replicas []scheduling.KernelReplica
	replicas map[int32]scheduling.KernelReplica
	// size     int

	persistentId    string
	shellListenPort int // Port that the KernelReplicaClient::shell socket listens on.
	iopubListenPort int // Port that the KernelReplicaClient::iopub socket listens on.

	numActiveAddOperations int // Number of active migrations of the associated kernel's replicas.

	nextNodeId int32

	// activeExecution is the current execution request that should be being processed by the kernels,
	// assuming the DefaultSchedulingPolicy Daemons forward the requests in the correct order.
	activeExecution *scheduling.ActiveExecution

	// activeExecutionsByExecuteRequestMsgId is a map used to keep track of all scheduling.ActiveExecutions
	// processed by the kernel. The keys are the Jupyter message IDs of the "execute_request" messages
	// sent by the client to submit code for execution.
	activeExecutionsByExecuteRequestMsgId *hashmap.CornelkMap[string, *scheduling.ActiveExecution]

	// activeExecutionQueue is a queue of ActiveExecution structs corresponding to
	// submitted/enqueued execution operations.
	activeExecutionQueue CodeExecutionQueue

	// Callback for when execution fails (such as all replicas proposing 'YIELD').
	executionFailedCallback scheduling.ExecutionFailedCallback

	// ExecutionLatencyCallback is provided by the internalCluster Gateway to each DistributedKernelClient.
	// When a DistributedKernelClient receives a notification that a kernel has started execution user-submitted code,
	// the DistributedKernelClient will check if its ActiveExecution struct has the original "sent-at" timestamp
	// of the original "execute_request". If it does, then it can calculate the latency between submission and when
	// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
	// so that a relevant Prometheus metric can be updated.
	executionLatencyCallback scheduling.ExecutionLatencyCallback

	// messagingMetricsProvider is an interface that enables the recording of metrics observed by the DistributedKernelClient.
	messagingMetricsProvider metrics.MessagingMetricsProvider

	session scheduling.UserSession

	debugMode bool

	log     logger.Logger
	mu      sync.RWMutex
	closing int32
	cleaned chan struct{}
}

// DistributedKernelClientProvider enables the creation of DistributedKernelClient structs.
type DistributedKernelClientProvider struct{}

// NewDistributedKernelClient creates a new DistributedKernelClient struct and returns
// a pointer to it in the form of an AbstractDistributedKernelClient interface.
func (p *DistributedKernelClientProvider) NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec, numReplicas int, hostId string,
	connectionInfo *jupyter.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string,
	debugMode bool, executionFailedCallback scheduling.ExecutionFailedCallback, executionLatencyCallback scheduling.ExecutionLatencyCallback,
	messagingMetricsProvider metrics.MessagingMetricsProvider) scheduling.Kernel {

	kernel := &DistributedKernelClient{
		id:                       spec.Id,
		persistentId:             persistentId,
		debugMode:                debugMode,
		messagingMetricsProvider: messagingMetricsProvider,
		server: server.New(ctx, &jupyter.ConnectionInfo{Transport: "tcp", SignatureScheme: connectionInfo.SignatureScheme, Key: connectionInfo.Key}, metrics.ClusterGateway, func(s *server.AbstractServer) {
			s.Sockets.Shell = messaging.NewSocket(zmq4.NewRouter(s.Ctx), shellListenPort, messaging.ShellMessage, fmt.Sprintf("DK-Router-Shell[%s]", spec.Id))
			s.Sockets.IO = messaging.NewSocket(zmq4.NewPub(s.Ctx), iopubListenPort, messaging.IOMessage, fmt.Sprintf("DK-Pub-IO[%s]", spec.Id)) // connectionInfo.IOSubPort}
			s.PrependId = true
			/* The DistributedKernelClient lives on the Gateway. The Shell forwarder only receives messages from the frontend, which should not be ACK'd. */
			s.ShouldAckMessages = false
			s.ReconnectOnAckFailure = false
			s.ComponentId = hostId
			s.DebugMode = debugMode
			s.Name = fmt.Sprintf("DistrKernelClient-%s", spec.Id)
			s.MessagingMetricsProvider = messagingMetricsProvider
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:                                jupyter.KernelStatusInitializing,
		spec:                                  spec,
		replicas:                              make(map[int32]scheduling.KernelReplica, numReplicas), // make([]scheduling.KernelReplica, numReplicas),
		cleaned:                               make(chan struct{}),
		shellListenPort:                       shellListenPort,
		iopubListenPort:                       iopubListenPort,
		activeExecutionsByExecuteRequestMsgId: hashmap.NewCornelkMap[string, *scheduling.ActiveExecution](32),
		numActiveAddOperations:                0,
		executionFailedCallback:               executionFailedCallback,
		activeExecutionQueue:                  make(CodeExecutionQueue, 0, 16),
		executionLatencyCallback:              executionLatencyCallback,
		nextNodeId:                            int32(numReplicas + 1),
	}
	kernel.BaseServer = kernel.server.Server()
	kernel.SessionManager = NewSessionManager(spec.Session)
	kernel.busyStatus = NewAggregateKernelStatus(kernel, numReplicas)
	kernel.log = kernel.server.Log
	return kernel
}

// SetSession sets/updates the scheduling.Session associated with the DistributedKernelClient.
func (c *DistributedKernelClient) SetSession(session scheduling.UserSession) {
	c.session = session
}

// GetSession returns the scheduling.Session associated with the DistributedKernelClient.
func (c *DistributedKernelClient) GetSession() scheduling.UserSession {
	return c.session
}

// GetContainers returns a slice containing all the scheduling.Container associated with the scheduling.Session
// (i.e., the scheduling.Session that itself is associated with the DistributedKernelClient).
func (c *DistributedKernelClient) GetContainers() []scheduling.KernelContainer {
	c.mu.Lock()
	defer c.mu.Unlock()

	containers := make([]scheduling.KernelContainer, len(c.replicas))
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

// GetActiveExecutionByExecuteRequestMsgId returns a pointer to the scheduling.ActiveExecution struct corresponding
// to the Jupyter "execute_request" message with the given Jupyter message ID, if one exists.
func (c *DistributedKernelClient) GetActiveExecutionByExecuteRequestMsgId(msgId string) (*scheduling.ActiveExecution, bool) {
	return c.activeExecutionsByExecuteRequestMsgId.Load(msgId)
}

func (c *DistributedKernelClient) ExecutionFailedCallback() scheduling.ExecutionFailedCallback {
	return c.executionFailedCallback
}

// SetActiveExecution attempts to set the ActiveExecution of the DistributedKernelClient.
// SetActiveExecution will replace the current ActiveExecution with the given ActiveExecution,
// bypassing all other ActiveExecution instances that are already enqueued.
func (c *DistributedKernelClient) SetActiveExecution(activeExecution *scheduling.ActiveExecution) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.activeExecutionsByExecuteRequestMsgId.Store(activeExecution.GetExecuteRequestMessageId(), activeExecution)
	c.activeExecution = activeExecution
}

// ExecutionComplete sets the current ActiveExecution to nil, or possibly to the next ActiveExecution in the queue,
// if the queue is non-empty. This also calls SessionStoppedTraining on the KernelReplicaClient that was performing the
// training.
//
// If there are any ActiveExecution structs enqueued, then the next struct is popped off the queue and
// assigned as the current ActiveExecution for the DistributedKernelClient. If this occurs, then
// ExecutionComplete returns true.
//
// If there are no ActiveExecution structs enqueued, then ExecutionComplete returns false.
func (c *DistributedKernelClient) ExecutionComplete(msg *messaging.JupyterMessage) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeExecution == nil {
		log.Fatalf(utils.RedStyle.Render("[ERROR] DistributedKernelClient %s does not have an active execution at all..."), c.id)
	} else if c.activeExecution.GetActiveReplica() == nil {
		log.Fatalf(utils.RedStyle.Render("[ERROR] DistributedKernelClient %s has an active execution, but the active replica is nil: %v"),
			c.id, c.activeExecution.String())
	}

	if msg.ReplicaId == -1 {
		log.Fatalf(utils.RedStyle.Render("[ERROR] Cannot determine which replica \"execute_reply\" %s message came from..."),
			msg.JupyterMessageId())
	}

	var (
		associatedExecution *scheduling.ActiveExecution
		loaded              bool
	)
	if msg.JupyterParentMessageId() != c.activeExecution.GetExecuteRequestMessageId() {
		c.log.Error("Received \"execute_reply\" targeting active execution \"%s\" of kernel %s; however, current active execution has \"execute_request\" ID = \"%s\"",
			msg.JupyterParentMessageId(), c.id, c.activeExecution.GetExecuteRequestMessageId())

		associatedExecution, loaded = c.activeExecutionsByExecuteRequestMsgId.Load(msg.JupyterParentMessageId())
		if !loaded {
			log.Fatalf(utils.RedStyle.Render("[ERROR] Cannot find active execution associated with \"execute_request\" \"%s\", for which we just received an \"execute_reply\"..."),
				msg.JupyterParentMessageId())
		}
	} else {
		associatedExecution = c.activeExecution
	}

	err := c.markExecutionAsComplete(associatedExecution, msg.ReplicaId)
	if err != nil {
		c.log.Error("Exception encountered while recording that active execution has finished: %v", err)
		return false, err
	}

	err = c.activeExecution.GetActiveReplica().KernelStoppedTraining()
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

// EnqueueActiveExecution will set the scheduling.ActiveExecution of the DistributedKernelClient to the
// given ActiveExecution if the DistributedKernelClient currently has no scheduling.ActiveExecution. In this case,
// EnqueueActiveExecution will return false.
//
// EnqueueActiveExecution returns a pointer to the new scheduling.ActiveExecution struct if one was created.
//
// If we are resubmitting an "execute_request" following a migration, then this will not create and return a new
// (pointer to a) scheduling.ActiveExecution struct, as the current active execution can simply be reused.
func (c *DistributedKernelClient) EnqueueActiveExecution(attemptId int, msg *messaging.JupyterMessage) *scheduling.ActiveExecution {
	c.mu.Lock()
	defer c.mu.Unlock()

	if msg.JupyterMessageType() != messaging.ShellExecuteRequest {
		log.Fatalf(utils.RedStyle.Render("Invalid or unexpected Jupyter message type of message passed to EnqueueActiveExecution: \"%s\". Message must have type \"%s\"."),
			msg.JupyterMessageType(), messaging.ShellExecuteRequest)
	}

	// Check if the Jupyter message ID for the "execute_request" associated with the current active execution
	// matches the message ID of the "execute_request" passed to EnqueueActiveExecution. If so, then we will
	// do a quick sanity check that we've already attempted (and failed) the current active execution, and then
	// we'll just return.
	if c.activeExecution != nil && c.activeExecution.GetExecuteRequestMessageId() == msg.JupyterMessageId() {
		c.log.Debug("Jupyter message ID of given \"execute_request\" matches that of current active execution (ID=\"%s\")",
			msg.JupyterMessageId())

		// Create the next execution attempt.
		nextExecutionAttempt := scheduling.NewActiveExecution(c.ID(), c.activeExecution.GetAttemptId()+1, c.Size(), msg)

		// Link the previous active execution with the current one (in both directions).
		nextExecutionAttempt.LinkPreviousAttempt(c.activeExecution)
		c.activeExecution.LinkNextAttempt(nextExecutionAttempt)

		// Replace the current execution attempt with the new one.
		c.activeExecution = nextExecutionAttempt

		// Replace the entry in the mapping with the next attempt.
		// We can still access the previous attempt by following the "previous attempt" link.
		c.activeExecutionsByExecuteRequestMsgId.Store(nextExecutionAttempt.ExecuteRequestMessageId, nextExecutionAttempt)

		c.log.Debug("Created, linked, and set next ActiveExecution attempt (#%d) for \"execute_request\" \"%s\" for Kernel %s: %v",
			nextExecutionAttempt.AttemptId, nextExecutionAttempt.ExecuteRequestMessageId, c.id, nextExecutionAttempt)

		// Return the next execution attempt.
		return nextExecutionAttempt
	}

	newActiveExecution := scheduling.NewActiveExecution(c.id, attemptId, c.Size(), msg)

	c.log.Debug("Setting or enqueuing new ActiveExecution targeting kernel %s for new \"execute_request\" \"%s\"",
		c.id, newActiveExecution.ExecuteRequestMessageId)

	c.activeExecutionsByExecuteRequestMsgId.Store(newActiveExecution.ExecuteRequestMessageId, newActiveExecution)

	// If there is already an active execution, then enqueue the new one.
	if c.activeExecution != nil {
		c.log.Debug("Found non-nil ActiveExec for kernel %s. Enqueuing new ActiveExec for \"execute_request\" \"%s\".",
			c.id, newActiveExecution.ExecuteRequestMessageId)
		c.activeExecutionQueue = append(c.activeExecutionQueue, newActiveExecution)
		c.log.Debug("Created and enqueued new ActiveExecution in Kernel %s's queue (which now has size=%d): %v",
			c.id, len(c.activeExecutionQueue), newActiveExecution)
		return newActiveExecution
	} else {
		c.log.Debug("No ActiveExec for kernel %s. Assigning ActiveExec targeting \"execute_request\" \"%s\".",
			c.id, newActiveExecution.ExecuteRequestMessageId)

		// There's no active execution currently, so just set the value.
		c.activeExecution = newActiveExecution
		c.log.Debug("Created and assigned new ActiveExecution to Kernel %s: %v", c.id, newActiveExecution)
		return newActiveExecution
	}
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

func (c *DistributedKernelClient) ResourceSpec() *types.DecimalSpec {
	return c.spec.DecimalSpecFromKernelSpec()
}

func (c *DistributedKernelClient) KernelSpec() *proto.KernelSpec {
	return c.spec
}

// ConnectionInfo returns the connection info.
func (c *DistributedKernelClient) ConnectionInfo() *jupyter.ConnectionInfo {
	return c.server.Meta
}

// Status returns the kernel status.
func (c *DistributedKernelClient) Status() jupyter.KernelStatus {
	return c.status
}

func (c *DistributedKernelClient) AggregateBusyStatus() string {
	return c.busyStatus.status
}

// BindSession binds a session ID to the client.
func (c *DistributedKernelClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	var restarted bool
	if c.status == jupyter.KernelStatusExited {
		// restarted = atomic.CompareAndSwapInt32((*int32)(&c.status), int32(jupyter.KernelStatusExited), int32(jupyter.KernelStatusInitializing))
		restarted = c.setStatus(jupyter.KernelStatusExited, jupyter.KernelStatusInitializing)
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
func (c *DistributedKernelClient) setStatus(oldStatus jupyter.KernelStatus, newStatus jupyter.KernelStatus) (swapped bool) {
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

// NumActiveExecutionOperations returns the number of scheduling.ActiveExecution structs registered with
// the kernel. This counts both the current scheduling.ActiveExecution as well as the length of the queue of
// scheduling.ActiveExecution structs.
//
// This method is thread safe.
func (c *DistributedKernelClient) NumActiveExecutionOperations() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var numActiveExecutions int

	if c.activeExecution != nil && !c.activeExecution.HasExecuted() {
		numActiveExecutions += 1
	}

	numActiveExecutions += len(c.activeExecutionQueue)

	return numActiveExecutions
}

// NumActiveMigrationOperations returns the number of active migrations of the associated kernel's replicas.
func (c *DistributedKernelClient) NumActiveMigrationOperations() int {
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

	return ret
}

func (c *DistributedKernelClient) PodOrContainerName(id int32) (string, error) {
	replica, err := c.GetReplicaByID(id)

	if err != nil {
		c.log.Debug("Could not find replica with id %d", id)
		return "", err
	}

	return replica.GetPodOrContainerName(), nil
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
	// 		spec.ReplicaID = int32(i + 1)
	// 		break
	// 	}
	// }
	// if spec.ReplicaID == 0 {
	// 	spec.ReplicaID = int32(len(c.replicas) + 1)
	// }

	// If we follows add one and remove one rule, replicas are expected to have no holes in
	// replica list (i.e. no nil in the list) at this point. So we spec.Replicas is leaved
	// empty, and leave invokers to deterministically fill the list.

	return spec
}

// AddReplica adds a replica peer to the kernel.
func (c *DistributedKernelClient) AddReplica(r scheduling.KernelReplica, host scheduling.Host) error {
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
	if statusChanged := c.setStatus(jupyter.KernelStatusInitializing, jupyter.KernelStatusRunning); statusChanged {
		// Update signature scheme and key.
		c.server.Meta.SignatureScheme = r.ConnectionInfo().SignatureScheme
		c.server.Meta.Key = r.ConnectionInfo().Key

		c.log.Debug("Replica %d of kernel %s is available. Kernel is ready. Assigned signature scheme \"%s\" and key \"%s\"",
			r.ReplicaID(), c.id, r.ConnectionInfo().SignatureScheme, r.ConnectionInfo().Key)

		// Collect the status of all replicas.
		c.busyStatus.Collect(context.Background(), 1, len(c.replicas), messaging.MessageKernelStatusStarting, c.pubIOMessage)
	}
	return nil
}

// RemoveReplica removes a kernel peer from the kernel.
func (c *DistributedKernelClient) RemoveReplica(r scheduling.KernelReplica, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
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

	if r.IsTraining() {
		err := r.KernelStoppedTraining()
		if err != nil {
			c.log.Error("Error whilst stopping training on replica %d (during removal process): %v",
				r.ReplicaID(), err)
		}
	}

	err = r.Container().ContainedStopped()
	if err != nil {
		c.log.Error("Failed to cleanly stop scheduling.Container %s-%d because: %v", r.ID(), r.ReplicaID(), err)
	}

	// If the error is either a ErrNilHost error or an ErrInvalidStateTransition error, then we probably didn't try
	// to call Host::ContainerRemoved, as the ContainerStopped method would have returned before that point.
	//
	// So, we'll explicitly try calling Host::ContainerRemoved now, but there's a good chance it'll fail (since there
	// was already some sort of issue when we tried to call ContainerStopped).
	if errors.As(err, &scheduling.ErrInvalidStateTransition) || errors.As(err, &scheduling.ErrNilHost) {
		hostContainerRemovalError := host.ContainerRemoved(r.Container())
		if hostContainerRemovalError != nil {
			c.log.Error("Failed to remove scheduling.Container %s-%d from Host %s because: %v", r.ID(), r.ReplicaID(), host.GetID(), hostContainerRemovalError)

			// If another error occurred, then we'll join the two together so that they get returned together.
			err = errors.Join(err, hostContainerRemovalError)
		}
	}

	delete(c.replicas, r.ReplicaID())

	r.Container().SetHost(nil) // Set the Host to nil...
	return host, err
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
func (c *DistributedKernelClient) RemoveReplicaByID(id int32, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
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
	if c.status >= jupyter.KernelStatusRunning {
		return nil
	}

	return jupyter.ErrKernelNotReady
}

// InitializeShellForwarder initializes the Shell serving.
func (c *DistributedKernelClient) InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*messaging.Socket, error) {
	c.log.Debug("Initializing shell forwarder for distributed kernel client.")

	shell := c.server.Sockets.Shell
	if err := c.server.Listen(shell); err != nil {
		return nil, err
	}

	go c.server.Serve(c, shell, func(srv messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		msg.AddDestinationId(c.KernelSpec().Id)
		c.log.Debug("Received shell message via DistributedShellForwarder. Message: %v", msg)
		return handler(srv.(*DistributedKernelClient), typ, msg)
	})

	return shell, nil
}

// InitializeIOForwarder initializes the IOPub serving.
func (c *DistributedKernelClient) InitializeIOForwarder() (*messaging.Socket, error) {
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
func (c *DistributedKernelClient) RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
	return c.RequestWithHandlerAndReplicas(ctx, typ, msg, handler, done)
}

// Process a response to a shell message. This is called before the handler that was passed when issuing the request.
// Return true if the message is a 'yield' message (indicating that the replica yielded an execution).
func (c *DistributedKernelClient) preprocessShellResponse(replica *KernelReplicaClient, msg *messaging.JupyterMessage) (error, bool) {
	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if msg.JupyterFrames.LenWithoutIdentitiesFrame(true) < 5 {
		c.log.Error("Received invalid Jupyter message from replica %d of kernel %s (detected in extractShellError)", replica.ReplicaID(), c.id)
		return messaging.ErrInvalidJupyterMessage, false
	}

	if len(*msg.JupyterFrames.ContentFrame()) == 0 {
		c.log.Warn("Received shell '%v' response with empty content.", msg.JupyterMessageType())
		return nil, false
	}

	var msgErr messaging.MessageError
	if err := json.Unmarshal(*msg.JupyterFrames.ContentFrame(), &msgErr); err != nil {
		c.log.Error("Failed to unmarshal shell message received from replica %d of kernel %s because: %v", replica.ReplicaID(), c.id, err)
		return err, false
	}

	// If it's not an "execute_reply" message, then we're done here.
	if msg.JupyterMessageType() != messaging.ShellExecuteReply {
		return nil, false
	}

	activeExec := c.getActiveExecution(msg.JupyterParentMessageId(), replica)
	if activeExec != nil && c.debugMode { // Replies are only saved if debug mode is enabled.
		err := activeExec.RegisterReply(replica.ReplicaID(), msg, true)
		if err != nil {
			c.log.Error("Failed to register \"execute_reply\" message: %v", err)
			return err, false
		}
	}

	if msgErr.Status == messaging.MessageStatusOK {
		replica.ReceivedExecuteReply(msg)
		return nil, false
	}

	if msgErr.ErrName == messaging.MessageErrYieldExecution {
		replica.ReceivedExecuteReply(msg)

		errWhileHandlingYield := c.handleExecutionYieldedNotification(replica, msg)
		if errWhileHandlingYield != nil {
			return errWhileHandlingYield, true
		}

		return messaging.ErrExecutionYielded, true
	}

	return nil, false
}

// markExecutionAsComplete records that the ActiveExecution of the DistributedKernelClient has been executed.
func (c *DistributedKernelClient) markExecutionAsComplete(execution *scheduling.ActiveExecution, replicaId int32) error {
	if execution == nil {
		panic("Execution is nil.")
	}

	if execution.HasValidOriginalSentTimestamp() {
		latency := time.Since(c.activeExecution.OriginalSentTimestamp())
		c.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d. Total time elapsed since submission: %v.",
			execution.GetExecutionId(), c.id, replicaId, latency)
	} else {
		c.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d.",
			execution.GetExecutionId(), c.id, replicaId)
	}

	err := execution.ReceivedLeadNotification(replicaId)
	if err != nil {
		return err
	}

	execution.SetExecuted()
	return nil
}

// getRequestContext returns a context.Context struct and a context.CancelFunc to be used by RequestWithHandlerAndReplicas.
// The 'type' of context returned depends upon the given typ messaging.MessageType.
func (c *DistributedKernelClient) getRequestContext(ctx context.Context, typ messaging.MessageType) (context.Context, context.CancelFunc) {
	if typ == messaging.ShellMessage {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, messaging.DefaultRequestTimeout)
}

// sendRequestToReplica is called when forwarding requests to more than one replica. This method forwards the given
// request to the specified scheduling.KernelReplica.
func (c *DistributedKernelClient) sendRequestToReplica(ctx context.Context, targetReplica scheduling.KernelReplica,
	jMsg *messaging.JupyterMessage, typ messaging.MessageType, responseReceivedWg *sync.WaitGroup, numResponsesSoFar *atomic.Int32,
	responseHandler scheduling.KernelReplicaMessageHandler) {

	var jupyterMessage *messaging.JupyterMessage
	if c.debugMode {
		jupyterMessage = jMsg.Clone()
	} else {
		jupyterMessage = jMsg
	}

	if jupyterMessage.JupyterMessageType() == messaging.ShellExecuteRequest || jupyterMessage.JupyterMessageType() == messaging.ShellYieldRequest {
		targetReplica.(*KernelReplicaClient).SentExecuteRequest(jupyterMessage)
	}

	// TODO: If the ACKs fail on this and we reconnect and retry, the responseReceivedWg.Done may be called too many times.
	// Need to fix this. Either make the timeout bigger, or... do something else. Maybe we don't need the pending request
	// to be cleared after the context ends; we just do it on ACK timeout.
	err := targetReplica.(*KernelReplicaClient).requestWithHandler(ctx, typ, jupyterMessage, responseHandler, c.getWaitResponseOption, func() {
		responseReceivedWg.Done()
		numResponsesSoFar.Add(1)
	})

	if err != nil {
		c.log.Error("Error while issuing %s '%s' request to targetReplica %s: %v",
			typ, jupyterMessage.JupyterMessageType(), c.id, err)
	}
}

// replaceResponseContentWithErrorMessage replaces the content of the given messaging.JupyterMessage with an
// encoded/serialized (to JSON) error message.
//
// replaceResponseContentWithErrorMessage returns nil on success.
func (c *DistributedKernelClient) replaceMessageContentWithError(resp *messaging.JupyterMessage, errName string, errMsg string) error {
	var originalContent map[string]interface{}
	err := resp.JupyterFrames.DecodeContent(&originalContent)
	if err != nil {
		c.log.Error("Failed to decode content of \"%s\" message \"%s\" (JupyterID=\"%s\") during content replacement: %v",
			resp.JupyterMessageType(), resp.RequestId, resp.JupyterMessageId(), err)
	}

	errorMessage := &messaging.MessageErrorWithOldContent{
		MessageError: &messaging.MessageError{
			Status:   messaging.MessageStatusError,
			ErrName:  errName,
			ErrValue: errMsg,
		},
		OriginalContent: originalContent,
	}

	err = resp.JupyterFrames.EncodeContent(errorMessage)
	if err != nil {
		c.log.Error("Failed to encode replacement content of \"%s\" message \"%s\" (JupyterID=\"%s\") during content replacement: %v",
			resp.JupyterMessageType(), resp.RequestId, resp.JupyterMessageId(), err)
		return err
	}

	c.log.Debug("Successfully replaced content of \"%s\" message \"%s\" (JupyterID=\"%s\"). Old content: %v. New content: %v.",
		resp.JupyterMessageType(), resp.RequestId, resp.JupyterMessageId(), originalContent, errorMessage)

	return nil
}

// getResponseForwarder returns a function that is to be passed to an individual scheduling.KernelReplica's
// requestWithHandler method as the handler parameter.
//
// getResponseForwarder returns a method that will forward the response from a scheduling.KernelReplica back to the
// Jupyter client, if necessary/appropriate.
//
// Important note: the error RETURNED by the response handler/forwarder does NOT reach the Jupyter client.
// The only way for something to get back to the Jupyter client is for the thing (e.g., a message) to be passed to
// the provided scheduling.KernelReplicaMessageHandler argument, as this typically handles sending a message back to
// the Jupyter Server and subsequently the Jupyter Client.
func (c *DistributedKernelClient) getResponseForwarder(handler scheduling.KernelReplicaMessageHandler, cancel context.CancelFunc, once *sync.Once) scheduling.KernelReplicaMessageHandler {
	return func(replica scheduling.KernelReplicaInfo, socketType messaging.MessageType, response *messaging.JupyterMessage) (respForwardingError error) {
		c.log.Debug(utils.BlueStyle.Render("Received %s \"%s\" response with Jupyter message ID \"%s\" from kernel %v"),
			socketType.String(), response.JupyterMessageType(), response.JupyterMessageId(), replica)
		response.ReplicaId = replica.ReplicaID()

		if socketType == messaging.ShellMessage {
			// "Preprocess" the response, which involves checking if it is a YIELD notification,
			// and handling a situation in which ALL replicas have proposed 'YIELD'.
			shellPreprocessingError, yielded := c.preprocessShellResponse(replica.(*KernelReplicaClient), response)

			// If we yielded, then the expectation is that the shellPreprocessingError will be a
			// messaging.ErrExecutionYielded. This is fine. But if the shellPreprocessingError is not a
			// messaging.ErrExecutionYielded (and is instead some other error), then that's problematic.
			//
			// For example, maybe all the replicas yielded, and then the handler for that was called, but that
			// handler encountered an error. In that case, we'll need to propagate the error back to the client.
			// So, we can't return nil here (in that scenario).
			if yielded && errors.Is(shellPreprocessingError, messaging.ErrExecutionYielded) {
				c.log.Debug("Discarding YIELD (%v \"%s\" response) with JupyterID=\"%s\" from kernel replica %v",
					socketType, response.JupyterMessageType(), response.JupyterMessageId(), replica)
				return nil
			}

			// If we get an error at this point, then we need this error to propagate back to the Jupyter client.
			// TODO: Replace content of response with an error message.
			if shellPreprocessingError != nil {
				c.log.Error("Error while pre-processing shell response for Jupyter \"%s\" message \"%s\" (JupyterID=\"%s\") targeting kernel \"%s\": %v",
					response.JupyterMessageType(), response.RequestId, response.JupyterParentMessageId(), c.id, shellPreprocessingError)

				replacementError := c.replaceMessageContentWithError(response, "", "")
				if replacementError != nil {
					c.log.Error("Failed to replace content of %s \"%s\" message because: %v",
						socketType.String(), response.JupyterMessageType(), replacementError)
				}
			}
		}

		// TODO: Remove this eventually once all bugs are fixed.
		if response.JupyterMessageType() == "ping_reply" {
			if handler == nil {
				panic("Handler is nil")
			}

			return handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, socketType, response)
		}

		// Handler will only be called once.
		// The handler is the thing that will send a message/response/reply back to the Jupyter server and
		// subsequently the Jupyter client.
		responseWasForwarded := false
		once.Do(func() {
			cancel()
			if handler != nil {
				c.log.Debug("Forwarding %v \"%s\" response with JupyterID=\"%s\" to client of kernel replica \"%s\"",
					socketType, response.JupyterMessageType(), response.JupyterMessageId(), replica)
				respForwardingError = handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, socketType, response)
			}
			responseWasForwarded = true
		})

		if !responseWasForwarded {
			c.log.Debug("Discarded %v \"%s\" response with JupyterID=\"%s\" from kernel replica %v",
				socketType, response.JupyterMessageType(), response.JupyterMessageId(), replica)
		}

		if respForwardingError != nil {
			c.log.Error("Error encountered while forwarding %v \"%s\" response with JupyterID=\"%s\" from kernel replica %v: %v",
				socketType, response.JupyterMessageType(), response.JupyterMessageId(), replica, respForwardingError)
		}

		return respForwardingError
	}
}

// RequestWithHandlerAndReplicas sends a request to specified replicas and handles the response.
//
// The forwarder function defined within this method must assign a value to the messaging.JupyterMessage's ReplicaID
// field. Importantly, it should assign a value to the received response from the kernel, not the jMsg parameter
// (of the DistributedKernelClient::RequestWithHandlerAndReplicas method) that is being sent out.
func (c *DistributedKernelClient) RequestWithHandlerAndReplicas(ctx context.Context, typ messaging.MessageType,
	jMsg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func(), replicas ...scheduling.KernelReplica) error {

	// Broadcast to all replicas if no replicas are specified.
	if len(replicas) == 0 {
		for _, replica := range c.replicas {
			replicas = append(replicas, replica)
		}
	}

	once := sync.Once{}
	replicaCtx, cancel := c.getRequestContext(ctx, typ)
	responseHandler := c.getResponseForwarder(handler, cancel, &once)

	//forwarder := func(replica scheduling.KernelReplicaInfo, socketType messaging.MessageType, msg *messaging.JupyterMessage) (err error) {
	//	c.log.Debug(utils.BlueStyle.Render("Received %s response from %v"), socketType.String(), replica)
	//	msg.ReplicaId = replica.ReplicaID()
	//
	//	if socketType == messaging.ShellMessage {
	//		// "Preprocess" the response, which involves checking if it is a YIELD notification,
	//		// and handling a situation in which ALL replicas have proposed 'YIELD'.
	//		shellPreprocessingError, yielded := c.preprocessShellResponse(replica.(*KernelReplicaClient), msg)
	//
	//		// If we yielded, then the expectation is that the shellPreprocessingError will be a
	//		// messaging.ErrExecutionYielded. This is fine. But if the shellPreprocessingError is not a
	//		// messaging.ErrExecutionYielded (and is instead some other error), then that's problematic.
	//		//
	//		// For example, maybe all the replicas yielded, and then the handler for that was called, but that
	//		// handler encountered an error. In that case, we'll need to propagate the error back to the client.
	//		// So, we can't return nil here (in that scenario).
	//		if yielded && errors.Is(shellPreprocessingError, messaging.ErrExecutionYielded) {
	//			return nil
	//		}
	//
	//		if shellPreprocessingError != nil {
	//			c.log.Error("Error while pre-processing shell response for Jupyter \"%s\" message \"%s\" (JupyterID=\"%s\") targeting kernel \"%s\": %v",
	//				msg.JupyterMessageType(), msg.RequestId, msg.JupyterParentMessageId(), c.id, shellPreprocessingError)
	//			return shellPreprocessingError
	//		}
	//	}
	//
	//	// TODO: Remove this eventually once all bugs are fixed.
	//	if msg.JupyterMessageType() == "ping_reply" {
	//		if handler == nil {
	//			panic("Handler is nil")
	//		}
	//
	//		return handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, socketType, msg)
	//	}
	//
	//	// Handler will only be called once.
	//	forwarded := false
	//	once.Do(func() {
	//		cancel()
	//		if handler != nil {
	//			err = handler(&ReplicaKernelInfo{KernelInfo: c, replica: replica}, socketType, msg)
	//		}
	//		forwarded = true
	//	})
	//	if !forwarded {
	//		c.log.Debug("Discard %v \"%s\" response from %v", socketType, msg.JupyterMessageType() /* header.MsgType */, replica)
	//	}
	//	return err
	//}

	// Send the request to all replicas.
	statusCtx, statusCancel := context.WithTimeout(context.Background(), messaging.DefaultRequestTimeout)
	defer statusCancel()
	c.busyStatus.Collect(statusCtx, len(c.replicas), len(c.replicas), messaging.MessageKernelStatusBusy, c.pubIOMessage)

	// Add the dest frame here, as there can be a race condition where multiple replicas will add the dest frame at the same time, leading to multiple dest frames.
	_, reqId, jOffset := jMsg.JupyterFrames.ExtractDestFrame(true)
	if reqId == "" {
		c.log.Debug("Adding destination '%s' to frames now. Old frames (%d): %v.",
			c.id, jMsg.JupyterFrames.Len(), jMsg.JupyterFrames.String())
		jMsg.AddDestinationId(c.id)
		c.log.Debug("Added destination '%s' to frames at offset %d. New frames (%d): %v.",
			c.id, jOffset, jMsg.JupyterFrames.Len(), jMsg.JupyterFrames.String())
	}

	// If there's just a single replica, then send the message to that one replica.
	if len(replicas) == 1 {
		return replicas[0].(*KernelReplicaClient).requestWithHandler(replicaCtx, typ, jMsg, responseHandler, c.getWaitResponseOption, done)
	}

	// Note: we do NOT need to create a barrier where the replicas all wait until they've each clone the
	// message, as we don't use the original message (in the case that the replicas send cloned versions).
	// Thus, the original message is never going to be changed, so it's safe for each replica to proceed
	// as soon as it clones the original message.
	var responseReceivedWg sync.WaitGroup
	numResponsesExpected := 0
	numResponsesSoFar := atomic.Int32{}

	// Iterate over each replica and forward the request to that kernel replica's Local Daemon,
	// which will then route the request to the kernel replica itself.
	for _, kernel := range replicas {
		if kernel == nil {
			continue
		}

		responseReceivedWg.Add(1)

		numResponsesExpected += 1
		go c.sendRequestToReplica(ctx, kernel, jMsg, typ, &responseReceivedWg, &numResponsesSoFar, responseHandler)
	}

	if done != nil {
		// If a `done` callback function was given to us, then we'll create a goroutine to handle it.
		go c.handleDoneCallbackForRequest(done, &responseReceivedWg, typ, jMsg, &numResponsesSoFar, numResponsesExpected)
	}

	return nil
}

// handleDoneCallbackForRequest is called from RequestWithHandlerAndReplicas when the RequestWithHandlerAndReplicas
// method is passed a non-nil done callback function.
//
// handleDoneCallbackForRequest should (probably) be called in its own goroutine (unless you have some particular,
// valid reason to want to call this in a blocking capacity, but that'll block the server underlying the
// DistributedKernelClient, and we typically don't want to do that).
//
// handleDoneCallbackForRequest basically listens for responses from each of the kernel replicas and calls done
// accordingly. handleDoneCallbackForRequest will ultimately spawn its own goroutine(s) to carry out the necessary logic.
func (c *DistributedKernelClient) handleDoneCallbackForRequest(done func(), responseReceivedWg *sync.WaitGroup,
	typ messaging.MessageType, jMsg *messaging.JupyterMessage, numResponsesSoFar *atomic.Int32, numResponsesExpected int) {

	st := time.Now()
	allResponsesReceivedNotificationChannel := make(chan interface{}, 1)

	// TODO: Make sure we can reliably resubmit messages if necessary, due to shell
	// 		 messages being blocked behind long-running "execute_request" messages.
	var timeout time.Duration
	if typ == messaging.ShellMessage {
		timeout = time.Second * 120
	} else {
		timeout = time.Second * 60
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Spawn a goroutine to just send a notification when the sync.WaitGroup reaches 0.
	go func() {
		responseReceivedWg.Wait()                             // Wait til the WaitGroup reaches 0.
		allResponsesReceivedNotificationChannel <- struct{}{} // Send the notification.
	}()

	select {
	case <-allResponsesReceivedNotificationChannel:
		{
			// The WaitGroup counter reached 0. Call done, and then return.
			c.log.Debug("Received all %d replies for %s \"%s\" message %s (JupyterID=\"%s\"). "+
				"Calling 'done' callback now. Time elapsed: %v.", numResponsesExpected, typ.String(),
				jMsg.JupyterParentMessageType(), jMsg.RequestId, jMsg.JupyterParentMessageId(), time.Since(st))

			// Call the 'done' callback.
			done()

			// Cancel the context before we return.
			cancel()
			return
		}
	case <-ctx.Done():
		{
			// We timed-out. What we do now depends on what type of request was sent.
			// For now, we'll always just keep waiting.
			// But we'll log different types of messages depending on how long we've been waiting.
			if typ == messaging.ShellMessage {
				// If this is a Shell request and the kernel is training, then it (probably) makes
				// sense that we've not yet received a response.
				//
				// If we aren't training, then it may be a little suspect that our message hasn't been
				// processed yet. We'll log a warning message, but we'll keep waiting.
				c.log.Warn("Giving up. Have been waiting for a total of %v for all responses to %s \"%s\" request %s (JupyterID=\"%s\"). Received %d/%d responses so far.",
					time.Since(st), typ.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), numResponsesSoFar.Load(), numResponsesExpected)
			} else {
				// We've waited for over 5 minutes, and we've not heard anything. This is a non-shell message.
				// NOTE: If we're in debug mode, then jMsg will not necessarily be the exact same message that was sent to the replica,
				// as we clone the messages before sending them!!!
				c.log.Warn("Giving up. Have been waiting for a total of %v for all responses to %s \"%s\" request %s (JupyterID=\"%s\"). Received %d/%d responses so far.",
					time.Since(st), typ.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), numResponsesSoFar.Load(), numResponsesExpected)
			}
		}
	}
}

// Shutdown releases all replicas and closes the session.
func (c *DistributedKernelClient) Shutdown(remover scheduling.ReplicaRemover, restart bool) error {
	c.log.Debug("Shutting down Kernel %s.", c.id)
	c.mu.RLock()

	if c.status == jupyter.KernelStatusExited {
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

		go func(replica scheduling.KernelReplica) {
			defer stopped.Done()

			if host, err := c.stopReplicaLocked(replica, remover, false); err != nil {
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

	if c.status == jupyter.KernelStatusExited {
		return nil
	}

	return c.closeLocked()
}

// WaitClosed waits for the replicas to be cleaned.
func (c *DistributedKernelClient) WaitClosed() jupyter.KernelStatus {
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

func (c *DistributedKernelClient) stopReplicaLocked(r scheduling.KernelReplica, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.log.Debug("Stopping replica %d of kernel %s now.", r.ReplicaID(), r.ID())
	host := r.Context().Value(CtxKernelHost).(scheduling.Host)
	if err := remover(host, c.session, noop); err != nil {
		return host, err
	}
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

	c.status = jupyter.KernelStatusExited
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

			host := kernelReplica.Context().Value(CtxKernelHost).(scheduling.Host)
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

// extractResourceSnapshotFromRequestMetadata extracts the *scheduling.ManagerSnapshot from the metadata
// of the given Jupyter ZMQ message.
//
// The returned error will be nil on success. If a *scheduling.ManagerSnapshot could not be extracted from
// the given/specified Jupyter ZMQ message, then the error will be non-nil.
//func (c *DistributedKernelClient) extractResourceSnapshotFromRequestMetadata(msg *messaging.JupyterMessage) (*resource.ManagerSnapshot, error) {
//	var snapshotWrapper *resource.MetadataResourceWrapperSnapshot
//	metadataFrame := msg.JupyterFrames.MetadataFrame()
//	err := metadataFrame.Decode(&snapshotWrapper)
//	if err != nil {
//		c.log.Error("Failed to decode metadata frame of \"%s\" message: %v", msg.JupyterMessageType(), err)
//		return nil, err // TODO: Should I panic here?
//	}
//
//	snapshot := snapshotWrapper.ManagerSnapshot
//	c.log.Debug(utils.LightBlueStyle.Render("Extracted ManagerSnapshot from metadata frame of Jupyter \"%s\" message: %s"),
//		messaging.MessageTypeSMRLeadTask, snapshot.String())
//
//	return snapshot, nil
//}

// handleSmrLeadTaskMessage handles an jupyter.MessageTypeSMRLeadTask IO Pub message.
// TODO: This logic is sort of buried away in a very non-obvious place...
func (c *DistributedKernelClient) handleSmrLeadTaskMessage(kernelReplica *KernelReplicaClient, msg *messaging.JupyterMessage) error {
	c.log.Debug("Received \"%s\" message from %v: %s", messaging.MessageTypeSMRLeadTask, kernelReplica.String(), msg.String())

	if c.activeExecution == nil {
		log.Fatalf("Kernel %s has started training; however, its active execution is nil...", c.id)
	}

	// Decode the jupyter.MessageSMRLeadTask message.
	var leadMessage messaging.MessageSMRLeadTask
	if err := msg.JupyterFrames.DecodeContent(&leadMessage); err != nil {
		log.Fatalf(utils.RedStyle.Render("Failed to decode content of SMR Lead ZMQ message: %v\n"), err)
	}

	// The time at which the kernel replica began executing the code.
	startedProcessingAt := time.UnixMilli(leadMessage.UnixMilliseconds)

	// The ID of the Jupyter "execute_request" message that initiated the associated training.
	executeRequestMsgId := leadMessage.ExecuteRequestMsgId

	// Ensure that the "smr_lead_task" message that we just got is associated with the current ActiveExecution struct.
	var targetActiveExecution *scheduling.ActiveExecution
	if c.activeExecution.GetExecuteRequestMessageId() != executeRequestMsgId {
		c.log.Warn("Received 'smr_lead_task' notification for active execution associated with \"execute_request\" %s; "+
			"however, the current active execution is associated with \"execute_request\" %s...",
			executeRequestMsgId, c.activeExecution.GetExecuteRequestMessageId())

		// See if we can retrieve the ActiveExecution associated with the "smr_lead_task" message.
		associatedActiveExecution, loaded := c.activeExecutionsByExecuteRequestMsgId.Load(executeRequestMsgId)
		if !loaded {
			log.Fatalf(utils.RedStyle.Render("[ERROR] Cannot find active execution with \"execute_request\" message ID of \"%s\" associated with kernel \"%s\"...\n"),
				executeRequestMsgId, c.id)
		} else {
			c.log.Warn("Received \"smr_lead_task\" notification for non-current ActiveExecution.")
			c.log.Warn("Current active execution: %s", c.activeExecution.String())
			c.log.Warn("Target active execution (of \"smr_lead_task\" message): %s", associatedActiveExecution.String())
			// log.Fatalf(utils.RedStyle.Render("[ERROR] Received \"smr_lead_task\" notification for non-current ActiveExecution.\n"))

			targetActiveExecution = associatedActiveExecution
		}
	} else {
		targetActiveExecution = c.activeExecution
	}

	if targetActiveExecution.HasValidOriginalSentTimestamp() {
		// Measure of the interactivity.
		// The latency here is calculated as the difference between when the kernel replica began executing the
		// user-submitted code, and the time at which the user's Jupyter client sent the "execute_request" message.
		latency := startedProcessingAt.Sub(targetActiveExecution.OriginalSentTimestamp())

		// Record metrics in Prometheus.
		if targetActiveExecution.HasValidWorkloadId() {
			c.executionLatencyCallback(latency, targetActiveExecution.GetWorkloadId(), kernelReplica.id)
		} else {
			c.log.Warn("ActiveExecution for \"execute_request\" \"%s\" had \"sent-at\" timestamp, but no workload ID...",
				targetActiveExecution.GetExecuteRequestMessageId())
		}
	} else {
		c.log.Warn("ActiveExecution for \"execute_request\" \"%s\" did not have original \"send\" timestamp available.",
			targetActiveExecution.GetExecuteRequestMessageId())
	}

	targetActiveExecution.SetActiveReplica(kernelReplica)

	// Record that the kernel has started training.
	if err := kernelReplica.KernelStartedTraining(); err != nil {
		c.log.Error("Failed to start training for kernel replica %s-%d: %v", c.id, kernelReplica.ReplicaID(), err)
		panic(err)
	}

	c.log.Debug("Session \"%s\" has successfully started training on replica %d.", c.id, kernelReplica.ReplicaID())

	return nil
}

func (c *DistributedKernelClient) handleMsg(replica messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	switch typ {
	case messaging.IOMessage:
		topic, jFrames := replica.(*KernelReplicaClient).extractIOTopicFrame(msg)
		switch topic {
		case messaging.IOTopicStatus:
			{
				return c.handleIOKernelStatus(replica.(*KernelReplicaClient), jFrames, msg)
			}
		case messaging.MessageTypeSMRLeadTask:
			{
				return c.handleSmrLeadTaskMessage(replica.(*KernelReplicaClient), msg)
			}
		default:
			{
				return c.server.Sockets.IO.Send(*msg.GetZmqMsg())
			}
		}
	default:
		{
		}
	}

	return ErrHandlerNotImplemented
}

func (c *DistributedKernelClient) handleIOKernelStatus(replica *KernelReplicaClient, frames *messaging.JupyterFrames, msg *messaging.JupyterMessage) error {
	err := replica.handleIOKernelStatus(replica, frames, msg)
	if err != nil {
		return err
	}

	status, msg := replica.BusyStatus()
	// c.log.Debug("reducing replica %d status %s to match %s", replica.ReplicaID(), status, c.busyStatus.expectingStatus)
	c.busyStatus.Reduce(replica.ReplicaID(), status, msg, c.pubIOMessage)
	return nil
}

// getActiveExecution returns the *scheduling.ActiveExecution associated with the given "execute_request" message ID.
func (c *DistributedKernelClient) getActiveExecution(msgId string, replica *KernelReplicaClient) *scheduling.ActiveExecution {
	var associatedActiveExecution *scheduling.ActiveExecution
	if c.activeExecution == nil {
		c.log.Error("Received 'YIELD' proposal from %v, but we have no active execution...", replica)
		associatedActiveExecution, _ = c.activeExecutionsByExecuteRequestMsgId.Load(msgId)
	} else if c.activeExecution.GetExecuteRequestMessageId() != msgId {
		c.log.Warn("Received 'YIELD' proposal for ActiveExecution associated with \"execute_request\" %s; however, current ActiveExecution is associated with \"execute_request\" %s...",
			msgId, c.activeExecution.GetExecuteRequestMessageId())
		associatedActiveExecution, _ = c.activeExecutionsByExecuteRequestMsgId.Load(msgId)
	} else {
		associatedActiveExecution = c.activeExecution
	}

	// If we couldn't find the associated active execution at all, then we should panic. That's bad.
	if associatedActiveExecution == nil {
		return nil
	}

	return associatedActiveExecution
}

// handleExecutionYieldedNotification is called when we receive a 'YIELD' proposal from a replica of a kernel.
// handleExecutionYieldedNotification registers the 'yield' proposal with the kernel's current scheduling.ActiveExecution
// struct. If we find that we've received all three proposals, and they were ALL 'yield', then we'll handle the situation
// according to the scheduling policy that we've been configured to use.
func (c *DistributedKernelClient) handleExecutionYieldedNotification(replica *KernelReplicaClient, msg *messaging.JupyterMessage) error {
	// targetExecuteRequestId is the Jupyter message ID of the "execute_request" message associated
	// with the 'YIELD' proposal that we just received.
	targetExecuteRequestId := msg.JupyterParentMessageId()

	// It's possible we received a 'YIELD' proposal for an ActiveExecution different from the current one.
	// So, retrieve the ActiveExecution associated with the 'YIELD' proposal (using the "execute_request" message IDs).
	associatedActiveExecution := c.getActiveExecution(targetExecuteRequestId, replica)

	// If we couldn't find the associated active execution at all, then we should panic. That's bad.
	if associatedActiveExecution == nil {
		log.Fatalf(utils.RedStyle.Render("Received 'YIELD' proposal from replica %d of kernel %s targeting unknown ActiveExecution associated with an \"execute_request\" message with msg_id=\"%s\"..."),
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId)
	}

	// Mark that we received the 'YIELD' proposal for the associated ActiveExecution.
	err := associatedActiveExecution.ReceivedYieldNotification(replica.ReplicaID())

	var currentStatus string
	if associatedActiveExecution == c.activeExecution {
		currentStatus = "current"
	} else {
		currentStatus = "non-current"
	}
	c.log.Debug("Received 'YIELD' proposal from replica %d of kernel %s for %s ActiveExecution associated with \"execute_request\" \"%s\". Received %d/%d proposals from replicas of kernel %s.",
		replica.ReplicaID(), replica.ID(), currentStatus, targetExecuteRequestId, associatedActiveExecution.NumRolesReceived(), associatedActiveExecution.GetNumReplicas(), replica.ID())

	if errors.Is(err, scheduling.ErrExecutionFailedAllYielded) {
		if currentStatus == "current" {
			return c.handleFailedExecutionAllYielded()
		} else {
			// This just really shouldn't happen...
			log.Fatalf(utils.RedStyle.Render("[ERROR] All replicas have proposed 'YIELD' for non-current ActiveExecution associated with \"execute_request\" \"%s\"...\n"), targetExecuteRequestId)
		}
	} else if err != nil {
		c.log.Error("Encountered error while processing 'YIELD' proposal from replica %d of kernel %s for %s ActiveExecution associated with \"execute_request\" \"%s\": %v",
			replica.ReplicaID(), replica.ID(), currentStatus, targetExecuteRequestId, err)

		return err
	}

	return nil
}

// Handle a failed execution in which all replicas proposed 'YIELD'.
//
// Note that 'final' means that it was the last replica whose message we received; all replicas proposed 'YIELD',however.
func (c *DistributedKernelClient) handleFailedExecutionAllYielded() error {
	c.log.Warn("Kernel %s failed to execute code; all replicas proposed 'YIELD'.", c.id)
	return c.executionFailedCallback(c)
}

func (c *DistributedKernelClient) pubIOMessage(msg *messaging.JupyterMessage, status string, how string) error {
	c.log.Debug("Publishing %v status(%s:%s): %v", messaging.IOMessage, status, how, msg)
	c.lastBStatusMsg = msg

	zmqMsg := msg.GetZmqMsg()

	var err error
	if zmqMsg != nil {
		err = c.server.Sockets.IO.Send(*zmqMsg)
	} else {
		c.log.Error("ZMQ4 Message is null. Cannot publish IO message... status: \"%s\", how: \"%s\"", status, how)
		err = fmt.Errorf("zmq4 message is null")
	}

	// Initiate idle status collection.
	if status == messaging.MessageKernelStatusBusy {
		c.busyStatus.Collect(context.Background(), len(c.replicas), len(c.replicas), messaging.MessageKernelStatusIdle, c.pubIOMessage)
		// Fill matched status that has been received before collecting.
		for _, replica := range c.replicas {
			if replica == nil {
				continue
			}

			status, msg := replica.BusyStatus()
			if status == messaging.MessageKernelStatusIdle {
				c.busyStatus.Reduce(replica.ReplicaID(), messaging.MessageKernelStatusIdle, msg, c.pubIOMessage)
			}
		}
	}

	return err
}
