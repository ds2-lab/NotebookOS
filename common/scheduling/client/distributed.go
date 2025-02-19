package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"golang.org/x/sync/semaphore"
	"slices"
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

var (
	CtxKernelHost = utils.ContextKey("host")

	ErrNoReplicas          = errors.New("kernel has no replicas")
	ErrAlreadyShuttingDown = errors.New("kernel is already in the process of shutting down")
)

// shutdownNotification is used to notify that a replica shutdown has either failed or succeeded.
type shutdownNotification struct {
	Error         error
	KernelReplica scheduling.KernelReplica
	ReplicaId     int32
	Host          scheduling.Host
}

// ReplicaKernelInfo offers hybrid information that reflects the replica source of messages.
type ReplicaKernelInfo struct {
	scheduling.KernelInfo
	replica scheduling.KernelReplicaInfo
}

// ReplicaID returns the SMR node ID of the associated KernelReplica.
func (r *ReplicaKernelInfo) ReplicaID() int32 {
	return r.replica.ReplicaID()
}

func (r *ReplicaKernelInfo) String() string {
	return r.replica.String()
}

// TemporaryKernelReplicaClient structs are used in place of KernelReplicaClient structs when the replica container(s)
// of a given kernel is/are not scheduled, and that kernel receives a message.
type TemporaryKernelReplicaClient struct {
	*DistributedKernelClient
}

func (c *TemporaryKernelReplicaClient) ReplicaID() int32 {
	return 0 // 0 is never used as an actual replica ID, so it indicates that it's a "fake" replica.
}

// DistributedKernelClient is a client of a Distributed Jupyter kernel that is used by the Gateway daemon.
// It wraps individual KernelReplicaClient instances -- one for each replica of the kernel.
type DistributedKernelClient struct {
	scheduling.SessionManager

	// ExecutionManager is responsible for managing the user-submitted cell executions.
	ExecutionManager scheduling.ExecutionManager

	session scheduling.UserSession

	log logger.Logger
	*server.BaseServer
	server *server.AbstractServer

	busyStatus     *AggregateKernelStatus
	lastBStatusMsg *messaging.JupyterMessage

	shuttingDown atomic.Int32

	temporaryKernelReplicaClient *TemporaryKernelReplicaClient

	spec *proto.KernelSpec
	// replicas map[int32]scheduling.KernelReplica
	replicas hashmap.HashMap[int32, scheduling.KernelReplica]

	// notificationCallback is used to send notifications to the frontend dashboard from this kernel/client.
	notificationCallback scheduling.NotificationCallback

	cleaned chan struct{}

	id string

	persistentId string

	mu sync.RWMutex

	// replicasMutex provides atomicity for operations that add or remove kernel replicas from the replicas slice.
	//replicasMutex sync.RWMutex

	status            jupyter.KernelStatus
	targetNumReplicas int32

	numActiveAddOperations atomic.Int32 // Number of active migrations of the associated kernel's replicas.

	// replicaContainersStartedAt is the time at which the target DistributedKernelClient's scheduling.KernelContainer
	// instances last started.
	replicaContainersStartedAt time.Time

	// replicaContainersStoppedAt is the time at which the DistributedKernelClient's scheduling.KernelContainer
	// instances last stopped.
	replicaContainersStoppedAt time.Time

	// createReplicaContainersAttempt is non-nil when there's an active 'create containers' operation for this
	// DistributedKernelClient. The createReplicaContainersAttempt encapsulates info about that operation.
	//
	// In order to check if there's an active operation, one should inspect the value of the
	// createReplicaContainersAttempt variable. Checking if createReplicaContainersAttempt is non-nil is not the
	// correct or intended way to check whether there is an active 'create containers' operation.
	createReplicaContainersAttempt *CreateReplicaContainersAttempt

	// replicaContainersAreBeingScheduled indicates whether there is an active 'create containers' operation.
	// It is the ground truth, whereas the non-nil-ness of the createReplicaContainersAttempt variable is sort of
	// a proxy indicator. (That is, createReplicaContainersAttempt is only non-nil when there is an active operation,
	// but in order to check if there's an active operation, one should inspect the value of the
	// createReplicaContainersAttempt variable.)
	//
	// When there is an active operation, the value of createReplicaContainersAttempt will be strictly positive.
	replicaContainersAreBeingScheduled atomic.Int32

	// removeReplicaContainersAttempt is non-nil when there's an active 'remove containers/replicas' operation for
	// this DistributedKernelClient. The removeReplicaContainersAttempt encapsulates info about that operation.
	//
	// In order to check if there's an active operation, one should inspect the value of the
	// replicaContainersAreBeingRemoved variable. Checking if removeReplicaContainersAttempt is non-nil is not the
	// correct or intended way to check whether there is an active 'remove containers/replicas' operation.
	removeReplicaContainersAttempt *RemoveReplicaContainersAttempt

	// replicaContainersAreBeingRemoved indicates whether a removal is occurring.
	replicaContainersAreBeingRemoved atomic.Int32

	// isIdleReclaimed indicates whether the DistributedKernelClient is in a state of being idle reclaimed.
	isIdleReclaimed atomic.Bool

	// numContainersCreated is the total number of KernelContainer instances that have been created or provisioned
	// for the target Kernel over the target Kernel's entire lifetime. This includes the very first creation of any
	// KernelContainer instance(s) as well as any KernelContainer instance(s) created during migrations or as on-demand.
	numContainersCreated atomic.Int32

	// NumColdContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
	// for the target Kernel, the KernelReplica / KernelContainer was created using a cold KernelContainer.
	numColdContainersUsed atomic.Int32

	// NumWarmContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
	// for the target Kernel, the KernelReplica / KernelContainer was created using a warm KernelContainer.
	numWarmContainersUsed atomic.Int32

	nextNodeId atomic.Int32

	closing int32

	debugMode bool
}

// DistributedKernelClientProvider enables the creation of DistributedKernelClient structs.
type DistributedKernelClientProvider struct{}

//func (p *DistributedKernelClientProvider) NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
//	numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
//	executionFailedCallback scheduling.ExecutionFailedCallback, executionLatencyCallback scheduling.ExecutionLatencyCallback,
//	statisticsProvider scheduling.StatisticsProvider, notificationCallback scheduling.NotificationCallback) scheduling.Kernel {

// NewDistributedKernelClient creates a new DistributedKernelClient struct and returns
// a pointer to it in the form of an AbstractDistributedKernelClient interface.
func (p *DistributedKernelClientProvider) NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
	numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
	statisticsProvider scheduling.StatisticsProvider, callbackProvider scheduling.CallbackProvider) scheduling.Kernel {

	kernelClient := &DistributedKernelClient{
		id:           spec.Id,
		persistentId: persistentId,
		debugMode:    debugMode,
		server: server.New(ctx, &jupyter.ConnectionInfo{Transport: "tcp", SignatureScheme: connectionInfo.SignatureScheme, Key: connectionInfo.Key}, metrics.ClusterGateway, func(s *server.AbstractServer) {
			s.Sockets.Shell = messaging.NewSocket(zmq4.NewRouter(s.Ctx), 0, messaging.ShellMessage, fmt.Sprintf("DK-Router-Shell[%s]", spec.Id))
			s.Sockets.IO = messaging.NewSocket(zmq4.NewPub(s.Ctx), 0, messaging.IOMessage, fmt.Sprintf("DK-Pub-IO[%s]", spec.Id)) // connectionInfo.IOSubPort}
			s.PrependId = true
			/* The DistributedKernelClient lives on the Gateway. The Shell forwarder only receives messages from the frontend, which should not be acknowledged. */
			s.ShouldAckMessages = false
			s.ReconnectOnAckFailure = false
			s.ComponentId = hostId
			s.DebugMode = debugMode
			s.Name = fmt.Sprintf("DistrKernelClient-%s", spec.Id)
			s.StatisticsAndMetricsProvider = statisticsProvider
			config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Id))
		}),
		status:               jupyter.KernelStatusInitializing,
		notificationCallback: callbackProvider.NotificationCallback,
		spec:                 spec,
		targetNumReplicas:    int32(numReplicas),
		cleaned:              make(chan struct{}),
		replicas:             hashmap.NewCornelkMap[int32, scheduling.KernelReplica](16),
		// replicas:             make(map[int32]scheduling.KernelReplica, numReplicas), // make([]scheduling.Replica, numReplicas),
	}
	kernelClient.nextNodeId.Store(int32(numReplicas + 1))
	kernelClient.BaseServer = kernelClient.server.Server()
	kernelClient.SessionManager = NewSessionManager(spec.Session)
	kernelClient.busyStatus = NewAggregateKernelStatus(kernelClient, numReplicas)
	kernelClient.log = kernelClient.server.Log

	temporaryKernelReplicaClient := &TemporaryKernelReplicaClient{kernelClient}
	kernelClient.temporaryKernelReplicaClient = temporaryKernelReplicaClient

	kernelClient.ExecutionManager = NewExecutionManager(kernelClient, numReplicas, statisticsProvider, callbackProvider)

	return kernelClient
}

// IsIdleReclaimed returns true if the DistributedKernelClient has been idle reclaimed.
func (c *DistributedKernelClient) IsIdleReclaimed() bool {
	return c.isIdleReclaimed.Load()
}

// IdleReclaim removes the replicas of the target DistributedKernelClient and sets its isIdleReclaimed flag to true.
func (c *DistributedKernelClient) IdleReclaim() error {
	return nil
}

// LastTrainingSubmittedAt returns the time at which the last training to occur was submitted to the kernel.
// If there is an active training when LastTrainingSubmittedAt is called, then LastTrainingSubmittedAt will return
// the time at which the active training was submitted to the kernel.
func (c *DistributedKernelClient) LastTrainingSubmittedAt() time.Time {
	return c.ExecutionManager.LastTrainingSubmittedAt()
}

// LastTrainingStartedAt returns the time at which the last training to occur began. If there is an active
// training when LastTrainingStartedAt is called, then LastTrainingStartedAt will return the time at which
// the active training began.
func (c *DistributedKernelClient) LastTrainingStartedAt() time.Time {
	return c.ExecutionManager.LastTrainingStartedAt()
}

// ReplicaContainersStartedAt returns the time at which the target Kernel's KernelContainer instances last started.
func (c *DistributedKernelClient) ReplicaContainersStartedAt() time.Time {
	return c.replicaContainersStartedAt
}

// ReplicaContainersAreBeingRemoved returns true if there is an active 'remove container(s)/replica(s)' operation
// for the target DistributedKernelClient.
func (c *DistributedKernelClient) ReplicaContainersAreBeingRemoved() (bool, scheduling.RemoveReplicaContainersAttempt) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.unsafeReplicaContainersAreBeingRemoved()
}

func (c *DistributedKernelClient) unsafeReplicaContainersAreBeingRemoved() (bool, scheduling.RemoveReplicaContainersAttempt) {
	areBeingRemoved := c.replicaContainersAreBeingRemoved.Load() > 0

	if !areBeingRemoved {
		return false, nil
	}

	return areBeingRemoved, c.removeReplicaContainersAttempt
}

// ReplicaContainersAreBeingScheduled returns true if there is an active 'create container(s)' operation
// for the target DistributedKernelClient.
func (c *DistributedKernelClient) ReplicaContainersAreBeingScheduled() bool {
	return c.replicaContainersAreBeingScheduled.Load() > 0
}

// RecordContainerPlacementStarted is called while scheduling the KernelContainer instances for the
// KernelReplica instances of the target Kernel.
//
// Specifically, RecordContainerPlacementStarted is called to signal that N viable host instances have
// been identified to serve the KernelContainer instances for the target Kernel, where N is the number of replicas
// of the target Kernel.
func (c *DistributedKernelClient) RecordContainerPlacementStarted() {
	if c.createReplicaContainersAttempt == nil {
		c.log.Error("Informed that placement of containers has began, but there is no active container creation operation...")
		return
	}

	c.createReplicaContainersAttempt.ContainerPlacementStarted()
}

// InitRemoveReplicaContainersOperation attempts to take ownership over the next/current removal attempt.
//
// If there's another active operation, then this will return false along with the RemoveReplicaContainersAttempt
// associated with the active/ongoing container removal operation.
//
// If the KernelContainer instances for the KernelReplica instances of this Kernel are already removed, then
// RemoveReplicaContainersAttempt will return false and nil.
func (c *DistributedKernelClient) InitRemoveReplicaContainersOperation() (bool, scheduling.RemoveReplicaContainersAttempt) {
	// TODO: What about concurrent scheduling/creation operation?

	// Attempt to take ownership over the next/current scheduling attempt.
	// If this CAS operation fails, then that means that there's another active container removal attempt.
	if !c.replicaContainersAreBeingRemoved.CompareAndSwap(0, 1) {
		return false, c.removeReplicaContainersAttempt
	}

	// Sanity check. This field should be nil if there was not an active scheduling operation.
	if c.removeReplicaContainersAttempt != nil {
		c.log.Error("Began attempt to remove %d replica container(s), but 'removeReplicaContainersAttempt' field is non-nil...",
			c.replicas.Len())

		panic("Found existing 'removeReplicaContainersAttempt' value upon beginning new container removal operation.")
	}

	if c.replicas.Len() == 0 {
		c.log.Warn("Began attempt to remove up to %d replica container(s), but replica(s) are already removed.",
			c.targetNumReplicas)

		concluded := c.concludeRemovingReplicaContainers()
		if !concluded {
			panic("Failed to conclude container removal operation immediately after initiating it...")
		}

		return false, nil
	}

	c.log.Debug("Beginning attempt to remove %d replica container(s).", c.replicas.Len())
	c.removeReplicaContainersAttempt = newRemoveReplicaContainersAttempt(c)
	return true, c.removeReplicaContainersAttempt
}

// InitSchedulingReplicaContainersOperation attempts to take ownership over the next/current scheduling attempt.
//
// If there's another active operation, then this will return false along with the CreateReplicaContainersAttempt
// associated with the active/ongoing container creation operation.
//
// If the KernelContainer instances for the KernelReplica instances of this Kernel are already scheduled, then
// InitSchedulingReplicaContainersOperation will return false and nil.
func (c *DistributedKernelClient) InitSchedulingReplicaContainersOperation() (bool, scheduling.CreateReplicaContainersAttempt) {
	// TODO: What about concurrent descheduling/removal operation?

	// Attempt to take ownership over the next/current scheduling attempt.
	// If this CAS operation fails, then that means that there's another active container creation attempt.
	if !c.replicaContainersAreBeingScheduled.CompareAndSwap(0, 1) {
		return false, c.createReplicaContainersAttempt
	}

	// Sanity check. This field should be nil if there was not an active scheduling operation.
	if c.createReplicaContainersAttempt != nil {
		c.log.Error("Began attempt to schedule %d replica container(s), but 'createReplicaContainersAttempt' field is non-nil...",
			c.targetNumReplicas)

		panic("Found existing, non-nil value for 'createReplicaContainersAttempt' field upon beginning new container creation operation.")
	}

	// Check if our replicas are already scheduled. If they are, then we'll return false and nil,
	// indicating that our replicas are scheduled, and that nobody needs to do anything as a result.
	if c.unsafeAreReplicasScheduled() {
		c.log.Debug("Began attempt to schedule %d replica container(s), but replica(s) are already scheduled.",
			c.targetNumReplicas)

		concluded := c.concludeSchedulingReplicaContainers()
		if !concluded {
			panic("Failed to conclude container creation operation immediately after initiating it...")
		}

		return false, nil
	}

	// If we're currently in the state of being idle-reclaimed, then let's attempt to change our status.
	if c.isIdleReclaimed.Load() {
		statusChanged := c.setStatus(jupyter.KernelStatusIdleReclaimed, jupyter.KernelStatusInitializing)
		if !statusChanged {
			c.log.Error("Attempted to change status from '%s' to '%s'; however, status change was rejected. Current status: '%s'.",
				jupyter.KernelStatusIdleReclaimed.String(), jupyter.KernelStatusInitializing.String(), c.status.String())
		}
	}

	c.log.Debug("Beginning attempt to schedule %d replica container(s).", c.targetNumReplicas)
	c.createReplicaContainersAttempt = newCreateReplicaContainersAttempt(c)
	return true, c.createReplicaContainersAttempt
}

// RecordContainerCreated records that a scheduling.KernelContainer was created for the target DistributedKernelClient.
//
// The argument to RecordContainerCreated indicates whether the created container was warm or cold.
func (c *DistributedKernelClient) RecordContainerCreated(warm bool) {
	c.numContainersCreated.Add(1)

	if warm {
		c.numWarmContainersUsed.Add(1)
	} else {
		c.numColdContainersUsed.Add(1)
	}
}

// NumContainersCreated returns the total number of KernelContainer instances that have been created or provisioned
// for the target Kernel over the target Kernel's entire lifetime. This includes the very first creation of any
// KernelContainer instance(s) as well as any KernelContainer instance(s) created during migrations or as on-demand.
//
// Technically, if a warm container is used, then that container wasn't strictly "created", but we count it in this
// statistic, in any case. To get the number of containers that were strictly created cold for the target
// DistributedKernelClient, simply compute NumContainersCreated - NumWarmContainersUsed.
func (c *DistributedKernelClient) NumContainersCreated() int32 {
	return c.numContainersCreated.Load()
}

// NumWarmContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
// for the target Kernel, the KernelReplica / KernelContainer was created using a warm KernelContainer.
func (c *DistributedKernelClient) NumWarmContainersUsed() int32 {
	return c.numWarmContainersUsed.Load()
}

// NumColdContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
// for the target Kernel, the KernelReplica / KernelContainer was created using a cold KernelContainer.
func (c *DistributedKernelClient) NumColdContainersUsed() int32 {
	return c.numColdContainersUsed.Load()
}

// concludeSchedulingReplicaContainers is called automatically by a CreateReplicaContainersAttempt struct
// when the container creation operation associated with the CreateReplicaContainersAttempt concludes.
//
// concludeSchedulingReplicaContainers should return true, meaning that the kernel's flag that indicates
// whether an active container-creation operation is occurring was successfully flipped from 1 --> 0.
//
// If concludeSchedulingReplicaContainers returns false, then the CreateReplicaContainersAttempt struct that is
// invoking the concludeSchedulingReplicaContainers method will ultimately end up panicking.
func (c *DistributedKernelClient) concludeSchedulingReplicaContainers() bool {
	if !c.replicaContainersAreBeingScheduled.CompareAndSwap(1, 0) {
		c.log.Error("Failed to flip value of 'CreatingReplicaContainers' flag from 1 to 0.")
		return false
	}

	if c.unsafeAreReplicasScheduled() {
		c.log.Debug("Successfully scheduled %d replica container(s).", c.targetNumReplicas)
		c.replicaContainersStartedAt = time.Now()
		c.isIdleReclaimed.Store(false)

		// If the status isn't already set to running, then we'll attempt to transition to that status.
		if c.status != jupyter.KernelStatusRunning {
			statusChanged := c.setStatus(jupyter.KernelStatusInitializing, jupyter.KernelStatusRunning)
			if !statusChanged {
				c.log.Warn("Attempted to change status from '%s' to '%s'; however, status change was rejected. Current status: '%s'.",
					jupyter.KernelStatusInitializing.String(), jupyter.KernelStatusInitializing.String(), c.status.String())
			}
		}
	} else {
		c.log.Warn("Attempt to schedule %d replica container(s) has failed.", c.targetNumReplicas)
	}

	c.createReplicaContainersAttempt = nil
	return true
}

// concludeRemovingReplicaContainers is called automatically by a RemoveReplicaContainersAttempt struct
// when the container removal operation associated with the RemoveReplicaContainersAttempt concludes.
//
// concludeRemovingReplicaContainers should return true, meaning that the kernel's flag that indicates
// whether an active container removal operation is occurring was successfully flipped from 1 --> 0.
//
// If concludeRemovingReplicaContainers returns false, then the RemoveReplicaContainersAttempt struct that is
// invoking the concludeSchedulingReplicaContainers method will ultimately end up panicking.
func (c *DistributedKernelClient) concludeRemovingReplicaContainers() bool {
	if !c.replicaContainersAreBeingRemoved.CompareAndSwap(1, 0) {
		c.log.Error("Failed to flip value of 'RemovingReplicaContainers' flag from 1 to 0.")
		return false
	}

	numReplicas := c.replicas.Len()
	if numReplicas == 0 {
		c.log.Debug("Removed (up to) %d replica container(s).", c.targetNumReplicas)
		c.replicaContainersStoppedAt = time.Now()
	} else {
		c.log.Warn("Attempt to remove (up to) %d replica container(s) has failed. Still have %d replica(s).",
			c.targetNumReplicas, numReplicas)
	}

	c.removeReplicaContainersAttempt = nil
	return true
}

// InitialContainerCreationFailed is called by the Cluster Gateway/Scheduler if the initial attempt to schedule
// the replica containers of the target DistributedKernelClient fails.
func (c *DistributedKernelClient) InitialContainerCreationFailed() {
	statusChanged := c.setStatus(jupyter.KernelStatusInitializing, jupyter.KernelStatusError)
	if !statusChanged {
		c.log.Warn("Attempted to change status from '%s' to '%s'; however, status change was rejected. Current status: '%s'.",
			jupyter.KernelStatusInitializing.String(), jupyter.KernelStatusError.String(), c.status.String())
	}
}

// SetSignatureScheme sets the SignatureScheme field of the ConnectionInfo of the server.AbstractServer underlying the
// DistributedKernelClient.
func (c *DistributedKernelClient) SetSignatureScheme(signatureScheme string) {
	//c.placementMu.Lock()
	//defer c.placementMu.Unlock()

	c.server.Meta.SignatureScheme = signatureScheme
}

// SetKernelKey sets the Key field of the ConnectionInfo of the server.AbstractServer underlying the DistributedKernelClient.
func (c *DistributedKernelClient) SetKernelKey(key string) {
	//c.placementMu.Lock()
	//defer c.placementMu.Unlock()

	c.server.Meta.Key = key
}

// TemporaryKernelReplicaClient returns the TemporaryKernelReplicaClient struct used by the DistributedKernelClient.
//
// TemporaryKernelReplicaClient structs are used in place of KernelReplicaClient structs when the replica container(s)
// of a given kernel is/are not scheduled, and that kernel receives a message.
func (c *DistributedKernelClient) TemporaryKernelReplicaClient() scheduling.KernelReplicaInfo {
	return c.temporaryKernelReplicaClient
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
	// c.replicasMutex.RLock()
	//defer c.replicasMutex.RUnlock()

	containers := make([]scheduling.KernelContainer, c.replicas.Len())
	//for _, replica := range c.replicas {
	//	containers = append(containers, replica.Container())
	//}

	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		containers = append(containers, replica.Container())
		return true
	})

	return containers
}

func (c *DistributedKernelClient) ShellListenPort() int {
	return c.server.GetSocketPort(messaging.ShellMessage)
}

func (c *DistributedKernelClient) IOPubListenPort() int {
	return c.server.GetSocketPort(messaging.IOMessage)
}

func (c *DistributedKernelClient) ExecutionFailedCallback() scheduling.ExecutionFailedCallback {
	return c.ExecutionManager.ExecutionFailedCallback()
}

func (c *DistributedKernelClient) GetExecutionManager() scheduling.ExecutionManager {
	return c.ExecutionManager
}

// RegisterActiveExecution registers a new execution.Execution or a new attempt of an existing execution,
// if there already exists an execution associated with the "msg_id" of the given messaging.JupyterMessage struct.
//
// RegisterActiveExecution returns a pointer to the new execution.Execution struct if one was created.
//
// If we are resubmitting an "execute_request" following a migration, then this will not create and return a new
// (pointer to a) execution.Execution struct, as the current active execution can simply be reused.
func (c *DistributedKernelClient) RegisterActiveExecution(msg *messaging.JupyterMessage) error {
	_, err := c.ExecutionManager.RegisterExecution(msg)
	if err != nil {
		c.log.Error("Error while registering new code execution \"%s\": %v",
			msg.JupyterMessageId(), err)
		return err
	}

	c.log.Debug("Registered new code execution \"%s\"", msg.JupyterMessageId())
	return nil
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

// updateResourceSpecOfReplicas updates the resource specs of the replicas of the target DistributedKernelClient.
func (c *DistributedKernelClient) updateResourceSpecOfReplicas(newSpec types.Spec) error {
	var coordinatedTransaction *transaction.CoordinatedTransaction
	if c.replicas.Len() > 1 {
		coordinatedTransaction = transaction.NewCoordinatedTransaction(c.replicas.Len(), c.id)
	}

	// Make sure that all the replicas -- however many there are -- have valid, non-nil containers.
	//for _, kernelReplica := range c.replicas {
	//	if kernelReplica.Container() == nil {
	//		c.log.Error("Replica %d of kernel %s is non-nil but lacks a valid container...",
	//			kernelReplica.ReplicaID(), c.id)
	//		return fmt.Errorf("replica %d of kernel %s is non-nil but lacks a valid container",
	//			kernelReplica.ReplicaID(), c.id)
	//	}
	//}

	var err error
	c.replicas.Range(func(i int32, kernelReplica scheduling.KernelReplica) bool {
		if kernelReplica.Container() == nil {
			c.log.Error("Replica %d of kernel %s is non-nil but lacks a valid container...",
				kernelReplica.ReplicaID(), c.id)
			err = fmt.Errorf("replica %d of kernel %s is non-nil but lacks a valid container",
				kernelReplica.ReplicaID(), c.id)
			return false
		}

		return true
	})

	if err != nil {
		return err
	}

	// If the coordinatedTransaction is non-nil, then there are multiple replicas.
	// Update their specs using goroutines.
	if coordinatedTransaction != nil {
		//for _, kernelReplica := range c.replicas {
		//	go func() {
		//		_ = kernelReplica.UpdateResourceSpec(newSpec, coordinatedTransaction)
		//	}()
		//}
		c.replicas.Range(func(_ int32, kernelReplica scheduling.KernelReplica) bool {
			go func() {
				_ = kernelReplica.UpdateResourceSpec(newSpec, coordinatedTransaction)
			}()

			return true
		})
	} else if c.replicas.Len() == 1 { // Is there at least one replica?
		// Just one replica. We'll update it ourselves.
		replica, _ := c.replicas.Load(int32(1))

		if replica != nil {
			return replica.UpdateResourceSpec(newSpec, nil)
		}

		c.log.Warn("Expected single replica to have ID=1...")
		c.replicas.Range(func(_ int32, kernelReplica scheduling.KernelReplica) bool {
			c.log.Warn("Updating spec of replica %d...", kernelReplica.ReplicaID())
			err = kernelReplica.UpdateResourceSpec(newSpec, nil)
			return false
		})

		return err
	}

	// If the coordinatedTransaction is non-nil, we'll return our result based on the success or failure of the tx.
	if coordinatedTransaction != nil {
		c.log.Debug("Waiting for coordinated TX %s to complete.", coordinatedTransaction.Id())
		success := coordinatedTransaction.Wait()
		if !success {
			return coordinatedTransaction.FailureReason()
		}
	}

	return nil
}

// UpdateResourceSpec updates the ResourceSpec of the kernel, all of its Replica instances, the UserSession
// of each Replica, and the KernelContainer of each Replica.
//
// It also ensures that the updated ResourceSpec is propagated to the host of each KernelContainer/Replica.
func (c *DistributedKernelClient) UpdateResourceSpec(newSpec types.CloneableSpec) error {
	c.log.Debug("Updating ResourceSpec of kernel \"%s\" from %v to %v. Number of replicas: %d.",
		c.id, c.spec.ResourceSpec.String(), newSpec.String(), c.replicas.Len())

	oldSpec := c.spec.DecimalSpecFromKernelSpec()

	if oldSpec.Equals(newSpec) {
		c.log.Debug("Old spec and new spec of kernel \"%s\" are equal. Nothing to update.", c.id)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.replicas.Len() > 0 {
		err := c.updateResourceSpecOfReplicas(newSpec)
		if err != nil {
			return err
		}
	}

	if c.session != nil {
		c.session.UpdateResourceSpec(newSpec)
	}

	c.spec.ResourceSpec.Cpu = int32(newSpec.CPU())
	c.spec.ResourceSpec.Memory = float32(newSpec.MemoryMB())
	c.spec.ResourceSpec.Gpu = int32(newSpec.GPU())
	c.spec.ResourceSpec.Vram = float32(newSpec.VRAM())

	c.log.Debug("Successfully updated ResourceSpec of kernel \"%s\" from %v to %v.",
		c.id, oldSpec.String(), newSpec.String())

	return nil
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
		c.log.Debug("Attempt to swap kernel status from %s to %s rejected. (Current status: %s.)",
			oldStatus.String(), newStatus.String(), c.status.String())
	}

	return
}

// Size returns the number of replicas in the kernel.
func (c *DistributedKernelClient) Size() int {
	return c.replicas.Len() // c.size
}

// NumActiveExecutionOperations returns the number of execution.Execution structs registered with
// the kernel. This counts both the current execution.Execution as well as the length of the queue of
// execution.Execution structs.
//
// This method is thread safe.
func (c *DistributedKernelClient) NumActiveExecutionOperations() int {
	return c.ExecutionManager.NumActiveExecutionOperations()
}

// NumActiveMigrationOperations returns the number of active migrations of the associated kernel's replicas.
func (c *DistributedKernelClient) NumActiveMigrationOperations() int {
	return int(c.numActiveAddOperations.Load())
}

func (c *DistributedKernelClient) AddOperationStarted() {
	c.numActiveAddOperations.Add(1)
}

func (c *DistributedKernelClient) AddOperationCompleted() {
	numActiveAddOperations := c.numActiveAddOperations.Add(-1)

	if numActiveAddOperations < 0 {
		panic("Number of active migration operations cannot fall below 0.")
	}
}

// Replicas returns the replicas in the kernel.
func (c *DistributedKernelClient) Replicas() []scheduling.KernelReplica {
	// c.replicasMutex.RLock()
	// Make a copy of references.
	ret := make([]scheduling.KernelReplica, 0, c.replicas.Len())
	//for _, replica := range c.replicas {
	//	ret = append(ret, replica)
	//}
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		ret = append(ret, replica)
		return true
	})
	//c.replicasMutex.RUnlock()

	// Ensure that the replicas are sorted from smallest to largest.
	slices.SortFunc(ret, func(a, b scheduling.KernelReplica) int {
		return int(a.ReplicaID() - b.ReplicaID())
	})

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

// PrepareNewReplica determines the replica ID for the new replica and returns the kernelReplicaSpec required to start the replica.
//
// Pass -1 for smrNodeId to automatically select the next node ID.
func (c *DistributedKernelClient) PrepareNewReplica(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec {
	c.mu.Lock()
	if smrNodeId == -1 {
		smrNodeId = c.nextNodeId.Add(1) - 1 // We want the value before we added 1.
	}
	c.mu.Unlock()

	spec := &proto.KernelReplicaSpec{
		Kernel:       c.spec,
		NumReplicas:  int32(c.replicas.Len()) + 1,
		Join:         true,
		PersistentId: &persistentId,
		ReplicaId:    smrNodeId,
	}

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

	//c.replicasMutex.Lock()
	c.replicas.Store(r.ReplicaID(), r)
	// c.replicas[r.ReplicaID()] = r
	//c.replicasMutex.Unlock()

	if statusChanged := c.setStatus(jupyter.KernelStatusInitializing, jupyter.KernelStatusRunning); statusChanged {
		// Update signature scheme and key.
		c.SetSignatureScheme(r.ConnectionInfo().SignatureScheme)
		c.SetKernelKey(r.ConnectionInfo().Key)

		c.log.Debug("Replica %d of kernel %s is available. kernel is ready. Assigned signature scheme \"%s\" and key \"%s\"",
			r.ReplicaID(), c.id, r.ConnectionInfo().SignatureScheme, r.ConnectionInfo().Key)

		// Collect the status of the replica(s).
		c.busyStatus.Collect(context.Background(), 1, c.replicas.Len(), messaging.MessageKernelStatusStarting, c.pubIOMessage)
	}

	return nil
}

// removeReplica is like RemoveReplica but removeReplica does not acquire or release any locks.
//
// Important: removeReplica must be called with mu and replicasMutex already locked.
func (c *DistributedKernelClient) removeReplica(r scheduling.KernelReplica, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.log.Debug("Removing replica %d from kernel \"%s\"", r.ReplicaID(), c.id)

	//c.replicasMutex.Lock()
	existingReplica, ok := c.replicas.LoadAndDelete(r.ReplicaID())
	if !ok || existingReplica != r {
		// This is bad and should never happen.
		c.log.Error("Replica stored under ID key %d has ID %d.", r.ReplicaID(), existingReplica.ID())
		return nil, scheduling.ErrReplicaNotFound
	}

	// Stop the replica FIRST -- before we call any other methods -- as we don't want to release the resources
	// until the replica has actually been stopped and the local daemon has released its resources.
	host, stopReplicaError := c.stopReplicaLocked(r, remover, noop) // We'll handle the stopReplicaError later.

	hostName := "N/A"
	hostId := "N/A"
	if host != nil {
		hostName = host.GetNodeName()
		hostId = host.GetID()
	}

	if stopReplicaError != nil {
		c.log.Error("Failed to stop replica %d on host %s (ID=%s) in container %s: %v",
			r.ReplicaID(), hostName, hostId, r.GetPodOrContainerName(), stopReplicaError)
	}

	var err error
	if r.IsTraining() {
		reason := fmt.Sprintf("Replica %d of kernel \"%s\" is being removed. Need to stop training before removal.",
			r.ReplicaID(), r.ID())
		err = r.KernelStoppedTraining(reason)
		if err != nil {
			c.log.Error("Error whilst stopping training on replica %d (during removal process) on host %s (ID=%s) in container %s: %v",
				r.ReplicaID(), hostName, hostId, r.GetPodOrContainerName(), err)
		}
	}

	container := r.Container()
	err = container.ContainerStopped()
	if err != nil {
		c.log.Error("Failed to cleanly stop container (ID=%s) for replica %d on host %s (ID=%s) because: %v",
			r.GetPodOrContainerName(), r.ReplicaID(), hostName, hostId, err)
	}

	if err = c.session.RemoveReplica(container); err != nil {
		c.log.Error("Failed to remove replica %d of kernel \"%s\" from associated UserSession: %v",
			container.ReplicaId(), c.id, err)
	}

	// If the error is either a ErrNilHost error or an ErrInvalidStateTransition error, then we probably didn't try
	// to call host::ContainerRemoved, as the ContainerStopped method would have returned before that point.
	//
	// So, we'll explicitly try calling host::ContainerRemoved now, but there's a good chance it'll fail (since there
	// was already some sort of issue when we tried to call ContainerStopped).
	if errors.As(err, &scheduling.ErrInvalidStateTransition) || errors.As(err, &scheduling.ErrNilHost) {
		host := r.Container().Host()

		c.log.Debug("Removing container for replica %d of kernel %s from host %s (ID=%s). Container spec: %v.",
			r.ReplicaID(), c.ID(), host.GetNodeName(), host.GetID(), r.Container().ResourceSpec())
		hostContainerRemovalError := host.ContainerRemoved(r.Container())
		if hostContainerRemovalError != nil {
			c.log.Error("Failed to remove scheduling.Container %s-%d from host %s because: %v",
				r.ID(), r.ReplicaID(), host.GetID(), hostContainerRemovalError)

			// If another error occurred, then we'll join the two together so that they get returned together.
			err = errors.Join(err, hostContainerRemovalError)
		}
	}

	r.Container().SetHost(nil) // Set the host to nil...

	err = r.Close()
	if err != nil {
		c.log.Error("Failed to cleanly close replica %d of kernel \"%s\": %v",
			r.ReplicaID(), c.id, err)

		if stopReplicaError != nil {
			err = errors.Join(stopReplicaError, err)
		}

		return host, err // Will include stopReplicaErr if stopReplicaErr is non-nil
	}

	return host, stopReplicaError // Will be nil on success
}

// RemoveReplica removes a kernel peer from the kernel. Returns the host that the scheduling.KernelReplica was
// running on.
func (c *DistributedKernelClient) RemoveReplica(r scheduling.KernelReplica, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.removeReplica(r, remover, noop)
}

func (c *DistributedKernelClient) GetReplicaByID(id int32) (scheduling.KernelReplica, error) {
	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()
	//replica, ok := c.replicas[id]
	replica, ok := c.replicas.Load(id)

	if replica == nil || !ok {
		validIds := make([]int32, 0, c.replicas.Len())
		//for validId := range c.replicas {
		//	validIds = append(validIds, validId)
		//}
		c.replicas.Range(func(_ int32, replica scheduling.KernelReplica) (contd bool) {
			validIds = append(validIds, replica.ReplicaID())
			return true
		})
		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, validIds)
		return nil, scheduling.ErrReplicaNotFound
	}

	return replica, nil
}

func (c *DistributedKernelClient) tellAllReplicasToPrepareToMigrate() error {
	c.log.Debug("Issuing 'Prepare to Migrate' requests to ensure replicas persist state to remote storage.")

	// We'll give the replicas up to 5 minutes to write their state to remote storage.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// *PrepareToMigrateResponse
	respChan := make(chan interface{}, c.targetNumReplicas)
	startTime := time.Now()

	responsesReceived := atomic.Int32{}

	// wrappedError wraps an error and a replica ID so that the error can be associated with a particular replica.
	type wrappedError struct {
		Error     error
		ReplicaId int32
	}

	// Send a PrepareToMigrate request to each replica in-parallel.
	// for _, replica := range replicas {
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		host := replica.Host()

		if host == nil {
			c.log.Error("Replica %d has a nil host...", replica.ReplicaID())
			return true
		}

		info := &proto.ReplicaInfo{
			ReplicaId: replica.ReplicaID(),
			KernelId:  replica.ID(),
		}

		// Send request.
		go func(replicaInfo *proto.ReplicaInfo) {
			// Send request.
			resp, err := host.PrepareToMigrate(ctx, replicaInfo)

			// Deliver response back to main goroutine.
			if err == nil {
				responsesReceived.Add(1)
				respChan <- resp
			} else {
				responsesReceived.Add(1)
				respChan <- &wrappedError{
					Error:     err,
					ReplicaId: replicaInfo.ReplicaId,
				}
			}
		}(info)

		return true
	})

	// Keep looping until we've received all responses.
	// We'll break out of the loop manually/explicitly if the context times-out or an error response is received.
	for responsesReceived.Load() < int32(c.replicas.Len()) {
		select {
		case <-ctx.Done():
			{
				timeElapsed := time.Since(startTime)
				finalNumResponsesReceived := responsesReceived.Load()

				c.log.Error("Timed out waiting for %d remaining response(s) to PrepareToMigrate request after %v.",
					finalNumResponsesReceived, timeElapsed)

				return fmt.Errorf("%w: failed to receive response to PrepareToMigrate request after %v (received %d/%d responses)",
					types.ErrRequestTimedOut, timeElapsed, finalNumResponsesReceived, c.replicas.Len())
			}
		case v := <-respChan:
			{
				switch v.(type) {
				case *wrappedError:
					{
						// Log an error message and return the error.
						err := v.(*wrappedError)
						c.log.Error("Replica %d failed to handle 'prepare to migrate' request during idle reclamation: %v",
							err.ReplicaId, err.Error)

						return err.Error
					}
				case *proto.PrepareToMigrateResponse:
					{
						// We'll just print a message indicating that we received the 'OK' response from this replica.
						// The for-loop will return once we've received responses from all replicas.
						c.log.Debug("Replica %d successfully prepared to migrate during idle reclamation. Received %d/%d responses so far. Time elapsed: %v.",
							v.(*proto.PrepareToMigrateResponse).Id, responsesReceived.Load(), c.replicas.Len(), time.Since(startTime))
					}
				}
			}
		}
	}

	return nil
}

// RemoveAllReplicas is used to remove all the replicas of the target DistributedKernelClient.
//
// As of right now, the RemoveAllReplicas method is just used by the Cluster Gateway in two scenarios:
//
// (i) idle reclamation
//
// (ii) removing the container(s) of a kernel when operating under a scheduling policy for which the container lifetime
// argument is set to 'single training event'.
func (c *DistributedKernelClient) RemoveAllReplicas(remover scheduling.ReplicaRemover, noop bool, forIdleReclamation bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.replicaContainersAreBeingRemoved.Load() != 1 {
		c.log.Warn("'Replicas Are Being Removed Flag' is not set to 1...")
	}

	c.log.Debug("Removing all %d replica(s) of kernel \"%s\".", c.Size(), c.id)

	// c.replicasMutex.RLock()
	if int32(c.replicas.Len()) < c.targetNumReplicas {
		if c.replicas.Len() == 0 {
			c.log.Warn("DistributedKernelClient::RemoveAllReplicas: already have 0/%d replica(s)...",
				c.targetNumReplicas)
			return nil
		}

		c.log.Warn("DistributedKernelClient::RemoveAllReplicas: only have %d/%d replica(s)",
			c.replicas.Len(), c.targetNumReplicas)
	}

	// If we're idle-reclaiming the containers of the kernel, then we need to issue 'Prepare to Migrate'
	// requests to prompt the containers to persist any important state to remote storage.
	if forIdleReclamation {
		err := c.tellAllReplicasToPrepareToMigrate()
		if err != nil {
			c.log.Error("Error while telling all replicas to prepare to migrate: %v", err)
			return err
		}

		c.log.Debug("Received all %d response(s) to 'Prepare to Migrate' requests. "+
			"Removing %d replica(s) now during idle reclamation.", c.replicas.Len(), c.replicas.Len())
	}

	numReplicas := c.Size()
	notifyChan := make(chan *shutdownNotification, numReplicas)
	startTime := time.Now()

	// In RLock, don't change anything in c.replicas.
	// for id, replica := range c.replicas {
	c.replicas.Range(func(id int32, replica scheduling.KernelReplica) (contd bool) {
		if replica == nil {
			c.log.Warn("Replica %d is nil", id)
			return true
		}

		go func(replica scheduling.KernelReplica) {
			// Get the current host of the replica so we can send it on the shutdownNotification
			// if the stopReplicaLocked call returns nil for the replica's host
			currHost := replica.Host()

			host, err := c.stopReplicaLocked(replica, remover, noop)
			if err != nil {
				c.log.Warn("Failed to stop %v on host %v: %v", replica, host, err)
			} else {
				c.log.Debug("Successfully stopped replica %v on host %s.", replica, host)
			}

			// Assign host to currHost so that we can assign a non-nil host in the shutdownNotification
			if host == nil {
				host = currHost
			}

			notifyChan <- &shutdownNotification{
				Error:         err,
				Host:          host,
				KernelReplica: replica,
				ReplicaId:     replica.ReplicaID(),
			}
		}(replica)

		return true
	})
	// c.replicasMutex.RUnlock()

	err := c.waitForReplicasToBeRemoved(numReplicas, startTime, notifyChan)
	if err != nil {
		return err
	}

	// If we're removing all replicas of the kernel for an idle reclamation, then we'll set the associated flag to
	// true, and we'll attempt to update the kernel's status to reflect that it has been idle reclaimed.
	if forIdleReclamation {
		c.isIdleReclaimed.Store(true)

		currentStatus := c.status
		statusChanged := c.setStatus(currentStatus, jupyter.KernelStatusIdleReclaimed)
		if !statusChanged {
			c.log.Warn("Attempted to change status from '%s' to '%s'; however, status change was rejected. Current status: '%s'.",
				currentStatus.String(), jupyter.KernelStatusError.String(), c.status.String())
		}
	}

	return nil
}

// waitForReplicasToBeRemoved waits for the specified number of replicas to be removed.
func (c *DistributedKernelClient) waitForReplicasToBeRemoved(numReplicas int, startTime time.Time, notifyChan chan *shutdownNotification) error {
	// Wait up to 6 minutes for the shutdown operation to complete.
	timeoutInterval := time.Minute * 6
	ctx, cancel := context.WithTimeout(context.Background(), timeoutInterval)
	defer cancel()

	// Wait until all replicas have stopped, or until the context times out.
	numReplicasShutdown := 0
	for numReplicasShutdown < numReplicas {
		select {
		case <-ctx.Done():
			{
				// We timed out. Boo.
				c.log.Error("Timed-out while attempting to stop %d kernel replica(s). Timed elapsed: %v.",
					numReplicas, time.Since(startTime))

				return fmt.Errorf("%w: replicas did not shutdown within timeout interval of %v",
					types.ErrRequestTimedOut, timeoutInterval)
			}
		case notification := <-notifyChan:
			{
				hostName := "N/A"
				hostId := "N/A"

				// We do this in case the Host field is nil for whatever reason.
				if notification.Host != nil {
					hostId = notification.Host.GetID()
					hostName = notification.Host.GetNodeName()
				}

				// If there was an error, then we'll log an error message and return the error.
				if notification.Error != nil {
					c.log.Error("Failed to shutdown replica %d on host %s (ID=%s). Time elapsed: %v.",
						notification.ReplicaId, hostName, hostId, time.Since(startTime))

					return notification.Error
				}

				// Success! Increment the counter and log a message.
				numReplicasShutdown += 1
				c.log.Debug(utils.LightGreenStyle.Render("Successfully terminated replica %d on host %s (ID=%s). "+
					"Shut down %d/%d replicas so far. Time elapsed: %v."),
					notification.ReplicaId, hostName, hostId, numReplicasShutdown, numReplicas, time.Since(startTime))
			}
		}
	}

	return nil
}

// RemoveReplicaByID removes a kernel peer from the kernel by replica ID. Returns the host that the
// scheduling.KernelReplica was running on.
func (c *DistributedKernelClient) RemoveReplicaByID(id int32, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
	c.mu.RLock()
	replica, ok := c.replicas.Load(id) // c.replicas[id]

	if replica == nil || !ok {
		validIds := make([]int32, 0, c.replicas.Len())
		//for validId := range c.replicas {
		//	validIds = append(validIds, validId)
		//}

		c.replicas.Range(func(_ int32, replica scheduling.KernelReplica) (contd bool) {
			validIds = append(validIds, replica.ReplicaID())
			return true
		})

		c.log.Warn("Could not retrieve kernel replica with id %d. Valid IDs are %v.", id, validIds)
		c.mu.RUnlock()
		return nil, scheduling.ErrReplicaNotFound
	}

	c.mu.RUnlock()
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
	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	//for _, replica := range c.replicas {
	//	if replica.IsReady() {
	//		return replica
	//	}
	//}

	var readyReplica scheduling.KernelReplica
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		if replica.IsReady() {
			readyReplica = replica
			return false
		}

		return true
	})

	return readyReplica
}

// IsReady returns true if ALL replicas associated with this distributed kernel client are ready.
func (c *DistributedKernelClient) IsReady() bool {
	// // c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	//for _, replica := range c.replicas {
	//	if !replica.IsReady() {
	//		return false
	//	}
	//}

	allReplicasAreReady := true
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		if !replica.IsReady() {
			allReplicasAreReady = false
			return false // stop iterating
		}

		return true // continue iterating
	})

	return allReplicasAreReady
}

// IsReplicaReady returns true if the replica with the specified ID is ready.
func (c *DistributedKernelClient) IsReplicaReady(replicaId int32) (bool, error) {
	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	replica, ok := c.replicas.Load(replicaId) // c.replicas[replicaId]
	if !ok {
		return false, scheduling.ErrReplicaNotFound
	}

	return replica.IsReady(), nil
}

func (c *DistributedKernelClient) DebugMode() bool {
	return c.debugMode
}

// RequestWithHandler sends a request to all replicas and handles the response.
func (c *DistributedKernelClient) RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType,
	jMsg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {

	kernelSize := c.Size()
	if kernelSize == 0 {
		c.log.Error("Cannot forward %s '%s' message \"%s\" to replica(s)... I have no replicas!",
			typ.String(), jMsg.JupyterMessageType(), jMsg.JupyterMessageId())

		return fmt.Errorf("%w: cannot forward %v \"%s\" message \"%s\"",
			ErrNoReplicas, typ.String(), jMsg.JupyterMessageType(), jMsg.JupyterMessageId())
	}

	// Broadcast to all replicas if no replicas are specified.
	replicas := make([]scheduling.KernelReplica, kernelSize)
	//for _, replica := range c.replicas {
	//	replicas = append(replicas, replica)
	//}
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		replicas = append(replicas, replica)
		return true
	})

	jMsg.AddDestFrameIfNecessary(c.id)

	// We create a slice of JupyterMessages here, with length equal to the number of replicas that we have.
	//
	// In debug mode, we clone the message for each replica. We do this because we'll be adding request traces
	// to the buffers of the message, and we want each replica to be able to add its own request trace.
	//
	// It would be bad if the replicas concurrently modified the same jupyter message, so we clone it and send
	// each replica its own unique copy.
	jupyterMessages := make([]*messaging.JupyterMessage, 0, kernelSize)
	for range replicas {
		var jupyterMessage *messaging.JupyterMessage
		if c.debugMode {
			// I believe we clone the message so we can embed our own independent data in the metadata/buffers frames,
			// like the request trace for this specific replica. We don't want to be writing to the buffers frames of the
			// same JupyterMessage as our peer replicas, so we clone it first.
			jupyterMessage = jMsg.Clone()
		} else {
			// Non-debug mode, so we just include a pointer to the same jupyter message 3 times in the slice.
			jupyterMessage = jMsg
		}

		jupyterMessages = append(jupyterMessages, jupyterMessage)
	}

	return c.RequestWithHandlerAndReplicas(ctx, "Forwarding", typ, jupyterMessages, handler, done, replicas...)
}

// LastTrainingEndedAt returns the time at which the last training ended.
//
// If the kernel is currently training, then TrainingEndedAt returns the time at which the previous training ended.
func (c *DistributedKernelClient) LastTrainingEndedAt() time.Time {
	return c.ExecutionManager.LastTrainingEndedAt()
}

// ActiveTrainingStartedAt returns the time at which one of the target DistributedKernelClient's replicas
// began actively training, if there is an actively-training replica.
func (c *DistributedKernelClient) ActiveTrainingStartedAt() time.Time {
	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	//for _, replica := range c.replicas {
	//	if replica.IsTraining() {
	//		return replica.LastTrainingStartedAt()
	//	}
	//}

	var lastTrainingStartedAt time.Time
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		if replica.IsTraining() {
			lastTrainingStartedAt = replica.LastTrainingStartedAt()
			return false
		}

		return true
	})

	return lastTrainingStartedAt
}

// NumCompletedTrainings returns the number of training events that have been completed successfully.
func (c *DistributedKernelClient) NumCompletedTrainings() int {
	return c.ExecutionManager.NumCompletedTrainings()
}

// IsTraining returns true if one of the target DistributedKernelClient's replicas is actively training.
func (c *DistributedKernelClient) IsTraining() bool {
	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	//for _, replica := range c.replicas {
	//	if replica.IsTraining() {
	//		return true
	//	}
	//}

	var isTraining bool
	c.replicas.Range(func(_ int32, replica scheduling.KernelReplica) (contd bool) {
		if replica.IsTraining() {
			isTraining = true
			return false // stop iterating
		}

		return true // keep iterating
	})

	return isTraining // false
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
//
// If we're in debug mode, then the given messaging.JupyterMessage will be a unique clone/copy of the original
// messaging.JupyterMessage, so that each replica can operate/use its own unique messaging.JupyterMessage, and thus
// there won't be issues with concurrently modifying the messaging.JupyterMessage instances, or with the fact that
// each replica will want to add its own unique request trace to the messaging.JupyterMessage.
func (c *DistributedKernelClient) sendRequestToReplica(ctx context.Context, targetReplica scheduling.KernelReplica,
	jMsg *messaging.JupyterMessage, typ messaging.MessageType, responseReceivedSem *semaphore.Weighted,
	numResponsesSoFar *atomic.Int32, numResponsesExpected int, respHandler scheduling.KernelReplicaMessageHandler) error {

	c.log.Debug("Sending %v '%s' message '%s' to replica %d of kernel '%s' on host %s (ID=%s) now.",
		typ.String(), jMsg.JupyterMessageType(), jMsg.JupyterMessageId(), targetReplica.ReplicaID(),
		c.id, targetReplica.Host().GetNodeName(), targetReplica.Host().GetID())

	doneFunc := func() {
		if responseReceivedSem != nil {
			// responseReceivedSem.SetDone()
			responseReceivedSem.Release(1)
			c.log.Debug("Called Release(1) on semaphore for %s '%s' message '%s'.",
				typ.String(), jMsg.JupyterMessageType(), jMsg.JupyterMessageId())
		}

		if numResponsesSoFar != nil {
			numResp := numResponsesSoFar.Add(1)
			c.log.Debug("Received %d/%d response(s) to %s '%s' message '%s' request so far.",
				numResp, numResponsesExpected, typ.String(), jMsg.JupyterMessageType(), jMsg.JupyterMessageId())
		}
	}

	// TODO: If the ACKs fail on this and we reconnect and retry, the responseReceivedSem may be called too many times.
	// Need to fix this. Either make the timeout bigger, or... do something else. Maybe we don't need the pending request
	// to be cleared after the context ends; we just do it on ACK timeout.
	err := targetReplica.RequestWithHandlerAndWaitOptionGetter(ctx, typ, jMsg, respHandler, c.getWaitResponseOption, doneFunc)

	if err != nil {
		c.log.Error("Error while issuing %s '%s' request to targetReplica %s: %v",
			typ, jMsg.JupyterMessageType(), c.id, err)
		return err
	}

	return nil
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

	err = resp.JupyterFrames.EncodeContent(&errorMessage)
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
// RequestWithHandlerAndWaitOptionGetter method as the handler parameter.
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
		c.log.Debug(utils.CyanStyle.Render("Received %s \"%s\" response with Jupyter message ID \"%s\" from kernel %v"),
			socketType.String(), response.JupyterMessageType(), response.JupyterMessageId(), replica)
		response.ReplicaId = replica.ReplicaID()

		if socketType == messaging.ShellMessage {
			// "Preprocess" the response, which involves checking if it is a YIELD notification,
			// and handling a situation in which ALL replicas have proposed 'YIELD'.
			var (
				yielded bool
				err     error
			)
			if response.JupyterMessageType() == messaging.ShellExecuteReply {
				yielded, err = c.ExecutionManager.HandleExecuteReplyMessage(response, replica.(scheduling.KernelReplica))
			}

			// If we yielded, then the expectation is that the shellPreprocessingError will be a
			// messaging.ErrExecutionYielded. This is fine. But if the shellPreprocessingError is not a
			// messaging.ErrExecutionYielded (and is instead some other error), then that's problematic.
			//
			// For example, maybe all the replicas yielded, and then the handler for that was called, but that
			// handler encountered an error. In that case, we'll need to propagate the error back to the client.
			// So, we can't return nil here (in that scenario).
			if yielded && errors.Is(err, messaging.ErrExecutionYielded) {
				c.log.Debug("Discarding YIELD (%v \"%s\" response) with JupyterID=\"%s\" from kernel replica %v",
					socketType, response.JupyterMessageType(), response.JupyterMessageId(), replica)

				// Standard procedure is to return nil for a 'YIELD' notification.
				// We only do things differently if we receive 3 'YIELD' notifications (for the same 'execute_request'
				// message); that is, if all replicas of the kernel yield the execution, then we handle things a bit
				// differently (by invoking the 'failure handler').
				return nil
			}

			// If we get an error at this point, then we need this error to propagate back to the Jupyter client.
			// We may have just handled the third 'yield' notification, and encountered an error while running the
			// 'failure handler'. Or there may have just been some other error. Either way, we need to send an error
			// back to the Jupyter client at this point.
			if err != nil {
				c.log.Warn("Error while pre-processing shell response for Jupyter \"%s\" message \"%s\" (JupyterID=\"%s\") targeting kernel \"%s\": %v",
					response.JupyterMessageType(), response.RequestId, response.JupyterParentMessageId(), c.id, err)

				replacementError := c.replaceMessageContentWithError(response, "Internal Processing Error", err.Error())
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
//
// Note: the handler will be wrapped in the DistributedKernelClient's generic response "forwarder function".
// This "response forwarder" function contains additional logic, such as for handling "execute_reply" and "yield"
// messages/notifications from kernel replicas.
func (c *DistributedKernelClient) RequestWithHandlerAndReplicas(ctx context.Context, _ string, typ messaging.MessageType,
	jupyterMessages []*messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func(),
	replicas ...scheduling.KernelReplica) error {

	startTime := time.Now()
	if len(replicas) == 0 {
		replicas = c.Replicas()
	}

	once := sync.Once{}
	replicaCtx, cancel := c.getRequestContext(ctx, typ)
	responseHandler := c.getResponseForwarder(handler, cancel, &once)

	// Send the request to all replicas.
	statusCtx, statusCancel := context.WithTimeout(context.Background(), messaging.DefaultRequestTimeout)
	defer statusCancel()
	c.busyStatus.Collect(statusCtx, c.replicas.Len(), c.replicas.Len(), messaging.MessageKernelStatusBusy, c.pubIOMessage)

	// Just pass the first message. Doesn't matter if it is "execute_request" or "yield_request".
	if messaging.IsExecuteOrYieldRequest(jupyterMessages[0]) {
		// Inform our ExecutionManager that we are sending an "execute_request" (or "yield_request") message.
		err := c.ExecutionManager.SendingExecuteRequest(jupyterMessages[0])
		if err != nil {
			c.log.Error("ExecutionManager reported error for \"%s\" message \"%s\": %v",
				jupyterMessages[0].JupyterMessageType(), jupyterMessages[0].JupyterMessageId(), err)

			return err
		}
	}

	// If there's just a single replica, then send the message to that one replica.
	if len(replicas) == 1 {
		return c.sendRequestToReplica(replicaCtx, replicas[0], jupyterMessages[0], typ,
			nil, nil, 1, responseHandler)
	}

	// Note: we do NOT need to create a barrier where the replicas all wait until they've each clone the
	// message, as we don't use the original message (in the case that the replicas send cloned versions).
	// Thus, the original message is never going to be changed, so it's safe for each replica to proceed
	// as soon as it clones the original message.
	//var responseReceivedWg sync.primarySemaphore
	numResponsesExpected := 0
	numResponsesSoFar := atomic.Int32{}

	respReceivedSemaphore := semaphore.NewWeighted(int64(len(replicas)))

	errorChannel := make(chan interface{}, len(replicas))

	// Iterate over each replica and forward the request to that kernel replica's Local Daemon,
	// which will then route the request to the kernel replica itself.
	for idx, replica := range replicas {
		if replica == nil {
			continue
		}

		//responseReceivedWg.Add(1)
		_ = respReceivedSemaphore.Acquire(ctx, 1)

		numResponsesExpected += 1

		msg := jupyterMessages[idx]

		go func() {
			err := c.sendRequestToReplica(replicaCtx, replica, msg, typ, respReceivedSemaphore, &numResponsesSoFar,
				numResponsesExpected, responseHandler)

			if err != nil {
				errorChannel <- err
			} else {
				errorChannel <- struct{}{}
			}
		}()
	}

	if done != nil {
		// If a `done` callback function was given to us, then we'll create a goroutine to handle it.
		go c.handleDoneCallbackForRequest(done, respReceivedSemaphore, typ, jupyterMessages[0], &numResponsesSoFar, numResponsesExpected)
	}

	// We'll wait at most 1 second to see if any of these send attempts immediately return an error.
	// If they do, then we'll receive the error and return it.
	waitContext, waitCancel := context.WithTimeout(context.Background(), time.Millisecond*250)
	defer waitCancel()

	numNotifications := 0
	select {
	case errOrOk := <-errorChannel:
		{
			// We got a value. It's either an error or a struct{}.
			// If it's an error, then we'll just return it. We may have received other errors
			// from the other replicas -- whatever, we'll just return the first error.
			//
			// Alternatively, if it's a struct{}, then that means the send went fine for whatever
			// replica, and we can just increment the numNotifications counter and possibly return.
			switch errOrOk.(type) {
			case error:
				{
					// It's an error. Whoops.
					// Return the error.
					return errOrOk.(error)
				}
			default:
				{
					// The send went fine. Just increment the counter.
					numNotifications += 1

					// Can we return yet? If we got replies from all replicas, we can.
					if numNotifications >= len(replicas) {
						c.log.Debug("RequestWithHandlerAndReplicas returning after %v. Received responses from all %d replicas.",
							time.Since(startTime), len(replicas))
						return nil
					}
				}
			}
		}
	case <-waitContext.Done():
		{
			// Timed-out. That's fine. Just return.
			// We used to not return anything ever.
			c.log.Debug("RequestWithHandlerAndReplicas returning after %v. Timed-out (not a bad thing).",
				time.Since(startTime), len(replicas))

			return nil
		}
	}

	return nil
}

// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
// code execution, or nil if no code executions have occurred.
func (c *DistributedKernelClient) LastPrimaryReplica() scheduling.KernelReplica {
	return c.ExecutionManager.LastPrimaryReplica()
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
func (c *DistributedKernelClient) handleDoneCallbackForRequest(done func(), responseReceivedSem *semaphore.Weighted,
	typ messaging.MessageType, jMsg *messaging.JupyterMessage, numResponsesSoFar *atomic.Int32, numResponsesExpected int) {

	st := time.Now()
	allResponsesReceivedNotificationChannel := make(chan interface{}, 1)

	var ctx context.Context
	var cancel context.CancelFunc
	if typ == messaging.ShellMessage {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*300)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
	}
	defer cancel()

	// Spawn a goroutine to just send a notification when the sync.primarySemaphore reaches 0.
	go func() {
		// responseReceivedSem.Wait()                            // Wait til the primarySemaphore reaches 0.
		err := responseReceivedSem.Acquire(ctx, int64(numResponsesExpected))
		if err == nil {
			allResponsesReceivedNotificationChannel <- struct{}{} // Send the notification.
		} else {
			cancel()
		}
	}()

	select {
	case <-allResponsesReceivedNotificationChannel:
		{
			// The primarySemaphore counter reached 0. Call done, and then return.
			c.log.Debug("Received all %d replies for %s \"%s\" message %s (JupyterID=\"%s\"). "+
				"Calling 'done' callback now. Time elapsed: %v.", numResponsesExpected, typ.String(),
				jMsg.JupyterParentMessageType(), jMsg.RequestId, jMsg.JupyterParentMessageId(), time.Since(st))

			// Call the 'done' callback.
			done()

			// Cancel the context before we return.
			return
		}
	case <-ctx.Done():
		{
			c.log.Warn("Giving up. Have been waiting for a total of %v for all responses to %s \"%s\" request %s (JupyterID=\"%s\"). Received %d/%d responses so far.",
				time.Since(st), typ.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), numResponsesSoFar.Load(), numResponsesExpected)
		}
	}
}

// IsShuttingDown returns true if the target DistributedKernelClient is in the process of shutting down.
func (c *DistributedKernelClient) IsShuttingDown() bool {
	return c.shuttingDown.Load() > 0
}

// Shutdown releases all replicas and closes the session.
func (c *DistributedKernelClient) Shutdown(remover scheduling.ReplicaRemover, restart bool) error {
	c.log.Debug("Shutting down kernel %s.", c.id)
	// c.replicasMutex.RLock()

	if c.status == jupyter.KernelStatusExited {
		// c.replicasMutex.RUnlock()
		c.log.Warn("kernel %s has already exited; there's no need to shutdown.", c.id)
		return nil
	}

	if !c.shuttingDown.CompareAndSwap(0, 1) {
		c.log.Warn("Already shutting down.")
		return ErrAlreadyShuttingDown
	}

	defer func() {
		if !c.shuttingDown.CompareAndSwap(1, 0) {
			c.log.Warn("Failed to swap 'shutting down' flag back to 0...")
		}
	}()

	// We may have already removed the replicas of the kernel.
	if c.replicas.Len() > 0 {
		if !c.replicaContainersAreBeingRemoved.CompareAndSwap(0, 1) {
			c.log.Warn("'Replicas are Being Removed' flag is already set to 1...")
		} else {
			defer c.replicaContainersAreBeingRemoved.CompareAndSwap(1, 0)
		}

		err := c.RemoveAllReplicas(remover, false, false)
		if err != nil {
			c.log.Error("Failed to remove all replicas: %v", err)
			return err
		}
	}

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
	c.log.Debug("Stopping replica %d of kernel %s in container %s.",
		r.ReplicaID(), r.ID(), r.GetPodOrContainerName())

	host := r.Host() // r.Context().Value(CtxKernelHost).(scheduling.Host)
	if err := remover(host, c.session, noop); err != nil {
		return host, err
	}

	if c.LastPrimaryReplica() == r {
		// Notify the ExecutionManager that we removed the replica.
		// This is so, if the replica we removed is the last primary replica, that we can set
		// the last primary replica to nil.
		c.ExecutionManager.ReplicaRemoved(r)
	}

	return host, nil
}

func (c *DistributedKernelClient) clearReplicasLocked() {
	c.log.Debug("Clearing replicas now.")
	toRemove := make([]int32, c.replicas.Len())
	// for id, kernel := range c.replicas {
	c.replicas.Range(func(_ int32, replica scheduling.KernelReplica) (contd bool) {
		c.log.Debug("Closing replica %d now.", replica.ReplicaID())

		err := replica.Close()
		if err != nil {
			// TODO(Ben): Handle this more cleanly.
			c.log.Error("Error while trying to close replica %d in container %s: %v",
				replica.ReplicaID(), replica.GetPodOrContainerName(), err)
		} else {
			c.log.Debug("Successfully closed replica %d.", replica.ReplicaID())
		}

		toRemove = append(toRemove, replica.ReplicaID())

		return true
	})

	for _, id := range toRemove {
		// delete(c.replicas, id)
		c.replicas.Delete(id)
	}

	c.status = jupyter.KernelStatusExited
}

func (c *DistributedKernelClient) closeLocked() error {
	if !atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		<-c.cleaned
		return nil
	}

	err := c.BaseServer.Close()
	if err != nil {
		c.log.Warn("Error while closing BaseServer for kernel \"%s\": %v", c.id, err)
	}

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
	stopped.Add(c.replicas.Len())

	// for _, replica := range c.replicas {
	c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
		go func(kernelReplica scheduling.KernelReplica) {
			defer stopped.Done()

			host := kernelReplica.Host() // kernelReplica.Context().Value(CtxKernelHost).(scheduling.Host)
			if _, err := host.WaitKernel(context.Background(), &proto.KernelId{Id: kernelReplica.ID()}); err != nil {
				c.log.Error("Error while waiting on replica %d of kernel %s to stop: %v",
					kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			}
		}(replica)

		return true
	})

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

// ReplicasAreScheduled returns a flag indicating whether the replicas of this kernel are scheduled.
// Under certain scheduling policies, we only schedule a Container when an "execute_request" arrives.
func (c *DistributedKernelClient) ReplicasAreScheduled() bool {
	// If we're in the middle of a removal, then just return false.
	if c.replicaContainersAreBeingRemoved.Load() == 1 || c.removeReplicaContainersAttempt != nil {
		return false
	}

	// c.replicasMutex.RLock()
	// defer c.replicasMutex.RUnlock()

	return c.unsafeAreReplicasScheduled()
}

// unsafeAreReplicasScheduled returns a flag indicating whether the replicas of this kernel are scheduled.
// Under certain scheduling policies, we only schedule a Container when an "execute_request" arrives.
func (c *DistributedKernelClient) unsafeAreReplicasScheduled() bool {
	return int32(c.replicas.Len()) == c.targetNumReplicas
}

func (c *DistributedKernelClient) handleMsg(replica messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	switch typ {
	case messaging.IOMessage:
		topic, jFrames := ExtractIOTopicFrame(msg)
		switch topic {
		case messaging.IOTopicStatus:
			{
				return c.handleIOKernelStatus(replica.(scheduling.KernelReplica), jFrames, msg)
			}
		case messaging.MessageTypeSMRLeadTask:
			{
				err := c.ExecutionManager.HandleSmrLeadTaskMessage(msg, replica.(scheduling.KernelReplica))
				if err != nil {
					c.log.Error("Error while handling \"%s\" message from replica %d of kernel \"%s\": %v",
						messaging.MessageTypeSMRLeadTask, replica.(scheduling.KernelReplica).ReplicaID(), c.id, err)
					return err
				}

				c.log.Debug("Forwarding \"%s\" message to Jupyter Client of kernel \"%s\"",
					messaging.MessageTypeSMRLeadTask, c.id)

				// Forward the "lead task" message to clients.
				err = c.server.Sockets.IO.Send(*msg.GetZmqMsg())
				if err != nil {
					c.log.Error("Failed to forward \"%s\" message to Jupyter Client of kernel \"%s\": %v",
						messaging.MessageTypeSMRLeadTask, c.id, err)
					return err
				}

				c.log.Debug("Forwarded \"%s\" message to Jupyter Client of kernel \"%s\"",
					messaging.MessageTypeSMRLeadTask, c.id)
				return nil
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

func (c *DistributedKernelClient) handleIOKernelStatus(replica scheduling.KernelReplica, frames *messaging.JupyterFrames, msg *messaging.JupyterMessage) error {
	err := replica.HandleIOKernelStatus(replica, frames, msg)
	if err != nil {
		return err
	}

	status, msg := replica.BusyStatus()
	// c.log.Debug("reducing replica %d status %s to match %s", replica.ReplicaID(), status, c.busyStatus.expectingStatus)
	c.busyStatus.Reduce(replica.ReplicaID(), status, msg, c.pubIOMessage)
	return nil
}

// ReleasePreCommitedResourcesFromReplica is called to release resources that were pre-commited to a
// scheduling.KernelReplica before an "execute_request" was forwarded, in order to ensure that the
// replica would have the resources available when it received the message.
func (c *DistributedKernelClient) ReleasePreCommitedResourcesFromReplica(replica scheduling.KernelReplica, msg *messaging.JupyterMessage) error {
	container := replica.Container()
	if container == nil {
		c.log.Warn("Container for non-nil replica %d of kernel \"%s\" is nil while processing \"execute_reply\" \"%s\"...",
			replica.ReplicaID(), c.id, msg.JupyterMessageId())
		return nil
	}

	host := container.Host()
	if host == nil {
		c.log.Warn("host of container for non-nil replica %d of kernel \"%s\" is nil while processing \"execute_reply\" \"%s\"...",
			replica.ReplicaID(), c.id, msg.JupyterMessageId())
		return nil
	}

	err := host.ReleasePreCommitedResources(container, msg.JupyterParentMessageId())
	if err != nil { // Not necessarily a bad thing.
		c.log.Debug("Failed to release pre-committed resources from replica %d of kernel \"%s\" because: %s",
			container.ReplicaId(), container.KernelID(), err.Error())

		if errors.Is(err, entity.ErrInvalidContainer) { // Container just didn't have resources pre-allocated to it.
			return nil
		}

		// TODO: I just added this; we may not want to do this? Like, we may just want to return nil.
		//	     But then why bother checking if the error was an entity.ErrInvalidContainer error?
		return err
	}

	return nil
}

func (c *DistributedKernelClient) SendIOMessage(msg *messaging.JupyterMessage) error {
	zmqMsg := msg.GetZmqMsg()
	return c.server.Sockets.IO.Send(*zmqMsg)
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
		c.busyStatus.Collect(context.Background(), c.replicas.Len(), c.replicas.Len(), messaging.MessageKernelStatusIdle, c.pubIOMessage)
		// Fill matched status that has been received before collecting.
		// for _, replica := range c.replicas {
		c.replicas.Range(func(i int32, replica scheduling.KernelReplica) (contd bool) {
			status, msg := replica.BusyStatus()
			if status == messaging.MessageKernelStatusIdle {
				c.busyStatus.Reduce(replica.ReplicaID(), messaging.MessageKernelStatusIdle, msg, c.pubIOMessage)
			}

			return true
		})
	}

	return err
}

// HasActiveTraining returns true if the target DistributedKernelClient has an active training -- meaning that the
// DistributedKernelClient has submitted an "execute_request" and is still awaiting a response.
//
// Having an active training prevents a DistributedKernelClient from being idle-reclaimed.
func (c *DistributedKernelClient) HasActiveTraining() bool {
	return c.ExecutionManager.HasActiveTraining()
}
