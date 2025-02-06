package scheduling

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

type MessageBrokerHandler[S any, T any, R any] func(source S, msg T, raw R) error

// KernelMessageHandler is an API defines the interface of messages that a JupyterRouter can intercept and handle.
type KernelMessageHandler func(KernelInfo, messaging.MessageType, *messaging.JupyterMessage) error

// ReplicaRemover is a function that removes a replica from a kernel.
// If noop is specified, it is the caller's responsibility to stop the replica.
type ReplicaRemover func(host Host, session UserSession, noop bool) error

type KernelReplicaMessageHandler func(KernelReplicaInfo, messaging.MessageType, *messaging.JupyterMessage) error

type KernelInfo interface {
	// Info provides kernel specific routing information.
	router.Info

	// ID returns kernel ID.
	ID() string

	// ResourceSpec returns resource resourceSpec, which defines the resource requirements of the kernel.
	ResourceSpec() *types.DecimalSpec

	// KernelSpec returns kernel resourceSpec.
	KernelSpec() *proto.KernelSpec
}

type KernelReplicaInfo interface {
	KernelInfo

	ReplicaID() int32
}

type SessionManager interface {
	Sessions() []string        // Session returns the associated session ID.
	BindSession(sess string)   // BindSession binds a session ID to the client.
	UnbindSession(sess string) // UnbindSession unbinds a session ID from the client.
	ClearSessions()            // ClearSessions clears all sessions.
}

// ExecutionLatencyCallback is provided by the internalCluster Gateway to each DistributedKernelClient.
// When a DistributedKernelClient receives a notification that a kernel has started execution user-submitted code,
// the DistributedKernelClient will check if its Execution struct has the original "sent-at" timestamp
// of the original "execute_request". If it does, then it can calculate the latency between submission and when
// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
// so that a relevant Prometheus metric can be updated.
type ExecutionLatencyCallback func(latency time.Duration, workloadId string, kernelId string)

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c Kernel, executeRequestMsg *messaging.JupyterMessage) error

type NotificationCallback func(title string, content string, notificationType messaging.NotificationType)

// CreateReplicaContainersAttempt is similar to kernelDescheduleAttempt, but CreateReplicaContainersAttempt is used
// to keep track of a kernel whose kernel replicas and kernel containers are being created, rather than removed.
type CreateReplicaContainersAttempt interface {
	// Kernel returns the scheduling.Kernel associated with the target CreateReplicaContainersAttempt (i.e., the
	// scheduling.Kernel whose scheduling.KernelContainer instances are being created).
	Kernel() Kernel

	// Wait blocks until the target CreateReplicaContainersAttempt is finished, or until the given
	// context.Context is cancelled.
	//
	// If the operation completes in a failed state and there's a failure reason,
	// then the failure reason will be returned.
	Wait(ctx context.Context) error

	// WaitForPlacementPhaseToBegin blocks until the placement phase begins.
	WaitForPlacementPhaseToBegin(ctx context.Context) error

	// IsComplete returns true if the target CreateReplicaContainersAttempt has finished.
	//
	// Note that if IsComplete is true, that doesn't necessarily mean that the associated container creation operation
	// finished successfully. It may have encountered errors or timed-out on its own.
	IsComplete() bool

	// SetDone records that the target CreateReplicaContainersAttempt has finished.
	//
	// If the operation failed, then the reason, in the form of an error, should be passed to SetDone.
	//
	// If the target CreateReplicaContainersAttempt has already been marked as having completed,
	// then SetDone will panic.
	SetDone(failureReason error)

	// ContainerPlacementStarted records that the placement of the associated Kernel's KernelContainer instances has
	// officially started.
	ContainerPlacementStarted()

	// PlacementInProgress returns true if the process of placing and creating the scheduling.KernelContainer
	// instances has started. Generally, if this stage is reached, then the operation will most-likely complete
	// successfully, as errors are unlikely, and it means that resources were available and whatnot.
	PlacementInProgress() bool

	// Succeeded returns true if the container creation operation(s) succeeded.
	Succeeded() bool

	// FailureReason returns a non-nil value if there is an error associated with the failure of the
	// container creation operation(s) succeeded.
	FailureReason() error

	// KernelId returns the kernel ID of the scheduling.Kernel associated with the target CreateReplicaContainersAttempt.
	KernelId() string

	// StartedAt returns the time at which the target CreateReplicaContainersAttempt began.
	StartedAt() time.Time

	// TimeElapsed returns the amount of time that has elapsed since the target CreateReplicaContainersAttempt began.
	TimeElapsed() time.Duration
}

type Kernel interface {
	types.Contextable
	SessionManager
	Server

	SetSession(session UserSession)
	GetSession() UserSession
	GetContainers() []KernelContainer
	ShellListenPort() int
	IOPubListenPort() int
	GetExecutionManager() ExecutionManager
	ReleasePreCommitedResourcesFromReplica(replica KernelReplica, msg *messaging.JupyterMessage) error
	ExecutionFailedCallback() ExecutionFailedCallback

	// ExecutionComplete(msg *messaging.JupyterMessage) error

	RegisterActiveExecution(msg *messaging.JupyterMessage) error
	ResetID(id string)
	PersistentID() string
	String() string
	ID() string
	SourceKernelID() string
	ResourceSpec() *types.DecimalSpec

	// NumContainersCreated returns the total number of KernelContainer instances that have been created or provisioned
	// for the target Kernel over the target Kernel's entire lifetime. This includes the very first creation of any
	// KernelContainer instance(s) as well as any KernelContainer instance(s) created during migrations or as on-demand.
	//
	// Technically, if a warm container is used, then that container wasn't strictly "created", but we count it in this
	// statistic, in any case. To get the number of containers that were strictly created cold for the target Kernel,
	// simply compute NumContainersCreated - NumWarmContainersUsed.
	NumContainersCreated() int32

	// NumWarmContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
	// for the target Kernel, the KernelReplica / KernelContainer was created using a warm KernelContainer.
	NumWarmContainersUsed() int32

	// NumColdContainersUsed returns the number of times that, when a KernelReplica / KernelContainer had to be created
	// for the target Kernel, the KernelReplica / KernelContainer was created using a cold KernelContainer.
	NumColdContainersUsed() int32

	// RecordContainerCreated records that a scheduling.KernelContainer was created for the target DistributedKernelClient.
	//
	// The argument to RecordContainerCreated indicates whether the created container was warm or cold.
	RecordContainerCreated(warm bool)

	// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
	// code execution, or nil if no code executions have occurred.
	LastPrimaryReplica() KernelReplica

	// UpdateResourceSpec updates the ResourceSpec of the Kernel, all of its KernelReplica instances, the UserSession
	// of each KernelReplica, and the KernelContainer of each KernelReplica.
	//
	// It also ensures that the updated ResourceSpec is propagated to the Host of each KernelContainer/Replica.
	UpdateResourceSpec(spec types.CloneableSpec) error
	KernelSpec() *proto.KernelSpec
	ConnectionInfo() *jupyter.ConnectionInfo
	Status() jupyter.KernelStatus
	// ReplicasAreScheduled returns a flag indicating whether the replicas of this Kernel are scheduled.
	// Under certain scheduling policies, we only schedule a Container when an "execute_request" arrives.
	ReplicasAreScheduled() bool
	// NumCompletedTrainings returns the number of training events that have been completed successfully.
	NumCompletedTrainings() int
	// ReplicaContainersAreBeingScheduled returns true if there is an active 'create container(s)' operation
	// for the target Kernel.
	ReplicaContainersAreBeingScheduled() bool
	// ReplicaContainersStartedAt returns the time at which the target Kernel's KernelContainer instances last started.
	ReplicaContainersStartedAt() time.Time
	AggregateBusyStatus() string
	BindSession(sess string)
	Size() int
	NumActiveMigrationOperations() int
	AddOperationStarted()
	AddOperationCompleted()
	Replicas() []KernelReplica
	PodOrContainerName(id int32) (string, error)
	PrepareNewReplica(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec
	AddReplica(r KernelReplica, host Host) error
	RemoveReplica(r KernelReplica, remover ReplicaRemover, noop bool) (Host, error)
	GetReplicaByID(id int32) (KernelReplica, error)
	RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (Host, error)
	RemoveAllReplicas(remover ReplicaRemover, noop bool, isIdleReclaim bool) error
	// InitialContainerCreationFailed is called by the Cluster Gateway/Scheduler if the initial attempt to schedule
	// the replica containers of the target DistributedKernelClient fails.
	InitialContainerCreationFailed()
	Validate() error
	InitializeShellForwarder(handler KernelMessageHandler) (*messaging.Socket, error)
	InitializeIOForwarder() (*messaging.Socket, error)
	GetReadyReplica() KernelReplica
	// IsIdleReclaimed returns true if the Kernel has been idle reclaimed.
	IsIdleReclaimed() bool
	IsReady() bool
	Socket(typ messaging.MessageType) *messaging.Socket
	GetSocketPort(typ messaging.MessageType) int
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, _ string, typ messaging.MessageType, jupyterMessages []*messaging.JupyterMessage, handler KernelReplicaMessageHandler, done func(), replicas ...KernelReplica) error
	Shutdown(remover ReplicaRemover, restart bool) error
	WaitClosed() jupyter.KernelStatus
	DebugMode() bool

	// SetKernelKey sets the Key field of the ConnectionInfo of the server.AbstractServer underlying the DistributedKernelClient.
	SetKernelKey(string)

	// SetSignatureScheme sets the SignatureScheme field of the ConnectionInfo of the server.AbstractServer underlying the
	// DistributedKernelClient.
	SetSignatureScheme(string)

	// NumActiveExecutionOperations returns the number of ActiveExecution structs registered with
	// the kernel. This counts both the current ActiveExecution and the length of the queue of
	// ActiveExecution structs.
	//
	// This method is thread safe.
	NumActiveExecutionOperations() int

	// TemporaryKernelReplicaClient returns the TemporaryKernelReplicaClient struct used by the DistributedKernelClient.
	//
	// TemporaryKernelReplicaClient structs are used in place of KernelReplicaClient structs when the replica container(s)
	// of a given kernel is/are not scheduled, and that kernel receives a message.
	TemporaryKernelReplicaClient() KernelReplicaInfo

	// ActiveTrainingStartedAt returns the time at which one of the target DistributedKernelClient's replicas
	// began actively training, if there is an actively-training replica.
	ActiveTrainingStartedAt() time.Time

	// LastTrainingStartedAt returns the time at which the last training to occur began. If there is an active
	// training when LastTrainingStartedAt is called, then LastTrainingStartedAt will return the time at which
	// the active training began.
	LastTrainingStartedAt() time.Time

	// LastTrainingSubmittedAt returns the time at which the last training to occur was submitted to the kernel.
	// If there is an active training when LastTrainingSubmittedAt is called, then LastTrainingSubmittedAt will return
	// the time at which the active training was submitted to the kernel.
	LastTrainingSubmittedAt() time.Time

	// LastTrainingEndedAt returns the time at which the last completed training ended.
	//
	// If the kernel is currently training, then LastTrainingEndedAt returns the time at which the previous
	// training ended.
	LastTrainingEndedAt() time.Time

	// HasActiveTraining returns true if the target Kernel has an active training -- meaning that the Kernel has
	// submitted an "execute_request" and is still awaiting a response.
	//
	// Having an "active" training does not necessarily mean that the Kernel is running code right now.
	// It simply means that an execution has been submitted to the Kernel.
	//
	// Having an active training prevents a Kernel from being idle-reclaimed.
	HasActiveTraining() bool

	// IsTraining returns true if one of the target Kernel's KernelReplica instances is actively training.
	IsTraining() bool

	// BeginSchedulingReplicaContainers attempts to take ownership over the next/current scheduling attempt.
	//
	// If there's another active operation, then this will return false along with the CreateReplicaContainersAttempt
	// associated with the active/ongoing container creation operation.
	//
	// If the KernelContainer instances for the KernelReplica instances of this Kernel are already scheduled, then
	// BeginSchedulingReplicaContainers will return false and nil.
	BeginSchedulingReplicaContainers() (bool, CreateReplicaContainersAttempt)

	// RecordContainerPlacementStarted is called while scheduling the KernelContainer instances for the
	// KernelReplica instances of the target Kernel.
	//
	// Specifically, PlacementBeganSchedulingReplicaContainers is called to signal that N viable Host instances have
	// been identified to serve the KernelContainer instances for the target Kernel, where N is the number of replicas
	// of the target Kernel.
	RecordContainerPlacementStarted()
}

type KernelReplica interface {
	types.Contextable
	messaging.JupyterServerInfo
	SessionManager
	Server

	// ID returns the ID of the associated Kernel.
	ID() string

	// ReplicaID returns the SMR node ID of the Replica.
	ReplicaID() int32

	// KernelStoppedTraining is called when the Replica has stopped training.
	KernelStoppedTraining(reason string) error

	Container() KernelContainer
	Host() Host
	SetContainer(container KernelContainer)
	IsTraining() bool
	WaitForTrainingToStop()
	KernelStartedTraining(trainingStartedAt time.Time) error
	WaitForPendingExecuteRequests()
	SetLastTrainingTimePrometheusUpdate()
	LastTrainingTimePrometheusUpdate() time.Time
	NumPendingExecuteRequests() int

	// SendingExecuteRequest records that an "execute_request" (or "yield_request") message is being sent.
	//
	// SendingExecuteRequest should be called RIGHT BEFORE the "execute_request" message is ACTUALLY sent.
	SendingExecuteRequest(msg *messaging.JupyterMessage)
	ReceivedExecuteReply(msg *messaging.JupyterMessage, own bool)
	LastTrainingStartedAt() time.Time
	WorkloadId() string
	SetWorkloadId(workloadId string)
	WorkloadIdSet() bool
	ShouldAckMessages() bool
	GetPodOrContainerName() string
	NodeName() string
	ShellListenPort() int
	IOPubListenPort() int
	YieldNextExecutionRequest()
	SetPodOrContainerName(name string)
	SetNodeName(name string)
	InitializeIOForwarder() (*messaging.Socket, error)
	YieldedNextExecutionRequest()
	SupposedToYieldNextExecutionRequest() bool
	SourceKernelID() string
	SetReplicaID(replicaId int32)
	SetPersistentID(persistentId string)
	PersistentID() string
	ResourceSpec() *types.DecimalSpec

	// InitializeResourceSpec sets the ResourceSpec of the KernelReplica.
	//
	// This does NOT propagate the updated spec to any UserSession or KernelContainer or Host.
	// As such, SetReplicaSpec should only be called when instantiating/initializing a new KernelReplica.
	//
	// If you wish to update the ResourceSpec of an existing KernelReplica, then you should use the
	// UpdateResourceSpec method.
	InitializeResourceSpec(spec *proto.ResourceSpec)

	// UpdateResourceSpec updates the ResourceSpec of the KernelReplica, the UserSession of the KernelReplica, and the
	// KernelContainer of the KernelReplica.
	//
	// It also ensures that the updated ResourceSpec is propagated to the Host of the KernelContainer / KernelReplica.
	//
	// UpdateResourceSpec should only be used to update the ResourceSpec of an existing KernelReplica. When
	// instantiating/initializing (the ResourceSpec of) a new KernelReplica, you should use the InitializeResourceSpec
	// method instead of UpdateResourceSpec.
	UpdateResourceSpec(newSpec types.Spec, tx CoordinatedTransaction) error
	KernelSpec() *proto.KernelSpec
	KernelReplicaSpec() *proto.KernelReplicaSpec
	Address() string
	String() string
	IsReady() bool
	HostId() string
	SetReady()
	Socket(typ messaging.MessageType) *messaging.Socket
	ConnectionInfo() *jupyter.ConnectionInfo
	Status() jupyter.KernelStatus
	BusyStatus() (string, *messaging.JupyterMessage)
	BindSession(sess string)
	ReconnectSocket(typ messaging.MessageType) (*messaging.Socket, error)
	Validate() error
	InitializeShellForwarder(handler KernelMessageHandler) (*messaging.Socket, error)
	AddIOHandler(topic string, handler MessageBrokerHandler[KernelReplica, *messaging.JupyterFrames, *messaging.JupyterMessage]) error
	RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndWaitOptionGetter(parentContext context.Context, typ messaging.MessageType, msg *messaging.JupyterMessage, handler KernelReplicaMessageHandler, getOption server.WaitResponseOptionGetter, done func()) error
	InitializeIOSub(handler messaging.MessageHandler, subscriptionTopic string) (*messaging.Socket, error)
	HandleIOKernelStatus(kernelReplica KernelReplica, frames *messaging.JupyterFrames, msg *messaging.JupyterMessage) error

	// ContainerType returns the current ContainerType of the (KernelContainer of the) target KernelReplica.
	ContainerType() (ContainerType, bool)

	// PromotePrewarmContainer is used to promote a KernelContainer whose ContainerType is PrewarmContainer
	// to a StandardContainer.
	PromotePrewarmContainer() error
}
