package scheduling

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
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
// the DistributedKernelClient will check if its ActiveExecution struct has the original "sent-at" timestamp
// of the original "execute_request". If it does, then it can calculate the latency between submission and when
// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
// so that a relevant Prometheus metric can be updated.
type ExecutionLatencyCallback func(latency time.Duration, workloadId string, kernelId string)

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c Kernel, msg *messaging.JupyterMessage) error

// YieldNotificationHandler is called when a YIELD notification is received.
type YieldNotificationHandler func(replica KernelReplica, msgErr *messaging.MessageErrorWithYieldReason, msg *messaging.JupyterMessage) error

type NotificationCallback func(title string, content string, notificationType messaging.NotificationType)

type Kernel interface {
	types.Contextable
	SessionManager
	Server

	SetSession(session UserSession)
	GetSession() UserSession
	GetContainers() []KernelContainer
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() *ActiveExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (*ActiveExecution, bool)
	// GetActiveExecution returns the *scheduling.ActiveExecution associated with the given "execute_request" message ID.
	GetActiveExecution(msgId string) *ActiveExecution
	CurrentActiveExecution() *ActiveExecution
	ReleasePreCommitedResourcesFromReplica(replica KernelReplica, msg *messaging.JupyterMessage) error
	ExecutionFailedCallback() ExecutionFailedCallback
	SetActiveExecution(activeExecution *ActiveExecution)
	ExecutionComplete(msg *messaging.JupyterMessage) (bool, error)
	EnqueueActiveExecution(attemptId int, msg *messaging.JupyterMessage) *ActiveExecution
	ResetID(id string)
	PersistentID() string
	String() string
	ID() string
	SourceKernelID() string
	ResourceSpec() *types.DecimalSpec

	// UpdateResourceSpec updates the ResourceSpec of the Kernel, all of its KernelReplica instances, the UserSession
	// of each KernelReplica, and the KernelContainer of each KernelReplica.
	//
	// It also ensures that the updated ResourceSpec is propagated to the Host of each KernelContainer/KernelReplica.
	UpdateResourceSpec(spec types.Spec) error
	KernelSpec() *proto.KernelSpec
	ConnectionInfo() *jupyter.ConnectionInfo
	Status() jupyter.KernelStatus
	// ReplicasAreScheduled returns a flag indicating whether the replicas of this Kernel are scheduled.
	// Under certain scheduling policies, we only schedule a Container when an "execute_request" arrives.
	ReplicasAreScheduled() bool
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
	RemoveAllReplicas(remover ReplicaRemover, noop bool) error
	Validate() error
	InitializeShellForwarder(handler KernelMessageHandler) (*messaging.Socket, error)
	InitializeIOForwarder() (*messaging.Socket, error)
	GetReadyReplica() KernelReplica
	IsReady() bool
	Socket(typ messaging.MessageType) *messaging.Socket
	GetSocketPort(typ messaging.MessageType) int
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, typ messaging.MessageType, jupyterMessages []*messaging.JupyterMessage, handler KernelReplicaMessageHandler, done func(), replicas ...KernelReplica) error
	Shutdown(remover ReplicaRemover, restart bool) error
	WaitClosed() jupyter.KernelStatus
	DebugMode() bool

	// AddDestFrameIfNecessary adds the destination frame to the specified Jupyter message if it isn't already present.
	AddDestFrameIfNecessary(jMsg *messaging.JupyterMessage) *messaging.JupyterMessage

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
}

type KernelReplica interface {
	types.Contextable
	SessionManager
	Server

	Container() KernelContainer
	Host() Host
	SetContainer(container KernelContainer)
	IsTraining() bool
	WaitForTrainingToStop()
	KernelStartedTraining() error
	WaitForPendingExecuteRequests()
	SetLastTrainingTimePrometheusUpdate()
	LastTrainingTimePrometheusUpdate() time.Time
	NumPendingExecuteRequests() int
	SentExecuteRequest(msg *messaging.JupyterMessage)
	ReceivedExecuteReply(msg *messaging.JupyterMessage)
	KernelStoppedTraining(reason string) error
	TrainingStartedAt() time.Time
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
	ID() string
	SourceKernelID() string
	ReplicaID() int32
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
	UpdateResourceSpec(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error
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
}
