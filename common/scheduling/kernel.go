package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyterTypes "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"time"
)

// KernelMessageHandler is an API defines the interface of messages that a JupyterRouter can intercept and handle.
type KernelMessageHandler func(KernelInfo, jupyterTypes.MessageType, *jupyterTypes.JupyterMessage) error

// ReplicaRemover is a function that removes a replica from a kernel.
// If noop is specified, it is the caller's responsibility to stop the replica.
type ReplicaRemover func(host Host, session UserSession, noop bool) error

type KernelReplicaMessageHandler func(KernelReplicaInfo, jupyterTypes.MessageType, *jupyterTypes.JupyterMessage) error

type KernelInfo interface {
	// RouterInfo provides kernel specific routing information.
	router.RouterInfo

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

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c Kernel) error

type Kernel interface {
	types.Contextable
	SessionManager

	SetSession(session UserSession)
	GetSession() UserSession
	GetContainers() []KernelContainer
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() CodeExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (CodeExecution, bool)
	ExecutionFailedCallback() ExecutionFailedCallback
	SetActiveExecution(activeExecution CodeExecution)
	ExecutionComplete(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot], msg *jupyterTypes.JupyterMessage) (bool, error)
	EnqueueActiveExecution(attemptId int, msg *jupyterTypes.JupyterMessage) CodeExecution
	ResetID(id string)
	PersistentID() string
	String() string
	ID() string
	SourceKernelID() string
	ResourceSpec() *types.DecimalSpec
	KernelSpec() *proto.KernelSpec
	ConnectionInfo() *jupyterTypes.ConnectionInfo
	Status() jupyterTypes.KernelStatus
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
	Validate() error
	InitializeShellForwarder(handler KernelMessageHandler) (*jupyterTypes.Socket, error)
	InitializeIOForwarder() (*jupyterTypes.Socket, error)
	GetReadyReplica() KernelReplica
	IsReady() bool
	Socket(typ jupyterTypes.MessageType) *jupyterTypes.Socket
	GetSocketPort(typ jupyterTypes.MessageType) int
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ jupyterTypes.MessageType, msg *jupyterTypes.JupyterMessage, handler KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, typ jupyterTypes.MessageType, jMsg *jupyterTypes.JupyterMessage, handler KernelReplicaMessageHandler, done func(), replicas ...KernelReplica) error
	Shutdown(remover ReplicaRemover, restart bool) error
	Close() error
	WaitClosed() jupyterTypes.KernelStatus

	// NumActiveExecutionOperations returns the number of ActiveExecution structs registered with
	// the kernel. This counts both the current ActiveExecution as well as the length of the queue of
	// ActiveExecution structs.
	//
	// This method is thread safe.
	NumActiveExecutionOperations() int
}

type KernelReplica interface {
	types.Contextable

	Container() KernelContainer
	SetContainer(container KernelContainer)
	IsTraining() bool
	WaitForTrainingToStop()
	WaitForPendingExecuteRequests()
	SetLastTrainingTimePrometheusUpdate()
	LastTrainingTimePrometheusUpdate() time.Time
	NumPendingExecuteRequests() int
	SentExecuteRequest(msg *jupyterTypes.JupyterMessage)
	ReceivedExecuteReply(msg *jupyterTypes.JupyterMessage)
	KernelStoppedTraining(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error
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
	YieldedNextExecutionRequest()
	SupposedToYieldNextExecutionRequest() bool
	ID() string
	SourceKernelID() string
	ReplicaID() int32
	SetReplicaID(replicaId int32)
	SetPersistentID(persistentId string)
	PersistentID() string
	ResourceSpec() *types.DecimalSpec
	SetResourceSpec(spec *proto.ResourceSpec)
	KernelSpec() *proto.KernelSpec
	Address() string
	String() string
	UpdateResourceSpec(types.Spec) error
	IsReady() bool
	HostId() string
	SetReady()
	Socket(typ jupyterTypes.MessageType) *jupyterTypes.Socket
	ConnectionInfo() *jupyterTypes.ConnectionInfo
	Status() jupyterTypes.KernelStatus
	BusyStatus() (string, *jupyterTypes.JupyterMessage)
	BindSession(sess string)
	ReconnectSocket(typ jupyterTypes.MessageType) (*jupyterTypes.Socket, error)
	Validate() error
	InitializeShellForwarder(handler KernelMessageHandler) (*jupyterTypes.Socket, error)
	AddIOHandler(topic string, handler client.MessageBrokerHandler[Kernel, *jupyterTypes.JupyterFrames, *jupyterTypes.JupyterMessage]) error
	RequestWithHandler(ctx context.Context, _ string, typ jupyterTypes.MessageType, msg *jupyterTypes.JupyterMessage, handler KernelReplicaMessageHandler, done func()) error
	Close() error
	GetHost() Host
	SetHost(host Host)
	InitializeIOSub(handler jupyterTypes.MessageHandler, subscriptionTopic string) (*jupyterTypes.Socket, error)
}
