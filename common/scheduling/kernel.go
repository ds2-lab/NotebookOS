package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
	jupyterTypes "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/entity"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"time"
)

// KernelMessageHandler is an API defines the interface of messages that a JupyterRouter can intercept and handle.
type KernelMessageHandler func(KernelInfo, jupyterTypes.MessageType, *jupyterTypes.JupyterMessage) error

// ReplicaRemover is a function that removes a replica from a kernel.
// If noop is specified, it is the caller's responsibility to stop the replica.
type ReplicaRemover func(host *entity.Host, session UserSession, noop bool) error

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

type Kernel interface {
	client.SessionManager

	SetSession(session UserSession)
	GetSession() UserSession
	GetContainers() []*entity.Container
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() *entity.ActiveExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (*entity.ActiveExecution, bool)
	ExecutionFailedCallback() client.ExecutionFailedCallback
	SetActiveExecution(activeExecution *entity.ActiveExecution)
	ExecutionComplete(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot], msg *jupyterTypes.JupyterMessage) (bool, error)
	EnqueueActiveExecution(attemptId int, msg *jupyterTypes.JupyterMessage) *entity.ActiveExecution
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
	AddReplica(r KernelReplica, host *entity.Host) error
	RemoveReplica(r KernelReplica, remover ReplicaRemover, noop bool) (*entity.Host, error)
	GetReplicaByID(id int32) (KernelReplica, error)
	RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (*entity.Host, error)
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
	Container() *entity.Container
	SetContainer(container *entity.Container)
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
	GetHost() *entity.Host
	SetHost(host *entity.Host)
	InitializeIOSub(handler jupyterTypes.MessageHandler, subscriptionTopic string) (*jupyterTypes.Socket, error)
}
