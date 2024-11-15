package client

import (
	"context"
	jupyterTypes "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/entity"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"time"
)

// AbstractDistributedKernelClient is just an extraction of the public/exported methods of the
// DistributedKernelClient struct into an interface so that we can mock the interface for unit tests.
type AbstractDistributedKernelClient interface {
	SessionManager

	SetSession(session *scheduling.UserSession)
	GetSession() *scheduling.UserSession
	GetContainers() []*entity.Container
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() *entity.ActiveExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (*entity.ActiveExecution, bool)
	ExecutionFailedCallback() ExecutionFailedCallback
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
	Replicas() []scheduling.KernelReplica
	PodOrContainerName(id int32) (string, error)
	PrepareNewReplica(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec
	AddReplica(r scheduling.KernelReplica, host *entity.Host) error
	RemoveReplica(r scheduling.KernelReplica, remover scheduling.ReplicaRemover, noop bool) (*entity.Host, error)
	GetReplicaByID(id int32) (scheduling.KernelReplica, error)
	RemoveReplicaByID(id int32, remover scheduling.ReplicaRemover, noop bool) (*entity.Host, error)
	Validate() error
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*jupyterTypes.Socket, error)
	InitializeIOForwarder() (*jupyterTypes.Socket, error)
	GetReadyReplica() scheduling.KernelReplica
	IsReady() bool
	Socket(typ jupyterTypes.MessageType) *jupyterTypes.Socket
	GetSocketPort(typ jupyterTypes.MessageType) int
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ jupyterTypes.MessageType, msg *jupyterTypes.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, typ jupyterTypes.MessageType, jMsg *jupyterTypes.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func(), replicas ...scheduling.KernelReplica) error
	Shutdown(remover scheduling.ReplicaRemover, restart bool) error
	Close() error
	WaitClosed() jupyterTypes.KernelStatus

	// NumActiveExecutionOperations returns the number of scheduling.ActiveExecution structs registered with
	// the kernel. This counts both the current scheduling.ActiveExecution as well as the length of the queue of
	// scheduling.ActiveExecution structs.
	//
	// This method is thread safe.
	NumActiveExecutionOperations() int
}

// AbstractKernelClient is just an extraction of the public/exported methods of the
// KernelReplicaClient struct into an interface so that we can mock the interface for unit tests.
type AbstractKernelClient interface {
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
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*jupyterTypes.Socket, error)
	AddIOHandler(topic string, handler MessageBrokerHandler[scheduling.Kernel, *jupyterTypes.JupyterFrames, *jupyterTypes.JupyterMessage]) error
	RequestWithHandler(ctx context.Context, _ string, typ jupyterTypes.MessageType, msg *jupyterTypes.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error
	Close() error
	GetHost() *entity.Host
	SetHost(host *entity.Host)
	InitializeIOSub(handler jupyterTypes.MessageHandler, subscriptionTopic string) (*jupyterTypes.Socket, error)
}
