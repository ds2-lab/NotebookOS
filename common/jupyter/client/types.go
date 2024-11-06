package client

import (
	"context"
	jupyterTypes "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"time"
)

// AbstractDistributedKernelClient is just an extraction of the public/exported methods of the
// DistributedKernelClient struct into an interface so that we can mock the interface for unit tests.
type AbstractDistributedKernelClient interface {
	SessionManager

	SetSession(session *scheduling.Session)
	GetSession() *scheduling.Session
	GetContainers() []*scheduling.Container
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() *scheduling.ActiveExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (*scheduling.ActiveExecution, bool)
	ExecutionFailedCallback() ExecutionFailedCallback
	SetActiveExecution(activeExecution *scheduling.ActiveExecution)
	ExecutionComplete(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot], msg *jupyterTypes.JupyterMessage) (bool, error)
	EnqueueActiveExecution(attemptId int, msg *jupyterTypes.JupyterMessage) *scheduling.ActiveExecution
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
	AddReplica(r scheduling.KernelReplica, host *scheduling.Host) error
	RemoveReplica(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (*scheduling.Host, error)
	GetReplicaByID(id int32) (scheduling.KernelReplica, error)
	RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (*scheduling.Host, error)
	Validate() error
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*jupyterTypes.Socket, error)
	InitializeIOForwarder() (*jupyterTypes.Socket, error)
	GetReadyReplica() scheduling.KernelReplica
	IsReady() bool
	Socket(typ jupyterTypes.MessageType) *jupyterTypes.Socket
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ jupyterTypes.MessageType, msg *jupyterTypes.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, typ jupyterTypes.MessageType, jMsg *jupyterTypes.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func(), replicas ...scheduling.KernelReplica) error
	Shutdown(remover ReplicaRemover, restart bool) error
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
	Container() *scheduling.Container
	SetContainer(container *scheduling.Container)
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
	GetHost() *scheduling.Host
	SetHost(host *scheduling.Host)
	InitializeIOSub(handler jupyterTypes.MessageHandler, subscriptionTopic string) (*jupyterTypes.Socket, error)
}
