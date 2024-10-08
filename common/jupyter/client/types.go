package client

import (
	"context"
	types2 "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"time"
)

// AbstractDistributedKernelClient is just an extraction of the public/exported methods of the
// DistributedKernelClient struct into an interface so that we can mock the interface for unit tests.
type AbstractDistributedKernelClient interface {
	SetSession(session *scheduling.Session)
	GetSession() *scheduling.Session
	GetContainers() []*scheduling.Container
	ShellListenPort() int
	IOPubListenPort() int
	ActiveExecution() *scheduling.ActiveExecution
	GetActiveExecutionByExecuteRequestMsgId(msgId string) (*scheduling.ActiveExecution, bool)
	ExecutionFailedCallback() ExecutionFailedCallback
	SetActiveExecution(activeExecution *scheduling.ActiveExecution)
	ExecutionComplete(snapshot types.HostResourceSnapshot[*scheduling.ResourceSnapshot], msg *types2.JupyterMessage) (bool, error)
	EnqueueActiveExecution(attemptId int, msg *types2.JupyterMessage) *scheduling.ActiveExecution
	ResetID(id string)
	PersistentID() string
	String() string
	ID() string
	SourceKernelID() string
	ResourceSpec() *types.DecimalSpec
	KernelSpec() *proto.KernelSpec
	ConnectionInfo() *types2.ConnectionInfo
	Status() types2.KernelStatus
	AggregateBusyStatus() string
	BindSession(sess string)
	Size() int
	NumActiveMigrationOperations() int
	AddOperationStarted()
	AddOperationCompleted()
	Replicas() []scheduling.KernelReplica
	PodName(id int32) (string, error)
	PrepareNewReplica(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec
	AddReplica(r scheduling.KernelReplica, host *scheduling.Host) error
	RemoveReplica(r scheduling.KernelReplica, remover ReplicaRemover, noop bool) (*scheduling.Host, error)
	GetReplicaByID(id int32) (scheduling.KernelReplica, error)
	RemoveReplicaByID(id int32, remover ReplicaRemover, noop bool) (*scheduling.Host, error)
	Validate() error
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types2.Socket, error)
	InitializeIOForwarder() (*types2.Socket, error)
	GetReadyReplica() scheduling.KernelReplica
	IsReady() bool
	IsReplicaReady(replicaId int32) (bool, error)
	RequestWithHandler(ctx context.Context, _ string, typ types2.MessageType, msg *types2.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error
	RequestWithHandlerAndReplicas(ctx context.Context, typ types2.MessageType, jMsg *types2.JupyterMessage, handler scheduling.KernelMessageHandler, done func(), replicas ...scheduling.KernelReplica) error
	Shutdown(remover ReplicaRemover, restart bool) error
	Close() error
	WaitClosed() types2.KernelStatus

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
	WaitForRepliesToPendingExecuteRequests()
	SetLastTrainingTimePrometheusUpdate()
	LastTrainingTimePrometheusUpdate() time.Time
	NumPendingExecuteRequests() int
	SentExecuteRequest(msg *types2.JupyterMessage)
	ReceivedExecuteReply(msg *types2.JupyterMessage)
	KernelStoppedTraining(snapshot types.HostResourceSnapshot[*scheduling.ResourceSnapshot]) error
	TrainingStartedAt() time.Time
	WorkloadId() string
	SetWorkloadId(workloadId string)
	WorkloadIdSet() bool
	ShouldAckMessages() bool
	PodName() string
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
	IsReady() bool
	HostId() string
	SetReady()
	Socket(typ types2.MessageType) *types2.Socket
	ConnectionInfo() *types2.ConnectionInfo
	Status() types2.KernelStatus
	BusyStatus() (string, *types2.JupyterMessage)
	BindSession(sess string)
	ReconnectSocket(typ types2.MessageType) (*types2.Socket, error)
	Validate() error
	InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types2.Socket, error)
	AddIOHandler(topic string, handler MessageBrokerHandler[scheduling.Kernel, *types2.JupyterFrames, *types2.JupyterMessage]) error
	RequestWithHandler(ctx context.Context, _ string, typ types2.MessageType, msg *types2.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error
	Close() error
	GetHost() *scheduling.Host
	SetHost(host *scheduling.Host)
	InitializeIOSub(handler types2.MessageHandler, subscriptionTopic string) (*types2.Socket, error)
}
