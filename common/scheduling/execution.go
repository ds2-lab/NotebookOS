package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"time"
)

var (
	// LeadProposal is issued by a kernel replica when it would like to execute the user-submitted code.
	LeadProposal ProposalKey = "LEAD"

	// YieldProposal is issued by a kernel replica when it would like to defer the execution of
	// the user-submitted code to another kernel replica.
	YieldProposal ProposalKey = "YIELD"
)

type ProposalKey string

func (pk ProposalKey) String() string {
	return string(pk)
}

// ExecutionManager instances are assigned to each Kernel in a 1-1 relationship. The ExecutionManager instance is
// responsible for managing the bookkeeping and state (transition) management related to the execution of user-submitted
// code. Note that code is submitted to Kernel instances by users/clients in the form of Jupyter "execute_request"
// messages.
//
// Per the documentation of the AllocationManager interface, resources are "pre-allocated" to a KernelContainer instances
// in anticipation of the KernelContainer beginning to execute code. In some cases, the Global Scheduler may explicitly
// select a replica to serve as the "primary replica" before forwarding the user-submitted code to the KernelReplica.
// Alternatively, the configured scheduling policy may specify that only one KernelReplica should exist per Kernel, in
// which case it is already known which KernelReplica will serve as the "primary replica". In these cases, resources are
// "pre-allocated" or "pre-committed" to a KernelContainer so that they are definitively available when the forwarded
// "execute_request" message reaches the KernelReplica running within that KernelContainer.
//
// Once the "smr_lead_task" notification that the KernelReplica running within the KernelContainer has actually started
// training is received, the "pre-allocated"/"pre-committed" resources are "promoted" to simply being "committed".
// (Note that "pre-allocated"/"pre-committed" resources are already included in a Host's "committed" resource count, so
// this promotion is purely semantic.) There are situations in which the execution of the user submitted code by the
// KernelReplica completes so quickly that the associated "execute_reply" message is received before the "smr_lead_task"
// notification. In this case, the "pre-allocated"/"pre-committed" resources are never semantically promoted to simply
// being "committed". When the "smr_lead_task" message is eventually received, it is simply ignored. The determination
// that a given "smr_lead_task" message should or should not be ignored is based upon a monotonically increasing counter
// that is assigned to each unique "execute_request" message sent to a particular Kernel. This information is managed by
// the scheduling.ExecutionManager instance.
//
// Specifically, when an "execute_request" message is received, the ExecutionManager compares the "msg_id" contained
// within the message's header against its records. If the message is new, then it is assigned the next value from the
// monotonically-increasing ExecutionIndex counter maintained by each ExecutionManager. Note that the ExecutionIndex is
// a "local" value -- local to each Kernel. When the ExecutionManager is notified that an "execute_reply" message has
// been received (from one of the KernelReplica instances associated with its Kernel), the ExecutionManager checks
// the "msg_id" field from the parent header of the "execute_reply" message. (The "msg_id" in the parent header is the
// message ID of the associated "execute_request" message in which the "execute_reply" message was sent as a response.)
// The ExecutionManager queries its internal records to retrieve the ExecutionIndex assigned to that "execute_request"
// message. If they match, then the ExecutionManager records that the execution has completed. Now, if a "smr_lead_task"
// message arrives, the ExecutionManager can retrieve the "msg_id" of the associated "execute_request" message from the
// metadata of the "smr_lead_task" message. (Note that "smr_lead_task" messages are sent by a KernelReplica to notify
// the system that it has officially started executing the user-submitted code.) From that, it can retrieve the
// associated ExecutionIndex. If the associated ExecutionIndex is less than or equal to the largest ExecutionIndex
// associated with a completed execution (i.e., an execution for which an "execute_reply" [with "status" == "ok"] has
// been received from a KernelReplica of the associated Kernel), then the "smr_lead_task" message can be safely ignored.
// If the associated ExecutionIndex is merely equal to the largest ExecutionIndex of an execution for which the associated
// "execute_request" had been forwarded to the KernelReplica instances of the associated Kernel, but for which no
// "execute_reply" has yet been received, then the "smr_lead_task" will not be ignored, and any necessary state updates
// will occur to reflect that the KernelReplica that sent the "smr_lead_task" message has officially started executing.
type ExecutionManager interface {
	RegisterExecution(msg *messaging.JupyterMessage) (Execution, error)
	YieldProposalReceived(replica KernelReplica, executeReplyMsg *messaging.JupyterMessage, msgErr *messaging.MessageErrorWithYieldReason) error
	HandleSmrLeadTaskMessage(msg *messaging.JupyterMessage, kernelReplica KernelReplica) error
	HandleExecuteReplyMessage(msg *messaging.JupyterMessage, replica KernelReplica) (bool, error)
	ExecutionComplete(msg *messaging.JupyterMessage, replica KernelReplica) (Execution, error)
	GetActiveExecution(msgId string) Execution
	NumActiveExecutionOperations() int
	TotalNumExecutionOperations() int
	ExecutionFailedCallback() ExecutionFailedCallback

	// IsExecutionComplete returns true if the execution associated with the given message ID is complete.
	IsExecutionComplete(executeRequestId string) bool

	// ReplicaRemoved is used to notify the ExecutionManager that a particular KernelReplica has been removed.
	// This allows the ExecutionManager to set the LastPrimaryReplica field to nil if the removed KernelReplica
	// is the LastPrimaryReplica.
	ReplicaRemoved(replica KernelReplica)

	// NumCompletedTrainings returns the number of training events that have been completed successfully.
	NumCompletedTrainings() int

	// LastTrainingStartedAt returns the time at which the last training to occur began. If there is an active
	// training when LastTrainingStartedAt is called, then LastTrainingStartedAt will return the time at which
	// the active training began.
	LastTrainingStartedAt() time.Time

	// GetSmrLeadTaskMessage returns the "smr_lead_task" IO pub message that was sent by the primary replica of the
	// execution triggered by the "execute_request" message with the specified ID.
	GetSmrLeadTaskMessage(executeRequestId string) (*messaging.JupyterMessage, bool)

	// GetExecuteReplyMessage returns the "execute_reply" message that was sent in response to the "execute_request"
	// message with the specified ID, if one exists. Specifically, it would be the "execute_reply" sent by the primary
	// replica when it finished executing the user-submitted code.
	GetExecuteReplyMessage(executeRequestId string) (*messaging.JupyterMessage, bool)

	// LastTrainingSubmittedAt returns the time at which the last training to occur was submitted to the kernel.
	// If there is an active training when LastTrainingSubmittedAt is called, then LastTrainingSubmittedAt will return
	// the time at which the active training was submitted to the kernel.
	LastTrainingSubmittedAt() time.Time

	// LastTrainingEndedAt returns the time at which the last completed training ended.
	//
	// If the kernel is currently training, then TrainingEndedAt returns the time at which the previous training ended.
	LastTrainingEndedAt() time.Time

	// SendingExecuteRequest records that an "execute_request" (or "yield_request") message is being sent.
	//
	// SendingExecuteRequest should be called RIGHT BEFORE the "execute_request" message is ACTUALLY sent.
	SendingExecuteRequest(messages []*messaging.JupyterMessage) error

	// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
	// code execution, or nil if no code executions have occurred.
	LastPrimaryReplica() KernelReplica

	// ExecutionIndexIsLarger returns true if the given executionIndex is larger than all 3 of the execution-index-related
	// fields of the KernelReplicaClient, namely submittedExecutionIndex, activeExecutionIndex, and completedExecutionIndex.
	ExecutionIndexIsLarger(executionIndex int32) bool

	// LastPrimaryReplicaId returns the SMR node ID of the KernelReplica that served as the primary replica for the
	// previous code execution, or nil if no code executions have occurred.
	//
	// LastPrimaryReplicaId is preserved even if the last primary replica is removed/migrated.
	LastPrimaryReplicaId() int32

	// NumExecutionsByReplica returns the number of times that the specified replica served as the primary replica
	// and successfully executed user-submitted code.
	NumExecutionsByReplica(replicaId int32) int

	// HasActiveTraining returns true if the target DistributedKernelClient has an active training -- meaning that the
	// Kernel has submitted an "execute_request" and is still awaiting a response.
	//
	// Having an "active" training does not necessarily mean that the Kernel is running code right now.
	// It simply means that an execution has been submitted to the Kernel.
	//
	// Having an active training prevents a Kernel from being idle-reclaimed.
	HasActiveTraining() bool
}

type Execution interface {
	GetAttemptNumber() int
	LinkPreviousAttempt(previousAttempt Execution)
	LinkNextAttempt(nextAttempt Execution)
	RegisterReply(replicaId int32, response *messaging.JupyterMessage, overwrite bool) error
	HasValidWorkloadId() bool
	HasValidOriginalSentTimestamp() bool
	OriginalTimestampOrCreatedAt() time.Time
	Msg() *messaging.JupyterMessage
	HasExecuted() bool
	SetExecuted(receivedExecuteReplyAt time.Time)
	String() string
	// ReceivedSmrLeadTaskMessage records that the specified kernel replica was selected as the primary replica
	// and will be executing the code.
	ReceivedSmrLeadTaskMessage(replica KernelReplica, receivedAt time.Time) error
	ReceivedYieldNotification(smrNodeId int32, yieldReason string) error
	NumRolesReceived() int
	NumLeadReceived() int
	NumYieldReceived() int
	RangeRoles(rangeFunc func(int32, Proposal) bool)
	IsRunning() bool
	IsPending() bool
	IsCompleted() bool
	IsErred() bool
	GetNumReplicas() int
	SetActiveReplica(replica KernelReplica)
	GetActiveReplica() KernelReplica
	GetOriginallySentAtTime() time.Time
	GetWorkloadId() string
	GetExecuteRequestMessageId() string

	// SetTargetReplica is used to record the expected target replica for an execution.
	SetTargetReplica(int32) error
	GetTargetReplicaId() int32

	// GetExecutionIndex returns the ExecutionIndex of the target Execution.
	GetExecutionIndex() int32

	// GetTrainingStartedAt returns the time at which the training began, as indicated in the payload of the
	// "smr_lead_task" message that we received from the primary replica.
	// GetTrainingStartedAt() time.Time

	// GetReceivedSmrLeadTaskAt returns the time at which we received a "smr_lead_task" message from the primary replica.
	GetReceivedSmrLeadTaskAt() time.Time

	// GetReceivedExecuteReplyAt returns the time at which the "execute_reply" message that indicated that the execution
	// had finished was received.
	GetReceivedExecuteReplyAt() time.Time

	// GetMigrationRequired returns a bool indicating whether a migration was required in order to serve this training.
	GetMigrationRequired() bool
	SetMigrationRequired(required bool)

	// GetNumViableReplicas returns the number of replicas that were viable to serve this training request.
	GetNumViableReplicas() int
	SetNumViableReplicas(n int)

	// GetGpuDeviceIDs returns the GPU device IDs assigned to the kernel for this execution.
	GetGpuDeviceIDs() []int

	// SetGpuDeviceIDs sets the GPU device IDs assigned to the kernel for this execution.
	SetGpuDeviceIDs(gpuDeviceIDs []int)
}

type Proposal interface {
	GetKey() ProposalKey
	GetReason() string
	IsYield() bool
	IsLead() bool
	String() string
}
