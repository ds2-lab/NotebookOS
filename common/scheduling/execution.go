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

	// TrainingEndedAt returns the time at which the last training ended.
	//
	// If the kernel is currently training, then TrainingEndedAt returns the time at which the previous training ended.
	TrainingEndedAt() time.Time

	// SendingExecuteRequest records that an "execute_request" (or "yield_request") message is being sent.
	//
	// SendingExecuteRequest should be called RIGHT BEFORE the "execute_request" message is ACTUALLY sent.
	SendingExecuteRequest(msg *messaging.JupyterMessage) error

	// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
	// code execution, or nil if no code executions have occurred.
	LastPrimaryReplica() KernelReplica

	// ExecutionIndexIsLarger returns true if the given executionIndex is larger than all 3 of the execution-index-related
	// fields of the KernelReplicaClient, namely submittedExecutionIndex, activeExecutionIndex, and completedExecutionIndex.
	ExecutionIndexIsLarger(executionIndex int32) bool
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
	SetExecuted()
	String() string
	ReceivedLeadNotification(smrNodeId int32) error
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
	GetOriginallySentAtTime() time.Time
	GetWorkloadId() string
	GetExecuteRequestMessageId() string

	// GetExecutionIndex returns the ExecutionIndex of the target Execution.
	GetExecutionIndex() int32
}
type Proposal interface {
	GetKey() ProposalKey
	GetReason() string
	IsYield() bool
	IsLead() bool
	String() string
}
