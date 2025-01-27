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

type ExecutionManager interface {
	RegisterExecution(msg *messaging.JupyterMessage) (Execution, error)
	YieldProposalReceived(replica KernelReplica, msg *messaging.JupyterMessage,
		msgErr *messaging.MessageErrorWithYieldReason) error
	HandleSmrLeadTaskMessage(msg *messaging.JupyterMessage, kernelReplica KernelReplica) error
	HandleExecuteReplyMessage(msg *messaging.JupyterMessage, replica KernelReplica) (bool, error)
	ExecutionComplete(msg *messaging.JupyterMessage, replica KernelReplica) (Execution, error)
	GetActiveExecution(msgId string) Execution
	NumActiveExecutionOperations() int
	TotalNumExecutionOperations() int
	ExecutionFailedCallback() ExecutionFailedCallback

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
