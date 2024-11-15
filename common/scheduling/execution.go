package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"time"
)

// CodeExecution encapsulates the submission of a single 'execute_request' message for a particular kernel.
// We observe the results of the SMR proposal protocol and take action accordingly, depending upon the results.
// For example, if all replicas of the kernel issue 'YIELD' roles, then we will need to perform some sort of
// scheduling action, depending upon what scheduling policy we're using.
//
// Specifically, under 'static' scheduling, we dynamically provision a new replica to handle the request.
// Alternatively, under 'dynamic' scheduling, we migrate existing replicas to another node to handle the request.
type CodeExecution interface {
	RegisterReply(replicaId int32, response *types.JupyterMessage, overwrite bool) error
	HasValidWorkloadId() bool
	HasValidOriginalSentTimestamp() bool
	OriginalSentTimestamp() time.Time
	OriginalTimestampOrCreatedAt() time.Time
	Msg() *types.JupyterMessage
	HasExecuted() bool
	SetExecuted()
	String() string
	ReceivedLeadNotification(smrNodeId int32) error
	ReceivedYieldNotification(smrNodeId int32) error
	NumRolesReceived() int
	LinkPreviousAttempt(previousAttempt CodeExecution)
	LinkNextAttempt(nextAttempt CodeExecution)

	GetActiveReplica() KernelReplica
	GetExecuteRequestMessageId() string
	GetAttemptId() int
	GetWorkloadId() string
}
