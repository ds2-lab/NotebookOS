package gateway

import (
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"

	"github.com/google/uuid"
)

const (
	// Proposal keys:

	//KeyVote  = "VOTE"
	KeyYield = "YIELD"
	KeyLead  = "LEAD"
)

var (
	ErrProposalAlreadyReceived   = errors.New("we already received a proposal from that replica")
	ErrExecutionFailedAllYielded = errors.New("an execution failed; all replicas proposed 'YIELD'")
)

// ActiveExecution encapsulates the submission of a single 'execute_request' message for a particular kernel.
// We observe the results of the SMR proposal protocol and take action accordingly, depending upon the results.
// For example, if all replicas of the kernel issue 'YIELD' proposals, then we will need to perform some sort of
// scheduling action, depending upon what scheduling policy we're using.
//
// Specifically, under 'static' scheduling, we dynamically provision a new replica to handle the request.
// Alternatively, under 'dynamic' scheduling, we migrate existing replicas to another node to handle the request.
type ActiveExecution struct {
	executionId string // Unique ID identifying the execution request.
	attemptId   int    // Beginning at 1, identifies the "attempt number", in case we have to retry due to timeouts.
	sessionId   string // The ID of the Jupyter session that initiated the request.
	kernelId    string // ID of the associated kernel.

	numReplicas int // The number of replicas that the kernel had with the execution request was originally received.

	numLeadProposals  int // Number of 'LEAD' proposals issued.
	numYieldProposals int // Number of 'YIELD' proposals issued.

	proposals map[int32]string // Map from replica ID to what it proposed ('YIELD' or 'LEAD')

	nextAttempt     *ActiveExecution // If we initiate a retry due to timeouts, then we link this attempt to the retry attempt.
	previousAttempt *ActiveExecution // The retry that preceded this one, if this is not the first attempt.

	msg *types.JupyterMessage // The original 'execute_request' message.

	executed bool
}

func NewActiveExecution(kernelId string, sessionId string, attemptId int, numReplicas int, msg *types.JupyterMessage) *ActiveExecution {
	return &ActiveExecution{
		executionId:     uuid.NewString(),
		sessionId:       sessionId,
		attemptId:       attemptId,
		proposals:       make(map[int32]string, 3),
		kernelId:        kernelId,
		numReplicas:     numReplicas,
		nextAttempt:     nil,
		previousAttempt: nil,
		msg:             msg,
	}
}

func (e *ActiveExecution) Msg() *types.JupyterMessage {
	return e.msg
}

func (e *ActiveExecution) HasExecuted() bool {
	return e.executed
}

func (e *ActiveExecution) SetExecuted() {
	e.executed = true
}

func (e *ActiveExecution) ExecutionId() string {
	return e.executionId
}

func (e *ActiveExecution) AttemptId() int {
	return e.attemptId
}

func (e *ActiveExecution) String() string {
	return fmt.Sprintf("ActiveExecution[ID=%s,Kernel=%s,Session=%s,Attempt=%d,NumReplicas=%d,NumLeadProposals=%d,NumYieldProposals=%d,HasNextAttempt=%v,HasPrevAttempt=%v]", e.executionId, e.kernelId, e.sessionId, e.attemptId, e.numReplicas, e.numLeadProposals, e.numYieldProposals, e.nextAttempt == nil, e.previousAttempt == nil)
}

func (e *ActiveExecution) ReceivedLeadProposal(smrNodeId int32) error {
	if _, ok := e.proposals[smrNodeId]; ok {
		return ErrProposalAlreadyReceived
	}

	e.proposals[smrNodeId] = KeyLead
	e.numLeadProposals += 1

	return nil
}

func (e *ActiveExecution) ReceivedYieldProposal(smrNodeId int32) error {
	if _, ok := e.proposals[smrNodeId]; ok {
		return ErrProposalAlreadyReceived
	}

	e.proposals[smrNodeId] = KeyYield
	e.numYieldProposals += 1

	if e.numYieldProposals == e.numReplicas {
		return ErrExecutionFailedAllYielded
	}

	return nil
}

// This does not count duplicate proposals received multiple times from the same node.
// It's more like the number of unique replicas from which we've received a proposal.
func (e *ActiveExecution) NumProposalsReceived() int {
	return e.numLeadProposals + e.numYieldProposals
}

func (e *ActiveExecution) LinkPreviousAttempt(previousAttempt *ActiveExecution) {
	e.previousAttempt = previousAttempt
}

func (e *ActiveExecution) LinkNextAttempt(nextAttempt *ActiveExecution) {
	e.nextAttempt = nextAttempt
}
