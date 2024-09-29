package scheduling

import (
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"time"

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

type ActiveExecutionQueue []*ActiveExecution

// Enqueue adds an element to the end of the queue
func (q *ActiveExecutionQueue) Enqueue(item *ActiveExecution) {
	*q = append(*q, item)
}

// Dequeue removes and returns the element from the front of the queue
func (q *ActiveExecutionQueue) Dequeue() *ActiveExecution {
	if len(*q) == 0 {
		return nil // Queue is empty
	}
	item := (*q)[0]
	*q = (*q)[1:]
	return item
}

// ActiveExecution encapsulates the submission of a single 'execute_request' message for a particular kernel.
// We observe the results of the SMR proposal protocol and take action accordingly, depending upon the results.
// For example, if all replicas of the kernel issue 'YIELD' proposals, then we will need to perform some sort of
// scheduling action, depending upon what scheduling policy we're using.
//
// Specifically, under 'static' scheduling, we dynamically provision a new replica to handle the request.
// Alternatively, under 'dynamic' scheduling, we migrate existing replicas to another node to handle the request.
type ActiveExecution struct {
	ExecutionId             string // Unique ID identifying the execution request.
	AttemptId               int    // Beginning at 1, identifies the "attempt number", in case we have to retry due to timeouts.
	SessionId               string // The ID of the Jupyter session that initiated the request.
	KernelId                string // ID of the associated kernel.
	ExecuteRequestMessageId string // The Jupyter message ID of the associated Jupyter "execute_request" ZMQ message.

	NumReplicas int // The number of replicas that the kernel had with the execution request was originally received.

	numLeadProposals  int // Number of 'LEAD' proposals issued.
	numYieldProposals int // Number of 'YIELD' proposals issued.

	// originallySentAt is the time at which the "execute_request" message associated with this ActiveExecution
	// was actually sent by the Jupyter client. We can only recover this if the client is an instance of our
	// Go-implemented Jupyter client, as those clients embed the unix milliseconds at which the message was
	// created and subsequently sent within the metadata field of the message.
	originallySentAt        time.Time
	originallySentAtDecoded bool

	// activeReplica is the KernelReplicaClient connected to the replica of the kernel that is actually
	// executing the user-submitted code.
	ActiveReplica KernelReplica

	// WorkloadId can be retrieved from the metadata dictionary of the Jupyter messages if the sender
	// was a Golang Jupyter client.
	WorkloadId    string
	workloadIdSet bool

	proposals map[int32]string // Map from replica ID to what it proposed ('YIELD' or 'LEAD')

	nextAttempt     *ActiveExecution // If we initiate a retry due to timeouts, then we link this attempt to the retry attempt.
	previousAttempt *ActiveExecution // The retry that preceded this one, if this is not the first attempt.

	msg *types.JupyterMessage // The original 'execute_request' message.

	executed bool
}

func NewActiveExecution(kernelId string, attemptId int, numReplicas int, msg *types.JupyterMessage) *ActiveExecution {
	activeExecution := &ActiveExecution{
		ExecutionId:             uuid.NewString(),
		SessionId:               msg.JupyterSession(),
		AttemptId:               attemptId,
		proposals:               make(map[int32]string, 3),
		KernelId:                kernelId,
		NumReplicas:             numReplicas,
		nextAttempt:             nil,
		previousAttempt:         nil,
		msg:                     msg,
		ExecuteRequestMessageId: msg.JupyterMessageId(),
		originallySentAtDecoded: false,
	}

	metadata, err := msg.DecodeMetadata()
	if err == nil {
		sentAtVal, ok := metadata["send-timestamp-unix-milli"]
		if ok {
			unixTimestamp := sentAtVal.(float64)
			activeExecution.originallySentAt = time.UnixMilli(int64(unixTimestamp))
			activeExecution.originallySentAtDecoded = true
		}

		workloadIdVal, ok := metadata["workload_id"]
		if ok {
			workloadId := workloadIdVal.(string)
			activeExecution.WorkloadId = workloadId
			activeExecution.workloadIdSet = true
		}
	}

	return activeExecution
}

// HasValidWorkloadId returns true if we were able to extract the associated workload ID from the metadata
// of the "execute_request" message that submitted the code associated with this ActiveExecution struct.
func (e *ActiveExecution) HasValidWorkloadId() bool {
	return e.workloadIdSet
}

// HasValidOriginalSentTimestamp returns true if we were able to decode the timestamp at which the
// associated "execute_request" message was sent when we first created the ActiveExecution struct.
func (e *ActiveExecution) HasValidOriginalSentTimestamp() bool {
	return e.originallySentAtDecoded
}

// OriginalSentTimestamp returns the time at which the associated "execute_request" message was sent
// by the Jupyter client that initiated the execution request. If we were able to decode/retrieve this
// value when we first created the ActiveExecution struct, then the value returned by OriginalSentTimestamp
// will be meaningless.
//
// To check if we were able to decode/retrieve the "send timestamp", use the HasValidOriginalSentTimestamp method.
// If the "sent at" timestamp is "invalid", then the OriginalSentTimestamp method simply returns the default
// value of a time.Time struct.
func (e *ActiveExecution) OriginalSentTimestamp() time.Time {
	return e.originallySentAt
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

func (e *ActiveExecution) String() string {
	return fmt.Sprintf("ActiveExecution[ID=%s,Kernel=%s,Session=%s,Attempt=%d,NumReplicas=%d,numLeadProposals=%d,numYieldProposals=%d,HasNextAttempt=%v,HasPrevAttempt=%v]", e.ExecutionId, e.KernelId, e.SessionId, e.AttemptId, e.NumReplicas, e.numLeadProposals, e.numYieldProposals, e.nextAttempt == nil, e.previousAttempt == nil)
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

	if e.numYieldProposals == e.NumReplicas {
		return ErrExecutionFailedAllYielded
	}

	return nil
}

// NumProposalsReceived does not count duplicate proposals received multiple times from the same node.
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
