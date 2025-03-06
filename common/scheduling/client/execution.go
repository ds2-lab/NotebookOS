package client

import (
	"fmt"
	"github.com/go-viper/mapstructure/v2"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"
)

const (
	defaultTargetReplicaId = int32(-1)
)

var (
	ErrReplyAlreadyRegistered = errors.New("already have response registered from specified replica")
)

// Execution encapsulates the submission of a single 'execute_request' message for a particular kernel.
// We observe the results of the SMR Proposal protocol and take action accordingly, depending upon the results.
// For example, if all replicas of the kernel issue 'YIELD' Proposals, then we will need to perform some sort of
// scheduling action, depending upon what scheduling policy we're using.
//
// Specifically, under 'static' scheduling, we dynamically provision a new replica to handle the request.
// Alternatively, under 'dynamic' scheduling, we migrate existing replicas to another node to handle the request.
type Execution struct {

	// CreatedAt is the time at which this Execution was created.
	CreatedAt time.Time

	// OriginallySentAt is the time at which the "execute_request" message associated with this Execution
	// was actually sent by the Jupyter client. We can only recover this if the client is an instance of our
	// Go-implemented Jupyter client, as those clients embed the unix milliseconds at which the message was
	// created and subsequently sent within the metadata field of the message.
	OriginallySentAt time.Time

	// ReceivedExecuteReplyAt is the time at which the "execute_reply" message that indicated that the execution had
	// finished was received.
	ReceivedExecuteReplyAt time.Time

	// ReceivedSmrLeadTaskAt is the time at which we received the "smr_lead_task" message from the primary replica.
	ReceivedSmrLeadTaskAt time.Time

	// TrainingStartedAt is the time at which the kernel began executing the user-submitted code.
	// TrainingStartedAt time.Time

	// NextAttempt is the Execution attempt that occurred after this one.
	NextAttempt scheduling.Execution

	// PreviousAttempt is the Execution attempt that preceded this one, if this is not the first attempt.
	PreviousAttempt scheduling.Execution

	// Replies is a map of the responses from each replica. Note that replies are only saved if debug mode is enabled.
	Replies hashmap.HashMap[int32, *messaging.JupyterMessage]

	// ActiveReplica is the kernel connected to the replica of the kernel that is actually
	// executing the user-submitted code.
	ActiveReplica scheduling.KernelReplica

	// Proposals is a map from replica ID to what it proposed ('YIELD' or 'LEAD')
	Proposals map[int32]scheduling.Proposal

	// JupyterMessage is the original 'execute_request' message.
	JupyterMessage *messaging.JupyterMessage

	// ExecuteRequestMessageId is the  Jupyter message ID of the associated Jupyter "execute_request" ZMQ message.
	ExecuteRequestMessageId string

	// SessionId is the ID of the Jupyter session that initiated the request.
	SessionId string

	// KernelId is the ID of the associated kernel.
	KernelId string

	// WorkloadId can be retrieved from the metadata dictionary of the Jupyter messages if the sender
	// was a Golang Jupyter client.
	WorkloadId string

	State State

	// AttemptNumber begins at 1, identifies the "attempt number", in case we have to retry due to timeouts.
	AttemptNumber int

	// NumReplicas is the number of replicas that the kernel had with the execution request was originally received.
	NumReplicas int

	// MigrationWasRequired indicates whether a migration was required in order to serve this training.
	MigrationWasRequired bool

	// NumViableReplicas is the number of replicas that were viable to serve this training request.
	NumViableReplicas int

	// NumLeadProposals is the number of 'LEAD' Proposals issued.
	NumLeadProposals int

	// NumYieldProposals is the number of 'YIELD' Proposals issued.
	NumYieldProposals int

	// replyMutex ensures atomicity of the RegisterReply method.
	replyMutex sync.Mutex

	// ExecutionIndex uniquely identifies this Execution and enables a total ordering between all Execution structs.
	ExecutionIndex int32

	// targetReplicaId is the replica that was explicitly selected as the primary replica by the Cluster Gateway.
	// The targetReplicaId is not always set. A value of defaultTargetReplicaId indicates that there was no single
	// target replica.
	targetReplicaId atomic.Int32

	originallySentAtDecoded bool

	workloadIdSet bool
}

func NewExecution(kernelId string, attemptId int, numReplicas int, executionIndex int32, msg *messaging.JupyterMessage) *Execution {
	activeExecution := &Execution{
		SessionId:               msg.JupyterSession(),
		AttemptNumber:           attemptId,
		Proposals:               make(map[int32]scheduling.Proposal, numReplicas),
		KernelId:                kernelId,
		NumReplicas:             numReplicas,
		Replies:                 hashmap.NewCornelkMap[int32, *messaging.JupyterMessage](numReplicas),
		NextAttempt:             nil,
		PreviousAttempt:         nil,
		JupyterMessage:          msg,
		ExecuteRequestMessageId: msg.JupyterMessageId(),
		originallySentAtDecoded: false,
		CreatedAt:               time.Now(),
		State:                   Pending,
		ExecutionIndex:          executionIndex,
	}

	activeExecution.targetReplicaId.Store(defaultTargetReplicaId)

	var metadataDict map[string]interface{}
	err := msg.JupyterFrames.DecodeMetadata(&metadataDict)
	if err == nil {
		// Attempt to decode it this way.
		var requestMetadata *messaging.ExecuteRequestMetadata
		err = mapstructure.Decode(metadataDict, &requestMetadata)
		if err == nil {
			if requestMetadata.SentAtUnixTimestamp != nil {
				activeExecution.OriginallySentAt = time.UnixMilli(int64(*requestMetadata.SentAtUnixTimestamp))
				activeExecution.originallySentAtDecoded = true
			}

			if requestMetadata.WorkloadId != nil {
				workloadId := *requestMetadata.WorkloadId
				activeExecution.WorkloadId = workloadId
				activeExecution.workloadIdSet = true
			}
		} else {
			// Fallback if the mapstructure way is broken.
			log.Printf(utils.OrangeStyle.Render("[WARNING] Failed to decode request metadata via mapstructure: %v\n"), err)

			sentAtVal, ok := metadataDict["send_timestamp_unix_milli"]
			if ok {
				unixTimestamp := sentAtVal.(float64)
				activeExecution.OriginallySentAt = time.UnixMilli(int64(unixTimestamp))
				activeExecution.originallySentAtDecoded = true
			}

			workloadIdVal, ok := metadataDict["workload_id"]
			if ok {
				workloadId := workloadIdVal.(string)
				activeExecution.WorkloadId = workloadId
				activeExecution.workloadIdSet = true
			}
		}
	}

	return activeExecution
}

// GetExecutionIndex returns the ExecutionIndex of the target Execution.
func (e *Execution) GetExecutionIndex() int32 {
	return e.ExecutionIndex
}

func (e *Execution) LinkPreviousAttempt(previousAttempt scheduling.Execution) {
	e.PreviousAttempt = previousAttempt
}

func (e *Execution) LinkNextAttempt(nextAttempt scheduling.Execution) {
	e.NextAttempt = nextAttempt
}

func (e *Execution) GetAttemptNumber() int {
	return e.AttemptNumber
}

// RegisterReply saves an "execute_reply" *messaging.JupyterMessage from one of the replicas of the kernel
// associated with the target Execution.
//
// NOTE: Replies are only saved if debug mode is enabled.
//
// This will return an error if the given *messaging.JupyterMessage is not of type "execute_request".
//
// This will return an error if the 'overwrite' parameter is false, and we've already registered a response
// from the specified kernel replica. (The replica is specified via the 'replicaId' parameter.)
//
// Even if 'overwrite' is specified as true, RegisterReply will not overwrite an existing messaging.ShellExecuteReply
// message with a new messaging.MessageTypeExecutionStatistics message.
//
// This method is thread safe.
func (e *Execution) RegisterReply(replicaId int32, response *messaging.JupyterMessage, overwrite bool) error {
	e.replyMutex.Lock()
	defer e.replyMutex.Unlock()

	if response.JupyterMessageType() != messaging.ShellExecuteReply && response.JupyterMessageType() != messaging.MessageTypeExecutionStatistics {
		return fmt.Errorf("illegal Jupyter message type of response: \"%s\"", messaging.ShellExecuteReply)
	}

	// If overwrite is false, then we return an error if we already have a response registered for the specified replica.
	existing, loaded := e.Replies.LoadOrStore(replicaId, response)

	if loaded && !overwrite {
		return fmt.Errorf("%w: \"%s\" message from replica %d of kernel \"%s\"",
			ErrReplyAlreadyRegistered, existing.JupyterMessageType(), replicaId, e.KernelId)
	}

	// If we're attempting to overwrite an "execute_reply" with an "execute_statistics" message,
	// then we'll leave the "execute_reply" and not overwrite it -- even if overwrite is
	// specified as 'true'.
	if loaded && response.JupyterMessageType() == messaging.MessageTypeExecutionStatistics {
		return nil
	}

	// This will overwrite the existing value if there is one.
	e.Replies.Store(replicaId, response)

	return nil
}

// HasValidWorkloadId returns true if we were able to extract the associated workload ID from the metadata
// of the "execute_request" message that submitted the code associated with this Execution struct.
func (e *Execution) HasValidWorkloadId() bool {
	return e.workloadIdSet
}

// HasValidOriginalSentTimestamp returns true if we were able to decode the timestamp at which the
// associated "execute_request" message was sent when we first created the Execution struct.
func (e *Execution) HasValidOriginalSentTimestamp() bool {
	return e.originallySentAtDecoded
}

// OriginalTimestampOrCreatedAt returns the original timestamp at which the associated "execute_request" message
// was sent, if this Execution has that information. If that information is presently unavailable, then
// OriginalTimestampOrCreatedAt will simply return the timestamp at which this Execution struct was created.
func (e *Execution) OriginalTimestampOrCreatedAt() time.Time {
	if e.HasValidOriginalSentTimestamp() {
		return e.OriginallySentAt
	} else {
		return e.CreatedAt
	}
}

func (e *Execution) Msg() *messaging.JupyterMessage {
	return e.JupyterMessage
}

func (e *Execution) HasExecuted() bool {
	return e.IsCompleted()
}

func (e *Execution) SetExecuted(receivedExecuteReplyAt time.Time) {
	e.State = Completed
	e.ReceivedExecuteReplyAt = receivedExecuteReplyAt
}

func (e *Execution) String() string {
	return fmt.Sprintf("Execution[ExecuteRequestMsgId=%s,kernel=%s,Session=%s,TransactionState=%s,Attempt=%d,NumReplicas=%d,"+
		"NumLeadProposals=%d,NumYieldProposals=%d,HasNextAttempt=%v,HasPrevAttempt=%v,OriginalSendTimestamp=%v,"+
		"CreatedAtTimestamp=%v]",
		e.ExecuteRequestMessageId, e.KernelId, e.SessionId, e.State.String(), e.AttemptNumber, e.NumReplicas,
		e.NumLeadProposals, e.NumYieldProposals, e.NextAttempt == nil, e.PreviousAttempt == nil,
		e.OriginallySentAt, e.CreatedAt)
}

// ReceivedSmrLeadTaskMessage records that the specified kernel replica was selected as the primary replica
// and will be executing the code.
func (e *Execution) ReceivedSmrLeadTaskMessage(replica scheduling.KernelReplica, receivedAt time.Time) error {
	// TODO: This part isn't necessarily accurate, as there may have just been a vote proposal.
	if _, ok := e.Proposals[replica.ReplicaID()]; ok {
		return ErrProposalAlreadyReceived
	}
	e.Proposals[replica.ReplicaID()] = NewProposal(scheduling.LeadProposal, "")
	e.NumLeadProposals += 1

	e.ReceivedSmrLeadTaskAt = receivedAt
	e.ActiveReplica = replica

	return nil
}

// ReceivedYieldNotification records that the specified replica ultimately yielded and did not lead the election.
func (e *Execution) ReceivedYieldNotification(smrNodeId int32, yieldReason string) error {
	if _, ok := e.Proposals[smrNodeId]; ok {
		return ErrProposalAlreadyReceived
	}

	e.Proposals[smrNodeId] = NewProposal(scheduling.YieldProposal, yieldReason)
	e.NumYieldProposals += 1

	if e.NumYieldProposals == e.NumReplicas {
		return ErrExecutionFailedAllYielded
	}

	return nil
}

// NumRolesReceived does not count duplicate Proposals received multiple times from the same node.
// It's more like the number of unique replicas from which we've received a Proposal.
func (e *Execution) NumRolesReceived() int {
	return e.NumLeadProposals + e.NumYieldProposals
}

// NumLeadReceived returns the number of 'lead' notifications received.
func (e *Execution) NumLeadReceived() int {
	return e.NumLeadProposals
}

// NumYieldReceived returns the number of 'yield' notifications received.
func (e *Execution) NumYieldReceived() int {
	return e.NumYieldProposals
}

func (e *Execution) RangeRoles(rangeFunc func(int32, scheduling.Proposal) bool) {
	for smrNodeId, role := range e.Proposals {
		shouldContinue := rangeFunc(smrNodeId, role)

		if !shouldContinue {
			return
		}
	}
}

func (e *Execution) IsRunning() bool {
	return e.State == Running
}

func (e *Execution) IsPending() bool {
	return e.State == Pending
}

func (e *Execution) IsCompleted() bool {
	return e.State == Completed
}

func (e *Execution) IsErred() bool {
	return e.State == Erred
}

func (e *Execution) GetNumReplicas() int {
	return e.NumReplicas
}

func (e *Execution) SetActiveReplica(replica scheduling.KernelReplica) {
	e.ActiveReplica = replica
}

func (e *Execution) GetOriginallySentAtTime() time.Time {
	return e.OriginallySentAt
}

func (e *Execution) GetWorkloadId() string {
	return e.WorkloadId
}

func (e *Execution) GetExecuteRequestMessageId() string {
	return e.ExecuteRequestMessageId
}

func (e *Execution) GetTargetReplicaId() int32 {
	return e.targetReplicaId.Load()
}

// SetTargetReplica is used to record the expected target replica for an execution.
func (e *Execution) SetTargetReplica(replicaId int32) error {
	if !e.targetReplicaId.CompareAndSwap(defaultTargetReplicaId, replicaId) {
		return fmt.Errorf("%w: %d", ErrTargetReplicaAlreadySpecified, e.targetReplicaId.Load())
	}

	return nil
}

// GetTrainingStartedAt returns the time at which the training began, as indicated in the payload of the
// "smr_lead_task" message that we received from the primary replica.
//func (e *Execution) GetTrainingStartedAt() time.Time {
//	return e.TrainingStartedAt
//}

// GetReceivedSmrLeadTaskAt returns the time at which we received a "smr_lead_task" message from the primary replica.
func (e *Execution) GetReceivedSmrLeadTaskAt() time.Time {
	return e.ReceivedSmrLeadTaskAt
}

// GetReceivedExecuteReplyAt returns the time at which the "execute_reply" message that indicated that the execution
// had finished was received.
func (e *Execution) GetReceivedExecuteReplyAt() time.Time {
	return e.ReceivedExecuteReplyAt
}

func (e *Execution) SetMigrationRequired(required bool) {
	e.MigrationWasRequired = required
}

// GetMigrationRequired returns a bool indicating whether a migration was required in order to serve this training.
func (e *Execution) GetMigrationRequired() bool {
	return e.MigrationWasRequired
}

func (e *Execution) SetNumViableReplicas(n int) {
	e.NumViableReplicas = n
}

// GetNumViableReplicas returns the number of replicas that were viable to serve this training request.
func (e *Execution) GetNumViableReplicas() int {
	return e.NumViableReplicas
}
