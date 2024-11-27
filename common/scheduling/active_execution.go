package scheduling

import (
	"errors"
	"fmt"
	"github.com/go-viper/mapstructure/v2"
	"github.com/goccy/go-json"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"log"
	"sync"
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
	ErrProposalAlreadyReceived = errors.New("we already received a proposal from that replica")
)

type proposal struct {
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

func newProposal(k string, r string) *proposal {
	return &proposal{
		Key:    k,
		Reason: r,
	}
}

func (p *proposal) GetKey() string {
	return p.Key
}

func (p *proposal) GetReason() string {
	return p.Reason
}

func (p *proposal) IsYield() bool {
	return p.Key == KeyYield
}

func (p *proposal) IsLead() bool {
	return p.Key == KeyLead
}

func (p *proposal) String() string {
	m, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	return string(m)
}

type ElectionProposal interface {
	GetKey() string
	GetReason() string
	IsYield() bool
	IsLead() bool
}

// ActiveExecution encapsulates the submission of a single 'execute_request' message for a particular kernel.
// We observe the results of the SMR proposal protocol and take action accordingly, depending upon the results.
// For example, if all replicas of the kernel issue 'YIELD' roles, then we will need to perform some sort of
// scheduling action, depending upon what scheduling policy we're using.
//
// Specifically, under 'static' scheduling, we dynamically provision a new replica to handle the request.
// Alternatively, under 'dynamic' scheduling, we migrate existing replicas to another node to handle the request.
type ActiveExecution struct {
	ExecutionId             string                                            // Unique ID identifying the execution request.
	AttemptId               int                                               // Beginning at 1, identifies the "attempt number", in case we have to retry due to timeouts.
	SessionId               string                                            // The ID of the Jupyter session that initiated the request.
	KernelId                string                                            // ID of the associated kernel.
	ExecuteRequestMessageId string                                            // The Jupyter message ID of the associated Jupyter "execute_request" ZMQ message.
	CreatedAt               time.Time                                         // The time at which this ActiveExecution was created.
	NumReplicas             int                                               // The number of replicas that the kernel had with the execution request was originally received.
	numLeadRoles            int                                               // Number of 'LEAD' roles issued.
	numYieldRoles           int                                               // Number of 'YIELD' roles issued.
	roles                   map[int32]ElectionProposal                        // Map from replica ID to what it proposed ('YIELD' or 'LEAD')
	nextAttempt             *ActiveExecution                                  // If we initiate a retry due to timeouts, then we link this attempt to the retry attempt.
	previousAttempt         *ActiveExecution                                  // The retry that preceded this one, if this is not the first attempt.
	msg                     *messaging.JupyterMessage                         // The original 'execute_request' message.
	Replies                 hashmap.HashMap[int32, *messaging.JupyterMessage] // The responses from each replica. Note that replies are only saved if debug mode is enabled.
	replyMutex              sync.Mutex                                        // Ensures atomicity of the RegisterReply method.

	// originallySentAt is the time at which the "execute_request" message associated with this ActiveExecution
	// was actually sent by the Jupyter client. We can only recover this if the client is an instance of our
	// Go-implemented Jupyter client, as those clients embed the unix milliseconds at which the message was
	// created and subsequently sent within the metadata field of the message.
	originallySentAt        time.Time
	originallySentAtDecoded bool

	// activeReplica is the Kernel connected to the replica of the kernel that is actually
	// executing the user-submitted code.
	ActiveReplica KernelReplica

	// WorkloadId can be retrieved from the metadata dictionary of the Jupyter messages if the sender
	// was a Golang Jupyter client.
	WorkloadId    string
	workloadIdSet bool

	executed bool
}

func (e *ActiveExecution) LinkPreviousAttempt(previousAttempt *ActiveExecution) {
	e.previousAttempt = previousAttempt
}

func (e *ActiveExecution) LinkNextAttempt(nextAttempt *ActiveExecution) {
	e.nextAttempt = nextAttempt
}

func (e *ActiveExecution) GetExecutionId() string {
	return e.ExecutionId
}
func (e *ActiveExecution) GetNumReplicas() int {
	return e.NumReplicas
}

func (e *ActiveExecution) SetActiveReplica(replica KernelReplica) {
	e.ActiveReplica = replica
}

func (e *ActiveExecution) GetActiveReplica() KernelReplica {
	return e.ActiveReplica
}

func (e *ActiveExecution) GetExecuteRequestMessageId() string {
	return e.ExecuteRequestMessageId
}

func (e *ActiveExecution) GetAttemptId() int {
	return e.AttemptId
}

func (e *ActiveExecution) GetWorkloadId() string {
	return e.WorkloadId
}

func NewActiveExecution(kernelId string, attemptId int, numReplicas int, msg *messaging.JupyterMessage) *ActiveExecution {
	activeExecution := &ActiveExecution{
		ExecutionId:             uuid.NewString(),
		SessionId:               msg.JupyterSession(),
		AttemptId:               attemptId,
		roles:                   make(map[int32]ElectionProposal, 3),
		KernelId:                kernelId,
		NumReplicas:             numReplicas,
		Replies:                 hashmap.NewCornelkMap[int32, *messaging.JupyterMessage](numReplicas),
		nextAttempt:             nil,
		previousAttempt:         nil,
		msg:                     msg,
		ExecuteRequestMessageId: msg.JupyterMessageId(),
		originallySentAtDecoded: false,
		CreatedAt:               time.Now(),
	}

	var metadataDict map[string]interface{}
	err := msg.JupyterFrames.DecodeMetadata(&metadataDict)
	if err == nil {
		// Attempt to decode it this way.
		var requestMetadata *messaging.ExecuteRequestMetadata
		err = mapstructure.Decode(metadataDict, &requestMetadata)
		if err == nil {
			if requestMetadata.SentAtUnixTimestamp != nil {
				activeExecution.originallySentAt = time.UnixMilli(int64(*requestMetadata.SentAtUnixTimestamp))
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
				activeExecution.originallySentAt = time.UnixMilli(int64(unixTimestamp))
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

	//metadata, err := msg.DecodeMetadata()
	//if err == nil {
	//	sentAtVal, ok := metadata["send_timestamp_unix_milli"]
	//	if ok {
	//		unixTimestamp := sentAtVal.(float64)
	//		activeExecution.originallySentAt = time.UnixMilli(int64(unixTimestamp))
	//		activeExecution.originallySentAtDecoded = true
	//	}
	//
	//	workloadIdVal, ok := metadata["workload_id"]
	//	if ok {
	//		workloadId := workloadIdVal.(string)
	//		activeExecution.WorkloadId = workloadId
	//		activeExecution.workloadIdSet = true
	//	}
	//}

	return activeExecution
}

// RegisterReply saves an "execute_reply" *messaging.JupyterMessage from one of the replicas of the kernel
// associated with the target ActiveExecution.
//
// NOTE: Replies are only saved if debug mode is enabled.
//
// This will return an error if the given *messaging.JupyterMessage is not of type "execute_request".
//
// This will return an error if the 'overwrite' parameter is false, and we've already registered a response
// from the specified kernel replica. (The replica is specified via the 'replicaId' parameter.)
//
// This method is thread safe.
func (e *ActiveExecution) RegisterReply(replicaId int32, response *messaging.JupyterMessage, overwrite bool) error {
	e.replyMutex.Lock()
	defer e.replyMutex.Unlock()

	if response.JupyterMessageType() != messaging.ShellExecuteReply {
		return fmt.Errorf("illegal Jupyter message type of response: \"%s\"", messaging.ShellExecuteReply)
	}

	// If overwrite is false, then we return an error if we already have a response registered for the specified replica.
	if _, loaded := e.Replies.LoadOrStore(replicaId, response); loaded && !overwrite {
		return fmt.Errorf("already have response from replica %d", replicaId)
	}

	// This will overwrite the existing value if there is one.
	e.Replies.Store(replicaId, response)

	return nil
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

// OriginalTimestampOrCreatedAt returns the original timestamp at which the associated "execute_request" message
// was sent, if this ActiveExecution has that information. If that information is presently unavailable, then
// OriginalTimestampOrCreatedAt will simply return the timestamp at which this ActiveExecution struct was created.
func (e *ActiveExecution) OriginalTimestampOrCreatedAt() time.Time {
	if e.HasValidOriginalSentTimestamp() {
		return e.originallySentAt
	} else {
		return e.CreatedAt
	}
}

func (e *ActiveExecution) Msg() *messaging.JupyterMessage {
	return e.msg
}

func (e *ActiveExecution) HasExecuted() bool {
	return e.executed
}

func (e *ActiveExecution) SetExecuted() {
	e.executed = true
}

func (e *ActiveExecution) String() string {
	return fmt.Sprintf("ActiveExecution[ID=%s,Kernel=%s,Session=%s,ExecuteRequestMsgId=%s,Attempt=%d,NumReplicas=%d,"+
		"numLeadRoles=%d,numYieldRoles=%d,HasNextAttempt=%v,HasPrevAttempt=%v,OriginalSendTimestamp=%v,CreatedAtTimestamp=%v,Executed=%v]",
		e.ExecutionId, e.KernelId, e.SessionId, e.ExecuteRequestMessageId, e.AttemptId, e.NumReplicas, e.numLeadRoles, e.numYieldRoles, e.nextAttempt == nil, e.previousAttempt == nil, e.originallySentAt, e.CreatedAt, e.executed)
}

// ReceivedLeadNotification records that the specified kernel replica lead the election and executed the code.
func (e *ActiveExecution) ReceivedLeadNotification(smrNodeId int32) error {
	if _, ok := e.roles[smrNodeId]; ok {
		return ErrProposalAlreadyReceived
	}

	e.roles[smrNodeId] = newProposal(KeyLead, "")
	e.numLeadRoles += 1

	return nil
}

// ReceivedYieldNotification records that the specified replica ultimately yielded and did not lead the election.
func (e *ActiveExecution) ReceivedYieldNotification(smrNodeId int32, yieldReason string) error {
	if _, ok := e.roles[smrNodeId]; ok {
		return ErrProposalAlreadyReceived
	}

	e.roles[smrNodeId] = newProposal(KeyYield, yieldReason)
	e.numYieldRoles += 1

	if e.numYieldRoles == e.NumReplicas {
		return ErrExecutionFailedAllYielded
	}

	return nil
}

// NumRolesReceived does not count duplicate roles received multiple times from the same node.
// It's more like the number of unique replicas from which we've received a proposal.
func (e *ActiveExecution) NumRolesReceived() int {
	return e.numLeadRoles + e.numYieldRoles
}

func (e *ActiveExecution) RangeRoles(rangeFunc func(int32, ElectionProposal) bool) {
	for smrNodeId, role := range e.roles {
		shouldContinue := rangeFunc(smrNodeId, role)

		if !shouldContinue {
			return
		}
	}
}

//func (e *ActiveExecution) LinkPreviousAttempt(previousAttempt *ActiveExecution) {
//	e.previousAttempt = previousAttempt
//}
//
//func (e *ActiveExecution) LinkNextAttempt(nextAttempt *ActiveExecution) {
//	e.nextAttempt = nextAttempt
//}
