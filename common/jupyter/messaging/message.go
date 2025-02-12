package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/utils"
)

const (
	MessageHeaderDefaultUsername = "username"

	IOStatusMessage              = "status"
	ControlResetKernelRequest    = "reset_kernel_request"
	ControlResetKernelReply      = "reset_kernel_reply"
	ShellExecuteRequest          = "execute_request"
	ShellExecuteReply            = "execute_reply"
	ShellYieldRequest            = "yield_request"
	ShellShutdownRequest         = "shutdown_request"
	ControlPromotePrewarmRequest = "promote_prewarm_request"
	ControlPromotePrewarmReply   = "promote_prewarm_reply"
	KernelInfoRequest            = "kernel_info_request"
	KernelInfoReply              = "kernel_info_reply"
	MessageTypeShutdownRequest   = "shutdown_request"
	MessageTypeShutdownReply     = "shutdown_reply"

	ErrorNotification   NotificationType = 0
	WarningNotification NotificationType = 1
	InfoNotification    NotificationType = 2
	SuccessNotification NotificationType = 3

	JavascriptISOString = "2006-01-02T15:04:05.999Z07:00"

	MessageTypeACK = "ACK"
)

var (
	ErrInvalidJupyterMessage       = fmt.Errorf("invalid jupyter message")
	ErrRequestAlreadyCompleted     = fmt.Errorf("the request cannot be cancelled as it has already been completed")
	ErrNotSupportedSignatureScheme = fmt.Errorf("not supported signature scheme")
	ErrInvalidJupyterSignature     = fmt.Errorf("invalid jupyter signature")
)

type JupyterMessageType string

func (t JupyterMessageType) String() string {
	return string(t)
}

// FramesToString returns a string of the given frames.
func FramesToString(frames [][]byte) string {
	if len(frames) == 0 {
		return "[]"
	}

	s := "["
	for i, frame := range frames {
		s += "\"" + string(frame) + "\""

		if i+1 < len(frames) {
			s += ", "
		}
	}

	s += "]"

	return s
}

// GetBaseMessageType returns the base portion of the Jupyter message type.
// The "base part" is best defined through an example:
//
// If the message type is "execute_request", then this returns "execute_" and true.
//
// If the message type is not of the form "{action}_request" or "{action}_reply", then this
// returns the empty string and false.
func (t JupyterMessageType) GetBaseMessageType() (string, bool) {
	if strings.HasSuffix(t.String(), "request") {
		return t.String()[0 : len(t.String())-7], true
	} else if strings.HasSuffix(t.String(), "reply") {
		return t.String()[0 : len(t.String())-5], true
	}

	return "", false
}

type NotificationType int32

// Int32 returns the NotificationType as an int32.
func (nt NotificationType) Int32() int32 {
	return int32(nt)
}

// Message represents an entire message in a high-level structure.
type Message struct {
	Content      interface{}            `json:"content"`
	Metadata     map[string]interface{} `json:"metadata"`
	Header       MessageHeader          `json:"header"`
	ParentHeader MessageHeader          `json:"parent_header"`
}

func (msg *Message) String() string {
	m, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// MessageHeader is a Jupyter message header.
// http://jupyter-client.readthedocs.io/en/latest/messaging.html#general-message-format
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type MessageHeader struct {
	MsgID    string             `json:"msg_id"`
	Username string             `json:"username"`
	Session  string             `json:"session"`
	Date     string             `json:"date"`
	MsgType  JupyterMessageType `json:"msg_type"`
	Version  string             `json:"version"`
}

func (header *MessageHeader) Clone() *MessageHeader {
	return &MessageHeader{
		MsgID:    header.MsgID,
		Username: header.Username,
		Session:  header.Session,
		Date:     header.Date,
		MsgType:  header.MsgType,
		Version:  header.Version,
	}
}

func (header *MessageHeader) String() string {
	m, err := json.Marshal(header)
	if err != nil {
		panic(err)
	}

	return string(m)
}

type MessageKernelStatus struct {
	Status string `json:"execution_state"`
}

const (
	MessageKernelStatusIdle     = "idle"
	MessageKernelStatusBusy     = "busy"
	MessageKernelStatusStarting = "starting"
)

// MessageErrorWithOldContent is similar to MessageError; however, MessageErrorWithOldContent is used
// when we're replacing the content of a Jupyter message with a custom error message.
//
// MessageErrorWithOldContent has an additional field (relative to MessageError) in which the original content
// of the Jupyter message is stored.
type MessageErrorWithOldContent struct {
	*MessageError

	// OriginalContent contains the original content of the Jupyter message whose content is being replaced
	// with a custom error message.
	OriginalContent map[string]interface{} `json:"original_content"`
}

func (m *MessageErrorWithOldContent) String() string {
	out, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(out)
}

// MessageErrorWithYieldReason is a wrapper around MessageError with an additional YieldReason field, in case
// the error is an 'execution yielded' error, and the replica that encountered this error was explicitly instructed
// to yield, and a reason was provided.
type MessageErrorWithYieldReason struct {
	*MessageError

	YieldReason string `json:"yield-reason"`
}

func (m *MessageErrorWithYieldReason) String() string {
	out, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(out)
}

type MessageError struct {
	Status   string `json:"status"`
	ErrName  string `json:"ename"`
	ErrValue string `json:"evalue"`
}

func (m *MessageError) String() string {
	out, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(out)
}

const (
	MessageStatusOK          = "ok"
	MessageStatusError       = "error"
	MessageErrYieldExecution = "ExecutionYieldError"
)

var (
	ErrExecutionYielded = errors.New("kernel replica failed to lead the execution")
)

type MessageShutdownRequest struct {
	Restart bool `json:"restart"`
}

type ZmqMessage interface {
	GetMsg() *zmq4.Msg
}

func extractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int) {
	_, jOffset = SkipIdentitiesFrame(frames)

	if jOffset > 0 {
		matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))

		if len(matches) > 0 {
			destID = matches[1]
			reqID = matches[2]
		}
	}
	return
}

// CopyRequestTraceFromBuffersToMetadata will attempt to extract a proto.RequestTrace from the (first) buffers frame
// of the given JupyterMessage. If successful, then CopyRequestTraceFromBuffersToMetadata will next attempt to
// add the proto.RequestTrace to the metadata frame of the JupyterMessage.
//
// Returns true on success. Returns false on failure.
func CopyRequestTraceFromBuffersToMetadata(msg *JupyterMessage, signatureScheme string, key string, logger logger.Logger) bool {
	if msg.JupyterFrames.LenWithoutIdentitiesFrame(true) <= JupyterFrameRequestTrace {
		logger.Warn("Jupyter \"%s\" request has just %d frames (after skipping identities frame). Cannot extract RequestTrace.",
			msg.JupyterMessageType(), msg.JupyterFrames.LenWithoutIdentitiesFrame(false))
		return false
	}

	_, requestTrace, err := extractRequestTraceFromJupyterMessage(msg, logger)
	if err != nil {
		logger.Warn("Failed to extract RequestTrace from \"%s\" message \"%s\" (JupyterID=\"%s\"). "+
			"Cannot copy RequestTrace to metadata.",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		return false
	}

	logger.Debug("Successfully extracted RequestTrace from first buffers frame of \"%s\" request \"%s\" (JupyterID=\"%s\") "+
		"Copying to metadata now.", msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())

	metadataDict, err := msg.DecodeMetadata()
	if err != nil {
		logger.Warn("Failed to decode metadata frame of \"%s\" message \"%s\" (JupyterID=\"%s\"). "+
			"Cannot copy RequestTrace to metadata.",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		metadataDict = make(map[string]interface{}) // Create a new metadata frame, I guess...
	}

	metadataDict[proto.RequestTraceMetadataKey] = requestTrace
	err = msg.EncodeMetadata(metadataDict)
	if err != nil {
		logger.Error("Failed to encode metadata frame of \"%s\" message \"%s\" (JupyterID=\"%s\") after embedding RequestTrace in it: %v",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)
		return false
	}

	// Resign and re-verify the message.
	if signatureScheme == "" {
		logger.Warn("kernel %s's signature scheme is blank. Defaulting to \"%s\"", JupyterSignatureScheme)
		signatureScheme = JupyterSignatureScheme
	}

	// Regenerate the signature.
	if _, err := msg.JupyterFrames.Sign(signatureScheme, []byte(key)); err != nil {
		logger.Error("Failed to sign frames because %v", err)
		return false
	}

	// Ensure that the frames are now correct.
	if err := msg.JupyterFrames.Verify(signatureScheme, []byte(key)); err != nil {
		logger.Error("Failed to verify modified message with signature scheme '%v' and key '%v': %v",
			signatureScheme, key, err)
		return false
	}

	return true
}

// extractRequestTraceFromJupyterMessage will attempt to extract and return a *proto.RequestTrace from the (first)
// buffers frame of the given JupyterMessage.
//
// It is the caller's responsibility to ensure that the given JupyterMessage has a buffers frame.
func extractRequestTraceFromJupyterMessage(msg *JupyterMessage, logger logger.Logger) (*proto.JupyterRequestTraceFrame, *proto.RequestTrace, error) {
	var wrapper *proto.JupyterRequestTraceFrame
	err := json.Unmarshal(msg.JupyterFrames.Frames[msg.JupyterFrames.Offset+JupyterFrameRequestTrace], &wrapper)
	if err != nil {
		// Presumably it just doesn't contain a RequestTrace for some reason. But that would be weird.
		// Could be that a Jupyter client unexpectedly sent some buffers with whatever message.
		// We don't handle this as of right now. To handle it, we would just add a new buffers frame before the
		// existing buffers frame and put the request trace there, and make sure to adjust things accordingly
		// for the client in the response (which may just involve removing the request trace, if the client is
		// expecting something else to be in the first buffers frame).
		logger.Error("Failed to JSON-decode RequestTrace from Frame #%d because: %v", msg.JupyterFrames.Offset+JupyterFrameRequestTrace, err)
		logger.Error("Frame #%d: %s\n", msg.JupyterFrames.Offset+JupyterFrameRequestTrace,
			string(msg.JupyterFrames.Frames[msg.JupyterFrames.Offset+JupyterFrameRequestTrace]))
		logger.Error("Frames: %s\n", msg.MsgToString())
		return nil, nil, err
	}

	requestTrace := wrapper.RequestTrace
	if requestTrace == nil {
		// Weird error.
		return nil, nil, fmt.Errorf("decoded JupyterRequestTraceFrame, but the included RequestTrace is nil")
	} else {
		return wrapper, requestTrace, nil
	}
}

// AddOrUpdateRequestTraceToJupyterMessage will add a RequestTrace to the given messaging.JupyterMessage's metadata frame.
// If there is already a RequestTrace within the messaging.JupyterMessage's metadata frame, then no change is made.
//
// AddOrUpdateRequestTraceToJupyterMessage returns true if a RequestTrace is serialized into the messaging.JupyterMessage's
// metadata frame. If there is already a RequestTrace encoded within the metadata frame, then false is returned.
//
// If there is an error decoding or encoding the metadata frame of the jupyter.JupyterMessage, then an error is
// returned, and the boolean returned along with the error is always false.
func AddOrUpdateRequestTraceToJupyterMessage(msg *JupyterMessage, timestamp time.Time, logger logger.Logger) (*proto.RequestTrace, bool, error) {
	var (
		wrapper      *proto.JupyterRequestTraceFrame
		requestTrace *proto.RequestTrace
		added        bool
		err          error
	)

	//logger.Debug("Attempting to add or update RequestTrace to/in Jupyter %s \"%s\" request.", msg.JupyterMessageType())

	// Check if the message has enough frames to have a RequestTrace in it (i.e., if there are buffers frames or not).
	// If not, then we'll assume that the message does not have a buffers frame/RequestTrace (as there aren't enough
	// frames for that to be the case), and we'll add additional frames and then add a new RequestTrace to the new
	// buffer frame.
	if msg.JupyterFrames.LenWithoutIdentitiesFrame(true) <= JupyterFrameRequestTrace {
		for msg.JupyterFrames.LenWithoutIdentitiesFrame(false) <= JupyterFrameRequestTrace {
			// logger.Debug("Jupyter \"%s\" request has just %d frames (after skipping identities frame). Adding additional frame. Offset: %d. Frames: %s",
			//	msg.JupyterMessageType(), msg.JupyterFrames.LenWithoutIdentitiesFrame(false), msg.Offset(), msg.JupyterFrames.String())

			// If the request doesn't already have a JupyterFrameRequestTrace frame, then we'll add one.
			msg.JupyterFrames.Frames = append(msg.JupyterFrames.Frames, make([]byte, 0))
		}

		// The metadata did not already contain a RequestTrace.
		// Let's first create one.
		requestTrace = proto.NewRequestTrace(msg.JupyterSession(), msg.JupyterMessageType(), msg.JupyterMessageId())
		added = true

		// Create the wrapper/frame itself.
		wrapper = &proto.JupyterRequestTraceFrame{RequestTrace: requestTrace}

		// logger.Debug("Added RequestTrace to Jupyter \"%s\" message.", msg.JupyterMessageType())
	} else {
		// logger.Debug("Extracting Jupyter RequestTrace frame from \"%s\" message (offset=%d): %s", msg.JupyterMessageType(), msg.JupyterFrames.Offset, msg.JupyterFrames.String())

		// The message has at least one buffers frame, so let's try to extract an existing RequestTrace.
		wrapper, requestTrace, err = extractRequestTraceFromJupyterMessage(msg, logger)
		if err != nil {
			// We failed to extract the RequestTrace for some reason.
			return nil, false, err
		}

		// logger.Debug("Extracted existing RequestTrace from Jupyter \"%s\" message.", msg.JupyterMessageType())
	}

	// Update the appropriate timestamp field of the RequestTrace.
	requestTrace.PopulateNextField(timestamp.UnixMilli(), logger)

	// logger.Debug("New/updated RequestTrace: %s.", requestTrace.String())

	marshalledFrame, err := json.Marshal(wrapper)
	if err != nil {
		logger.Error("Failed to JSON-encode RequestTrace because: %v", err)
		return nil, false, err
	}

	msg.JupyterFrames.Frames[msg.JupyterFrames.Offset+JupyterFrameRequestTrace] = marshalledFrame

	// logger.Debug("Updated frames: %s.", msg.JupyterFrames.String())

	msg.RequestTrace = requestTrace

	return requestTrace, added, nil
}

// ElectionLeaderProposalMetadata encodes the fields of a "leader proposal", an object used by the kernel
// replicas when selecting a replica to execute code.
type ElectionLeaderProposalMetadata struct {
	Tag           any    `json:"tag"`
	Key           string `json:"key"`
	Op            string `json:"op"`
	Timestamp     string `json:"timestamp"`
	ID            string `json:"id"`
	Proposer      int    `json:"proposer"`
	ElectionTerm  int    `json:"election_term"`
	AttemptNumber int    `json:"attempt_number"`
	End           bool   `json:"end"`
}

// ElectionVoteProposalMetadata encodes the fields of a "vote proposal", an object used by the kernel
// replicas when selecting a replica to execute code.
type ElectionVoteProposalMetadata struct {
	Tag            any    `json:"tag"`
	Key            string `json:"key"`
	Op             string `json:"op"`
	Timestamp      string `json:"timestamp"`
	ID             string `json:"id"`
	Proposer       int    `json:"proposer"`
	ElectionTerm   int    `json:"election_term"`
	AttemptNumber  int    `json:"attempt_number"`
	ProposedNodeID int    `json:"proposed_node_id"`
	End            bool   `json:"end"`
}

// ElectionMetadata is metadata from the Python Election that took place to determine which
// replica would execute the code. This is only sent on the return (i.e., "execute_reply").
type ElectionMetadata struct {
	Proposals                 map[int]*ElectionLeaderProposalMetadata `json:"proposals" mapstructure:"proposals"`
	VoteProposals             map[int]*ElectionVoteProposalMetadata   `json:"vote_proposals" mapstructure:"vote_proposals"`
	DiscardedProposals        map[int]*ElectionLeaderProposalMetadata `json:"discarded_proposals" mapstructure:"discarded_proposals"`
	CompletionReason          string                                  `json:"completion_reason" mapstructure:"completion_reason"`
	MissingProposals          []int                                   `json:"missing_proposals" mapstructure:"missing_proposals"`
	TermNumber                int                                     `json:"term_number" mapstructure:"term_number"`
	ElectionState             int                                     `json:"election_state" mapstructure:"election_state"`
	ElectionStateString       int                                     `json:"election_state_string" mapstructure:"election_state_string"`
	WinnerID                  int                                     `json:"winner_id" mapstructure:"winner_id"`
	NumDiscardedProposals     int                                     `json:"num_discarded_proposals" mapstructure:"num_discarded_proposals"`
	NumDiscardedVoteProposals int                                     `json:"num_discarded_vote_proposals" mapstructure:"num_discarded_vote_proposals"`
	NumLeadProposalsReceived  int                                     `json:"num_lead_proposals_received" mapstructure:"num_lead_proposals_received"`
	NumYieldProposalsReceived int                                     `json:"num_yield_proposals_received" mapstructure:"num_yield_proposals_received"`
	NumRestarts               int                                     `json:"num_restarts" mapstructure:"num_restarts"`
	CurrentAttemptNumber      int                                     `json:"current_attempt_number" mapstructure:"current_attempt_number"`
	WinnerSelected            bool                                    `json:"winner_selected" mapstructure:"winner_selected"`
}

// ExecuteRequestMetadata includes all the metadata entries we might expect to find in the metadata frame
// of an "execute_request" message.
type ExecuteRequestMetadata struct {

	// ResourceWrapperSnapshot is a snapshot of the resources available on the Local Daemon.
	ResourceWrapperSnapshot types.ArbitraryResourceSnapshot `json:"resource_snapshot" mapstructure:"resource_snapshot,omitempty"`

	// TargetReplicaId is the SMR node ID of the replica of the kernel associated with this message (or more accurately,
	// the kernel associated with the message in which this ExecuteRequestMetadata is contained) that should lead
	// the execution of the code included in the "execute_request".
	TargetReplicaId *int32 `json:"target_replica" mapstructure:"target_replica,omitempty"`

	// WorkloadId is the identifier of the workload in which this code execution is taking place.
	// Workloads are a construct of the workload orchestrator/cluster dashboard.
	WorkloadId *string `json:"workload_id" mapstructure:"workload_id,omitempty"`

	KernelId *string `json:"kernel_id" mapstrcture:"kernel_id,omitempty"`

	// ResourceRequest is an updated types.Spec for the kernel targeted by the containing "execute_request".
	ResourceRequest *types.Float64Spec `json:"resource_request" mapstructure:"resource_request,omitempty"`

	// SentAtUnixTimestamp is the Unix epoch time (milliseconds) at which the "execute_request" message
	// was originally sent by the Jupyter client.
	SentAtUnixTimestamp *float64 `json:"send_timestamp_unix_milli,omitempty" mapstructure:"send_timestamp_unix_milli,omitempty"`

	// ElectionMetadata is metadata from the Python Election that took place to determine which
	// replica would execute the code. This is only sent on the return (i.e., "execute_reply").
	ElectionMetadata *ElectionMetadata `json:"election_metadata" mapstructure:"resource_snapshot,omitempty"`

	// RemoteStorageDefinition defines the remote remote_storage that should be used by the kernel when simulating
	// checkpointing its state.
	RemoteStorageDefinition *proto.RemoteStorageDefinition `json:"remote_storage_definition" mapstructure:"remote_storage_definition"`

	// OtherMetadata contains any other entries in the metadata frame that aren't explicitly listed above.
	// OtherMetadata will only be populated if the metadata frame is decoded using the mapstructure library.
	OtherMetadata map[string]interface{} `mapstructure:",remain"`

	// GpuDeviceIds are the GPU device IDs allocated to the replica.
	GpuDeviceIds []int `json:"gpu_device_ids" mapstructure:"gpu_device_ids"`
}

func (m *ExecuteRequestMetadata) String() string {
	s, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(s)
}

// JupyterMessage is a wrapper around ZMQ4 messages, specifically Jupyter ZMQ4 messages.
// We encode the message ID and message type for convenience.
type JupyterMessage struct {
	// msg is the *zmq4.msg struct that is wrapped by the JupyterMessage.
	msg *zmq4.Msg

	// JupyterFrames is a wrapper around the [][]byte from the *zmq4.msg field.
	// JupyterFrames provides a bunch of helper/utility methods for manipulating the [][]byte.
	JupyterFrames *JupyterFrames

	RequestTraceUpdated *proto.RequestTraceUpdated
	RequestTrace        *proto.RequestTrace

	header       *MessageHeader
	parentHeader *MessageHeader
	metadata     map[string]interface{}

	RequestId     string
	DestinationId string

	// signatureScheme is the signature scheme of the associated kernel.
	// This has to be populated manually.
	signatureScheme string

	// Key is the key of the associated kernel.
	// This has to be populated manually.
	key string

	// ReplicaId is the replica of the kernel that received the message.
	// This should be assigned a value in the forwarder function defined in the DistributedKernelClient's
	// RequestWithHandlerAndReplicas method.
	ReplicaId int32
	// Indicates whether the signatureScheme field has been set.
	signatureSchemeSet bool

	// Indicates whether the key field has been set.
	keySet bool

	// IsFailedExecuteRequest is a flag indicating whether this JupyterMessage contains a failed "execute_request"
	// (or really, a failed "execute_reply" or "yield_reply"), in which there was an error while executing the "all
	// replicas yielded" handler.
	IsFailedExecuteRequest bool

	parentHeaderDecoded bool
	headerDecoded       bool
	metadataDecoded     bool
}

// NewJupyterMessage creates and returns a new JupyterMessage from a ZMQ4 message.
func NewJupyterMessage(msg *zmq4.Msg) *JupyterMessage {
	if msg == nil {
		panic("Cannot create JupyterMessage from nil ZMQ4 message...")
	}

	frames := msg.Frames
	if len(frames) == 0 {
		return nil
	}

	destId, reqId, _ := extractDestFrame(msg.Frames)

	return &JupyterMessage{
		msg:                 msg,
		ReplicaId:           -1,
		JupyterFrames:       NewJupyterFramesFromBytes(msg.Frames),
		header:              nil, // &header,
		parentHeader:        nil, // &parentHeader,
		DestinationId:       destId,
		RequestId:           reqId,
		headerDecoded:       false,
		parentHeaderDecoded: false,
	}
}

func cloneMap(src map[string]interface{}, dst map[string]interface{}) {
	for k, v := range src {
		if innerSrc, ok := v.(map[string]interface{}); ok {
			innerDst := make(map[string]interface{})
			cloneMap(innerSrc, innerDst)
			dst[k] = innerDst
		} else {
			dst[k] = v
		}
	}
}

// AddDestFrameIfNecessary adds the destination frame to the specified Jupyter message if it isn't already present.
func (m *JupyterMessage) AddDestFrameIfNecessary(dstId string) {
	// Add the dest frame here, as there can be a race condition where multiple replicas will add the dest frame at
	// the same time, leading to multiple dest frames.
	_, reqId, _ := m.JupyterFrames.ExtractDestFrame(true)
	if reqId == "" {
		m.AddDestinationId(dstId)
	}
}

func (m *JupyterMessage) Clone() *JupyterMessage {
	var clonedHeader *MessageHeader
	if m.headerDecoded {
		clonedHeader = m.header.Clone()
	}

	var clonedParentHeader *MessageHeader
	if m.parentHeaderDecoded {
		clonedParentHeader = m.parentHeader.Clone()
	}

	var clonedRequestTrace *proto.RequestTrace
	if m.RequestTrace != nil {
		clonedRequestTrace = m.RequestTrace.Clone()
	}

	var clonedFrames *JupyterFrames
	if m.JupyterFrames != nil {
		clonedFrames = m.JupyterFrames.Clone()
	}

	var clonedZmqMsg *zmq4.Msg
	if m.msg != nil {
		clone := m.msg.Clone()
		clone.Type = m.msg.Type
		clonedZmqMsg = &clone
	}

	// Best-effort attempt to deep copy...
	clonedMetadata := make(map[string]interface{})
	cloneMap(m.metadata, clonedMetadata)

	clonedJupyterMessage := &JupyterMessage{
		ReplicaId:           m.ReplicaId,
		RequestId:           m.RequestId,
		DestinationId:       m.DestinationId,
		header:              clonedHeader,
		parentHeader:        clonedParentHeader,
		signatureScheme:     m.signatureScheme,
		signatureSchemeSet:  m.signatureSchemeSet,
		metadata:            clonedMetadata,
		key:                 m.key,
		keySet:              m.keySet,
		parentHeaderDecoded: m.parentHeaderDecoded,
		headerDecoded:       m.headerDecoded,
		metadataDecoded:     m.metadataDecoded,
		RequestTrace:        clonedRequestTrace,
		JupyterFrames:       clonedFrames,
		msg:                 clonedZmqMsg,
	}

	return clonedJupyterMessage
}

// MsgToString returns the Frames of the msg field as a string.
func (m *JupyterMessage) MsgToString() string {
	if len(m.msg.Frames) == 0 {
		return "[]"
	}

	s := "["
	for i, frame := range m.msg.Frames {
		s += "\"" + string(frame) + "\""

		if i+1 < len(m.msg.Frames) {
			s += ", "
		}
	}

	s += "]"

	return s
}

// GetZmqMsg returns the *zmq4.Msg wrapped by the target JupyterMessage struct.
//
// Before being returned, the Frames of the target *zmq4.Msg are set to the current frames of
// the JupyterFrames struct that is also wrapped by the target JupyterMessage.
func (m *JupyterMessage) GetZmqMsg() *zmq4.Msg {
	m.msg.Frames = m.JupyterFrames.Frames
	return m.msg
}

// Offset returns the offset of the underlying JupyterFrames.
func (m *JupyterMessage) Offset() int {
	return m.JupyterFrames.Offset
}

// SetSignatureScheme sets the signature scheme of the JupyterMessage.
// This only sets the signature scheme if its length is positive (i.e., the signatureScheme parameter cannot be the empty string).
func (m *JupyterMessage) SetSignatureScheme(signatureScheme string) {
	if len(signatureScheme) == 0 {
		return
	}

	m.signatureScheme = signatureScheme
	m.signatureSchemeSet = true
}

// EncodeMetadata attempts to marshal the given metadata map into the metadata frame of the underlying JupyterFrames.
// If successful, then the metadata field of the JupyterMessage, which essentially serves as a cached
// version of the JupyterFrames' serialized metadata dictionary, will be updated (i.e., assigned to the
// metadata parameter of this EncodeMetadata method).
func (m *JupyterMessage) EncodeMetadata(metadata map[string]interface{}) error {
	err := m.JupyterFrames.EncodeMetadata(metadata)
	if err == nil {
		m.metadata = metadata
		m.metadataDecoded = true
		return nil
	}

	return err
}

// EncodeContent attempts to marshal the given map into the content frame of the underlying JupyterFrames.
func (m *JupyterMessage) EncodeContent(content map[string]interface{}) error {
	return m.JupyterFrames.EncodeContent(content)
}

func (m *JupyterMessage) EncodeMessageHeader(header *MessageHeader) error {
	err := m.JupyterFrames.EncodeHeader(&header)
	if err == nil {
		m.header = header
		return nil
	}

	return err
}

// SetSignatureSchemeIfNotSet sets the signature scheme of the JupyterMessage if it has not already been set.
func (m *JupyterMessage) SetSignatureSchemeIfNotSet(signatureScheme string) {
	if !m.signatureSchemeSet {
		m.SetSignatureScheme(signatureScheme)
	}
}

// DecodeMetadata decodes the metadata frame and returns the resulting map[string]interface{},
// or an error if the metadata frame could not be decoded successfully.
func (m *JupyterMessage) DecodeMetadata() (map[string]interface{}, error) {
	if m.metadataDecoded {
		return m.metadata, nil
	}

	err := m.JupyterFrames.DecodeMetadata(&m.metadata)
	if err != nil {
		return nil, err
	}

	return m.metadata, nil
}

// SignatureScheme returns the signature scheme of the JupyterMessage
// and a boolean indicating whether the returned signature scheme is valid.
func (m *JupyterMessage) SignatureScheme() (string, bool) {
	return m.signatureScheme, m.signatureSchemeSet
}

// SetKey sets the key of the JupyterMessage.
// This only sets the key if its length is positive (i.e., the key parameter cannot be the empty string).
func (m *JupyterMessage) SetKey(key string) {
	if len(key) == 0 {
		return
	}

	m.key = key
	m.keySet = true
}

// SetKeyIfNotSet sets the key of the JupyterMessage if it has not already been set.
func (m *JupyterMessage) SetKeyIfNotSet(key string) {
	if !m.keySet {
		m.SetKey(key)
	}
}

// Key returns the key of the JupyterMessage and a boolean indicating whether the returned key is valid.
func (m *JupyterMessage) Key() (string, bool) {
	return m.key, m.keySet
}

// IsAck returns true if this is an ACK message.
func (m *JupyterMessage) IsAck() bool {
	return m.JupyterMessageType() == MessageTypeACK
}

func (m *JupyterMessage) AddDestinationId(destID string) (string, int) {
	reqID := m.JupyterFrames.AddDestFrame(destID, true)

	if len(m.RequestId) > 0 && m.RequestId != reqID {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Overwriting existing RequestId \"%s\" of JupyterMessage with value \"%s\"\n"), m.RequestId, reqID)
	}

	if len(m.DestinationId) > 0 && m.DestinationId != destID {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Overwriting existing DestinationId \"%s\" of JupyterMessage with value \"%s\"\n"), m.DestinationId, destID)
	}

	m.RequestId = reqID
	m.DestinationId = destID

	log.Printf("Added destination ID \"%s\" to JupyterMessage. Request ID: \"%s\". Offset: %d.\n", destID, reqID, m.JupyterFrames.Offset)

	return reqID, m.JupyterFrames.Offset
}

// GetParentHeader decodes/deserializes the Jupyter parent header.
// (The parent header is lazily decoded in general.)
func (m *JupyterMessage) GetParentHeader() *MessageHeader {
	if m.parentHeaderDecoded {
		return m.parentHeader
	}

	if m.msg == nil {
		panic("Cannot decode parent header of JupyterMessage because the underlying ZMQ message is nil...")
	}

	var parentHeader MessageHeader
	if err := m.JupyterFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil
	}

	if len(m.JupyterFrames.Frames[JupyterFrameParentHeader]) == 0 {
		m.parentHeader = &parentHeader
		m.parentHeaderDecoded = true

		return m.parentHeader
	}

	if err := m.JupyterFrames.DecodeParentHeader(&parentHeader); err != nil {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Failed to decode parent header from frame \"%v\" because: %v\n"),
			string(m.JupyterFrames.Frames[JupyterFrameParentHeader]), err)
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Message frames (for which we failed to decode parent header): %s\n"),
			m.msg.String())
	}

	m.parentHeader = &parentHeader
	m.parentHeaderDecoded = true

	return m.parentHeader
}

func (m *JupyterMessage) ParentHeaderFrame() *JupyterFrame {
	return m.JupyterFrames.ParentHeaderFrame()
}

func (m *JupyterMessage) HeaderFrame() *JupyterFrame {
	return m.JupyterFrames.HeaderFrame()
}

// GetHeader decodes/deserializes the Jupyter message header.
// (The header is lazily decoded in general.)
func (m *JupyterMessage) GetHeader() (*MessageHeader, error) {
	if m.headerDecoded {
		return m.header, nil
	}

	if m.msg == nil {
		panic("Cannot decode header of JupyterMessage because the underlying ZMQ message is nil...")
	}
	if m.JupyterFrames == nil {
		panic("Cannot decode header of JupyterMessage because the underlying JupyterFrames struct is nil...")
	}

	var header MessageHeader
	if err := m.JupyterFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil, err
	}

	if err := m.JupyterFrames.DecodeHeader(&header); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to decode header from frame \"%v\" because: %v\n"), string(m.JupyterFrames.Frames[JupyterFrameHeader]), err)
		fmt.Printf(utils.RedStyle.Render("[ERROR] Erroneous message: %s\n"), m.String())
		return nil, err
	}

	m.header = &header
	m.headerDecoded = true

	return m.header, nil
}

func (m *JupyterMessage) Validate() error {
	if m.JupyterFrames.Len() < 5 /* 6, but buffers are optional, so 5 */ {
		return ErrInvalidJupyterMessage
	}
	return nil
}

func (m *JupyterMessage) SetMessageType(typ JupyterMessageType, reEncode bool) error {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	header.MsgType = typ
	m.header = header

	if reEncode {
		return m.EncodeMessageHeader(m.header)
	}

	return nil
}

func (m *JupyterMessage) SetMessageId(msgId string, reEncode bool) error {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	header.MsgID = msgId
	m.header = header

	if reEncode {
		return m.EncodeMessageHeader(m.header)
	}

	return nil
}

func (m *JupyterMessage) SetDate(date string, reEncode bool) error {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	header.Date = date
	m.header = header

	if reEncode {
		return m.EncodeMessageHeader(m.header)
	}

	return nil
}

// JupyterMessageType is a convenience/utility method for retrieving the Jupyter message type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageType() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return string(header.MsgType)
}

// JupyterParentMessageType is a convenience/utility method for retrieving the (parent) Jupyter message type from
// the Jupyter parent message header.
func (m *JupyterMessage) JupyterParentMessageType() string {
	parentHeader := m.GetParentHeader() // Instantiate the header in case it isn't already.
	if parentHeader == nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s.\n", m.msg.String()))
	}
	return string(parentHeader.MsgType)
}

// JupyterMessageDate is a convenience/utility method for retrieving the Jupyter date type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageDate() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return header.Date
}

// JupyterSession is a convenience/utility method for retrieving the Jupyter session from the Jupyter message header.
func (m *JupyterMessage) JupyterSession() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return header.Session
}

// JupyterUsername is a convenience/utility method for retrieving the Jupyter username from the Jupyter message header.
func (m *JupyterMessage) JupyterUsername() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return header.Username
}

// JupyterVersion is a convenience/utility method for retrieving the Jupyter version from the Jupyter message header.
func (m *JupyterMessage) JupyterVersion() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return header.Version
}

// JupyterMessageId is a convenience/utility method for retrieving the Jupyter message ID from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageId() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		debug.PrintStack()
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.msg.String(), err))
	}
	return header.MsgID
}

// JupyterParentMessageId is a convenience/utility method for retrieving the Jupyter message ID
// from the parent Jupyter message header.
func (m *JupyterMessage) JupyterParentMessageId() string {
	parentHeader := m.GetParentHeader() // Instantiate the parentHeader in case it isn't already.
	if parentHeader == nil {
		panic(fmt.Sprintf("Failed to decode message parentHeader. Message: %s.\n", m.msg.String()))
	}
	return parentHeader.MsgID
}

func (m *JupyterMessage) String() string {
	return fmt.Sprintf("JupyterMessage[ReqId=%s,DestId=%s,Offset=%d]; JupyterMessage's JupyterFrames=%s", m.RequestId, m.DestinationId, m.Offset(), m.JupyterFrames.String())
}

func (m *JupyterMessage) StringFormatted() string {
	return fmt.Sprintf("JupyterMessage[ReqId=%s,DestId=%s,Offset=%d]; JupyterMessage's JupyterFrames=%s", m.RequestId, m.DestinationId, m.Offset, m.JupyterFrames.StringFormatted())
}

// CreateAndReturnYieldRequestMessage creates a "yield_request" message from the target JupyterMessage.
//
// If the target JupyterMessage is already a "yield_request" message, then the target JupyterMessage is simply returned.
//
// If the target message is not of type "execute_request", then an error is returned.
//
// This will return a COPY of the original message with the type field modified to contact "yield_request" instead of "execute_request".
// On success, the returned error will be nil. If an error occurs, then the returned message will be nil, and the error will be non-nil.
//
// PRECONDITION: The given message must be an "execute_request" message.
// This function will NOT check this. It should be checked before calling this function.
func (m *JupyterMessage) CreateAndReturnYieldRequestMessage() (*JupyterMessage, error) {
	// If the message is already a yield request, then just return a copy of it,
	// as the expectation is that the returned message from this method will be a clone/copy.
	if m.JupyterMessageType() == ShellYieldRequest {
		return m.Clone(), nil
	}

	if m.JupyterMessageType() != ShellExecuteRequest {
		return nil, fmt.Errorf("%w: message is of type \"%s\", not \"%s\"", ErrInvalidJupyterMessage, m.JupyterMessageType(), ShellExecuteRequest)
	}

	// Clone the original message.
	var newMessage = m.GetZmqMsg().Clone()
	jMsg := NewJupyterMessage(&newMessage)

	// Change the message header.
	_ = jMsg.SetMessageType(ShellYieldRequest, false)

	// Create a JupyterFrames struct by wrapping with the message's frames.
	if err := jMsg.Validate(); err != nil {
		// m.notifyClusterGatewayAndPanic("Failed to Validate \"yield_request\" Message", err.Error(), err) // TODO(Ben): Handle this error more gracefully.
		return nil, err
	}

	// Replace the header with the new header (that has the 'yield_request' MsgType).
	header, err := jMsg.GetHeader()
	if err != nil {
		return nil, err
	}

	if err = jMsg.JupyterFrames.EncodeHeader(&header); err != nil {
		// m.notifyClusterGatewayAndPanic("Failed to Encode Header for \"yield_request\" Message", err.Error(), err) // TODO(Ben): Handle this error more gracefully.
		return nil, err
	}

	// Replace the frames of the cloned ZMQ message with the new JupyterMessage's frames.
	// I don't think this is really necessary, as we do this automatically, but whatever.
	newMessage.Frames = jMsg.JupyterFrames.Frames

	return jMsg, nil
}
