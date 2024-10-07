package types

import (
	"encoding/json"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"log"
	"strings"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	MessageHeaderDefaultUsername = "username"

	ShellExecuteRequest        = "execute_request"
	ShellExecuteReply          = "execute_reply"
	ShellYieldRequest          = "yield_request"
	ShellKernelInfoRequest     = "kernel_info_request"
	ShellShutdownRequest       = "shutdown_request"
	MessageTypeShutdownRequest = "shutdown_request"
	MessageTypeShutdownReply   = "shutdown_reply"

	ErrorNotification   NotificationType = 0
	WarningNotification NotificationType = 1
	InfoNotification    NotificationType = 2
	SuccessNotification NotificationType = 3

	JavascriptISOString = "2006-01-02T15:04:05.999Z07:00"

	MessageTypeACK = "ACK"
)

type JupyterMessageType string

func (t JupyterMessageType) String() string {
	return string(t)
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

// Message represents an entire message in a high-level structure.
type Message struct {
	Header       MessageHeader          `json:"header"`
	ParentHeader MessageHeader          `json:"parent_header"`
	Metadata     map[string]interface{} `json:"metadata"`
	Content      interface{}            `json:"content"`
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

// JupyterMessage is a wrapper around ZMQ4 messages, specifically Jupyter ZMQ4 messages.
// We encode the message ID and message type for convenience.
type JupyterMessage struct {
	// Msg is the *zmq4.Msg struct that is wrapped by the JupyterMessage.
	Msg *zmq4.Msg

	// JupyterFrames is a wrapper around the [][]byte from the *zmq4.Msg field.
	// JupyterFrames provides a bunch of helper/utility methods for manipulating the [][]byte.
	JupyterFrames *JupyterFrames

	// ReplicaId is the replica of the kernel that received the message.
	// This should be assigned a value in the forwarder function defined in the DistributedKernelClient's
	// RequestWithHandlerAndReplicas method.
	ReplicaId     int32
	RequestId     string
	DestinationId string

	RequestTrace *proto.RequestTrace

	header       *MessageHeader
	parentHeader *MessageHeader
	metadata     map[string]interface{}

	// signatureScheme is the signature scheme of the associated kernel.
	// This has to be populated manually.
	signatureScheme string
	// Indicates whether the signatureScheme field has been set.
	signatureSchemeSet bool

	// Key is the key of the associated kernel.
	// This has to be populated manually.
	key string
	// Indicates whether the key field has been set.
	keySet bool

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
		Msg:                 msg,
		ReplicaId:           -1,
		JupyterFrames:       NewJupyterFramesFromBytes(&msg.Frames),
		header:              nil, // &header,
		parentHeader:        nil, // &parentHeader,
		DestinationId:       destId,
		RequestId:           reqId,
		headerDecoded:       false,
		parentHeaderDecoded: false,
	}
}

// MsgToString returns the Frames of the Msg field as a string.
func (m *JupyterMessage) MsgToString() string {
	if len(m.Msg.Frames) == 0 {
		return "[]"
	}

	s := "["
	for i, frame := range m.Msg.Frames {
		s += "\"" + string(frame) + "\""

		if i+1 < len(m.Msg.Frames) {
			s += ", "
		}
	}

	s += "]"

	return s
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

	if m.Msg == nil {
		panic("Cannot decode parent header of JupyterMessage because the underlying ZMQ message is nil...")
	}

	var parentHeader MessageHeader
	if err := m.JupyterFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil
	}

	if err := m.JupyterFrames.DecodeParentHeader(&parentHeader); err != nil {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Failed to decode parent header from frame \"%v\" because: %v\n"), string((*m.JupyterFrames.Frames)[JupyterFrameHeader]), err)
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Message frames (for which we failed to decode parent header): %s\n"), m.Msg.String())
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

	if m.Msg == nil {
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
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to decode header from frame \"%v\" because: %v\n"), string((*m.JupyterFrames.Frames)[JupyterFrameHeader]), err)
		fmt.Printf(utils.RedStyle.Render("[ERROR] Erroneous message: %s\n"), m.String())
		return nil, err
	}

	m.header = &header
	m.headerDecoded = true

	return m.header, nil
}

func (m *JupyterMessage) SetMessageType(typ string) {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	header.MsgType = JupyterMessageType(typ)
	m.header = header
}

func (m *JupyterMessage) SetDate(date string) {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	header.Date = date
	m.header = header
}

func (m *JupyterMessage) GetMsg() *zmq4.Msg {
	return m.Msg
}

// JupyterMessageType is a convenience/utility method for retrieving the Jupyter message type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageType() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return string(header.MsgType)
}

// JupyterParentMessageType is a convenience/utility method for retrieving the (parent) Jupyter message type from
// the Jupyter parent message header.
func (m *JupyterMessage) JupyterParentMessageType() string {
	parentHeader := m.GetParentHeader() // Instantiate the header in case it isn't already.
	if parentHeader == nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s.\n", m.Msg.String()))
	}
	return string(parentHeader.MsgType)
}

// JupyterMessageDate is a convenience/utility method for retrieving the Jupyter date type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageDate() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Date
}

// JupyterSession is a convenience/utility method for retrieving the Jupyter session from the Jupyter message header.
func (m *JupyterMessage) JupyterSession() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Session
}

// JupyterUsername is a convenience/utility method for retrieving the Jupyter username from the Jupyter message header.
func (m *JupyterMessage) JupyterUsername() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Username
}

// JupyterVersion is a convenience/utility method for retrieving the Jupyter version from the Jupyter message header.
func (m *JupyterMessage) JupyterVersion() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Version
}

// JupyterMessageId is a convenience/utility method for retrieving the Jupyter message ID from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageId() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.MsgID
}

// JupyterParentMessageId is a convenience/utility method for retrieving the Jupyter message ID
// from the parent Jupyter message header.
func (m *JupyterMessage) JupyterParentMessageId() string {
	parentHeader := m.GetParentHeader() // Instantiate the parentHeader in case it isn't already.
	if parentHeader == nil {
		panic(fmt.Sprintf("Failed to decode message parentHeader. Message: %s.\n", m.Msg.String()))
	}
	return parentHeader.MsgID
}

func (m *JupyterMessage) String() string {
	return fmt.Sprintf("JupyterMessage[ReqId=%s,DestId=%s,Offset=%d]; JupyterMessage's JupyterFrames=%s", m.RequestId, m.DestinationId, m.Offset, m.JupyterFrames.String())
}
