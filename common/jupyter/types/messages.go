package types

import (
	"fmt"
	"log"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	MessageHeaderDefaultUsername = "username"

	MessageTypeShutdownRequest = "shutdown_request"

	ErrorNotification   NotificationType = 0
	WarningNotification NotificationType = 1
	InfoNotfication     NotificationType = 2
	SuccessNotification NotificationType = 3
)

type NotificationType int32

// Message represents an entire message in a high-level structure.
type Message struct {
	Header       MessageHeader
	ParentHeader MessageHeader
	Metadata     map[string]interface{}
	Content      interface{}
}

// http://jupyter-client.readthedocs.io/en/latest/messaging.html#general-message-format
// https://hackage.haskell.org/package/jupyter-0.9.0/docs/Jupyter-Messages.html
type MessageHeader struct {
	MsgID    string `json:"msg_id"`
	Username string `json:"username"`
	Session  string `json:"session"`
	Date     string `json:"date"`
	MsgType  string `json:"msg_type"`
	Version  string `json:"version"`
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

// Wrapper around ZMQ4 messages, specifically Jupyter ZMQ4 messages.
// We encode the message ID and message type for convenience.
type JupyterMessage struct {
	*zmq4.Msg
	JupyterFrames

	RequestId     string
	DestinationId string
	Offset        int

	header       *MessageHeader
	parentHeader *MessageHeader

	parentHeaderDecoded bool
	headerDecoded       bool
}

// Create a new JupyterMessage from a ZMQ4 message.
func NewJupyterMessage(msg *zmq4.Msg) *JupyterMessage {
	if msg == nil {
		panic("Cannot create JupyterMessage from nil ZMQ4 message...")
	}

	frames := msg.Frames
	if len(frames) == 0 {
		return nil
	}

	destId, reqId, offset := extractDestFrame(msg.Frames)

	if len(destId) == 0 {
		log.Printf("[WARNING] Destination ID is empty when creating JupyterMessage: %v\n", msg.String())
	}

	if len(reqId) == 0 {
		log.Printf("[WARNING] Request ID is empty when creating JupyterMessage: %v\n", msg.String())
	}

	// jFrames := JupyterFrames(frames[offset:])
	// if err := jFrames.Validate(); err != nil {
	// 	fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
	// 	return nil
	// }

	// var header MessageHeader
	// if err := jFrames.DecodeHeader(&header); err != nil {
	// 	fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to decode header \"%v\" from message frames: %v", string(jFrames[JupyterFrameHeader])), err)
	// 	fmt.Printf(utils.RedStyle.Render("[ERROR] Message frames (for which we failed to decode header): %v"), msg)
	// 	return nil
	// }

	// var parentHeader MessageHeader
	// if err := jFrames.DecodeParentHeader(&parentHeader); err != nil {
	// 	fmt.Printf(utils.OrangeStyle.Render("[WARNING] Failed to decode parent header \"%v\" from message frames: %v", string(jFrames[JupyterFrameHeader])), err)
	// 	fmt.Printf(utils.OrangeStyle.Render("[WARNING] Message frames (for which we failed to decode parent header): %v"), msg)
	// }

	return &JupyterMessage{
		Msg:                 msg,
		header:              nil, // &header,
		parentHeader:        nil, // &parentHeader,
		DestinationId:       destId,
		Offset:              offset,
		RequestId:           reqId,
		headerDecoded:       false,
		parentHeaderDecoded: false,
	}
}

func extractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int) {
	jOffset = 0
	if len(frames) >= 1 {
		// Jupyter messages start from "<IDS|MSG>" frame.
		for jOffset < len(frames) && string(frames[jOffset]) != "<IDS|MSG>" {
			jOffset++
		}
	}

	if jOffset > 0 {
		matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))

		if len(matches) > 0 {
			destID = matches[1]
			reqID = matches[2]
		}
	}
	return
}

func (m *JupyterMessage) AddDestinationId(destID string) (reqID string, jOffset int) {
	m.Frames, reqID, jOffset = AddDestFrame(m.Frames, destID, jupyter.JOffsetAutoDetect)

	if len(m.RequestId) > 0 && m.RequestId != reqID {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Overwriting existing RequestId \"%s\" of JupyterMessage with value \"%s\"\n"), m.RequestId, reqID)
	}

	if len(m.DestinationId) > 0 && m.DestinationId != destID {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Overwriting existing DestinationId \"%s\" of JupyterMessage with value \"%s\"\n"), m.DestinationId, destID)
	}

	m.RequestId = reqID
	m.DestinationId = destID
	m.Offset = jOffset

	log.Printf("Added destination ID \"%s\" to JupyterMessage. Request ID: \"%s\". Offset: %d.\n", destID, reqID, jOffset)

	return reqID, jOffset
}

// The parent header is lazily decoded/deserialized.
// This decodes/deserializes it.
func (m *JupyterMessage) GetParentHeader() *MessageHeader {
	if m.parentHeaderDecoded {
		return m.parentHeader
	}

	if m.Msg == nil {
		panic("Cannot decode parent header of JupyterMessage because the underlying ZMQ message is nil...")
	}

	var parentHeader MessageHeader
	jFrames := JupyterFrames(m.Frames[m.Offset:])
	if err := jFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil
	}

	if err := jFrames.DecodeParentHeader(&parentHeader); err != nil {
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Failed to decode parent header from frame \"%v\" because: %v\n"), string(jFrames[JupyterFrameHeader]), err)
		fmt.Printf(utils.OrangeStyle.Render("[WARNING] Message frames (for which we failed to decode parent header): %s\n"), m.Msg.String())
	}

	m.parentHeader = &parentHeader
	m.parentHeaderDecoded = true

	return m.parentHeader
}

func (m *JupyterMessage) ParentHeaderFrame() JupyterFrame {
	return JupyterFrame(JupyterFrames(m.Msg.Frames[m.Offset:])[JupyterFrameParentHeader])
}

// The header is lazily decoded/deserialized.
// This decodes/deserializes it.
func (m *JupyterMessage) GetHeader() (*MessageHeader, error) {
	if m.headerDecoded {
		return m.header, nil
	}

	if m.Msg == nil {
		panic("Cannot decode header of JupyterMessage because the underlying ZMQ message is nil...")
	}

	var header MessageHeader
	jFrames := JupyterFrames(m.Frames[m.Offset:])
	if err := jFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil, err
	}

	if err := jFrames.DecodeHeader(&header); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to decode header from frame \"%v\" because: %v\n"), string(jFrames[JupyterFrameHeader]), err)
		fmt.Printf(utils.RedStyle.Render("[ERROR] Erroneous message: %s\n"), m.String())
		return nil, err
	}

	m.header = &header
	m.headerDecoded = true

	return m.header, nil
}

func (m *JupyterMessage) ToJFrames() JupyterFrames {
	return JupyterFrames(m.Frames[m.Offset:])
}

func (m *JupyterMessage) SetMessageType(typ string) {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	header.MsgType = typ
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

// Convenience/utility method for retrieving the Jupyter message type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageType() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.MsgType
}

// Convenience/utility method for retrieving the Jupyter date type from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageDate() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Date
}

// Convenience/utility method for retrieving the Jupyter session from the Jupyter message header.2
func (m *JupyterMessage) JupyterSession() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.Session
}

// Convenience/utility method for retrieving the Jupyter message ID from the Jupyter message header.
func (m *JupyterMessage) JupyterMessageId() string {
	header, err := m.GetHeader() // Instantiate the header in case it isn't already.
	if header == nil || err != nil {
		panic(fmt.Sprintf("Failed to decode message header. Message: %s. Error: %v\n", m.Msg.String(), err))
	}
	return header.MsgID
}

func (m *JupyterMessage) String() string {
	return fmt.Sprintf("JupyterMessage[ReqId=%s,DestId=%s,Offset=%d,Msg=%s]", m.RequestId, m.DestinationId, m.Offset, JupyterFrames(m.Frames).String())
}
