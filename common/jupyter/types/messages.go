package types

import (
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	MessageHeaderDefaultUsername = "username"

	MessageTypeShutdownRequest = "shutdown_request"
)

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
	Header   *MessageHeader
	KernelId string
}

func (m *JupyterMessage) GetMsg() *zmq4.Msg {
	return m.Msg
}

// Create a new JupyterMessage from a ZMQ4 message.
func NewJupyterMessage(msg *zmq4.Msg) *JupyterMessage {
	frames := msg.Frames
	if len(frames) == 0 {
		return nil
	}

	offset := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for offset < len(frames) && string(frames[offset]) != "<IDS|MSG>" {
		offset++
	}

	var (
		kernelId string
	)
	matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[offset-1]))
	if len(matches) > 0 {
		kernelId = matches[1]
	}

	jFrames := JupyterFrames(frames[offset:])
	if err := jFrames.Validate(); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to validate message frames while extracting header: %v\n"), err)
		return nil
	}

	var header MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to decode header \"%v\" from message frames: %v", string(jFrames[JupyterFrameHeader])), err)
		return nil
	}

	return &JupyterMessage{
		Msg:      msg,
		Header:   &header,
		KernelId: kernelId,
	}
}
