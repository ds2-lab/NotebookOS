package types

// Message represents an entire message in a high-level structure.
type Message struct {
	Header       MessageHeader
	ParentHeader MessageHeader
	Metadata     map[string]interface{}
	Content      interface{}
}

// http://jupyter-client.readthedocs.io/en/latest/messaging.html#general-message-format
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
