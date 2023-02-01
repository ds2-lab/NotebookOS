package types

// http://jupyter-client.readthedocs.io/en/latest/messaging.html#general-message-format
type MessageHeader struct {
	MsgID    string `json:"msg_id"`
	Username string `json:"username"`
	Session  string `json:"session"`
	Date     string `json:"date"`
	MsgType  string `json:"msg_type"`
	Version  string `json:"version"`
}

// Message represents an entire message in a high-level structure.
type Message struct {
	Header       MessageHeader
	ParentHeader MessageHeader
	Metadata     map[string]interface{}
	Content      interface{}
}
