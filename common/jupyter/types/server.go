package types

// Sender is a single-method interface exposing the ability to send a Request via a particular Socket.
type Sender interface {
	// SendRequest sends a types.Request on the given types.Socket.
	// If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
	//
	// SendRequest returns nil on success.
	SendRequest(request Request, socket *Socket) error
}
