package types

type Sender interface {
	// SendMessage sends a message. If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
	SendMessage(request Request, socket *Socket) error

	// SendMessage(requiresACK bool, socket *types.Socket, reqId string, req *zmq4.Msg, dest types.RequestDest, sourceKernel types.SourceKernel, offset int) error
}
