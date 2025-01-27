package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
)

var (
	ErrIOSocketAlreadySet = errors.New("the server already has a non-nil IO ZeroMQ Socket")
)

// Info defines the interface to provider infos of a JupyterRouter.
type Info interface {
	fmt.Stringer

	Socket(messaging.MessageType) *messaging.Socket
}

// BaseServer exposes the basic operations of a Jupyter server. Get BaseServer from AbstractServer.Server().
type BaseServer struct {
	server *AbstractServer
}

func (s *BaseServer) SendRequest(request messaging.Request, socket *messaging.Socket) error {
	return s.server.SendRequest(request, socket)
}

func (s *BaseServer) SetComponentId(id string) {
	s.server.ComponentId = id
}

// AssignMessagingMetricsProvider sets the StatisticsAndMetricsProvider on the AbstractServer encapsulated by the BaseServer.
func (s *BaseServer) AssignMessagingMetricsProvider(provider MessagingMetricsProvider) {
	s.server.StatisticsAndMetricsProvider = provider
}

// func (s *BaseServer) SendRequest(requiresACK bool, socket *messaging.Socket, reqId string, req *zmq4.msg, dest RequestDest, sourceKernel SourceKernel, offset int) error {
// 	jMsg := messaging.NewJupyterMessage(req)
// 	return s.server.SendRequest(requiresACK, socket, reqId, jMsg, dest, sourceKernel, offset)
// }

// RegisterAck begins listening for an ACK for a message with the given ID.
func (s *BaseServer) RegisterAck(msg *messaging.JupyterMessage) (chan struct{}, bool) {
	// _, reqId, _ := types.ExtractDestFrame(msg.JupyterFrames)
	return s.server.RegisterAck(msg.RequestId)
}

// RegisterAckForRequest begins listening for an ACK for a message with the given ID.
func (s *BaseServer) RegisterAckForRequest(req messaging.Request) (chan struct{}, bool) {
	// _, reqId, _ := types.ExtractDestFrame(msg.JupyterFrames)
	return s.server.RegisterAck(req.RequestId())
}

// Socket returns the zmq socket of the given type.
func (s *BaseServer) Socket(typ messaging.MessageType) *messaging.Socket {
	return s.server.Sockets.All[typ]
}

// GetSocketPort returns the port of a particular Socket.
func (s *BaseServer) GetSocketPort(typ messaging.MessageType) int {
	socket := s.Socket(typ)

	if socket != nil {
		return socket.Port
	}

	return -1
}

// SetIOPubSocket sets the IOPub socket for the server.
// SetIOPubSocket returns an error if the Socket is already set, as it should only be set once when the IO socket is nil.
func (s *BaseServer) SetIOPubSocket(iopub *messaging.Socket) error {
	if s.server.Sockets.IO != nil {
		return ErrIOSocketAlreadySet
	}

	s.server.Sockets.IO = iopub
	s.server.Sockets.All[messaging.IOMessage] = iopub

	return nil
}

func (s *BaseServer) MessageAcknowledgementsEnabled() bool {
	return s.server.MessageAcknowledgementsEnabled
}

// Context returns the context of this server.
func (s *BaseServer) Context() context.Context {
	return s.server.Ctx
}

// SetContext sets the context of this server.
func (s *BaseServer) SetContext(ctx context.Context) {
	s.server.Ctx = ctx
}

func (s *BaseServer) Close() error {
	s.server.CancelCtx()
	return nil
}
