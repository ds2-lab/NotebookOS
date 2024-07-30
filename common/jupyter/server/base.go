package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	ErrIOSocketAlreadySet = errors.New("the server already has a non-nil IO ZeroMQ Socket")
)

// Router defines the interface to provider infos of a JupyterRouter.
type ServerInfo interface {
	fmt.Stringer

	Socket(types.MessageType) *types.Socket
}

// BaseServer exposes the basic operations of a Jupyter server. Get BaseServer from AbstractServer.Server().
type BaseServer struct {
	server *AbstractServer
}

func (s *BaseServer) SendMessage(request types.Request, socket *types.Socket) error {
	return s.server.SendMessage(request, socket)
}

// func (s *BaseServer) SendMessage(requiresACK bool, socket *types.Socket, reqId string, req *zmq4.Msg, dest RequestDest, sourceKernel SourceKernel, offset int) error {
// 	jMsg := types.NewJupyterMessage(req)
// 	return s.server.SendMessage(requiresACK, socket, reqId, jMsg, dest, sourceKernel, offset)
// }

// Begin listening for an ACK for a message with the given ID.
func (s *BaseServer) RegisterAck(msg *zmq4.Msg) (chan struct{}, bool) {
	_, reqId, _ := types.ExtractDestFrame(msg.Frames)
	return s.server.RegisterAck(reqId)
}

// Socket returns the zmq socket of the given type.
func (s *BaseServer) Socket(typ types.MessageType) *types.Socket {
	return s.server.Sockets.All[typ]
}

// Set the IOPub socket for the server.
// Returns an error if the Socket is already set, as it should only be set once when the IO socket is nil.
func (s *BaseServer) SetIOPubSocket(iopub *types.Socket) error {
	if s.server.Sockets.IO != nil {
		return ErrIOSocketAlreadySet
	}

	s.server.Sockets.IO = iopub
	s.server.Sockets.All[types.IOMessage] = iopub

	return nil
}

// Context returns the context of this server.
func (s *BaseServer) Context() context.Context {
	return s.server.Ctx
}

// SetContext sets the context of this server.
func (s *BaseServer) SetContext(ctx context.Context) {
	s.server.Ctx = ctx
}

// Expose abstract server methods.
// func (s *BaseServer) ExtractDestFrame(frames [][]byte) (kernelID string, reqID string, jOffset int) {
// 	return s.server.ExtractDestFrame(frames)
// }

// func (s *BaseServer) AddDestFrame(frames [][]byte, kernelID string, jOffset int) (newFrames [][]byte, reqID string) {
// 	return s.server.AddDestFrame(frames, kernelID, jOffset)
// }

// func (s *BaseServer) RemoveDestFrame(frames [][]byte, jOffset int) (removed [][]byte) {
// 	return s.server.RemoveDestFrame(frames, jOffset)
// }

// func (s *BaseServer) ExtractSourceKernelFrame(frames [][]byte) (kernelID string, jOffset int) {
// 	return s.server.ExtractSourceKernelFrame(frames)
// }

// func (s *BaseServer) AddSourceKernelFrame(frames [][]byte, kernelID string, jOffset int) (newFrames [][]byte) {
// 	return s.server.AddSourceKernelFrame(frames, kernelID, jOffset)
// }

// func (s *BaseServer) RemoveSourceKernelFrame(frames [][]byte, jOffset int) (removed [][]byte) {
// 	return s.server.RemoveSourceKernelFrame(frames, jOffset)
// }

func (s *BaseServer) Close() {
	s.server.CancelCtx()
}
