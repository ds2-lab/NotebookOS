package server

import (
	"context"
	"fmt"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
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

// Socket returns the zmq socket of the given type.
func (s *BaseServer) Socket(typ types.MessageType) *types.Socket {
	return s.server.Sockets.All[typ]
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
func (s *BaseServer) ExtractDestFrame(frames [][]byte) (kernelID string, reqID string, jOffset int) {
	return s.server.ExtractDestFrame(frames)
}

func (s *BaseServer) AddDestFrame(frames [][]byte, kernelID string, jOffset int) (newFrames [][]byte, reqID string) {
	return s.server.AddDestFrame(frames, kernelID, jOffset)
}

func (s *BaseServer) RemoveDestFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	return s.server.RemoveDestFrame(frames, jOffset)
}

func (s *BaseServer) SkipIdentities(frames [][]byte) (types.JupyterFrames, int) {
	return s.server.SkipIdentities(frames)
}

func (s *BaseServer) Close() {
	s.server.CancelCtx()
}
