package server

import (
	"context"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

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

func (s *BaseServer) Close() {
	s.server.CancelCtx()
}
