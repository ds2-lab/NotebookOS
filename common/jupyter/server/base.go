package server

import (
	"context"
	"regexp"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	ZMQKernelIDFrameFormatter  = "kernel.%s.req.%s" // kernel.<kernel-id>.<req-id>
	ZMQKernelIDFrameRecognizer = regexp.MustCompile(`^kernel\.([0-9a-f-]+)\.req\.([0-9a-f-]+)$`)
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

// SetContext sets the context of this server.
func (s *BaseServer) SetContext(ctx context.Context) {
	s.server.Ctx = ctx
}

// Expose abstract server methods.
func (s *BaseServer) AddKernelFrame(frames [][]byte, kernelID string) [][]byte {
	return s.server.AddKernelFrame(frames, kernelID)
}

func (s *BaseServer) ExtractKernelFrames(frames [][]byte) (kernelID string, reqID string, jFrames [][]byte) {
	return s.server.ExtractKernelFrames(frames)
}

func (s *BaseServer) RemoveKernelFrame(frames [][]byte) (kernelID string, reqID string, removed [][]byte) {
	return s.server.RemoveKernelFrame(frames)
}

func (s *BaseServer) SkipIdentities(frames [][]byte) ([][]byte, int) {
	return s.server.SkipIdentities(frames)
}

func (s *BaseServer) Close() {
	s.server.CancelCtx()
}
