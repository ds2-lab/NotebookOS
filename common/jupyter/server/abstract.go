package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/logger"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	errServeOnce = errors.New("break after served once")
)

// AbstractServer implements the basic socket serving useful for a Jupyter server. Embed this struct in your server implementation.
type AbstractServer struct {
	Meta *types.ConnectionInfo

	// ctx of this server and a func to cancel it.
	Ctx       context.Context
	CancelCtx func()

	// ZMQ sockets
	Sockets *types.JupyterSocket

	// logger
	Log logger.Logger
}

func New(ctx context.Context, info *types.ConnectionInfo, init func(server *AbstractServer)) *AbstractServer {
	var cancelCtx func()
	ctx, cancelCtx = context.WithCancel(ctx)

	server := &AbstractServer{
		Meta:      info,
		Ctx:       ctx,
		CancelCtx: cancelCtx,
		Sockets:   &types.JupyterSocket{},
		// Log:       logger.NilLogger, // To be overwritten by init.
	}
	init(server)
	server.Sockets.All = [5]*types.Socket{server.Sockets.HB, server.Sockets.Control, server.Sockets.Shell, server.Sockets.Stdin, server.Sockets.IO}
	for i, socket := range server.Sockets.All {
		if socket != nil {
			socket.Type = types.MessageType(i)
		}
	}

	return server
}

func (s *AbstractServer) Server() *BaseServer {
	return &BaseServer{s}
}

func (s *AbstractServer) Listen(socket *types.Socket) error {
	if s.Meta.Transport != "tcp" {
		return types.ErrNotSupported
	}

	err := socket.Listen(fmt.Sprintf("tcp://:%d", socket.Port))
	if err != nil {
		return err
	}

	// Update the port number if it is 0.
	socket.Port = socket.Addr().(*net.TCPAddr).Port
	return nil
}

// Serve starts serving the socket with the specified handler. THe handler
// is passed as an argument to allow multiple sockets sharing the same handler.
func (s *AbstractServer) Serve(socket *types.Socket, handler types.MessageHandler) {
	if !atomic.CompareAndSwapInt32(&socket.Serving, 0, 1) {
		// Already serving.
		return
	}
	defer atomic.StoreInt32(&socket.Serving, 0)

	chMsg := make(chan interface{})
	s.Log.Debug("Start serving %v messages", socket.Type)
	go s.poll(socket, chMsg)

	for {
		select {
		case <-s.Ctx.Done():
			return
		case msg := <-chMsg:
			if msg == nil {
				return
			}

			var err error
			switch v := msg.(type) {
			case error:
				err = v
			case *zmq4.Msg:
				err = handler(socket.Type, v)
			}

			// Stop sering on error.
			if err == io.EOF {
				s.Log.Debug("Socket %v closed.", socket.Type)
				return
			} else if err == errServeOnce {
				// Stop serving.
				return
			} else if err != nil {
				s.Log.Warn("Error on handle %v message: %v", socket.Type, err)
				return
			}
		}
	}
}

func (s *AbstractServer) ServeOnce(ctx context.Context, socket *types.Socket, req *zmq4.Msg, remvoeKernelFrame bool, handler types.MessageHandler) {
	socket.Mu.Lock()
	// Last request must have been timeout and we simply replace it with the new one.
	socket.PendingReq = req
	socket.Mu.Unlock()

	// Use Serve to support timeout;
	// Late response will be ignored and serve routing will be stopped if no request is pending.
	if atomic.LoadInt32(&socket.Serving) == 0 {
		go s.Serve(socket, s.getOneTimeMessageHandler(socket, remvoeKernelFrame, handler))
	}

	// Wait for timeout
	if ctx.Done() == nil {
		return
	}

	<-ctx.Done()
	// Clear pending request.
	socket.Mu.Lock()
	if socket.PendingReq == req {
		socket.PendingReq = nil
	}
	socket.Mu.Unlock()
}

// GenerateKernelFrame appends a frame contains the kernel ID to the given ZMQ frames.
func (s *AbstractServer) AddKernelFrame(frames [][]byte, kernelID string) [][]byte {
	jFrame, i := s.SkipIdentities(frames)
	// Add kernel ID frame just before "<IDS|MSG>" frame.
	ret := append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(ret[i+1:], jFrame)
	ret[i] = []byte(fmt.Sprintf(ZMQKernelIDFrameFormatter, kernelID, uuid.New().String()))
	return ret
}

func (s *AbstractServer) ExtractKernelFrames(frames [][]byte) (kernelID string, reqID string, jFrames [][]byte) {
	kernelID, reqID, jFrames, _ = s.extractKernelFramesImpl(frames, false)
	return
}

func (s *AbstractServer) RemoveKernelFrame(frames [][]byte) (kernelID string, reqID string, removed [][]byte) {
	kernelID, reqID, _, removed = s.extractKernelFramesImpl(frames, true)
	return
}

func (s *AbstractServer) SkipIdentities(frames [][]byte) ([][]byte, int) {
	if len(frames) == 0 {
		return frames, 0
	}

	i := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for i < len(frames) && string(frames[i]) != "<IDS|MSG>" {
		i++
	}
	return frames[i:], i
}

func (s *AbstractServer) extractKernelFramesImpl(frames [][]byte, removeKernelFrame bool) (kernelID string, reqID string, jFrames [][]byte, newFrames [][]byte) {
	jFrames, i := s.SkipIdentities(frames)
	if i > 0 {
		matches := ZMQKernelIDFrameRecognizer.FindStringSubmatch(string(frames[i-1]))
		if len(matches) > 0 {
			kernelID = matches[1]
			reqID = matches[2]
			if removeKernelFrame {
				copy(frames[i-1:], jFrames)
				frames = frames[:len(frames)-1]
				jFrames = frames[i-1:]
			}
		}
	}
	return kernelID, reqID, jFrames, frames
}

func (s *AbstractServer) poll(socket *types.Socket, chMsg chan<- interface{}) {
	defer close(chMsg)

	var msg interface{}
	for {
		got, err := socket.Recv()
		s.Log.Debug("Incoming %v message: %v.", socket.Type, &got)
		if err == nil {
			msg = &got
		} else {
			msg = err
		}
		select {
		case chMsg <- msg:
		// Quit on router closed.
		case <-s.Ctx.Done():
			return
		}
		// Quit on error.
		if err != nil {
			return
		}
	}
}

func (s *AbstractServer) getOneTimeMessageHandler(socket *types.Socket, removeKernelFrame bool, handler types.MessageHandler) types.MessageHandler {
	return func(msgType types.MessageType, msg *zmq4.Msg) error {
		skip := true
		retErr := errServeOnce
		req := socket.PendingReq
		if req != nil {
			_, reqId, _ := s.ExtractKernelFrames(req.Frames)
			_, rspId, _, newFrames := s.extractKernelFramesImpl(msg.Frames, removeKernelFrame)
			// Automatically remove kernel ID frame.
			if rspId != "" && removeKernelFrame {
				msg.Frames = newFrames
			}
			if reqId == rspId {
				skip = false
				// Clear pending request.
				socket.Mu.Lock()
				if socket.PendingReq == req {
					socket.PendingReq = nil
				}
				socket.Mu.Unlock()
			} else {
				// Response expired
				retErr = nil // Continue serving.
			}
		}
		// else: No request pending, must be timeout, skip.

		if !skip {
			err := handler(msgType, msg)
			if err != nil {
				s.Log.Warn("Error on handle %v message: %v", msgType, err)
			}
		} else {
			s.Log.Debug("Discard %v message: %v.", msgType, msg)
		}

		// Stop serving anyway.
		return retErr
	}
}
