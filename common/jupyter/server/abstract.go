package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/logger"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	errServeOnce = errors.New("break after served once")

	DefaultRequestTimeout  = 1 * time.Second
	ZMQDestFrameFormatter  = "dest.%s.req.%s" // dest.<kernel-id>.req.<req-id>
	ZMQDestFrameRecognizer = regexp.MustCompile(`^dest\.([0-9a-f-]+)\.req\.([0-9a-f-]+)$`)

	WROptionRemoveDestFrame = "RemoveDestFrame"

	JOffsetAutoDetect = -1
)

type WaitResponseOptionGetter func(key string) interface{}

// RequestDestination is an interface for describing the destination of a request.
type RequestDest interface {
	// ID returns the ID of the destination.
	ID() string

	// ExtractDestFrame extracts the destination info from the specified zmq4 frames.
	// Returns the destination ID, request ID and the offset to the jupyter frames.
	ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int)

	// AddDestFrame adds the destination frame to the specified zmq4 frames,
	// which should generate a unique request ID that can be extracted by ExtractDestFrame.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	AddDestFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte, reqID string)

	// RemoveDestFrame removes the destination frame from the specified zmq4 frames.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	RemoveDestFrame(frames [][]byte, jOffset int) (oldFrams [][]byte)
}

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
func (s *AbstractServer) Serve(server types.JupyterServerInfo, socket *types.Socket, handler types.MessageHandler) {
	if !atomic.CompareAndSwapInt32(&socket.Serving, 0, 1) {
		// Already serving.
		return
	}
	defer atomic.StoreInt32(&socket.Serving, 0)

	chMsg := make(chan interface{})
	var contd chan interface{}
	if socket.PendingReq == nil {
		go s.poll(socket, chMsg, nil)
		s.Log.Debug("Start serving %v messages", socket.Type)
	} else {
		contd = make(chan interface{})
		go s.poll(socket, chMsg, contd)
		s.Log.Debug("Start waiting for the resposne of %v requests", socket.Type)
	}

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
				err = handler(server, socket.Type, v)
			}

			// Stop sering on error.
			if err == io.EOF {
				s.Log.Debug("Socket %v closed.", socket.Type)
				return
			} else if err == errServeOnce {
				// Stop serving safely by setting and testing:
				// 1. Claim the serve routing will quit.
				atomic.StoreInt32(&socket.Serving, 0) // Set to 0 is safe because we are the only one running.
				// 2. Confirm no new request is pending.
				if socket.PendingReq.Len() == 0 {
					// Now any newer request will see the serving flag is 0 and will start a new serve routing.
					return
				}
				// 3. If a new request is pending, compete with the new serve routing to serve the request.
				if !atomic.CompareAndSwapInt32(&socket.Serving, 0, 1) {
					// We failed to set the flag to 1, quit.
					return
				}
			} else if err != nil {
				s.Log.Warn("Error on handle %v message: %v", socket.Type, err)
				return
			}
		}

		if socket.PendingReq != nil {
			contd <- &struct{}{}
			s.Log.Debug("Continue waiting for the resposne of %v requests(%d)", socket.Type, socket.PendingReq.Len())
		}
	}
}

// Request sends the request and wait until receiving the response corresponding to the given request or timeout.
// On being called, the function
//
//  0. Sends the request.
//  1. Queues the request.
//  2. A serve routine is started to wait on the socket for response. The serve routing will quit after a response is received and
//     the response matches the request. Specifically:
//     2.1. If no request is pending, the response times out and is discarded. The serve routing will quit.
//     2.2. If the response matches the pending request, the response is handled by "handler" and the serve routing will quit.
//     2.3. If the response does not match the pending request, the response is discarded (because the corresponding request is timed out and replaced by a new request),
//     and the serve routing will continue to wait for response corresponding to the pending request.
//  3. Wait for timeout. If the context is not cancellable, a default timeout will be applied.
//
// Available options:
//   - SROptionRemoveDestFrame bool Remove the destination frame from the response.
//
// Params:
//   - server: The jupyter server instance that will be passed to the handler to get the socket for forwarding the response.
//   - socket: The client socket to forward the request.
//   - req: The request to be sent.
//   - dest: The info of request destination that the WaitResponse can use to track individual request.
//   - handler: The handler to handle the response.
//   - getOption: The function to get the options.
func (s *AbstractServer) Request(ctx context.Context, server types.JupyterServerInfo, socket *types.Socket, req *zmq4.Msg, dest RequestDest, handler types.MessageHandler, done types.MessageDone, getOption WaitResponseOptionGetter) error {
	socket.InitPendingReq()
	// Normalize the request, we do not assume that the RequestDest implements the auto-detect feature.
	_, reqId, jOffset := dest.ExtractDestFrame(req.Frames)
	if reqId == "" {
		req.Frames, reqId = dest.AddDestFrame(req.Frames, dest.ID(), jOffset)
	}

	// Send request.
	if err := socket.Send(*req); err != nil {
		return err
	}

	// Track the pending request.
	socket.PendingReq.Store(reqId, types.GetMessageHandlerWrapper(handler, done))

	// Apply a default timeout
	var cancel context.CancelFunc
	if ctx.Done() == nil {
		ctx, cancel = context.WithTimeout(ctx, DefaultRequestTimeout)
	}

	// Use Serve to support timeout;
	// Late response will be ignored and serve routing will be stopped if no request is pending.
	if atomic.LoadInt32(&socket.Serving) == 0 {
		go s.Serve(server, socket, s.getOneTimeMessageHandler(socket, dest, getOption, nil)) // Pass nil as handler to discard any response without dest frame.
	}

	// Wait for timeout.
	go func() {
		<-ctx.Done()
		err := ctx.Err()
		if cancel != nil {
			cancel()
		}

		// Clear pending request.
		if pending, exist := socket.PendingReq.LoadAndDelete(reqId); exist {
			pending.Release()
			s.Log.Debug("Request(%p) %v", req, err)
		}
	}()
	return nil
}

func (s *AbstractServer) ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int) {
	_, jOffset = s.SkipIdentities(frames)
	if jOffset > 0 {
		matches := ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
		if len(matches) > 0 {
			destID = matches[1]
			reqID = matches[2]
		}
	}
	return
}

// GenerateKernelFrame appends a frame contains the kernel ID to the given ZMQ frames.
func (s *AbstractServer) AddDestFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte, reqID string) {
	// Automatically detect the dest frame.
	if jOffset == JOffsetAutoDetect {
		_, reqID, jOffset = s.ExtractDestFrame(frames)
		// If the dest frame is already there, we are done.
		if reqID != "" {
			return
		}
	}

	// Add dest frame just before "<IDS|MSG>" frame.
	newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(newFrames[jOffset+1:], frames[jOffset:])
	reqID = uuid.New().String()
	newFrames[jOffset] = []byte(fmt.Sprintf(ZMQDestFrameFormatter, destID, reqID))
	return
}

func (s *AbstractServer) RemoveDestFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	// Automatically detect the dest frame.
	if jOffset == JOffsetAutoDetect {
		var reqID string
		_, reqID, jOffset = s.ExtractDestFrame(frames)
		// If the dest frame is not available, we are done.
		if reqID == "" {
			return frames
		}
	}

	// Remove dest frame.
	if jOffset > 0 {
		copy(frames[jOffset-1:], frames[jOffset:])
		frames[len(frames)-1] = nil
		frames = frames[:len(frames)-1]
	}
	return frames
}

func (s *AbstractServer) SkipIdentities(frames [][]byte) (types.JupyterFrames, int) {
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

func (s *AbstractServer) poll(socket *types.Socket, chMsg chan<- interface{}, contd <-chan interface{}) {
	defer close(chMsg)

	var msg interface{}
	for {
		got, err := socket.Recv()
		s.Log.Debug("Incoming %v message.", socket.Type)
		s.Log.Trace("Incoming %v message: %v.", socket.Type, &got)
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

		// Wait for continue signal or quit.
		if contd != nil {
			proceed := <-contd
			if proceed == nil {
				return
			}
		}
	}
}

func (s *AbstractServer) getOneTimeMessageHandler(socket *types.Socket, dest RequestDest, getOption WaitResponseOptionGetter, defaultHandler types.MessageHandler) types.MessageHandler {
	return func(info types.JupyterServerInfo, msgType types.MessageType, msg *zmq4.Msg) error {
		// This handler returns errServeOnce if any to indicate that the server should stop serving.
		retErr := errServeOnce
		pendings := socket.PendingReq
		var matchReqId string
		var handler types.MessageHandler

		if pendings != nil {
			// We do not assume that the RequestDest implements the auto-detect feature.
			_, rspId, offset := dest.ExtractDestFrame(msg.Frames)
			if rspId == "" {
				// Unexpected response without request ID, fallback to default handler.
				handler = defaultHandler
			} else {
				matchReqId = rspId

				// Automatically remove kernel ID frame.
				if remove, _ := getOption(WROptionRemoveDestFrame).(bool); remove {
					msg.Frames = dest.RemoveDestFrame(msg.Frames, offset)
				}

				// Remove pending request and return registered handler. If timeout, the handler will be nil.
				if pending, exist := pendings.LoadAndDelete(rspId); exist {
					handler = pending.Handle // Handle will release the pending request once called.
				}
				// Continue serving if there are pending requests.
				if pendings.Len() > 0 {
					retErr = nil
				}
			}
		} else {
			// If PendingReq not available, fallback to default handler.
			handler = defaultHandler
		}

		if handler != nil {
			err := handler(info, msgType, msg)
			if err != nil {
				s.Log.Warn("Error on handle %v response: %v", msgType, err)
			}
		} else if matchReqId != "" {
			s.Log.Debug("Discard %v response to request %s.", msgType, matchReqId)
		} else {
			s.Log.Debug("Discard %v response: %v.", msgType, msg)
		}

		// Stop serving anyway.
		return retErr
	}
}
