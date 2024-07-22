package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/petermattis/goid"

	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

var (
	errServeOnce = errors.New("break after served once")
	// errMessageNotFound = errors.New("message not found")
)

type WaitResponseOptionGetter func(key string) interface{}

type Sender interface {
	RequestDest

	// Sends a message. If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
	SendMessage(requiresACK bool, socket *types.Socket, reqId string, req *zmq4.Msg, dest RequestDest, sourceKernel SourceKernel, offset int) error
}

// RequestDestination is an interface for describing the destination of a request.
type RequestDest interface {
	// ID returns the ID of the destination.
	RequestDestID() string

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

// SourceKernel is an interface for describing the kernel from which a particular IO message originated.
// This interface is designed similarly to the RequestDest interface.
type SourceKernel interface {
	// ID returns the ID of the destination.
	SourceKernelID() string

	// ExtractSourceKernelFrame extracts the source kernel info from the specified zmq4 frames.
	// Returns the destination ID, request ID and the offset to the jupyter frames.
	ExtractSourceKernelFrame(frames [][]byte) (destID string, jOffset int)

	// AddSourceKernelFrame adds the source kernel to the specified zmq4 frames,
	// which should generate a unique request ID that can be extracted by ExtractSourceKernelFrame.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	AddSourceKernelFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte)

	// RemoveSourceKernelFrame removes the source kernel frame from the specified zmq4 frames.
	// Pass JOffsetAutoDetect to jOffset to let the function automatically detect the jupyter frames.
	RemoveSourceKernelFrame(frames [][]byte, jOffset int) (oldFrams [][]byte)

	ConnectionInfo() *types.ConnectionInfo
}

type Server interface {
	ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int)

	// GenerateKernelFrame appends a frame contains the kernel ID to the given ZMQ frames.
	AddDestFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte, reqID string)

	SkipIdentities(frames [][]byte) (types.JupyterFrames, int)
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

	// Used when sending ACKs. Basically, if this server uses Router sockets, then we need to prepend the ID to the messages.
	// Some servers use Dealer sockets, whcih don't need to prepend the ID.
	PrependId bool

	// If true, then will ACK messages upon receiving them (for CONTROL and SHELL sockets only).
	ShouldAckMessages bool

	// Local IPv4, for debugging purposes.
	localIpAdderss string

	// Unique name of the server, mostly for debugging.
	Name string

	// Keep track of the total number of ACKs we've received.
	// Primarily used in unit tests.
	numAcksReceived int

	// Maximum number of send attempts. Default: 5, when a message requires an ACK. Otherwise, 1.
	// If we fail to receive an ACK after sending this many messages, then we'll attempt to reconnect
	// to the remote entity if `reconnectOnAckFailure` is true.
	MaxNumRetries int

	// If true, then we'll attempt to reconnect to the remote if we fail to receive an ACK from the remote
	// after `MaxNumRetries` attempts. Some servers dial while others listen; hence, we have this flag to
	// configure whether we should attempt to re-dial the remote or not.
	ReconnectOnAckFailure bool

	// Map from Request ID to a boolean indicating whether the ACK has been received.
	// So, a value of false means that the request has not yet been received, whereas a value of true means that it has.
	acksReceived hashmap.BaseHashMap[string, bool]

	ackChannels hashmap.BaseHashMap[string, chan struct{}]
}

func New(ctx context.Context, info *types.ConnectionInfo, init func(server *AbstractServer)) *AbstractServer {
	var cancelCtx func()
	ctx, cancelCtx = context.WithCancel(ctx)

	server := &AbstractServer{
		Meta:            info,
		Ctx:             ctx,
		CancelCtx:       cancelCtx,
		Sockets:         &types.JupyterSocket{},
		numAcksReceived: 0,
		MaxNumRetries:   3,
		acksReceived:    hashmap.NewSyncMap[string, bool](),
		ackChannels:     hashmap.NewSyncMap[string, chan struct{}](),
		// Log:       logger.NilLogger, // To be overwritten by init.
	}
	init(server)
	server.Sockets.All = [5]*types.Socket{server.Sockets.HB, server.Sockets.Control, server.Sockets.Shell, server.Sockets.Stdin, server.Sockets.IO}
	for i, socket := range server.Sockets.All {
		if socket != nil {
			socket.Type = types.MessageType(i)
		}
	}

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		server.Log.Error("Could not resolve local IPv4 because: %v", err)
	} else {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		server.localIpAdderss = localAddr.IP.String()
	}

	return server
}

// Socket implements types.JupyterServerInfo.
func (s *AbstractServer) Socket(typ types.MessageType) *types.Socket {
	return s.Sockets.All[typ]
}

// String implements types.JupyterServerInfo.
func (s *AbstractServer) String() string {
	return "AbstractServer"
}

func (s *AbstractServer) Server() *BaseServer {
	return &BaseServer{s}
}

func (s *AbstractServer) NumAcksReceived() int {
	return s.numAcksReceived
}

func (s *AbstractServer) Listen(socket *types.Socket) error {
	if s.Meta.Transport != "tcp" {
		s.Log.Error("Unsupported transport specified: \"%s\". Only \"tcp\" is supported.", s.Meta.Transport)
		return types.ErrNotSupported
	}

	// s.Log.Debug("%s [%s] socket about to listen. Socket has port %d", socket.Type.String(), socket.Socket.Type(), socket.Port)

	err := socket.Listen(fmt.Sprintf("tcp://:%d", socket.Port))
	if err != nil {
		return err
	}

	// s.Log.Debug("%s [%s] socket started to listen. Socket has port %d", socket.Type.String(), socket.Socket.Type(), socket.Port)

	// Update the port number if it is 0.
	socket.Port = socket.Addr().(*net.TCPAddr).Port
	return nil
}

func (s *AbstractServer) handleAck(msg *zmq4.Msg, socket *types.Socket, dest RequestDest, rspId string) {
	goroutineId := goid.Get()

	s.numAcksReceived += 1

	if len(rspId) == 0 {
		_, rspId, _ = dest.ExtractDestFrame(msg.Frames) // Redundant, will optimize later.
	}

	ackChan, _ := s.ackChannels.Load(rspId)
	var (
		ackReceived bool
		loaded      bool
	)
	if ackReceived, loaded = s.acksReceived.Load(rspId); loaded && !ackReceived && ackChan != nil {
		// Notify that we received an ACK and return.
		s.acksReceived.Store(rspId, true)
		// s.Log.Debug("[gid=%d] [2] Received ACK for %v message %v via %s.", goroutineId, socket.Type, rspId, socket.Name)
		// s.Log.Debug("Notifying ACK: %v (%v): %v", rspId, socket.Type, msg)
		ackChan <- struct{}{}
		// s.Log.Debug("Notified ACK: %v (%v): %v", rspId, socket.Type, msg)
	} else if ackChan == nil { // If ackChan is nil, then that means we weren't expecting an ACK in the first place.
		// s.Log.Error("[gid=%d] [3] Received ACK for %v message %v via %s; however, we were not expecting an ACK for that message...", goroutineId, socket.Type, rspId, socket.Name)
	} else if ackReceived {
		// s.Log.Error("[gid=%d] [4] Received ACK for %v message %v via %s; however, we already received an ACK for that message...", goroutineId, socket.Type, rspId, socket.Name)
	} else if !loaded {
		panic(fmt.Sprintf("[gid=%d] Did not have ACK entry for message %s", goroutineId, rspId))
	} else {
		panic(fmt.Sprintf("[gid=%d] Unexpected condition.", goroutineId))
	}
}

func (s *AbstractServer) sendAck(msg *zmq4.Msg, socket *types.Socket, dest RequestDest) error {
	goroutineId := goid.Get()

	// If we should ACK the message, then we'll ACK it.
	// For a message M, we'll send an ACK for M if the following are true:
	// (1) M is not an ACK itself
	// (2) This particular "instance" of AbstractServer is configured to ACK messages (as opposed to having ACKs disabled)
	// (3) The message was sent via the Shell socket or the Control socket. (We do not ACK heartbeats, IO messages, etc.)

	// s.Log.Debug("Message is of type %v and is NOT an ACK. Will send an ACK.", socket.Type)

	dstId, rspId, jOffset := dest.ExtractDestFrame(msg.Frames)
	parentHeader, err := s.headerFromMessage(msg, jOffset)
	if err != nil {
		panic(err)
	}

	parentHeaderEncoded, err := json.Marshal(&parentHeader)
	if err != nil {
		panic(err)
	}

	headerMap := make(map[string]string)
	headerMap["msg_id"] = uuid.NewString()
	headerMap["date"] = time.Now().UTC().Format(time.RFC3339Nano)
	headerMap["msg_type"] = "ACK"
	header, _ := json.Marshal(&headerMap)

	var ack_msg zmq4.Msg
	if s.PrependId {
		ack_msg = zmq4.NewMsgFrom(
			msg.Frames[0],
			[]byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, dstId, rspId)),
			[]byte("<IDS|MSG>"),
			[]byte(""),
			header,
			parentHeaderEncoded,
			[]byte(fmt.Sprintf("%s (R: %s, L: %s)", socket.Name, s.Meta.IP, s.localIpAdderss)),
			[]byte(fmt.Sprintf("%s (%s)", time.Now().Format(time.RFC3339Nano), s.Name)))
	} else {
		ack_msg = zmq4.NewMsgFrom(
			[]byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, dstId, rspId)),
			[]byte("<IDS|MSG>"),
			[]byte(""),
			header,
			parentHeaderEncoded,
			[]byte(fmt.Sprintf("%s (R: %s, L: %s)", socket.Name, s.Meta.IP, s.localIpAdderss)),
			[]byte(fmt.Sprintf("%s (%s)", time.Now().Format(time.RFC3339Nano), s.Name)))
	}

	// s.Log.Debug(utils.LightBlueStyle.Render("[gid=%d] Sending ACK for %v \"%v\" (MsgId=%v, ReqId=%v) message via %s: %v"), goroutineId, socket.Type, parentHeader.MsgType, parentHeader.MsgID, rspId, socket.Name, ack_msg)

	err = socket.Send(ack_msg)
	if err != nil {
		s.Log.Error("[gid=%d] Error while sending ACK message: %v", goroutineId, err)
		return err
	}

	return nil
}

// Serve starts serving the socket with the specified handler.
// The handler is passed as an argument to allow multiple sockets sharing the same handler.
func (s *AbstractServer) Serve(server types.JupyterServerInfo, socket *types.Socket, dest RequestDest, handler types.MessageHandler, sendAcks bool) {
	goroutineId := goid.Get()

	if !atomic.CompareAndSwapInt32(&socket.Serving, 0, 1) {
		// Already serving.
		return
	}
	defer atomic.StoreInt32(&socket.Serving, 0)

	chMsg := make(chan interface{})
	var contd chan bool
	if socket.PendingReq == nil {
		go s.poll(socket, chMsg, nil)
		// s.Log.Debug("[gid=%d] Start serving %v messages via %s", goroutineId, socket.Type.String(), socket.Name)
	} else {
		contd = make(chan bool)
		go s.poll(socket, chMsg, contd)
		// s.Log.Debug("[gid=%d] Start waiting for the response of %v requests via %s", goroutineId, socket.Type.String(), socket.Name)
	}

	for {
		select {
		case <-socket.StopServingChan:
			s.Log.Debug("Received 'stop-serving' notification for %v socket. Will cease serving now.", socket.Type)
			// TODO: Do we need to notify the `poll` goroutine, or is sending `false` to the `cond` channel sufficient?
			if contd != nil {
				contd <- false
			}
			return
		case <-s.Ctx.Done():
			if contd != nil {
				contd <- false
			}
			return
		case msg := <-chMsg:
			if msg == nil {
				if contd != nil {
					contd <- false
				}
				return
			}

			var (
				err    error
				is_ack bool
			)
			switch v := msg.(type) {
			case error:
				err = v
			case *zmq4.Msg:
				if socket.Type == types.ShellMessage || socket.Type == types.ControlMessage {
					// s.Log.Debug(utils.BlueStyle.Render("[gid=%d] Received %v message of type %d with %d frame(s) via %s: %v"), goroutineId, socket.Type, v.Type, len(v.Frames), socket.Name, v)
				}

				// TODO: Optimize this. Lots of redundancy here, and that we also do the same parsing again later.
				is_ack, err = s.IsMessageAnAck(v, socket.Type)
				if err != nil {
					panic(fmt.Sprintf("[gid=%d] Could not determine if message is an 'ACK'. Message: %v. Error: %v.", goroutineId, msg, err))
				}

				_, rspId, _ := dest.ExtractDestFrame(v.Frames)
				if is_ack {
					// s.Log.Debug(utils.GreenStyle.Render("[gid=%d] [1] Received ACK for %v message %v via %s: %v"), goroutineId, socket.Type, rspId, socket.Name, msg)
					s.handleAck(v, socket, dest, rspId)
					if contd != nil {
						contd <- true
					}
					continue
				} else if !is_ack && (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && s.ShouldAckMessages {
					s.sendAck(v, socket, dest)
				}

				err = handler(server, socket.Type, v)

				// s.Log.Debug("[gid=%d] Handler for %v message \"%v\" has returned. Error: %v.", goroutineId, socket.Type, rspId, err)
			}

			// Stop serving on error.
			if err == io.EOF {
				// s.Log.Debug(utils.OrangeStyle.Render("[gid=%d] Socket %s [%v] closed."), goroutineId, socket.Name, socket.Type)
				if contd != nil {
					contd <- false
				}
				return
			} else if errors.Is(err, errServeOnce) || errors.Is(err, context.Canceled) {
				// Stop serving safely by setting and testing:
				// 1. Claim the serve routing will quit.
				atomic.StoreInt32(&socket.Serving, 0) // Set to 0 is safe because we are the only one running.
				// 2. Confirm no new request is pending.
				if socket.PendingReq == nil || socket.PendingReq.Len() == 0 {
					// Now any newer request will see the serving flag is 0 and will start a new serve routing.
					if contd != nil {
						contd <- false
					}
					// s.Log.Debug("[gid=%d] Done handling %s messages: %v.", goroutineId, socket.Type.String(), err)
					return
				}
				// 3. If a new request is pending, compete with the new serve routing to serve the request.
				if !atomic.CompareAndSwapInt32(&socket.Serving, 0, 1) {
					// We failed to set the flag to 1, quit.
					if contd != nil {
						contd <- false
					}
					// s.Log.Debug("[gid=%d] Done handling %s messages: %v.", goroutineId, socket.Type.String(), err)
					return
				}
			} else if err != nil {
				s.Log.Error(utils.RedStyle.Render("[gid=%d] Error on handle %s message: %v. Message: %v."), goroutineId, socket.Type.String(), err, msg)

				// s.Log.Debug("[gid=%d] Will NOT abort serving for now.", goroutineId)
				if contd != nil {
					contd <- true
				}
				continue

				// if errors.Is(err, context.Canceled) {
				// 	if contd != nil {
				// 		contd <- false
				// 	}
				// 	return
				// } else {
				// 	s.Log.Error("Will NOT abort serving for now.")
				// 	if contd != nil {
				// 		contd <- true
				// 	}
				// 	continue
				// }
			}
		}

		if socket.PendingReq != nil {
			contd <- true
			// s.Log.Debug("[gid=%d] Continue waiting for the resposne of %s requests(%d)", goroutineId, socket.Type.String(), socket.PendingReq.Len())
		}
	}
}

// Begin listening for an ACK for a message with the given ID.
func (s *AbstractServer) RegisterAck(reqId string) (chan struct{}, bool) {
	return s.ackChannels.LoadOrStore(reqId, make(chan struct{}, 1))
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
//   - sourceKernel: Entity that implements the SourceKernel interface and thus can add the SourceKernel frame to the message.
//   - dest: The info of request destination that the WaitResponse can use to track individual request.
//   - handler: The handler to handle the response.
//   - getOption: The function to get the options.
//   - requiresACK: If true, then we should expect an ACK for this message, and we should resend it if no ACK is receive before a timeout.
func (s *AbstractServer) Request(ctx context.Context, server types.JupyterServerInfo, socket *types.Socket, msg *zmq4.Msg, dest RequestDest, sourceKernel SourceKernel, handler types.MessageHandler, done types.MessageDone, getOption WaitResponseOptionGetter, requiresACK bool) error {
	goroutineId := goid.Get()

	socket.InitPendingReq()

	// Normalize the request, we do not assume that the RequestDest implements the auto-detect feature.
	_, reqId, jOffset := dest.ExtractDestFrame(msg.Frames)
	if reqId == "" {
		// var jFrames types.JupyterFrames
		// if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
		// jFrames = types.JupyterFrames(msg.Frames)
		// s.Log.Debug("[gid=%d] Adding destination '%s' to frames at offset %d now. Old frames: %v.", goroutineId, dest.RequestDestID(), jOffset, jFrames.String())
		// }
		msg.Frames, reqId = dest.AddDestFrame(msg.Frames, dest.RequestDestID(), jOffset)

		// if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
		// 	s.Log.Debug("[gid=%d] Added destination '%s' to frames at offset %d. New frames: %v.", goroutineId, dest.RequestDestID(), jOffset, jFrames.String())
		// }
	}

	jMsg := types.NewJupyterMessage(msg)
	if jMsg == nil {
		panic(fmt.Sprintf("Could not convert message to JupyterMessage: %v", msg))
	}

	// dest.Unlock()
	_, alreadyRegistered := s.RegisterAck(reqId)
	if alreadyRegistered {
		s.Log.Warn(utils.OrangeStyle.Render("[gid=%d] Already listening for ACKs for %v request %s. Current request with that ID is a \"%s\" message."), goroutineId, socket.Type, reqId, jMsg.Header.MsgType)
	}

	// Track the pending request.
	socket.PendingReq.Store(reqId, types.GetMessageHandlerWrapper(handler, done))

	// Apply a default timeout
	var cancel context.CancelFunc
	if ctx.Done() == nil {
		ctx, cancel = context.WithTimeout(ctx, time.Minute)
	}

	// Use Serve to support timeout;
	// Late response will be ignored and serve routing will be stopped if no request is pending.
	if atomic.LoadInt32(&socket.Serving) == 0 {
		go s.Serve(server, socket, dest, s.getOneTimeMessageHandler(socket, dest, getOption, nil), s.ShouldAckMessages) // Pass nil as handler to discard any response without dest frame.
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

			if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
				s.Log.Debug("Released pending request associated with %v request %s (%p), error: %v", socket.Type, reqId, jMsg, err)
			}
		}
	}()

	// go func() {
	// 	if err := s.SendMessage(requiresACK, socket, reqId, jMsg, dest, sourceKernel, jOffset); err != nil {
	// 		s.Log.Debug("Error while sending %v message %s: %v", socket.Type, reqId, err)
	// 		if cancel != nil {
	// 			cancel()
	// 		}
	// 	}
	// }()

	if err := s.SendMessage(requiresACK, socket, reqId, jMsg, dest, sourceKernel, jOffset); err != nil {
		s.Log.Debug("Error while sending %v message %s: %v", socket.Type, reqId, err)
		if cancel != nil {
			cancel()
		}

		return err
	}

	return nil
}

// Update the timestamp of the message's header so that it is signed with a different signature.
// This is used when re-sending un-ACK'd (unacknowledged) messages.
func (s *AbstractServer) updateMessageHeader(msg *types.JupyterMessage, offset int, sourceKernel SourceKernel) error {
	// We need to modify the message slightly to avoid a "duplicate signature" error.
	// To do this, we'll simply increment the timestamp of the message's header by a single microsecond.
	// We must first extract the header. After doing so, we'll re-encode the header with the new timestamp, and then regenerate the message's signature.
	header, err := s.headerFromMessage(msg.Msg, offset)
	if err != nil {
		s.Log.Error("Could not extract header from message while waiting for ACK timeout because: %v", err)
		s.Log.Error("offset: %d. Message: %v", offset, msg)
		return err
	}

	// Parse the date.
	date, err := time.Parse(time.RFC3339Nano, header.Date)
	if err != nil {
		s.Log.Error("Could not parse date \"%s\" from header of message after ACK timeout because: %v", header.Date, err)
		return err
	}

	// Add a single microsecond to the date.
	modifiedDate := date.Add(time.Microsecond)

	// Change the date in the header.
	header.Date = modifiedDate.Format(time.RFC3339Nano)

	if modifiedDate.Equal(date) {
		panic("Modified date is not different.")
	}

	// Re-encode the header.
	frames := types.JupyterFrames(msg.Frames)
	msg.Header = header

	err = frames[offset:].EncodeHeader(header)
	if err != nil {
		s.Log.Error("Failed to re-encode header of message after ACK timeout because: %v", err)
		return err
	}

	// Regenerate the signature.
	framesWithoutIdentities, _ := s.SkipIdentities(frames)
	framesWithoutIdentities, err = framesWithoutIdentities.Sign(sourceKernel.ConnectionInfo().SignatureScheme, []byte(sourceKernel.ConnectionInfo().Key)) // Ignore the error, log it if necessary.
	if err != nil {
		s.Log.Error("Failed to re-sign frames of message after updating date during ACK timeout because: %v", offset, err)
		return err
	}

	msg.Frames = frames
	return nil
}

// Wait for an ACK on the given channel, or time-out after the specified timeout.
func (s *AbstractServer) waitForAck(ackChan chan struct{}, timeout time.Duration) bool {
	select {
	case <-ackChan:
		{
			return true
		}
	case <-time.After(timeout):
		{
			return false
		}
	}
}

// Sends a message. If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
func (s *AbstractServer) SendMessage(requiresACK bool, socket *types.Socket, reqId string, req *types.JupyterMessage, dest RequestDest, sourceKernel SourceKernel, offset int) error {
	goroutineId := goid.Get()

	num_tries := 0
	var max_num_tries int

	if reqId == "" || offset <= 0 {
		_, reqId, offset = s.ExtractDestFrame(req.Frames)
	}

	// If the message requires an ACK, then we'll try sending it multiple times.
	// Otherwise, we'll just send it the one time.
	ackChan, _ := s.ackChannels.Load(reqId)
	if requiresACK {
		s.acksReceived.Store(reqId, false)
		max_num_tries = s.MaxNumRetries

		if ackChan == nil {
			panic(fmt.Sprintf("We need an ACK for %v message %s; however, the ACK channel is nil.", socket.Type, reqId))
		}
	} else {
		max_num_tries = 1
	}

	for num_tries < max_num_tries {
		// Send request.
		if err := socket.Send(*req.Msg); err != nil {
			s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to send %v \"%s\" message on attempt %d/%d via %s because: %v"), goroutineId, socket.Type, req.Header.MsgType, num_tries+1, max_num_tries, socket.Name, err.Error())
			return err
		}

		// if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
		// 	s.Log.Debug(utils.LightBlueStyle.Render("[gid=%d] Sent %v \"%s\" message with reqID=%v via %s. Src: %v. Dest: %v. Requires ACK: %v. Attempt %d/%d. Message: %v"), goroutineId, socket.Type, req.Header.MsgType, reqId, socket.Name, sourceKernel.SourceKernelID(), dest.RequestDestID(), requiresACK, num_tries+1, max_num_tries, req)
		// }

		// If an ACK is required, then we'll block until the ACK is received, or until timing out, at which point we'll try sending the message again.
		if requiresACK {
			success := s.waitForAck(ackChan, time.Second*5)

			if success {
				// if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
				// 	s.Log.Debug(utils.GreenStyle.Render("[gid=%d] %v \"%s\" message %v has successfully been ACK'd on attempt %d/%d."), goroutineId, socket.Type, req.Header.MsgType, reqId, num_tries+1, max_num_tries)
				// }
				return nil
			} else {
				s.Log.Error(utils.RedStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %v message %v (src: %v, dest: %v) during attempt %d/%d."), goroutineId, socket.Name, socket.Addr(), socket.Type, reqId, sourceKernel.SourceKernelID(), dest.RequestDestID(), num_tries+1, max_num_tries)
				// Just to avoid going through the process of sleeping and updating the header if that was our last try.
				if (num_tries + 1) >= max_num_tries {
					break
				}

				time.Sleep(time.Millisecond * 325 * time.Duration(num_tries)) // Sleep for an amount of time proportional to the number of attempts.
				num_tries += 1

				err := s.updateMessageHeader(req, offset, sourceKernel)
				if err != nil {
					s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to update message header for %v \"%s\" message %v: %v"), goroutineId, req.Header.MsgType, socket.Type, reqId, err)
					return err
				}
			}
		}
	}

	if requiresACK {
		s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to receive ACK for %v \"%s\" message %v (src: %v, dest: %v) after %d attempt(s)."), goroutineId, req.Header.MsgType, socket.Type, reqId, sourceKernel.SourceKernelID(), dest.RequestDestID(), max_num_tries)
		return jupyter.ErrNoAck
	}

	return nil
}

// Given a jOffset, attempt to extract a DestFrame from the given set of frames.
func (s *AbstractServer) extractDestFrame(frames [][]byte, jOffset int) (destID string, reqID string) {
	matches := jupyter.ZMQDestFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
	if len(matches) > 0 {
		destID = matches[1]
		reqID = matches[2]
	}

	return
}

func (s *AbstractServer) ExtractDestFrame(frames [][]byte) (destID string, reqID string, jOffset int) {
	_, jOffset = s.SkipIdentities(frames)
	if jOffset > 0 {
		destID, reqID = s.extractDestFrame(frames, jOffset)
	}
	return
}

// GenerateKernelFrame appends a frame contains the kernel ID to the given ZMQ frames.
func (s *AbstractServer) AddDestFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte, reqID string) {
	// Automatically detect the dest frame.
	if jOffset == jupyter.JOffsetAutoDetect {
		_, reqID, jOffset = s.ExtractDestFrame(frames)
		// If the dest frame is already there, we are done.
		if reqID != "" {
			// s.Log.Debug("Destination frame found. ReqID: %s", reqID)
			return frames, reqID
		}
	}

	// Add dest frame just before "<IDS|MSG>" frame.
	newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(newFrames[jOffset+1:], frames[jOffset:])
	reqID = uuid.New().String()
	newFrames[jOffset] = []byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, destID, reqID))

	return
}

func (s *AbstractServer) framesToString(frames [][]byte) string {
	buf := new(bytes.Buffer)
	buf.WriteString("Msg{Frames:{")
	for i, frame := range frames {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(buf, "%q", frame)
	}
	buf.WriteString("}}")
	return buf.String()
}

func (s *AbstractServer) RemoveDestFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	// Automatically detect the dest frame.
	if jOffset == jupyter.JOffsetAutoDetect {
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

func (s *AbstractServer) ExtractSourceKernelFrame(frames [][]byte) (kernelID string, jOffset int) {
	// _, jOffset = s.SkipIdentities(frames)
	// if jOffset > 0 {
	// 	// If there's no DEST frame, then the frame immediately preceeding the identities will be the "Source Kernel" frame.
	// 	matches := ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
	// 	if len(matches) > 0 {
	// 		kernelID = matches[1]
	// 		return
	// 	}
	// 	// Alternatively, there may be a DEST frame immediately preceeding the identities,
	// 	// in which case we need to check the frame before the DEST frame.
	// 	destID, reqID := s.extractDestFrame(frames, jOffset)

	// 	// If these are non-empty strings, then there was indeed a Dest frame. So, we need to check the frame before that.
	// 	if destID != "" && reqID != "" {
	// 		jOffset = jOffset - 1 // Update this since we have to return it along with the kernel ID.

	// 		// If the offset is now 0, then there's no preceeding frames, so we can just return.
	// 		if jOffset == 0 {
	// 			return
	// 		}

	// 		matches := ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(frames[jOffset-1]))
	// 		if len(matches) > 0 {
	// 			kernelID = matches[1]
	// 			return
	// 		}
	// 	}
	// }
	// return

	matches := jupyter.ZMQSourceKernelFrameRecognizer.FindStringSubmatch(string(frames[0]))
	if len(matches) > 0 {
		kernelID = matches[1]
	}

	return
}

func (s *AbstractServer) AddSourceKernelFrame(frames [][]byte, kernelID string, jOffset int) (newFrames [][]byte) {
	// // Automatically detect the "source kernel" frame.
	// if jOffset == JOffsetAutoDetect {
	// 	var existingKernelID string
	// 	existingKernelID, jOffset = s.ExtractSourceKernelFrame(frames)
	// 	// If the "source kernel" frame is already there, we are done.
	// 	if existingKernelID != "" {
	// 		return
	// 	}
	// }

	// // Add "source kernel" frame just before "<IDS|MSG>" frame.
	// newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	// copy(newFrames[jOffset+1:], frames[jOffset:])
	// newFrames[jOffset] = []byte(fmt.Sprintf(ZMQSourceKernelFrameFormatter, kernelID))
	// return

	// Add "source kernel" frame to the very beginning.
	newFrames = append(frames, nil) // Let "append" allocate a new slice if necessary.
	copy(newFrames[1:], frames[0:])
	newFrames[0] = []byte(fmt.Sprintf(jupyter.ZMQSourceKernelFrameFormatter, kernelID))
	return
}

func (s *AbstractServer) RemoveSourceKernelFrame(frames [][]byte, jOffset int) (removed [][]byte) {
	// Automatically detect the "source kernel" frame.
	// if jOffset == JOffsetAutoDetect {
	// 	var existingKernelID string
	// 	existingKernelID, jOffset = s.ExtractSourceKernelFrame(frames)
	// 	// If the "source kernel" frame is not available, we are done.
	// 	if existingKernelID == "" {
	// 		return frames
	// 	}
	// }

	// // Remove "source kernel" frame.
	// if jOffset > 0 {
	// 	copy(frames[jOffset-1:], frames[jOffset:])
	// 	frames[len(frames)-1] = nil
	// 	frames = frames[:len(frames)-1]
	// }

	existingKernelID, _ := s.ExtractSourceKernelFrame(frames)
	if existingKernelID != "" {
		return frames[1:]
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

func (s *AbstractServer) poll(socket *types.Socket, chMsg chan<- interface{}, contd <-chan bool) {
	goroutineId := goid.Get()
	defer close(chMsg)

	var msg interface{}
	for {
		got, err := socket.Recv()

		if err == nil {
			msg = &got
			// s.Log.Debug("[gid=%d] Got message from socket: %v", goroutineId, types.JupyterFrames(got.Frames))
		} else {
			msg = err
			// s.Log.Error("[gid=%d] Received error upon trying to read %v message: %v", goroutineId, socket.Type, err)
		}
		select {
		case chMsg <- msg:
		// Quit on router closed.
		case <-s.Ctx.Done():
			// s.Log.Warn("[gid=%d] Polling is stopping. Router is closed.", goroutineId)
			return
		}
		// Quit on error.
		if err != nil {
			// s.Log.Warn("[gid=%d] Polling is stopping. Received error: %v", goroutineId, err)
			return
		}

		// Wait for continue signal or quit.
		if contd != nil {
			// s.Log.Debug("[gid=%d] %v socket %s is waiting to be instructed to continue.", goroutineId, socket.Type, socket.Name)
			proceed := <-contd
			if !proceed {
				s.Log.Warn("[gid=%d] Polling is stopping.", goroutineId)
				return
			}
			// s.Log.Debug("[gid=%d] %v socket %s has been instructed to continue.", goroutineId, socket.Type, socket.Name)
		}
	}
}

func (s *AbstractServer) headerFromMessage(msg *zmq4.Msg, offset int) (*types.MessageHeader, error) {
	jFrames := types.JupyterFrames(msg.Frames[offset:])
	if err := jFrames.Validate(); err != nil {
		s.Log.Error("Failed to validate message frames while extracting header: %v", err)
		return nil, err
	}

	var header types.MessageHeader
	if err := jFrames.DecodeHeader(&header); err != nil {
		s.Log.Error("Failed to decode header \"%v\" from message frames: %v", string(jFrames[types.JupyterFrameHeader]), err)
		return nil, err
	}

	return &header, nil
}

func (s *AbstractServer) IsMessageAnAck(msg *zmq4.Msg, typ types.MessageType) (bool, error) {
	// ACKs are only sent for Shell/Control messages.
	if typ != types.ShellMessage && typ != types.ControlMessage {
		return false, nil
	}

	_, offset := s.SkipIdentities(msg.Frames)
	header, err := s.headerFromMessage(msg, offset)
	if err != nil {
		return false, err
	}

	return (header.MsgType == jupyter.MessageTypeACK), nil
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
				s.Log.Warn("Unexpected response without request ID, fallback to default handler.")
				// Unexpected response without request ID, fallback to default handler.
				handler = defaultHandler
			} else {
				// s.Log.Debug(utils.BlueStyle.Render("Received response with ID=%s on socket %s"), rspId, socket.Name)
				matchReqId = rspId

				// Automatically remove destination kernel ID frame.
				if remove, _ := getOption(jupyter.WROptionRemoveDestFrame).(bool); remove {
					msg.Frames = dest.RemoveDestFrame(msg.Frames, offset)
				}

				// is_ack, err := s.IsMessageAnAck(msg, socket.Type)
				// if err != nil {
				// 	panic(fmt.Sprintf("Could not determine if message is an 'ACK'. Offset: %d. Message: %v. Error: %v.", offset, msg, err))
				// }

				// TODO: Check if message is an ACK message.
				// If so, then we'll report that the message was ACK'd, and we'll wait for the "actual" response.
				ackChan, _ := s.ackChannels.Load(matchReqId)
				// if is_ack {
				// 	s.Log.Debug("[2] Received ACK via %s: %v (%v): %v", socket.Name, rspId, socket.Type, msg)
				// 	s.handleAck(msg, socket, dest)
				// 	return nil
				// }

				// TODO: Do we need this?
				if ackReceived, loaded := s.acksReceived.Load(rspId); loaded && !ackReceived && ackChan != nil {
					s.Log.Debug(utils.PurpleStyle.Render("Received actual response (rather than ACK) for %v request %v via %s before receiving ACK. Marking request as acknowledeged anyway."), socket.Type, rspId, socket.Name)
					// Notify that we received an ACK and return.
					s.acksReceived.Store(rspId, true)
					ackChan <- struct{}{}
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
				s.Log.Warn(utils.RedStyle.Render("Error on handle %v response: %v. Message: %v."), msgType, err, msg)
			}
		} else if matchReqId != "" {
			// s.Log.Debug(utils.OrangeStyle.Render("Discard %v response to request %s."), msgType, matchReqId)
		}

		// Stop serving anyway.
		return retErr
	}
}
