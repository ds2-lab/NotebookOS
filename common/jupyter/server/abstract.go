package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
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
	// Sends a message. If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
	SendMessage(request types.Request, socket *types.Socket) error

	// SendMessage(requiresACK bool, socket *types.Socket, reqId string, req *zmq4.Msg, dest types.RequestDest, sourceKernel types.SourceKernel, offset int) error
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

	// If true, then we'll attempt to reconnect to the remote if we fail to receive an ACK from the remote
	// after `MaxNumRetries` attempts. Some servers dial while others listen; hence, we have this flag to
	// configure whether we should attempt to re-dial the remote or not.
	ReconnectOnAckFailure bool

	// The base interval for sleeping in between request resubmission attempts when a particular request is not ACK'd successfully.
	RetrySleepInterval time.Duration

	// The maximum for the sleep interval between non-ACK'd requests that are retried.
	MaxSleepInterval time.Duration

	// How long to wait for a request's response before giving up and cancelling the request.
	RequestTimeout time.Duration

	// If true, use jitter when generating sleep intervals for retransmitted ACKs.
	UseJitter bool

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
		// RetrySleepInterval: time.Millisecond * 500,
		UseJitter:        true,
		MaxSleepInterval: time.Second * 5,
		RequestTimeout:   time.Second * 60,
		acksReceived:     hashmap.NewSyncMap[string, bool](),
		ackChannels:      hashmap.NewSyncMap[string, chan struct{}](),
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

func (s *AbstractServer) handleAck(msg *zmq4.Msg, rspId string, socket *types.Socket) {
	goroutineId := goid.Get()

	s.numAcksReceived += 1

	if len(rspId) == 0 {
		_, rspId, _ = types.ExtractDestFrame(msg.Frames) // Redundant, will optimize later.
	}

	if len(rspId) == 0 {
		panic(fmt.Sprintf("Received %s message on socket %s [remoteName=%s] with no response ID: %v", socket.Type.String(), socket.Name, socket.RemoteName, msg))
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
		s.Log.Error("[gid=%d] [3] Received ACK for %s message %s via local socket %s [remoteSocket=%s]; however, we were not expecting an ACK for that message...", goroutineId, socket.Type.String(), rspId, socket.Name, socket.RemoteName)
	} else if ackReceived {
		s.Log.Error("[gid=%d] [4] Received ACK for %s message %s via local socket %s [remoteSocket=%s]; however, we already received an ACK for that message...", goroutineId, socket.Type.String(), rspId, socket.Name, socket.RemoteName)
	} else if !loaded {
		panic(fmt.Sprintf("[gid=%d] Did not have ACK entry for message %s", goroutineId, rspId))
	} else {
		panic(fmt.Sprintf("[gid=%d] Unexpected condition.", goroutineId))
	}
}

func (s *AbstractServer) sendAck(msg *zmq4.Msg, socket *types.Socket) error {
	goroutineId := goid.Get()

	// If we should ACK the message, then we'll ACK it.
	// For a message M, we'll send an ACK for M if the following are true:
	// (1) M is not an ACK itself
	// (2) This particular "instance" of AbstractServer is configured to ACK messages (as opposed to having ACKs disabled)
	// (3) The message was sent via the Shell socket or the Control socket. (We do not ACK heartbeats, IO messages, etc.)

	// s.Log.Debug("Message is of type %v and is NOT an ACK. Will send an ACK.", socket.Type)

	dstId, rspId, jOffset := types.ExtractDestFrame(msg.Frames)
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

	firstPart := fmt.Sprintf(utils.LightBlueStyle.Render("[gid=%d] Sending ACK for %v \"%v\""), goroutineId, socket.Type, parentHeader.MsgType)
	secondPart := fmt.Sprintf("(MsgId=%v, ReqId=%v)", utils.PurpleStyle.Render(rspId), utils.LightPurpleStyle.Render(parentHeader.MsgID))
	thirdPart := fmt.Sprintf(utils.LightBlueStyle.Render("message via %s: %v"), socket.Name, ack_msg)
	s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)

	err = socket.Send(ack_msg)
	if err != nil {
		s.Log.Error("[gid=%d] Error while sending ACK message: %v", goroutineId, err)
		return err
	}

	return nil
}

// Handle special messages that aren't necessarily meant to be forwarded.
//
// Returns a flag where 'true' means to continue calling the normal message handlers, and 'false' means to not forward the message around or do anything else for this message,
// including sending an ACK. (That is, there is no need to even send an ACK if 'false' is returned.)
func (s *AbstractServer) tryHandleSpecial(jMsg *types.JupyterMessage, socket *types.Socket) (bool, error) {
	goroutineId := goid.Get()

	if s.isMessageAnAck(jMsg, socket.Type) {
		firstPart := fmt.Sprintf(utils.GreenStyle.Render("[gid=%d] [1] Received ACK for %s \"%s\""), goroutineId, socket.Type, jMsg.ParentHeader.MsgType)
		secondPart := fmt.Sprintf("%s (JupyterID=%s, ParentJupyterId=%s)", utils.PurpleStyle.Render(jMsg.RequestId), utils.LightPurpleStyle.Render(jMsg.Header.MsgID), utils.LightPurpleStyle.Render(jMsg.ParentHeader.MsgID))
		thirdPart := fmt.Sprintf(utils.GreenStyle.Render("via local socket %s [remoteSocket=%s]: %v"), socket.Name, socket.RemoteName, jMsg)
		s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
		s.handleAck(jMsg.Msg, jMsg.RequestId, socket)

		return false, nil
	}

	if s.isGolangFrontendRegistration(jMsg, socket.Type) {
		jFrames := types.JupyterFrames(jMsg.Frames)
		var messageContent map[string]interface{}
		err := jFrames.DecodeContent(&messageContent)
		if err != nil {
			s.Log.Error("Failed to decode content of 'golang_frontend_registration' message because: %v", err)
			return false, err
		}

		senderId := messageContent["sender-id"].(string)
		socket.RemoteName = senderId

		s.Log.Debug(utils.LightBlueStyle.Render("Registered Golang Frontend: %s."), senderId)
	}

	return true, nil
}

// Serve starts serving the socket with the specified handler.
// The handler is passed as an argument to allow multiple sockets sharing the same handler.
func (s *AbstractServer) Serve(server types.JupyterServerInfo, socket *types.Socket, handler types.MessageHandler) {
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
		s.Log.Debug("[gid=%d] Start serving %v messages via %s [MyName=%s]", goroutineId, socket.Type.String(), socket.Name, s.Name)
	} else {
		contd = make(chan bool)
		go s.poll(socket, chMsg, contd)
		s.Log.Debug("[gid=%d] Start waiting for the response of %v requests via %s [MyName=%s]", goroutineId, socket.Type.String(), socket.Name, s.Name)
	}

	for {
		select {
		case <-socket.StopServingChan:
			s.Log.Debug("Received 'stop-serving' notification for %v socket [MyName: \"%s\"]. Will cease serving now.", socket.Type, s.Name)
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
			goroutineId = goid.Get()

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
				jMsg := types.NewJupyterMessage(v)

				if (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && !is_ack {
					firstPart := fmt.Sprintf(utils.BlueStyle.Render("[gid=%d] Received %s \"%s\" message"), goroutineId, socket.Type, jMsg.Header.MsgType)
					secondPart := fmt.Sprintf("%s (JupyterID=%s)", utils.PurpleStyle.Render(jMsg.RequestId), utils.LightPurpleStyle.Render(jMsg.Header.MsgID))
					thirdPart := fmt.Sprintf(utils.BlueStyle.Render("via local socket %s [remoteSocket=%s]: %v"), socket.Name, socket.RemoteName, jMsg)
					s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
				}

				// This checks if the message is an ACK.
				keep_processing, err := s.tryHandleSpecial(jMsg, socket)
				if err != nil {
					panic(fmt.Sprintf("[gid=%d] Fatal error while attempting to handle message as special. Message: %v. Error: %v.", goroutineId, msg, err))
				}

				if !keep_processing {
					if contd != nil {
						contd <- true
					}

					// Don't even send an ACK if we're not meant to keep processing this message.
					continue
				}

				// TODO: Optimize this. Lots of redundancy here, and that we also do the same parsing again later.
				// is_ack, err = s.isMessageAnAck(v, socket.Type)
				// if err != nil {
				// 	panic(fmt.Sprintf("[gid=%d] Could not determine if message is an 'ACK'. Message: %v. Error: %v.", goroutineId, msg, err))
				// }

				// if is_ack {
				// 	firstPart := fmt.Sprintf(utils.GreenStyle.Render("[gid=%d] [1] Received ACK for %s \"%s\""), goroutineId, socket.Type, jMsg.ParentHeader.MsgType)
				// 	secondPart := fmt.Sprintf("%s (JupyterID=%s, ParentJupyterId=%s)", utils.PurpleStyle.Render(jMsg.RequestId), utils.LightPurpleStyle.Render(jMsg.Header.MsgID), utils.LightPurpleStyle.Render(jMsg.ParentHeader.MsgID))
				// 	thirdPart := fmt.Sprintf(utils.GreenStyle.Render("via local socket %s [remoteSocket=%s]: %v"), socket.Name, socket.RemoteName, jMsg)
				// 	s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
				// 	s.handleAck(v, jMsg.RequestId, socket)
				// 	if contd != nil {
				// 		contd <- true
				// 	}
				// 	continue
				// } else if !is_ack && (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && s.ShouldAckMessages {
				// 	s.sendAck(v, socket)
				// }

				// Send an ACK if it is a Shell or Control message and we've been configured to ACK messages in general.
				//
				// AbstractServers that live on the Cluster Gateway and receive messages from frontend clients typically
				// do not ACK messages unless the frontend client is our custom Jupyter Golang client.
				if !is_ack && (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && s.ShouldAckMessages {
					s.sendAck(v, socket)
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
//   - sourceKernel: Entity that implements the types.SourceKernel interface and thus can add the types.SourceKernel frame to the message.
//   - dest: The info of request destination that the WaitResponse can use to track individual request.
//   - handler: The handler to handle the response.
//   - getOption: The function to get the options.
//   - requiresACK: If true, then we should expect an ACK for this message, and we should resend it if no ACK is receive before a timeout.
func (s *AbstractServer) Request(request types.Request, socket *types.Socket) error {
	goroutineId := goid.Get()

	socket.InitPendingReq()

	jMsg := request.Payload()
	reqId := request.RequestId()
	s.Log.Debug("[gid=%d] %s [socket=%s] is sending %s \"%s\" message from %s to %s [remoteSocket=%s].", goroutineId, s.Name, socket.Name, socket.Type.String(), jMsg.Header.MsgType, request.SourceID(), request.DestinationId(), socket.RemoteName)

	// dest.Unlock()
	_, alreadyRegistered := s.RegisterAck(request.RequestId())
	if alreadyRegistered {
		s.Log.Warn(utils.OrangeStyle.Render("[gid=%d] Already listening for ACKs for %v request %s. Current request with that ID is a \"%s\" message."), goroutineId, socket.Type, reqId, jMsg.Header.MsgType)
	}

	// Track the pending request.
	socket.PendingReq.Store(reqId, types.GetMessageHandlerWrapper(request))

	// Apply a default timeout
	// var cancel context.CancelFunc
	// if ctx.Done() == nil {
	// 	ctx, cancel = context.WithTimeout(ctx, s.RequestTimeout)
	// }
	ctx, cancel := request.ContextAndCancel()

	// Use Serve to support timeout;
	// Late response will be ignored and serve routing will be stopped if no request is pending.
	if atomic.LoadInt32(&socket.Serving) == 0 {
		go s.Serve(request.SocketProvider(), socket, s.getOneTimeMessageHandler(socket, request.ShouldDestFrameBeRemoved(), nil)) // Pass nil as handler to discard any response without dest frame.
	}

	var cleaned int32 = 0
	cleanUpRequest := func(err error) {
		// Claim the responsibility of cleaning up the request.
		//
		// The first goroutine to get here will clean the request, and this necessarily avoids the potential
		// race in which we clean up the pending request after we've already started resubmitting it.
		//
		// If the goroutine waiting on <-ctx.Done() gets here first, then it's fine.
		// If the goroutine executing the AbstractServer::SendMessage method gets here first,
		// then the pending request will be claimed and cleaned-up, and we can safely begin
		// to resubmit the pending request without fear that the <-ctx.Done() thread will
		// attempt to clean up while we're resubmitting.
		//
		// This assumes that I know how closures in Golang work.
		if atomic.CompareAndSwapInt32(&cleaned, 0, 1) {
			return
		}

		// Clear pending request.
		// TODO(Ben): There is conceivably a race here in which we call the code below AFTER we begin resubmitting the request, which causes it to be prematurely released.
		if pending, exist := socket.PendingReq.LoadAndDelete(reqId); exist {
			pending.Release()

			if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
				s.Log.Debug("Released pending request associated with %v request %s (%p), error: %v", socket.Type, reqId, jMsg, err)
			}
		}
	}

	// TODO: This is sort of incompatible with the ACK system. If we're waiting for ACKs -- and especially if we timeout, recreate socket, and retry,
	// then this may end up cancelling the request too early, or it'll happen multiple times. (We'll call the done() method multiple times.)
	//
	// Wait for timeout. Release the pending request, if it exists.
	go func() {
		<-ctx.Done()
		err := ctx.Err()
		if cancel != nil {
			cancel()
		}

		if errors.Is(err, context.DeadlineExceeded) {
			request.SetTimedOut()
		}

		// Clear pending request.
		// TODO(Ben): There is conceivably a race here in which we call the code below AFTER we begin resubmitting the request, which causes it to be prematurely released.
		cleanUpRequest(err)
	}()

	if err := s.SendMessage(request, socket); err != nil {
		s.Log.Debug("Error while sending %s \"%s\" message %s (Jupyter ID: %s): %v", socket.Type.String(), jMsg.Header.MsgType, reqId, jMsg.Header.MsgID, err)
		if cancel != nil {
			cancel()
		}

		cleanUpRequest(err)

		// Should we clear the pending request here? Should we call done() here?
		// If we do, then the automatic reconnect code is sort of in the wrong place.
		// We'd want it to be here, which could work, but this code here is more generic.
		// We'd need a flag in AbstractServer indicating whether we should try to reconnect on failure.

		return err
	}

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
func (s *AbstractServer) SendMessage(request types.Request, socket *types.Socket) error {
	goroutineId := goid.Get()
	reqId := request.RequestId()
	max_num_tries := request.MaxNumAttempts()
	num_tries := 0
	requiresACK := request.RequiresAck()

	// If the message requires an ACK, then we'll try sending it multiple times.
	// Otherwise, we'll just send it the one time.
	ackChan, _ := s.ackChannels.Load(reqId)
	if requiresACK {
		s.acksReceived.Store(reqId, false)

		if ackChan == nil {
			panic(fmt.Sprintf("We need an ACK for %v message %s; however, the ACK channel is nil.", socket.Type, reqId))
		}
	}

	for num_tries < max_num_tries {
		// Send request.
		if err := socket.Send(*request.Payload().Msg); err != nil {
			s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to send %v \"%s\" message on attempt %d/%d via %s because: %v"), goroutineId, socket.Type, request.JupyterMessageType(), num_tries+1, max_num_tries, socket.Name, err.Error())
			request.SetErred(err)
			return err
		}

		_, err := request.SetSubmitted()
		if err != nil {
			panic(fmt.Sprintf("Request transition to 'submitted' state failed for %s \"%s\" request %s (JupyterID=%s): %v", socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
		}

		if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
			firstPart := fmt.Sprintf(utils.LightBlueStyle.Render("[gid=%d] Sent %s \"%s\" message with"), goroutineId, socket.Type.String(), request.JupyterMessageType())
			secondPart := fmt.Sprintf("reqID=%v (JupyterID=%s)", utils.PurpleStyle.Render(reqId), utils.LightPurpleStyle.Render(request.JupyterMessageId()))
			thirdPart := fmt.Sprintf(utils.LightBlueStyle.Render("via %s. Attempt %d/%d. Message: %v"), socket.Name, num_tries+1, max_num_tries, request.Payload().Msg)
			s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
		}

		// If an ACK is required, then we'll block until the ACK is received, or until timing out, at which point we'll try sending the message again.
		if requiresACK {
			success := s.waitForAck(ackChan, time.Second*5)

			if success {
				if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
					firstPart := fmt.Sprintf(utils.GreenStyle.Render("[gid=%d] %v \"%s\" message"), goroutineId, socket.Type, request.JupyterMessageType())
					secondPart := fmt.Sprintf("%s (JupyterID=%s)", utils.PurpleStyle.Render(reqId), utils.LightPurpleStyle.Render(request.JupyterMessageId()))
					thirdPart := fmt.Sprintf(utils.GreenStyle.Render("has successfully been ACK'd on attempt %d/%d."), num_tries+1, max_num_tries)
					s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
				}

				_, err = request.SetProcessing()
				if err != nil {
					panic(fmt.Sprintf("Request transition to 'processing' state failed for %s \"%s\" request %s (JupyterID=%s): %v", socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
				}

				return nil
			} else {
				// Just to avoid going through the process of sleeping and updating the header if that was our last try.
				if (num_tries + 1) >= max_num_tries {
					s.Log.Error(utils.RedStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %s \"%s\" message %v (src: %v, dest: %v, jupyter ID: %v) from remote socket %s during attempt %d/%d. Giving up."), goroutineId, socket.Name, socket.Addr(), socket.Type.String(), request.JupyterMessageType(), reqId, request.SourceID(), request.DestinationId(), request.JupyterMessageId(), socket.RemoteName, num_tries+1, max_num_tries)

					_, err := request.SetTimedOut()
					if err != nil {
						panic(fmt.Sprintf("Request transition to 'timed-out' state failed for %s \"%s\" request %s (JupyterID=%s): %v", socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
					}

					break
				}

				// Sleep for an amount of time proportional to the number of attempts with some random jitter added.
				next_sleep_interval := s.getSleepInterval(num_tries)
				s.Log.Warn(utils.OrangeStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %s \"%s\" message %v (src: %v, dest: %v, jupyter ID: %v) from remote socket %s during attempt %d/%d. Serving: %d. Will sleep for %v before trying again."), goroutineId, socket.Name, socket.Addr(), socket.Type.String(), request.JupyterMessageType(), reqId, request.SourceID(), request.DestinationId(), request.JupyterMessageId(), socket.RemoteName, num_tries+1, max_num_tries, next_sleep_interval, atomic.LoadInt32(&socket.Serving))

				pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
				panic("Failed to receive ACK (after just one try).")

				time.Sleep(next_sleep_interval)
				num_tries += 1

				// err := s.UpdateMessageHeader(req, offset, sourceKernel)
				err := request.PrepareForResubmission()
				if err != nil {
					s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to update message header for %v \"%s\" message %v: %v"), goroutineId, request.JupyterMessageType(), socket.Type, reqId, err)
					request.SetErred(err)
					return err
				}
			}
		}
	}

	if requiresACK {
		s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to receive ACK for %v \"%s\" message %v (src: %v, dest: %v) from remote socket %s after %d attempt(s)."), goroutineId, request.JupyterMessageType(), socket.Type, reqId, request.SourceID(), request.DestinationId(), socket.RemoteName, max_num_tries)
		return jupyter.ErrNoAck
	}

	return nil
}

// Get the next sleep interval for a request, optionally including jitter.
//
// In general, jitter should probably be used:
// - https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func (s *AbstractServer) getSleepInterval(attempt int) time.Duration {
	// Compute sleep interval as BaseSleep * 2^NumTries.
	next_sleep_interval := s.RetrySleepInterval * time.Duration(math.Pow(2, float64(attempt)))

	// Clamp the sleep interval.
	next_sleep_interval = time.Duration(math.Min(float64(s.MaxSleepInterval), float64(next_sleep_interval)))

	if s.UseJitter {
		// Generating a random value between [0..next_sleep_interval)
		next_sleep_interval = time.Duration(rand.Float64() * (float64(next_sleep_interval)))
	}

	return next_sleep_interval
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
			s.Log.Warn("[gid=%d] Polling on %s socket %s is stopping. Router is closed.", goroutineId, socket.Type.String(), socket.Name)
			return
		}
		// Quit on error.
		if err != nil {
			// s.Log.Warn("[gid=%d] Polling is stopping. Received error: %v", goroutineId, err)
			s.Log.Warn("[gid=%d] Polling on %s socket %s is stopping. Received error: %v", goroutineId, socket.Type.String(), socket.Name, err)
			return
		}

		// Wait for continue signal or quit.
		if contd != nil {
			// s.Log.Debug("[gid=%d] %v socket %s is waiting to be instructed to continue.", goroutineId, socket.Type, socket.Name)
			proceed := <-contd
			if !proceed {
				s.Log.Warn("[gid=%d] Polling on %s socket %s is stopping.", goroutineId, socket.Type.String(), socket.Name)
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

// func (s *AbstractServer) isMessageAnAck(msg *zmq4.Msg, typ types.MessageType) bool {
// 	// ACKs are only sent for Shell/Control messages.
// 	if typ != types.ShellMessage && typ != types.ControlMessage {
// 		return false
// 	}

// 	return (msg.Header.MsgType == jupyter.MessageTypeACK)
// }

func (s *AbstractServer) isMessageAnAck(msg *types.JupyterMessage, typ types.MessageType) bool {
	// ACKs are only sent for Shell/Control messages.
	if typ != types.ShellMessage && typ != types.ControlMessage {
		return false
	}

	return (msg.Header.MsgType == jupyter.MessageTypeACK)
}

func (s *AbstractServer) isGolangFrontendRegistration(msg *types.JupyterMessage, typ types.MessageType) bool {
	// Golang frontend registration requests are only sent for Shell/Control messages.
	if typ != types.ShellMessage && typ != types.ControlMessage {
		return false
	}

	return (msg.Header.MsgType == jupyter.GolangFrontendRegistration)
}

func (s *AbstractServer) getOneTimeMessageHandler(socket *types.Socket, shouldDestFrameBeRemoved bool, defaultHandler types.MessageHandler) types.MessageHandler {
	return func(info types.JupyterServerInfo, msgType types.MessageType, msg *zmq4.Msg) error {
		// This handler returns errServeOnce if any to indicate that the server should stop serving.
		retErr := errServeOnce
		pendings := socket.PendingReq
		// var matchReqId string
		var handler types.MessageHandler
		var request types.Request

		if pendings != nil {
			// We do not assume that the types.RequestDest implements the auto-detect feature.
			_, rspId, offset := types.ExtractDestFrame(msg.Frames)
			if rspId == "" {
				s.Log.Warn("Unexpected response without request ID, fallback to default handler.")
				// Unexpected response without request ID, fallback to default handler.
				handler = defaultHandler
			} else {
				// s.Log.Debug(utils.BlueStyle.Render("Received response with ID=%s on socket %s"), rspId, socket.Name)
				// matchReqId = rspId

				// Automatically remove destination kernel ID frame.
				// if remove, _ := getOption(jupyter.WROptionRemoveDestFrame).(bool); remove {
				if shouldDestFrameBeRemoved {
					msg.Frames = types.RemoveDestFrame(msg.Frames, offset)
				}

				// if is_ack := s.isMessageAnAck(msg, socket.Type); is_ack {
				// 	s.Log.Debug(utils.GreenStyle.Render("[2] Received ACK for %v message %v via local socket %s [remoteSocket=%s]: %v"), socket.Type, rspId, socket.Name, socket.RemoteName, msg)
				// 	s.handleAck(msg, matchReqId, socket)
				// 	return nil
				// }

				// Remove pending request and return registered handler. If timeout, the handler will be nil.
				if pending, exist := pendings.LoadAndDelete(rspId); exist {
					handler = pending.Handle // Handle will release the pending request once called.
					request = pending.Request()
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
				s.Log.Error(utils.RedStyle.Render("Error on handle %v response: %v. Message: %v."), msgType, err, msg)
			}

			_, err = request.SetComplete()
			if err != nil {
				panic(fmt.Sprintf("Request transition to 'completed' state failed for %s \"%s\" request %s (JupyterID=%s): %v", socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
			}
		}

		// Stop serving anyway.
		return retErr
	}
}
