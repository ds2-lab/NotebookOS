package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"io"
	"log"
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

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

const (
	// GolangFrontendRegistrationRequest is a message type sent by our custom Golang Jupyter frontend clients.
	// These inform us that we should expect the frontend to send ACKs, which does not happen for "regular" Jupyter frontends.
	GolangFrontendRegistrationRequest = "golang_frontend_registration_request"

	// DefaultMessageQueueCapacity is the default capacity of the "received" messages queue (which is a chan).
	//DefaultMessageQueueCapacity = 64

	ACK = "ACK"
)

var (
	errServeOnce              = errors.New("break after served once")
	errMissingSignatureScheme = errors.New("signature scheme has not been set")
	errMissingKey             = errors.New("key has not been set")

	ErrRequestOutOfAttempts = errors.New("request failed to be acknowledged within configured number of attempts")
	// errMessageNotFound = errors.New("message not found")
)

type WaitResponseOptionGetter func(key string) interface{}

// AbstractServer implements the basic socket serving useful for a Jupyter server. Embed this struct in your server implementation.
type AbstractServer struct {
	Meta *types.ConnectionInfo

	// MessagingMetricsProvider is an interface that enables the recording of metrics observed by the AbstractServer.
	// This should be assigned a value in the init function passed as a parameter in the "constructor" of the AbstractServer.
	MessagingMetricsProvider metrics.MessagingMetricsProvider

	// ctx of this server and a func to cancel it.
	Ctx       context.Context
	CancelCtx func()

	// ZMQ sockets
	Sockets *types.JupyterSocket

	// logger
	Log logger.Logger

	// DebugMode is a config parameter. When enabled, the server will embed metrics.RequestTrace structs
	// in the first "buffer" frame of Jupyter ZMQ messages.
	DebugMode bool

	// PrependId is used when sending ACKs. Basically, if this server uses Router sockets, then we need to prepend the ID to the messages.
	// Some servers use Dealer sockets, which don't need to prepend the ID.
	PrependId bool

	// MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements
	// for the ZMQ messages that we're forwarding back and forth between the server components.
	//
	// MessageAcknowledgementsEnabled is controlled by the "acks_enabled" field of the configuration file.
	MessageAcknowledgementsEnabled bool

	// MessageQueueCapacity is the amount that the message queue (which is a chan) is buffered
	//MessageQueueCapacity int

	// If true, then will ACK messages upon receiving them (for CONTROL and SHELL sockets only).
	ShouldAckMessages bool

	// The ID of the node/component on which the server resides.
	ComponentId string

	// localIpAdderss is the local IPv4, for debugging purposes.
	localIpAdderss string

	// Name is the unique name of the server, mostly for debugging.
	Name string

	// numAcksReceived keeps track of the total number of ACKs we've received.
	// Primarily used in unit tests.
	numAcksReceived atomic.Int32

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

	// The node type of THIS server.
	nodeType metrics.NodeType

	// If true, use jitter when generating sleep intervals for retransmitted ACKs.
	UseJitter bool

	// Map from Request ID to a boolean indicating whether the ACK has been received.
	// So, a value of false means that the request has not yet been received, whereas a value of true means that it has.
	acksReceived hashmap.BaseHashMap[string, bool]

	// ackChannels is a mapping from request ID to the channel on which the ACK delivery notification for that
	// message is to be sent.
	ackChannels hashmap.BaseHashMap[string, chan struct{}]

	// discardACKs is a map from Request ID to struct{}{} (basically just a set) of messages whose ACKs we
	// explicitly don't need.
	discardACKs hashmap.BaseHashMap[string, struct{}]

	// NumSends is the number of times we've sent a message, including resubmissions.
	NumSends atomic.Int32
	// NumUniqueSends is the number of times we've sent a message, excluding resubmissions.
	NumUniqueSends atomic.Int32

	// PanicOnFirstFailedSend is a flag which directs the AbstractServer to panic the first time a message
	// is not acknowledged within its configured timeout interval.
	//
	// Normally, the message will simply be resubmitted. If PanicOnFirstFailedSend is true, then the server will
	// instead panic, rather than resend the message.
	PanicOnFirstFailedSend bool
}

func New(ctx context.Context, info *types.ConnectionInfo, nodeType metrics.NodeType, init func(server *AbstractServer)) *AbstractServer {
	var cancelCtx func()
	ctx, cancelCtx = context.WithCancel(ctx)

	server := &AbstractServer{
		Meta:                           info,
		Ctx:                            ctx,
		CancelCtx:                      cancelCtx,
		MessageAcknowledgementsEnabled: true, // Default to true
		Sockets:                        &types.JupyterSocket{},
		UseJitter:                      true,
		MaxSleepInterval:               time.Second * 5,
		RequestTimeout:                 time.Second * 60,
		acksReceived:                   hashmap.NewSyncMap[string, bool](),
		ackChannels:                    hashmap.NewSyncMap[string, chan struct{}](),
		discardACKs:                    hashmap.NewSyncMap[string, struct{}](),
		nodeType:                       nodeType,
		PanicOnFirstFailedSend:         false,
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

// NumAcknowledgementsReceived returns the number of ACKs that the server has received.
func (s *AbstractServer) NumAcknowledgementsReceived() int32 {
	return s.numAcksReceived.Load()
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

func (s *AbstractServer) handleAck(jMsg *types.JupyterMessage, rspId string, socket *types.Socket) {
	goroutineId := goid.Get()

	if !s.MessageAcknowledgementsEnabled {
		s.Log.Warn("Received ACK even though ACKs are disabled. The ACK will be discarded. ACK: %s", jMsg.String())
		return
	}

	s.numAcksReceived.Add(1)

	if len(rspId) == 0 {
		_, rspId, _ = jMsg.JupyterFrames.ExtractDestFrame(false) // Redundant, will optimize later.
	}

	if len(rspId) == 0 {
		panic(fmt.Sprintf("Received %s message on socket %s [remoteName=%s] with no response ID: %v", socket.Type.String(), socket.Name, socket.RemoteName, jMsg))

		//if !socket.IsGolangFrontend { // Golang frontend sockets do not send messages the same way. So, if it is a frontend socket, then this is OK. If not, then we panic.
		//	panic(fmt.Sprintf("Received %s message on socket %s [remoteName=%s] with no response ID: %v", socket.Type.String(), socket.Name, socket.RemoteName, msg))
		//} else {
		//	// TODO: How to get rspId in this case...? Maybe we can add it as metadata to the replies sent to Golang Frontends?
		//	// Should we fallback to using the Jupyter request ID or something?
		//	s.Log.Warn("Received ACK from Golang Frontend %s; however, we have no way to recover the response ID at this point...", socket.RemoteName)
		//	return
		//}
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
	} else if ackReceived {
		s.Log.Warn("[gid=%d] [4] Duplicate ACK received for %s message %s via local socket %s [remoteSocket=%s]",
			goroutineId, socket.Type.String(), rspId, socket.Name, socket.RemoteName)
	} else if ackChan == nil {
		// If ackChan is nil, then that means we weren't expecting an ACK in the first place.
		//
		// For messages that we explicitly indicate do not require an ACK, we make note of this, just for debugging purposes.
		// If we receive an ACK for a message, and we have no "ack channel" registered for that message, then we just do a sanity
		// check and look in the `discardACKs` map to see if we know that we didn't want an ACK for this. If we managed to
		// get an ACK anyway, then that's fine. But if we don't see an associated entry in `discardACKs`, then we *really*
		// weren't expecting to get this ACK, so we'll print a warning message.
		//
		// (Even if we don't need an ACK, components will still send one. It's just that we're adjusting the send protocol
		// to not wait for an ACK before returning -- and to not resend if no ACKs are received. Not requiring an ACK
		// does not prevent ACKs from being transmitted. In the future, we could embed a piece of metadata in the request
		// that says "you don't need to ACK this message" if we really don't want the ACK to be sent for whatever reason.)
		if _, loaded = s.discardACKs.LoadAndDelete(rspId); !loaded {
			s.Log.Warn("[gid=%d] [3] Unexpected (and unwelcome) ACK received for %s \"%s\" message %s (JupyterID=\"%s\") via local socket %s [remoteSocket=%s]",
				goroutineId, socket.Type.String(), jMsg.JupyterParentMessageType(), rspId, jMsg.JupyterParentMessageId(), socket.Name, socket.RemoteName)
		} else {
			s.Log.Warn("[gid=%d] Unexpected (but welcome) ACK received for %s \"%s\" message %s (JupyterID=\"%s\").",
				goroutineId, socket.Type.String(), jMsg.JupyterParentMessageType(), rspId, jMsg.JupyterParentMessageId())
		}
	} else {
		s.Log.Error(utils.RedStyle.Render("No conditions matched for ACK message: %s"), jMsg.String())
	}
}

func (s *AbstractServer) sendAck(msg *types.JupyterMessage, socket *types.Socket) error {
	// If ACKs are disabled, just return immediately.
	if !s.MessageAcknowledgementsEnabled {
		return nil
	}

	goroutineId := goid.Get()

	// If we should ACK the message, then we'll ACK it.
	// For a message M, we'll send an ACK for M if the following are true:
	// (1) M is not an ACK itself
	// (2) This particular "instance" of AbstractServer is configured to ACK messages (as opposed to having ACKs disabled)
	// (3) The message was sent via the Shell socket or the Control socket. (We do not ACK heartbeats, IO messages, etc.)

	// The parent header of the ACK will be the header of the message we're ACK-ing.
	messageHeader, err := msg.GetHeader()
	if err != nil {
		panic(err)
	}

	parentHeaderEncoded, err := json.Marshal(messageHeader)
	if err != nil {
		panic(err)
	}

	headerMap := make(map[string]string)
	headerMap["msg_id"] = uuid.NewString()
	headerMap["date"] = time.Now().UTC().Format(time.RFC3339Nano)
	headerMap["msg_type"] = ACK
	header, _ := json.Marshal(&headerMap)

	var ackMsg zmq4.Msg
	if s.PrependId {
		ackMsg = zmq4.NewMsgFrom(
			msg.JupyterFrames.Frames[0],
			[]byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, msg.DestinationId, msg.RequestId)),
			[]byte("<IDS|MSG>"),
			[]byte(""),
			header,
			parentHeaderEncoded,
			[]byte(fmt.Sprintf("%s (RemoteIP: %s, LocalIP: %s)", socket.Name, s.Meta.IP, s.localIpAdderss)),
			[]byte(fmt.Sprintf("%s (%s)", time.Now().Format(time.RFC3339Nano), s.Name)))
	} else {
		ackMsg = zmq4.NewMsgFrom(
			[]byte(fmt.Sprintf(jupyter.ZMQDestFrameFormatter, msg.DestinationId, msg.RequestId)),
			[]byte("<IDS|MSG>"),
			[]byte(""),
			header,
			parentHeaderEncoded,
			[]byte(fmt.Sprintf("%s (RemoteIP: %s, LocalIP: %s)", socket.Name, s.Meta.IP, s.localIpAdderss)),
			[]byte(fmt.Sprintf("%s (%s)", time.Now().Format(time.RFC3339Nano), s.Name)))
	}

	messageHeader, err = msg.GetHeader()
	if err != nil {
		panic(err)
	}

	firstPart := fmt.Sprintf(utils.LightBlueStyle.Render("[gid=%d] Sending ACK for %v \"%v\""), goroutineId, socket.Type, messageHeader.MsgType)
	secondPart := fmt.Sprintf("(MsgId=%v, ReqId=%v)", utils.PurpleStyle.Render(msg.RequestId), utils.LightPurpleStyle.Render(messageHeader.MsgID))
	thirdPart := fmt.Sprintf(utils.LightBlueStyle.Render("message via %s: %v"), socket.Name, ackMsg)
	s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)

	sendStart := time.Now()
	err = socket.Send(ackMsg)
	if err != nil {
		s.Log.Error("[gid=%d] Error while sending ACK message: %v", goroutineId, err)
		return err
	}
	sendDuration := time.Since(sendStart)

	firstPart = fmt.Sprintf(utils.LightBlueStyle.Render("[gid=%d] Successfully sent ACK for %s \"%s\""), goroutineId, socket.Type.String(), messageHeader.MsgType.String())
	secondPart = fmt.Sprintf("(MsgId=%v, ReqId=%v)", utils.PurpleStyle.Render(msg.RequestId), utils.LightPurpleStyle.Render(messageHeader.MsgID))
	thirdPart = fmt.Sprintf(utils.LightBlueStyle.Render("message via %s in %v."), socket.Name, sendDuration)
	s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)

	s.printSendLatencyWarning(time.Since(sendStart), messageHeader.MsgType.String(), msg.RequestId)

	s.NumSends.Add(1)
	s.NumUniqueSends.Add(1)

	if metricError := s.MessagingMetricsProvider.SentMessage(s.ComponentId, sendDuration, s.nodeType, socket.Type, ACK); metricError != nil {
		s.Log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if metricError := s.MessagingMetricsProvider.SentMessageUnique(s.ComponentId, s.nodeType, socket.Type, ACK); metricError != nil {
		s.Log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	return nil
}

// Check and see if the message is "special", such as an ACK. Special messages aren't necessarily meant to be forwarded. We handle them differently.
//
// Returns a flag where 'true' means to continue calling the normal message handlers, and 'false' means to not forward the message around or do anything else for this message,
// including sending an ACK. (That is, there is no need to even send an ACK if 'false' is returned.)
func (s *AbstractServer) tryHandleSpecialMessage(jMsg *types.JupyterMessage, socket *types.Socket) (bool, error) {
	// This is generally unused for now; we don't do anything special for Golang frontends as of right now.
	if s.isMessageGolangFrontendRegistrationRequest(jMsg, socket.Type) {
		s.Log.Debug("Golang frontend registration JFrames: %v", jMsg.JupyterFrames)

		var messageContent map[string]interface{}
		err := jMsg.JupyterFrames.DecodeContent(&messageContent)
		if err != nil {
			s.Log.Error("Failed to decode content of 'golang_frontend_registration_request' message because: %v", err)
			return false, err
		}

		s.Log.Debug("Decoded Golang registration content: %v", messageContent)

		senderId := messageContent["sender-id"].(string)
		socket.RemoteName = senderId
		socket.IsGolangFrontend = true

		s.Log.Debug(utils.LightBlueStyle.Render("Registered Golang Frontend: %s."), senderId)

		return false, nil
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

	chMsg := make(chan interface{}) // , s.MessageQueueCapacity)
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
				err   error
				isAck bool
			)
			switch v := msg.(type) {
			case error:
				err = v
			case *types.JupyterMessage:
				jMsg := v

				if (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && !jMsg.IsAck() {
					firstPart := fmt.Sprintf(utils.BlueStyle.Render("[gid=%d] Handling %s \"%s\" message"), goroutineId, socket.Type, jMsg.JupyterMessageType())
					secondPart := fmt.Sprintf("'%s' (JupyterID=%s)", utils.PurpleStyle.Render(jMsg.RequestId), utils.LightPurpleStyle.Render(jMsg.JupyterMessageId()))
					thirdPart := fmt.Sprintf(utils.BlueStyle.Render("via local socket %s [remoteSocket=%s]: %v"), socket.Name, socket.RemoteName, jMsg)
					s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
				}

				// This checks if the message is an ACK.
				keepProcessing, err := s.tryHandleSpecialMessage(jMsg, socket)
				if err != nil {
					panic(fmt.Sprintf("[gid=%d] Fatal error while attempting to handle message as special. Message: %v. Error: %v.", goroutineId, msg, err))
				}

				// If it was a special message, like an ACK, then we don't process it any further.
				if !keepProcessing {
					if contd != nil {
						contd <- true
					}

					// Don't even send an ACK if we're not meant to keep processing this message.
					continue
				}

				// Send an ACK if (a) it is a Shell or Control message and (b) we've been configured to ACK messages in general.
				//
				// AbstractServers that live on the Cluster Gateway and receive messages from frontend clients typically
				// do not ACK messages unless the frontend client is our custom Jupyter Golang client.
				if !isAck && (socket.Type == types.ShellMessage || socket.Type == types.ControlMessage) && s.ShouldAckMessages {
					ackErr := s.sendAck(jMsg, socket)
					if ackErr != nil {
						s.Log.Error("Error while sending 'ACK' for message. Message we failed to ACK: %s. Error: %v.", jMsg.String(), ackErr)
					}
				}

				handlerStart := time.Now()
				err = handler(server, socket.Type, jMsg)
				if err != nil && !errors.Is(err, errServeOnce) {
					s.Log.Error(utils.OrangeStyle.Render("[gid=%d] Handler for %s \"%s\" message \"%s\" (JupyterID=\"%s\") has returned with an error after %v: %v."), goroutineId, socket.Type.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), time.Since(handlerStart), err)

					// Send an error message of some sort back to the original sender, in Jupyter format.
					_ = s.replyWithError(jMsg, socket, err)
				} else if socket.Type == types.ShellMessage || socket.Type == types.ControlMessage || (socket.Type == types.IOMessage && jMsg.JupyterMessageType() != "stream") {
					// Only print this next bit for Shell, Control, and non-stream IOPub messages.
					// That's all we care about here.
					s.Log.Debug(utils.LightGreenStyle.Render("[gid=%d] Finished handling %s \"%s\" message \"%s\" (JupyterID=\"%s\") in %v."),
						goroutineId, socket.Type.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), time.Since(handlerStart))
				}
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
			}
		}

		if socket.PendingReq != nil {
			contd <- true
			// s.Log.Debug("[gid=%d] Continue waiting for the response of %s requests(%d)", goroutineId, socket.Type.String(), socket.PendingReq.Len())
		}
	}
}

func (s *AbstractServer) generateErrorMessage(originalMessage *types.JupyterMessage, err error) (*types.JupyterMessage, error) {
	// We need the signature scheme and key in order to sign the error message that we create here.
	// If those haven't been populated yet (they must be populated manually), then we cannot send the error message.
	var (
		signatureScheme    string
		signatureSchemeSet bool
		key                string
		keySet             bool
	)

	// If we're a server in a KernelReplicaClient or DistributedKernelClient, then the Meta field should be non-nil
	// and will have the connection info. So, we can get the signature scheme and key from there.
	if s.Meta != nil {
		signatureScheme = s.Meta.SignatureScheme
		key = s.Meta.Key
	}

	// If we couldn't retrieve these from the Meta field, then we'll need to hope that they were populated elsewhere.
	// If not, then we cannot send the message.
	if len(signatureScheme) == 0 || len(key) == 0 {
		if signatureScheme, signatureSchemeSet = originalMessage.SignatureScheme(); !signatureSchemeSet {
			return nil, errMissingSignatureScheme
		}

		if key, keySet = originalMessage.Key(); !keySet {
			return nil, errMissingKey
		}
	}

	msgType := types.JupyterMessageType(originalMessage.JupyterMessageType())

	var respMsgType string
	baseType, ok := msgType.GetBaseMessageType()
	if ok {
		respMsgType = baseType + "reply"
	} else {
		respMsgType = baseType
	}

	// Create the new message.
	frames := types.NewJupyterFramesWithReservation(1)

	// Create the message header.
	header := &types.MessageHeader{
		Date:     time.Now().UTC().Format(types.JavascriptISOString),
		MsgID:    uuid.New().String(),
		MsgType:  types.JupyterMessageType(respMsgType),
		Session:  originalMessage.JupyterSession(),
		Username: originalMessage.JupyterUsername(),
		Version:  originalMessage.JupyterVersion(),
	}

	// Encode the message header, returning an error if the encoding fails.
	if encodingError := frames.EncodeHeader(header); encodingError != nil {
		return nil, encodingError
	}

	// Create the message content.
	errorContent := &types.MessageError{
		Status:   types.MessageStatusError,
		ErrName:  fmt.Sprintf("%T", err),
		ErrValue: err.Error(),
	}

	// Encode the message content, returning an error if the encoding fails.
	if encodingError := frames.EncodeContent(errorContent); encodingError != nil {
		return nil, encodingError
	}

	var (
		msg          zmq4.Msg
		signingError error
	)
	msg.Frames, signingError = frames.Sign(signatureScheme, []byte(key))
	if signingError != nil {
		return nil, signingError
	}

	// Add the destination frame, in case we're in the Local Daemon and this has to go back to the Cluster Gateway.
	jMsg := types.NewJupyterMessage(&msg)
	jMsg.AddDestinationId(originalMessage.DestinationId)

	return jMsg, nil
}

// replyWithError replies to the sender with a message encoding the provided error.
// The message that is sent back to the sender is in Jupyter format.
// The message is constructed using information from the original message.
func (s *AbstractServer) replyWithError(originalMessage *types.JupyterMessage, socket *types.Socket, err error) error {
	errorMessage, generationError := s.generateErrorMessage(originalMessage, err)
	if generationError != nil {
		s.Log.Warn("Failed to generate error message because: %v", generationError)
		return generationError
	}

	sendStart := time.Now()
	err = socket.Send(*errorMessage.GetZmqMsg())
	sendDuration := time.Since(sendStart)

	s.NumSends.Add(1)
	s.NumUniqueSends.Add(1)

	if metricError := s.MessagingMetricsProvider.SentMessage(s.ComponentId, sendDuration, s.nodeType, socket.Type, errorMessage.JupyterMessageType()); metricError != nil {
		s.Log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	if metricError := s.MessagingMetricsProvider.SentMessageUnique(s.ComponentId, s.nodeType, socket.Type, errorMessage.JupyterMessageType()); metricError != nil {
		s.Log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	return err
}

// RegisterAck instructs the Server to begin listening for an ACK for a message with the given ID.
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

	// Validate that the request is non-nil...
	if request == nil {
		panic(fmt.Sprintf("[gid=%d] %s request is nil.", goroutineId, socket.Type.String()))
	}

	// ... and that the request's Payload is non-nil...
	if request.Payload() == nil {
		panic(fmt.Sprintf("[gid=%d] %s request payload is nil: %s", goroutineId, socket.Type.String(), request.String()))
	}

	// Validate that the request has a non-nil payload.
	jMsg := request.Payload()
	if jMsg == nil {
		panic(fmt.Sprintf("[gid=%d] %s request payload is nil. Request: %v", goroutineId, socket.Type.String(), request))
	}

	socket.InitPendingReq()
	reqId := request.RequestId()
	s.Log.Debug("[gid=%d] %s [socket=%s] is sending %s \"%s\" message %s (JupyterID=%s) [remoteSocket=%s]. RequiresACK: %v.",
		goroutineId, s.Name, socket.Name, socket.Type.String(), jMsg.JupyterMessageType(), request.RequestId(),
		request.JupyterMessageId(), socket.RemoteName, request.RequiresAck())

	// dest.Unlock()
	if request.RequiresAck() {
		_, alreadyRegistered := s.RegisterAck(request.RequestId())
		if alreadyRegistered {
			s.Log.Warn(utils.OrangeStyle.Render("[gid=%d] Already listening for ACKs for %v request %s. Current request with that ID is a \"%s\" message."),
				goroutineId, socket.Type, reqId, jMsg.JupyterMessageType())
		}
	} else {
		// Record that we don't need an ACK for this request.
		s.discardACKs.Store(request.RequestId(), struct{}{})
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
		// If the goroutine executing the AbstractServer::SendRequest method gets here first,
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

		// Only try to set the request to the 'timed-out' state if it hasn't yet completed.
		if errors.Is(err, context.DeadlineExceeded) && !request.HasCompleted() {
			_, err := request.SetTimedOut()
			if err != nil {
				s.Log.Error(
					"Failed to transition %s \"%s\" request \"%s\" (JupyterID = \"%s\") to 'timed-out' state: %v",
					request.MessageType(), request.JupyterMessageType(),
					request.RequestId(), request.JupyterMessageId(), err)
			}
		}

		// TODO(Ben): There is conceivably a race here in which we call the code below AFTER we begin resubmitting the request, which causes it to be prematurely released.
		// TODO: Does this race condition still exist...?
		// Clear pending request.
		cleanUpRequest(err)
	}()

	if err := s.SendRequest(request, socket); err != nil {
		s.Log.Debug("Error while sending %s \"%s\" message %s (Jupyter ID: %s): %v", socket.Type.String(), jMsg.JupyterMessageType(), reqId, jMsg.JupyterMessageId(), err)
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

// Wait for an ACK on the given channel, or time-out after the Request's configured timeout.
//
// If the Request does not actually require an acknowledgement, then this returns true immediately, indicating success.
func (s *AbstractServer) waitForAck(request types.Request) bool {
	// If there's no ACK required, then just return immediately.
	if !request.RequiresAck() {
		s.Log.Warn("%s \"%s\" request %s (JupyterID=%s) does not require an ACK. Returning immediately instead of waiting for an ACK.",
			request.MessageType().String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId())
		return true
	}

	var (
		ackChan <-chan struct{}
		loaded  bool
	)
	if ackChan, loaded = s.ackChannels.Load(request.RequestId()); !loaded {
		panic(fmt.Sprintf("We need an ACK for %v message %s; however, the ACK channel is nil.",
			request.MessageType(), request.RequestId()))
	}

	s.Log.Debug("Waiting up to %v to receive ACK for %s \"%s\" message %s (JupyterID=%s).", request.AckTimeout(),
		request.MessageType().String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId())
	select {
	case <-ackChan:
		{
			return true
		}
	case <-time.After(request.AckTimeout()):
		{
			return false
		}
	}
}

// printSendLatencyWarning prints a warning if the latency of a send operation was long enough to warrant
// printing a warning.
func (s *AbstractServer) printSendLatencyWarning(sendDuration time.Duration, msgType string, msgId string) {
	if sendDuration >= time.Millisecond*50 {
		style := utils.YellowStyle

		// If it took over 100ms, then we'll use orange-colored text instead of yellow.
		if sendDuration >= time.Millisecond*100 {
			style = utils.OrangeStyle
		}

		s.Log.Warn(style.Render("Sending %s \"%s\" message %s took %v."), msgType, msgId, sendDuration)
	}
}

// sendRequest sends the zmq4.msg encapsulated by the types.Request on the given types.Socket.
//
// If the send operation is successful, then sendRequest returns nil.
//
// If the send operation fails, then the types.Request is transitioned to an error state, and the associated
// error is returned.
func (s *AbstractServer) sendRequest(request types.Request, socket *types.Socket) error {
	// This updates the frames of the zmq4.Msg, assigning them to be the frames of the JupyterMessage/JupyterFrames.
	zmqMsg := request.Payload().GetZmqMsg()

	s.Log.Debug(
		utils.PurpleStyle.Render(
			"Sending %s \"%s\" message %s (JupyterID=\"%s\").\n\nJupyterFrames (%p): %s.\n\nzmq4.Msg Frames (%p): %s\n\n"),
		socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(),
		request.Payload().JupyterFrames.Frames, request.Payload().JupyterFrames.String(),
		zmqMsg.Frames, types.FramesToString(zmqMsg.Frames))

	// Send the request.
	sendStart := time.Now()
	err := socket.Send(*zmqMsg)
	sendDuration := time.Since(sendStart)

	if err != nil {
		// If there was an error sending the request, then print an error message and transition the request to the 'erred' state.
		if _, transitionErr := request.SetErred(err); transitionErr != nil {
			s.Log.Error("Failed to transition %s \"%s\" request \"%s\" (JupyterID = \"%s\") to 'erred' state: %v",
				request.MessageType(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), transitionErr)
		}

		return err
	}

	// Display a warning if the send operation took a while.
	s.printSendLatencyWarning(sendDuration, request.JupyterMessageType(), request.RequestId())

	// Record metrics.
	s.NumSends.Add(1)
	if metricError := s.MessagingMetricsProvider.SentMessage(s.ComponentId, sendDuration, s.nodeType, socket.Type, request.JupyterMessageType()); metricError != nil {
		s.Log.Error("Could not record 'SentMessage' Prometheus metric because: %v", metricError)
	}

	// Print a fairly verbose log message when debug logging is enabled.
	if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
		goroutineId := goid.Get()
		reqId := request.RequestId()

		firstPart := fmt.Sprintf(utils.LightBlueStyle.Render("[gid=%d] Sent %s \"%s\" message with"), goroutineId, socket.Type.String(), request.JupyterMessageType())
		secondPart := fmt.Sprintf("reqID=%v (JupyterID=%s)", utils.PurpleStyle.Render(reqId), utils.LightPurpleStyle.Render(request.JupyterMessageId()))
		thirdPart := fmt.Sprintf(utils.LightBlueStyle.Render("via %s. Attempt %d/%d. NumSends: %d. NumUniqueSends: %d. AckRequired: %v. Message: %v"),
			socket.Name, request.CurrentAttemptNumber()+1, request.MaxNumAttempts(), s.NumSends.Load(), s.NumUniqueSends.Load(), request.RequiresAck(), request.Payload().JupyterFrames.String())
		s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
	}

	// Record that the request has been submitted.
	if _, err := request.SetSubmitted(); err != nil {
		panic(fmt.Sprintf("Request transition to 'submitted' state failed for %s \"%s\" request %s (JupyterID=%s): %v",
			socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
	}

	return nil
}

// shouldAddRequestTrace returns true if the AbstractServer should embed or update an existing metrics.RequestTrace
// struct within the first "buffer" frame of a Jupyter message.
func (s *AbstractServer) shouldAddRequestTrace(msg *types.JupyterMessage, socket *types.Socket) bool {
	if msg == nil {
		panic("JupyterMessage is nil when evaluating whether to add RequestTrace.")
	}

	if socket == nil {
		panic("Socket is nil when evaluating whether to add RequestTrace.")
	}

	if !s.DebugMode {
		return false
	}

	if socket.Type == types.ShellMessage || socket.Type == types.ControlMessage {
		return true
	}

	if socket.Type == types.IOMessage && msg.JupyterMessageType() != "stream" && msg.JupyterMessageType() != "status" {
		return true
	}

	return false
}

// sendRequestWithRetries encapsulates the logic of sending the given types.Request using the given types.Socket
// in a reliable way; that is, sendRequestWithRetries will resubmit the given types.Request if an ACK is not received
// within the types.Request's configured timeout window, up to the types.Request's configured maximum number of attempts.
//
// sendRequestWithRetries will return nil on success.
func (s *AbstractServer) sendRequestWithRetries(request types.Request, socket *types.Socket) error {
	goroutineId := goid.Get()

	// We only want to record the "unique send" metric once per message sent (i.e., don't include resubmissions).
	recordedUniqueSend := false
	request.SendStarting()

	// We only want to add traces to Shell, Control, and a subset of IOPub messages.
	if s.shouldAddRequestTrace(request.Payload(), socket) {
		s.Log.Debug("Attempting to add or update RequestTrace to/in Jupyter %s \"%s\" request.",
			socket.Type.String(), request.JupyterMessageType())
		_, _, err := types.AddOrUpdateRequestTraceToJupyterMessage(request.Payload(), socket, time.Now(), s.Log)
		if err != nil {
			s.Log.Error("Failed to add or update RequestTrace to Jupyter message: %v", err)
			s.Log.Error("The serving is using the following connection info: %v", s.Meta)
			panic(err)
		}
	}

	// We'll loop until the send operation is successful, or until we run out of attempts.
	// The logic for stopping the loop is implemented in onNoAcknowledgementReceived.
	for /* request.CurrentAttemptNumber() < request.MaxNumAttempts() */ {
		if err := s.sendRequest(request, socket); err != nil {
			// If there was an error sending the request, then print an error message and transition the request to the 'erred' state.
			s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to send %v \"%s\" message on attempt %d/%d via %s because: %v"),
				goroutineId, socket.Type, request.JupyterMessageType(), request.CurrentAttemptNumber()+1, request.MaxNumAttempts(), socket.Name, err.Error())

			// If there's a send error, then we won't retry -- this indicates that there's a big issue here,
			// such as the Socket losing connection entirely.
			return err
		}

		// Record metrics.
		// We only want to record the "unique send" metric once per message sent (i.e., don't include resubmissions).
		if !recordedUniqueSend {
			// We only increment the unique "sent messages" counter here.
			// We increment the other one earlier, before knowing if the send operation was ultimately "successful" or not.
			s.NumUniqueSends.Add(1)
			if metricError := s.MessagingMetricsProvider.SentMessageUnique(s.ComponentId, s.nodeType, socket.Type, request.JupyterMessageType()); metricError != nil {
				s.Log.Error("Could not record 'SentMessageUnique' Prometheus metric because: %v", metricError)
			}

			recordedUniqueSend = true
		}

		// If the request requires an acknowledgement to be sent, then we'll wait for that acknowledgement.
		// Otherwise, we'll return immediately.
		if s.MessageAcknowledgementsEnabled && request.RequiresAck() && s.waitForAck(request) {
			// We were successful!
			s.onAcknowledgementReceived(request, socket)
			s.onSuccessfullySentMessage(request, socket, request.CurrentAttemptNumber())
			return nil
		} else if !s.MessageAcknowledgementsEnabled || !request.RequiresAck() {
			s.onSuccessfullySentMessage(request, socket, request.CurrentAttemptNumber())
			return nil
		}

		// This will return an error if we're supposed to stop looping at this point.
		if err := s.onNoAcknowledgementReceived(request, socket); err != nil {
			return err
		}
	}
}

// onAcknowledgementReceived is to be called when an acknowledgement is received for the given Request within
// the Request's configured time-out window.
func (s *AbstractServer) onAcknowledgementReceived(request types.Request, socket *types.Socket) {
	if !request.RequiresAck() {
		s.Log.Error("We're recording that we successfully received an ACK for %s \"%s\" request %s (JupyterID=%s), but that request doesn't require an ACK...",
			request.MessageType().String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId())
		return
	}

	startTime, _ := request.BeganSendingAt()
	ackReceivedLatency := time.Since(startTime)

	if s.Log.GetLevel() == logger.LOG_LEVEL_ALL {
		goroutineId := goid.Get()
		firstPart := fmt.Sprintf(utils.GreenStyle.Render("[gid=%d] %v \"%s\" message"), goroutineId, socket.Type, request.JupyterMessageType())
		secondPart := fmt.Sprintf("%s (JupyterID=%s)", utils.PurpleStyle.Render(request.RequestId()), utils.LightPurpleStyle.Render(request.JupyterMessageId()))
		thirdPart := fmt.Sprintf(utils.GreenStyle.Render("has successfully been acknowledged on attempt %d/%d after %v."), request.CurrentAttemptNumber()+1, request.MaxNumAttempts(), ackReceivedLatency)
		s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)
	}

	if err := s.MessagingMetricsProvider.AddAckReceivedLatency(ackReceivedLatency, s.ComponentId, s.nodeType, socket.Type, request.JupyterMessageType()); err != nil {
		s.Log.Warn("Could not record \"ack received latency\" metric because: %v", err)
	}
}

// onNoAcknowledgementReceived is to be called when no acknowledgement is received for the given Request
// within the Request's configured time-out window.
func (s *AbstractServer) onNoAcknowledgementReceived(request types.Request, socket *types.Socket) error {
	goroutineId := goid.Get()

	// If ACKs are disabled, then just return immediately.
	if !s.MessageAcknowledgementsEnabled {
		s.Log.Warn("onNoAcknowledgementReceived handler called for %s \"%s\" request %s despite the fact that ACKs are disabled...",
			socket.Type.String(), request.JupyterMessageType(), request.RequestId())
		return nil
	}

	// Just to avoid going through the process of sleeping and updating the header if that was our last try.
	if (request.CurrentAttemptNumber() + 1) >= request.MaxNumAttempts() {
		s.Log.Error(utils.RedStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %s \"%s\" message %v (dest: %v, jupyter ID: %v) from remote socket %s during attempt %d/%d. Giving up."),
			goroutineId, socket.Name, socket.Addr(), socket.Type.String(), request.JupyterMessageType(), request.RequestId(),
			request.DestinationId(), request.JupyterMessageId(), socket.RemoteName, request.CurrentAttemptNumber()+1, request.MaxNumAttempts())
		s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to receive ACK for %v \"%s\" message %v (dest: %v) from remote socket %s after %d attempt(s)."),
			goroutineId, request.JupyterMessageType(), socket.Type, request.RequestId(), request.DestinationId(), socket.RemoteName, request.MaxNumAttempts())

		if _, err := request.SetTimedOut(); err != nil {
			panic(fmt.Sprintf("Request transition to 'timed-out' state failed for %s \"%s\" request %s (JupyterID=%s): %v",
				socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
		}

		// Record metric.
		_ = s.MessagingMetricsProvider.AddFailedSendAttempt(s.ComponentId, s.nodeType, socket.Type, request.JupyterMessageType())
		return ErrRequestOutOfAttempts
	}

	// If we're supposed to panic on the first failed send attempt (i.e., no acknowledgement received), then panic.
	if s.PanicOnFirstFailedSend {
		if pprofErr := pprof.Lookup("goroutine").WriteTo(os.Stderr, 1); pprofErr != nil {
			s.Log.Error("Error encountered when calling pprof.Lookup(\"goroutine\").WriteTo(os.Stderr, 1): %v", pprofErr)
		}

		log.Fatalf(utils.OrangeStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %s \"%s\" message %v (src: %v, dest: %v, jupyter ID: %v) from remote socket %s."),
			goroutineId, socket.Name, socket.Addr(), socket.Type.String(), request.JupyterMessageType(), request.RequestId(),
			request.SourceID(), request.DestinationId(), request.JupyterMessageId(), socket.RemoteName)
	}

	// Sleep for an amount of time proportional to the number of attempts with some random jitter added.
	nextSleepInterval := s.getSleepInterval(request.CurrentAttemptNumber())
	s.Log.Warn(utils.OrangeStyle.Render("[gid=%d] Socket %v (%v) timed-out waiting for ACK for %s \"%s\" message %v (src: %v, dest: %v, jupyter ID: %v) from remote socket %s during attempt %d/%d. Serving: %d. Will sleep for %v before trying again."),
		goroutineId, socket.Name, socket.Addr(), socket.Type.String(), request.JupyterMessageType(), request.RequestId(),
		request.SourceID(), request.DestinationId(), request.JupyterMessageId(), socket.RemoteName,
		request.CurrentAttemptNumber()+1, request.MaxNumAttempts(), nextSleepInterval, atomic.LoadInt32(&socket.Serving))

	// Sleep for a bit.
	time.Sleep(nextSleepInterval)
	request.IncrementAttemptNumber()

	// Update the request's header and whatnot so we can resend it without Jupyter complaining about
	// a duplicate request signature, in the event that the last message was actually/eventually received.
	if err := request.PrepareForResubmission(); err != nil {
		s.Log.Error(utils.RedStyle.Render("[gid=%d] Failed to update message header for %v \"%s\" message %v: %v"), goroutineId, request.JupyterMessageType(), socket.Type, request.RequestId(), err)
		if _, transitionErr := request.SetErred(err); transitionErr != nil {
			s.Log.Error("Failed to transition %s \"%s\" request \"%s\" (JupyterID = \"%s\") to 'erred' state: %v", request.MessageType(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), transitionErr)
		}

		return err
	}

	return nil
}

// SendRequest sends a types.Request on the given types.Socket.
// If this message requires ACKs, then this will retry until an ACK is received, or it will give up.
//
// SendRequest returns nil on success.
func (s *AbstractServer) SendRequest(request types.Request, socket *types.Socket) error {
	// If the message requires an ACK, then we'll try sending it multiple times.
	// Otherwise, we'll just send it the one time.
	if s.MessageAcknowledgementsEnabled && request.RequiresAck() {
		s.acksReceived.Store(request.RequestId(), false)
	}

	return s.sendRequestWithRetries(request, socket)
}

// onSuccessfullySentMessage is to be called when a message is sent successfully.
func (s *AbstractServer) onSuccessfullySentMessage(request types.Request, socket *types.Socket, numTries int) {
	if _, err := request.SetProcessing(); err != nil {
		panic(fmt.Sprintf("Request transition to 'processing' state failed for %s \"%s\" request %s (JupyterID=%s): %v", socket.Type.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err))
	}

	if err := s.MessagingMetricsProvider.AddNumSendAttemptsRequiredObservation(float64(numTries+1), s.ComponentId, s.nodeType, socket.Type, request.JupyterMessageType()); err != nil {
		s.Log.Error("Could not record 'NumSendAttemptsRequired' observation because: %v", err)
	}
}

// Get the next sleep interval for a request, optionally including jitter.
//
// In general, jitter should probably be used. See [this] for a discussion.
//
// [this]: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func (s *AbstractServer) getSleepInterval(attempt int) time.Duration {
	// Compute sleep interval as BaseSleep * 2^NumTries.
	nextSleepInterval := s.RetrySleepInterval * time.Duration(math.Pow(2, float64(attempt)))

	// Clamp the sleep interval.
	nextSleepInterval = time.Duration(math.Min(float64(s.MaxSleepInterval), float64(nextSleepInterval)))

	if s.UseJitter {
		// Generating a random value between [0..next_sleep_interval)
		nextSleepInterval = time.Duration(rand.Float64() * (float64(nextSleepInterval)))
	}

	return nextSleepInterval
}

func (s *AbstractServer) poll(socket *types.Socket, chMsg chan<- interface{}, contd <-chan bool) {
	goroutineId := goid.Get()
	defer close(chMsg)

	var msg interface{}
	for {
		got, err := socket.Recv()
		receivedAt := time.Now()

		if err == nil {
			// Deserialize the message now so we can print some debug info + inspect if it is an ACK.
			msg = types.NewJupyterMessage(&got)
			jMsg := msg.(*types.JupyterMessage)

			// If the message is just an ACK, then we'll spawn another goroutine to handle it and keep polling.
			if jMsg.IsAck() {
				goroutineId := goid.Get()

				firstPart := fmt.Sprintf(utils.GreenStyle.Render("[gid=%d] [1] Received ACK for %s \"%s\""), goroutineId, socket.Type, jMsg.GetParentHeader().MsgType)
				secondPart := fmt.Sprintf("%s (JupyterID=%s, ParentJupyterId=%s)", utils.PurpleStyle.Render(jMsg.RequestId), utils.LightPurpleStyle.Render(jMsg.JupyterMessageId()), utils.LightPurpleStyle.Render(jMsg.GetParentHeader().MsgID))
				thirdPart := fmt.Sprintf(utils.GreenStyle.Render("via local socket %s [remoteSocket=%s]: %v"), socket.Name, socket.RemoteName, jMsg)
				s.Log.Debug("%s %s %s", firstPart, secondPart, thirdPart)

				// Handle the ACK in a separate goroutine and continue.
				go s.handleAck(jMsg, jMsg.RequestId, socket)
				continue
			}

			if socket.Type == types.ShellMessage || socket.Type == types.ControlMessage || (socket.Type == types.IOMessage && jMsg.JupyterMessageType() != "stream" && jMsg.JupyterMessageType() != "status" && jMsg.JupyterMessageType() != "execute_input") {
				s.Log.Debug("[gid=%d] Poller received new %s \"%s\" message %s (JupyterID=\"%s\", Session=\"%s\").", goroutineId, socket.Type.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), jMsg.JupyterSession())

				if s.DebugMode {
					// We only want to add traces to Shell, Control, and a subset of IOPub messages.
					s.Log.Debug("Attempting to add or update RequestTrace to/in Jupyter %s \"%s\" request.",
						socket.Type.String(), jMsg.JupyterMessageType())
					_, _, err := types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, socket, receivedAt, s.Log)
					if err != nil {
						s.Log.Error("Failed to add RequestTrace to JupyterMessage: %v.", err)
						panic(err)
					}
				}
			}
		} else {
			msg = err
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

//func (s *AbstractServer) isMessageAnAck(msg *types.JupyterMessage, typ types.MessageType) bool {
//	// ACKs are only sent for Shell/Control messages.
//	if typ != types.ShellMessage && typ != types.ControlMessage {
//		return false
//	}
//
//	return msg.JupyterMessageType() == types.MessageTypeACK
//}

func (s *AbstractServer) isMessageGolangFrontendRegistrationRequest(msg *types.JupyterMessage, typ types.MessageType) bool {
	// Golang frontend registration requests are only sent for Shell/Control messages.
	if typ != types.ShellMessage && typ != types.ControlMessage {
		return false
	}

	return msg.JupyterMessageType() == GolangFrontendRegistrationRequest
}

func (s *AbstractServer) getOneTimeMessageHandler(socket *types.Socket, shouldDestFrameBeRemoved bool, defaultHandler types.MessageHandler) types.MessageHandler {
	return func(info types.JupyterServerInfo, msgType types.MessageType, msg *types.JupyterMessage) error {
		// This handler returns errServeOnce if any to indicate that the server should stop serving.
		retErr := errServeOnce
		pendingRequests := socket.PendingReq
		// var matchReqId string
		var handler types.MessageHandler
		var request types.Request

		if pendingRequests != nil {
			// We do not assume that the types.RequestDest implements the auto-detect feature.
			_, rspId, _ := msg.JupyterFrames.ExtractDestFrame(true)
			if rspId == "" {
				s.Log.Warn("Unexpected response without request ID, fallback to default handler.")
				// Unexpected response without request ID, fallback to default handler.
				handler = defaultHandler
			} else {
				// s.Log.Debug(utils.BlueStyle.Render("Received response with ID=%s on socket %s"), rspId, socket.Name)
				// matchReqId = rspId

				// Automatically remove destination kernel ID frame.
				if shouldDestFrameBeRemoved {
					msg.JupyterFrames.RemoveDestFrame(true)
				}

				// Remove pending request and return registered handler. If timeout, the handler will be nil.
				if pending, exist := pendingRequests.LoadAndDelete(rspId); exist {
					handler = pending.Handle // Handle will release the pending request once called.
					request = pending.Request()

					e2eLatency := time.Since(pending.Start)
					s.Log.Debug("Received response to %s \"%s\" request %s (JupyterID=\"%s\") after %v.",
						request.MessageType().String(), request.JupyterMessageType(), request.RequestId(),
						request.JupyterMessageId(), e2eLatency)

					// Record the latency in (microseconds) in Prometheus.
					if err := s.MessagingMetricsProvider.AddMessageE2ELatencyObservation(e2eLatency,
						s.ComponentId, s.nodeType, request.MessageType(), request.JupyterMessageType()); err != nil {
						s.Log.Error("Could not record E2E latency of %v for %s \"%s\" message %s (JupyterID=\"%s\") because: %v",
							request.MessageType().String(), request.JupyterMessageType(), request.RequestId(),
							request.JupyterMessageId(), err)
					}
				}
				// Continue serving if there are pending requests.
				if pendingRequests.Len() > 0 {
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
