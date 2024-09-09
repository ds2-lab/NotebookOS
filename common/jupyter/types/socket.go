package types

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

const (
	IOTopicStatus   = "status"
	IOTopicShutdown = "shutdown"

	// RemoteNameUnspecified is the default value for Socket::RemoteName.
	RemoteNameUnspecified = "unspecified_remote"
)

var (
	ErrSocketNotAvailable   = errors.New("socket not available")
	IOTopicStatusRecognizer = regexp.MustCompile(`^kernel\.([0-9a-f-]+)\.([^.]+)$`)

	mhwPool = sync.Pool{
		New: func() interface{} {
			return &MessageHandlerWrapper{}
		},
	}
)

const (
	HBMessage MessageType = iota
	ControlMessage
	ShellMessage
	StdinMessage
	IOMessage
	AckMessage
)

type MessageType int

func (t MessageType) String() string {
	return [...]string{"heartbeat", "control", "shell", "stdin", "io", "ack"}[t]
}

type MessageHandler func(JupyterServerInfo, MessageType, *JupyterMessage) error

type MessageDone func()

type MessageHandlerWrapper struct {
	handle  MessageHandler
	done    MessageDone
	once    int32
	request Request
}

func GetMessageHandlerWrapper(request Request) *MessageHandlerWrapper {
	var (
		h    = request.MessageHandler()
		done = request.DoneCallback()
	)

	m := mhwPool.Get().(*MessageHandlerWrapper)
	m.handle = h
	m.done = done
	m.once = 0
	m.request = request
	return m
}
func (m *MessageHandlerWrapper) Request() Request {
	return m.request
}

func (m *MessageHandlerWrapper) Handle(info JupyterServerInfo, t MessageType, msg *JupyterMessage) error {
	err := m.handle(info, t, msg)
	m.Release()
	return err
}

func (m *MessageHandlerWrapper) Release() {
	done := m.done
	if done != nil && atomic.CompareAndSwapInt32(&m.once, 0, 1) {
		done()
	}
	m.handle = nil
	m.done = nil
	m.request = nil
	mhwPool.Put(m)
}

type Socket struct {
	zmq4.Socket
	Port             int                                             // The port that the socket is bound to/listening on.
	Type             MessageType                                     // The type of Socket that this is (e.g., shell, stdin, control, heartbeat, or io pub/sub).
	Handler          MessageHandler                                  // The handler for responses. TODO: Is this actually used?
	PendingReq       hashmap.HashMap[string, *MessageHandlerWrapper] // Requests that have been sent on this socket, for which we're waiting for responses.
	Serving          int32                                           // Indicates whether we have a goroutine monitoring for messages + handling those messages. Must be read/updated atomically.
	Name             string                                          // Mostly used for debugging.
	RemoteName       string                                          // Mostly used for debugging.
	StopServingChan  chan struct{}                                   // Used to tell a goroutine serving this socket to stop (such as if we're recreating+reconnecting due to no ACKs)
	IsGolangFrontend bool                                            // If true, then this Socket is connected to a Golang Jupyter frontend.
	mu               sync.Mutex                                      // Synchronizes access to the underlying ZMQ socket, only for sends.
}

// NewSocket creates a new Socket, without specifying the message handler.
func NewSocket(socket zmq4.Socket, port int, typ MessageType, name string) *Socket {
	return &Socket{
		Socket:          socket,
		Port:            port,
		Type:            typ,
		Name:            name,
		StopServingChan: make(chan struct{}, 1),
		RemoteName:      RemoteNameUnspecified,
	}
}

// NewSocketWithRemoteName creates a new Socket, without specifying the message handler.
func NewSocketWithRemoteName(socket zmq4.Socket, port int, typ MessageType, name string, remoteName string) *Socket {
	return &Socket{
		Socket:          socket,
		Port:            port,
		Type:            typ,
		Name:            name,
		StopServingChan: make(chan struct{}, 1),
		RemoteName:      remoteName,
	}
}

// NewSocketWithHandler creates a new Socket with a message handler specified at creation time.
func NewSocketWithHandler(socket zmq4.Socket, port int, typ MessageType, name string, handler MessageHandler) *Socket {
	return &Socket{
		Socket:          socket,
		Port:            port,
		Type:            typ,
		Name:            name,
		StopServingChan: make(chan struct{}, 1),
		Handler:         handler,
		RemoteName:      RemoteNameUnspecified,
	}
}

// NewSocketWithHandlerAndRemoteName creates a new Socket with a message handler specified at creation time.
func NewSocketWithHandlerAndRemoteName(socket zmq4.Socket, port int, typ MessageType, name string, remoteName string, handler MessageHandler) *Socket {
	return &Socket{
		Socket:          socket,
		Port:            port,
		Type:            typ,
		Name:            name,
		StopServingChan: make(chan struct{}, 1),
		Handler:         handler,
		RemoteName:      remoteName,
	}
}

func (s *Socket) Send(msg zmq4.Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.Socket.Send(msg)

	return err
}

func (s *Socket) String() string {
	return fmt.Sprintf("%s(%d)", s.Type, s.Port)
}

// InitPendingReq initializes the pending request map.
func (s *Socket) InitPendingReq() {
	if s.PendingReq == nil {
		s.PendingReq = hashmap.NewCornelkMap[string, *MessageHandlerWrapper](10)
	}
}

type JupyterSocket struct {
	HB      *Socket
	Control *Socket
	Shell   *Socket
	Stdin   *Socket
	IO      *Socket // Pub for server and Sub for client.
	// Ack     *Socket // Socket for receiving ACKs.
	All [5]*Socket
}

// JupyterServerInfo defines the interface to provider infos of a JupyterServer.
type JupyterServerInfo interface {
	fmt.Stringer

	Socket(MessageType) *Socket
}
