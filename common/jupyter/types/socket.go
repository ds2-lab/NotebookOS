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

type MessageHandler func(JupyterServerInfo, MessageType, *zmq4.Msg) error

type MessageDone func()

type MessageHandlerWrapper struct {
	handle MessageHandler
	done   MessageDone
	once   int32
}

func GetMessageHandlerWrapper(h MessageHandler, done MessageDone) *MessageHandlerWrapper {
	m := mhwPool.Get().(*MessageHandlerWrapper)
	m.handle = h
	m.done = done
	m.once = 0
	return m
}

func (m *MessageHandlerWrapper) Handle(info JupyterServerInfo, t MessageType, msg *zmq4.Msg) error {
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
	mhwPool.Put(m)
}

type Socket struct {
	zmq4.Socket
	Port       int
	Type       MessageType
	Handler    MessageHandler
	PendingReq hashmap.HashMap[string, *MessageHandlerWrapper]
	Serving    int32
	Name       string // Mostly used for debugging.
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
