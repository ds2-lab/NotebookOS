package types

import (
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

const (
	IOTopicStatus = "status"
)

var (
	ErrSocketNotAvailable   = errors.New("socket not available")
	IOTopicStatusRecognizer = regexp.MustCompile(`^kernel\.([0-9a-f-]+)\.status$`)

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
)

type MessageType int

func (t MessageType) String() string {
	return [...]string{"heartbeat", "control", "shell", "stdin", "io"}[t]
}

type MessageHandler func(JupyterServerInfo, MessageType, *zmq4.Msg) error

type MessageHandlerWrapper struct {
	Handle MessageHandler
}

func GetMessageHandlerWrapper(h MessageHandler) *MessageHandlerWrapper {
	m := mhwPool.Get().(*MessageHandlerWrapper)
	m.Handle = h
	return m
}

func (m *MessageHandlerWrapper) Release() {
	m.Handle = nil
	mhwPool.Put(m)
}

type Socket struct {
	zmq4.Socket
	Port       int
	Type       MessageType
	Handler    MessageHandler
	PendingReq hashmap.HashMap[string, *MessageHandlerWrapper]
	Serving    int32
	Mu         sync.Mutex
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
	All     [5]*Socket
}

// JupyterServerInfo defines the interface to provider infos of a JupyterServer.
type JupyterServerInfo interface {
	fmt.Stringer

	Socket(MessageType) *Socket
}
