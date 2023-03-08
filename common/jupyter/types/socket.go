package types

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-zeromq/zmq4"
)

var (
	ErrSocketNotAvailable = errors.New("socket not available")
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

type MessageHandler func(MessageType, *zmq4.Msg) error

type Socket struct {
	zmq4.Socket
	Port       int
	Type       MessageType
	Handler    MessageHandler
	PendingReq *zmq4.Msg
	Serving    int32
	Mu         sync.Mutex
}

func (s *Socket) String() string {
	return fmt.Sprintf("%s(%d)", s.Type, s.Port)
}

type JupyterSocket struct {
	HB      *Socket
	Control *Socket
	Shell   *Socket
	Stdin   *Socket
	IO      *Socket // Pub for server and Sub for client.
	All     [5]*Socket
}
