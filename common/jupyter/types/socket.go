package types

import (
	"errors"

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
	IOPubMessage
)

type MessageType int

func (t MessageType) String() string {
	return [...]string{"heartbeat", "control", "shell", "stdin", "iopub"}[t]
}

type Socket struct {
	zmq4.Socket
	Port int
}

type JupyterSocket struct {
	HB      *Socket
	Control *Socket
	Shell   *Socket
	Stdin   *Socket
	IOPub   *Socket
	All     [5]*Socket
}
