package types

import (
	"github.com/go-zeromq/zmq4"
)

const (
	HBMessage MessageType = iota
	ControlMessage
	ShellMessage
	StdinMessage
	IOPubMessage
)

type MessageType int

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
