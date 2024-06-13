package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	AckMsg zmq4.MsgType = 2
)

var (
	ErrCannotExtractAck = errors.New("could not extract acknowledgement from message")
)

type MessageAcknowledgement interface {
	GetRequestID() string             // The ID of the request being ACK'd.
	GetSocketType() types.MessageType // The Socket type (e.g., Shell, Control, IO, etc.) of the request being ACK'd.
	String() string                   // ToString
}

type messageAcknowledgementImpl struct {
	RequestID  string            `json:"request_id"`  // The ID of the request being ACK'd.
	SocketType types.MessageType `json:"socket_type"` // The Socket type (e.g., Shell, Control, IO, etc.) of the request being ACK'd.
}

func newMessageAcknowledgement(reqId string, typ types.MessageType) *messageAcknowledgementImpl {
	return &messageAcknowledgementImpl{
		RequestID:  reqId,
		SocketType: typ,
	}
}

func (ack *messageAcknowledgementImpl) String() string {
	return fmt.Sprintf("messageAcknowledgementImpl[reqID=%v, typ=%v]", ack.RequestID, ack.SocketType)
}

func (ack *messageAcknowledgementImpl) GetRequestID() string {
	return ack.RequestID
}

func (ack *messageAcknowledgementImpl) GetSocketType() types.MessageType {
	return ack.SocketType
}

func (ack *messageAcknowledgementImpl) toZMQ4Message(dest RequestDest) *zmq4.Msg {
	ackFrame, err := json.Marshal(ack)
	if err != nil {
		panic(err)
	}

	frames := [][]byte{
		[]byte("<IDS|MSG>"),
		ackFrame,
	}

	frames, _ = dest.AddDestFrame(frames, dest.RequestDestID(), JOffsetAutoDetect)
	msg := &zmq4.Msg{
		Frames: frames,
	}

	return msg
}

func ExtractMessageAcknowledgement(msg *zmq4.Msg) (MessageAcknowledgement, error) {
	frames := msg.Frames
	if len(frames) == 0 {
		return nil, ErrCannotExtractAck
	}

	i := 0
	// Jupyter messages start from "<IDS|MSG>" frame.
	for i < len(frames) && string(frames[i]) != "<IDS|MSG>" {
		i++
	}

	var ack *messageAcknowledgementImpl
	err := json.Unmarshal(frames[i], &ack)

	return ack, err
}
