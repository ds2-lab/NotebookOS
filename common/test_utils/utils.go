package test_utils

import (
	"encoding/json"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
)

const (
	SignatureScheme string = "hmac-sha256"
)

func CreateJupyterMessage(messageType messaging.JupyterMessageType, kernelId string, kernelKey string) *messaging.JupyterMessage {
	header := &messaging.MessageHeader{
		MsgID:    uuid.NewString(),
		Username: kernelId,
		Session:  kernelId,
		Date:     "2024-04-03T22:55:52.605Z",
		MsgType:  messageType,
		Version:  "5.2",
	}

	unsignedExecReqFrames := [][]byte{
		[]byte("<IDS|MSG>"), /* Frame start */
		[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
		[]byte(""),   /* Header */
		[]byte(""),   /* Parent header */
		[]byte("{}"), /* Metadata */
		[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
	}
	jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecReqFrames)
	err := jFrames.EncodeHeader(header)
	Expect(err).To(BeNil())

	frames, _ := jFrames.Sign(SignatureScheme, []byte(kernelKey))
	msg := &zmq4.Msg{
		Frames: frames,
		Type:   zmq4.UsrMsg,
	}

	return messaging.NewJupyterMessage(msg)
}

func CreateJupyterMessageWithMetadata(messageType messaging.JupyterMessageType, kernelId string, kernelKey string, metadata map[string]interface{}) *messaging.JupyterMessage {
	header := &messaging.MessageHeader{
		MsgID:    uuid.NewString(),
		Username: kernelId,
		Session:  kernelId,
		Date:     "2024-04-03T22:55:52.605Z",
		MsgType:  messageType,
		Version:  "5.2",
	}

	metadataEndoded, err := json.Marshal(metadata)
	Expect(err).To(BeNil())

	unsignedExecReqFrames := [][]byte{
		[]byte("<IDS|MSG>"), /* Frame start */
		[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
		[]byte(""),              /* Header */
		[]byte(""),              /* Parent header */
		[]byte(metadataEndoded), /* Metadata */
		[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
	}
	jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecReqFrames)
	err = jFrames.EncodeHeader(header)
	Expect(err).To(BeNil())

	frames, _ := jFrames.Sign(SignatureScheme, []byte(kernelKey))
	msg := &zmq4.Msg{
		Frames: frames,
		Type:   zmq4.UsrMsg,
	}

	return messaging.NewJupyterMessage(msg)
}

func CreateJupyterMessageWithContent(messageType messaging.JupyterMessageType, kernelId string, kernelKey string,
	content map[string]interface{}, parentHeader *messaging.MessageHeader) *messaging.JupyterMessage {

	header := &messaging.MessageHeader{
		MsgID:    uuid.NewString(),
		Username: kernelId,
		Session:  kernelId,
		Date:     "2024-04-03T22:55:52.605Z",
		MsgType:  messageType,
		Version:  "5.2",
	}

	contentEndoded, err := json.Marshal(content)
	Expect(err).To(BeNil())

	unsignedExecReqFrames := [][]byte{
		[]byte("<IDS|MSG>"), /* Frame start */
		[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
		[]byte(""),     /* Header */
		[]byte(""),     /* Parent header */
		[]byte("{}"),   /* Metadata */
		contentEndoded, /* Content */
	}
	jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecReqFrames)
	err = jFrames.EncodeHeader(header)
	Expect(err).To(BeNil())

	if parentHeader != nil {
		err = jFrames.EncodeParentHeader(parentHeader)
		Expect(err).To(BeNil())
	}

	frames, _ := jFrames.Sign(SignatureScheme, []byte(kernelKey))
	msg := &zmq4.Msg{
		Frames: frames,
		Type:   zmq4.UsrMsg,
	}

	return messaging.NewJupyterMessage(msg)
}
