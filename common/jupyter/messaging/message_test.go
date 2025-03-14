package messaging_test

import (
	"encoding/json"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
)

var _ = Describe("Message", func() {
	It("Will correctly convert between messaging.MessageHeader and proto.JupyterMessageHeader structs", func() {
		msgId := "119856f2-efd6-4131-8d9f-f1081fc3c920"
		username := "09ba3ce8-3644-4d27-bd17-e94901ac1936"
		session := "f8b1709e-51e5-46e7-9047-99a3636bef14"
		date := "2024-04-03T22:55:52.605Z"
		msgType := "execute_request"
		version := "5.2"

		header := &messaging.MessageHeader{
			MsgID:    msgId,
			Username: username,
			Session:  session,
			Date:     date,
			MsgType:  messaging.JupyterMessageType(msgType),
			Version:  version,
		}

		protoHeader := header.ToProto()
		Expect(protoHeader).ToNot(BeNil())

		Expect(protoHeader.MessageId).To(Equal(msgId))
		Expect(protoHeader.MessageId).To(Equal(header.MsgID))

		Expect(protoHeader.Username).To(Equal(username))
		Expect(protoHeader.Username).To(Equal(header.Username))

		Expect(protoHeader.Session).To(Equal(session))
		Expect(protoHeader.Session).To(Equal(header.Session))

		Expect(protoHeader.Date).To(Equal(date))
		Expect(protoHeader.Date).To(Equal(header.Date))

		Expect(protoHeader.MessageType).To(Equal(msgType))
		Expect(protoHeader.MessageType).To(Equal(header.MsgType.String()))

		Expect(protoHeader.Version).To(Equal(version))
		Expect(protoHeader.Version).To(Equal(header.Version))

		convertedHeader := messaging.MessageHeaderFromProto(protoHeader)
		Expect(convertedHeader).ToNot(BeNil())
		Expect(convertedHeader.MsgID).To(Equal(msgId))
	})

	It("Will correctly convert between messaging.JupyterMessage and proto.JupyterMessage structs", func() {
		requestMsgId := "303afbe8-156e-4570-8542-b830b7e593-1"
		replyMsgId := "303afbe8-156e-4570-8542-b830b7e593-2"

		username := "jovyan"
		session := "8d929395-c277-4174-ba35-98eb1dcafbd1"

		requestDate := "2024-04-03T20:35:34.641Z"
		replyDate := "2024-04-03T22:55:52.605Z"

		requestMsgType := "execute_request"
		replyMsgType := "execute_reply"

		version := "5.2"

		header := &messaging.MessageHeader{
			MsgID:    replyMsgId,
			Username: username,
			Session:  session,
			Date:     replyDate,
			MsgType:  messaging.JupyterMessageType(replyMsgType),
			Version:  version,
		}

		parentHeader := &messaging.MessageHeader{
			MsgID:    requestMsgId,
			Username: username,
			Session:  session,
			Date:     requestDate,
			MsgType:  messaging.JupyterMessageType(requestMsgType),
			Version:  version,
		}

		encodedHeader, err := json.Marshal(header)
		Expect(err).To(BeNil())
		Expect(encodedHeader).ToNot(BeNil())

		encodedParentHeader, err := json.Marshal(parentHeader)
		Expect(err).To(BeNil())
		Expect(encodedParentHeader).ToNot(BeNil())

		metadata := map[string]interface{}{
			messaging.TargetReplicaArg: 3,
		}

		encodedMetadata, err := json.Marshal(metadata)
		Expect(err).To(BeNil())
		Expect(encodedMetadata).ToNot(BeNil())

		content := map[string]interface{}{
			"silent":           false,
			"store_history":    true,
			"user_expressions": make(map[string]interface{}),
			"allow_stdin":      true,
			"stop_on_error":    false,
			"code":             "a = 1 + 2\nprint(f'a: {a}')",
		}

		encodedContent, err := json.Marshal(content)
		Expect(err).To(BeNil())
		Expect(encodedContent).ToNot(BeNil())

		frames := [][]byte{
			[]byte("8e32bb68-baf5-4842-b3c8-2e8c109af095"), /* Identity */
			[]byte("<IDS|MSG>"),                            /* Start */
			[]byte(""),                                     /* Signature */
			encodedHeader,                                  /* Header */
			encodedParentHeader,                            /* Parent header*/
			encodedMetadata,                                /* Metadata */
			encodedContent,                                 /* Content */
		}

		jupyterFrames := messaging.NewJupyterFramesFromBytes(frames)
		Expect(jupyterFrames).ToNot(BeNil())

		msg := &zmq4.Msg{
			Frames: frames,
			Type:   zmq4.UsrMsg,
		}

		jMsg := messaging.NewJupyterMessage(msg)
		Expect(jMsg).ToNot(BeNil())

		Expect(jMsg.JupyterFrames).To(Equal(jupyterFrames))
		Expect(jMsg.JupyterFrames.Frames).To(Equal(frames))

		protoMessage, err := jMsg.ToProto()
		Expect(err).To(BeNil())
		Expect(protoMessage).ToNot(BeNil())

		protoHeader := protoMessage.Header
		Expect(protoHeader).ToNot(BeNil())

		protoParentHeader := protoMessage.ParentHeader
		Expect(protoParentHeader).ToNot(BeNil())

		convertedHeader := messaging.MessageHeaderFromProto(protoHeader)
		Expect(convertedHeader).ToNot(BeNil())

		Expect(protoHeader.MessageId).To(Equal(replyMsgId))
		Expect(protoHeader.MessageId).To(Equal(header.MsgID))

		Expect(protoHeader.Username).To(Equal(username))
		Expect(protoHeader.Username).To(Equal(header.Username))

		Expect(protoHeader.Session).To(Equal(session))
		Expect(protoHeader.Session).To(Equal(header.Session))

		Expect(protoHeader.Date).To(Equal(replyDate))
		Expect(protoHeader.Date).To(Equal(header.Date))

		Expect(protoHeader.MessageType).To(Equal(replyMsgType))
		Expect(protoHeader.MessageType).To(Equal(header.MsgType.String()))

		Expect(protoHeader.Version).To(Equal(version))
		Expect(protoHeader.Version).To(Equal(header.Version))

		convertedParentHeader := messaging.MessageHeaderFromProto(protoParentHeader)
		Expect(convertedParentHeader).ToNot(BeNil())

		Expect(protoParentHeader.MessageId).To(Equal(requestMsgId))
		Expect(protoParentHeader.MessageId).To(Equal(parentHeader.MsgID))

		Expect(protoParentHeader.Username).To(Equal(username))
		Expect(protoParentHeader.Username).To(Equal(parentHeader.Username))

		Expect(protoParentHeader.Session).To(Equal(session))
		Expect(protoParentHeader.Session).To(Equal(parentHeader.Session))

		Expect(protoParentHeader.Date).To(Equal(requestDate))
		Expect(protoParentHeader.Date).To(Equal(parentHeader.Date))

		Expect(protoParentHeader.MessageType).To(Equal(requestMsgType))
		Expect(protoParentHeader.MessageType).To(Equal(parentHeader.MsgType.String()))

		Expect(protoParentHeader.Version).To(Equal(version))
		Expect(protoParentHeader.Version).To(Equal(parentHeader.Version))

		Expect(header.Equals(convertedHeader)).To(BeTrue())
		Expect(parentHeader.Equals(convertedParentHeader)).To(BeTrue())

		Expect(protoMessage.Metadata).To(Equal(encodedMetadata))
		Expect(protoMessage.Content).To(Equal(encodedContent))
	})
})
