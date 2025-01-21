package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/mock_metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/utils"
	"go.uber.org/mock/gomock"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	shellListenPort int    = 7700
	transport       string = "tcp"
	ip              string = "127.0.0.1"
	signatureScheme string = "hmac-sha256"
	kernelKey              = "23d90942-8c3de3a713a5c3611792b7a5"
)

type wrappedServer struct {
	*AbstractServer
	sync.Mutex

	shellPort int
	id        string
}

func (s *wrappedServer) ID() string {
	return s.id
}

// SourceKernelID implements SourceKernel.
func (s *wrappedServer) SourceKernelID() string {
	return s.id
}

// ConnectionInfo implements SourceKernel.
func (s *wrappedServer) ConnectionInfo() *jupyter.ConnectionInfo {
	return &jupyter.ConnectionInfo{
		IP:              ip,
		ShellPort:       s.shellPort,
		Transport:       "tcp",
		SignatureScheme: "hmac-sha256",
		Key:             "149a41b5-0df54cf013c3035a3084a319",
	}
}

var _ = Describe("AbstractServer", func() {
	var (
		server                *wrappedServer
		client                *wrappedServer
		mockCtrl              *gomock.Controller
		serverMetricsProvider *mock_metrics.MockMessagingMetricsProvider
		clientMetricsProvider *mock_metrics.MockMessagingMetricsProvider
	)

	serverName := "TestServer"
	clientName := "TestClient"

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		serverMetricsProvider = mock_metrics.NewMockMessagingMetricsProvider(mockCtrl)
		clientMetricsProvider = mock_metrics.NewMockMessagingMetricsProvider(mockCtrl)

		_server := New(context.Background(), &jupyter.ConnectionInfo{Transport: "tcp"}, metrics.ClusterGateway, func(s *AbstractServer) {
			s.Sockets.Shell = &messaging.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: shellListenPort, Type: messaging.ShellMessage, Name: "TestServer_Router_Shell"}
			s.DebugMode = true
			s.ComponentId = serverName
			s.MessagingMetricsProvider = serverMetricsProvider
			config.InitLogger(&s.Log, "[SERVER] ")
		})
		server = &wrappedServer{AbstractServer: _server, shellPort: shellListenPort, id: "[SERVER]"}

		_client := New(context.Background(), &jupyter.ConnectionInfo{Transport: "tcp"}, metrics.LocalDaemon, func(s *AbstractServer) {
			s.Sockets.Shell = &messaging.Socket{Socket: zmq4.NewDealer(s.Ctx), Port: shellListenPort + 1, Type: messaging.ShellMessage, Name: "TestClient_Dealer_Shell"}
			s.DebugMode = true
			s.ComponentId = clientName
			s.MessagingMetricsProvider = clientMetricsProvider
			config.InitLogger(&s.Log, "[CLIENT] ")
		})
		client = &wrappedServer{AbstractServer: _client, shellPort: shellListenPort + 1, id: "[CLIENT]"}
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Reliable Message Delivery", func() {
		It("Will re-send messages until an ACK is received", func() {
			serverMetricsProvider.EXPECT().SentMessage(serverName, gomock.Any(), metrics.ClusterGateway, messaging.ShellMessage, gomock.Any()).MaxTimes(1)
			clientMetricsProvider.EXPECT().SentMessage(clientName, gomock.Any(), metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MinTimes(3).MaxTimes(3)
			clientMetricsProvider.EXPECT().SentMessageUnique(clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddMessageE2ELatencyObservation(gomock.Any(), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddAckReceivedLatency(gomock.Any(), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddNumSendAttemptsRequiredObservation(float64(3), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)

			err := server.Listen(server.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Dial(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed server socket @ %v", address1)

			var wg sync.WaitGroup
			wg.Add(5)

			serverMessagesReceived := 0
			respondAfterNMessages := 3
			handleServerMessage := func(info messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
				defer GinkgoRecover()

				server.Log.Info("Server received message: %v\n", msg)
				serverMessagesReceived += 1

				wg.Done()

				// Don't ackResponse until we've received several "retry" messages.
				if serverMessagesReceived < respondAfterNMessages {
					server.Log.Info("Discarding message. Number of messages received: %d / %d.", serverMessagesReceived, respondAfterNMessages)
					return nil
				}

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["session"] = DEST_KERNEL_ID
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				idFrame := msg.JupyterFrames.Frames[0]

				// Respond with ACK.
				ackResponse := zmq4.NewMsgFrom(idFrame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					*msg.HeaderFrame(),
					[]byte(""),
					[]byte(""))
				_, err = messaging.NewJupyterFramesFromBytes(ackResponse.Frames).Sign(signatureScheme, []byte(kernelKey))

				server.Log.Info("Responding to message with ACK: %v", ackResponse)

				time.Sleep(time.Millisecond * 5)

				err := info.Socket(typ).Send(ackResponse)
				Expect(err).To(BeNil())

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap["session"] = DEST_KERNEL_ID
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				bufferFrame := *msg.JupyterFrames.BuffersFrame()

				// Respond with "actual" message.
				actualResponse := zmq4.NewMsgFrom(idFrame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					*msg.HeaderFrame(),
					[]byte(""),
					[]byte(""),
					bufferFrame)

				jMsg := messaging.NewJupyterMessage(&actualResponse)
				_, err = jMsg.JupyterFrames.Sign(signatureScheme, []byte(kernelKey))
				Expect(err).To(BeNil())
				requestTrace, added, reqErr := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, time.Now(), server.Log)
				Expect(added).To(BeFalse())
				GinkgoWriter.Printf("reqErr: %v\n", reqErr)
				Expect(reqErr).To(BeNil())

				server.Log.Info("Now sending \"actual\" response: %v", actualResponse)

				time.Sleep(time.Millisecond * 5)

				err = info.Socket(typ).Send(actualResponse)
				Expect(err).To(BeNil())

				Expect(requestTrace.RequestReceivedByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestReceivedByLocalDaemon > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByLocalDaemon > 0).To(BeTrue())
				Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

				Expect(requestTrace.RequestSentByGateway > requestTrace.RequestReceivedByGateway).To(BeTrue())
				// Greater than or equal because local send is superfast.
				Expect(requestTrace.RequestReceivedByLocalDaemon >= requestTrace.RequestSentByGateway).To(BeTrue())
				Expect(requestTrace.RequestSentByLocalDaemon > requestTrace.RequestReceivedByLocalDaemon).To(BeTrue())

				wg.Done()

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, handleServerMessage)

			kernelId := DEST_KERNEL_ID
			msgId := uuid.NewString()
			headerMap := make(map[string]string)
			headerMap["msg_id"] = msgId
			headerMap["date"] = "2018-11-07T00:25:00.073876Z"
			headerMap["msg_type"] = messaging.KernelInfoRequest
			headerMap["session"] = kernelId
			header, _ := json.Marshal(&headerMap)

			msg := zmq4.NewMsgFrom(
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
				[]byte(""),
				header,
				[]byte(""),
				[]byte(""),
				[]byte(""))

			requestReceivedByGateway := time.Now()
			jMsg := messaging.NewJupyterMessage(&msg)
			fmt.Printf("msg.JupyterFrames.LenWithoutIdentitiesFrame: %d\nMsg.Frames length: %d\n\n", jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false), len(msg.Frames))
			requestTrace, added, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestReceivedByGateway, client.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeTrue())
			Expect(err).To(BeNil())
			//Expect(client.RequestLog.Size()).To(Equal(1))
			//Expect(client.RequestLog.EntriesByJupyterMsgId.Len()).To(Equal(1))
			Expect(jMsg.JupyterFrames.Len()).To(Equal(8))
			Expect(jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false)).To(Equal(7))

			fmt.Printf("msg.JupyterFrames.LenWithoutIdentitiesFrame: %d\nMsg.Frames length: %d\n\n", jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false), len(msg.Frames))

			Expect(requestTrace.MessageId).To(Equal(msgId))
			Expect(requestTrace.MessageType).To(Equal(messaging.KernelInfoRequest))
			Expect(requestTrace.KernelId).To(Equal(kernelId))

			m, err := json.Marshal(&proto.JupyterRequestTraceFrame{RequestTrace: requestTrace})
			Expect(err).To(BeNil())

			fmt.Printf("jMsg.JupyterFrames[%d+%d]: %s\n", jMsg.Offset(), messaging.JupyterFrameRequestTrace, string(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace]))
			fmt.Printf("Marshalled RequestTrace: %s\n", string(m))
			Expect(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace]).To(Equal(m))
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway.UnixMilli()))
			Expect(requestTrace.RequestSentByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			clientHandleMessage := func(info messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
				client.Log.Info("Client received %v message: %v", typ, msg)
				wg.Done()
				return nil
			}

			builder := messaging.NewRequestBuilder(context.Background(), client.id, client.id, client.ConnectionInfo()).
				WithAckRequired(true).
				WithAckTimeout(time.Millisecond * 1000).
				WithMessageType(messaging.ShellMessage).
				WithBlocking(true).
				WithTimeout(messaging.DefaultRequestTimeout).
				WithDoneCallback(messaging.DefaultDoneHandler).
				WithMessageHandler(clientHandleMessage).
				WithNumAttempts(3).
				WithRemoveDestFrame(true).
				WithSocketProvider(client).
				WithJMsgPayload(jMsg)
			request, err := builder.BuildRequest()
			Expect(err).To(BeNil())

			fmt.Printf("Request frames: %s\n", request.Payload().JupyterFrames.String())
			time.Sleep(time.Millisecond * 5)

			err = client.Request(request, client.Sockets.Shell)
			Expect(err).To(BeNil())

			// When no ACK is received, the server waits 5 seconds, then sleeps for a bit, then retries.
			wg.Wait()
			Expect(client.NumAcknowledgementsReceived()).To(Equal(int32(1)))
			Expect(serverMessagesReceived).To(Equal(3))

			_ = client.Sockets.Shell.Close()
			_ = server.Sockets.Shell.Close()
		})

		It("Will halt the retry procedure upon receiving an ACK.", func() {
			serverMetricsProvider.EXPECT().SentMessage(serverName, gomock.Any(), metrics.ClusterGateway, messaging.ShellMessage, gomock.Any()).MaxTimes(1)
			clientMetricsProvider.EXPECT().SentMessage(clientName, gomock.Any(), metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MinTimes(1).MaxTimes(1)
			clientMetricsProvider.EXPECT().SentMessageUnique(clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddMessageE2ELatencyObservation(gomock.Any(), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddAckReceivedLatency(gomock.Any(), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)
			clientMetricsProvider.EXPECT().AddNumSendAttemptsRequiredObservation(float64(1), clientName, metrics.LocalDaemon, messaging.ShellMessage, messaging.KernelInfoRequest).MaxTimes(1)

			err := server.Listen(server.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Dial(address1)
			Expect(err).To(BeNil())

			var wg sync.WaitGroup
			wg.Add(3)

			client.Log.Debug("Dialed server socket @ %v", address1)

			serverMessagesReceived := 0
			handleServerMessage := func(info messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
				defer GinkgoRecover()

				server.Log.Info("Server received message: %v\n", msg)
				serverMessagesReceived += 1

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["session"] = DEST_KERNEL_ID
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				idFrame := msg.JupyterFrames.Frames[0]

				var wrapper *proto.JupyterRequestTraceFrame
				err := json.Unmarshal(msg.JupyterFrames.Frames[msg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace], &wrapper)
				Expect(err).To(BeNil())

				requestTrace := wrapper.RequestTrace
				Expect(requestTrace).ToNot(BeNil())
				server.Log.Debug(utils.LightBlueStyle.Render("RequestTrace: %s"), requestTrace.String())
				Expect(requestTrace.RequestReceivedByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestReceivedByLocalDaemon > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

				GinkgoWriter.Printf("requestTrace.RequestReceivedByGateway: %d\n", requestTrace.RequestSentByGateway)
				GinkgoWriter.Printf("requestTrace.RequestSentByGateway: %d\n", requestTrace.RequestSentByGateway)
				GinkgoWriter.Printf("requestTrace.RequestReceivedByLocalDaemon: %d\n", requestTrace.RequestSentByGateway)

				Expect(requestTrace.RequestSentByGateway > requestTrace.RequestReceivedByGateway).To(BeTrue())
				// Greater than or equal because local send is superfast.
				Expect(requestTrace.RequestReceivedByLocalDaemon >= requestTrace.RequestSentByGateway).To(BeTrue())

				// Respond with ACK.
				ackResponse := zmq4.NewMsgFrom(idFrame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					*msg.HeaderFrame(),
					[]byte(""),
					[]byte(""))

				server.Log.Info("Responding to message with ACK: %v", ackResponse)

				time.Sleep(time.Millisecond * 5)

				err = info.Socket(typ).Send(ackResponse)
				Expect(err).To(BeNil())

				wg.Done()

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				bufferFrame := *msg.JupyterFrames.BuffersFrame()

				// Respond with "actual" message.
				actualResponse := zmq4.NewMsgFrom(idFrame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					*msg.HeaderFrame(),
					[]byte(""),
					[]byte(""),
					bufferFrame)

				time.Sleep(time.Millisecond * 5)

				jMsg := messaging.NewJupyterMessage(&actualResponse)
				_, err = jMsg.JupyterFrames.Sign(signatureScheme, []byte(kernelKey))
				Expect(err).To(BeNil())
				requestTrace, added, reqErr := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, time.Now(), server.Log) // Simulate send
				server.Log.Debug(utils.LightBlueStyle.Render("RequestTrace: %s"), requestTrace.String())
				Expect(added).To(BeFalse())
				GinkgoWriter.Printf("reqErr: %v\n", reqErr)
				Expect(reqErr).To(BeNil())

				server.Log.Debug("Now sending \"actual\" response: %v", jMsg)

				err = info.Socket(typ).Send(*jMsg.GetZmqMsg())
				Expect(err).To(BeNil())

				Expect(requestTrace.RequestReceivedByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByGateway > 0).To(BeTrue())
				Expect(requestTrace.RequestReceivedByLocalDaemon > 0).To(BeTrue())
				Expect(requestTrace.RequestSentByLocalDaemon > 0).To(BeTrue())
				Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
				Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

				Expect(requestTrace.RequestSentByGateway > requestTrace.RequestReceivedByGateway).To(BeTrue())
				// Greater than or equal because local send is superfast.
				Expect(requestTrace.RequestReceivedByLocalDaemon >= requestTrace.RequestSentByGateway).To(BeTrue())
				Expect(requestTrace.RequestSentByLocalDaemon > requestTrace.RequestReceivedByLocalDaemon).To(BeTrue())

				wg.Done()

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, handleServerMessage)

			msgId := uuid.NewString()
			headerMap := make(map[string]string)
			headerMap["msg_id"] = msgId
			headerMap["session"] = DEST_KERNEL_ID
			headerMap["date"] = "2018-11-07T00:25:00.073876Z"
			headerMap["msg_type"] = "kernel_info_request"
			header, _ := json.Marshal(&headerMap)

			msg := zmq4.NewMsgFrom(
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
				[]byte(""),
				header,
				[]byte(""),
				[]byte(""),
				[]byte(""))
			now := time.Now()
			jMsg := messaging.NewJupyterMessage(&msg)
			_, err = jMsg.JupyterFrames.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			fmt.Printf("[a] jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false): %d\njMsg.JupyterFrames.Len(): %d\nOffset: %d\n\n", jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false), jMsg.JupyterFrames.Len(), jMsg.JupyterFrames.Offset)
			requestTrace, added, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, now, client.Log) // Simulate recv
			client.Log.Debug(utils.LightBlueStyle.Render("RequestTrace: %s"), requestTrace.String())
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeTrue())
			Expect(err).To(BeNil())
			//Expect(client.RequestLog.Size()).To(Equal(1))
			//Expect(client.RequestLog.EntriesByJupyterMsgId.Len()).To(Equal(1))
			Expect(jMsg.JupyterFrames.Len()).To(Equal(8))
			Expect(jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false)).To(Equal(7))

			fmt.Printf("[b] jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false): %d\njMsg.JupyterFrames.Len(): %d\nOffset: %d\n\n", jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false), jMsg.JupyterFrames.Len(), jMsg.JupyterFrames.Offset)

			Expect(requestTrace.MessageId).To(Equal(msgId))
			Expect(requestTrace.MessageType).To(Equal(messaging.KernelInfoRequest))
			Expect(requestTrace.KernelId).To(Equal(DEST_KERNEL_ID))

			m, err := json.Marshal(&proto.JupyterRequestTraceFrame{RequestTrace: requestTrace})
			Expect(err).To(BeNil())

			Expect(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace]).To(Equal(m))
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(now.UnixMilli()))
			Expect(requestTrace.RequestSentByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			time.Sleep(time.Millisecond * 125) // Sleep long enough to ensure timestamps in RequestTrace are different

			clientHandleMessage := func(info messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
				client.Log.Info("Client received %v message: %v", typ, msg)
				wg.Done()
				return nil
			}

			builder := messaging.NewRequestBuilder(context.Background(), client.id, client.id, client.ConnectionInfo()).
				WithAckRequired(true).
				WithMessageType(messaging.ShellMessage).
				WithBlocking(true).
				WithTimeout(messaging.DefaultRequestTimeout).
				WithDoneCallback(messaging.DefaultDoneHandler).
				WithMessageHandler(clientHandleMessage).
				WithNumAttempts(3).
				WithRemoveDestFrame(true).
				WithSocketProvider(client).
				WithJMsgPayload(jMsg)
			request, err := builder.BuildRequest()
			Expect(err).To(BeNil())

			err = client.Request(request, client.Sockets.Shell)
			Expect(err).To(BeNil())

			wg.Wait()
			Expect(client.NumAcknowledgementsReceived()).To(Equal(int32(1)))
			Expect(serverMessagesReceived).To(Equal(1))

			_ = client.Sockets.Shell.Close()
			_ = server.Sockets.Shell.Close()
		})
	})
})
