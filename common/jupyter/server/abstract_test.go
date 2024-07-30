package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	shellListenPort int    = 7700
	transport       string = "tcp"
	ip              string = "127.0.0.1"
)

type wrappedServer struct {
	*AbstractServer
	sync.Mutex

	shellPort int
	id        string
}

// SourceKernelID implements SourceKernel.
func (s *wrappedServer) SourceKernelID() string {
	return s.id
}

// AddSourceKernelFrame implements SourceKernel.
// Subtle: this method shadows the method (*AbstractServer).AddSourceKernelFrame of wrappedServer.AbstractServer.
func (s *wrappedServer) AddSourceKernelFrame(frames [][]byte, destID string, jOffset int) (newFrames [][]byte) {
	return types.AddSourceKernelFrame(frames, destID, jOffset)
}

// ConnectionInfo implements SourceKernel.
func (s *wrappedServer) ConnectionInfo() *types.ConnectionInfo {
	return &types.ConnectionInfo{
		IP:              ip,
		ShellPort:       s.shellPort,
		Transport:       "tcp",
		SignatureScheme: "hmac-sha256",
		Key:             "149a41b5-0df54cf013c3035a3084a319",
	}
}

// ExtractSourceKernelFrame implements SourceKernel.
// Subtle: this method shadows the method (*AbstractServer).ExtractSourceKernelFrame of wrappedServer.AbstractServer.
func (s *wrappedServer) ExtractSourceKernelFrame(frames [][]byte) (destID string, jOffset int) {
	return types.ExtractSourceKernelFrame(frames)
}

// RemoveSourceKernelFrame implements SourceKernel.
// Subtle: this method shadows the method (*AbstractServer).RemoveSourceKernelFrame of wrappedServer.AbstractServer.
func (s *wrappedServer) RemoveSourceKernelFrame(frames [][]byte, jOffset int) (oldFrams [][]byte) {
	return types.RemoveSourceKernelFrame(frames, jOffset)
}

var _ = Describe("AbstractServer", func() {
	var server *wrappedServer
	var client *wrappedServer

	config.LogLevel = logger.LOG_LEVEL_ALL

	Context("Reliable Message Delivery", func() {
		BeforeEach(func() {
			_server := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: shellListenPort, Type: types.ShellMessage, Name: "TestServer_Router_Shell"}
			})
			config.InitLogger(&_server.Log, "[SERVER]")
			server = &wrappedServer{AbstractServer: _server, shellPort: shellListenPort, id: "[SERVER]"}

			_client := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: zmq4.NewDealer(s.Ctx), Port: shellListenPort + 1, Type: types.ShellMessage, Name: "TestClient_Dealer_Shell"}
			})
			config.InitLogger(&_client.Log, "[CLIENT]")
			client = &wrappedServer{AbstractServer: _client, shellPort: shellListenPort + 1, id: "[CLIENT]"}
		})

		It("Will re-send messages until an ACK is received", func() {
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
			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				server.Log.Info("Server received message: %v\n", msg)
				serverMessagesReceived += 1

				wg.Done()

				// Don't reply until we've received several "retry" messages.
				if serverMessagesReceived < respondAfterNMessages {
					server.Log.Info("Discarding message. Number of messages received: %d / %d.", serverMessagesReceived, respondAfterNMessages)
					return nil
				}

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				id_frame := []byte(msg.Frames[0])

				// Respond with ACK.
				reply := zmq4.NewMsgFrom(id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					[]byte(""),
					[]byte(""),
					[]byte(""),
					[]byte(""))

				server.Log.Info("Responding to message with ACK: %v", reply)

				err := info.Socket(typ).Send(reply)
				Expect(err).To(BeNil())

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				// Respond with "actual" message.
				reply2 := zmq4.NewMsgFrom(id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					[]byte("2"),
					[]byte(""),
					[]byte(""),
					[]byte(""))

				server.Log.Info("Now sending \"actual\" response: %v", reply2)

				err = info.Socket(typ).Send(reply2)
				Expect(err).To(BeNil())

				wg.Done()

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, handleServerMessage)

			headerMap := make(map[string]string)
			headerMap["msg_id"] = uuid.NewString()
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

			clientHandleMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				client.Log.Info("Client received %v message: %v", typ, msg)
				wg.Done()
				return nil
			}

			builder := types.NewRequestBuilder(context.Background(), client.id, client.id, client.ConnectionInfo()).
				WithAckRequired(true).
				WithBlocking(true).
				WithDoneCallback(types.DefaultDoneHandler).
				WithMessageHandler(clientHandleMessage).
				WithNumAttempts(3).
				WithRemoveDestFrame(true).
				WithSocketProvider(client).
				WithPayload(&msg)
			request, err := builder.BuildRequest()
			Expect(err).To(BeNil())

			err = client.Request(request, client.Sockets.Shell)
			// err = client.Request(context.Background(), client, client.Sockets.Shell, &msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			// When no ACK is received, the server waits 5 seconds, then sleeps for a bit, then retries.
			wg.Wait()
			Expect(client.NumAcksReceived()).To(Equal(1))
			Expect(serverMessagesReceived).To(Equal(3))

			client.Sockets.Shell.Close()
			server.Sockets.Shell.Close()
		})

		It("Will halt the retry procedure upon receiving an ACK.", func() {
			err := server.Listen(server.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Dial(address1)
			Expect(err).To(BeNil())

			var wg sync.WaitGroup
			wg.Add(3)

			client.Log.Debug("Dialed server socket @ %v", address1)

			serverMessagesReceived := 0
			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				server.Log.Info("Server received message: %v\n", msg)
				serverMessagesReceived += 1

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				id_frame := []byte(msg.Frames[0])

				// Respond with ACK.
				reply := zmq4.NewMsgFrom(id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					[]byte(""),
					[]byte(""),
					[]byte(""),
					[]byte(""))

				server.Log.Info("Responding to message with ACK: %v", reply)

				err := info.Socket(typ).Send(reply)
				Expect(err).To(BeNil())

				wg.Done()

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				// Respond with "actual" message.
				reply2 := zmq4.NewMsgFrom(id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					[]byte("2"),
					[]byte(""),
					[]byte(""),
					[]byte(""))

				server.Log.Info("Now sending \"actual\" response: %v", reply2)

				err = info.Socket(typ).Send(reply2)
				Expect(err).To(BeNil())

				wg.Done()

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, handleServerMessage)

			headerMap := make(map[string]string)
			headerMap["msg_id"] = uuid.NewString()
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

			clientHandleMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				client.Log.Info("Client received %v message: %v", typ, msg)
				wg.Done()
				return nil
			}

			builder := types.NewRequestBuilder(context.Background(), client.id, client.id, client.ConnectionInfo()).
				WithAckRequired(true).
				WithBlocking(true).
				WithDoneCallback(types.DefaultDoneHandler).
				WithMessageHandler(clientHandleMessage).
				WithNumAttempts(3).
				WithRemoveDestFrame(true).
				WithSocketProvider(client).
				WithPayload(&msg)
			request, err := builder.BuildRequest()
			Expect(err).To(BeNil())

			err = client.Request(request, client.Sockets.Shell)
			// err = client.Request(context.Background(), client, client.Sockets.Shell, &msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			wg.Wait()
			Expect(client.NumAcksReceived()).To(Equal(1))
			Expect(serverMessagesReceived).To(Equal(1))

			client.Sockets.Shell.Close()
			server.Sockets.Shell.Close()
		})
	})
})
