package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	return s.AbstractServer.AddSourceKernelFrame(frames, destID, jOffset)
}

// ConnectionInfo implements SourceKernel.
func (s *wrappedServer) ConnectionInfo() *types.ConnectionInfo {
	return &types.ConnectionInfo{
		IP:        ip,
		ShellPort: s.shellPort,
		Transport: "tcp",
	}
}

// ExtractSourceKernelFrame implements SourceKernel.
// Subtle: this method shadows the method (*AbstractServer).ExtractSourceKernelFrame of wrappedServer.AbstractServer.
func (s *wrappedServer) ExtractSourceKernelFrame(frames [][]byte) (destID string, jOffset int) {
	return s.AbstractServer.ExtractSourceKernelFrame(frames)
}

// RemoveSourceKernelFrame implements SourceKernel.
// Subtle: this method shadows the method (*AbstractServer).RemoveSourceKernelFrame of wrappedServer.AbstractServer.
func (s *wrappedServer) RemoveSourceKernelFrame(frames [][]byte, jOffset int) (oldFrams [][]byte) {
	return s.AbstractServer.RemoveSourceKernelFrame(frames, jOffset)
}

func (s *wrappedServer) RequestDestID() string {
	return s.id
}

var _ = Describe("AbstractServer", func() {
	var server *wrappedServer
	var client *wrappedServer

	config.LogLevel = logger.LOG_LEVEL_ALL

	Context("Reliable Message Delivery", func() {
		BeforeEach(func() {
			_server := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, true, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: shellListenPort, Type: types.ShellMessage}
			})
			config.InitLogger(&_server.Log, "[SERVER]")
			server = &wrappedServer{_server, shellListenPort, "[SERVER]"}

			_client := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, true, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: zmq4.NewDealer(s.Ctx), Port: shellListenPort + 1, Type: types.ShellMessage}
			})
			config.InitLogger(&_client.Log, "[CLIENT]")
			client = &wrappedServer{_client, shellListenPort + 1, "[CLIENT]"}
		})

		It("Will re-send messages until an ACK is received", func() {
			err := server.Listen(server.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Dial(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed server socket @ %v", address1)

			serverMessagesReceived := 0
			respondAfterNMessages := 3
			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				server.Log.Info("Server received message: %v\n", msg)
				serverMessagesReceived += 1

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

				server.Log.Info("Responding to message with reply: %v", reply)

				err := info.Socket(typ).Send(reply)
				Expect(err).To(BeNil())

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, server, handleServerMessage)

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
				return nil
			}
			err = client.Request(context.Background(), client, client.Sockets.Shell, &msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			// When no ACK is received, the server waits 5 seconds, then sleeps for a bit, then retries.
			time.Sleep(time.Millisecond * 18000)
			Expect(client.NumAcksReceived()).To(Equal(1))

			client.Sockets.Shell.Close()
			server.Sockets.Shell.Close()
		})

		It("Will not retry sending messages upon receiving an ACK.", func() {
			err := server.Listen(server.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Dial(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed server socket @ %v", address1)

			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
				server.Log.Info("Server received message: %v\n", msg)

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

				server.Log.Info("Responding to message with reply: %v", reply)

				err := info.Socket(typ).Send(reply)
				Expect(err).To(BeNil())

				server.Log.Info("Responded to message.")

				return nil
			}

			go server.Serve(server, server.Sockets.Shell, server, handleServerMessage)

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
				return nil
			}
			err = client.Request(context.Background(), client, client.Sockets.Shell, &msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			time.Sleep(time.Millisecond * 1500)
			Expect(client.NumAcksReceived()).To(Equal(1))

			client.Sockets.Shell.Close()
			server.Sockets.Shell.Close()
		})
	})
})
