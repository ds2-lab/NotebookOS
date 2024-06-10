package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pebbe/zmq4"
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
	return s.AbstractServer.AddSourceKernelFrame(frames, destID, jOffset)
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
	var srvr *wrappedServer
	var client *wrappedServer

	config.LogLevel = logger.LOG_LEVEL_ALL

	Context("Reliable Message Delivery", func() {
		BeforeEach(func() {
			shell_socket, err := zmq4.NewSocket(zmq4.ROUTER)
			if err != nil {
				panic(err)
			}
			if err := shell_socket.SetRouterMandatory(1); err != nil {
				panic(err)
			}
			_server := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: shell_socket, Port: shellListenPort, Type: types.ShellMessage, Name: "TestServer_Shell"}
			})
			config.InitLogger(&_server.Log, "[SERVER]")
			srvr = &wrappedServer{AbstractServer: _server, shellPort: shellListenPort, id: "[SERVER]"}

			dealer_socket, err := zmq4.NewSocket(zmq4.DEALER)
			if err != nil {
				panic(err)
			}
			_client := New(context.Background(), &types.ConnectionInfo{Transport: "tcp"}, func(s *AbstractServer) {
				s.Sockets.Shell = &types.Socket{Socket: dealer_socket, Port: shellListenPort + 1, Type: types.ShellMessage, Name: "TestClient_Shell"}
			})
			config.InitLogger(&_client.Log, "[CLIENT]")
			client = &wrappedServer{AbstractServer: _client, shellPort: shellListenPort + 1, id: "[CLIENT]"}
		})

		It("Will re-send messages until an ACK is received", func() {
			err := srvr.Listen(srvr.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Connect(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed srvr socket @ %v", address1)

			var wg sync.WaitGroup
			wg.Add(5)

			serverMessagesReceived := 0
			respondAfterNMessages := 3
			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				srvr.Log.Info("Server received message: %v\n", FramesToString(msg))
				serverMessagesReceived += 1

				wg.Done()

				// Don't reply until we've received several "retry" messages.
				if serverMessagesReceived < respondAfterNMessages {
					srvr.Log.Info("Discarding message. Number of messages received: %d / %d.", serverMessagesReceived, respondAfterNMessages)
					return nil
				}

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				id_frame := []byte(msg[0])

				// Respond with ACK.
				reply := [][]byte{id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					[]byte(""),
					[]byte(""),
					[]byte(""),
					[]byte("")}

				srvr.Log.Info("Responding to message with ACK: %v", FramesToString(reply))

				total, err := info.Socket(typ).SendMessage(reply)
				Expect(err).To(BeNil())
				Expect(total > 0).To(BeTrue())

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				// Respond with "actual" message.
				reply2 := [][]byte{id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					[]byte("2"),
					[]byte(""),
					[]byte(""),
					[]byte("")}

				srvr.Log.Info("Now sending \"actual\" response: %v", FramesToString(reply2))

				total, err = info.Socket(typ).SendMessage(reply2)
				Expect(err).To(BeNil())
				Expect(total > 0).To(BeTrue())

				wg.Done()

				srvr.Log.Info("Responded to message.")

				return nil
			}

			go srvr.Serve(srvr, srvr.Sockets.Shell, srvr, handleServerMessage, true)

			headerMap := make(map[string]string)
			headerMap["msg_id"] = uuid.NewString()
			headerMap["date"] = "2018-11-07T00:25:00.073876Z"
			headerMap["msg_type"] = "kernel_info_request"
			header, _ := json.Marshal(&headerMap)

			msg := [][]byte{
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
				[]byte(""),
				header,
				[]byte(""),
				[]byte(""),
				[]byte("")}

			clientHandleMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				client.Log.Info("Client received %v message: %v", typ, FramesToString(msg))
				wg.Done()
				return nil
			}
			err = client.Request(context.Background(), client, client.Sockets.Shell, msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			// When no ACK is received, the srvr waits 5 seconds, then sleeps for a bit, then retries.
			wg.Wait()
			Expect(client.NumAcksReceived()).To(Equal(1))

			client.Sockets.Shell.Close()
			srvr.Sockets.Shell.Close()
		})

		It("Will panic when sending an ACK via a Router socket without the identity frame.", func() {
			err := srvr.Listen(srvr.Sockets.Shell)
			Expect(err).To(BeNil())

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Connect(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed srvr socket @ %v", address1)

			var wg sync.WaitGroup
			wg.Add(1)

			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				srvr.Log.Info("Server received message: %v\n", FramesToString(msg))

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				// Respond with ACK that does NOT have an identity frame at the beginning.
				// This should panic.
				reply := [][]byte{getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					[]byte(""),
					[]byte(""),
					[]byte(""),
					[]byte("")}

				srvr.Log.Info("Responding to message with reply: %v", FramesToString(reply))

				total, err := info.Socket(typ).SendMessage(reply)
				Expect(total).To(Equal(-1))
				Expect(err).ToNot(BeNil())

				wg.Done()

				return nil
			}

			go srvr.Serve(srvr, srvr.Sockets.Shell, srvr, handleServerMessage, true)

			headerMap := make(map[string]string)
			headerMap["msg_id"] = uuid.NewString()
			headerMap["date"] = "2018-11-07T00:25:00.073876Z"
			headerMap["msg_type"] = "kernel_info_request"
			header, _ := json.Marshal(&headerMap)

			msg := [][]byte{
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
				[]byte(""),
				header,
				[]byte(""),
				[]byte(""),
				[]byte("")}

			clientHandleMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				client.Log.Info("Client received %v message: %v", typ, FramesToString(msg))
				return nil
			}
			err = client.Request(context.Background(), client, client.Sockets.Shell, msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			wg.Wait()

			client.Sockets.Shell.Close()
			srvr.Sockets.Shell.Close()
		})

		It("Will halt the retry procedure upon receiving an ACK.", func() {
			err := srvr.Listen(srvr.Sockets.Shell)
			Expect(err).To(BeNil())

			var wg sync.WaitGroup
			wg.Add(3)

			address1 := fmt.Sprintf("%s://%s:%d", transport, ip, shellListenPort)
			err = client.Sockets.Shell.Connect(address1)
			Expect(err).To(BeNil())

			client.Log.Debug("Dialed srvr socket @ %v", address1)

			handleServerMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				srvr.Log.Info("Server received message: %v\n", FramesToString(msg))

				headerMap := make(map[string]string)
				headerMap["msg_id"] = uuid.NewString()
				headerMap["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap["msg_type"] = "ACK"
				header, _ := json.Marshal(&headerMap)

				id_frame := []byte(msg[0])

				// Respond with ACK.
				reply := [][]byte{id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header,
					[]byte("1"),
					[]byte(""),
					[]byte(""),
					[]byte("")}

				srvr.Log.Info("Responding to message with ACK: %v", FramesToString(reply))

				total, err := info.Socket(typ).SendMessage(reply)
				Expect(err).To(BeNil())
				Expect(total > 0).To(BeTrue())

				srvr.Log.Info("Responded to message.")

				wg.Done()

				headerMap2 := make(map[string]string)
				headerMap2["msg_id"] = uuid.NewString()
				headerMap2["date"] = "2018-11-07T00:26:00.073876Z"
				headerMap2["msg_type"] = "kernel_info_reply"
				header2, _ := json.Marshal(&headerMap2)

				// Respond with "actual" message.
				reply2 := [][]byte{id_frame,
					getDestFrame(DEST_KERNEL_ID, "a98c"),
					[]byte("<IDS|MSG>"),
					[]byte(""),
					header2,
					[]byte("2"),
					[]byte(""),
					[]byte(""),
					[]byte("")}

				srvr.Log.Info("Now sending \"actual\" response: %v", FramesToString(reply2))

				total, err = info.Socket(typ).SendMessage(reply2)
				Expect(err).To(BeNil())
				Expect(total > 0).To(BeTrue())

				wg.Done()

				return nil
			}

			go srvr.Serve(srvr, srvr.Sockets.Shell, srvr, handleServerMessage, true)

			headerMap := make(map[string]string)
			headerMap["msg_id"] = uuid.NewString()
			headerMap["date"] = "2018-11-07T00:25:00.073876Z"
			headerMap["msg_type"] = "kernel_info_request"
			header, _ := json.Marshal(&headerMap)

			msg := [][]byte{
				getDestFrame(DEST_KERNEL_ID, "a98c"),
				[]byte("<IDS|MSG>"),
				[]byte(""),
				header,
				[]byte(""),
				[]byte(""),
				[]byte("")}

			clientHandleMessage := func(info types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
				client.Log.Info("Client received %v message: %v", typ, FramesToString(msg))
				wg.Done()
				return nil
			}
			err = client.Request(context.Background(), client, client.Sockets.Shell, msg, client, client, clientHandleMessage, func() {}, func(key string) interface{} { return true }, true)
			Expect(err).To(BeNil())

			wg.Wait()
			Expect(client.NumAcksReceived()).To(Equal(1))

			client.Sockets.Shell.Close()
			srvr.Sockets.Shell.Close()
		})
	})
})
