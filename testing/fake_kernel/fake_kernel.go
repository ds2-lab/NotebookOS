package fake_kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/jupyter/types"
	"github.com/scusemua/distributed-notebook/common/utils"
)

type SocketWrapper struct {
	zmq4.Socket

	Type types.MessageType
}

type FakeKernel struct {
	ID                          string
	ReplicaID                   int
	Session                     string
	BaseSocketPort              int
	LocalDaemonPort             int
	LocalDaemonRegistrationPort int
	Key                         string

	ShellSocket     *SocketWrapper
	IOPubSocket     *SocketWrapper
	StdinSocket     *SocketWrapper
	ControlSocket   *SocketWrapper
	HeartbeatSocket *SocketWrapper

	Serving atomic.Bool

	log logger.Logger
}

func NewFakeKernel(replicaId int, session string, key string, baseSocketPort int, localDaemonPort int) *FakeKernel {
	ctx := context.Background()
	fullID := fmt.Sprintf("FakeKernel-%s-%d", session, replicaId)
	kernel := &FakeKernel{
		ID:                          session,
		ReplicaID:                   replicaId,
		Session:                     session,
		LocalDaemonPort:             localDaemonPort,
		LocalDaemonRegistrationPort: localDaemonPort - 5, /* This is just how I've configured these test ports; the registration port is 5 less than the main gRPC port */
		BaseSocketPort:              baseSocketPort,
		Key:                         key,
		HeartbeatSocket:             &SocketWrapper{zmq4.NewRep(ctx), types.HBMessage},
		ControlSocket:               &SocketWrapper{zmq4.NewRouter(ctx), types.ControlMessage},
		ShellSocket:                 &SocketWrapper{zmq4.NewRouter(ctx), types.ShellMessage},
		StdinSocket:                 &SocketWrapper{zmq4.NewRouter(ctx), types.StdinMessage},
		IOPubSocket:                 &SocketWrapper{zmq4.NewPub(ctx), types.IOMessage},
	}

	config.InitLogger(&kernel.log, fullID+" ")

	kernel.ControlSocket.Socket.SetOption("ROUTER_MANDATORY", 1)
	kernel.ShellSocket.Socket.SetOption("ROUTER_MANDATORY", 1)

	return kernel
}

func (k *FakeKernel) Start() {
	k.Serving.Store(true)

	k.log.Debug("is listening and serving HeartbeatSocket at tcp://127.0.0.1:%d", k.BaseSocketPort)
	err := k.HeartbeatSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", k.BaseSocketPort))
	if err != nil {
		panic(err)
	}
	go k.Serve(k.HeartbeatSocket, false, true)

	k.log.Debug("is listening and serving ControlSocket at tcp://127.0.0.1:%d", k.BaseSocketPort+1)
	err = k.ControlSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", k.BaseSocketPort+1))
	if err != nil {
		panic(err)
	}
	go k.Serve(k.ControlSocket, true, true)

	k.log.Debug("is listening and serving ShellSocket at tcp://127.0.0.1:%d", k.BaseSocketPort+2)
	err = k.ShellSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", k.BaseSocketPort+2))
	if err != nil {
		panic(err)
	}
	go k.Serve(k.ShellSocket, true, true)

	k.log.Debug("is listening and serving StdinSocket at tcp://127.0.0.1:%d", k.BaseSocketPort+3)
	err = k.StdinSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", k.BaseSocketPort+3))
	if err != nil {
		panic(err)
	}
	go k.Serve(k.StdinSocket, false, true)

	err = k.IOPubSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", k.BaseSocketPort+4))
	if err != nil {
		panic(err)
	}
}

func (k *FakeKernel) RegisterWithLocalDaemon() error {
	k.log.Debug("Preparing to register with local daemon (port=%d) now.", k.LocalDaemonRegistrationPort)

	connInfo := &types.ConnectionInfo{
		IP:              "127.0.0.1",
		Transport:       "tcp",
		HBPort:          k.BaseSocketPort,
		ControlPort:     k.BaseSocketPort + 1,
		ShellPort:       k.BaseSocketPort + 2,
		StdinPort:       k.BaseSocketPort + 3,
		IOPubPort:       k.BaseSocketPort + 4,
		IOSubPort:       k.BaseSocketPort + 5,
		SignatureScheme: "hmac-sha256",
		Key:             k.Key,
	}

	registration_payload := make(map[string]interface{})

	registration_payload["op"] = "register"
	registration_payload["signature_scheme"] = "hmac-sha256"
	registration_payload["key"] = "149a41b5-0df54cf013c3035a3084a319"
	registration_payload["replicaId"] = k.ReplicaID
	registration_payload["numReplicas"] = 3
	registration_payload["join"] = true
	registration_payload["podName"] = fmt.Sprintf("fake-kernel-pod-%d", k.ReplicaID)
	registration_payload["nodeName"] = "LocalNode"

	kernel_spec := make(map[string]interface{})
	kernel_spec["id"] = k.ID
	kernel_spec["session"] = k.ID
	kernel_spec["signature_scheme"] = "hmac-sha256"
	kernel_spec["key"] = k.Key

	registration_payload["kernel"] = kernel_spec

	registration_payload["connection-info"] = connInfo

	payload, err := json.Marshal(&registration_payload)
	if err != nil {
		panic(err)
	}

	localDaemonAddr := fmt.Sprintf("localhost:%d", k.LocalDaemonRegistrationPort)
	k.log.Debug("Attempting to connect via TCP to local daemon's registration server at %s", localDaemonAddr)
	conn, err := net.Dial("tcp", localDaemonAddr)
	if err != nil {
		k.log.Error("Failed to dial/connect with local daemon at %s because: %v", localDaemonAddr, err)
		return err
	}

	k.log.Debug("Writing registration payload to local daemon at %s now", localDaemonAddr)
	_, err = conn.Write(payload)
	if err != nil {
		k.log.Error("Failed to write registration payload to local daemon at %s because: %v", localDaemonAddr, err)
		return err
	}

	k.log.Debug("Successfully registered with local daemon at %s", localDaemonAddr)

	return nil
}

func (k *FakeKernel) Close() {
	k.Serving.Store(false)

	k.ShellSocket.Close()
	k.IOPubSocket.Close()
	k.StdinSocket.Close()
	k.HeartbeatSocket.Close()
	k.ControlSocket.Close()
}

func (k *FakeKernel) Serve(socket *SocketWrapper, sendAcks bool, sendReplies bool) {
	k.log.Debug("is serving %v socket now. sendACKs: %v. sendReplies: %v.", socket.Type, sendAcks, sendReplies)

	for k.Serving.Load() {
		msg, err := socket.Recv()
		if err != nil {
			k.log.Debug(utils.RedStyle.Render("[ERROR] Error reading from %v socket: %v"), socket.Type, err)
			return
		}

		k.log.Debug("[%v] Received message: %v", socket.Type, msg)

		var idents [][]byte
		var delimIndex int
		for i, frame := range msg.Frames {
			if string(frame) == "<IDS|MSG>" {
				idents = msg.Frames[0:i]
				delimIndex = i
				break
			}
		}

		// Need to respond with an ACK.
		if sendAcks {
			messageFrames := make([][]byte, len(idents)+6)

			for i, identity_frame := range idents {
				messageFrames[i] = make([]byte, len(identity_frame))
				copy(messageFrames[i], identity_frame)
			}

			jFrames := types.JupyterFrames{
				Frames: msg.Frames,
				Offset: delimIndex,
			}
			var header map[string]interface{}
			if err := jFrames.DecodeHeader(&header); err != nil {
				panic(err)
			}

			msg_id := header["msg_id"].(string)

			idx := len(idents)
			messageFrames[idx] = []byte("<IDS|MSG>")
			messageFrames[idx+1] = []byte("dbbdb1eb6f7934ef17e76d92347d57b21623a0775b5d6c4dae9ea972e8ac1e9d")
			messageFrames[idx+2] = []byte(fmt.Sprintf("{\"msg_type\": \"ACK\", \"msg_id\": \"%s\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", msg_id, k.ID))
			messageFrames[idx+3] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+4] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+5] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")

			ack_message := zmq4.NewMsgFrom(messageFrames...)

			err := socket.Send(ack_message)
			if err != nil {
				k.log.Error(utils.RedStyle.Render("[ERROR] Failed to send %v ACK because: %v"), socket.Type, err)
				return
			} else {
				k.log.Debug("sent 'ACK' (LocalAddr=%v): %v", socket.Addr(), ack_message)
			}
		}

		if sendReplies {
			jFrames := types.JupyterFrames{
				Frames: msg.Frames,
				Offset: delimIndex,
			}
			var header map[string]interface{}
			if err := jFrames.DecodeHeader(&header); err != nil {
				panic(err)
			}

			if msg_type := header["msg_type"].(string); msg_type == "kernel_info_reply" {
				k.log.Error(utils.RedStyle.Render("Received 'kernel_info_reply' message for some reason..."))
				continue // Don't send anything else.
			}

			messageFrames := make([][]byte, len(idents)+6)

			for i, identity_frame := range idents {
				messageFrames[i] = make([]byte, len(identity_frame))
				copy(messageFrames[i], identity_frame)
			}

			msg_id := header["msg_id"].(string)

			idx := len(idents)
			messageFrames[idx] = []byte("<IDS|MSG>")
			messageFrames[idx+1] = []byte("dbbdb1eb6f7934ef17e76d92347d57b21623a0775b5d6c4dae9ea972e8ac1e9d")
			messageFrames[idx+2] = []byte(fmt.Sprintf("{\"msg_type\": \"kernel_info_reply\", \"msg_id\": \"%s\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", msg_id, k.ID))
			messageFrames[idx+3] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+4] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+5] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")

			response := zmq4.NewMsgFrom(messageFrames...)

			err = socket.Send(response)
			if err != nil {
				k.log.Error(utils.RedStyle.Render("[ERROR] Failed to send %v message 'kernel_info_reply' because: %v"), socket.Type, err)
				return
			} else {
				k.log.Debug("sent 'kernel_info_reply' (LocalAddr=%v): %v", socket.Addr(), response)
			}
		}
	}
}
