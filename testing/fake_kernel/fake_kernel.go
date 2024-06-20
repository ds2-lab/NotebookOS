package fake_kernel

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

type SocketWrapper struct {
	zmq4.Socket

	Type types.MessageType
}

type FakeKernel struct {
	ID              string
	ReplicaID       int
	Session         string
	BaseSocketPort  int
	LocalDaemonPort int

	ShellSocket     *SocketWrapper
	IOPubSocket     *SocketWrapper
	StdinSocket     *SocketWrapper
	ControlSocket   *SocketWrapper
	HeartbeatSocket *SocketWrapper

	Serving atomic.Bool

	log logger.Logger
}

func NewFakeKernel(replicaId int, session string, baseSocketPort int, localDaemonPort int) *FakeKernel {
	ctx := context.Background()
	fullID := fmt.Sprintf("%s-%d", session, replicaId)
	kernel := &FakeKernel{
		ID:              fullID,
		ReplicaID:       replicaId,
		Session:         session,
		LocalDaemonPort: localDaemonPort,
		BaseSocketPort:  baseSocketPort,
		HeartbeatSocket: &SocketWrapper{zmq4.NewRep(ctx), types.HBMessage},
		ControlSocket:   &SocketWrapper{zmq4.NewRouter(ctx), types.ControlMessage},
		ShellSocket:     &SocketWrapper{zmq4.NewRouter(ctx), types.ShellMessage},
		StdinSocket:     &SocketWrapper{zmq4.NewRouter(ctx), types.StdinMessage},
		IOPubSocket:     &SocketWrapper{zmq4.NewPub(ctx), types.IOMessage},
	}

	kernel.Serving.Store(true)

	config.InitLogger(&kernel.log, kernel)

	kernel.ControlSocket.Socket.SetOption("ROUTER_MANDATORY", 1)
	kernel.ShellSocket.Socket.SetOption("ROUTER_MANDATORY", 1)

	kernel.log.Debug("Kernel %s is listening and serving HeartbeatSocket at tcp://127.0.0.1:%d", kernel.ID, baseSocketPort)
	err := kernel.HeartbeatSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort))
	if err != nil {
		panic(err)
	}
	go kernel.Serve(kernel.HeartbeatSocket, false, true)

	kernel.log.Debug("Kernel %s is listening and serving ControlSocket at tcp://127.0.0.1:%d", kernel.ID, baseSocketPort+1)
	err = kernel.ControlSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+1))
	if err != nil {
		panic(err)
	}
	go kernel.Serve(kernel.ControlSocket, true, true)

	kernel.log.Debug("Kernel %s is listening and serving ShellSocket at tcp://127.0.0.1:%d", kernel.ID, baseSocketPort+2)
	err = kernel.ShellSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+2))
	if err != nil {
		panic(err)
	}
	go kernel.Serve(kernel.ShellSocket, true, true)

	kernel.log.Debug("Kernel %s is listening and serving StdinSocket at tcp://127.0.0.1:%d", kernel.ID, baseSocketPort+3)
	err = kernel.StdinSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+3))
	if err != nil {
		panic(err)
	}
	go kernel.Serve(kernel.StdinSocket, false, true)

	err = kernel.IOPubSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+4))
	if err != nil {
		panic(err)
	}

	return kernel
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
	k.log.Debug("Kernel %s is serving %v socket now. sendACKs: %v. sendReplies: %v.", k.ID, socket.Type, sendAcks, sendReplies)

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

			jFrames := types.JupyterFrames(msg.Frames[delimIndex:])
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
				k.log.Debug("%s sent 'ACK' (LocalAddr=%v): %v", k.ID, socket.Addr(), ack_message)
			}
		}

		if sendReplies {

			jFrames := types.JupyterFrames(msg.Frames[delimIndex:])
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
				k.log.Debug("%s sent 'kernel_info_reply' (LocalAddr=%v): %v", k.ID, socket.Addr(), response)
			}
		}
	}
}
