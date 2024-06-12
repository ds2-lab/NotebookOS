package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	// _ "net/http"

	_ "net/http/pprof"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/charmbracelet/lipgloss"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	RedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#EE4266"))
	OrangeStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFA113"))
	YellowStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFD23F"))
	GreenStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#2A9D8F"))
	LightBlueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#3185FC"))
	BlueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#0A64E2"))
	PurpleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#8400D6"))
)

type socketWrapper struct {
	zmq4.Socket

	Type types.MessageType
}

type FakeKernel struct {
	ID              string
	ReplicaID       int
	Session         string
	BaseSocketPort  int
	LocalDaemonPort int

	ShellSocket     *socketWrapper
	IOPubSocket     *socketWrapper
	StdinSocket     *socketWrapper
	ControlSocket   *socketWrapper
	HeartbeatSocket *socketWrapper
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
		HeartbeatSocket: &socketWrapper{zmq4.NewRep(ctx), types.HBMessage},
		ControlSocket:   &socketWrapper{zmq4.NewRouter(ctx), types.ControlMessage},
		ShellSocket:     &socketWrapper{zmq4.NewRouter(ctx), types.ShellMessage},
		StdinSocket:     &socketWrapper{zmq4.NewRouter(ctx), types.StdinMessage},
		IOPubSocket:     &socketWrapper{zmq4.NewPub(ctx), types.IOMessage},
	}

	kernel.ControlSocket.Socket.SetOption("ROUTER_MANDATORY", 1)
	kernel.ShellSocket.Socket.SetOption("ROUTER_MANDATORY", 1)

	fmt.Printf("Kernel %s is listening and serving HeartbeatSocket at tcp://127.0.0.1:%d\n", kernel.ID, baseSocketPort)
	err := kernel.HeartbeatSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort))
	if err != nil {
		panic(err)
	}
	go Serve(kernel.HeartbeatSocket, fullID, false, true)

	fmt.Printf("Kernel %s is listening and serving ControlSocket at tcp://127.0.0.1:%d\n", kernel.ID, baseSocketPort+1)
	err = kernel.ControlSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+1))
	if err != nil {
		panic(err)
	}
	go Serve(kernel.ControlSocket, fullID, true, true)

	fmt.Printf("Kernel %s is listening and serving ShellSocket at tcp://127.0.0.1:%d\n", kernel.ID, baseSocketPort+2)
	err = kernel.ShellSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+2))
	if err != nil {
		panic(err)
	}
	go Serve(kernel.ShellSocket, fullID, true, true)

	fmt.Printf("Kernel %s is listening and serving StdinSocket at tcp://127.0.0.1:%d\n", kernel.ID, baseSocketPort+3)
	err = kernel.StdinSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+3))
	if err != nil {
		panic(err)
	}
	go Serve(kernel.StdinSocket, fullID, false, true)

	err = kernel.IOPubSocket.Listen(fmt.Sprintf("tcp://127.0.0.1:%d", baseSocketPort+4))
	if err != nil {
		panic(err)
	}

	return kernel
}

func Serve(socket *socketWrapper, id string, sendAcks bool, sendReplies bool) {
	for {
		msg, err := socket.Recv()
		if err != nil {
			fmt.Printf(RedStyle.Render("[ERROR] Error reading from %v socket: %v\n"), socket.Type, err)
			return
		}

		fmt.Printf("\n[%v] Received message: %v\n", socket.Type, msg)

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
			messageFrames[idx+2] = []byte(fmt.Sprintf("{\"msg_type\": \"ACK\", \"msg_id\": \"%s\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", msg_id, id))
			messageFrames[idx+3] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+4] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+5] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")

			// frames := [][]byte{
			// 	[]byte("<IDS|MSG>"),
			// 	[]byte("dbbdb1eb6f7934ef17e76d92347d57b21623a0775b5d6c4dae9ea972e8ac1e9d"),
			// 	[]byte(fmt.Sprintf("{\"msg_type\": \"ACK\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", id)),
			// 	[]byte("FROM KERNEL"),
			// 	[]byte("FROM KERNEL"),
			// 	[]byte("FROM KERNEL"),
			// }

			// idents = append(idents, frames...)

			ack_message := zmq4.NewMsgFrom(messageFrames...)

			err := socket.Send(ack_message)
			if err != nil {
				fmt.Printf(RedStyle.Render("[ERROR] Failed to send %v ACK because: %v\n"), socket.Type, err)
				return
			} else {
				fmt.Printf("\n%s sent 'ACK' (LocalAddr=%v): %v\n", id, socket.Addr(), ack_message)
			}
		}

		if sendReplies {

			jFrames := types.JupyterFrames(msg.Frames[delimIndex:])
			var header map[string]interface{}
			if err := jFrames.DecodeHeader(&header); err != nil {
				panic(err)
			}

			if msg_type := header["msg_type"].(string); msg_type == "kernel_info_reply" {
				log.Println(RedStyle.Render("Received 'kernel_info_reply' message for some reason..."))
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
			messageFrames[idx+2] = []byte(fmt.Sprintf("{\"msg_type\": \"kernel_info_reply\", \"msg_id\": \"%s\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", msg_id, id))
			messageFrames[idx+3] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+4] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")
			messageFrames[idx+5] = []byte("{\"FROM KERNEL\": \"FROM KERNEL\"}")

			response := zmq4.NewMsgFrom(messageFrames...)

			err = socket.Send(response)
			if err != nil {
				fmt.Printf(RedStyle.Render("[ERROR] Failed to send %v message 'kernel_info_reply' because: %v\n"), socket.Type, err)
				return
			} else {
				fmt.Printf("\n%s sent 'kernel_info_reply' (LocalAddr=%v): %v\n", id, socket.Addr(), response)
			}
		}
	}
}

func RegisterFakeKernel(kernelId string, replicaId int, wg *sync.WaitGroup) {
	registration_payload := make(map[string]interface{})

	var (
		baseSocketPort  int
		localDaemonPort int
	)
	if replicaId == 0 {
		baseSocketPort = 28000
		localDaemonPort = 28075
	} else if replicaId == 1 {
		baseSocketPort = 38000
		localDaemonPort = 38075
	} else {
		baseSocketPort = 48000
		localDaemonPort = 48075
	}

	registration_payload["op"] = "register"
	registration_payload["signature_scheme"] = "hmac-sha256"
	registration_payload["key"] = "149a41b5-0df54cf013c3035a3084a319"
	registration_payload["replicaId"] = replicaId
	registration_payload["numReplicas"] = 3
	registration_payload["join"] = true
	registration_payload["podName"] = fmt.Sprintf("fake-kernel-pod-%d", replicaId)
	registration_payload["nodeName"] = "LocalNode"

	kernel_spec := make(map[string]interface{})
	kernel_spec["id"] = kernelId
	kernel_spec["session"] = kernelId
	kernel_spec["signature_scheme"] = "hmac-sha256"
	kernel_spec["key"] = "149a41b5-0df54cf013c3035a3084a319"

	registration_payload["kernel"] = kernel_spec

	connInfo := &types.ConnectionInfo{
		IP:              "127.0.0.1",
		Transport:       "tcp",
		HBPort:          baseSocketPort,
		ControlPort:     baseSocketPort + 1,
		ShellPort:       baseSocketPort + 2,
		StdinPort:       baseSocketPort + 3,
		IOPubPort:       baseSocketPort + 4,
		IOSubPort:       baseSocketPort + 5,
		SignatureScheme: "hmac-sha256",
		Key:             "149a41b5-0df54cf013c3035a3084a319",
	}

	registration_payload["connection-info"] = connInfo

	payload, err := json.Marshal(&registration_payload)
	if err != nil {
		panic(err)
	}

	kernel := NewFakeKernel(replicaId, kernelId, baseSocketPort, localDaemonPort)
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", kernel.LocalDaemonPort))
	if err != nil {
		panic(err)
	}

	_, err = conn.Write(payload)
	if err != nil {
		panic(err)
	}

	wg.Done()
}

func StartFakeKernel(kernelId string, wg *sync.WaitGroup) *gateway.KernelConnectionInfo {
	conn, err := grpc.Dial("localhost:18080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Printf("Connected to Gateway.\n")

	client := gateway.NewLocalGatewayClient(conn)

	fmt.Printf("Created new ClusterGatewayClient.\n")

	resp, err := client.StartKernel(context.Background(),
		&gateway.KernelSpec{
			Id:              kernelId,
			Session:         kernelId,
			Argv:            make([]string, 0),
			SignatureScheme: "hmac-sha256",
			Key:             "",
			ResourceSpec: &gateway.ResourceSpec{
				Cpu:    1,
				Memory: 1,
				Gpu:    1,
			},
		})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Response: %v\n", resp)

	wg.Done()

	return resp
}

func TestZMQ() {
	kernelId := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	respChannel := make(chan *gateway.KernelConnectionInfo)

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		resp := StartFakeKernel(kernelId, &wg1)

		respChannel <- resp
	}()

	var wg2 sync.WaitGroup
	wg2.Add(3)
	for i := 0; i < 3; i++ {
		go RegisterFakeKernel(kernelId, i, &wg2)
	}

	wg2.Wait()
	wg1.Wait()

	resp := <-respChannel

	shellSocket := zmq4.NewDealer(context.Background())

	shellDialAddr := fmt.Sprintf("tcp://127.0.0.1:%d", resp.ShellPort)
	err := shellSocket.Dial(shellDialAddr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Connected to %s (shell).\n", shellDialAddr)

	ctrlDialAddr := fmt.Sprintf("tcp://127.0.0.1:%d", resp.ControlPort)
	controlSocket := zmq4.NewDealer(context.Background())
	err = controlSocket.Dial(ctrlDialAddr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Connected to %s (control).\n", ctrlDialAddr)

	go Serve(&socketWrapper{shellSocket, types.ShellMessage}, kernelId, false, false)
	go Serve(&socketWrapper{controlSocket, types.ControlMessage}, kernelId, false, false)

	for {
		fmt.Println("\n\n\n\n[1] Control. [2] Shell. [0] Quit.")
		var input string
		fmt.Scanln(&input)

		var socket zmq4.Socket
		var socketType string
		if input == "1" {
			socket = controlSocket
			socketType = "CONTROL"
		} else if input == "2" {
			socket = shellSocket
			socketType = "SHELL"
		} else if input == "0" {
			fmt.Printf("Exiting now.")
			break
		} else {
			fmt.Printf("[ERROR] Invalid selection: \"%s\".\n", input)
			continue
		}

		reqId := uuid.New()
		msgId := uuid.New()

		fmt.Printf("Request ID: \"%s\"\n", reqId)
		fmt.Printf("Message ID: \"%s\"\n", msgId)

		frames := [][]byte{[]byte(kernelId),
			// []byte(fmt.Sprintf("dest.%s.req.%s", kernelId, reqId)),
			[]byte("<IDS|MSG>"),
			[]byte("dbbdb1eb6f7934ef17e76d92347d57b21623a0775b5d6c4dae9ea972e8ac1e9d"),
			[]byte(fmt.Sprintf("{\"msg_id\": \"%s\", \"msg_type\": \"kernel_info_request\", \"username\": \"username\", \"session\": \"%s\", \"date\": \"2024-06-06T14:45:58.228995Z\", \"version\": \"5.3\"}", msgId, kernelId)),
			[]byte("{\"FROM FRONTEND\": \"FROM FRONTEND\"}"),
			[]byte("{\"FROM FRONTEND\": \"FROM FRONTEND\"}"),
			[]byte("{\"FROM FRONTEND\": \"FROM FRONTEND\"}"),
		}

		msg := zmq4.NewMsgFrom(frames...)
		err = socket.Send(msg)
		if err != nil {
			fmt.Printf("[ERROR] Failed to send %s message because: %v\n", socketType, err)
		}

		time.Sleep(time.Millisecond * 1250)
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		log.Println("Serving HTTP.")

		http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%d - Stopping\n", http.StatusOK)))
			wg.Done()
		})

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
		})

		http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
		})

		if err := http.ListenAndServe("localhost:5050", nil); err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()

	TestZMQ()

	wg.Wait()
}
