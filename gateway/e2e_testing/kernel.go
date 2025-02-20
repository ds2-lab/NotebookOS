package e2e_testing

import (
	"fmt"
	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/queue"
	"golang.org/x/net/context"
)

// createKernelSockets creates a control, shell, stdin, heartbeat, and iopub socket.
//
// The sockets bind to (i.e., listen on) startingPort, startingPort + 1, ..., respectively.
func createKernelSockets(connInfo *jupyter.ConnectionInfo,
	kernelId string) (sockets map[messaging.MessageType]*messaging.Socket, messageQueues map[messaging.MessageType]*queue.ThreadsafeFifo[*messaging.JupyterMessage], closeFunc func(), err error) {

	sockets = make(map[messaging.MessageType]*messaging.Socket)
	messageQueues = make(map[messaging.MessageType]*queue.ThreadsafeFifo[*messaging.JupyterMessage])

	closeFunc = func() {
		for _, socket := range sockets {
			_ = socket.Close()
		}
	}

	controlPort := connInfo.ControlPort
	shellPort := connInfo.ShellPort
	stdinPort := connInfo.StdinPort
	hbPort := connInfo.HBPort
	ioPort := connInfo.IOPubPort

	zmqControl := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("control")))
	controlSocket := messaging.NewSocket(zmqControl, controlPort, messaging.ControlMessage, fmt.Sprintf("Kernel%s-Control", kernelId))
	sockets[messaging.ControlMessage] = controlSocket
	fmt.Printf("\nCreated CONTROL socket for kernel \"%s\" with assigned port %d.\n", kernelId, controlSocket.Port)

	zmqShell := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("shell")))
	shellSocket := messaging.NewSocket(zmqShell, shellPort, messaging.ShellMessage, fmt.Sprintf("Kernel%s-Shell", kernelId))
	sockets[messaging.ShellMessage] = shellSocket
	fmt.Printf("Created SHELL socket for kernel \"%s\" with assigned port %d.\n", kernelId, shellSocket.Port)

	zmqStdin := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("stdin")))
	stdinSocket := messaging.NewSocket(zmqStdin, stdinPort, messaging.StdinMessage, fmt.Sprintf("Kernel%s-Stdin", kernelId))
	sockets[messaging.StdinMessage] = stdinSocket
	fmt.Printf("Created STDIN socket for kernel \"%s\" with assigned port %d.\n", kernelId, stdinSocket.Port)

	zmqHeartbeat := zmq4.NewRep(context.Background())
	hbSocket := messaging.NewSocket(zmqHeartbeat, hbPort, messaging.HBMessage, fmt.Sprintf("Kernel%s-HB", kernelId))
	sockets[messaging.HBMessage] = hbSocket
	fmt.Printf("Created HEARTBEAT socket for kernel \"%s\" with assigned port %d.\n", kernelId, hbSocket.Port)

	zmqIoPub := zmq4.NewPub(context.Background())
	ioPubSocket := messaging.NewSocket(zmqIoPub, ioPort, messaging.IOMessage, fmt.Sprintf("Kernel%s-IOPub", kernelId))
	sockets[messaging.IOMessage] = ioPubSocket
	fmt.Printf("Created IOPUB socket for kernel \"%s\" with assigned port %d.\n\n", kernelId, ioPubSocket.Port)

	for _, socket := range sockets {
		err = socket.Listen(fmt.Sprintf("tcp://:%d", socket.Port))
		socketType := socket.Type
		if err != nil {
			closeFunc()
			return nil, nil, nil, err
		}

		fmt.Printf("\nBound %s socket for kernel \"%s\" to port %d.\n", socketType.String(), kernelId, socket.Port)

		messageQueue := queue.NewThreadsafeFifo[*messaging.JupyterMessage](4)
		messageQueues[socketType] = messageQueue

		closeSocket := func(sock *messaging.Socket) {
			_ = sock.Close()
		}

		// We don't want to start serving or call Recv on the IO Pub socket, as they cannot receive messages.
		if socketType == messaging.IOMessage {
			continue
		}

		// Start handler.
		go func(socketTyp messaging.MessageType, messageQueue *queue.ThreadsafeFifo[*messaging.JupyterMessage]) {
			sock := sockets[socketTyp]
			if sock == nil {
				panic(fmt.Sprintf("nil %v socket", socketTyp))
			}

			defer closeSocket(sock)

			numReceived := 0

			fmt.Printf("\nServing %v socket on port %d for kernel \"%s\" now.\n\n",
				sock.Type, sock.Port, kernelId)

			for {
				msg, recvErr := sock.Recv()
				if recvErr != nil {
					fmt.Printf("\n\n\n\n[ERROR] Failed to read/recv from %v socket for kernel \"%s\": %v\n\n\n\n",
						sock.Type, kernelId, recvErr)

					_ = sock.Close()
					return
				}

				numReceived += 1
				jMsg := messaging.NewJupyterMessage(&msg)
				msgType := jMsg.JupyterMessageType()
				msgId := jMsg.JupyterMessageId()

				fmt.Printf("\n\n\n\nReceived %v message #%d with MsgType=\"%s\" and MsgId=\"%s\" for kernel \"%s\":\n%s\n\n\n\n",
					sock.Type, numReceived, msgType, msgId, kernelId, jMsg.StringFormatted())

				messageQueue.Enqueue(jMsg)
			}
		}(socketType, messageQueue)
	}

	return sockets, messageQueues, closeFunc, nil
}
