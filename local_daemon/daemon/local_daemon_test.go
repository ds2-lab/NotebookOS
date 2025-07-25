package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/configuration"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/test_utils"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/local_daemon/device"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
	"github.com/scusemua/distributed-notebook/local_daemon/invoker"
	"github.com/scusemua/distributed-notebook/local_daemon/mock_device"
	"github.com/shopspring/decimal"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

const (
	signatureScheme               string = "hmac-sha256"
	dockerInvokerKernelConnInfoIp        = "127.0.0.1"

	prewarmConnFilePrefix   = "connection-prewarm"
	prewarmKernelFilePrefix = "config-prewarm"

	standardConnFilePrefix   = "connection-kernel"
	standardKernelFilePrefix = "config-kernel"
)

var (
	kernelArgv = []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"}
)

// createKernelSockets creates a control, shell, stdin, heartbeat, and iopub socket.
//
// The sockets bind to (i.e., listen on) startingPort, startingPort + 1, ..., respectively.
func createKernelSockets(startingPort int, kernelId string) (sockets map[messaging.MessageType]*messaging.Socket, messageQueues map[messaging.MessageType]*queue.ThreadsafeFifo[*messaging.JupyterMessage], closeFunc func(), err error) {
	sockets = make(map[messaging.MessageType]*messaging.Socket)
	messageQueues = make(map[messaging.MessageType]*queue.ThreadsafeFifo[*messaging.JupyterMessage])

	closeFunc = func() {
		for _, socket := range sockets {
			_ = socket.Close()
		}
	}

	controlPort := startingPort
	shellPort := startingPort + 1
	stdinPort := startingPort + 2
	hbPort := startingPort + 3
	ioPort := startingPort + 4

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

func processExecuteRequestWithUpdatedResourceSpec(schedulerDaemon *LocalScheduler, messageType messaging.JupyterMessageType, kernelReplica scheduling.KernelReplica, updatedResourceSpec *types.Float64Spec) *messaging.JupyterMessage {
	metadata := map[string]interface{}{
		"resource_request": updatedResourceSpec,
	}

	jMsg := test_utils.CreateJupyterMessageWithMetadata(messageType, kernelReplica.ID(), kernelReplica.ConnectionInfo().Key, metadata)
	processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernelReplica) // , header, offset)

	return processedMessage
}

func processExecuteRequest(schedulerDaemon *LocalScheduler, messageType messaging.JupyterMessageType, kernelReplica scheduling.KernelReplica) *messaging.JupyterMessage {
	jMsg := test_utils.CreateJupyterMessage(messageType, kernelReplica.ID(), kernelReplica.ConnectionInfo().Key)
	processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernelReplica) // , header, offset)

	return processedMessage
}

func createKernelReplica(mockController *gomock.Controller, kernelId string, kernelKey string, workloadId string, replicaId int32, kernelSpec *proto.KernelSpec, resourceSpec types.Spec) *mock_scheduling.MockKernelReplica {
	kernelReplica := mock_scheduling.NewMockKernelReplica(mockController)

	decimalSpec := types.ToDecimalSpec(resourceSpec)

	kernelReplica.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
	kernelReplica.EXPECT().KernelSpec().Return(kernelSpec).AnyTimes()
	kernelReplica.EXPECT().ResourceSpec().Return(decimalSpec).AnyTimes()
	kernelReplica.EXPECT().ReplicaID().Return(replicaId).AnyTimes()
	kernelReplica.EXPECT().ID().Return(kernelId).AnyTimes()
	kernelReplica.EXPECT().WaitForPendingExecuteRequests().AnyTimes()
	kernelReplica.EXPECT().WorkloadId().AnyTimes().Return(workloadId)
	kernelReplica.EXPECT().String().AnyTimes().DoAndReturn(func() string {
		return fmt.Sprintf("replica(%s:%d)", kernelId, replicaId)
	})

	return kernelReplica
}

var _ = Describe("Local Daemon Tests", func() {
	Context("Newer Unit Tests", func() {
		var (
			localScheduler             *LocalScheduler
			hostSpec                   *types.DecimalSpec
			mockCtrl                   *gomock.Controller
			mockedClusterGatewayClient *mock_proto.MockClusterGatewayClient
			workloadId                 string
		)

		callRegisterKernel := func(kernelId, kernelKey string, resourceSpec *proto.ResourceSpec, prewarmContainer bool, replicaId, numReplicas int32) *jupyter.ConnectionInfo {
			var kernelInvoker invoker.KernelInvoker
			for kernelInvoker == nil {
				time.Sleep(time.Millisecond * 250)
				kernelInvoker, _ = localScheduler.getInvokerByKernelId(kernelId)
			}

			kernelInvoker.(invoker.ContainerInvoker).WaitForContainerToBeCreated()

			var kernelFile, connFile, kernelFilePrefix, connFilePrefix string

			if prewarmContainer {
				kernelFilePrefix = prewarmKernelFilePrefix
				connFilePrefix = prewarmConnFilePrefix
			} else {
				kernelFilePrefix = standardKernelFilePrefix
				connFilePrefix = standardConnFilePrefix
			}

			items, _ := os.ReadDir("/tmp")
			for _, item := range items {
				if item.IsDir() {
					continue
				}

				info, err := item.Info()
				Expect(err).To(BeNil())

				fileName := info.Name()

				if kernelFile == "" && strings.Contains(fileName, kernelId) && strings.Contains(fileName, kernelFilePrefix) {
					kernelFile = path.Join("/tmp", info.Name())
					continue
				}

				if connFile == "" && strings.Contains(fileName, kernelId) && strings.Contains(fileName, connFilePrefix) {
					connFile = path.Join("/tmp", info.Name())
					continue
				}
			}

			Expect(kernelFile).ToNot(Equal(""))
			Expect(connFile).ToNot(Equal(""))

			// RemoveHost the kernel file. We don't need it, so let's just clean it up now.
			Expect(os.Remove(kernelFile)).To(BeNil())

			var connInfo *jupyter.ConnectionInfo

			file, err := os.Open(connFile)
			Expect(err).To(BeNil())

			decoder := json.NewDecoder(file)
			Expect(decoder).ToNot(BeNil())

			Expect(decoder.Decode(&connInfo)).To(BeNil())
			_ = file.Close()
			Expect(connInfo).ToNot(BeNil())

			Expect(connInfo.Key).To(Equal(kernelKey))
			Expect(connInfo.SignatureScheme).To(Equal(messaging.JupyterSignatureScheme))

			// RemoveHost the connection file. We don't need it anymore, so let's just clean it up now.
			Expect(os.Remove(connFile)).To(BeNil())

			regPayload := &KernelRegistrationPayload{
				Kernel: &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					SignatureScheme: connInfo.SignatureScheme,
					Key:             connInfo.Key,
				},
				ConnectionInfo:     connInfo,
				PersistentId:       nil,
				NodeName:           localScheduler.nodeName,
				Key:                kernelKey,
				PodOrContainerName: kernelId,
				Op:                 "register",
				SignatureScheme:    messaging.JupyterSignatureScheme,
				WorkloadId:         workloadId,
				ReplicaId:          replicaId,
				NumReplicas:        numReplicas,
				Cpu:                resourceSpec.Cpu,
				Memory:             int32(resourceSpec.Memory),
				Gpu:                resourceSpec.Gpu,
				Join:               true,
				PrewarmContainer:   prewarmContainer,
			}

			var containerType scheduling.ContainerType
			if prewarmContainer {
				containerType = scheduling.PrewarmContainer
			} else {
				containerType = scheduling.StandardContainer
			}

			respPayload := localScheduler.registerKernelReplica(regPayload, "127.0.0.1", containerType)
			Expect(respPayload).ToNot(BeNil())

			return connInfo
		}

		It("Will work with the sockets", func() {
			sockets, messageQueues, closeFunc, err := createKernelSockets(17000, "TestId")
			Expect(err).To(BeNil())
			Expect(sockets).ToNot(BeNil())
			defer closeFunc()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sendMessage := func(msgType messaging.MessageType) {
				dealer := messaging.NewSocket(zmq4.NewDealer(ctx), 0, msgType, "Dealer")
				err = dealer.Listen(fmt.Sprintf("tcp://:%d", dealer.Port))
				Expect(err).To(BeNil())

				dealer.Port = dealer.Addr().(*net.TCPAddr).Port
				Expect(dealer.Port > 0).To(BeTrue())

				// Connect to the server
				serverAddress := fmt.Sprintf("tcp://localhost:%d", sockets[msgType].Port)
				err = dealer.Dial(serverAddress)
				Expect(err).To(BeNil())

				messageHeader := &messaging.MessageHeader{
					MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
					Username: "TestIdUser",
					Session:  "TestIdSession",
					Date:     "2024-04-03T22:55:52.605Z",
					MsgType:  "sample_request",
					Version:  "5.2",
				}

				unsignedFrames := [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1*/
					[]byte(fmt.Sprintf("{\"socket_type\": \"%s\"}", msgType.String())), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err = jFrames.EncodeHeader(messageHeader)
				Expect(err).To(BeNil())

				msg := &zmq4.Msg{
					Frames: jFrames.Frames,
					Type:   zmq4.UsrMsg,
				}

				// Send a message to the server
				err = dealer.Send(*msg)
				Expect(err).To(BeNil())

				fmt.Printf("Client sent %v message:\n%s\n", msgType, jFrames.StringFormatted())

				Eventually(func() *messaging.JupyterMessage {
					msgQueue := messageQueues[messaging.ControlMessage]
					msg, ok := msgQueue.Peek()
					if ok {
						return msg
					}

					return nil
				}, time.Millisecond*500, time.Millisecond*50).ShouldNot(BeNil())
			}

			sendMessage(messaging.ControlMessage)
			sendMessage(messaging.ShellMessage)
			sendMessage(messaging.StdinMessage)
		})

		BeforeEach(func() {
			mockCtrl = gomock.NewController(GinkgoT())

			mockedClusterGatewayClient = mock_proto.NewMockClusterGatewayClient(mockCtrl)
		})

		AfterEach(func() {
			if mockCtrl != nil {
				mockCtrl.Finish()
			}
		})

		Context("Middle-Ground Scheduling", func() {
			// middleGroundOpts := `{"Debug":true,"Verbose":true,"ProvisionerAddr":"gateway:8081","JaegerAddr":"","ConsulAddr":"","NodeName":"0","s3_bucket":"distributed-notebook-storage","aws_region":"us-east-1","redis_password":"","DevicePluginPath":"/var/lib/kubelet/device-plugins/","NumVirtualGPUs":72,"ip":"","transport":"tcp","signature_scheme":"","key":"","control_port":12000,"shell_port":12001,"stdin_port":12002,"hb_port":12003,"iopub_port":12004,"iosub_port":12005,"ack_port":12006,"starting_resource_port":12007,"num_resource_ports":4096,"docker-storage-base":"","cluster_scheduler_options":{"common_options":{"deployment_mode":"docker-compose","docker_app_name":"distributed_notebook","docker_network_name":"","scheduling-policy":"middle-ground","idle-session-reclamation-policy":"none","remote-storage-endpoint":"redis:6379","remote-storage":"redis","gpus-per-host":8,"prometheus_interval":15,"prometheus_port":-1,"num_resend_attempts":1,"smr-port":12080,"debug_port":12996,"election_timeout_seconds":3,"local_mode":false,"use_real_gpus":false,"acks_enabled":false,"debug_mode":true,"simulate_checkpointing_latency":true,"disable_prometheus_metrics_publishing":false,"simulate_training_using_sleep":false,"bind_debugpy_port":false,"save_stopped_kernel_containers":true,"pretty_print_options":false},"custom_idle_session_reclamation_options":{"idle_session_replay_all_cells":false,"idle_session_timeout_interval_sec":0},"subscribed-ratio-update-interval":1,"scaling-factor":1.1,"scaling-interval":15,"scaling-limit":1.15,"scaling-in-limit":2,"scaling-buffer-size":3,"min_cluster_nodes":6,"max_cluster_nodes":48,"initial-cluster-size":0,"gpu_poll_interval":0,"max-subscribed-ratio":7,"execution-time-sampling-window":0,"migration-time-sampling-window":0,"scheduler-http-port":8078,"mean_scale_out_per_host_sec":0,"std_dev_scale_out_per_host_sec":0,"mean_scale_in_per_host_sec":0,"std_dev_scale_in_per_host_sec":0,"millicpus_per_host":64000,"memory_mb_per_host":128000,"vram_gb_per_host":40,"prewarming_enabled":true,"min_prewarm_containers_per_host":1,"max_prewarm_containers_per_host":3,"initial_num_containers_per_host":0,"prewarm_run_interval_sec":0,"prewarming_policy":"maintain_minimum_capacity","initial-connection-period":0,"predictive_autoscaling":true,"assign_kernel_debug_ports":false},"DirectServer":false,"run_kernels_in_gdb":false,"JupyterGrpcPort":12080,"kernel-registry-port":12075,"redis_port":6379,"redis_database":0}`
			middleGroundOpts := `{"aws_region":"us-east-1","connection_info":{"ip":"","transport":"tcp","signature_scheme":"","key":"","control_port":19001,"shell_port":19002,"stdin_port":19003,"hb_port":19000,"iopub_port":19004,"iosub_port":19005,"ack_port":19006,"starting_resource_port":29007,"num_resource_ports":4096},"consul":"","jaeger":"","kernel-registry-port":18075,"node_name":"0","port":18082,"provisioner":"127.0.0.1:10000","redis_database":0,"redis_password":"","redis_port":6379,"s3_bucket":"distributed-notebook-storage","scheduler_daemon_options":{"docker-storage-base":"/remote_storage","direct":false,"run_kernels_in_gdb":false,"cluster_scheduler_options":{"common_options":{"deployment_mode":"docker-compose","docker_app_name":"distributed_notebook","docker_network_name":"","scheduling-policy":"middle-ground","idle-session-reclamation-policy":"none","remote-storage-endpoint":"redis:6379","remote-storage":"redis","gpus-per-host":8,"prometheus_interval":15,"prometheus_port":-1,"num_resend_attempts":1,"smr-port":17080,"debug_port":19997,"election_timeout_seconds":3,"local_mode":true,"use_real_gpus":false,"acks_enabled":false,"debug_mode":true,"simulate_checkpointing_latency":true,"disable_prometheus_metrics_publishing":false,"simulate_training_using_sleep":false,"bind_debugpy_port":false,"save_stopped_kernel_containers":false,"pretty_print_options":false},"custom_idle_session_reclamation_options":{"idle_session_replay_all_cells":false,"idle_session_timeout_interval_sec":0},"subscribed-ratio-update-interval":1,"scaling-factor":1.1,"scaling-interval":15,"scaling-limit":1.15,"scaling-in-limit":2,"scaling-buffer-size":3,"min_cluster_nodes":6,"max_cluster_nodes":48,"gpu_poll_interval":0,"max-subscribed-ratio":7,"execution-time-sampling-window":0,"migration-time-sampling-window":0,"scheduler-http-port":8078,"mean_scale_out_per_host_sec":0,"std_dev_scale_out_per_host_sec":0,"mean_scale_in_per_host_sec":0,"std_dev_scale_in_per_host_sec":0,"millicpus_per_host":64000,"memory_mb_per_host":128000,"vram_gb_per_host":40,"predictive_autoscaling":true,"assign_kernel_debug_ports":false}},"virtual_gpu_plugin_server_options":{"device-plugin-path":"/var/lib/kubelet/device-plugins/","num-virtual-gpus-per-node":72}}`

			var (
				policy   scheduling.Policy
				options  *domain.LocalDaemonOptions
				nodeName = "TestLocalDaemon1"
				nodeId   = "ABC123-TestLocalDaemon1"
			)

			BeforeEach(func() {
				err := json.Unmarshal([]byte(middleGroundOpts), &options)
				Expect(err).To(BeNil())

				devicePluginServer := device.NewVirtualGpuPluginServer(
					&options.VirtualGpuPluginServerOptions, nodeName, true)

				localScheduler = New(&options.ConnectionInfo, options, options.KernelRegistryPort, options.Port,
					devicePluginServer, nodeName, "TestDockerContainer")
				localScheduler.provisioner = mockedClusterGatewayClient

				resp, err := localScheduler.SetID(context.Background(), &proto.HostId{
					Id:       nodeId,
					NodeName: nodeName,
				})
				Expect(err).To(BeNil())
				Expect(resp).ToNot(BeNil())

				policy = localScheduler.schedulingPolicy
				Expect(policy).ToNot(BeNil())
				Expect(policy.PolicyKey()).To(Equal(scheduling.MiddleGround))

				err = os.Setenv(invoker.DisableActualContainerCreationEnv, "1")
				Expect(err).To(BeNil())
				Expect(os.Getenv(invoker.DisableActualContainerCreationEnv)).To(Equal("1"))

				err = os.Setenv(invoker.DockerInvokerKernelConnInfoIp, dockerInvokerKernelConnInfoIp)
				Expect(err).To(BeNil())
				Expect(os.Getenv(invoker.DockerInvokerKernelConnInfoIp)).To(Equal(dockerInvokerKernelConnInfoIp))

				hostSpec = localScheduler.allocationManager.SpecResources()
			})

			AfterEach(func() {
				if localScheduler != nil {
					go localScheduler.cleanUpAfterClosed()
					_ = localScheduler.Close()
				}
			})

			It("Will handle creating a pre-warm container", func() {
				Expect(localScheduler).ToNot(BeNil())

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				workloadId = uuid.NewString()
				dataDirectory := uuid.NewString()

				sockets, _, closeFunc, err := createKernelSockets(options.ConnectionInfo.ControlPort, kernelId)
				Expect(err).To(BeNil())
				Expect(sockets).ToNot(BeNil())
				Expect(len(sockets) == 5).To(BeTrue())
				defer closeFunc()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()

				resourceSpec := proto.NewResourceSpec(128, 256, 2, 4)

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            kernelArgv,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				kernelReplicaSpec := &proto.KernelReplicaSpec{
					Kernel:                    kernelSpec,
					ReplicaId:                 1,
					Join:                      true,
					NumReplicas:               3,
					DockerModeKernelDebugPort: -1,
					PersistentId:              &dataDirectory,
					WorkloadId:                workloadId,
					Replicas:                  []string{},
					PrewarmContainer:          true,
				}

				resultChan := make(chan *proto.KernelConnectionInfo, 1)

				go func() {
					resp, err := localScheduler.StartKernelReplica(ctx, kernelReplicaSpec)
					Expect(err).To(BeNil())
					Expect(resp).ToNot(BeNil())

					resultChan <- resp
				}()

				go func() {
					callRegisterKernel(kernelId, kernelKey, resourceSpec, true, 0, 3)
				}()

				var resp *proto.KernelConnectionInfo
				Eventually(resultChan, ctx).Should(Receive(&resp))

				Expect(resp).ToNot(BeNil())

				Expect(localScheduler.NumKernels()).To(Equal(0))
				Expect(localScheduler.NumPrewarmContainers()).To(Equal(1))
			})

			It("Will handle creating a standard kernel replica", func() {
				Expect(localScheduler).ToNot(BeNil())

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				workloadId = uuid.NewString()
				dataDirectory := uuid.NewString()

				sockets, _, closeFunc, err := createKernelSockets(options.ConnectionInfo.ControlPort, kernelId)
				Expect(err).To(BeNil())
				Expect(sockets).ToNot(BeNil())
				Expect(len(sockets) == 5).To(BeTrue())
				defer closeFunc()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()

				resourceSpec := proto.NewResourceSpec(128, 256, 2, 4)

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            kernelArgv,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				kernelReplicaSpec := &proto.KernelReplicaSpec{
					Kernel:                    kernelSpec,
					ReplicaId:                 1,
					Join:                      true,
					NumReplicas:               3,
					DockerModeKernelDebugPort: -1,
					PersistentId:              &dataDirectory,
					WorkloadId:                workloadId,
					Replicas:                  []string{},
					PrewarmContainer:          false,
				}

				resultChan := make(chan *proto.KernelConnectionInfo, 1)

				go func() {
					resp, err := localScheduler.StartKernelReplica(ctx, kernelReplicaSpec)
					Expect(err).To(BeNil())
					Expect(resp).ToNot(BeNil())

					resultChan <- resp
				}()

				go func() {
					callRegisterKernel(kernelId, kernelKey, resourceSpec, false, 1, 3)
				}()

				var resp *proto.KernelConnectionInfo
				Eventually(resultChan, ctx).Should(Receive(&resp))

				Expect(resp).ToNot(BeNil())

				Expect(localScheduler.NumKernels()).To(Equal(1))
				Expect(localScheduler.NumPrewarmContainers()).To(Equal(0))
			})

			It("Will handle promoting a pre-warmed container to a standard container", func() {
				By("First creating the pre-warm container")

				Expect(localScheduler).ToNot(BeNil())

				allocationManager := localScheduler.allocationManager
				Expect(allocationManager).ToNot(BeNil())

				prewarmKernelId := uuid.NewString()
				prewarmKernelKey := uuid.NewString()
				workloadId = uuid.NewString()

				sockets, messageQueues, closeFunc, err := createKernelSockets(options.ConnectionInfo.ControlPort, prewarmKernelId)
				Expect(err).To(BeNil())
				Expect(sockets).ToNot(BeNil())
				Expect(messageQueues).ToNot(BeNil())
				Expect(len(sockets) == 5).To(BeTrue())
				Expect(len(messageQueues) == 5).To(BeTrue())
				defer closeFunc()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()

				resourceSpec := proto.NewResourceSpec(128, 256, 2, 4)

				prewarmKernelSpec := &proto.KernelSpec{
					Id:              prewarmKernelId,
					Session:         prewarmKernelId,
					Argv:            kernelArgv,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             prewarmKernelKey,
					ResourceSpec:    resourceSpec,
				}

				prewarmKernelReplicaSpec := &proto.KernelReplicaSpec{
					Kernel:                    prewarmKernelSpec,
					ReplicaId:                 1,
					Join:                      true,
					NumReplicas:               3,
					DockerModeKernelDebugPort: -1,
					PersistentId:              nil,
					WorkloadId:                workloadId,
					Replicas:                  []string{},
					PrewarmContainer:          true,
				}

				resultChan := make(chan *proto.KernelConnectionInfo, 1)

				go func() {
					resp, err := localScheduler.StartKernelReplica(ctx, prewarmKernelReplicaSpec)
					Expect(err).To(BeNil())
					Expect(resp).ToNot(BeNil())

					resultChan <- resp
				}()

				connInfoChan := make(chan *jupyter.ConnectionInfo, 1)

				go func() {
					connInfo := callRegisterKernel(prewarmKernelId, prewarmKernelKey, resourceSpec, true, 0, 3)
					Expect(connInfo).ToNot(BeNil())

					connInfoChan <- connInfo
				}()

				var resp *proto.KernelConnectionInfo
				Eventually(resultChan, ctx).Should(Receive(&resp))

				Expect(resp).ToNot(BeNil())

				Expect(localScheduler.NumKernels()).To(Equal(0))
				Expect(localScheduler.NumPrewarmContainers()).To(Equal(1))

				By("Correctly handling the promotion of the prewarm container to a standard container")

				promotionCtx, promotionCancel := context.WithTimeout(context.Background(), time.Second*5)
				defer promotionCancel()

				promotionRespChan := make(chan *proto.KernelConnectionInfo, 1)

				kernelId := uuid.NewString()
				kernelKey := uuid.NewString()
				workloadId = uuid.NewString()
				dataDirectory := uuid.NewString()

				GinkgoWriter.Printf("allocationManager.IdleResources: %s\n",
					allocationManager.IdleResources().String())
				GinkgoWriter.Printf("allocationManager.PendingResources: %s\n",
					allocationManager.PendingResources().String())
				GinkgoWriter.Printf("allocationManager.CommittedResources: %s\n",
					allocationManager.CommittedResources().String())
				GinkgoWriter.Printf("hostSpec: %s\n", hostSpec.String())

				Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
				Expect(allocationManager.NumAllocations()).To(Equal(0))
				Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

				Expect(allocationManager.KernelHasCommittedResources(kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasCommittedResources(1, kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasPendingGPUs(1, kernelId)).To(BeFalse())
				Expect(allocationManager.PendingResources().Equals(hostSpec))
				Expect(allocationManager.CommittedResources().IsZero()).To(BeTrue())

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            kernelArgv,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				kernelReplicaSpec := &proto.KernelReplicaSpec{
					Kernel:                    kernelSpec,
					ReplicaId:                 1,
					Join:                      true,
					NumReplicas:               3,
					DockerModeKernelDebugPort: -1,
					PersistentId:              &dataDirectory,
					WorkloadId:                workloadId,
					Replicas:                  []string{},
					PrewarmContainer:          false,
				}

				time.Sleep(time.Millisecond * 50)

				go func() {
					prewarmedKernelReplicaSpec := &proto.PrewarmedKernelReplicaSpec{
						KernelReplicaSpec:    kernelReplicaSpec,
						PrewarmedContainerId: prewarmKernelId,
					}

					resp, err := localScheduler.PromotePrewarmedContainer(promotionCtx, prewarmedKernelReplicaSpec)
					Expect(err).To(BeNil())
					Expect(resp).ToNot(BeNil())

					promotionRespChan <- resp
				}()

				Expect(localScheduler.provisioner).To(Equal(mockedClusterGatewayClient))

				mockedClusterGatewayClient.
					EXPECT().
					PingGateway(gomock.Any(), gomock.Any(), gomock.Any()).
					Times(1).
					Return(nil, nil)

				mockedClusterGatewayClient.
					EXPECT().
					NotifyKernelRegistered(gomock.Any(), gomock.Any()).
					Times(1).
					DoAndReturn(
						func(ctx context.Context, in *proto.KernelRegistrationNotification, opts ...*grpc.CallOption) (*proto.KernelRegistrationNotificationResponse, error) {
							resp := &proto.KernelRegistrationNotificationResponse{
								Id:                              in.ReplicaId,
								Replicas:                        map[int32]string{1: "127.0.0.1:12345", 2: "127.0.0.1:12343", 3: "127.0.0.1:12323"},
								PersistentId:                    &dataDirectory,
								ResourceSpec:                    kernelSpec.ResourceSpec,
								SmrPort:                         int32(localScheduler.smrPort), // The kernel should already have this info, but we'll send it anyway.
								ShouldReadDataFromRemoteStorage: false,
							}

							return resp, nil
						})

				Eventually(func() *messaging.JupyterMessage {
					controlQueue := messageQueues[messaging.ControlMessage]
					Expect(controlQueue).ToNot(BeNil())

					val, ok := controlQueue.Peek()
					if ok {
						return val
					}

					return nil
				}, time.Second*4, time.Millisecond*250).ShouldNot(BeNil())

				go func() {
					defer GinkgoRecover()

					time.Sleep(time.Millisecond * 100)

					fmt.Printf("\n\nRegistering promoted PreWarm container \"%s\" now...\n\n", kernelId)

					var connInfo *jupyter.ConnectionInfo
					Eventually(connInfoChan, time.Millisecond*500, time.Millisecond*50).Should(Receive(&connInfo))

					regPayload := &KernelRegistrationPayload{
						Kernel: &proto.KernelSpec{
							Id:              kernelId,
							Session:         kernelId,
							SignatureScheme: messaging.JupyterSignatureScheme,
							Key:             kernelKey,
						},
						ConnectionInfo:     connInfo,
						PersistentId:       nil,
						NodeName:           localScheduler.nodeName,
						Key:                kernelKey,
						PodOrContainerName: kernelId,
						Op:                 "register",
						SignatureScheme:    messaging.JupyterSignatureScheme,
						WorkloadId:         workloadId,
						ReplicaId:          1,
						NumReplicas:        3,
						Cpu:                resourceSpec.Cpu,
						Memory:             int32(resourceSpec.Memory),
						Gpu:                resourceSpec.Gpu,
						Join:               true,
						PrewarmContainer:   false,
					}

					respPayload := localScheduler.registerKernelReplica(regPayload, "127.0.0.1", scheduling.StandardContainer)
					Expect(respPayload).ToNot(BeNil())
				}()

				messageHeader := &messaging.MessageHeader{
					MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
					Username: kernelId,
					Session:  kernelId,
					Date:     "2024-04-03T22:55:52.605Z",
					MsgType:  "promote_prewarm_reply",
					Version:  "5.2",
				}

				unsignedFrames := [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1*/
					[]byte(""), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err = jFrames.EncodeHeader(messageHeader)
				Expect(err).To(BeNil())

				controlQueue := messageQueues[messaging.ControlMessage]
				Expect(controlQueue).ToNot(BeNil())

				promoteKernelRequestMsg, ok := controlQueue.Dequeue()
				Expect(ok).To(BeTrue())
				Expect(promoteKernelRequestMsg).ToNot(BeNil())

				msg := zmq4.Msg{
					Frames: jFrames.Frames,
					Type:   zmq4.UsrMsg,
				}

				var identityFrames [][]byte
				for idx, frame := range promoteKernelRequestMsg.JupyterFrames.Frames {
					if bytes.Equal(frame, messaging.JupyterFrameIDSMSG) {
						identityFrames = promoteKernelRequestMsg.JupyterFrames.Frames[0:idx]
						break
					}
				}

				Expect(identityFrames).ToNot(BeNil())
				Expect(len(identityFrames)).To(Equal(2)) // the zmq identity and the dest.req thing.

				jFrames.Frames = append(identityFrames, jFrames.Frames...)

				msg = zmq4.Msg{
					Frames: jFrames.Frames,
					Type:   zmq4.UsrMsg,
				}

				controlSocket := sockets[messaging.ControlMessage]
				Expect(controlSocket).ToNot(BeNil())

				err = controlSocket.Send(msg)
				Expect(err).To(BeNil())

				var promotionResp *proto.KernelConnectionInfo
				Eventually(promotionRespChan, time.Second*3, time.Millisecond*250).Should(Receive(&promotionResp))

				resourceSpecDecimal := resourceSpec.ToDecimalSpec()

				GinkgoWriter.Printf("allocationManager.IdleResources: %s\n",
					allocationManager.IdleResources().String())
				GinkgoWriter.Printf("hostSpec: %s\n",
					hostSpec.String())
				GinkgoWriter.Printf("resourceSpecDecimal: %s\n",
					resourceSpecDecimal.String())
				GinkgoWriter.Printf("hostSpec.Subtract(resourceSpecDecimal): %s\n",
					hostSpec.Subtract(resourceSpecDecimal).String())

				Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
				Expect(allocationManager.NumAllocations()).To(Equal(1))
				Expect(allocationManager.NumCommittedAllocations()).To(Equal(1))

				Expect(allocationManager.PendingResources().Equals(resourceSpecDecimal))
				Expect(allocationManager.IdleResources().Equals(hostSpec.Subtract(resourceSpecDecimal))).To(BeTrue())
				Expect(allocationManager.IdleCPUs().Equals(hostSpec.Millicpus.Sub(resourceSpecDecimal.Millicpus))).To(BeTrue())
				Expect(allocationManager.IdleMemoryMB().Equals(hostSpec.MemoryMb.Sub(resourceSpecDecimal.MemoryMb))).To(BeTrue())
				Expect(allocationManager.IdleGPUs().Equals(hostSpec.GPUs.Sub(resourceSpecDecimal.GPUs))).To(BeTrue())
				Expect(allocationManager.IdleVRamGB().Equals(hostSpec.VRam.Sub(resourceSpecDecimal.VRam))).To(BeTrue())
				Expect(allocationManager.CommittedResources().IsZero()).To(BeFalse())
				Expect(allocationManager.CommittedCPUs().Equals(resourceSpecDecimal.Millicpus)).To(BeTrue())
				Expect(allocationManager.CommittedMemoryMB().Equals(resourceSpecDecimal.MemoryMb)).To(BeTrue())
				Expect(allocationManager.CommittedGPUs().Equals(resourceSpecDecimal.GPUs)).To(BeTrue())
				Expect(allocationManager.CommittedVRamGB().Equals(resourceSpecDecimal.VRam)).To(BeTrue())

				gpuDeviceIds, err := allocationManager.GetGpuDeviceIdsAssignedToReplica(1, kernelId)
				Expect(err).To(BeNil())
				Expect(gpuDeviceIds).ToNot(BeNil())
				Expect(len(gpuDeviceIds) == 2).To(BeTrue())

				allocation, ok := allocationManager.GetAllocation(1, kernelId)
				Expect(ok).To(BeTrue())
				Expect(allocation).ToNot(BeNil())
				Expect(allocation.GetKernelId()).To(Equal(kernelId))
				Expect(allocation.GetReplicaId()).To(Equal(int32(1)))
				Expect(allocation.GetMillicpus()).To(Equal(resourceSpec.CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(resourceSpec.MemoryMB()))
				Expect(allocation.GetGpus()).To(Equal(resourceSpec.GPU()))
				Expect(allocation.GetVramGb()).To(Equal(resourceSpec.VRAM()))
				Expect(allocation.ToDecimalSpec().Equals(resourceSpec)).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsNonZero()).To(BeTrue())

				Expect(allocationManager.KernelHasCommittedResources(kernelId)).To(BeTrue())
				Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernelId)).To(BeTrue())
				Expect(allocationManager.ReplicaHasCommittedResources(1, kernelId)).To(BeTrue())
				Expect(allocationManager.ReplicaHasPendingGPUs(1, kernelId)).To(BeFalse())

				By("Correctly stopping the kernel replica when instructed to do so")

				stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second*2)
				defer stopCancel()

				stopRespCan := make(chan struct{}, 1)

				go func() {
					restart := false

					resp, err := localScheduler.StopKernel(stopCtx, &proto.KernelId{
						Id:      kernelId,
						Restart: &restart,
					})

					Expect(err).To(BeNil())
					Expect(resp).ToNot(BeNil())

					stopRespCan <- struct{}{}
				}()

				// Python kernel should receive "shutdown_request"
				Eventually(func() *messaging.JupyterMessage {
					controlQueue := messageQueues[messaging.ControlMessage]
					Expect(controlQueue).ToNot(BeNil())

					val, ok := controlQueue.Peek()
					if ok {
						return val
					}

					return nil
				}, time.Second*4, time.Millisecond*250).ShouldNot(BeNil())

				messageHeader = &messaging.MessageHeader{
					MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
					Username: kernelId,
					Session:  kernelId,
					Date:     "2024-04-03T22:55:52.605Z",
					MsgType:  "shutdown_reply",
					Version:  "5.2",
				}

				unsignedFrames = [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1*/
					[]byte(""), /* Metadata */
					[]byte(""),
				}
				jFrames = messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err = jFrames.EncodeHeader(messageHeader)
				Expect(err).To(BeNil())

				shutdownRequestMsg, ok := controlQueue.Dequeue()
				Expect(ok).To(BeTrue())
				Expect(shutdownRequestMsg).ToNot(BeNil())

				msg = zmq4.Msg{
					Frames: jFrames.Frames,
					Type:   zmq4.UsrMsg,
				}

				identityFrames = nil
				for idx, frame := range shutdownRequestMsg.JupyterFrames.Frames {
					if bytes.Equal(frame, messaging.JupyterFrameIDSMSG) {
						identityFrames = shutdownRequestMsg.JupyterFrames.Frames[0:idx]
						break
					}
				}

				Expect(identityFrames).ToNot(BeNil())
				Expect(len(identityFrames)).To(Equal(2)) // the zmq identity and the dest.req thing.

				jFrames.Frames = append(identityFrames, jFrames.Frames...)

				msg = zmq4.Msg{
					Frames: jFrames.Frames,
					Type:   zmq4.UsrMsg,
				}

				err = controlSocket.Send(msg)
				Expect(err).To(BeNil())

				Eventually(stopRespCan, time.Millisecond*2250, time.Millisecond*250).Should(Receive())

				Expect(allocationManager.NumPendingAllocations()).To(Equal(0))
				Expect(allocationManager.NumAllocations()).To(Equal(0))
				Expect(allocationManager.NumCommittedAllocations()).To(Equal(0))

				Expect(allocationManager.KernelHasCommittedResources(kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasCommittedGPUs(1, kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasCommittedResources(1, kernelId)).To(BeFalse())
				Expect(allocationManager.ReplicaHasPendingGPUs(1, kernelId)).To(BeFalse())
			})
		})
	})

	// Not outdated, just older.
	Context("Older Unit Tests", func() {
		var (
			schedulerDaemon  *LocalScheduler
			vgpuPluginServer device.VirtualGpuPluginServer
			mockCtrl         *gomock.Controller
			kernel1Replica1  *mock_scheduling.MockKernelReplica
			kernel2Replica2  *mock_scheduling.MockKernelReplica
			resourceManager  *resource.AllocationManager
			hostSpec         *types.DecimalSpec
			schedulingPolicy *mock_scheduling.MockPolicy
			hostId           string
			hostName         string

			kernel1Key = "23d90942-8c3de3a713a5c3611792b7a5"
			kernel2Key = "d2324990-3563adca181e235c77317a9b"
			//kernel3Key          = "7d1657ee-0ec2-468b-9f08-60269954b181"
			kernel1Id = "66902bac-9386-432e-b1b9-21ac853fa1c9"
			kernel2Id = "c8fd0d64-b35d-4e14-80fa-4ed2d399bcb6"
			//kernel3Id           = "a40f1f8b-ed62-4c0f-b3c6-e42c781c917e"
			kernel1PersistentId = "63914d5f-57f6-4ff4-b95a-16d5a9e85946"

			workloadId = uuid.NewString()

			kernel1ResourceSpec = &proto.ResourceSpec{
				Gpu:    2,
				Cpu:    100,
				Memory: 1000,
				Vram:   4,
			}

			kernel1Spec = &proto.KernelSpec{
				Id:              kernel1Id,
				Session:         kernel1Id,
				Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
				SignatureScheme: "hmac-sha256",
				Key:             kernel1Key,
				ResourceSpec:    kernel1ResourceSpec,
				WorkloadId:      workloadId,
			}

			kernel2ResourceSpec = &proto.ResourceSpec{
				Gpu:    4,
				Cpu:    2048,
				Memory: 1250,
				Vram:   12,
			}

			kernel2Spec = &proto.KernelSpec{
				Id:              kernel2Id,
				Session:         kernel2Id,
				Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
				SignatureScheme: "hmac-sha256",
				Key:             kernel2Key,
				ResourceSpec:    kernel2ResourceSpec,
				WorkloadId:      workloadId,
			}

			//kernel3ResourceSpec = &proto.ResourceSpec{
			//	Gpu:    8,
			//	Cpu:    2048,
			//	Memory: 1250,
			//	Vram:   32,
			//}
			//
			//kernel3Spec = &proto.KernelSpec{
			//	Id:              kernel3Id,
			//	Session:         kernel3Id,
			//	Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			//	SignatureScheme: "hmac-sha256",
			//	Key:             kernel3Key,
			//	ResourceSpec:    kernel3ResourceSpec,
			//	workloadId:      workloadId,
			//}
		)

		BeforeEach(func() {
			hostId = uuid.NewString()
			hostName = "TestNode"
			mockCtrl = gomock.NewController(GinkgoT())
			schedulingPolicy = mock_scheduling.NewMockPolicy(mockCtrl)
			vgpuPluginServer = mock_device.NewMockVirtualGpuPluginServer(mockCtrl)
			hostSpec = &types.DecimalSpec{
				GPUs:      decimal.NewFromFloat(8),
				Millicpus: decimal.NewFromFloat(64000),
				MemoryMb:  decimal.NewFromFloat(128000),
				VRam:      decimal.NewFromFloat(32),
			}

			schedulingPolicy.EXPECT().ResourceBindingMode().AnyTimes().Return(scheduling.BindResourcesAtTrainingStart)
			schedulingPolicy.EXPECT().ContainerLifetime().AnyTimes().Return(scheduling.LongRunning)
			schedulingPolicy.EXPECT().NumReplicas().AnyTimes().Return(3)

			kernel1Replica1 = createKernelReplica(mockCtrl, kernel1Id, kernel1Key, workloadId, 1, kernel1Spec, kernel1ResourceSpec)
			kernel2Replica2 = createKernelReplica(mockCtrl, kernel2Id, kernel2Key, workloadId, 2, kernel2Spec, kernel2ResourceSpec)
			//kernel3Replica3 = createKernelReplica(mockCtrl, kernel3Id, kernel3Key, workloadId, 3, kernel3Spec, kernel3ResourceSpec)
			resourceManager = resource.NewAllocationManager(hostSpec, schedulingPolicy, hostId, hostName)

			schedulingPolicy, err := scheduler.GetSchedulingPolicy(&scheduling.SchedulerOptions{
				CommonOptions: configuration.CommonOptions{
					SchedulingPolicy:             string(scheduling.Static),
					IdleSessionReclamationPolicy: string(scheduling.NoIdleSessionReclamation),
					GpusPerHost:                  int(hostSpec.GPU()),
				},
				MinimumNumNodes: 3,
				MaximumNumNodes: 16,
			})
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())

			schedulerDaemon = &LocalScheduler{
				id:                     hostId,
				transport:              "tcp",
				kernels:                hashmap.NewCornelkMap[string, scheduling.KernelReplica](1000),
				closed:                 make(chan struct{}),
				cleaned:                make(chan struct{}),
				allocationManager:      resourceManager,
				virtualGpuPluginServer: vgpuPluginServer,
				schedulingPolicy:       schedulingPolicy,
			}

			notifyCallback := func(title string, content string, notificationType messaging.NotificationType) {
				schedulerDaemon.notifyClusterGatewayOfError(context.Background(), &proto.Notification{
					Id:               uuid.NewString(),
					Title:            title,
					Message:          content,
					NotificationType: int32(notificationType),
					Panicked:         false,
				})
			}

			schedulerDaemon.executeRequestForwarder = client.NewExecuteRequestForwarder[*messaging.JupyterMessage](
				notifyCallback, func(msg *messaging.JupyterMessage, kernel client.MessageRecipient) *messaging.JupyterMessage {
					return schedulerDaemon.processExecOrYieldRequest(msg, kernel.(scheduling.KernelReplica))
				})

			config.InitLogger(&schedulerDaemon.log, schedulerDaemon)
		})

		Context("Processing 'execute_request' messages", func() {
			var (
				headerKernel1 *messaging.MessageHeader
			)

			BeforeEach(func() {
				headerKernel1 = &messaging.MessageHeader{
					MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
					Username: "",
					Session:  kernel1Id,
					Date:     "2024-04-03T22:55:52.605Z",
					MsgType:  "execute_request",
					Version:  "5.2",
				}
			})

			It("Should convert the 'execute_request' message to a 'yield_request' message if there is a different replica specified as the target", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

				unsignedFrames := [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1*/
					[]byte(fmt.Sprintf("{\"%s\": 2}", domain.TargetReplicaArg)), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err := jFrames.EncodeHeader(headerKernel1)
				Expect(err).To(BeNil())
				frames, _ := jFrames.Sign(signatureScheme, []byte(kernel1Key))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)
				processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())
				Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

				var header *messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&header)

				GinkgoWriter.Printf("Header: %s\n", header.String())

				Expect(err).To(BeNil())
				Expect(header.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
			})

			It("Should convert the 'execute_request' message to a 'yield_request' message if there are insufficient GPUs available", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

				unsignedFrames := [][]byte{
					[]byte("<IDS|MSG>"), /* Frame start */
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1 */
					[]byte(""), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err := jFrames.EncodeHeader(headerKernel1)
				Expect(err).To(BeNil())
				frames, _ := jFrames.Sign(signatureScheme, []byte(kernel1Key))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)

				// Make it so that there are no idle GPUs available.
				resourceManager.DebugSetIdleGPUs(0)

				processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1Replica1) // , headerKernel1, offset)
				Expect(processedMessage).ToNot(BeNil())
				Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

				err = processedMessage.JupyterFrames.DecodeHeader(&headerKernel1)

				GinkgoWriter.Printf("Header: %v\n", headerKernel1)

				Expect(err).To(BeNil())
				Expect(headerKernel1.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
			})

			It("Should correctly return a 'yield_request' message if the kernel1Replica1 is set to yield the next execute request.", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(true).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Times(1)
				unsignedFrames := [][]byte{
					[]byte("<IDS|MSG>"), /* Frame start */
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1 */
					[]byte(""), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
				err := jFrames.EncodeHeader(headerKernel1)
				Expect(err).To(BeNil())
				frames, _ := jFrames.Sign(signatureScheme, []byte(kernel1Key))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)
				// Make it so that there are no idle GPUs available.
				resourceManager.DebugSetIdleGPUs(0)

				processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1Replica1) // , headerKernel1, offset)
				Expect(processedMessage).ToNot(BeNil())
				Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

				var header messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&header)

				GinkgoWriter.Printf("Header: %v\n", header)

				Expect(err).To(BeNil())
				Expect(header.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
			})

			It("Should correctly return two different signatures when the Jupyter message's headerKernel1 is changed by modifying the date.", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				unsignedFrames1 := [][]byte{
					[]byte("<IDS|MSG>"), /* Frame start */
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
					[]byte("{\"msg_id\":\"84f3e8e7-1aa96818ad5b99a0f38802ac_17_100\",\"msg_type\":\"ACK\",\"username\":\"username\",\"session\":\"84f3e8e7-1aa96818ad5b99a0f38802ac\",\"date\":\"2024-06-04T22:38:56.949010\",\"version\":\"5.3\"}"),                                                                /* Header */
					[]byte("{\"parent_header\":{\"msg_id\":\"2dbc4069-d766-4c23-8eba-c15069760869\",\"username\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"session\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"date\":\"2024-06-04T22:38:56.949010\",\"msg_type\":\"kernel_info_request\",\"version\":\"5.2\"}}"), /* Parent headerKernel1 */
					[]byte("{}"), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
				}
				jFrames1 := messaging.NewJupyterFramesFromBytes(unsignedFrames1)
				frames1, err := jFrames1.Sign(signatureScheme, []byte(kernel1Key))
				Expect(err).To(BeNil())
				Expect(frames1).ToNot(BeNil())
				signature1 := frames1[messaging.JupyterFrameSignature]

				unsignedFrames2 := [][]byte{
					[]byte("<IDS|MSG>"), /* Frame start */
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
					[]byte("{\"msg_id\":\"84f3e8e7-1aa96818ad5b99a0f38802ac_17_100\",\"msg_type\":\"ACK\",\"username\":\"username\",\"session\":\"84f3e8e7-1aa96818ad5b99a0f38802ac\",\"date\":\"2024-06-04T22:38:56.949011\",\"version\":\"5.3\"}"),                                                                /* Header */
					[]byte("{\"parent_header\":{\"msg_id\":\"2dbc4069-d766-4c23-8eba-c15069760869\",\"username\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"session\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"date\":\"2024-06-04T22:38:56.949010\",\"msg_type\":\"kernel_info_request\",\"version\":\"5.2\"}}"), /* Parent headerKernel1 */
					[]byte("{}"), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
				}
				jFrames2 := messaging.NewJupyterFramesFromBytes(unsignedFrames2)
				frames2, err := jFrames2.Sign(signatureScheme, []byte(kernel1Key))
				Expect(err).To(BeNil())
				Expect(frames2).ToNot(BeNil())
				signature2 := frames2[messaging.JupyterFrameSignature]

				fmt.Printf("Signature #1: \"%s\"\n", signature1)
				fmt.Printf("Signature #2: \"%s\"\n", signature2)

				Expect(signature1).ToNot(Equal(signature2))
			})

			It("Should correctly process and return an 'execute_request' message if there are no reasons to convert it to a YIELD request", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

				unsignedExecReqFrames := [][]byte{
					[]byte("<IDS|MSG>"), /* Frame start */
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
					[]byte(""), /* Header */
					[]byte(""), /* Parent headerKernel1 */
					[]byte(""), /* Metadata */
					[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
				}
				jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecReqFrames)
				err = jFrames.EncodeHeader(headerKernel1)
				Expect(err).To(BeNil())
				frames, _ := jFrames.Sign(signatureScheme, []byte(kernel1Key))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)
				processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1Replica1) // , headerKernel1, offset)
				Expect(processedMessage).ToNot(BeNil())
				Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var header messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&header)

				GinkgoWriter.Printf("Header: %v\n", header)

				Expect(err).To(BeNil())
				Expect(header.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
			})
		})

		Context("Resource allocations", func() {
			validatePending := func(pendingReplicas []scheduling.KernelReplica) {
				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(len(pendingReplicas)))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				combinedSpec := types.NewDecimalSpec(0, 0, 0, 0)
				for _, pendingKernelReplica := range pendingReplicas {
					combinedSpec = types.ToDecimalSpec(combinedSpec.Add(pendingKernelReplica.ResourceSpec()))
				}

				Expect(resourceManager.PendingCPUs().Equals(combinedSpec.Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(combinedSpec.VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(combinedSpec.MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(combinedSpec.GPUs)).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				for _, pendingKernelReplica := range pendingReplicas {
					allocation, exists := resourceManager.GetAllocation(pendingKernelReplica.ReplicaID(), pendingKernelReplica.ID())
					Expect(exists).To(BeTrue())
					Expect(allocation).ToNot(BeNil())

					Expect(allocation.GetReplicaId()).To(Equal(pendingKernelReplica.ReplicaID()))
					Expect(allocation.GetKernelId()).To(Equal(pendingKernelReplica.ID()))

					Expect(allocation.GetGpus()).To(Equal(pendingKernelReplica.ResourceSpec().GPU()))
					Expect(allocation.GetMillicpus()).To(Equal(pendingKernelReplica.ResourceSpec().CPU()))
					Expect(allocation.GetMemoryMb()).To(Equal(pendingKernelReplica.ResourceSpec().MemoryMB()))
					Expect(allocation.GetVramGb()).To(Equal(pendingKernelReplica.ResourceSpec().VRAM()))

					Expect(allocation.IsPending()).To(BeTrue())
					Expect(allocation.IsCommitted()).To(BeFalse())
					Expect(allocation.IsReservation()).To(BeFalse())
				}
			}

			It("Should allocate resources to multiple kernels upon receiving 'execute_request' messages", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				kernel2Replica2.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel2Replica2.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

				err = resourceManager.ContainerStartedRunningOnHost(kernel2Replica2.ReplicaID(), kernel2Replica2.ID(), kernel2Replica2.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				By("Embedding the idle GPUs in the metadata of the message for kernel 1")
				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var processedMessageHeader messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				By("Creating a pending allocation for kernel 2")

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				processedMessage = processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel2Replica2)
				Expect(processedMessage).ToNot(BeNil())

				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				By("Embedding the idle GPUs in the metadata of the message")
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(2))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(6))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(2))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(16))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			})

			It("Should commit resources to a kernel upon receiving an 'smr_lead_task' message", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				allocation, exists := resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Committing resources (as a reservation) when an 'execute_request' message is received")

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				By("Embedding the idle GPUs in the metadata of the message for kernel 1")
				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var processedMessageHeader messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				kernel1Replica1.EXPECT().KernelStartedTraining(gomock.Any()).Times(1).Return(nil)

				leadTaskMsg := test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
				err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())
			})

			It("Should release resources that were fully committed to a kernel replica if that kernel replica is stopped", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				allocation, exists := resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Committing resources (as a reservation) when an 'execute_request' message is received")

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				By("Embedding the idle GPUs in the metadata of the message for kernel 1")
				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var processedMessageHeader messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				kernel1Replica1.EXPECT().Sessions().Return([]string{kernel1Id}).Times(1)
				kernel1Replica1.EXPECT().RequestWithHandler(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
					if done != nil {
						done()
					}

					return nil
				})

				restart := false
				_, err = schedulerDaemon.StopKernel(context.Background(), &proto.KernelId{
					Id:           kernel1Id,
					Restart:      &restart,
					PersistentId: &kernel1PersistentId,
				})

				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())
			})

			It("Should release pending resources allocated to a kernel replica if that kernel replica is stopped", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				allocation, exists := resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation()).To(BeFalse())

				kernel1Replica1.EXPECT().Sessions().Return([]string{kernel1Id}).Times(1)
				kernel1Replica1.EXPECT().RequestWithHandler(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
					if done != nil {
						done()
					}

					return nil
				})

				restart := false
				_, err = schedulerDaemon.StopKernel(context.Background(), &proto.KernelId{
					Id:           kernel1Id,
					Restart:      &restart,
					PersistentId: &kernel1PersistentId,
				})

				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())
			})

			It("Should release resources that were committed as a reservation to a kernel replica if that kernel replica is stopped", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				allocation, exists := resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Committing resources (as a reservation) when an 'execute_request' message is received")

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var processedMessageHeader messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				kernel1Replica1.EXPECT().KernelStartedTraining(gomock.Any()).Times(1).Return(nil)

				leadTaskMsg := test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
				err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				kernel1Replica1.EXPECT().Sessions().Return([]string{kernel1Id}).Times(1)
				kernel1Replica1.EXPECT().RequestWithHandler(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
					if done != nil {
						done()
					}

					return nil
				})

				restart := false
				_, err = schedulerDaemon.StopKernel(context.Background(), &proto.KernelId{
					Id:           kernel1Id,
					Restart:      &restart,
					PersistentId: &kernel1PersistentId,
				})

				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())
			})

			It("Should release resources that were fully committed to a kernel replica once that replica stops training", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				allocation, exists := resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Committing resources (as a reservation) when an 'execute_request' message is received")

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				By("Embedding the idle GPUs in the metadata of the message for kernel 1")
				var metadata map[string]interface{}
				err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
				GinkgoWriter.Printf("metadata: %v\n", metadata)
				Expect(err).To(BeNil())
				Expect(len(metadata)).To(Equal(9))

				var processedMessageHeader messaging.MessageHeader
				err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

				GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

				Expect(err).To(BeNil())
				Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

				GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
				GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
				GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
				GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Promoting the committed resource reservation to a fully-committed allocation when an 'smr_lead_task' message is received")

				kernel1Replica1.EXPECT().KernelStartedTraining(gomock.Any()).Times(1).Return(nil)

				leadTaskMsg := test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
				err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
				Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
				Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

				allocation, exists = resourceManager.GetAllocation(kernel1Replica1.ReplicaID(), kernel1Replica1.ID())
				Expect(exists).To(BeTrue())
				Expect(allocation).ToNot(BeNil())

				Expect(allocation.GetReplicaId()).To(Equal(kernel1Replica1.ReplicaID()))
				Expect(allocation.GetKernelId()).To(Equal(kernel1Replica1.ID()))

				Expect(allocation.GetGpus()).To(Equal(kernel1Replica1.ResourceSpec().GPU()))
				Expect(allocation.GetMillicpus()).To(Equal(kernel1Replica1.ResourceSpec().CPU()))
				Expect(allocation.GetMemoryMb()).To(Equal(kernel1Replica1.ResourceSpec().MemoryMB()))
				Expect(allocation.GetVramGb()).To(Equal(kernel1Replica1.ResourceSpec().VRAM()))

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation()).To(BeFalse())

				By("Releasing the resources once training ends")

				kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any(), gomock.Any()).Times(1)
				kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.", gomock.Any()).Times(1).Return(nil)

				execReqHeader, err := processedMessage.GetHeader()
				Expect(err).To(BeNil())
				Expect(execReqHeader).ToNot(BeNil())

				executeReplyContent := map[string]interface{}{"status": "ok"}
				executeReplyMsg := test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply,
					kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent, execReqHeader)
				err = schedulerDaemon.processExecuteReply(executeReplyMsg, kernel1Replica1)
				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

				By("Releasing the pending resources if that kernel is stopped")

				kernel1Replica1.EXPECT().Sessions().Return([]string{kernel1Id}).Times(1)
				kernel1Replica1.EXPECT().RequestWithHandler(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
					if done != nil {
						done()
					}

					return nil
				})

				restart := false
				_, err = schedulerDaemon.StopKernel(context.Background(), &proto.KernelId{
					Id:           kernel1Id,
					Restart:      &restart,
					PersistentId: &kernel1PersistentId,
				})

				Expect(err).To(BeNil())

				Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
				Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

				Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
				Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

				Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
				Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
				Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
				Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())
			})

			It("Should successfully handle two pending kernel replicas", func() {
				kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
				kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

				schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)
				schedulerDaemon.kernels.Store(kernel2Replica2.ID(), kernel2Replica2)

				err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				validatePending([]scheduling.KernelReplica{kernel1Replica1})

				err = resourceManager.ContainerStartedRunningOnHost(kernel2Replica2.ReplicaID(), kernel2Replica2.ID(), kernel2Replica2.ResourceSpec())
				Expect(err).To(BeNil())

				validatePending([]scheduling.KernelReplica{kernel1Replica1, kernel2Replica2})
			})

			Context("Adjusting resource specs", func() {
				updateKernelResourceSpec := func(kernelReplica *mock_scheduling.MockKernelReplica, newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
					GinkgoWriter.Printf("Updating resource spec of kernel 1 Replica 1 from %v to %v.\n", kernelReplica.ResourceSpec(), newSpec)

					Expect(tx).To(BeNil())

					currentSpec := kernelReplica.ResourceSpec()

					currentSpec.Millicpus = decimal.NewFromFloat(newSpec.CPU())
					currentSpec.MemoryMb = decimal.NewFromFloat(newSpec.MemoryMB())
					currentSpec.GPUs = decimal.NewFromFloat(newSpec.GPU())
					currentSpec.VRam = decimal.NewFromFloat(newSpec.VRAM())

					GinkgoWriter.Printf("kernel 1 Replica 1 resource spec post-modification: %v\n", kernelReplica.ResourceSpec())

					return nil
				}

				updateKernel1Replica1ResourceSpec := func(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
					return updateKernelResourceSpec(kernel1Replica1, newSpec, tx)
				}

				//updateKernel2Replica2ResourceSpec := func(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
				//	return updateKernelResourceSpec(kernel2Replica2, newSpec, tx)
				//}
				//
				//updateKernel3Replica3ResourceSpec := func(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
				//	return updateKernelResourceSpec(kernel3Replica3, newSpec, tx)
				//}

				validateCommittedReserved := func(kernelReplica scheduling.KernelReplica) {
					GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
					GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
					GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
					GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

					Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
					Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
					Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(int(kernelReplica.ResourceSpec().GPU())))
					Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(8 - int(kernelReplica.ResourceSpec().GPU())))

					allocation, exists := resourceManager.GetAllocation(kernelReplica.ReplicaID(), kernelReplica.ID())
					Expect(exists).To(BeTrue())
					Expect(allocation).ToNot(BeNil())

					Expect(allocation.GetReplicaId()).To(Equal(kernelReplica.ReplicaID()))
					Expect(allocation.GetKernelId()).To(Equal(kernelReplica.ID()))

					Expect(allocation.GetGpus()).To(Equal(kernelReplica.ResourceSpec().GPU()))
					Expect(allocation.GetMillicpus()).To(Equal(kernelReplica.ResourceSpec().CPU()))
					Expect(allocation.GetMemoryMb()).To(Equal(kernelReplica.ResourceSpec().MemoryMB()))
					Expect(allocation.GetVramGb()).To(Equal(kernelReplica.ResourceSpec().VRAM()))

					Expect(allocation.IsPending()).To(BeFalse())
					Expect(allocation.IsCommitted()).To(BeTrue())
					Expect(allocation.IsReservation()).To(BeFalse())

					Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

					Expect(resourceManager.CommittedGPUs().Equals(kernelReplica.ResourceSpec().GPUs)).To(BeTrue())
					Expect(resourceManager.CommittedVRamGB().Equals(kernelReplica.ResourceSpec().VRam)).To(BeTrue())
					Expect(resourceManager.CommittedCPUs().Equals(kernelReplica.ResourceSpec().Millicpus)).To(BeTrue())
					Expect(resourceManager.CommittedMemoryMB().Equals(kernelReplica.ResourceSpec().MemoryMb)).To(BeTrue())

					Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8).Sub(kernelReplica.ResourceSpec().GPUs))).To(BeTrue())
					Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32).Sub(kernelReplica.ResourceSpec().VRam))).To(BeTrue())
					Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000).Sub(kernelReplica.ResourceSpec().MemoryMb))).To(BeTrue())
					Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000).Sub(kernelReplica.ResourceSpec().Millicpus))).To(BeTrue())
				}

				validateCommittedFully := func(kernelReplica scheduling.KernelReplica) {
					GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
					GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(1))
					GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(1))
					GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(1))

					Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
					Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
					Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(int(kernelReplica.ResourceSpec().GPU())))
					Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(8 - int(kernelReplica.ResourceSpec().GPU())))

					allocation, exists := resourceManager.GetAllocation(kernelReplica.ReplicaID(), kernelReplica.ID())
					Expect(exists).To(BeTrue())
					Expect(allocation).ToNot(BeNil())

					Expect(allocation.GetReplicaId()).To(Equal(kernelReplica.ReplicaID()))
					Expect(allocation.GetKernelId()).To(Equal(kernelReplica.ID()))

					Expect(allocation.GetGpus()).To(Equal(kernelReplica.ResourceSpec().GPU()))
					Expect(allocation.GetMillicpus()).To(Equal(kernelReplica.ResourceSpec().CPU()))
					Expect(allocation.GetMemoryMb()).To(Equal(kernelReplica.ResourceSpec().MemoryMB()))
					Expect(allocation.GetVramGb()).To(Equal(kernelReplica.ResourceSpec().VRAM()))

					Expect(allocation.IsPending()).To(BeFalse())
					Expect(allocation.IsCommitted()).To(BeTrue())
					Expect(allocation.IsReservation()).To(BeFalse())

					Expect(resourceManager.PendingCPUs().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingVRAM().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingMemoryMB().Equals(decimal.Zero)).To(BeTrue())
					Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())

					Expect(resourceManager.CommittedGPUs().Equals(kernelReplica.ResourceSpec().GPUs)).To(BeTrue())
					Expect(resourceManager.CommittedVRamGB().Equals(kernelReplica.ResourceSpec().VRam)).To(BeTrue())
					Expect(resourceManager.CommittedCPUs().Equals(kernelReplica.ResourceSpec().Millicpus)).To(BeTrue())
					Expect(resourceManager.CommittedMemoryMB().Equals(kernelReplica.ResourceSpec().MemoryMb)).To(BeTrue())

					Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8).Sub(kernelReplica.ResourceSpec().GPUs))).To(BeTrue())
					Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32).Sub(kernelReplica.ResourceSpec().VRam))).To(BeTrue())
					Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000).Sub(kernelReplica.ResourceSpec().MemoryMb))).To(BeTrue())
					Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000).Sub(kernelReplica.ResourceSpec().Millicpus))).To(BeTrue())
				}

				It("Will update attempt to the resource request of a kernel replica if a new request is included in an 'execute_request' message", func() {
					kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
					kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

					schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

					err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
					Expect(err).To(BeNil())

					validatePending([]scheduling.KernelReplica{kernel1Replica1})

					By("Committing resources (as a reservation) when an 'execute_request' message is received")

					processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
					Expect(processedMessage).ToNot(BeNil())

					validateCommittedReserved(kernel1Replica1)

					By("Embedding the idle GPUs in the metadata of the message for kernel 1")
					var metadata map[string]interface{}
					err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
					GinkgoWriter.Printf("metadata: %v\n", metadata)
					Expect(err).To(BeNil())
					Expect(len(metadata)).To(Equal(9))

					var processedMessageHeader messaging.MessageHeader
					err = processedMessage.JupyterFrames.DecodeHeader(&processedMessageHeader)

					GinkgoWriter.Printf("Header: %v\n", processedMessageHeader)

					Expect(err).To(BeNil())
					Expect(processedMessageHeader.MsgType.String()).To(Equal(messaging.ShellExecuteRequest))

					By("Promoting the committed resource reservation to a fully-committed allocation when an 'smr_lead_task' message is received")

					kernel1Replica1.EXPECT().KernelStartedTraining(gomock.Any()).Times(1).Return(nil)

					leadTaskMsg := test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
					err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
					Expect(err).To(BeNil())

					validateCommittedFully(kernel1Replica1)

					By("Releasing the resources once training ends")

					kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any(), gomock.Any()).Times(1)
					kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.", gomock.Any()).Times(1).Return(nil)

					execReqHeader, err := processedMessage.GetHeader()
					Expect(err).To(BeNil())
					Expect(execReqHeader).ToNot(BeNil())

					executeReplyContent := map[string]interface{}{"status": "ok"}
					executeReplyMsg := test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply,
						kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent, execReqHeader)
					err = schedulerDaemon.processExecuteReply(executeReplyMsg, kernel1Replica1)
					Expect(err).To(BeNil())

					validatePending([]scheduling.KernelReplica{kernel1Replica1})

					By("Correctly updating the resource request of the kernel replica upon receiving another 'execute_request' " +
						"message with a resource request encoded within the message's metadata")

					updatedResourceSpecs := []*types.Float64Spec{
						types.NewFloat64Spec(128, 256, 1, 1),
						types.NewFloat64Spec(256, 512, 2, 2),
						types.NewFloat64Spec(512, 1024, 4, 4),
						types.NewFloat64Spec(1024, 2048, 8, 8),
						types.NewFloat64Spec(2048, 4096, 6, 16),
						types.NewFloat64Spec(4096, 8192, 8, 32),
						types.NewFloat64Spec(5797, 26821, 1, 16),
						types.NewFloat64Spec(6641, 16023, 6, 32),
						types.NewFloat64Spec(5965, 19281, 8, 7),
						types.NewFloat64Spec(4910, 16966, 2, 30),
						types.NewFloat64Spec(4345, 27219, 8, 14),
						types.NewFloat64Spec(1247, 532, 4, 6),
						types.NewFloat64Spec(635, 6546, 5, 5),
						types.NewFloat64Spec(2336, 25878, 4, 28),
						types.NewFloat64Spec(5698, 5090, 8, 17),
						types.NewFloat64Spec(6711, 21702, 1, 20),
						types.NewFloat64Spec(6638, 22094, 8, 17),
						types.NewFloat64Spec(932, 15690, 7, 17),
						types.NewFloat64Spec(892, 20568, 1, 9),
						types.NewFloat64Spec(6760, 26074, 6, 30),
						types.NewFloat64Spec(6449, 26315, 8, 14),
						types.NewFloat64Spec(676, 10821, 5, 5),
					}

					for _, updatedSpec := range updatedResourceSpecs {
						kernel1Replica1.EXPECT().UpdateResourceSpec(gomock.Any(), nil).Times(1).DoAndReturn(updateKernel1Replica1ResourceSpec)

						processedMessage = processExecuteRequestWithUpdatedResourceSpec(schedulerDaemon, messaging.ShellExecuteRequest,
							kernel1Replica1, updatedSpec)

						Expect(processedMessage).ToNot(BeNil())
						Expect(kernel1Replica1.ResourceSpec().Equals(updatedSpec)).To(BeTrue())
						Expect(resourceManager.CommittedResources().Equals(updatedSpec)).To(BeTrue())
						Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(updatedSpec))).To(BeTrue())
						Expect(resourceManager.SpecResources().Equals(hostSpec.Clone())).To(BeTrue())
						Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())
						validateCommittedReserved(kernel1Replica1)

						By("Promoting the committed resource reservation to a fully-committed allocation when an 'smr_lead_task' message is received")

						kernel1Replica1.EXPECT().KernelStartedTraining(gomock.Any()).Times(1).Return(nil)

						leadTaskMsg = test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
						err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
						Expect(err).To(BeNil())

						validateCommittedFully(kernel1Replica1)
						Expect(kernel1Replica1.ResourceSpec().Equals(updatedSpec)).To(BeTrue())
						Expect(resourceManager.CommittedResources().Equals(updatedSpec)).To(BeTrue())
						Expect(resourceManager.IdleResources().Equals(hostSpec.Subtract(updatedSpec))).To(BeTrue())
						Expect(resourceManager.SpecResources().Equals(hostSpec.Clone())).To(BeTrue())
						Expect(resourceManager.PendingResources().IsZero()).To(BeTrue())

						By("Releasing the resources once training ends")

						kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any(), gomock.Any()).Times(1)
						kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.", gomock.Any()).Times(1).Return(nil)

						execReqHeader, err := processedMessage.GetHeader()
						Expect(err).To(BeNil())
						Expect(execReqHeader).ToNot(BeNil())

						executeReplyContent = map[string]interface{}{"status": "ok"}
						executeReplyMsg = test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply,
							kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent, execReqHeader)
						err = schedulerDaemon.processExecuteReply(executeReplyMsg, kernel1Replica1)
						Expect(err).To(BeNil())

						validatePending([]scheduling.KernelReplica{kernel1Replica1})
						Expect(kernel1Replica1.ResourceSpec().Equals(updatedSpec)).To(BeTrue())
						Expect(resourceManager.CommittedResources().IsZero()).To(BeTrue())
						Expect(resourceManager.IdleResources().Equals(hostSpec)).To(BeTrue())
						Expect(resourceManager.SpecResources().Equals(hostSpec.Clone())).To(BeTrue())
						Expect(resourceManager.PendingResources().Equals(updatedSpec)).To(BeTrue())
					}
				})

				It("Will fail to reserve an updated resource spec for a kernel replica if the updated spec exceeds the host's available resources", func() {
					kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
					kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

					schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

					err := resourceManager.ContainerStartedRunningOnHost(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
					Expect(err).To(BeNil())

					validatePending([]scheduling.KernelReplica{kernel1Replica1})

					impossibleSpec := types.NewFloat64Spec(500, 500, 16, 32)

					By("Updating the pending resource request of the kernel, but failing to upgrade the pending request to committed")

					processedMessage := processExecuteRequestWithUpdatedResourceSpec(schedulerDaemon,
						messaging.ShellExecuteRequest, kernel1Replica1, impossibleSpec)

					fmt.Printf("processedMessage: %v\n", processedMessage.StringFormatted())

					Expect(processedMessage).ToNot(BeNil())

					By("Returning a 'yield_request' message after processing the 'execute_request' message that contained the large spec")

					Expect(processedMessage.JupyterMessageType()).To(Equal(messaging.ShellYieldRequest))
				})
			})
		})
	})
})
