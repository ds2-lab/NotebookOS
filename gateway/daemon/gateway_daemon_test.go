package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/promise"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"github.com/shopspring/decimal"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"go.uber.org/mock/gomock"
)

const (
	signatureScheme string = "hmac-sha256"

	kernelId string = "66902bac-9386-432e-b1b9-21ac853fa1c9"
)

var (
	persistentId = "a45e4331-8fdc-4143-aac8-00d3e9df54fa"

	GatewayOptsAsJsonString = `{
	"logger_options": {
		"Debug": true,
		"Verbose": false
	},
	"connection_info": {
		"ip": "",
		"control_port": 9001,
		"shell_port": 9002,
		"stdin_port": 9003,
		"hb_port": 9000,
		"iopub_port": 9004,
		"iosub_port": 9005,
		"ack_port": 9006,
		"transport": "tcp",
		"signature_scheme": "",
		"key": "",
		"starting_resource_port": 9007,
		"num_resource_ports": 256
	},
	"cluster_daemon_options": {
		"cluster_scheduler_options": {
			"num-virtual-gpus-per-node": 72,
			"subscribed-ratio-update-interval": 1,
			"scaling-factor": 1.10,
			"scaling-interval": 15,
			"scaling-limit": 1.15,
			"scaling-in-limit": 2,
			"predictive_autoscaling": false,
			"scaling-buffer-size": 3,
			"min_cluster_nodes": 4,
			"max_cluster_nodes": 32,
			"gpu_poll_interval": 5,
			"num-replicas": 3,
			"max-subscribed-ratio": 7,
			"execution-time-sampling-window": 10,
			"migration-time-sampling-window": 10,
			"scheduler-http-port": 8078,
			"common_options": {
			"gpus-per-host": 8,
			"deployment_mode": "docker-compose",
			"using-wsl": true,
			"docker_network_name": "distributed_cluster_default",
			"prometheus_interval": 15,
			"prometheus_port": -1,
			"num_resend_attempts": 1,
			"acks_enabled": false,
			"scheduling-policy": "static",
			"idle-session-reclamation-policy": "none",
			"remote-storage-endpoint": "host.docker.internal:10000",
			"smr-port": 8080,
			"debug_mode": true,
			"debug_port": 9996,
			"simulate_checkpointing_latency": true,
			"disable_prometheus_metrics_publishing": true
		}
	},
	"local-daemon-service-name": "local-daemon-network",
	"local-daemon-service-port": 8075,
	"global-daemon-service-name": "daemon-network",
	"global-daemon-service-port": 0,
	"kubernetes-namespace": "",
	"use-stateful-set": false,
	"notebook-image-name": "scusemua/jupyter",
	"notebook-image-tag": "latest",
	"distributed-cluster-service-port": 8079,
	"remote-docker-event-aggregator-port": 5821,
	"initial-cluster-size": -1,
	"initial-connection-period": 0
},
	"port": 8080,
	"provisioner_port": 8081,
	"jaeger_addr": "",
	"consul_addr": ""
}`
)

type MockedDistributedKernelClientProvider struct {
	ctrl *gomock.Controller

	// expectedKernels are registered ahead of time and returned when a call to NewDistributedKernelClient
	// is passed a proto.KernelSpec whose ID is a key to expectedKernels.
	expectedKernels map[string]*mock_scheduling.MockKernel
}

func NewMockedDistributedKernelClientProvider(ctrl *gomock.Controller) *MockedDistributedKernelClientProvider {
	return &MockedDistributedKernelClientProvider{
		ctrl:            ctrl,
		expectedKernels: make(map[string]*mock_scheduling.MockKernel),
	}
}

func (p *MockedDistributedKernelClientProvider) CreateAndRegisterMockedDistributedKernel(kernelId string) *mock_scheduling.MockKernel {
	kernel := mock_scheduling.NewMockKernel(p.ctrl)

	kernel.EXPECT().InitializeShellForwarder(gomock.Any()).Times(1)
	kernel.EXPECT().InitializeIOForwarder().Times(1)
	kernel.EXPECT().ID().Return(kernelId).AnyTimes()
	kernel.EXPECT().SetSession(gomock.Any()).MaxTimes(1)
	kernel.EXPECT().AddReplica(gomock.Any(), gomock.Any()).MinTimes(3).Return(nil)
	kernel.EXPECT().PersistentID().Times(1).Return(persistentId)
	kernel.EXPECT().NumActiveMigrationOperations().Times(1).Return(0)

	p.expectedKernels[kernelId] = kernel
	return kernel
}

func (p *MockedDistributedKernelClientProvider) RegisterMockedDistributedKernel(kernelId string, kernel *mock_scheduling.MockKernel) {
	p.expectedKernels[kernelId] = kernel
}

func (p *MockedDistributedKernelClientProvider) NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec,
	numReplicas int, hostId string, connectionInfo *jupyter.ConnectionInfo, persistentId string, debugMode bool,
	executionFailedCallback scheduling.ExecutionFailedCallback, executionLatencyCallback scheduling.ExecutionLatencyCallback,
	statisticsProvider scheduling.StatisticsProvider, notificationCallback scheduling.NotificationCallback) scheduling.Kernel {

	if kernel, ok := p.expectedKernels[spec.Id]; ok {
		return kernel
	}

	panic(fmt.Sprintf("No mocked kernel registered with ID=\"%s\"\n", spec.Id))
}

// addHost creates and returns a new Host whose LocalGatewayClient is mocked.
func addHost(idx int, clusterGateway *ClusterGatewayImpl, mockCtrl *gomock.Controller) (scheduling.Host, *mock_proto.MockLocalGatewayClient, *distNbTesting.ResourceSpoofer, error) {
	hostId := uuid.NewString()
	nodeName := fmt.Sprintf("TestNode%d", idx)
	resourceSpoofer := distNbTesting.NewResourceSpoofer(nodeName, hostId, clusterGateway.hostSpec)
	host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, clusterGateway.cluster, hostId, nodeName, resourceSpoofer)

	return host, localGatewayClient, resourceSpoofer, err
}

// prepareMockedGatewayForStartKernel prepares the given *mock_proto.MockLocalGatewayClient to have its StartKernelReplica
// method called during the creation of a new kernel.
func prepareMockedGatewayForStartKernel(localGatewayClient *mock_proto.MockLocalGatewayClient, idx int, resourceSpoofer *distNbTesting.ResourceSpoofer, resourceSpec *proto.ResourceSpec, startKernelReturnValChan chan *proto.KernelConnectionInfo, startKernelReplicaCalled *sync.WaitGroup, numKernels int) {
	localGatewayClient.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
		GinkgoWriter.Printf("LocalGateway #%d has called spoofed StartKernelReplica\n", idx)

		// defer GinkgoRecover()

		err := resourceSpoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())

		if err != nil {
			GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
		}

		Expect(err).To(BeNil())
		err = resourceSpoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())

		if err != nil {
			GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)
		}

		Expect(err).To(BeNil())

		startKernelReplicaCalled.Done()

		GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #%d to be passed via channel.\n", idx)
		ret := <-startKernelReturnValChan

		GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #%d: %v\n", idx, ret)

		return ret, nil
	}).Times(numKernels)
}

var _ = Describe("Cluster Gateway Tests", func() {
	var (
		clusterGateway          *ClusterGatewayImpl
		abstractServer          *server.AbstractServer
		session                 *mock_scheduling.MockUserSession
		mockCtrl                *gomock.Controller
		kernelKey               = "23d90942-8c3de3a713a5c3611792b7a5"
		jupyterExecuteRequestId = "c7074e5b-b90f-44f8-af5d-63201ec3a527"

		hostSpec = &types.DecimalSpec{
			GPUs:      decimal.NewFromFloat(8),
			Millicpus: decimal.NewFromFloat(64000),
			MemoryMb:  decimal.NewFromFloat(128000),
			VRam:      decimal.NewFromFloat(40),
		}
	)

	// initMockedKernelForCreation creates and returns a new MockAbstractDistributedKernelClient that is
	// set up for use in a unit test that involves creating a new kernel.
	initMockedKernelForCreation := func(mockCtrl *gomock.Controller, kernelId string, kernelKey string, resourceSpec *proto.ResourceSpec, numReplicas int) (*mock_scheduling.MockKernel, *proto.KernelSpec) {
		persistentId := uuid.NewString()

		kernelSpec := &proto.KernelSpec{
			Id:              kernelId,
			Session:         kernelId,
			Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
			SignatureScheme: messaging.JupyterSignatureScheme,
			Key:             kernelKey,
			ResourceSpec:    resourceSpec,
		}

		kernel := mock_scheduling.NewMockKernel(mockCtrl)
		var currentSize atomic.Int32
		var sessionId string

		kernel.EXPECT().InitializeShellForwarder(gomock.Any()).Times(1)
		kernel.EXPECT().InitializeIOForwarder().Times(1)
		kernel.EXPECT().ID().Return(kernelId).AnyTimes()

		kernel.EXPECT().SetSession(gomock.Any()).MaxTimes(1).DoAndReturn(func(session scheduling.UserSession) {
			sessionId = session.ID()
		})

		kernel.EXPECT().AddReplica(gomock.Any(), gomock.Any()).
			Times(3).
			DoAndReturn(func(r scheduling.KernelReplica, h scheduling.Host) error {
				currentSize.Add(1)

				return nil
			})

		kernel.EXPECT().PersistentID().AnyTimes().Return(persistentId)
		kernel.EXPECT().NumActiveMigrationOperations().Times(3).Return(0)
		kernel.EXPECT().Size().AnyTimes().DoAndReturn(func() int {
			return int(currentSize.Load())
		})

		kernel.EXPECT().Sessions().MaxTimes(1).Return([]string{sessionId})
		kernel.EXPECT().GetSocketPort(messaging.ShellMessage).MaxTimes(1).Return(9001)
		kernel.EXPECT().GetSocketPort(messaging.IOMessage).MaxTimes(2).Return(9004)
		kernel.EXPECT().KernelSpec().MaxTimes(2).Return(kernelSpec)
		kernel.EXPECT().String().AnyTimes().Return("SPOOFED KERNEL " + kernelId + " STRING")

		executionManager := client.NewExecutionManager(kernel, numReplicas, clusterGateway.executionFailed,
			nil, nil, nil)
		kernel.EXPECT().GetExecutionManager().AnyTimes().Return(executionManager)

		return kernel, kernelSpec
	}

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	AfterEach(func() {
		if clusterGateway != nil {
			_ = clusterGateway.Close()
		}

		mockCtrl.Finish()
	})

	Context("ZMQ4 Socket Connectivity Tests", func() {
		It("Zmq4 Router Server, Dealer Client", func() {
			serverCtx, serverCancel := context.WithCancel(context.Background())
			defer serverCancel()

			serverPortChan := make(chan int, 1)

			serverFunc := func() {
				// Create a ROUTER socket
				server := zmq4.NewRouter(serverCtx, zmq4.WithID(zmq4.SocketIdentity("router")))
				defer server.Close()

				// Bind the server to an address
				address := "tcp://localhost:0"
				err := server.Listen(address)
				Expect(err).To(BeNil())

				serverPort := server.Addr().(*net.TCPAddr).Port
				fmt.Printf("Server started on port %d, waiting for messages...\n", serverPort)

				serverPortChan <- serverPort

				for {
					// Receive a message
					msg, err := server.Recv()
					if errors.Is(err, context.Canceled) {
						fmt.Printf("Server context cancelled.\n")
						return
					}

					if err != nil {
						fmt.Printf("[ERROR] Failed to receive message from client: %v\n", err)
					}

					Expect(err).To(BeNil())

					identity := msg.Frames[0]
					content := msg.Frames[1]
					fmt.Printf("Server received: [%s] %s\n", identity, content)

					// Send a response back to the client
					response := zmq4.NewMsgFrom([][]byte{
						identity,                           // Identity frame
						[]byte("Echo: " + string(content)), // Content frame
					}...)

					err = server.Send(response)
					if err != nil {
						fmt.Printf("[ERROR] Failed to send message back to client: %v\n", err)
					}

					Expect(err).To(BeNil())
				}
			}

			go serverFunc()

			// Create a ROUTER socket
			client := zmq4.NewDealer(context.Background(), zmq4.WithID(zmq4.SocketIdentity("dealer")))
			defer client.Close()

			serverPort := <-serverPortChan

			fmt.Printf("Received server port: %d\n", serverPort)

			// Connect to the server
			serverAddress := fmt.Sprintf("tcp://localhost:%d", serverPort)
			err := client.Dial(serverAddress)

			if err != nil {
				fmt.Printf("[ERROR] Failed to connect to the server: %v\n", err)
			}
			Expect(err).To(BeNil())

			fmt.Println("Client connected to server.")

			for i := 0; i < 5; i++ {
				// Send a message to the server
				message := zmq4.NewMsgFrom([][]byte{
					[]byte(fmt.Sprintf("Hello %d", i)), // Content frame
				}...)
				fmt.Printf("Client sending: %s\n", message.Frames[0])

				err := client.Send(message)
				if err != nil {
					fmt.Printf("[ERROR] Failed to send message to server: %v\n", err)
				}

				Expect(err).To(BeNil())

				fmt.Printf("Client sent: %s\n", message.Frames[0])

				// Receive a response from the server
				reply, err := client.Recv()
				if err != nil {
					fmt.Printf("[ERROR] Failed to receive reply from server: %v\n", err)
				}

				Expect(err).To(BeNil())
				Expect(reply).ToNot(BeNil())

				fmt.Printf("Client received: %s\n", reply.Frames[0])
				time.Sleep(250 * time.Millisecond)
			}
		})

		It("Zmq4 Router Server, Router Client", func() {
			serverCtx, serverCancel := context.WithCancel(context.Background())
			defer serverCancel()

			serverPortChan := make(chan int, 1)

			serverFunc := func() {
				// Create a ROUTER socket
				server := zmq4.NewRouter(serverCtx, zmq4.WithID(zmq4.SocketIdentity("server")))
				defer server.Close()

				// Bind the server to an address
				address := "tcp://localhost:0"
				err := server.Listen(address)
				Expect(err).To(BeNil())

				serverPort := server.Addr().(*net.TCPAddr).Port
				fmt.Printf("Server started on port %d, waiting for messages...\n", serverPort)

				serverPortChan <- serverPort

				numReceived := 0

				for numReceived < 5 {
					// Receive a message
					msg, err := server.Recv()
					if err != nil {
						fmt.Printf("[ERROR] Failed to receive message from client: %v\n", err)
					}

					Expect(err).To(BeNil())

					identity := msg.Frames[0]
					content := msg.Frames[1]
					fmt.Printf("Server received: [%s] %s\n", identity, content)

					// Send a response back to the client
					response := zmq4.NewMsgFrom([][]byte{
						identity,                           // Identity frame
						[]byte("Echo: " + string(content)), // Content frame
					}...)

					err = server.Send(response)
					if err != nil {
						fmt.Printf("[ERROR] Failed to send message back to client: %v\n", err)
					}

					Expect(err).To(BeNil())

					numReceived += 1
				}
			}

			go serverFunc()

			// Create a ROUTER socket
			client := zmq4.NewRouter(context.Background(), zmq4.WithID(zmq4.SocketIdentity("client")))
			defer client.Close()

			serverPort := <-serverPortChan

			fmt.Printf("Received server port: %d\n", serverPort)

			// Connect to the server
			serverAddress := fmt.Sprintf("tcp://localhost:%d", serverPort)
			err := client.Dial(serverAddress)

			if err != nil {
				fmt.Printf("[ERROR] Failed to connect to the server: %v\n", err)
			}
			Expect(err).To(BeNil())

			fmt.Println("Client connected to server.")

			for i := 0; i < 5; i++ {
				// Send a message to the server
				message := zmq4.NewMsgFrom([][]byte{
					[]byte("server"),                   // Identity frame
					[]byte(fmt.Sprintf("Hello %d", i)), // Content frame
				}...)
				fmt.Printf("Client sending: %s\n", message.Frames[1])

				err := client.Send(message)
				if err != nil {
					fmt.Printf("[ERROR] Failed to send message to server: %v\n", err)
				}

				Expect(err).To(BeNil())

				fmt.Printf("Client sent: %s\n", message.Frames[1])

				// Receive a response from the server
				reply, err := client.Recv()
				if err != nil {
					fmt.Printf("[ERROR] Failed to receive reply from server: %v\n", err)
				}

				Expect(err).To(BeNil())
				Expect(reply).ToNot(BeNil())

				fmt.Printf("Client received: %s\n", reply.Frames[0])
				time.Sleep(250 * time.Millisecond)
			}
		})
	})

	Context("Processing 'execute_request' messages under static scheduling", func() {
		var (
			kernel           *mock_scheduling.MockKernel
			header           *messaging.MessageHeader
			cluster          *mock_scheduling.MockCluster
			mockScheduler    *mock_scheduling.MockScheduler
			schedulingPolicy scheduling.Policy
		)

		persistentId := uuid.NewString()

		BeforeEach(func() {
			kernel = mock_scheduling.NewMockKernel(mockCtrl)
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			session = mock_scheduling.NewMockUserSession(mockCtrl)
			mockScheduler = mock_scheduling.NewMockScheduler(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			var err error
			schedulingPolicy, err = policy.NewStaticPolicy(scheduling.DefaultStaticSchedulerOptions)
			Expect(err).To(BeNil())
			Expect(schedulingPolicy).ToNot(BeNil())
			Expect(schedulingPolicy.PolicyKey()).To(Equal(scheduling.Static))

			mockScheduler.EXPECT().Policy().Return(schedulingPolicy).AnyTimes()
			mockScheduler.EXPECT().PolicyKey().Return(scheduling.Static).AnyTimes()

			clusterGateway = &ClusterGatewayImpl{
				cluster:           cluster,
				RequestLog:        metrics.NewRequestLog(),
				ClusterStatistics: metrics.NewClusterStatistics(),
			}
			clusterGateway.metricsProvider = metrics.NewClusterMetricsProvider(-1, clusterGateway, clusterGateway.updateClusterStatistics,
				clusterGateway.IncrementResourceCountsForNewHost, clusterGateway.DecrementResourceCountsForRemovedHost)
			config.InitLogger(&clusterGateway.log, clusterGateway)

			kernel.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
			kernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
				Id:              kernelId,
				Session:         kernelId,
				SignatureScheme: signatureScheme,
				Key:             "23d90942-8c3de3a713a5c3611792b7a5",
				ResourceSpec: &proto.ResourceSpec{
					Gpu:    2,
					Cpu:    100,
					Memory: 1000,
					Vram:   1,
				},
			}).AnyTimes()
			kernel.EXPECT().ResourceSpec().Return(&types.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(100),
				MemoryMb:  decimal.NewFromFloat(1000),
			}).AnyTimes()
			kernel.EXPECT().ID().Return(kernelId).AnyTimes()
			cluster.EXPECT().GetSession(kernelId).Return(session, true).AnyTimes()

			header = &messaging.MessageHeader{
				MsgID:    jupyterExecuteRequestId,
				Username: "",
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			kernel.EXPECT().Size().Return(3).AnyTimes()

			setActiveCall := kernel.EXPECT().RegisterActiveExecution(gomock.Any()).Return(nil).AnyTimes()
			kernel.EXPECT().NumActiveExecutionOperations().Return(0).Times(1)
			kernel.EXPECT().NumActiveExecutionOperations().After(setActiveCall).Return(1).AnyTimes()

			cluster.EXPECT().Close().AnyTimes()
		})

		It("should correctly handle execute_request messages via the processExecuteRequest method", func() {
			encodedHeader, err := json.Marshal(header)
			Expect(err).To(BeNil())

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				encodedHeader, /* Header */
				[]byte(""),    /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)

			session.EXPECT().IsTraining().Return(false).MaxTimes(1)
			session.EXPECT().SetExpectingTraining().Return(promise.Resolved(nil)).MaxTimes(1)

			host1 := mock_scheduling.NewMockHost(mockCtrl)
			host1.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host1.EXPECT().GetNodeName().AnyTimes().Return("MockedHost1")

			host2 := mock_scheduling.NewMockHost(mockCtrl)
			host2.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host2.EXPECT().GetNodeName().AnyTimes().Return("MockedHost2")

			host3 := mock_scheduling.NewMockHost(mockCtrl)
			host3.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host3.EXPECT().GetNodeName().AnyTimes().Return("MockedHost3")

			addReplica := func(id int32, kernelId string, persistentId string, host *mock_scheduling.MockHost) (*mock_scheduling.MockKernelReplica, *mock_scheduling.MockKernelContainer) {
				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					SignatureScheme: signatureScheme,
					Key:             "23d90942-8c3de3a713a5c3611792b7a5",
					ResourceSpec: &proto.ResourceSpec{
						Gpu:    2,
						Cpu:    100,
						Memory: 1000,
						Vram:   1,
					},
				}

				container := mock_scheduling.NewMockKernelContainer(mockCtrl)
				container.EXPECT().ReplicaId().Return(id).AnyTimes()
				container.EXPECT().KernelID().Return(kernelId).AnyTimes()
				container.EXPECT().ResourceSpec().Return(types.NewDecimalSpec(100, 1000, 2, 1)).AnyTimes()
				container.EXPECT().Host().Return(host).AnyTimes()

				replica := mock_scheduling.NewMockKernelReplica(mockCtrl)
				replica.EXPECT().ReplicaID().Return(id).AnyTimes()
				replica.EXPECT().ID().Return(kernelId).AnyTimes()
				replica.EXPECT().Container().Return(container).AnyTimes()
				replica.EXPECT().KernelSpec().Return(kernelSpec).AnyTimes()
				replica.EXPECT().Host().Return(host).AnyTimes()
				replica.EXPECT().KernelReplicaSpec().Return(&proto.KernelReplicaSpec{
					Kernel:       kernelSpec,
					NumReplicas:  3,
					Join:         true,
					PersistentId: &persistentId,
					ReplicaId:    id,
				}).AnyTimes()

				return replica, container
			}

			replica1, _ /* container1 */ := addReplica(1, kernelId, persistentId, host1)
			replica2, _ /* container2 */ := addReplica(2, kernelId, persistentId, host2)
			replica3, _ /* container3 */ := addReplica(3, kernelId, persistentId, host3)

			replicas := []*mock_scheduling.MockKernelReplica{replica1, replica2, replica3}

			mockScheduler.EXPECT().FindReadyReplica(kernel, jMsg.JupyterMessageId()).Times(1).DoAndReturn(func(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
				selectedReplica := replicas[2]

				return selectedReplica, nil
			})

			kernel.EXPECT().LastPrimaryReplica().Times(1).Return(nil)

			cluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			targetReplica, err := clusterGateway.processExecuteRequest(jMsg, kernel)
			Expect(targetReplica).ToNot(BeNil())
			Expect(targetReplica.ReplicaID()).To(Equal(int32(3)))
			Expect(err).To(BeNil())
			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))
		})

		It("should correctly handle execute_request messages via the executeRequestHandler method", func() {
			encodedHeader, err := json.Marshal(header)
			Expect(err).To(BeNil())

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				encodedHeader, /* Header */
				[]byte(""),    /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)

			session.EXPECT().IsTraining().Return(false).MaxTimes(1)
			session.EXPECT().SetExpectingTraining().Return(promise.Resolved(nil)).MaxTimes(1)

			host1 := mock_scheduling.NewMockHost(mockCtrl)
			host1.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host1.EXPECT().GetNodeName().AnyTimes().Return("MockedHost1")
			host1.EXPECT().ResourceSpec().AnyTimes().Return(hostSpec)

			host2 := mock_scheduling.NewMockHost(mockCtrl)
			host2.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host2.EXPECT().GetNodeName().AnyTimes().Return("MockedHost2")
			host2.EXPECT().ResourceSpec().AnyTimes().Return(hostSpec)

			host3 := mock_scheduling.NewMockHost(mockCtrl)
			host3.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host3.EXPECT().GetNodeName().AnyTimes().Return("MockedHost3")
			host3.EXPECT().ResourceSpec().AnyTimes().Return(hostSpec)

			hosts := []*mock_scheduling.MockHost{host1, host2, host3}

			addReplica := func(id int32, kernelId string, persistentId string, host *mock_scheduling.MockHost) (*mock_scheduling.MockKernelReplica, *mock_scheduling.MockKernelContainer) {
				resourceSpec := &proto.ResourceSpec{
					Gpu:    2,
					Cpu:    100,
					Memory: 1000,
					Vram:   1,
				}
				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					SignatureScheme: signatureScheme,
					Key:             "23d90942-8c3de3a713a5c3611792b7a5",
					ResourceSpec:    resourceSpec,
				}

				container := mock_scheduling.NewMockKernelContainer(mockCtrl)
				container.EXPECT().ReplicaId().Return(id).AnyTimes()
				container.EXPECT().KernelID().Return(kernelId).AnyTimes()
				container.EXPECT().ResourceSpec().Return(resourceSpec.ToDecimalSpec()).AnyTimes()
				container.EXPECT().Host().Return(host).AnyTimes()

				replica := mock_scheduling.NewMockKernelReplica(mockCtrl)
				replica.EXPECT().ReplicaID().Return(id).AnyTimes()
				replica.EXPECT().ID().Return(kernelId).AnyTimes()
				replica.EXPECT().Container().Return(container).AnyTimes()
				replica.EXPECT().KernelSpec().Return(kernelSpec).AnyTimes()
				replica.EXPECT().Host().Return(host).AnyTimes()
				replica.EXPECT().ResourceSpec().Return(resourceSpec.ToDecimalSpec()).AnyTimes()
				replica.EXPECT().KernelReplicaSpec().Return(&proto.KernelReplicaSpec{
					Kernel:       kernelSpec,
					NumReplicas:  3,
					Join:         true,
					PersistentId: &persistentId,
					ReplicaId:    id,
				}).AnyTimes()

				return replica, container
			}

			replica1, _ /* container1 */ := addReplica(1, kernelId, persistentId, host1)
			replica2, _ /* container2 */ := addReplica(2, kernelId, persistentId, host2)
			replica3, _ /* container3 */ := addReplica(3, kernelId, persistentId, host3)

			replicas := []scheduling.KernelReplica{replica1, replica2, replica3}
			kernel.EXPECT().Replicas().AnyTimes().Return(replicas)
			kernel.EXPECT().ReplicasAreScheduled().AnyTimes().Return(true)
			kernel.EXPECT().DebugMode().AnyTimes().Return(true)

			selectedReplicaChan := make(chan scheduling.KernelReplica)

			var host1IdleGpus, host2IdleGpus, host3IdleGpus atomic.Int64
			var host1CommittedGpus, host2CommittedGpus, host3CommittedGpus atomic.Int64

			// We'll artificially say that Host 3 has 8 idle GPUs, whereas hosts 1 and 2 have less.
			initialIdleGpuValues := map[int32]int64{
				1: 6,
				2: 7,
				3: 8,
			}
			initialCommittedGpuValues := map[int32]int64{
				1: 2,
				2: 1,
				3: 0,
			}

			host1IdleGpus.Store(initialIdleGpuValues[int32(1)])
			host2IdleGpus.Store(initialIdleGpuValues[int32(2)])
			host3IdleGpus.Store(initialIdleGpuValues[int32(3)])
			host1CommittedGpus.Store(initialCommittedGpuValues[int32(1)])
			host2CommittedGpus.Store(initialCommittedGpuValues[int32(2)])
			host3CommittedGpus.Store(initialCommittedGpuValues[int32(3)])

			idleGpus := []*atomic.Int64{&host1IdleGpus, &host2IdleGpus, &host3IdleGpus}
			committedGpus := []*atomic.Int64{&host1CommittedGpus, &host2CommittedGpus, &host3CommittedGpus}

			// Set up all the state management for the mocked hosts.
			for i, host := range hosts {
				hostIndex := i
				host.EXPECT().CommittedGPUs().DoAndReturn(func() float64 {
					hostCommittedGpus := committedGpus[hostIndex]
					return float64(hostCommittedGpus.Load())
				}).AnyTimes()

				host.EXPECT().IdleGPUs().DoAndReturn(func() float64 {
					hostIdleGpus := idleGpus[hostIndex]
					return float64(hostIdleGpus.Load())
				}).AnyTimes()

				currReplica := replicas[hostIndex]
				host.EXPECT().PreCommitResources(currReplica.Container(), jupyterExecuteRequestId).AnyTimes().DoAndReturn(func(container scheduling.KernelContainer, executeId string) error {
					Expect(container.ReplicaId()).To(Equal(currReplica.ReplicaID()))
					Expect(currReplica.Container()).To(Equal(container))

					hostCommittedGpus := committedGpus[hostIndex]
					committed := hostCommittedGpus.Load()
					if committed+int64(container.ResourceSpec().GPU()) > int64(hostSpec.GPU()) {
						return fmt.Errorf("%w: committed GPUs (%d) would exceed spec GPUs (%d)",
							transaction.ErrTransactionFailed, committed, int(hostSpec.GPU()))
					}

					hostIdleGpus := idleGpus[hostIndex]
					idle := hostIdleGpus.Load()
					if idle-int64(container.ResourceSpec().GPU()) < 0 {
						return fmt.Errorf("%w: %w (Idle GPUs = %d)", transaction.ErrTransactionFailed,
							transaction.ErrNegativeResourceCount, idle)
					}

					hostCommittedGpus.Add(int64(container.ResourceSpec().GPU()))
					hostIdleGpus.Add(int64(-1 * container.ResourceSpec().GPU()))

					return nil
				})
			}

			kernel.EXPECT().LastPrimaryReplica().AnyTimes().Return(nil)
			mockScheduler.EXPECT().FindReadyReplica(kernel, jMsg.JupyterMessageId()).Times(1).DoAndReturn(
				func(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
					selectedReplica, err := schedulingPolicy.(scheduler.SchedulingPolicy).FindReadyReplica(kernel, executionId)
					Expect(err).To(BeNil())
					Expect(selectedReplica).ToNot(BeNil())

					selectedReplicaChan <- selectedReplica

					return selectedReplica, nil
				})

			cluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)

			jupyterMessagesChan := make(chan []*messaging.JupyterMessage)

			kernel.EXPECT().RequestWithHandlerAndReplicas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, typ messaging.MessageType,
				jupyterMessages []*messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func(), replicas ...scheduling.KernelReplica) error {

				jupyterMessagesChan <- jupyterMessages

				return nil
			})

			kernel.EXPECT().LastPrimaryReplica().AnyTimes().Return(nil)

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			go func() {
				err = clusterGateway.executeRequestHandler(kernel, jMsg)
				Expect(err).To(BeNil())
			}()

			selectedReplica := <-selectedReplicaChan

			GinkgoWriter.Printf("Selected replica %d on targetReplicaHost %s (ID=%s)\n", selectedReplica.ReplicaID(),
				selectedReplica.Host().GetNodeName(), selectedReplica.Host().GetID())

			Expect(selectedReplica.ReplicaID()).To(Equal(int32(3)))

			jupyterMessages := <-jupyterMessagesChan

			Expect(jupyterMessages).ToNot(BeNil())
			Expect(len(jupyterMessages)).To(Equal(3))

			for idx, msg := range jupyterMessages {
				GinkgoWriter.Printf("Jupyter Message #%d:\n%v\n", idx, msg.StringFormatted())
				Expect(msg.JupyterMessageId()).To(Equal(jupyterExecuteRequestId))

				// We add 1 because replica IDs start at 1.
				if int32(idx+1) == selectedReplica.ReplicaID() {
					Expect(msg.JupyterMessageType()).To(Equal(messaging.ShellExecuteRequest))
				} else {
					Expect(msg.JupyterMessageType()).To(Equal(messaging.ShellYieldRequest))
				}
			}

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))

			targetReplicaHost := hosts[selectedReplica.ReplicaID()-1]

			// Check that the resources were updated correctly on the targetReplicaHost of the selected/target replica.
			Expect(targetReplicaHost.IdleGPUs()).To(Equal(float64(idleGpus[selectedReplica.ReplicaID()-1].Load())))
			Expect(targetReplicaHost.CommittedGPUs()).To(Equal(float64(committedGpus[selectedReplica.ReplicaID()-1].Load())))

			for idx, host := range hosts {
				if replicas[idx].ReplicaID() == selectedReplica.ReplicaID() {
					// The host of the target replica should have changed resource values.
					Expect(host.IdleGPUs()).ToNot(Equal(float64(initialIdleGpuValues[replicas[idx].ReplicaID()])))
					Expect(host.CommittedGPUs()).ToNot(Equal(float64(initialCommittedGpuValues[replicas[idx].ReplicaID()])))

					Expect(host.IdleGPUs()).ToNot(Equal(float64(initialIdleGpuValues[selectedReplica.ReplicaID()])))
					Expect(host.CommittedGPUs()).ToNot(Equal(float64(initialCommittedGpuValues[selectedReplica.ReplicaID()])))
				} else {
					// The other two hosts should not have changed resources values.
					Expect(host.IdleGPUs()).To(Equal(float64(initialIdleGpuValues[replicas[idx].ReplicaID()])))
					Expect(host.CommittedGPUs()).To(Equal(float64(initialCommittedGpuValues[replicas[idx].ReplicaID()])))
				}
			}
		})

		It("should correctly handle execute_request messages with an offset", func() {
			cluster.EXPECT().Scheduler().AnyTimes().Return(mockScheduler)

			reqId := uuid.NewString()
			destReqFrame := fmt.Sprintf("dest.%s.req.%s", kernelId, reqId)

			encodedHeader, err := json.Marshal(header)
			Expect(err).To(BeNil())

			unsignedFrames := [][]byte{
				[]byte(destReqFrame),
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				encodedHeader, /* Header */
				[]byte(""),    /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))

			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)
			Expect(jMsg.RequestId).To(Equal(reqId))
			Expect(jMsg.DestinationId).To(Equal(kernelId))
			Expect(jMsg.Offset()).To(Equal(1))

			session.EXPECT().IsTraining().Return(false).MaxTimes(1)
			session.EXPECT().SetExpectingTraining().Return(promise.Resolved(nil)).MaxTimes(1)

			host1 := mock_scheduling.NewMockHost(mockCtrl)
			host1.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host1.EXPECT().GetNodeName().AnyTimes().Return("MockedHost1")

			host2 := mock_scheduling.NewMockHost(mockCtrl)
			host2.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host2.EXPECT().GetNodeName().AnyTimes().Return("MockedHost2")

			host3 := mock_scheduling.NewMockHost(mockCtrl)
			host3.EXPECT().GetID().AnyTimes().Return(uuid.NewString())
			host3.EXPECT().GetNodeName().AnyTimes().Return("MockedHost3")

			addReplica := func(id int32, kernelId string, persistentId string, host scheduling.Host) (*mock_scheduling.MockKernelReplica, *mock_scheduling.MockKernelContainer) {
				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					SignatureScheme: signatureScheme,
					Key:             "23d90942-8c3de3a713a5c3611792b7a5",
					ResourceSpec: &proto.ResourceSpec{
						Gpu:    2,
						Cpu:    100,
						Memory: 1000,
						Vram:   1,
					},
				}

				container := mock_scheduling.NewMockKernelContainer(mockCtrl)
				container.EXPECT().ReplicaId().Return(id).AnyTimes()
				container.EXPECT().KernelID().Return(kernelId).AnyTimes()
				container.EXPECT().ResourceSpec().Return(types.NewDecimalSpec(100, 1000, 2, 1)).AnyTimes()
				container.EXPECT().Host().Return(host).AnyTimes()

				replica := mock_scheduling.NewMockKernelReplica(mockCtrl)
				replica.EXPECT().ReplicaID().Return(id).AnyTimes()
				replica.EXPECT().ID().Return(kernelId).AnyTimes()
				replica.EXPECT().Container().Return(container).AnyTimes()
				replica.EXPECT().KernelSpec().Return(kernelSpec).AnyTimes()
				replica.EXPECT().Host().Return(host).AnyTimes()
				replica.EXPECT().KernelReplicaSpec().Return(&proto.KernelReplicaSpec{
					Kernel:       kernelSpec,
					NumReplicas:  3,
					Join:         true,
					PersistentId: &persistentId,
					ReplicaId:    id,
				}).AnyTimes()

				return replica, container
			}

			replica1, _ /* container1 */ := addReplica(1, kernelId, persistentId, host1)
			replica2, _ /* container2 */ := addReplica(2, kernelId, persistentId, host2)
			replica3, _ /* container3 */ := addReplica(3, kernelId, persistentId, host3)

			kernel.EXPECT().Replicas().AnyTimes().Return([]scheduling.KernelReplica{replica1, replica2, replica3})
			kernel.EXPECT().ReplicasAreScheduled().Return(true).AnyTimes()

			mockScheduler.EXPECT().FindReadyReplica(kernel, jMsg.JupyterMessageId()).Times(1).DoAndReturn(func(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
				replicas := kernel.Replicas()
				selectedReplicaIndex := rand.Intn(len(replicas))
				selectedReplica := replicas[selectedReplicaIndex]

				return selectedReplica, nil
			})

			kernel.EXPECT().Replicas().AnyTimes().Return([]scheduling.KernelReplica{replica1, replica2, replica3})

			kernel.EXPECT().LastPrimaryReplica().Times(1).Return(nil)

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			targetReplica, err := clusterGateway.processExecuteRequest(jMsg, kernel)
			Expect(targetReplica).ToNot(BeNil())
			Expect(err).To(BeNil())
			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))
		})

		//It("should respond correctly upon receiving three YIELD notifications", func() {
		//
		//})
	})

	Context("Processing general ZMQ Messages", func() {
		var (
			kernel  *mock_scheduling.MockKernel
			header  *messaging.MessageHeader
			cluster *mock_scheduling.MockCluster
		)

		// Signature after signing the message using the header defined below
		// initialSignature := "c34f1638ae4d0ead9ffefa13e91202b74a9d012fee8ee6b55274f29bcc7b5427"

		BeforeEach(func() {
			kernel = mock_scheduling.NewMockKernel(mockCtrl)
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			session = mock_scheduling.NewMockUserSession(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			clusterGateway = &ClusterGatewayImpl{
				cluster:    cluster,
				RequestLog: metrics.NewRequestLog(),
			}
			clusterGateway.metricsProvider = metrics.NewClusterMetricsProvider(-1, clusterGateway, clusterGateway.updateClusterStatistics,
				clusterGateway.IncrementResourceCountsForNewHost, clusterGateway.DecrementResourceCountsForRemovedHost)
			config.InitLogger(&clusterGateway.log, clusterGateway)

			kernel.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
			kernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
				Id:              kernelId,
				Session:         kernelId,
				SignatureScheme: signatureScheme,
				Key:             "23d90942-8c3de3a713a5c3611792b7a5",
				ResourceSpec: &proto.ResourceSpec{
					Gpu:    2,
					Cpu:    100,
					Memory: 1000,
					Vram:   1,
				},
			}).AnyTimes()
			kernel.EXPECT().ResourceSpec().Return(&types.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(100),
				MemoryMb:  decimal.NewFromFloat(1000),
			}).AnyTimes()
			kernel.EXPECT().ID().Return(kernelId).AnyTimes()
			cluster.EXPECT().GetSession(kernelId).Return(session, true).AnyTimes()

			header = &messaging.MessageHeader{
				MsgID:    jupyterExecuteRequestId,
				Username: "",
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			cluster.EXPECT().Close().AnyTimes()
		})

		It("Should embed a RequestTrace struct in the buffers frame of a JupyterMessage in DebugMode", func() {
			reqId := uuid.NewString()
			destReqFrame := fmt.Sprintf("dest.%s.req.%s", kernelId, reqId)

			encodedHeader, err := json.Marshal(header)
			Expect(err).To(BeNil())

			unsignedFrames := [][]byte{
				[]byte(destReqFrame),
				[]byte("<IDS|MSG>"),
				[]byte(""),    /* Signature */
				encodedHeader, /* Header */
				[]byte(""),    /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			frames, err := jFrames.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)
			Expect(jMsg.RequestId).To(Equal(reqId))
			Expect(jMsg.DestinationId).To(Equal(kernelId))
			Expect(jMsg.Offset()).To(Equal(1))

			// We'll just call this multiple times.
			//requestLogHelper := func(server *server.AbstractServer) {
			//	Expect(server.RequestLog.Size()).To(Equal(1))
			//	Expect(server.RequestLog.EntriesByJupyterMsgId.Len()).To(Equal(1))
			//	Expect(server.RequestLog.RequestsPerKernel.Len()).To(Equal(1))
			//}

			// We'll just call this multiple times.
			requestTraceHelper := func(trace *proto.RequestTrace) {
				Expect(trace.MessageId).To(Equal(jupyterExecuteRequestId))
				Expect(trace.MessageType).To(Equal(messaging.ShellExecuteRequest))
				Expect(trace.KernelId).To(Equal(kernelId))

				m, err := json.Marshal(&proto.JupyterRequestTraceFrame{RequestTrace: trace})
				Expect(err).To(BeNil())

				fmt.Printf("jMsg.JupyterFrames[%d+%d]: %s\n", jMsg.Offset(), messaging.JupyterFrameRequestTrace, string(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace]))
				fmt.Printf("Marshalled RequestTrace: %s\n", string(m))
				Expect(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+messaging.JupyterFrameRequestTrace]).To(Equal(m))
			}

			requestReceivedByGateway := int64(257894000000)
			requestReceivedByGatewayTs := time.UnixMilli(requestReceivedByGateway) // 2009-11-10 23:00:00 +0000 UTC
			requestTrace, added, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestReceivedByGatewayTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeTrue())
			Expect(err).To(BeNil())
			// requestLogHelper(abstractServer)
			Expect(jMsg.JupyterFrames.Len()).To(Equal(8))
			Expect(jMsg.JupyterFrames.LenWithoutIdentitiesFrame(false)).To(Equal(7))

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			//requests, loaded := abstractServer.RequestLog.RequestsPerKernel.Load(kernelId)
			//Expect(loaded).To(Equal(true))
			//Expect(requests).ToNot(BeNil())
			//Expect(requests.Len()).To(Equal(1))

			//fmt.Printf("RequestTrace: %s\n", requestTrace.String())
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			requestSentByGateway := requestReceivedByGateway + 1000
			requestSentByGatewayTs := time.UnixMilli(requestSentByGateway)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestSentByGatewayTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			requestReceivedByLocalDaemon := requestSentByGateway + 1000
			requestReceivedByLocalDaemonTs := time.UnixMilli(requestReceivedByLocalDaemon)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestReceivedByLocalDaemonTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			requestSentByLocalDaemon := requestSentByGateway + 1000
			requestSentByLocalDaemonTs := time.UnixMilli(requestSentByLocalDaemon)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestSentByLocalDaemonTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			requestReceivedByKernelReplica := requestSentByGateway + 1000
			requestReceivedByKernelReplicaTs := time.UnixMilli(requestReceivedByKernelReplica)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, requestReceivedByKernelReplicaTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			replySentByKernelReplica := requestSentByGateway + 1000
			replySentByKernelReplicaTs := time.UnixMilli(replySentByKernelReplica)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, replySentByKernelReplicaTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(replySentByKernelReplica))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			replyReceivedByLocalDaemon := requestSentByGateway + 1000
			replyReceivedByLocalDaemonTs := time.UnixMilli(replyReceivedByLocalDaemon)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, replyReceivedByLocalDaemonTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(replySentByKernelReplica))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(replyReceivedByLocalDaemon))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			replySentByLocalDaemon := requestSentByGateway + 1000
			replySentByLocalDaemonTs := time.UnixMilli(replySentByLocalDaemon)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, replySentByLocalDaemonTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(replySentByKernelReplica))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(replyReceivedByLocalDaemon))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(replySentByLocalDaemon))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(proto.DefaultTraceTimingValue))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			replyReceivedByGateway := requestSentByGateway + 1000
			replyReceivedByGatewayTs := time.UnixMilli(replyReceivedByGateway)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, replyReceivedByGatewayTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(replySentByKernelReplica))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(replyReceivedByLocalDaemon))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(replySentByLocalDaemon))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(replyReceivedByGateway))
			Expect(requestTrace.ReplySentByGateway).To(Equal(proto.DefaultTraceTimingValue))

			replySentByGateway := requestSentByGateway + 1000
			replySentByGatewayTs := time.UnixMilli(replySentByGateway)
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, replySentByGatewayTs, abstractServer.Log)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			err = jMsg.JupyterFrames.Verify(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())

			// requestLogHelper(abstractServer)
			requestTraceHelper(requestTrace)
			Expect(requestTrace.RequestReceivedByGateway).To(Equal(requestReceivedByGateway))
			Expect(requestTrace.RequestSentByGateway).To(Equal(requestSentByGateway))
			Expect(requestTrace.RequestReceivedByLocalDaemon).To(Equal(requestReceivedByLocalDaemon))
			Expect(requestTrace.RequestSentByLocalDaemon).To(Equal(requestSentByLocalDaemon))
			Expect(requestTrace.RequestReceivedByKernelReplica).To(Equal(requestReceivedByKernelReplica))
			Expect(requestTrace.ReplySentByKernelReplica).To(Equal(replySentByKernelReplica))
			Expect(requestTrace.ReplyReceivedByLocalDaemon).To(Equal(replyReceivedByLocalDaemon))
			Expect(requestTrace.ReplySentByLocalDaemon).To(Equal(replySentByLocalDaemon))
			Expect(requestTrace.ReplyReceivedByGateway).To(Equal(replyReceivedByGateway))
			Expect(requestTrace.ReplySentByGateway).To(Equal(replySentByGateway))
		})
	})

	Context("Migrating Kernels", func() {
		var (
			mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider
			kernelId                              string
			mockedKernelSpec                      *proto.KernelSpec
			mockedKernel                          *mock_scheduling.MockKernel
			mockedKernelReplica1                  *mock_scheduling.MockKernelReplica
			mockedKernelReplica2                  *mock_scheduling.MockKernelReplica
			mockedKernelReplica3                  *mock_scheduling.MockKernelReplica
			mockedSession                         *mock_scheduling.MockUserSession
			resourceSpec                          *proto.ResourceSpec
			host1, host2, host3, host4            scheduling.Host

			host1Spoofer, host2Spoofer, host3Spoofer, host4Spoofer                             *distNbTesting.ResourceSpoofer
			localGatewayClient1, localGatewayClient2, localGatewayClient3, localGatewayClient4 *mock_proto.MockLocalGatewayClient
		)

		BeforeEach(func() {
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			var options *domain.ClusterGatewayOptions
			err := json.Unmarshal([]byte(GatewayOptsAsJsonString), &options)
			if err != nil {
				panic(err)
			}

			mockedDistributedKernelClientProvider = NewMockedDistributedKernelClientProvider(mockCtrl)

			// fmt.Printf("Gateway options:\n%s\n", options.PrettyString(2))
			clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
				globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
				srv.SetClusterOptions(&options.SchedulerOptions)
				srv.SetDistributedClientProvider(mockedDistributedKernelClientProvider)
			})
			config.InitLogger(&clusterGateway.log, clusterGateway)

			Expect(clusterGateway.cluster).ToNot(BeNil())

			kernelId = "8247310f-7bb1-47ee-b234-a4529bab1274"

			resourceSpec = &proto.ResourceSpec{
				Gpu:    2,
				Vram:   2,
				Cpu:    1250,
				Memory: 2048,
			}

			mockedKernel, mockedKernelSpec = initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec, 3)

			Expect(mockedKernelSpec).ToNot(BeNil())
			mockedKernel.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
			mockedKernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
				Id:              kernelId,
				Session:         kernelId,
				SignatureScheme: signatureScheme,
				Key:             "23d90942-8c3de3a713a5c3611792b7a5",
				WorkloadId:      "SpoofedWorkloadId",
				ResourceSpec: &proto.ResourceSpec{
					Gpu:    2,
					Cpu:    1250,
					Memory: 2048,
					Vram:   2,
				},
			}).AnyTimes()
			mockedKernel.EXPECT().ResourceSpec().Return(&types.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(1250),
				MemoryMb:  decimal.NewFromFloat(2048),
				VRam:      decimal.NewFromFloat(2),
			}).AnyTimes()
			mockedKernel.EXPECT().ID().Return(kernelId).AnyTimes()
			mockedKernel.EXPECT().
				RequestWithHandler(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()

			mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, mockedKernel)

			mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, mockedKernel)

			cluster := clusterGateway.cluster

			host1Id := uuid.NewString()
			node1Name := "TestNode1"
			host1Spoofer = distNbTesting.NewResourceSpoofer(node1Name, host1Id, clusterGateway.hostSpec)
			host1, localGatewayClient1, _ = distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host1Id, node1Name, host1Spoofer)

			host2Id := uuid.NewString()
			node2Name := "TestNode2"
			host2Spoofer = distNbTesting.NewResourceSpoofer(node2Name, host2Id, clusterGateway.hostSpec)
			host2, localGatewayClient2, _ = distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host2Id, node2Name, host2Spoofer)

			host3Id := uuid.NewString()
			node3Name := "TestNode3"
			host3Spoofer = distNbTesting.NewResourceSpoofer(node3Name, host3Id, clusterGateway.hostSpec)
			host3, localGatewayClient3, _ = distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host3Id, node3Name, host3Spoofer)

			err = cluster.NewHostAddedOrConnected(host1)
			Expect(err).To(BeNil())

			err = cluster.NewHostAddedOrConnected(host2)
			Expect(err).To(BeNil())

			err = cluster.NewHostAddedOrConnected(host3)
			Expect(err).To(BeNil())

			localGatewayClient1.EXPECT().PrepareToMigrate(gomock.Any(), gomock.Any()).MaxTimes(1).Return(&proto.PrepareToMigrateResponse{
				KernelId: kernelId,
				Id:       1,
				DataDir:  "./store",
			}, nil)
			localGatewayClient2.EXPECT().PrepareToMigrate(gomock.Any(), gomock.Any()).MaxTimes(1).Return(&proto.PrepareToMigrateResponse{
				KernelId: kernelId,
				Id:       2,
				DataDir:  "./store",
			}, nil)
			localGatewayClient3.EXPECT().PrepareToMigrate(gomock.Any(), gomock.Any()).MaxTimes(1).Return(&proto.PrepareToMigrateResponse{
				KernelId: kernelId,
				Id:       3,
				DataDir:  "./store",
			}, nil)

			hosts := []scheduling.Host{host1, host2, host3}

			By("Correctly registering the first Host")

			// Add first host.
			err = clusterGateway.registerNewHost(host1)

			By("Correctly registering the second Host")

			// Add second host.
			err = clusterGateway.registerNewHost(host2)

			By("Correctly registering the third Host")

			// Add third host.
			err = clusterGateway.registerNewHost(host3)

			var startKernelReplicaCalled sync.WaitGroup
			startKernelReplicaCalled.Add(3)

			startKernelReturnValChan1 := make(chan *proto.KernelConnectionInfo)
			startKernelReturnValChan2 := make(chan *proto.KernelConnectionInfo)
			startKernelReturnValChan3 := make(chan *proto.KernelConnectionInfo)

			localGatewayClient1.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
				GinkgoWriter.Printf("LocalGateway #1 has called spoofed StartKernelReplica\n")

				// defer GinkgoRecover()

				err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)

				startKernelReplicaCalled.Done()

				GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #1 to be passed via channel.\n")
				ret := <-startKernelReturnValChan1

				GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #1: %v\n", ret)

				return ret, nil
			})

			localGatewayClient2.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
				GinkgoWriter.Printf("LocalGateway #2 has called spoofed StartKernelReplica\n")

				// defer GinkgoRecover()

				err := host2Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host2Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)

				startKernelReplicaCalled.Done()

				GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #2 to be passed via channel.\n")
				ret := <-startKernelReturnValChan2

				GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #2: %v\n", ret)

				return ret, nil
			})

			localGatewayClient3.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
				GinkgoWriter.Printf("LocalGateway #3 has called spoofed StartKernelReplica\n")

				// defer GinkgoRecover()

				err := host3Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host3Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)

				startKernelReplicaCalled.Done()

				GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #3 to be passed via channel.\n")
				ret := <-startKernelReturnValChan3

				GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #3: %v\n", ret)

				return ret, nil
			})

			By("Correctly initiating the creation of a new kernel")

			startKernelReturnValChan := make(chan *proto.KernelConnectionInfo)
			go func() {
				connInfo, _ := clusterGateway.StartKernel(context.Background(), mockedKernelSpec)
				startKernelReturnValChan <- connInfo
			}()

			doneChan := make(chan interface{}, 1)
			go func() {
				startKernelReplicaCalled.Wait()
				doneChan <- struct{}{}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			defer cancel()

			select {
			case <-ctx.Done():
				GinkgoWriter.Printf("Timed-out waiting for StartKernelReplica to be called on Local Daemons by Placer.\n")
				Expect(false).To(BeTrue())
			case <-doneChan:
				{
					// Do nothing / continue with the unit test
				}
			}

			By("Correctly handling the KernelConnectionInfo")

			startKernelReturnValChan1 <- &proto.KernelConnectionInfo{
				Ip:              "10.0.0.1",
				Transport:       "tcp",
				ControlPort:     9000,
				ShellPort:       9001,
				StdinPort:       9002,
				HbPort:          9003,
				IopubPort:       9004,
				IosubPort:       9005,
				SignatureScheme: messaging.JupyterSignatureScheme,
				Key:             kernelKey,
			}
			startKernelReturnValChan2 <- &proto.KernelConnectionInfo{
				Ip:              "10.0.0.2",
				Transport:       "tcp",
				ControlPort:     9000,
				ShellPort:       9001,
				StdinPort:       9002,
				HbPort:          9003,
				IopubPort:       9004,
				IosubPort:       9005,
				SignatureScheme: messaging.JupyterSignatureScheme,
				Key:             kernelKey,
			}
			startKernelReturnValChan3 <- &proto.KernelConnectionInfo{
				Ip:              "10.0.0.3",
				Transport:       "tcp",
				ControlPort:     9000,
				ShellPort:       9001,
				StdinPort:       9002,
				HbPort:          9003,
				IopubPort:       9004,
				IosubPort:       9005,
				SignatureScheme: messaging.JupyterSignatureScheme,
				Key:             kernelKey,
			}

			time.Sleep(time.Millisecond * 125)

			var notifyKernelRegisteredCalled sync.WaitGroup
			notifyKernelRegisteredCalled.Add(3)

			By("Correctly notifying that the kernel registered")

			sleepIntervals := make(chan time.Duration, 3)
			notifyKernelRegistered := func(replicaId int32, targetHost scheduling.Host) {
				// defer GinkgoRecover()

				log.Printf("Notifying Gateway that replica %d has registered.\n", replicaId)

				time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
				time.Sleep(<-sleepIntervals)

				ctx := context.WithValue(context.Background(), SkipValidationKey, "true")
				_, _ = clusterGateway.NotifyKernelRegistered(ctx, &proto.KernelRegistrationNotification{
					ConnectionInfo: &proto.KernelConnectionInfo{
						Ip:              "10.0.0.1",
						Transport:       "tcp",
						ControlPort:     9000,
						ShellPort:       9001,
						StdinPort:       9002,
						HbPort:          9003,
						IopubPort:       9004,
						IosubPort:       9005,
						SignatureScheme: messaging.JupyterSignatureScheme,
						Key:             kernelKey,
					},
					KernelId:           kernelId,
					SessionId:          "N/A",
					ReplicaId:          replicaId,
					HostId:             targetHost.GetID(),
					KernelIp:           "10.0.0.1",
					PodOrContainerName: "kernel1pod",
					NodeName:           targetHost.GetNodeName(),
					NotificationId:     uuid.NewString(),
				})

				notifyKernelRegisteredCalled.Done()
			}

			sleepIntervals <- time.Millisecond * 250
			sleepIntervals <- time.Millisecond * 250
			sleepIntervals <- time.Millisecond * 750

			go notifyKernelRegistered(1, host1)
			go notifyKernelRegistered(2, host2)
			go notifyKernelRegistered(3, host3)

			notifyKernelRegisteredCalled.Wait()

			time.Sleep(time.Millisecond * 250)

			var smrReadyCalled sync.WaitGroup
			smrReadyCalled.Add(3)
			callSmrReady := func(replicaId int32) {
				// defer GinkgoRecover()

				time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
				time.Sleep(<-sleepIntervals)

				_, _ = clusterGateway.SmrReady(context.Background(), &proto.SmrReadyNotification{
					KernelId:     kernelId,
					ReplicaId:    replicaId,
					PersistentId: persistentId,
					Address:      "10.0.0.1",
				})

				smrReadyCalled.Done()
			}

			sleepIntervals <- time.Millisecond * 500
			sleepIntervals <- time.Millisecond * 250
			sleepIntervals <- time.Millisecond * 750

			By("Correctly calling SMR ready and handling that correctly")

			go callSmrReady(1)
			go callSmrReady(2)
			go callSmrReady(3)

			smrReadyCalled.Wait()

			<-startKernelReturnValChan

			go func() {
				defer GinkgoRecover()

				if err := clusterGateway.router.Close(); err != nil {
					clusterGateway.log.Error("Failed to cleanly shutdown router because: %v", err)
				}

				// Close the listener
				if clusterGateway.listener != nil {
					if err := clusterGateway.listener.Close(); err != nil {
						clusterGateway.log.Error("Failed to cleanly shutdown listener because: %v", err)
					}
				}
			}()

			mockedKernelReplica1 = mock_scheduling.NewMockKernelReplica(mockCtrl)
			mockedKernelReplica2 = mock_scheduling.NewMockKernelReplica(mockCtrl)
			mockedKernelReplica3 = mock_scheduling.NewMockKernelReplica(mockCtrl)

			mockedSession = mock_scheduling.NewMockUserSession(mockCtrl)
			mockedSession.EXPECT().ID().AnyTimes().Return(kernelId)
			mockedSession.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())

			prepareReplica := func(replica *mock_scheduling.MockKernelReplica, replicaId int32) {
				container := mock_scheduling.NewMockKernelContainer(mockCtrl)
				container.EXPECT().ReplicaId().AnyTimes().Return(replicaId)
				container.EXPECT().KernelID().AnyTimes().Return(kernelId)
				container.EXPECT().ContainerID().AnyTimes().Return(fmt.Sprintf("%s-%d", kernelId, replicaId))
				container.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
				container.EXPECT().String().AnyTimes().Return(fmt.Sprintf("MockedContainer-%d", replicaId))
				container.EXPECT().Host().AnyTimes().Return(hosts[replicaId-1])
				container.EXPECT().Session().AnyTimes().Return(mockedSession)

				mockedSession.EXPECT().GetReplicaContainer(replicaId).AnyTimes().Return(container, true)
				mockedSession.EXPECT().RemoveReplicaById(replicaId).MaxTimes(1).Return(nil)

				shellSocket := messaging.NewSocket(zmq4.NewRouter(context.Background()), 0, messaging.ShellMessage, fmt.Sprintf("SpoofedSocket-Kernel-%s-Replica-%d", kernelId, replicaId))
				replica.EXPECT().Socket(messaging.ShellMessage).AnyTimes().Return(shellSocket)
				replica.EXPECT().KernelSpec().AnyTimes().Return(mockedKernelSpec)
				replica.EXPECT().ReplicaID().AnyTimes().Return(replicaId)
				replica.EXPECT().ID().AnyTimes().Return(kernelId)
				replica.EXPECT().ResourceSpec().AnyTimes().Return(resourceSpec.ToDecimalSpec())
				replica.EXPECT().Container().AnyTimes().Return(container)
				replica.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
				replica.EXPECT().String().AnyTimes().Return("MockedKernelReplica")
				replica.EXPECT().Host().AnyTimes().Return(hosts[replicaId-1])
				replica.EXPECT().ReplicaID().AnyTimes().Return(replicaId)
				replica.EXPECT().PersistentID().AnyTimes().Return(persistentId)
				replica.EXPECT().KernelReplicaSpec().AnyTimes().Return(&proto.KernelReplicaSpec{
					Kernel:                    mockedKernelSpec,
					NumReplicas:               3,
					Join:                      true,
					PersistentId:              &persistentId,
					ReplicaId:                 replicaId,
					DockerModeKernelDebugPort: -1,
					WorkloadId:                "MockedWorkloadId",
					Replicas:                  []string{"10.0.0.1:8000", "10.0.0.3:8000", "10.0.0.2:8000"},
				})
			}

			prepareReplica(mockedKernelReplica1, 1)
			prepareReplica(mockedKernelReplica2, 2)
			prepareReplica(mockedKernelReplica3, 3)

			mockedKernel.EXPECT().Status().AnyTimes().Return(jupyter.KernelStatusRunning)
			mockedKernel.EXPECT().PersistentID().AnyTimes().Return(persistentId)
		})

		AfterEach(func() {
			if err := clusterGateway.router.Close(); err != nil {
				clusterGateway.log.Error("Failed to cleanly shutdown router because: %v", err)
			}

			// Close the listener
			if clusterGateway.listener != nil {
				if err := clusterGateway.listener.Close(); err != nil {
					clusterGateway.log.Error("Failed to cleanly shutdown listener because: %v", err)
				}
			}
		})

		It("Will correctly return an error to the client when a migration fails and an 'execute_request' cannot be handled", func() {
			clusterGateway.SetDistributedClientProvider(&client.DistributedKernelClientProvider{})
			kernel := clusterGateway.DistributedClientProvider.NewDistributedKernelClient(context.Background(), mockedKernelSpec, 3, clusterGateway.id,
				clusterGateway.connectionOptions, uuid.NewString(), clusterGateway.DebugMode, clusterGateway.executionFailed, clusterGateway.executionLatencyCallback,
				clusterGateway.metricsProvider, clusterGateway.notifyDashboard)

			shellSocket, err := kernel.InitializeShellForwarder(clusterGateway.kernelShellHandler)
			Expect(err).To(BeNil())
			Expect(shellSocket).ToNot(BeNil())

			// Overwrite the mocked kernel entry.
			clusterGateway.kernels.Store(kernelId, kernel)

			var (
				mockedKernelReplica1Context context.Context
				mockedKernelReplica2Context context.Context
				mockedKernelReplica3Context context.Context
			)

			// Initially, the replica's context is just a context.Background().
			// But when adding replica to distributed kernel client, a new context will be assigned.
			mockedKernelReplica1ContextCall1 := mockedKernelReplica1.EXPECT().Context().Times(1).Return(context.Background())
			mockedKernelReplica2ContextCall1 := mockedKernelReplica2.EXPECT().Context().Times(1).Return(context.Background())
			mockedKernelReplica3ContextCall1 := mockedKernelReplica3.EXPECT().Context().Times(1).Return(context.Background())

			mockedKernelReplica1.EXPECT().SetContext(gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context) {
				mockedKernelReplica1Context = ctx
			})
			mockedKernelReplica2.EXPECT().SetContext(gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context) {
				mockedKernelReplica2Context = ctx
			})
			mockedKernelReplica3.EXPECT().SetContext(gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context) {
				mockedKernelReplica3Context = ctx
			})

			mockedKernelReplica1.EXPECT().Context().AnyTimes().DoAndReturn(func() context.Context {
				return mockedKernelReplica1Context
			}).AnyTimes().After(mockedKernelReplica1ContextCall1)
			mockedKernelReplica2.EXPECT().Context().AnyTimes().DoAndReturn(func() context.Context {
				return mockedKernelReplica2Context
			}).AnyTimes().After(mockedKernelReplica2ContextCall1)
			mockedKernelReplica3.EXPECT().Context().AnyTimes().DoAndReturn(func() context.Context {
				return mockedKernelReplica3Context
			}).AnyTimes().After(mockedKernelReplica3ContextCall1)

			mockedKernelReplica1.EXPECT().InitializeIOSub(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(handler messaging.MessageHandler, subscriptionTopic string) (*messaging.Socket, error) {
				// Handler is set, so server routing will be started on dialing.
				socket := messaging.NewSocketWithHandler(zmq4.NewSub(mockedKernelReplica1Context), 0, messaging.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s-%d]", kernelId, 1), handler)
				err := socket.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
				Expect(err).To(BeNil())

				return socket, nil
			}).Times(1)
			err = kernel.AddReplica(mockedKernelReplica1, host1)
			Expect(err).To(BeNil())

			mockedKernelReplica2.EXPECT().InitializeIOSub(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(handler messaging.MessageHandler, subscriptionTopic string) (*messaging.Socket, error) {
				// Handler is set, so server routing will be started on dialing.
				socket := messaging.NewSocketWithHandler(zmq4.NewSub(mockedKernelReplica2Context), 0, messaging.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s-%d]", kernelId, 2), handler)
				err := socket.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
				Expect(err).To(BeNil())

				return socket, nil
			}).Times(1)
			err = kernel.AddReplica(mockedKernelReplica2, host2)
			Expect(err).To(BeNil())

			mockedKernelReplica3.EXPECT().InitializeIOSub(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(handler messaging.MessageHandler, subscriptionTopic string) (*messaging.Socket, error) {
				// Handler is set, so server routing will be started on dialing.
				socket := messaging.NewSocketWithHandler(zmq4.NewSub(mockedKernelReplica3Context), 0, messaging.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s-%d]", kernelId, 3), handler)
				err := socket.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
				Expect(err).To(BeNil())

				return socket, nil
			}).Times(1)
			err = kernel.AddReplica(mockedKernelReplica3, host3)
			Expect(err).To(BeNil())

			clusterGateway.cluster.AddSession(kernelId, mockedSession)

			clusterGateway.cluster.DisableScalingOut()

			mockedSession.EXPECT().IsIdle().AnyTimes().Return(true)
			mockedSession.EXPECT().IsTraining().AnyTimes().Return(false)

			// Technically it might want to return true at some point...?
			mockedSession.EXPECT().IsMigrating().AnyTimes().Return(false)
			// Technically it might want to return true at some point...?
			mockedSession.EXPECT().IsStopped().AnyTimes().Return(false)

			mockedSession.EXPECT().SetExpectingTraining().Times(1).Return(promise.Resolved(nil))

			clientShellSocket := messaging.NewSocket(zmq4.NewDealer(context.Background()), 0, messaging.ShellMessage, "ClientShellSocket")

			addressWithPort := fmt.Sprintf("tcp://localhost:%d", shellSocket.Port)
			fmt.Printf("Dialing kernel shell socket at address \"%s\"\n", addressWithPort)
			err = clientShellSocket.Socket.Dial(addressWithPort)
			Expect(err).To(BeNil())

			err = clientShellSocket.Listen("tcp://localhost:0")
			Expect(err).To(BeNil())
			clientShellSocket.Port = clientShellSocket.Addr().(*net.TCPAddr).Port

			unsignedExecuteRequestFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent executeRequestMessageHeader*/
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}

			executeRequestMessageHeader := &messaging.MessageHeader{
				MsgID:    jupyterExecuteRequestId,
				Username: kernelId,
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecuteRequestFrames)
			err = jFrames.EncodeHeader(executeRequestMessageHeader)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)

			loadedKernel, loaded := clusterGateway.kernels.Load(kernelId)
			Expect(loaded).To(BeTrue())
			Expect(loadedKernel).ToNot(BeNil())
			Expect(loadedKernel).To(Equal(kernel))

			mockedKernelReplica1.EXPECT().SentExecuteRequest(gomock.Any()).MaxTimes(1)
			mockedKernelReplica2.EXPECT().SentExecuteRequest(gomock.Any()).MaxTimes(1)
			mockedKernelReplica3.EXPECT().SentExecuteRequest(gomock.Any()).MaxTimes(1)

			mockedKernelReplica1.EXPECT().RequestWithHandlerAndWaitOptionGetter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)
			mockedKernelReplica2.EXPECT().RequestWithHandlerAndWaitOptionGetter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)
			mockedKernelReplica3.EXPECT().RequestWithHandlerAndWaitOptionGetter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)

			err = host1.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
			Expect(err).To(BeNil())
			err = host2.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
			Expect(err).To(BeNil())
			err = host3.AddToCommittedResources(types.NewDecimalSpec(0, 0, 8, 32))
			Expect(err).To(BeNil())

			fmt.Printf("[DEBUG] Sending 'execute_request' message now:\n%v\n", jMsg.StringFormatted())
			err = clientShellSocket.Send(*jMsg.GetZmqMsg())
			Expect(err).To(BeNil())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			respChan := make(chan interface{}, 1)

			go func() {
				zmqMsg, err := clientShellSocket.Recv()
				if err != nil {
					respChan <- err
					return
				}

				respChan <- zmqMsg
			}()

			select {
			case v := <-respChan:
				{
					switch v.(type) {
					case error:
						{
							err = v.(error)
							Fail(err.Error())
						}
					case zmq4.Msg:
						{
							zmqMsg, ok := v.(zmq4.Msg)
							Expect(ok).To(BeTrue())
							Expect(zmqMsg).ToNot(BeNil())

							jMsg := messaging.NewJupyterMessage(&zmqMsg)

							fmt.Printf("Received response to 'execute_request' message:\n%s\n", jMsg.StringFormatted())

							Expect(jMsg.JupyterParentMessageType()).To(Equal(messaging.ShellExecuteRequest))
							Expect(jMsg.JupyterMessageType()).To(Equal(messaging.ShellExecuteReply))
						}
					}
				}
			case <-ctx.Done():
				{
					Fail("Did not receive 'execute_reply' message in time")
				}
			}
		})

		It("Will correctly migrate a kernel replica when using static scheduling", func() {
			hosts := []scheduling.Host{host1, host2, host3}

			mockedKernel.EXPECT().RemoveReplicaByID(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(id int32, remover scheduling.ReplicaRemover, noop bool) (scheduling.Host, error) {
				return hosts[id-1], nil
			})

			host4Id := uuid.NewString()
			node4Name := "TestNode4"
			host4Spoofer = distNbTesting.NewResourceSpoofer(node4Name, host4Id, clusterGateway.hostSpec)
			host4, localGatewayClient4, _ = distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, clusterGateway.cluster, host4Id, node4Name, host4Spoofer)

			err := clusterGateway.cluster.NewHostAddedOrConnected(host4)
			Expect(err).To(BeNil())

			mockedKernelReplicas := []*mock_scheduling.MockKernelReplica{mockedKernelReplica1, mockedKernelReplica2, mockedKernelReplica3}
			mockedGatewayClients := []*mock_proto.MockLocalGatewayClient{localGatewayClient1, localGatewayClient2, localGatewayClient3}

			clusterGateway.cluster.AddSession(kernelId, mockedSession)

			unsignedExecuteRequestFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent executeRequestMessageHeader*/
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}

			executeRequestMessageHeader := &messaging.MessageHeader{
				MsgID:    jupyterExecuteRequestId,
				Username: kernelId,
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecuteRequestFrames)
			err = jFrames.EncodeHeader(executeRequestMessageHeader)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)

			loadedKernel, loaded := clusterGateway.kernels.Load(kernelId)
			Expect(loaded).To(BeTrue())
			Expect(loadedKernel).ToNot(BeNil())
			Expect(loadedKernel).To(Equal(mockedKernel))

			var wg sync.WaitGroup
			wg.Add(1)

			var activeExecution scheduling.Execution
			mockedKernel.EXPECT().RegisterActiveExecution(gomock.Any()).DoAndReturn(func(msg *messaging.JupyterMessage) error {
				Expect(msg).ToNot(BeNil())
				Expect(msg).To(Equal(jMsg))

				executionManager := mockedKernel.GetExecutionManager()
				Expect(executionManager).ToNot(BeNil())

				var err error
				activeExecution, err = executionManager.RegisterExecution(msg)
				wg.Done()

				return err // Nil on success
			}).Times(1)

			fmt.Printf("[DEBUG] Forwarding 'execute_request' message now:\n%v\n", jMsg.StringFormatted())

			mockedKernel.EXPECT().ReplicasAreScheduled().AnyTimes().Return(true)
			mockedKernel.EXPECT().Replicas().Times(2).Return([]scheduling.KernelReplica{mockedKernelReplica1, mockedKernelReplica2, mockedKernelReplica3})
			//mockedKernel.EXPECT().AddDestFrameIfNecessary(gomock.Any()).Times(1).DoAndReturn(func(msg *messaging.JupyterMessage) *messaging.JupyterMessage {
			//	return msg
			//})
			mockedKernel.EXPECT().DebugMode().Times(1).Return(true)

			mockedSession.EXPECT().IsTraining().Times(1).Return(false)
			mockedSession.EXPECT().SetExpectingTraining().Times(1).Return(promise.Resolved(nil))

			// ctx, typ, jupyterMessages, handler, done any, replicas ...any
			mockedKernel.EXPECT().RequestWithHandlerAndReplicas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)

			//mockedKernel.EXPECT().RegisterActiveExecution(jMsg).Times(1).Return(nil, nil)

			mockedKernel.EXPECT().LastPrimaryReplica().AnyTimes().Return(nil)

			var shellHandlerWaitGroup sync.WaitGroup
			shellHandlerWaitGroup.Add(1)
			go func() {
				//defer GinkgoRecover()

				fmt.Printf("[DEBUG] Calling shell handler for \"%s\" message now.", jMsg.JupyterMessageType())
				err = clusterGateway.ShellHandler(nil, jMsg)
				fmt.Printf("[DEBUG] Successfully called shell handler for \"%s\" message now.", jMsg.JupyterMessageType())
				Expect(err).To(BeNil())
				shellHandlerWaitGroup.Done()
			}()

			wg.Wait()
			Expect(activeExecution).ToNot(BeNil())
			Expect(activeExecution.GetExecuteRequestMessageId()).To(Equal(jupyterExecuteRequestId))

			getExecuteReplyMessage := func(id int) *messaging.JupyterMessage {
				unsignedExecuteReplyFrames := [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent executeReplyMessageHeader*/
					[]byte(""), /* Metadata */
					[]byte("{\"status\": \"error\", \"ename\": \"ExecutionYieldError\", \"evalue\": \"kernel replica failed to lead the execution\"}"),
				}

				executeReplyJupterMessageId := "c7074e5b-b90f-44f8-af5d-63201ec3a528"
				executeReplyMessageHeader := &messaging.MessageHeader{
					MsgID:    executeReplyJupterMessageId,
					Username: kernelId,
					Session:  kernelId,
					Date:     "2024-04-03T22:56:52.605Z",
					MsgType:  "execute_reply",
					Version:  "5.2",
				}

				executeReplyJFrames := messaging.NewJupyterFramesFromBytes(unsignedExecuteReplyFrames)
				err := executeReplyJFrames.EncodeParentHeader(executeRequestMessageHeader)
				Expect(err).To(BeNil())
				err = executeReplyJFrames.EncodeHeader(executeReplyMessageHeader)
				Expect(err).To(BeNil())
				frames, _ := executeReplyJFrames.Sign(signatureScheme, []byte(kernelKey))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)

				GinkgoWriter.Printf("Generated Jupyter \"execute_reply\" message:\n%s\n", jMsg.StringFormatted())

				Expect(jMsg.JupyterParentMessageId()).To(Equal(jupyterExecuteRequestId))
				Expect(jMsg.JupyterMessageId()).To(Equal(executeReplyJupterMessageId))

				return jMsg
			}

			execReply1 := getExecuteReplyMessage(1)
			Expect(execReply1).ToNot(BeNil())

			execReply2 := getExecuteReplyMessage(2)
			Expect(execReply2).ToNot(BeNil())

			execReply3 := getExecuteReplyMessage(3)
			Expect(execReply3).ToNot(BeNil())

			shellHandlerWaitGroup.Wait()

			//yieldReason := &messaging.MessageErrorWithYieldReason{
			//	MessageError: &messaging.MessageError{
			//		Status:   messaging.MessageStatusError,
			//		ErrName:  messaging.MessageErrYieldExecution,
			//		ErrValue: messaging.ErrExecutionYielded.Error(),
			//	},
			//	YieldReason: "N/A",
			//}

			preparedReplicaIdChan := make(chan int32, 1)

			mockedKernel.EXPECT().GetReplicaByID(int32(1)).MaxTimes(1).Return(mockedKernelReplica1, nil)
			mockedKernel.EXPECT().GetReplicaByID(int32(2)).MaxTimes(1).Return(mockedKernelReplica2, nil)
			mockedKernel.EXPECT().GetReplicaByID(int32(3)).MaxTimes(1).Return(mockedKernelReplica3, nil)
			mockedKernel.EXPECT().Replicas().Times(2).Return([]scheduling.KernelReplica{mockedKernelReplica1, mockedKernelReplica2, mockedKernelReplica3})
			mockedKernel.EXPECT().AddOperationStarted().Times(1)
			mockedKernel.EXPECT().AddOperationCompleted().Times(1)
			mockedKernel.EXPECT().PrepareNewReplica(persistentId, gomock.Any()).MaxTimes(1).DoAndReturn(func(persistentId string, smrNodeId int32) *proto.KernelReplicaSpec {
				preparedReplicaIdChan <- smrNodeId

				return &proto.KernelReplicaSpec{
					Kernel:       mockedKernel.KernelSpec(),
					NumReplicas:  3,
					Join:         true,
					PersistentId: &persistentId,
					ReplicaId:    smrNodeId,
				}
			}).Times(1)

			var replicaStartedOnHost4WaitGroup sync.WaitGroup
			replicaStartedOnHost4WaitGroup.Add(1)

			startKernelReturnValChan4 := make(chan *proto.KernelConnectionInfo)
			localGatewayClient4.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
				GinkgoWriter.Printf("LocalGateway #4 has called spoofed StartKernelReplica\n")

				err := host4Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host4Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)

				GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #4 to be passed via channel.\n")
				ret := <-startKernelReturnValChan4

				GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #4: %v\n", ret)

				replicaStartedOnHost4WaitGroup.Done()

				return ret, nil
			}).Times(1)

			mockedKernelReplica1.EXPECT().ReceivedExecuteReply(execReply1).Times(1)
			mockedKernel.EXPECT().ReleasePreCommitedResourcesFromReplica(mockedKernelReplica1, gomock.Any()).Times(1).Return(nil)
			executionManager := mockedKernel.GetExecutionManager()
			Expect(executionManager).ToNot(BeNil())

			var yielded bool
			yielded, err = executionManager.HandleExecuteReplyMessage(execReply1, mockedKernelReplica1)
			Expect(errors.Is(err, messaging.ErrExecutionYielded)).To(BeTrue())
			Expect(yielded).To(BeTrue())

			Expect(activeExecution.NumRolesReceived()).To(Equal(1))
			Expect(activeExecution.NumYieldReceived()).To(Equal(1))
			Expect(activeExecution.NumLeadReceived()).To(Equal(0))

			mockedKernelReplica2.EXPECT().ReceivedExecuteReply(execReply2).Times(1)
			mockedKernel.EXPECT().ReleasePreCommitedResourcesFromReplica(mockedKernelReplica2, gomock.Any()).Times(1).Return(nil)
			yielded, err = executionManager.HandleExecuteReplyMessage(execReply2, mockedKernelReplica2)
			Expect(errors.Is(err, messaging.ErrExecutionYielded)).To(BeTrue())
			Expect(yielded).To(BeTrue())

			Expect(activeExecution.NumRolesReceived()).To(Equal(2))
			Expect(activeExecution.NumYieldReceived()).To(Equal(2))
			Expect(activeExecution.NumLeadReceived()).To(Equal(0))

			mockedKernelReplica3.EXPECT().ReceivedExecuteReply(execReply3).Times(1)
			mockedKernel.EXPECT().ReleasePreCommitedResourcesFromReplica(mockedKernelReplica3, gomock.Any()).Times(1).Return(nil)
			mockedKernel.EXPECT().NumActiveMigrationOperations().Times(1).Return(1)

			var handledLastYieldNotificationWaitGroup sync.WaitGroup
			handledLastYieldNotificationWaitGroup.Add(1)

			go func(wg *sync.WaitGroup) {
				yielded, err = executionManager.HandleExecuteReplyMessage(execReply3, mockedKernelReplica3)
				Expect(errors.Is(err, messaging.ErrExecutionYielded)).To(BeTrue())
				Expect(yielded).To(BeTrue())

				handledLastYieldNotificationWaitGroup.Done()
			}(&handledLastYieldNotificationWaitGroup)

			startKernelReturnValChan4 <- &proto.KernelConnectionInfo{
				Ip:              "0.0.0.0",
				Transport:       "tcp",
				ControlPort:     9000,
				ShellPort:       9001,
				StdinPort:       9002,
				HbPort:          9003,
				IopubPort:       9004,
				IosubPort:       9005,
				SignatureScheme: messaging.JupyterSignatureScheme,
				Key:             kernelKey,
			}

			replicaStartedOnHost4WaitGroup.Wait()

			Expect(activeExecution.NumRolesReceived()).To(Equal(3))
			Expect(activeExecution.NumYieldReceived()).To(Equal(3))
			Expect(activeExecution.NumLeadReceived()).To(Equal(0))

			var notifyKernelRegisteredCalled sync.WaitGroup
			notifyKernelRegisteredCalled.Add(1)

			smrNodeIdOfMigratedReplica := <-preparedReplicaIdChan
			Expect(smrNodeIdOfMigratedReplica >= 1 && smrNodeIdOfMigratedReplica <= 3).To(BeTrue())
			replicaBeingMigrated := mockedKernelReplicas[smrNodeIdOfMigratedReplica-1]

			mockedSession.EXPECT().AddReplica(gomock.Any()).Times(1).DoAndReturn(func(container scheduling.KernelContainer) error {
				// Update the container that is returned for the associated mocked kernel replica.
				replicaBeingMigrated.EXPECT().Container().Return(container).AnyTimes()

				return nil
			})

			migratedReplicaChan := make(chan scheduling.KernelReplica, 1)
			mockedKernel.EXPECT().AddReplica(gomock.Any(), host4).Times(1).DoAndReturn(func(r scheduling.KernelReplica, host scheduling.Host) error {
				Expect(host).To(Equal(host4))
				migratedReplicaChan <- r
				return nil
			})
			mockedKernel.EXPECT().GetReadyReplica().Times(1).DoAndReturn(func() scheduling.KernelReplica {
				// Return a mocked kernel replica that is NOT the one that is being migrated.
				var i int32
				for i = 1; i < 4; i++ {
					if i == smrNodeIdOfMigratedReplica {
						continue
					}

					replica := mockedKernelReplicas[i-1]
					replica.EXPECT().IsReady().Times(1).Return(true)

					mockedGatewayClient := mockedGatewayClients[i-1]
					mockedGatewayClient.EXPECT().UpdateReplicaAddr(gomock.Any(), gomock.Any()).Times(1).Return(&proto.Void{}, nil)

					return replica
				}

				panic("Failed to find ready replica of mocked kernel")
			})

			createSocketAndListen := func(name string, typ messaging.MessageType, smrNodeId int32) *messaging.Socket {
				socketName := fmt.Sprintf("MigratedKernel-Router-%s[SmrNodeId=%d]", name, smrNodeId)
				zmqSocket := zmq4.NewRouter(context.Background())
				socket := messaging.NewSocket(zmqSocket, 0, typ, socketName)

				err = socket.Listen(fmt.Sprintf("tcp://:%d", socket.Port))
				Expect(err).To(BeNil())
				socket.Port = socket.Addr().(*net.TCPAddr).Port

				return socket
			}

			// Create sockets and call 'listen' so when Cluster Gateway tries to connect, it succeeds.
			heartbeatSocket := createSocketAndListen("HB", messaging.HBMessage, smrNodeIdOfMigratedReplica)
			controlSocket := createSocketAndListen("Ctrl", messaging.ControlMessage, smrNodeIdOfMigratedReplica)
			shellSocket := createSocketAndListen("Shell", messaging.ShellMessage, smrNodeIdOfMigratedReplica)
			stdinSocket := createSocketAndListen("Stdin", messaging.StdinMessage, smrNodeIdOfMigratedReplica)

			notifyKernelRegistered := func(replicaId int32, targetHost scheduling.Host) {
				log.Printf("Notifying Gateway that replica %d has registered.\n", replicaId)

				time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25) /* 25 - 50 ms */)

				ctx := context.WithValue(context.Background(), SkipValidationKey, "true")
				resp, err := clusterGateway.NotifyKernelRegistered(ctx, &proto.KernelRegistrationNotification{
					ConnectionInfo: &proto.KernelConnectionInfo{
						Ip:              "localhost",
						Transport:       "tcp",
						ControlPort:     int32(controlSocket.Port),
						ShellPort:       int32(shellSocket.Port),
						StdinPort:       int32(stdinSocket.Port),
						HbPort:          int32(heartbeatSocket.Port),
						IopubPort:       9004,
						IosubPort:       9005,
						SignatureScheme: messaging.JupyterSignatureScheme,
						Key:             kernelKey,
					},
					KernelId:           kernelId,
					SessionId:          "N/A",
					ReplicaId:          replicaId,
					HostId:             targetHost.GetID(),
					KernelIp:           "localhost",
					PodOrContainerName: fmt.Sprintf("kernel1replica%dcontainer", replicaId),
					DockerContainerId:  uuid.NewString(),
					NodeName:           targetHost.GetNodeName(),
					NotificationId:     uuid.NewString(),
				})
				Expect(resp).ToNot(BeNil())
				Expect(err).To(BeNil())
				Expect(resp.Id).To(Equal(replicaId))

				notifyKernelRegisteredCalled.Done()
			}

			go notifyKernelRegistered(smrNodeIdOfMigratedReplica, host4)

			notifyKernelRegisteredCalled.Wait()

			migratedKernelReplica := <-migratedReplicaChan
			mockedKernel.EXPECT().GetReplicaByID(smrNodeIdOfMigratedReplica).Times(1).Return(migratedKernelReplica, nil)

			var smrReadyCalled sync.WaitGroup
			smrReadyCalled.Add(1)
			callSmrReady := func(replicaId int32) {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 ms */))

				_, err := clusterGateway.SmrReady(context.Background(), &proto.SmrReadyNotification{
					KernelId:     kernelId,
					ReplicaId:    replicaId,
					PersistentId: persistentId,
					Address:      "localhost",
				})
				Expect(err).To(BeNil())

				smrReadyCalled.Done()
			}

			go func() {
				callSmrReady(smrNodeIdOfMigratedReplica)
			}()

			smrReadyCalled.Wait()

			handledLastYieldNotificationWaitGroup.Wait()

			Expect(host4.NumContainers()).To(Equal(1))

			_ = heartbeatSocket.Close()
			_ = controlSocket.Close()
			_ = shellSocket.Close()
			_ = stdinSocket.Close()
		})
	})

	Context("DockerCluster", func() {
		var (
			lastSpecGpu  = 0.0
			lastSpecCpu  = 0.0
			lastSpecVram = 0.0
			lastSpecMem  = 0.0
		)

		// This is used to check that the ClusterStatistics is reporting the correct resource counts.
		assertClusterResourceCounts := func(stats *metrics.ClusterStatistics, expectDiff bool, clusterSize int) {
			Expect(stats.IdleGPUs).To(Equal(float64(clusterSize) * hostSpec.GPU()))
			Expect(stats.SpecGPUs).To(Equal(float64(clusterSize) * hostSpec.GPU()))
			Expect(stats.PendingGPUs).To(Equal(0.0))
			Expect(stats.CommittedGPUs).To(Equal(0.0))

			Expect(stats.IdleCPUs).To(Equal(float64(clusterSize) * hostSpec.CPU()))
			Expect(stats.SpecCPUs).To(Equal(float64(clusterSize) * hostSpec.CPU()))
			Expect(stats.PendingCPUs).To(Equal(0.0))
			Expect(stats.CommittedCPUs).To(Equal(0.0))

			Expect(stats.IdleVRAM).To(Equal(float64(clusterSize) * hostSpec.VRAM()))
			Expect(stats.SpecVRAM).To(Equal(float64(clusterSize) * hostSpec.VRAM()))
			Expect(stats.PendingVRAM).To(Equal(0.0))
			Expect(stats.CommittedVRAM).To(Equal(0.0))

			Expect(stats.IdleMemory).To(Equal(float64(clusterSize) * hostSpec.MemoryMB()))
			Expect(stats.SpecMemory).To(Equal(float64(clusterSize) * hostSpec.MemoryMB()))
			Expect(stats.PendingMemory).To(Equal(0.0))
			Expect(stats.CommittedMemory).To(Equal(0.0))

			if expectDiff {
				Expect(lastSpecCpu).ToNot(Equal(stats.SpecCPUs))
				Expect(lastSpecMem).ToNot(Equal(stats.SpecMemory))
				Expect(lastSpecGpu).ToNot(Equal(stats.SpecGPUs))
				Expect(lastSpecVram).ToNot(Equal(stats.SpecVRAM))
			} else {
				Expect(lastSpecCpu).To(Equal(stats.SpecCPUs))
				Expect(lastSpecMem).To(Equal(stats.SpecMemory))
				Expect(lastSpecGpu).To(Equal(stats.SpecGPUs))
				Expect(lastSpecVram).To(Equal(stats.SpecVRAM))
			}

			lastSpecCpu = stats.SpecCPUs
			lastSpecMem = stats.SpecMemory
			lastSpecGpu = stats.SpecGPUs
			lastSpecVram = stats.SpecVRAM
		}

		BeforeEach(func() {
			lastSpecGpu = 0.0
			lastSpecCpu = 0.0
			lastSpecVram = 0.0
			lastSpecMem = 0.0
		})

		Context("Initial Connection Period", func() {
			var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider
			var options *domain.ClusterGatewayOptions

			BeforeEach(func() {
				abstractServer = &server.AbstractServer{
					DebugMode: true,
					Log:       config.GetLogger("TestAbstractServer"),
				}

				err := json.Unmarshal([]byte(GatewayOptsAsJsonString), &options)
				if err != nil {
					panic(err)
				}
			})

			It("Will correctly disable hosts once 'INITIAL_CLUSTER_SIZE' hosts have joined.", func() {
				InitialClusterSize := 3
				InitialConnectionTimeSeconds := 3
				InitialConnectionTime := time.Duration(InitialConnectionTimeSeconds) * time.Second

				options.InitialClusterSize = InitialClusterSize
				options.InitialClusterConnectionPeriodSec = InitialConnectionTimeSeconds

				mockedDistributedKernelClientProvider = NewMockedDistributedKernelClientProvider(mockCtrl)

				startTime := time.Now()
				clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
					globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
					srv.SetClusterOptions(&options.ClusterDaemonOptions.SchedulerOptions)
					srv.SetDistributedClientProvider(mockedDistributedKernelClientProvider)
					srv.(*ClusterGatewayImpl).hostSpec = hostSpec
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)

				Expect(clusterGateway.metricsProvider).ToNot(BeNil())
				Expect(clusterGateway.metricsProvider.GetGatewayPrometheusManager()).To(BeNil())
				Expect(clusterGateway.initialClusterSize).To(Equal(InitialClusterSize))
				Expect(clusterGateway.initialConnectionPeriod).To(Equal(InitialConnectionTime))
				Expect(clusterGateway.inInitialConnectionPeriod.Load()).To(Equal(true))

				cluster := clusterGateway.cluster
				index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := cluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := cluster.Scheduler()
				Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

				Expect(cluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				// Make sure the metrics provider is non-nil.
				Expect(cluster.MetricsProvider()).ToNot(BeNil())

				By("Not disabling the first 'InitialClusterSize' Local Daemons that connect to the Cluster Gateway.")

				clusterSize := 0

				assertClusterResourceCounts(clusterGateway.ClusterStatistics, false, clusterSize)

				for i := 0; i < InitialClusterSize; i++ {
					hostId := uuid.NewString()
					hostName := fmt.Sprintf("TestHost%d", i)
					hostSpoofer := distNbTesting.NewResourceSpoofer(hostName, hostId, clusterGateway.hostSpec)
					host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, hostId, hostName, hostSpoofer)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())

					err = clusterGateway.registerNewHost(host)
					Expect(err).To(BeNil())
					clusterSize += 1

					Expect(cluster.Len()).To(Equal(clusterSize))
					Expect(index.Len()).To(Equal(clusterSize))
					Expect(placer.NumHostsInIndex()).To(Equal(clusterSize))
					Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(clusterSize))
					Expect(cluster.NumDisabledHosts()).To(Equal(0))
					Expect(host.Enabled()).To(Equal(true))

					assertClusterResourceCounts(clusterGateway.ClusterStatistics, true, clusterSize)
				}

				Expect(cluster.Len()).To(Equal(InitialClusterSize))
				Expect(cluster.NumDisabledHosts()).To(Equal(0))
				Expect(clusterGateway.inInitialConnectionPeriod.Load()).To(Equal(true))

				By("Disabling any additional Local Daemons that connect to the Cluster Gateway during the Initial Connection Period after the first 'InitialClusterSize' Local Daemons have already connected.")

				numDisabledHosts := 0
				for i := InitialClusterSize; i < InitialClusterSize*2; i++ {
					hostId := uuid.NewString()
					hostName := fmt.Sprintf("TestHost%d", i)
					hostSpoofer := distNbTesting.NewResourceSpoofer(hostName, hostId, clusterGateway.hostSpec)
					host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, hostId, hostName, hostSpoofer)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())

					err = clusterGateway.registerNewHost(host)
					Expect(err).To(BeNil())
					numDisabledHosts += 1

					Expect(cluster.Len()).To(Equal(InitialClusterSize))
					Expect(cluster.Len()).To(Equal(clusterSize))
					Expect(host.Enabled()).To(Equal(false))
					Expect(cluster.NumDisabledHosts()).To(Equal(numDisabledHosts))

					assertClusterResourceCounts(clusterGateway.ClusterStatistics, false, clusterSize)
				}

				timeElapsed := time.Since(startTime)
				timeRemaining := InitialConnectionTime - timeElapsed

				log.Printf("Sleeping for %v (+ 250ms) until 'Initial Connection Period' has ended.\n", timeRemaining)

				// Sleep for the amount of time left in the 'Initial Connection Period',
				// plus a little extra, to be sure.
				time.Sleep(timeRemaining + (time.Millisecond * time.Duration(250)))

				for i := InitialClusterSize * 2; i < InitialClusterSize*3; i++ {
					hostId := uuid.NewString()
					hostName := fmt.Sprintf("TestHost%d", i)
					hostSpoofer := distNbTesting.NewResourceSpoofer(hostName, hostId, clusterGateway.hostSpec)
					host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, hostId, hostName, hostSpoofer)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())

					err = clusterGateway.registerNewHost(host)
					Expect(err).To(BeNil())
					clusterSize += 1

					Expect(cluster.Len()).To(Equal(clusterSize))
					Expect(index.Len()).To(Equal(clusterSize))
					Expect(placer.NumHostsInIndex()).To(Equal(clusterSize))
					Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(clusterSize))
					Expect(cluster.NumDisabledHosts()).To(Equal(numDisabledHosts))
					Expect(host.Enabled()).To(Equal(true))

					assertClusterResourceCounts(clusterGateway.ClusterStatistics, true, clusterSize)
				}
			})

			It("Will correctly disable the 'Initial Connection Time' behavior when the InitialClusterSize config parameter is set to a negative value.", func() {
				InitialClusterSize := -1
				InitialConnectionTimeSeconds := 60
				InitialConnectionTime := time.Duration(InitialConnectionTimeSeconds) * time.Second

				options.InitialClusterSize = InitialClusterSize
				options.InitialClusterConnectionPeriodSec = InitialConnectionTimeSeconds

				mockedDistributedKernelClientProvider = NewMockedDistributedKernelClientProvider(mockCtrl)

				clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
					globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
					srv.SetClusterOptions(&options.SchedulerOptions)
					srv.SetDistributedClientProvider(mockedDistributedKernelClientProvider)
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)

				Expect(clusterGateway.metricsProvider).ToNot(BeNil())
				Expect(clusterGateway.metricsProvider.GetGatewayPrometheusManager()).To(BeNil())
				Expect(clusterGateway.initialClusterSize).To(Equal(InitialClusterSize))
				Expect(clusterGateway.initialConnectionPeriod).To(Equal(InitialConnectionTime))
				Expect(clusterGateway.inInitialConnectionPeriod.Load()).To(Equal(false))
			})
		})

		Context("Autoscaling", func() {
			var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider
			var options *domain.ClusterGatewayOptions

			BeforeEach(func() {
				abstractServer = &server.AbstractServer{
					DebugMode: true,
					Log:       config.GetLogger("TestAbstractServer"),
				}

				err := json.Unmarshal([]byte(GatewayOptsAsJsonString), &options)
				if err != nil {
					panic(err)
				}
			})

			It("Will correctly and automatically scale-out", func() {
				MinimumNumNodes := 4
				ScalingBufferSize := 3
				InitialClusterSize := 4
				NumHostsToCreate := 10
				InitialConnectionTimeSeconds := 1
				InitialConnectionTime := time.Duration(InitialConnectionTimeSeconds) * time.Second

				Hosts := make([]scheduling.Host, 0)

				// Relatively quick, but long enough that we can see individual scale-outs.
				MeanScaleInPerHostSec := 1.0
				MeanScaleOutPerHostSec := 1.0

				options.ScalingIntervalSec = 1
				options.InitialClusterSize = InitialClusterSize
				options.InitialClusterConnectionPeriodSec = InitialConnectionTimeSeconds
				options.MinimumNumNodes = MinimumNumNodes
				options.ScalingBufferSize = ScalingBufferSize
				options.MeanScaleInPerHostSec = MeanScaleInPerHostSec
				options.MeanScaleOutPerHostSec = MeanScaleOutPerHostSec

				mockedDistributedKernelClientProvider = NewMockedDistributedKernelClientProvider(mockCtrl)

				clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
					globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
					srv.SetClusterOptions(&options.ClusterDaemonOptions.SchedulerOptions)
					srv.SetDistributedClientProvider(mockedDistributedKernelClientProvider)
					srv.(*ClusterGatewayImpl).hostSpec = hostSpec
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)

				Expect(clusterGateway.metricsProvider).ToNot(BeNil())
				Expect(clusterGateway.metricsProvider.GetGatewayPrometheusManager()).To(BeNil())
				Expect(clusterGateway.initialClusterSize).To(Equal(InitialClusterSize))
				Expect(clusterGateway.initialConnectionPeriod).To(Equal(InitialConnectionTime))
				Expect(clusterGateway.inInitialConnectionPeriod.Load()).To(Equal(true))

				dockerCluster := clusterGateway.cluster
				index, ok := dockerCluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := dockerCluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := dockerCluster.Scheduler()
				Expect(scheduler.Placer()).To(Equal(dockerCluster.Placer()))

				Expect(dockerCluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				// Make sure the metrics provider is non-nil.
				Expect(dockerCluster.MetricsProvider()).ToNot(BeNil())

				By("Not disabling the first 'InitialClusterSize' Local Daemons that connect to the Cluster Gateway.")

				clusterSize := 0
				numDisabledHosts := 0

				assertClusterResourceCounts(clusterGateway.ClusterStatistics, false, clusterSize)

				createHost := func(i int) scheduling.Host {
					hostId := uuid.NewString()
					hostName := fmt.Sprintf("TestHost%d", i)
					hostSpoofer := distNbTesting.NewResourceSpoofer(hostName, hostId, clusterGateway.hostSpec)
					host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, dockerCluster, hostId, hostName, hostSpoofer)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())

					err = clusterGateway.registerNewHost(host)
					Expect(err).To(BeNil())

					Hosts = append(Hosts, host)

					return host
				}

				By("Creating all of the initial-size hosts")

				for i := 0; i < InitialClusterSize; i++ {
					host := createHost(i)
					clusterSize += 1

					Expect(dockerCluster.Len()).To(Equal(clusterSize))
					Expect(index.Len()).To(Equal(clusterSize))
					Expect(placer.NumHostsInIndex()).To(Equal(clusterSize))
					Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(clusterSize))
					Expect(dockerCluster.NumDisabledHosts()).To(Equal(numDisabledHosts))
					Expect(host.Enabled()).To(Equal(true))

					assertClusterResourceCounts(clusterGateway.ClusterStatistics, true, clusterSize)
				}

				By("Creating additional hosts that are added as disabled hosts")

				for i := InitialClusterSize; i < NumHostsToCreate; i++ {
					host := createHost(i)
					numDisabledHosts += 1

					Expect(dockerCluster.Len()).To(Equal(clusterSize))
					Expect(index.Len()).To(Equal(clusterSize))
					Expect(placer.NumHostsInIndex()).To(Equal(clusterSize))
					Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(clusterSize))
					Expect(dockerCluster.NumDisabledHosts()).To(Equal(numDisabledHosts))
					Expect(host.Enabled()).To(Equal(false))

					assertClusterResourceCounts(clusterGateway.ClusterStatistics, false, clusterSize)
				}

				// The initial connection period should elapse.
				Eventually(func() bool {
					return clusterGateway.inInitialConnectionPeriod.Load()
				}, time.Duration(float64(time.Millisecond*InitialConnectionTime)*1.5), time.Millisecond*50).
					Should(BeFalse())

				Expect(dockerCluster.MeanScaleOutTime()).To(Equal(time.Millisecond * time.Duration(MeanScaleOutPerHostSec*1000)))
				Expect(dockerCluster.MeanScaleInTime()).To(Equal(time.Millisecond * time.Duration(MeanScaleInPerHostSec*1000)))

				Expect(dockerCluster.Scheduler().MinimumCapacity()).To(Equal(int32(MinimumNumNodes)))
				Expect(dockerCluster.Scheduler().Policy().ScalingConfiguration().ScalingBufferSize).To(Equal(int32(ScalingBufferSize)))
				Expect(dockerCluster.Len()).To(Equal(clusterSize))
				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize))

				// Scaling won't happen unless there's at least one busy GPU.
				mockSession := mock_scheduling.NewMockUserSession(mockCtrl)
				spec := types.NewDecimalSpec(0, 0, 1, 0)
				mockSession.EXPECT().ResourceSpec().AnyTimes().Return(spec)
				sessionId := uuid.NewString()
				mockSession.EXPECT().ID().AnyTimes().Return(sessionId)
				mockSession.EXPECT().IsIdle().AnyTimes().Return(false)
				mockSession.EXPECT().IsTraining().AnyTimes().Return(true)
				// Technically it might want to return true at some point...?
				mockSession.EXPECT().IsMigrating().AnyTimes().Return(false)
				// Technically it might want to return true at some point...?
				mockSession.EXPECT().IsStopped().AnyTimes().Return(false)
				dockerCluster.AddSession(uuid.NewString(), mockSession)
				err := Hosts[0].AddToCommittedResources(spec)
				Expect(err).To(BeNil())

				By("Scaling out")

				// Now, we should scale out. The minimum dockerCluster size is set to 4,
				// and the scaling buffer is set to 3, so the minimum number of hosts
				// that we should have is 7. We only have 4 right now.
				Eventually(func() int {
					return dockerCluster.Len()
				}, time.Second*time.Duration(10), time.Millisecond*50).
					Should(Equal(clusterSize + 1))

				clusterSize += 1
				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 1))
				Expect(clusterSize).To(Equal(InitialClusterSize + 1))

				By("Scaling out again")

				// We should scale out again. The minimum dockerCluster size is set to 4,
				// and the scaling buffer is set to 3, so the minimum number of hosts
				// that we should have is 7. We only have 5 right now.
				Eventually(func() int {
					return dockerCluster.Len()
				}, time.Second*time.Duration(5*MeanScaleOutPerHostSec), time.Millisecond*50).
					Should(Equal(clusterSize + 1))

				clusterSize += 1
				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 2))
				Expect(clusterSize).To(Equal(InitialClusterSize + 2))

				By("Scaling out yet again")

				// We should scale out again. The minimum dockerCluster size is set to 4,
				// and the scaling buffer is set to 3, so the minimum number of hosts
				// that we should have is 7. We only have 6 right now.
				Eventually(func() int {
					return dockerCluster.Len()
				}, time.Second*time.Duration(5*MeanScaleOutPerHostSec), time.Millisecond*50).
					Should(Equal(clusterSize + 1))

				clusterSize += 1
				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 3))
				Expect(clusterSize).To(Equal(InitialClusterSize + 3))

				By("Not scaling out again")

				// Now we have 7 hosts, so we shouldn't scale-out again.
				time.Sleep(time.Second * 2)

				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 3))
				Expect(clusterSize).To(Equal(InitialClusterSize + 3))
				Expect(dockerCluster.HasActiveScalingOperation()).To(BeFalse())

				// Now, let's artificially increase the number of committed GPUs on each host.
				// The formula for scaling out is:
				// <Scaled Out Number of Hosts> = ⌈ (<Cluster Committed GPUs> x <Scale Factor>) / <GPUs Per Host> ⌉
				//
				// We want <Scaled Out Number of Hosts> to equal 8.
				//
				// <Scale Factor> is set to 1.10 in the configuration and <GPUs Per Host> is 8.
				//
				// Therefore, we have:
				// 8 = ⌈ 1.10x / 8 ⌉
				//
				// Which is actually an inequality:
				//
				// 7 < 1.10x / 8 <= 8
				// 56 < 1.10x <= 64
				// 50.9 < x <= 58.18
				//
				// So, we need at least 50.9 committed GPUs to trigger a scale-out to 8 nodes.

				var lastHost scheduling.Host
				dockerCluster.RangeOverHosts(func(key string, host scheduling.Host) bool {
					// Add 7-<Current Committed>, since one of the hosts already had 1 committed GPU.
					spec := types.NewDecimalSpec(0, 0, 7-host.CommittedGPUs(), 0)
					err := host.AddToCommittedResources(spec)
					Expect(err).To(BeNil())

					lastHost = host

					return true
				})

				// Cluster GPU load is 49, which is less than 50.9.
				// We still shouldn't scale out.
				time.Sleep(time.Second * 3)

				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 3))
				Expect(clusterSize).To(Equal(InitialClusterSize + 3))
				Expect(dockerCluster.HasActiveScalingOperation()).To(BeFalse())

				err = lastHost.AddToCommittedResources(types.NewDecimalSpec(0, 0, 1, 0))
				Expect(err).To(BeNil())

				// Cluster GPU load is 50, which is less than 50.9.
				// We still shouldn't scale out.
				time.Sleep(time.Second * 3)

				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 3))
				Expect(clusterSize).To(Equal(InitialClusterSize + 3))
				Expect(dockerCluster.HasActiveScalingOperation()).To(BeFalse())

				err = lastHost.AddToCommittedResources(types.NewDecimalSpec(0, 0, 1, 0))
				Expect(err).To(BeNil())

				// Cluster GPU load is 51, which is greater than 50.9.
				// We SHOULD scale out now.
				Eventually(func() int {
					return dockerCluster.Len()
				}, time.Second*time.Duration(5*MeanScaleOutPerHostSec), time.Millisecond*50).
					Should(Equal(clusterSize + 1))

				clusterSize += 1
				Expect(dockerCluster.Len()).To(Equal(InitialClusterSize + 4))
				Expect(clusterSize).To(Equal(InitialClusterSize + 4))

				time.Sleep(time.Second * 3)

				dockerCluster.RangeOverHosts(func(key string, host scheduling.Host) bool {
					// Remove all committed GPUs from all hosts.
					spec := types.NewDecimalSpec(0, 0, host.CommittedGPUs(), 0)
					err := host.SubtractFromCommittedResources(spec)
					Expect(err).To(BeNil())

					lastHost = host

					return true
				})

				// Except make sure there's 1 host with committed resources.
				err = lastHost.AddToCommittedResources(types.NewDecimalSpec(0, 0, 1, 0))

				// Now we should scale back in...
				Expect(err).To(BeNil())
				Eventually(func() int {
					return dockerCluster.Len()
				}, time.Second*10, time.Millisecond*50).
					Should(Equal(clusterSize - 1))
			})
		})

		Context("Scheduling Kernels", func() {
			var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider

			BeforeEach(func() {
				abstractServer = &server.AbstractServer{
					DebugMode: true,
					Log:       config.GetLogger("TestAbstractServer"),
				}

				var options *domain.ClusterGatewayOptions
				err := json.Unmarshal([]byte(GatewayOptsAsJsonString), &options)
				if err != nil {
					panic(err)
				}

				mockedDistributedKernelClientProvider = NewMockedDistributedKernelClientProvider(mockCtrl)

				// fmt.Printf("Gateway options:\n%s\n", options.PrettyString(2))
				clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
					globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
					srv.SetClusterOptions(&options.SchedulerOptions)
					srv.SetDistributedClientProvider(mockedDistributedKernelClientProvider)
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)

				Expect(clusterGateway.metricsProvider).ToNot(BeNil())
				Expect(clusterGateway.metricsProvider.GetGatewayPrometheusManager()).To(BeNil())
			})

			AfterEach(func() {
				if err := clusterGateway.router.Close(); err != nil {
					clusterGateway.log.Error("Failed to cleanly shutdown router because: %v", err)
				}

				// Close the listener
				if clusterGateway.listener != nil {
					if err := clusterGateway.listener.Close(); err != nil {
						clusterGateway.log.Error("Failed to cleanly shutdown listener because: %v", err)
					}
				}
			})

			It("Will correctly schedule a new kernel", func() {
				kernelId := uuid.NewString()

				resourceSpec := &proto.ResourceSpec{
					Gpu:    2,
					Vram:   2,
					Cpu:    1250,
					Memory: 2048,
				}

				kernel, kernelSpec := initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec, 3)
				mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, kernel)

				mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, kernel)

				cluster := clusterGateway.cluster
				index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := cluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := cluster.Scheduler()
				Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

				Expect(cluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				host1Id := uuid.NewString()
				node1Name := "TestNode1"
				host1Spoofer := distNbTesting.NewResourceSpoofer(node1Name, host1Id, clusterGateway.hostSpec)
				host1, localGatewayClient1, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host1Id, node1Name, host1Spoofer)
				Expect(err).To(BeNil())

				host2Id := uuid.NewString()
				node2Name := "TestNode2"
				host2Spoofer := distNbTesting.NewResourceSpoofer(node2Name, host2Id, clusterGateway.hostSpec)
				host2, localGatewayClient2, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host2Id, node2Name, host2Spoofer)
				Expect(err).To(BeNil())

				host3Id := uuid.NewString()
				node3Name := "TestNode3"
				host3Spoofer := distNbTesting.NewResourceSpoofer(node3Name, host3Id, clusterGateway.hostSpec)
				host3, localGatewayClient3, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host3Id, node3Name, host3Spoofer)
				Expect(err).To(BeNil())

				By("Correctly registering the first Host")

				// Add first host.
				err = clusterGateway.registerNewHost(host1)
				Expect(err).To(BeNil())

				Expect(cluster.Len()).To(Equal(1))
				Expect(index.Len()).To(Equal(1))
				Expect(placer.NumHostsInIndex()).To(Equal(1))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(1))

				By("Correctly registering the second Host")

				// Add second host.
				err = clusterGateway.registerNewHost(host2)
				Expect(err).To(BeNil())

				Expect(cluster.Len()).To(Equal(2))
				Expect(index.Len()).To(Equal(2))
				Expect(placer.NumHostsInIndex()).To(Equal(2))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(2))

				By("Correctly registering the third Host")

				// Add third host.
				err = clusterGateway.registerNewHost(host3)
				Expect(err).To(BeNil())

				Expect(cluster.Len()).To(Equal(3))
				Expect(index.Len()).To(Equal(3))
				Expect(placer.NumHostsInIndex()).To(Equal(3))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(3))

				var startKernelReplicaCalled sync.WaitGroup
				startKernelReplicaCalled.Add(3)

				startKernelReturnValChan1 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan2 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan3 := make(chan *proto.KernelConnectionInfo)

				localGatewayClient1.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #1 has called spoofed StartKernelReplica\n")

					// defer GinkgoRecover()

					err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)
					Expect(err).To(BeNil())

					startKernelReplicaCalled.Done()

					GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #1 to be passed via channel.\n")
					ret := <-startKernelReturnValChan1

					GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #1: %v\n", ret)

					return ret, nil
				})

				localGatewayClient2.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #2 has called spoofed StartKernelReplica\n")

					// defer GinkgoRecover()

					err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)
					Expect(err).To(BeNil())

					startKernelReplicaCalled.Done()

					GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #2 to be passed via channel.\n")
					ret := <-startKernelReturnValChan2

					GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #2: %v\n", ret)

					return ret, nil
				})

				localGatewayClient3.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #3 has called spoofed StartKernelReplica\n")

					// defer GinkgoRecover()

					err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after adding to from pending resources: %v\n", err)
					Expect(err).To(BeNil())

					startKernelReplicaCalled.Done()

					GinkgoWriter.Printf("Waiting for return value for spoofed StartKernelReplica call for mocked LocalGatewayClient #3 to be passed via channel.\n")
					ret := <-startKernelReturnValChan3

					GinkgoWriter.Printf("Returning value from spoofed StartKernelReplica call for mocked LocalGatewayClient #3: %v\n", ret)

					return ret, nil
				})

				By("Correctly initiating the creation of a new kernel")

				startKernelReturnValChan := make(chan *proto.KernelConnectionInfo)
				go func() {
					// defer GinkgoRecover()

					connInfo, err := clusterGateway.StartKernel(context.Background(), kernelSpec)
					Expect(err).To(BeNil())
					Expect(connInfo).ToNot(BeNil())

					startKernelReturnValChan <- connInfo
				}()

				doneChan := make(chan interface{}, 1)
				go func() {
					startKernelReplicaCalled.Wait()
					doneChan <- struct{}{}
				}()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				select {
				case <-ctx.Done():
					GinkgoWriter.Printf("Timed-out waiting for StartKernelReplica to be called on Local Daemons by Placer.\n")
					Expect(false).To(BeTrue())
				case <-doneChan:
					{
						// Do nothing / continue with the unit test
					}
				}

				By("Correctly handling the KernelConnectionInfo")

				startKernelReturnValChan1 <- &proto.KernelConnectionInfo{
					Ip:              "10.0.0.1",
					Transport:       "tcp",
					ControlPort:     9000,
					ShellPort:       9001,
					StdinPort:       9002,
					HbPort:          9003,
					IopubPort:       9004,
					IosubPort:       9005,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
				}
				startKernelReturnValChan2 <- &proto.KernelConnectionInfo{
					Ip:              "10.0.0.2",
					Transport:       "tcp",
					ControlPort:     9000,
					ShellPort:       9001,
					StdinPort:       9002,
					HbPort:          9003,
					IopubPort:       9004,
					IosubPort:       9005,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
				}
				startKernelReturnValChan3 <- &proto.KernelConnectionInfo{
					Ip:              "10.0.0.3",
					Transport:       "tcp",
					ControlPort:     9000,
					ShellPort:       9001,
					StdinPort:       9002,
					HbPort:          9003,
					IopubPort:       9004,
					IosubPort:       9005,
					SignatureScheme: messaging.JupyterSignatureScheme,
					Key:             kernelKey,
				}

				// Ensure that the resource counts of the hosts are correct.
				Expect(host1.NumContainers()).To(Equal(0))
				Expect(host1.NumReservations()).To(Equal(1))
				Expect(host1.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec()))
				Expect(host1.IdleResources().Equals(host1.ResourceSpec()))
				Expect(host1.CommittedResources().IsZero()).To(BeTrue())

				Expect(host2.NumContainers()).To(Equal(0))
				Expect(host2.NumReservations()).To(Equal(1))
				Expect(host2.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec()))
				Expect(host2.IdleResources().Equals(host2.ResourceSpec()))
				Expect(host2.CommittedResources().IsZero()).To(BeTrue())

				Expect(host3.NumContainers()).To(Equal(0))
				Expect(host3.NumReservations()).To(Equal(1))
				Expect(host3.PendingResources().Equals(kernelSpec.DecimalSpecFromKernelSpec()))
				Expect(host3.IdleResources().Equals(host3.ResourceSpec()))
				Expect(host3.CommittedResources().IsZero()).To(BeTrue())

				time.Sleep(time.Millisecond * 500)

				var notifyKernelRegisteredCalled sync.WaitGroup
				notifyKernelRegisteredCalled.Add(3)

				By("Correctly notifying that the kernel registered")

				sleepIntervals := make(chan time.Duration, 3)
				notifyKernelRegistered := func(replicaId int32, targetHost scheduling.Host) {
					// defer GinkgoRecover()

					log.Printf("Notifying Gateway that replica %d has registered.\n", replicaId)

					time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
					time.Sleep(<-sleepIntervals)

					ctx := context.WithValue(context.Background(), SkipValidationKey, "true")
					resp, err := clusterGateway.NotifyKernelRegistered(ctx, &proto.KernelRegistrationNotification{
						ConnectionInfo: &proto.KernelConnectionInfo{
							Ip:              "0.0.0.0",
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: messaging.JupyterSignatureScheme,
							Key:             kernelKey,
						},
						KernelId:           kernelId,
						SessionId:          "N/A",
						ReplicaId:          replicaId,
						HostId:             targetHost.GetID(),
						KernelIp:           "0.0.0.0",
						PodOrContainerName: "kernel1pod",
						NodeName:           targetHost.GetNodeName(),
						NotificationId:     uuid.NewString(),
					})
					Expect(resp).ToNot(BeNil())
					Expect(err).To(BeNil())
					Expect(resp.Id).To(Equal(replicaId))

					notifyKernelRegisteredCalled.Done()
				}

				sleepIntervals <- time.Millisecond * 250
				sleepIntervals <- time.Millisecond * 250
				sleepIntervals <- time.Millisecond * 750

				go notifyKernelRegistered(1, host1)
				go notifyKernelRegistered(2, host2)
				go notifyKernelRegistered(3, host3)

				notifyKernelRegisteredCalled.Wait()

				time.Sleep(time.Millisecond * 250)

				var smrReadyCalled sync.WaitGroup
				smrReadyCalled.Add(3)
				callSmrReady := func(replicaId int32) {
					// defer GinkgoRecover()

					time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
					time.Sleep(<-sleepIntervals)

					_, err := clusterGateway.SmrReady(context.Background(), &proto.SmrReadyNotification{
						KernelId:     kernelId,
						ReplicaId:    replicaId,
						PersistentId: persistentId,
						Address:      "0.0.0.0",
					})
					Expect(err).To(BeNil())

					smrReadyCalled.Done()
				}

				sleepIntervals <- time.Millisecond * 500
				sleepIntervals <- time.Millisecond * 250
				sleepIntervals <- time.Millisecond * 750

				By("Correctly calling SMR ready and handling that correctly")

				go callSmrReady(1)
				go callSmrReady(2)
				go callSmrReady(3)

				smrReadyCalled.Wait()

				connInfo := <-startKernelReturnValChan
				Expect(connInfo).ToNot(BeNil())

				go func() {
					defer GinkgoRecover()

					if err := clusterGateway.router.Close(); err != nil {
						clusterGateway.log.Error("Failed to cleanly shutdown router because: %v", err)
					}

					// Close the listener
					if clusterGateway.listener != nil {
						if err := clusterGateway.listener.Close(); err != nil {
							clusterGateway.log.Error("Failed to cleanly shutdown listener because: %v", err)
						}
					}
				}()

				Expect(host1.PendingResources().Equals(kernelSpec.ResourceSpec.ToDecimalSpec()))
				Expect(host1.IdleResources().Equals(host1.ResourceSpec()))
				Expect(host1.CommittedResources().IsZero()).To(BeTrue())
			})

			It("Will correctly schedule multiple kernel replicas at the same time", func() {
				numKernels := 3
				numHosts := 3

				kernels := make(map[string]*mock_scheduling.MockKernel)
				kernelSpecs := make(map[string]*proto.KernelSpec)

				kernelsByIdx := make(map[int]*mock_scheduling.MockKernel)
				kernelSpecsByIdx := make(map[int]*proto.KernelSpec)

				resourceSpec := &proto.ResourceSpec{
					Gpu:    2,
					Vram:   2,
					Cpu:    1250,
					Memory: 2048,
				}

				for i := 0; i < numKernels; i++ {
					kernelId := uuid.NewString()
					kernelKey := uuid.NewString()
					kernel, kernelSpec := initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec, 3)
					mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, kernel)

					kernels[kernelId] = kernel
					kernelSpecs[kernelId] = kernelSpec

					kernelsByIdx[i] = kernel
					kernelSpecsByIdx[i] = kernelSpec
				}

				hosts := make(map[int]scheduling.Host)
				localGatewayClients := make(map[int]*mock_proto.MockLocalGatewayClient)
				resourceSpoofers := make(map[int]*distNbTesting.ResourceSpoofer)

				cluster := clusterGateway.cluster
				index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := cluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := cluster.Scheduler()
				Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

				Expect(cluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				for i := 0; i < numHosts; i++ {
					host, localGatewayClient, resourceSpoofer, err := addHost(i, clusterGateway, mockCtrl)
					Expect(err).To(BeNil())
					Expect(host).ToNot(BeNil())
					Expect(localGatewayClient).ToNot(BeNil())
					Expect(resourceSpoofer).ToNot(BeNil())

					hosts[i] = host
					localGatewayClients[i] = localGatewayClient
					resourceSpoofers[i] = resourceSpoofer
				}

				size := 0
				for i, host := range hosts {
					By(fmt.Sprintf("Correctly registering Host %d (%d/%d)", i, size+1, len(hosts)))
					err := clusterGateway.registerNewHost(host)
					Expect(err).To(BeNil())
					size += 1

					Expect(cluster.Len()).To(Equal(size))
					Expect(index.Len()).To(Equal(size))
					Expect(placer.NumHostsInIndex()).To(Equal(size))
					Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(size))
				}

				var startKernelReplicaCalled sync.WaitGroup
				startKernelReplicaCalled.Add(3 * numKernels)

				startKernelReturnValChannels := make(map[int]chan *proto.KernelConnectionInfo)

				for i := 0; i < numHosts; i++ {
					startKernelReturnValChan := make(chan *proto.KernelConnectionInfo, 3)
					startKernelReturnValChannels[i] = startKernelReturnValChan

					By(fmt.Sprintf("Preparing mocked LocalGatewayClient %d/%d to expect a call to StartKernelReplica.", i+1, numHosts))
					localGatewayClient := localGatewayClients[i]
					resourceSpoofer := resourceSpoofers[i]
					prepareMockedGatewayForStartKernel(localGatewayClient, i, resourceSpoofer, resourceSpec, startKernelReturnValChan, &startKernelReplicaCalled, numKernels)
				}

				By(fmt.Sprintf("Correctly initiating the creation of %d new kernels", numKernels))

				startKernelReturnValChan := make(chan *proto.KernelConnectionInfo, numKernels)
				for i := 0; i < numKernels; i++ {
					index := i
					go func() {
						connInfo, err := clusterGateway.StartKernel(context.Background(), kernelSpecsByIdx[index])
						Expect(err).To(BeNil())
						Expect(connInfo).ToNot(BeNil())

						startKernelReturnValChan <- connInfo
					}()
				}

				doneChan := make(chan interface{}, 1)
				go func() {
					startKernelReplicaCalled.Wait()
					doneChan <- struct{}{}
				}()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()

				select {
				case <-ctx.Done():
					GinkgoWriter.Printf("Timed-out waiting for StartKernelReplica to be called on Local Daemons by Placer.\n")
					Expect(false).To(BeTrue())
				case <-doneChan:
					{
						// Do nothing / continue with the unit test
					}
				}

				By("Correctly handling the KernelConnectionInfo")
				for _, channel := range startKernelReturnValChannels {
					for i := 0; i < numKernels; i++ {
						kernelSpec := kernelSpecsByIdx[i]

						channel <- &proto.KernelConnectionInfo{
							Ip:              "0.0.0.0",
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: messaging.JupyterSignatureScheme,
							Key:             kernelSpec.Key,
						}
					}
				}

				// Ensure that the resource counts of the hosts are correct.
				for _, host := range hosts {
					var combinedSpec types.Spec

					for _, kernelSpec := range kernelSpecsByIdx {
						if combinedSpec == nil {
							combinedSpec = kernelSpec.ResourceSpec.ToDecimalSpec()
						} else {
							combinedSpec = combinedSpec.Add(kernelSpec.ResourceSpec.ToDecimalSpec())
						}
					}

					Expect(host.NumContainers()).To(Equal(0))
					Expect(host.NumReservations()).To(Equal(numKernels))
					Expect(host.PendingResources().Equals(combinedSpec))
					Expect(host.IdleResources().Equals(host.ResourceSpec()))
					Expect(host.CommittedResources().IsZero()).To(BeTrue())
				}

				time.Sleep(time.Millisecond * 1250)

				var notifyKernelRegisteredCalled sync.WaitGroup
				notifyKernelRegisteredCalled.Add(numKernels * 3)

				By("Correctly notifying that the kernel registered")

				sleepIntervals := make(chan time.Duration, numKernels*3)
				notifyKernelRegistered := func(replicaId int32, kernelId string, targetHost scheduling.Host) {
					// defer GinkgoRecover()

					log.Printf("Notifying Gateway that replica %d of kernel %s has registered.\n", replicaId, kernelId)

					time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
					time.Sleep(<-sleepIntervals)

					ctx := context.WithValue(context.Background(), SkipValidationKey, "true")
					resp, err := clusterGateway.NotifyKernelRegistered(ctx, &proto.KernelRegistrationNotification{
						ConnectionInfo: &proto.KernelConnectionInfo{
							Ip:              "0.0.0.0",
							Transport:       "tcp",
							ControlPort:     9000,
							ShellPort:       9001,
							StdinPort:       9002,
							HbPort:          9003,
							IopubPort:       9004,
							IosubPort:       9005,
							SignatureScheme: messaging.JupyterSignatureScheme,
							Key:             kernelKey,
						},
						KernelId:           kernelId,
						SessionId:          "N/A",
						ReplicaId:          replicaId,
						HostId:             targetHost.GetID(),
						KernelIp:           "0.0.0.0",
						PodOrContainerName: "kernel1pod",
						NodeName:           targetHost.GetNodeName(),
						NotificationId:     uuid.NewString(),
					})
					Expect(resp).ToNot(BeNil())
					Expect(err).To(BeNil())
					Expect(resp.Id).To(Equal(replicaId))

					log.Printf("Successfully notified Gateway that replica %d of kernel %s has registered.\n", replicaId, kernelId)

					notifyKernelRegisteredCalled.Done()
				}

				for i := 0; i < numKernels; i++ {
					sleepIntervals <- time.Millisecond * 250
					sleepIntervals <- time.Millisecond * 500
					sleepIntervals <- time.Millisecond * 750
				}

				for i := 0; i < numKernels; i++ {
					spec := kernelSpecsByIdx[i]

					go notifyKernelRegistered(1, spec.Id, hosts[0])
					go notifyKernelRegistered(2, spec.Id, hosts[1])
					go notifyKernelRegistered(3, spec.Id, hosts[2])
				}

				notifyKernelRegisteredCalled.Wait()

				time.Sleep(time.Millisecond * 250)

				var smrReadyCalled sync.WaitGroup
				smrReadyCalled.Add(3 * numKernels)
				callSmrReady := func(replicaId int32, kernelId string, persistentId string) {
					// defer GinkgoRecover()

					time.Sleep(time.Millisecond * time.Duration(rand.Intn(25)+25 /* 25 - 50 */))
					time.Sleep(<-sleepIntervals)

					_, err := clusterGateway.SmrReady(context.Background(), &proto.SmrReadyNotification{
						KernelId:     kernelId,
						ReplicaId:    replicaId,
						PersistentId: persistentId,
						Address:      "0.0.0.0",
					})
					Expect(err).To(BeNil())

					smrReadyCalled.Done()
				}

				for i := 0; i < numKernels; i++ {
					sleepIntervals <- time.Millisecond * 600
					sleepIntervals <- time.Millisecond * 1200
					sleepIntervals <- time.Millisecond * 800
				}

				By("Correctly calling SMR ready and handling that correctly")

				for i := 0; i < numKernels; i++ {
					kernel := kernelsByIdx[i]

					go callSmrReady(1, kernel.ID(), kernel.PersistentID())
					go callSmrReady(2, kernel.ID(), kernel.PersistentID())
					go callSmrReady(3, kernel.ID(), kernel.PersistentID())
				}

				smrReadyCalled.Wait()

				connInfo := <-startKernelReturnValChan
				Expect(connInfo).ToNot(BeNil())

				go func() {
					defer GinkgoRecover()

					if err := clusterGateway.router.Close(); err != nil {
						clusterGateway.log.Error("Failed to cleanly shutdown router because: %v", err)
					}

					// Close the listener
					if clusterGateway.listener != nil {
						if err := clusterGateway.listener.Close(); err != nil {
							clusterGateway.log.Error("Failed to cleanly shutdown listener because: %v", err)
						}
					}
				}()

				// Ensure that the resource counts of the hosts are still correct.
				for _, host := range hosts {
					var combinedSpec types.Spec

					for _, kernelSpec := range kernelSpecsByIdx {
						if combinedSpec == nil {
							combinedSpec = kernelSpec.ResourceSpec.ToDecimalSpec()
						} else {
							combinedSpec = combinedSpec.Add(kernelSpec.ResourceSpec.ToDecimalSpec())
						}
					}

					Expect(host.NumContainers()).To(Equal(numKernels))
					Expect(host.NumReservations()).To(Equal(0))
					Expect(host.PendingResources().Equals(combinedSpec))
					Expect(host.IdleResources().Equals(host.ResourceSpec()))
					Expect(host.CommittedResources().IsZero()).To(BeTrue())
				}
			})
		})
	})
})
