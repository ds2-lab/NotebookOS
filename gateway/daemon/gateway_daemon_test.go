package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"github.com/shopspring/decimal"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter"
	"go.uber.org/mock/gomock"
)

const (
	signatureScheme string = "hmac-sha256"

	kernelId string = "66902bac-9386-432e-b1b9-21ac853fa1c9"

	persistentId string = "a45e4331-8fdc-4143-aac8-00d3e9df54fa"
)

var (
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
			"scaling-factor": 1.05,
			"scaling-interval": 30,
			"scaling-limit": 1.1,
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
			"hdfs-namenode-endpoint": "host.docker.internal:10000",
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

func TestProxy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Daemon Suite")
}

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

func (p *MockedDistributedKernelClientProvider) NewDistributedKernelClient(ctx context.Context, spec *proto.KernelSpec, numReplicas int, hostId string,
	connectionInfo *jupyter.ConnectionInfo, shellListenPort int, iopubListenPort int, persistentId string,
	debugMode bool, executionFailedCallback scheduling.ExecutionFailedCallback, executionLatencyCallback scheduling.ExecutionLatencyCallback,
	messagingMetricsProvider metrics.MessagingMetricsProvider) scheduling.Kernel {

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

// initMockedKernelForCreation creates and returns a new MockAbstractDistributedKernelClient that is
// set up for use in a unit test that involves creating a new kernel.
func initMockedKernelForCreation(mockCtrl *gomock.Controller, kernelId string, kernelKey string, resourceSpec *proto.ResourceSpec) (*mock_scheduling.MockKernel, *proto.KernelSpec) {
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
	kernel.EXPECT().AddReplica(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(func(r scheduling.KernelReplica, host scheduling.Host) error {
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

	return kernel, kernelSpec
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
		clusterGateway *ClusterGatewayImpl
		abstractServer *server.AbstractServer
		session        *mock_scheduling.MockUserSession
		mockCtrl       *gomock.Controller
		kernelKey      = "23d90942-8c3de3a713a5c3611792b7a5"
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Processing 'execute_request' messages", func() {
		var (
			kernel  *mock_scheduling.MockKernel
			header  *messaging.MessageHeader
			cluster *mock_scheduling.MockCluster
		)

		BeforeEach(func() {
			kernel = mock_scheduling.NewMockKernel(mockCtrl)
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			session = mock_scheduling.NewMockUserSession(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			clusterGateway = &ClusterGatewayImpl{
				cluster:                  cluster,
				RequestLog:               metrics.NewRequestLog(),
				gatewayPrometheusManager: nil,
			}
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
				MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
				Username: "",
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			kernel.EXPECT().Size().Return(3).AnyTimes()

			setActiveCall := kernel.EXPECT().EnqueueActiveExecution(gomock.Any(), gomock.Any())
			kernel.EXPECT().NumActiveExecutionOperations().Return(0).Times(1)
			kernel.EXPECT().NumActiveExecutionOperations().After(setActiveCall).Return(1).Times(1)
		})

		It("should correctly handle execute_request messages", func() {
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

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			err = clusterGateway.processExecuteRequest(jMsg, kernel)
			Expect(err).To(BeNil())
			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))
		})

		It("should correctly handle execute_request messages with an offset", func() {
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

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			err = clusterGateway.processExecuteRequest(jMsg, kernel)
			Expect(err).To(BeNil())
			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))
		})
	})

	Context("ZMQ Messages", func() {
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
				cluster:                  cluster,
				RequestLog:               metrics.NewRequestLog(),
				gatewayPrometheusManager: nil,
			}
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
				MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
				Username: "",
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}
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
				Expect(trace.MessageId).To(Equal("c7074e5b-b90f-44f8-af5d-63201ec3a527"))
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
			requestTrace, added, err := messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, requestReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, requestSentByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, requestReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, requestSentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, requestReceivedByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, replySentByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, replyReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, replySentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, replyReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = messaging.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &messaging.Socket{Type: messaging.ShellMessage}, replySentByGatewayTs, abstractServer.Log)
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
		var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider
		var kernelId string
		var mockedKernelSpec *proto.KernelSpec
		var mockedKernel *mock_scheduling.MockKernel
		var resourceSpec *proto.ResourceSpec

		BeforeEach(func() {
			config.LogLevel = logger.LOG_LEVEL_ALL

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

			kernelId = uuid.NewString()

			resourceSpec = &proto.ResourceSpec{
				Gpu:    2,
				Vram:   2,
				Cpu:    1250,
				Memory: 2048,
			}

			mockedKernel, mockedKernelSpec = initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec)

			Expect(mockedKernelSpec).ToNot(BeNil())
			mockedKernel.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
			mockedKernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
				Id:              kernelId,
				Session:         kernelId,
				SignatureScheme: signatureScheme,
				Key:             "23d90942-8c3de3a713a5c3611792b7a5",
				ResourceSpec: &proto.ResourceSpec{
					Gpu:    2,
					Cpu:    100,
					Memory: 1000,
				},
			}).AnyTimes()
			mockedKernel.EXPECT().ResourceSpec().Return(&types.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(100),
				MemoryMb:  decimal.NewFromFloat(1000),
			}).AnyTimes()
			mockedKernel.EXPECT().ID().Return(kernelId).AnyTimes()

			mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, mockedKernel)

			mockedDistributedKernelClientProvider.RegisterMockedDistributedKernel(kernelId, mockedKernel)

			cluster := clusterGateway.cluster

			host1Id := uuid.NewString()
			node1Name := "TestNode1"
			host1Spoofer := distNbTesting.NewResourceSpoofer(node1Name, host1Id, clusterGateway.hostSpec)
			host1, localGatewayClient1, _ := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host1Id, node1Name, host1Spoofer)

			host2Id := uuid.NewString()
			node2Name := "TestNode2"
			host2Spoofer := distNbTesting.NewResourceSpoofer(node2Name, host2Id, clusterGateway.hostSpec)
			host2, localGatewayClient2, _ := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host2Id, node2Name, host2Spoofer)

			host3Id := uuid.NewString()
			node3Name := "TestNode3"
			host3Spoofer := distNbTesting.NewResourceSpoofer(node3Name, host3Id, clusterGateway.hostSpec)
			host3, localGatewayClient3, _ := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, cluster, host3Id, node3Name, host3Spoofer)

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

				err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
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

				err := host1Spoofer.Manager.IdleResources().Subtract(resourceSpec.ToDecimalSpec())
				GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
				err = host1Spoofer.Manager.PendingResources().Add(resourceSpec.ToDecimalSpec())
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
			startKernelReturnValChan2 <- &proto.KernelConnectionInfo{
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
			startKernelReturnValChan3 <- &proto.KernelConnectionInfo{
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
					Address:      "0.0.0.0",
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

			mockedKernel.EXPECT().Status().AnyTimes().Return(jupyter.KernelStatusRunning)
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

		It("Will correctly migrate a kernel replica when using static scheduling", func() {
			unsignedExecuteRequestFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent executeRequestMessageHeader*/
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}

			executeRequestMessageHeader := &messaging.MessageHeader{
				MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
				Username: "",
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}

			jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecuteRequestFrames)
			err := jFrames.EncodeHeader(executeRequestMessageHeader)
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

			var activeExecution *scheduling.ActiveExecution
			mockedKernel.EXPECT().EnqueueActiveExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(attemptId int, msg *messaging.JupyterMessage) *scheduling.ActiveExecution {
				Expect(attemptId).To(Equal(1))
				Expect(msg).ToNot(BeNil())
				Expect(msg).To(Equal(jMsg))

				activeExecution = scheduling.NewActiveExecution(kernelId, attemptId, 3, msg)
				wg.Done()

				return activeExecution
			}).Times(1)

			fmt.Printf("[DEBUG] Forwarding 'execute_request' message now:\n%v\n", jMsg.StringFormatted())

			go func() {
				err = clusterGateway.ShellHandler(nil, jMsg)
				Expect(err).ToNot(BeNil())
			}()

			wg.Wait()
			Expect(activeExecution).ToNot(BeNil())

			getExecuteReplyMessage := func(id int) *messaging.JupyterMessage {
				unsignedExecuteReplyFrames := [][]byte{
					[]byte("<IDS|MSG>"),
					[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
					[]byte(""), /* Header */
					[]byte(""), /* Parent executeReplyMessageHeader*/
					[]byte(""), /* Metadata */
					[]byte("{\"status\": \"error\", \"ename\": \"ExecutionYieldError\", \"evalue\": \"ExecutionYieldError\"}"),
				}

				executeReplyMessageHeader := &messaging.MessageHeader{
					MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a528",
					Username: "",
					Session:  kernelId,
					Date:     "2024-04-03T22:56:52.605Z",
					MsgType:  "execute_reply",
					Version:  "5.2",
				}

				executeReplyJFrames := messaging.NewJupyterFramesFromBytes(unsignedExecuteReplyFrames)
				err := jFrames.EncodeParentHeader(executeRequestMessageHeader)
				Expect(err).To(BeNil())
				err = executeReplyJFrames.EncodeHeader(executeReplyMessageHeader)
				Expect(err).To(BeNil())
				frames, _ := executeReplyJFrames.Sign(signatureScheme, []byte(kernelKey))
				msg := &zmq4.Msg{
					Frames: frames,
					Type:   zmq4.UsrMsg,
				}
				jMsg := messaging.NewJupyterMessage(msg)

				return jMsg
			}

			execReply1 := getExecuteReplyMessage(1)
			Expect(execReply1).ToNot(BeNil())

			execReply2 := getExecuteReplyMessage(2)
			Expect(execReply2).ToNot(BeNil())

			execReply3 := getExecuteReplyMessage(3)
			Expect(execReply3).ToNot(BeNil())
		})
	})

	Context("DockerCluster", func() {
		Context("Initial Connection Period", func() {
			var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider
			var options *domain.ClusterGatewayOptions

			BeforeEach(func() {
				config.LogLevel = logger.LOG_LEVEL_ALL

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
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)

				Expect(clusterGateway.gatewayPrometheusManager).To(BeNil())
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

				By("Not disabling the first 'InitialClusterSize' Local Daemons that connect to the Cluster Gateway.")

				clusterSize := 0
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
					Expect(host.Enabled()).To(Equal(false))
					Expect(cluster.NumDisabledHosts()).To(Equal(numDisabledHosts))
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

				Expect(clusterGateway.gatewayPrometheusManager).To(BeNil())
				Expect(clusterGateway.initialClusterSize).To(Equal(InitialClusterSize))
				Expect(clusterGateway.initialConnectionPeriod).To(Equal(InitialConnectionTime))
				Expect(clusterGateway.inInitialConnectionPeriod.Load()).To(Equal(false))
			})
		})

		Context("Scheduling Kernels", func() {
			var mockedDistributedKernelClientProvider *MockedDistributedKernelClientProvider

			BeforeEach(func() {
				config.LogLevel = logger.LOG_LEVEL_ALL

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

				Expect(clusterGateway.gatewayPrometheusManager).To(BeNil())
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

				kernel, kernelSpec := initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec)
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
				startKernelReturnValChan2 <- &proto.KernelConnectionInfo{
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
				startKernelReturnValChan3 <- &proto.KernelConnectionInfo{
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
					kernel, kernelSpec := initMockedKernelForCreation(mockCtrl, kernelId, kernelKey, resourceSpec)
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
