package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/mock_scheduling"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	types2 "github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/mock_client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"go.uber.org/mock/gomock"
)

const (
	signatureScheme string = "hmac-sha256"
	kernelId               = "66902bac-9386-432e-b1b9-21ac853fa1c9"
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
      "gpus-per-host": 8,
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
      "scheduler-http-port": 8078
    },
    "common_options": {
      "deployment_mode": "docker-compose",
      "using-wsl": true,
      "docker_network_name": "distributed_cluster_default",
      "prometheus_interval": 15,
      "prometheus_port": 8089,
      "num_resend_attempts": 1,
      "acks_enabled": false,
      "scheduling-policy": "static",
      "hdfs-namenode-endpoint": "host.docker.internal:10000",
      "smr-port": 8080,
      "debug_mode": true,
      "debug_port": 9996,
      "simulate_checkpointing_latency": true,
      "disable_prometheus_metrics_publishing": false
    },
    "local-daemon-service-name": "local-daemon-network",
    "local-daemon-service-port": 8075,
    "global-daemon-service-name": "daemon-network",
    "global-daemon-service-port": 0,
    "kubernetes-namespace": "",
    "use-stateful-set": false,
    "notebook-image-name": "scusemua/jupyter",
    "notebook-image-tag": "latest",
    "distributed-cluster-service-port": 8079
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

var _ = Describe("Cluster Gateway Tests", func() {
	var (
		clusterGateway *ClusterGatewayImpl
		abstractServer *server.AbstractServer
		cluster        *mock_scheduling.MockCluster
		session        *mock_scheduling.MockAbstractSession
		mockCtrl       *gomock.Controller
		kernel         *mock_client.MockAbstractDistributedKernelClient
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
			header *types.MessageHeader
		)

		BeforeEach(func() {
			kernel = mock_client.NewMockAbstractDistributedKernelClient(mockCtrl)
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			session = mock_scheduling.NewMockAbstractSession(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			clusterGateway = &ClusterGatewayImpl{
				cluster:    cluster,
				RequestLog: metrics.NewRequestLog(),
			}
			config.InitLogger(&clusterGateway.log, clusterGateway)

			kernel.EXPECT().ConnectionInfo().Return(&types.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
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
			kernel.EXPECT().ResourceSpec().Return(&types2.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(100),
				MemoryMb:  decimal.NewFromFloat(1000),
			}).AnyTimes()
			kernel.EXPECT().ID().Return(kernelId).AnyTimes()
			cluster.EXPECT().GetSession(kernelId).Return(session, true).AnyTimes()

			header = &types.MessageHeader{
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
			jFrames := types.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := types.NewJupyterMessage(msg)

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
			jFrames := types.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))

			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := types.NewJupyterMessage(msg)
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
			header *types.MessageHeader
		)

		// Signature after signing the message using the header defined below
		// initialSignature := "c34f1638ae4d0ead9ffefa13e91202b74a9d012fee8ee6b55274f29bcc7b5427"

		BeforeEach(func() {
			kernel = mock_client.NewMockAbstractDistributedKernelClient(mockCtrl)
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			session = mock_scheduling.NewMockAbstractSession(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			clusterGateway = &ClusterGatewayImpl{
				cluster:    cluster,
				RequestLog: metrics.NewRequestLog(),
			}
			config.InitLogger(&clusterGateway.log, clusterGateway)

			kernel.EXPECT().ConnectionInfo().Return(&types.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
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
			kernel.EXPECT().ResourceSpec().Return(&types2.DecimalSpec{
				GPUs:      decimal.NewFromFloat(2),
				Millicpus: decimal.NewFromFloat(100),
				MemoryMb:  decimal.NewFromFloat(1000),
			}).AnyTimes()
			kernel.EXPECT().ID().Return(kernelId).AnyTimes()
			cluster.EXPECT().GetSession(kernelId).Return(session, true).AnyTimes()

			header = &types.MessageHeader{
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
			jFrames := types.NewJupyterFramesFromBytes(unsignedFrames)
			frames, err := jFrames.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := types.NewJupyterMessage(msg)
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
				Expect(trace.MessageType).To(Equal(types.ShellExecuteRequest))
				Expect(trace.KernelId).To(Equal(kernelId))

				m, err := json.Marshal(&proto.JupyterRequestTraceFrame{RequestTrace: trace})
				Expect(err).To(BeNil())

				fmt.Printf("jMsg.JupyterFrames[%d+%d]: %s\n", jMsg.Offset(), types.JupyterFrameRequestTrace, string(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+types.JupyterFrameRequestTrace]))
				fmt.Printf("Marshalled RequestTrace: %s\n", string(m))
				Expect(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+types.JupyterFrameRequestTrace]).To(Equal(m))
			}

			requestReceivedByGateway := int64(257894000000)
			requestReceivedByGatewayTs := time.UnixMilli(requestReceivedByGateway) // 2009-11-10 23:00:00 +0000 UTC
			requestTrace, added, err := types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestSentByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestSentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replyReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replyReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = types.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByGatewayTs, abstractServer.Log)
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

	Context("Scheduling Kernels", func() {
		BeforeEach(func() {
			cluster = mock_scheduling.NewMockCluster(mockCtrl)
			abstractServer = &server.AbstractServer{
				DebugMode: true,
				Log:       config.GetLogger("TestAbstractServer"),
			}

			var options *domain.ClusterGatewayOptions
			err := json.Unmarshal([]byte(GatewayOptsAsJsonString), &options)
			if err != nil {
				panic(err)
			}

			fmt.Printf("Gateway options:\n%s\n", options.PrettyString(2))
			clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv scheduling.ClusterGateway) {
				globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
				srv.SetClusterOptions(&options.ClusterSchedulerOptions)
			})

			config.InitLogger(&clusterGateway.log, clusterGateway)
		})

		It("Will correctly schedule a new kernel", func() {
			kernelId := uuid.NewString()

			resourceSpec := &proto.ResourceSpec{
				Gpu:    4,
				Vram:   4,
				Cpu:    1250,
				Memory: 2048,
			}

			kernelSpec := &proto.KernelSpec{
				Id:              kernelId,
				Session:         kernelId,
				Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
				SignatureScheme: types.JupyterSignatureScheme,
				Key:             kernelKey,
				ResourceSpec:    resourceSpec,
			}

			connInfo, err := clusterGateway.StartKernel(context.Background(), kernelSpec)
			Expect(err).To(BeNil())
			Expect(connInfo).ToNot(BeNil())
		})
	})
})
