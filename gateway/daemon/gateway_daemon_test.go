package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/mock_proto"
	"github.com/zhangjyr/distributed-notebook/common/mock_scheduling"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	types "github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/mock_client"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
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
		  "disable_prometheus_metrics_publishing": false
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
	"remote-docker-event-aggregator-port": 5821
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

// ResourceSpoofer is used to provide spoofed resources to be used by mock_proto.MockLocalGatewayClient
// instances when spoofing calls to proto.LocalGatewayClient.ResourcesSnapshot.
type ResourceSpoofer struct {
	snapshotId atomic.Int32
	hostId     string
	nodeName   string
	managerId  string
	hostSpec   types.Spec
	wrapper    *scheduling.ResourcesWrapper
}

func NewResourceSpoofer(nodeName string, hostId string, hostSpec types.Spec) *ResourceSpoofer {
	spoofer := &ResourceSpoofer{
		hostId:    hostId,
		nodeName:  nodeName,
		managerId: uuid.NewString(),
		hostSpec:  hostSpec,
		wrapper:   scheduling.NewResourcesWrapper(hostSpec),
	}

	GinkgoWriter.Printf("Created new ResourceSpoofer for host %s (ID=%s) with spec=%s\n", nodeName, hostId, hostSpec.String())

	return spoofer
}

func (s *ResourceSpoofer) ResourcesSnapshot(_ context.Context, _ *proto.Void, _ ...grpc.CallOption) (*proto.NodeResourcesSnapshotWithContainers, error) {
	snapshotId := s.snapshotId.Add(1)
	resourceSnapshot := &proto.NodeResourcesSnapshot{
		// SnapshotId uniquely identifies the NodeResourcesSnapshot and defines a total order amongst all NodeResourcesSnapshot
		// structs originating from the same node. Each newly-created NodeResourcesSnapshot is assigned an ID from a
		// monotonically-increasing counter by the ResourceManager.
		SnapshotId: snapshotId,
		// NodeId is the ID of the node from which the resourceSnapshot originates.
		NodeId: s.hostId,
		// ManagerId is the unique ID of the ResourceManager struct from which the NodeResourcesSnapshot was constructed.
		ManagerId: s.managerId,
		// Timestamp is the time at which the NodeResourcesSnapshot was taken/created.
		Timestamp:          timestamppb.New(time.Now()),
		IdleResources:      s.wrapper.IdleProtoResourcesSnapshot(snapshotId),
		PendingResources:   s.wrapper.PendingProtoResourcesSnapshot(snapshotId),
		CommittedResources: s.wrapper.CommittedProtoResourcesSnapshot(snapshotId),
		SpecResources:      s.wrapper.SpecProtoResourcesSnapshot(snapshotId),
	}

	snapshotWithContainers := &proto.NodeResourcesSnapshotWithContainers{
		Id:               uuid.NewString(),
		ResourceSnapshot: resourceSnapshot,
		Containers:       make([]*proto.ReplicaInfo, 0), // TODO: Incorporate this field into ResourceSpoofer.
	}

	fmt.Printf("Spoofing resources for Host \"%s\" (ID=\"%s\") with SnapshotID=%d now: %v\n", s.nodeName, s.hostId, snapshotId, resourceSnapshot.String())
	return snapshotWithContainers, nil
}

// NewHostWithSpoofedGRPC creates a new scheduling.Host struct with a spoofed proto.LocalGatewayClient.
func NewHostWithSpoofedGRPC(ctrl *gomock.Controller, cluster scheduling.Cluster, hostId string, nodeName string, resourceSpoofer *ResourceSpoofer) (*scheduling.Host, *mock_proto.MockLocalGatewayClient, error) {
	gpuSchedulerId := uuid.NewString()

	localGatewayClient := mock_proto.NewMockLocalGatewayClient(ctrl)

	localGatewayClient.EXPECT().SetID(
		gomock.Any(),
		&proto.HostId{
			Id: hostId,
		},
	).Return(&proto.HostId{
		Id:       hostId,
		NodeName: nodeName,
	}, nil)

	localGatewayClient.EXPECT().GetActualGpuInfo(
		gomock.Any(),
		&proto.Void{},
	).Return(&proto.GpuInfo{
		SpecGPUs:              8,
		IdleGPUs:              8,
		CommittedGPUs:         0,
		PendingGPUs:           0,
		NumPendingAllocations: 0,
		NumAllocations:        0,
		GpuSchedulerID:        gpuSchedulerId,
		LocalDaemonID:         hostId,
	}, nil)

	localGatewayClient.EXPECT().ResourcesSnapshot(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(resourceSpoofer.ResourcesSnapshot).AnyTimes()

	host, err := scheduling.NewHost(hostId, "0.0.0.0", scheduling.MillicpusPerHost,
		scheduling.MemoryMbPerHost, scheduling.VramPerHostGb, cluster, nil, localGatewayClient,
		func(_ string, _ string, _ string, _ string) error { return nil })

	return host, localGatewayClient, err
}

var _ = Describe("Cluster Gateway Tests", func() {
	var (
		clusterGateway *ClusterGatewayImpl
		abstractServer *server.AbstractServer
		session        *mock_scheduling.MockAbstractSession
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
			kernel  *mock_client.MockAbstractDistributedKernelClient
			header  *jupyter.MessageHeader
			cluster *mock_scheduling.MockCluster
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

			header = &jupyter.MessageHeader{
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
			jFrames := jupyter.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := jupyter.NewJupyterMessage(msg)

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
			jFrames := jupyter.NewJupyterFramesFromBytes(unsignedFrames)
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))

			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := jupyter.NewJupyterMessage(msg)
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
			kernel  *mock_client.MockAbstractDistributedKernelClient
			header  *jupyter.MessageHeader
			cluster *mock_scheduling.MockCluster
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

			header = &jupyter.MessageHeader{
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
			jFrames := jupyter.NewJupyterFramesFromBytes(unsignedFrames)
			frames, err := jFrames.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := jupyter.NewJupyterMessage(msg)
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
				Expect(trace.MessageType).To(Equal(jupyter.ShellExecuteRequest))
				Expect(trace.KernelId).To(Equal(kernelId))

				m, err := json.Marshal(&proto.JupyterRequestTraceFrame{RequestTrace: trace})
				Expect(err).To(BeNil())

				fmt.Printf("jMsg.JupyterFrames[%d+%d]: %s\n", jMsg.Offset(), jupyter.JupyterFrameRequestTrace, string(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+jupyter.JupyterFrameRequestTrace]))
				fmt.Printf("Marshalled RequestTrace: %s\n", string(m))
				Expect(jMsg.JupyterFrames.Frames[jMsg.JupyterFrames.Offset+jupyter.JupyterFrameRequestTrace]).To(Equal(m))
			}

			requestReceivedByGateway := int64(257894000000)
			requestReceivedByGatewayTs := time.UnixMilli(requestReceivedByGateway) // 2009-11-10 23:00:00 +0000 UTC
			requestTrace, added, err := jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, requestReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, requestSentByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, requestReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, requestSentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, requestReceivedByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, replySentByKernelReplicaTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, replyReceivedByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, replySentByLocalDaemonTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, replyReceivedByGatewayTs, abstractServer.Log)
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
			requestTrace, added, err = jupyter.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &jupyter.Socket{Type: jupyter.ShellMessage}, replySentByGatewayTs, abstractServer.Log)
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

	Context("DockerCluster", func() {
		Context("Scheduling Kernels", func() {
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

				// fmt.Printf("Gateway options:\n%s\n", options.PrettyString(2))
				clusterGateway = New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv scheduling.ClusterGateway) {
					globalLogger.Info("Initializing internalCluster Daemon with options: %s", options.ClusterDaemonOptions.String())
					srv.SetClusterOptions(&options.ClusterSchedulerOptions)
				})
				config.InitLogger(&clusterGateway.log, clusterGateway)
			})

			It("Will correctly schedule a new kernel", func() {
				kernelId := uuid.NewString()
				persistentId := uuid.NewString()

				cluster := clusterGateway.cluster
				index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := cluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := cluster.ClusterScheduler()
				Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

				Expect(cluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				host1Id := uuid.NewString()
				node1Name := "TestNode1"
				host1Spoofer := NewResourceSpoofer(node1Name, host1Id, clusterGateway.hostSpec)
				host1, localGatewayClient1, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host1Id, node1Name, host1Spoofer)
				Expect(err).To(BeNil())

				host2Id := uuid.NewString()
				node2Name := "TestNode2"
				host2Spoofer := NewResourceSpoofer(node2Name, host2Id, clusterGateway.hostSpec)
				host2, localGatewayClient2, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host2Id, node2Name, host2Spoofer)
				Expect(err).To(BeNil())

				host3Id := uuid.NewString()
				node3Name := "TestNode3"
				host3Spoofer := NewResourceSpoofer(node3Name, host3Id, clusterGateway.hostSpec)
				host3, localGatewayClient3, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host3Id, node3Name, host3Spoofer)
				Expect(err).To(BeNil())

				By("Correctly registering the first Host")

				// Add first host.
				cluster.NewHostAddedOrConnected(host1)

				Expect(cluster.Len()).To(Equal(1))
				Expect(index.Len()).To(Equal(1))
				Expect(placer.NumHostsInIndex()).To(Equal(1))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(1))

				By("Correctly registering the second Host")

				// Add second host.
				cluster.NewHostAddedOrConnected(host2)

				Expect(cluster.Len()).To(Equal(2))
				Expect(index.Len()).To(Equal(2))
				Expect(placer.NumHostsInIndex()).To(Equal(2))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(2))

				By("Correctly registering the third Host")

				// Add third host.
				cluster.NewHostAddedOrConnected(host3)

				Expect(cluster.Len()).To(Equal(3))
				Expect(index.Len()).To(Equal(3))
				Expect(placer.NumHostsInIndex()).To(Equal(3))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(3))

				resourceSpec := &proto.ResourceSpec{
					Gpu:    2,
					Vram:   2,
					Cpu:    1250,
					Memory: 2048,
				}

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				var startKernelReplicaCalled sync.WaitGroup
				startKernelReplicaCalled.Add(3)

				startKernelReturnValChan1 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan2 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan3 := make(chan *proto.KernelConnectionInfo)

				localGatewayClient1.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #1 has called spoofed StartKernelReplica\n")

					defer GinkgoRecover()

					err := host1Spoofer.wrapper.IdleResources().(*scheduling.Resources).Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.wrapper.PendingResources().(*scheduling.Resources).Add(resourceSpec.ToDecimalSpec())
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

					defer GinkgoRecover()

					err := host1Spoofer.wrapper.IdleResources().(*scheduling.Resources).Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.wrapper.PendingResources().(*scheduling.Resources).Add(resourceSpec.ToDecimalSpec())
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

					defer GinkgoRecover()

					err := host1Spoofer.wrapper.IdleResources().(*scheduling.Resources).Subtract(resourceSpec.ToDecimalSpec())
					GinkgoWriter.Printf("Error after subtracting from idle resources: %v\n", err)
					Expect(err).To(BeNil())
					err = host1Spoofer.wrapper.PendingResources().(*scheduling.Resources).Add(resourceSpec.ToDecimalSpec())
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
					defer GinkgoRecover()

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
					SignatureScheme: jupyter.JupyterSignatureScheme,
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
					SignatureScheme: jupyter.JupyterSignatureScheme,
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
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
				}

				time.Sleep(time.Millisecond * 1250)

				var notifyKernelRegisteredCalled sync.WaitGroup
				notifyKernelRegisteredCalled.Add(3)

				By("Correctly notifying that the kernel registered")

				sleepIntervals := make(chan time.Duration, 3)
				notifyKernelRegistered := func(replicaId int32) {
					defer GinkgoRecover()

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
							SignatureScheme: jupyter.JupyterSignatureScheme,
							Key:             kernelKey,
						},
						KernelId:           kernelId,
						SessionId:          "N/A",
						ReplicaId:          replicaId,
						HostId:             host1Id,
						KernelIp:           "0.0.0.0",
						PodOrContainerName: "kernel1pod",
						NodeName:           node1Name,
						NotificationId:     uuid.NewString(),
					})
					Expect(resp).ToNot(BeNil())
					Expect(err).To(BeNil())
					Expect(resp.Id).To(Equal(replicaId))

					notifyKernelRegisteredCalled.Done()
				}

				sleepIntervals <- time.Millisecond * 500
				sleepIntervals <- time.Millisecond * 1000
				sleepIntervals <- time.Millisecond * 1500

				go notifyKernelRegistered(1)
				go notifyKernelRegistered(2)
				go notifyKernelRegistered(3)

				notifyKernelRegisteredCalled.Wait()

				time.Sleep(time.Millisecond * 2500)

				var smrReadyCalled sync.WaitGroup
				smrReadyCalled.Add(3)
				callSmrReady := func(replicaId int32) {
					defer GinkgoRecover()

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

				sleepIntervals <- time.Millisecond * 1000
				sleepIntervals <- time.Millisecond * 500
				sleepIntervals <- time.Millisecond * 1500

				By("Correctly calling SMR ready and handling that correctly")

				go callSmrReady(1)
				go callSmrReady(2)
				go callSmrReady(3)

				smrReadyCalled.Wait()

				connInfo := <-startKernelReturnValChan
				Expect(connInfo).ToNot(BeNil())

				go func() {
					defer GinkgoRecover()
					_ = clusterGateway.Close()
				}()
			})

			It("Will attempt to scale-out when there are insufficient resources available on existing hosts", func() {
				kernelId := uuid.NewString()
				// persistentId := uuid.NewString()

				cluster := clusterGateway.cluster
				index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
				Expect(ok).To(BeTrue())
				Expect(index).ToNot(BeNil())

				placer := cluster.Placer()
				Expect(placer).ToNot(BeNil())

				scheduler := cluster.ClusterScheduler()
				Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

				Expect(cluster.Len()).To(Equal(0))
				Expect(index.Len()).To(Equal(0))
				Expect(placer.NumHostsInIndex()).To(Equal(0))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(0))

				host1Id := uuid.NewString()
				node1Name := "TestNode1"
				host1Spoofer := NewResourceSpoofer(node1Name, host1Id, clusterGateway.hostSpec)
				host1, localGatewayClient1, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host1Id, node1Name, host1Spoofer)
				Expect(err).To(BeNil())

				host2Id := uuid.NewString()
				node2Name := "TestNode2"
				host2Spoofer := NewResourceSpoofer(node2Name, host2Id, clusterGateway.hostSpec)
				host2, localGatewayClient2, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host2Id, node2Name, host2Spoofer)
				Expect(err).To(BeNil())

				host3Id := uuid.NewString()
				node3Name := "TestNode3"
				host3Spoofer := NewResourceSpoofer(node3Name, host3Id, clusterGateway.hostSpec)
				host3, localGatewayClient3, err := NewHostWithSpoofedGRPC(mockCtrl, cluster, host3Id, node3Name, host3Spoofer)
				Expect(err).To(BeNil())

				alreadyScheduled := &proto.ResourceSpec{
					Gpu:    40,
					Vram:   40,
					Cpu:    8,
					Memory: 32000,
				}

				// Reserve some resources on one of the hosts.
				err = host1Spoofer.wrapper.PendingResources().(*scheduling.Resources).Add(alreadyScheduled.ToDecimalSpec())
				Expect(err).To(BeNil())
				err = host1.SynchronizeResourceInformation()

				// TODO: Need to actually schedule a session or multiple sessions with high gpu usage
				// TODO: and then try to schedule a session.

				newSubRatio := host1.RecomputeSubscribedRatio()
				fmt.Printf("Current resource usage: %s\n", host1.CurrentResourcesToString())
				fmt.Printf("New subscription ratio: %s\n", newSubRatio.StringFixed(4))

				Expect(err).To(BeNil())

				Expect(host1.IdleResources().GPU()).To(Equal(8.0))
				Expect(host1.PendingResources().GPU()).To(Equal(40.0))
				Expect(host1.CommittedResources().GPU()).To(Equal(0.0))

				// Add first host.
				cluster.NewHostAddedOrConnected(host1)

				Expect(cluster.Len()).To(Equal(1))
				Expect(index.Len()).To(Equal(1))
				Expect(placer.NumHostsInIndex()).To(Equal(1))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(1))

				// Add second host.
				cluster.NewHostAddedOrConnected(host2)

				Expect(cluster.Len()).To(Equal(2))
				Expect(index.Len()).To(Equal(2))
				Expect(placer.NumHostsInIndex()).To(Equal(2))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(2))

				// Add third host.
				cluster.NewHostAddedOrConnected(host3)

				Expect(cluster.Len()).To(Equal(3))
				Expect(index.Len()).To(Equal(3))
				Expect(placer.NumHostsInIndex()).To(Equal(3))
				Expect(scheduler.Placer().NumHostsInIndex()).To(Equal(3))

				resourceSpec := &proto.ResourceSpec{
					Gpu:    2,
					Vram:   2,
					Cpu:    1250,
					Memory: 2048,
				}

				kernelSpec := &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				}

				var startKernelReplicaCalled sync.WaitGroup
				startKernelReplicaCalled.Add(3)

				startKernelReturnValChan1 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan2 := make(chan *proto.KernelConnectionInfo)
				startKernelReturnValChan3 := make(chan *proto.KernelConnectionInfo)

				localGatewayClient1.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #1 has called spoofed StartKernelReplica\n")

					startKernelReplicaCalled.Done()

					ret := <-startKernelReturnValChan1
					return ret, nil
				})

				localGatewayClient2.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #2 has called spoofed StartKernelReplica\n")

					startKernelReplicaCalled.Done()
					ret := <-startKernelReturnValChan2
					return ret, nil
				})

				localGatewayClient3.EXPECT().StartKernelReplica(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx any, in any, opts ...any) (*proto.KernelConnectionInfo, error) {
					GinkgoWriter.Printf("LocalGateway #3 has called spoofed StartKernelReplica\n")

					startKernelReplicaCalled.Done()
					ret := <-startKernelReturnValChan3
					return ret, nil
				})

				startKernelReturnValChan := make(chan *proto.KernelConnectionInfo)
				go func() {
					defer GinkgoRecover()

					connInfo, err := clusterGateway.StartKernel(context.Background(), kernelSpec)
					Expect(err).To(BeNil())
					Expect(connInfo).ToNot(BeNil())

					startKernelReturnValChan <- connInfo
				}()
				startKernelReplicaCalled.Wait()
			})
		})
	})
})
