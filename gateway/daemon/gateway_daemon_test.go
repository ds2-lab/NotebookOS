package daemon

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/mock_scheduling"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	types2 "github.com/zhangjyr/distributed-notebook/common/types"
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
		kernel = mock_client.NewMockAbstractDistributedKernelClient(mockCtrl)
		cluster = mock_scheduling.NewMockCluster(mockCtrl)
		session = mock_scheduling.NewMockAbstractSession(mockCtrl)
		abstractServer = &server.AbstractServer{
			DebugMode:  true,
			Log:        config.GetLogger("TestAbstractServer"),
			RequestLog: metrics.NewRequestLog(),
		}

		clusterGateway = &ClusterGatewayImpl{
			cluster: cluster,
		}
		config.InitLogger(&clusterGateway.log, clusterGateway)

		kernel.EXPECT().ConnectionInfo().Return(&types.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
		kernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
			Id:              "66902bac-9386-432e-b1b9-21ac853fa1c9",
			Session:         "66902bac-9386-432e-b1b9-21ac853fa1c9",
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
		kernel.EXPECT().ID().Return("66902bac-9386-432e-b1b9-21ac853fa1c9").AnyTimes()
		cluster.EXPECT().GetSession("66902bac-9386-432e-b1b9-21ac853fa1c9").Return(session, true).AnyTimes()
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Processing 'execute_request' messages", func() {
		var (
			header *types.MessageHeader
		)

		BeforeEach(func() {
			header = &types.MessageHeader{
				MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
				Username: "",
				Session:  "10cb49c9-b17e-425e-9bc1-ee3ff66e6974",
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
			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := types.JupyterFrames(unsignedFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
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
	})

	Context("ZMQ Messages", func() {
		var (
			header *types.MessageHeader
		)

		BeforeEach(func() {
			header = &types.MessageHeader{
				MsgID:    "c7074e5b-b90f-44f8-af5d-63201ec3a527",
				Username: "",
				Session:  "10cb49c9-b17e-425e-9bc1-ee3ff66e6974",
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  "execute_request",
				Version:  "5.2",
			}
		})

		It("Should embed a RequestTrace struct in the buffers frame of a JupyterMessage in DebugMode", func() {
			kernelId := "10cb49c9-b17e-425e-9bc1-ee3ff66e6974"

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := types.JupyterFrames(unsignedFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := types.NewJupyterMessage(msg)
			jMsg.DestinationId = kernelId

			// We'll just call this multiple times.
			requestLogHelper := func(server *server.AbstractServer) {
				Expect(server.RequestLog.Size()).To(Equal(1))
				Expect(server.RequestLog.EntriesByRequestId.Len()).To(Equal(1))
				Expect(server.RequestLog.EntriesByJupyterMsgId.Len()).To(Equal(1))
				Expect(server.RequestLog.RequestsPerKernel.Len()).To(Equal(1))
			}

			// We'll just call this multiple times.
			requestTraceHelper := func(trace *proto.RequestTrace) {
				Expect(trace.MessageId).To(Equal("c7074e5b-b90f-44f8-af5d-63201ec3a527"))
				Expect(trace.MessageType).To(Equal(types.ShellExecuteRequest))
				Expect(trace.KernelId).To(Equal(kernelId))
			}

			requestReceivedByGateway := int64(257894000000)
			requestReceivedByGatewayTs := time.UnixMilli(requestReceivedByGateway) // 2009-11-10 23:00:00 +0000 UTC
			requestTrace, added, err := abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByGatewayTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeTrue())
			Expect(err).To(BeNil())
			requestLogHelper(abstractServer)

			requests, loaded := abstractServer.RequestLog.RequestsPerKernel.Load(kernelId)
			Expect(loaded).To(Equal(true))
			Expect(requests).ToNot(BeNil())
			Expect(requests.Len()).To(Equal(1))

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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestSentByGatewayTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByLocalDaemonTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestSentByLocalDaemonTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, requestReceivedByKernelReplicaTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByKernelReplicaTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replyReceivedByLocalDaemonTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByLocalDaemonTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replyReceivedByGatewayTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
			requestTrace, added, err = abstractServer.AddOrUpdateRequestTraceToJupyterMessage(jMsg, &types.Socket{Type: types.ShellMessage}, replySentByGatewayTs)
			Expect(requestTrace).ToNot(BeNil())
			Expect(added).To(BeFalse())
			Expect(err).To(BeNil())

			requestLogHelper(abstractServer)
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
})
