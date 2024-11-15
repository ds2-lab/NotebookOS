package daemon

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	types2 "github.com/scusemua/distributed-notebook/common/types"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter/client"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/mock_client"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/local_daemon/device"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
	"github.com/scusemua/distributed-notebook/local_daemon/mock_device"
	"github.com/shopspring/decimal"
	"go.uber.org/mock/gomock"
)

const (
	signatureScheme string = "hmac-sha256"
)

var _ = Describe("Local Daemon Tests", func() {
	var (
		schedulerDaemon  *SchedulerDaemonImpl
		vgpuPluginServer device.VirtualGpuPluginServer
		mockCtrl         *gomock.Controller
		kernel           *mock_client.MockAbstractKernelClient
		kernelKey        = "23d90942-8c3de3a713a5c3611792b7a5"
		resourceManager  *scheduling.ResourceManager
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		vgpuPluginServer = mock_device.NewMockVirtualGpuPluginServer(mockCtrl)
		kernel = mock_client.NewMockAbstractKernelClient(mockCtrl)
		resourceManager = scheduling.NewResourceManager(&types2.DecimalSpec{
			GPUs:      decimal.NewFromFloat(8),
			Millicpus: decimal.NewFromFloat(64000),
			MemoryMb:  decimal.NewFromFloat(8000),
		})

		schedulerDaemon = &SchedulerDaemonImpl{
			transport:              "tcp",
			kernels:                hashmap.NewCornelkMap[string, *client.KernelReplicaClient](1000),
			closed:                 make(chan struct{}),
			cleaned:                make(chan struct{}),
			resourceManager:        resourceManager,
			virtualGpuPluginServer: vgpuPluginServer,
		}
		config.InitLogger(&schedulerDaemon.log, schedulerDaemon)

		kernel.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernelKey}).AnyTimes()
		kernel.EXPECT().KernelSpec().Return(&proto.KernelSpec{
			Id:              "66902bac-9386-432e-b1b9-21ac853fa1c9",
			Session:         "10cb49c9-b17e-425e-9bc1-ee3ff66e6974",
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
		kernel.EXPECT().ReplicaID().Return(int32(1)).AnyTimes()
		kernel.EXPECT().ID().Return("66902bac-9386-432e-b1b9-21ac853fa1c9").AnyTimes()
		kernel.EXPECT().WaitForPendingExecuteRequests().AnyTimes()
	})

	Context("Processing 'execute_request' messages", func() {
		var (
			// offset int = 0
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

			// kernel.EXPECT().SkipIdentities(gomock.Any()).DoAndReturn(func(arg [][]byte) (messaging.JupyterFrames, int) {
			// 	return arg, 0
			// }).AnyTimes()
		})

		It("Should convert the 'execute_request' message to a 'yield_request' message if there is a different replica specified as the target", func() {
			kernel.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", domain.TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)
			processedMessage := schedulerDaemon.processExecuteRequest(jMsg, kernel)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			var header *types.MessageHeader
			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %s\n", header.String())

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(types.ShellYieldRequest))
		})

		It("Should convert the 'execute_request' message to a 'yield_request' message if there are insufficient GPUs available", func() {
			kernel.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""), /* Header */
				[]byte(""), /* Parent header */
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)

			// Make it so that there are no idle GPUs available.
			resourceManager.DebugSetIdleGPUs(0)

			processedMessage := schedulerDaemon.processExecuteRequest(jMsg, kernel) // , header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(types.ShellYieldRequest))
		})

		It("Should correctly return a 'yield_request' message if the kernel is set to yield the next execute request.", func() {
			kernel.EXPECT().SupposedToYieldNextExecutionRequest().Return(true).AnyTimes()
			kernel.EXPECT().YieldedNextExecutionRequest().Times(1)
			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""), /* Header */
				[]byte(""), /* Parent header */
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)
			// Make it so that there are no idle GPUs available.
			resourceManager.DebugSetIdleGPUs(0)

			processedMessage := schedulerDaemon.processExecuteRequest(jMsg, kernel) // , header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			var header types.MessageHeader
			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(types.ShellYieldRequest))
		})

		It("Should correctly return two different signatures when the Jupyter message's header is changed by modifying the date.", func() {
			kernel.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			unsignedFrames1 := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte("{\"msg_id\":\"84f3e8e7-1aa96818ad5b99a0f38802ac_17_100\",\"msg_type\":\"ACK\",\"username\":\"username\",\"session\":\"84f3e8e7-1aa96818ad5b99a0f38802ac\",\"date\":\"2024-06-04T22:38:56.949010\",\"version\":\"5.3\"}"),                                                                /* Header */
				[]byte("{\"parent_header\":{\"msg_id\":\"2dbc4069-d766-4c23-8eba-c15069760869\",\"username\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"session\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"date\":\"2024-06-04T22:38:56.949010\",\"msg_type\":\"kernel_info_request\",\"version\":\"5.2\"}}"), /* Parent header */
				[]byte("{}"), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames1 := messaging.NewJupyterFramesFromBytes(unsignedFrames1)
			frames1, err := jFrames1.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			Expect(frames1).ToNot(BeNil())
			signature1 := frames1[types.JupyterFrameSignature]

			unsignedFrames2 := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte("{\"msg_id\":\"84f3e8e7-1aa96818ad5b99a0f38802ac_17_100\",\"msg_type\":\"ACK\",\"username\":\"username\",\"session\":\"84f3e8e7-1aa96818ad5b99a0f38802ac\",\"date\":\"2024-06-04T22:38:56.949011\",\"version\":\"5.3\"}"),                                                                /* Header */
				[]byte("{\"parent_header\":{\"msg_id\":\"2dbc4069-d766-4c23-8eba-c15069760869\",\"username\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"session\":\"c49d4463-b47b-4975-82cc-3444e2df9ca1\",\"date\":\"2024-06-04T22:38:56.949010\",\"msg_type\":\"kernel_info_request\",\"version\":\"5.2\"}}"), /* Parent header */
				[]byte("{}"), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames2 := messaging.NewJupyterFramesFromBytes(unsignedFrames2)
			frames2, err := jFrames2.Sign(signatureScheme, []byte(kernelKey))
			Expect(err).To(BeNil())
			Expect(frames2).ToNot(BeNil())
			signature2 := frames2[types.JupyterFrameSignature]

			fmt.Printf("Signature #1: \"%s\"\n", signature1)
			fmt.Printf("Signature #2: \"%s\"\n", signature2)

			Expect(signature1).ToNot(Equal(signature2))
		})

		It("Should correctly process and return an 'execute_request' message if there are no reasons to convert it to a YIELD request", func() {
			kernel.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			err := resourceManager.KernelReplicaScheduled(kernel.ReplicaID(), kernel.ID(), kernel.ResourceSpec())
			Expect(err).To(BeNil())

			GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
			GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(0))
			GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(0))
			GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(0))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""), /* Header */
				[]byte(""), /* Parent header */
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedFrames)
			err = jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())
			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			jMsg := messaging.NewJupyterMessage(msg)
			processedMessage := schedulerDaemon.processExecuteRequest(jMsg, kernel) // , header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			By("Embedding the idle GPUs in the metadata of the message")
			var metadata map[string]interface{}
			err = processedMessage.JupyterFrames.DecodeMetadata(&metadata)
			GinkgoWriter.Printf("metadata: %v\n", metadata)
			Expect(err).To(BeNil())
			Expect(len(metadata)).To(Equal(6))

			var header types.MessageHeader
			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(types.ShellExecuteRequest))

			By("Creating a pending allocation for the associated kernel")

			GinkgoWriter.Printf("NumPendingAllocations: %d\n", resourceManager.NumPendingAllocations())
			GinkgoWriter.Printf("PendingGPUs: %s\n", resourceManager.PendingGPUs().StringFixed(0))
			GinkgoWriter.Printf("IdleGPUs: %s\n", resourceManager.IdleGPUs().StringFixed(0))
			GinkgoWriter.Printf("CommittedGPUs: %s\n", resourceManager.CommittedGPUs().StringFixed(0))

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
		})
	})
})
