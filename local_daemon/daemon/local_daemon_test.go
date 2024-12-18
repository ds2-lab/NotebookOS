package daemon

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/configuration"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	types2 "github.com/scusemua/distributed-notebook/common/types"

	"github.com/Scusemua/go-utils/config"
	"github.com/go-zeromq/zmq4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
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
		kernel1          *mock_scheduling.MockKernelReplica
		kernel2          *mock_scheduling.MockKernelReplica
		kernel1Key       = "23d90942-8c3de3a713a5c3611792b7a5"
		kernel2Key       = "d2324990-3563adca181e235c77317a9b"
		kernel1Id        = "66902bac-9386-432e-b1b9-21ac853fa1c9"
		kernel2Id        = "c8fd0d64-b35d-4e14-80fa-4ed2d399bcb6"
		resourceManager  *resource.AllocationManager
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		vgpuPluginServer = mock_device.NewMockVirtualGpuPluginServer(mockCtrl)
		kernel1 = mock_scheduling.NewMockKernelReplica(mockCtrl)
		kernel2 = mock_scheduling.NewMockKernelReplica(mockCtrl)
		resourceManager = resource.NewAllocationManager(&types2.DecimalSpec{
			GPUs:      decimal.NewFromFloat(8),
			Millicpus: decimal.NewFromFloat(64000),
			MemoryMb:  decimal.NewFromFloat(128000),
			VRam:      decimal.NewFromFloat(32),
		})

		schedulingPolicy, err := policy.GetSchedulingPolicy(&scheduling.SchedulerOptions{
			CommonOptions: configuration.CommonOptions{
				SchedulingPolicy:             string(scheduling.Static),
				IdleSessionReclamationPolicy: string(scheduling.NoIdleSessionReclamation),
			},
		})
		Expect(err).To(BeNil())
		Expect(schedulingPolicy).ToNot(BeNil())

		schedulerDaemon = &SchedulerDaemonImpl{
			transport:              "tcp",
			kernels:                hashmap.NewCornelkMap[string, scheduling.KernelReplica](1000),
			closed:                 make(chan struct{}),
			cleaned:                make(chan struct{}),
			resourceManager:        resourceManager,
			virtualGpuPluginServer: vgpuPluginServer,
			schedulingPolicy:       schedulingPolicy,
		}
		config.InitLogger(&schedulerDaemon.log, schedulerDaemon)

		kernel1.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernel1Key}).AnyTimes()
		kernel1.EXPECT().KernelSpec().Return(&proto.KernelSpec{
			Id:              kernel1Id,
			Session:         kernel1Id,
			SignatureScheme: signatureScheme,
			Key:             kernel1Key,
			ResourceSpec: &proto.ResourceSpec{
				Gpu:    2,
				Cpu:    100,
				Memory: 1000,
				Vram:   4,
			},
		}).AnyTimes()
		kernel1.EXPECT().ResourceSpec().Return(&types2.DecimalSpec{
			GPUs:      decimal.NewFromFloat(2),
			Millicpus: decimal.NewFromFloat(100),
			MemoryMb:  decimal.NewFromFloat(1000),
			VRam:      decimal.NewFromFloat(4),
		}).AnyTimes()
		kernel1.EXPECT().ReplicaID().Return(int32(1)).AnyTimes()
		kernel1.EXPECT().ID().Return(kernel1Id).AnyTimes()
		kernel1.EXPECT().WaitForPendingExecuteRequests().AnyTimes()

		kernel2.EXPECT().ConnectionInfo().Return(&jupyter.ConnectionInfo{SignatureScheme: signatureScheme, Key: kernel2Key}).AnyTimes()
		kernel2.EXPECT().ID().Return(kernel2Id).AnyTimes()
		kernel2.EXPECT().KernelSpec().Return(&proto.KernelSpec{
			Id:              kernel2Id,
			Session:         kernel2Id,
			SignatureScheme: signatureScheme,
			Key:             kernel2Key,
			ResourceSpec: &proto.ResourceSpec{
				Gpu:    4,
				Cpu:    2048,
				Memory: 1250,
				Vram:   12,
			},
		}).AnyTimes()
		kernel2.EXPECT().ResourceSpec().Return(&types2.DecimalSpec{
			GPUs:      decimal.NewFromFloat(4),
			Millicpus: decimal.NewFromFloat(2048),
			MemoryMb:  decimal.NewFromFloat(1250),
			VRam:      decimal.NewFromFloat(12),
		}).AnyTimes()
		kernel2.EXPECT().ReplicaID().Return(int32(2)).AnyTimes()
		kernel2.EXPECT().WaitForPendingExecuteRequests().AnyTimes()
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
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

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
			processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			var header *messaging.MessageHeader
			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %s\n", header.String())

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
		})

		It("Should convert the 'execute_request' message to a 'yield_request' message if there are insufficient GPUs available", func() {
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()

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

			processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1) // , headerKernel1, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			err = processedMessage.JupyterFrames.DecodeHeader(&headerKernel1)

			GinkgoWriter.Printf("Header: %v\n", headerKernel1)

			Expect(err).To(BeNil())
			Expect(headerKernel1.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
		})

		It("Should correctly return a 'yield_request' message if the kernel1 is set to yield the next execute request.", func() {
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(true).AnyTimes()
			kernel1.EXPECT().YieldedNextExecutionRequest().Times(1)
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

			processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1) // , headerKernel1, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			var header messaging.MessageHeader
			err = processedMessage.JupyterFrames.DecodeHeader(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType.String()).To(Equal(messaging.ShellYieldRequest))
		})

		It("Should correctly return two different signatures when the Jupyter message's headerKernel1 is changed by modifying the date.", func() {
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

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
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			err := resourceManager.KernelReplicaScheduled(kernel1.ReplicaID(), kernel1.ID(), kernel1.ResourceSpec())
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
			processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernel1) // , headerKernel1, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(processedMessage.JupyterFrames.Len()).To(Equal(len(frames)))

			By("Embedding the idle GPUs in the metadata of the message")
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

			By("Creating a pending allocation for the associated kernel1")

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
		createJupyterMessage := func(messageType messaging.JupyterMessageType, kernelId string, kernelKey string) *messaging.JupyterMessage {
			header := &messaging.MessageHeader{
				MsgID:    uuid.NewString(),
				Username: kernelId,
				Session:  kernelId,
				Date:     "2024-04-03T22:55:52.605Z",
				MsgType:  messageType,
				Version:  "5.2",
			}

			unsignedExecReqFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""),   /* Header */
				[]byte(""),   /* Parent header */
				[]byte("{}"), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jFrames := messaging.NewJupyterFramesFromBytes(unsignedExecReqFrames)
			err := jFrames.EncodeHeader(header)
			Expect(err).To(BeNil())

			frames, _ := jFrames.Sign(signatureScheme, []byte(kernelKey))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}

			return messaging.NewJupyterMessage(msg)
		}

		processExecuteRequest := func(messageType messaging.JupyterMessageType, kernelReplica scheduling.KernelReplica) *messaging.JupyterMessage {
			jMsg := createJupyterMessage(messageType, kernelReplica.ID(), kernelReplica.ConnectionInfo().Key)
			processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernelReplica) // , header, offset)

			return processedMessage
		}

		It("Should allocate resources to multiple kernels upon receiving 'execute_request' messages", func() {
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			kernel2.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel2.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			err := resourceManager.KernelReplicaScheduled(kernel1.ReplicaID(), kernel1.ID(), kernel1.ResourceSpec())
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(kernel2.ReplicaID(), kernel2.ID(), kernel2.ResourceSpec())
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

			processedMessage := processExecuteRequest(messaging.ShellExecuteRequest, kernel1)
			Expect(processedMessage).ToNot(BeNil())

			By("Embedding the idle GPUs in the metadata of the message for Kernel 1")
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

			By("Creating a pending allocation for Kernel 2")

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

			processedMessage = processExecuteRequest(messaging.ShellExecuteRequest, kernel2)
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
			kernel1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			err := resourceManager.KernelReplicaScheduled(kernel1.ReplicaID(), kernel1.ID(), kernel1.ResourceSpec())
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))

			Expect(resourceManager.PendingCPUs().Equals(kernel1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(resourceManager.PendingVRAM().Equals(kernel1.ResourceSpec().VRam)).To(BeTrue())
			Expect(resourceManager.PendingMemoryMB().Equals(kernel1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())

			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.CommittedCPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.CommittedVRamGB().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.CommittedMemoryMB().Equals(decimal.Zero)).To(BeTrue())

			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())
			Expect(resourceManager.IdleVRamGB().Equals(decimal.NewFromFloat(32))).To(BeTrue())
			Expect(resourceManager.IdleMemoryMB().Equals(decimal.NewFromFloat(128000))).To(BeTrue())
			Expect(resourceManager.IdleCPUs().Equals(decimal.NewFromFloat(64000))).To(BeTrue())

			allocation, exists := resourceManager.GetAllocation(kernel1.ReplicaID(), kernel1.ID())
			Expect(exists).To(BeTrue())
			Expect(allocation).ToNot(BeNil())

			Expect(allocation.ReplicaId).To(Equal(kernel1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1.ID()))

			Expect(allocation.GPUs.Equal(kernel1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

			processedMessage := processExecuteRequest(messaging.ShellExecuteRequest, kernel1)
			Expect(processedMessage).ToNot(BeNil())

			By("Embedding the idle GPUs in the metadata of the message for Kernel 1")
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

			By("Creating a pending allocation for Kernel 2")

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

			allocation, exists = resourceManager.GetAllocation(kernel1.ReplicaID(), kernel1.ID())
			Expect(exists).To(BeTrue())
			Expect(allocation).ToNot(BeNil())

			Expect(allocation.ReplicaId).To(Equal(kernel1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1.ID()))

			Expect(allocation.GPUs.Equal(kernel1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeTrue())

			kernel1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

			leadTaskMsg := createJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1.ID(), kernel1.ConnectionInfo().Key)
			err = schedulerDaemon.handleSMRLeadTask(kernel1, leadTaskMsg.JupyterFrames, leadTaskMsg)
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(0))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedGpuDevices()).To(Equal(2))
			Expect(resourceManager.NumAvailableGpuDevices()).To(Equal(6))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			Expect(resourceManager.CommittedVRamGB().Equals(decimal.NewFromFloat(4))).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(0))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())

			allocation, exists = resourceManager.GetAllocation(kernel1.ReplicaID(), kernel1.ID())
			Expect(exists).To(BeTrue())
			Expect(allocation).ToNot(BeNil())

			Expect(allocation.ReplicaId).To(Equal(kernel1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1.ID()))

			Expect(allocation.GPUs.Equal(kernel1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeFalse())
		})
	})
})
