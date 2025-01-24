package daemon

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/configuration"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/mock_scheduling"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/test_utils"
	"github.com/scusemua/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"sync"

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

func processExecuteRequestWithUpdatedResourceSpec(schedulerDaemon *SchedulerDaemonImpl, messageType messaging.JupyterMessageType, kernelReplica scheduling.KernelReplica, updatedResourceSpec *types.Float64Spec) *messaging.JupyterMessage {
	metadata := map[string]interface{}{
		"resource_request": updatedResourceSpec,
	}

	jMsg := test_utils.CreateJupyterMessageWithMetadata(messageType, kernelReplica.ID(), kernelReplica.ConnectionInfo().Key, metadata)
	processedMessage := schedulerDaemon.processExecOrYieldRequest(jMsg, kernelReplica) // , header, offset)

	return processedMessage
}

func processExecuteRequest(schedulerDaemon *SchedulerDaemonImpl, messageType messaging.JupyterMessageType, kernelReplica scheduling.KernelReplica) *messaging.JupyterMessage {
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
	var (
		schedulerDaemon  *SchedulerDaemonImpl
		vgpuPluginServer device.VirtualGpuPluginServer
		mockCtrl         *gomock.Controller
		kernel1Replica1  *mock_scheduling.MockKernelReplica
		kernel2Replica2  *mock_scheduling.MockKernelReplica
		//kernel3Replica3  *mock_scheduling.MockKernelReplica
		resourceManager *resource.AllocationManager
		hostSpec        *types.DecimalSpec

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
		//	WorkloadId:      workloadId,
		//}
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		vgpuPluginServer = mock_device.NewMockVirtualGpuPluginServer(mockCtrl)
		hostSpec = &types.DecimalSpec{
			GPUs:      decimal.NewFromFloat(8),
			Millicpus: decimal.NewFromFloat(64000),
			MemoryMb:  decimal.NewFromFloat(128000),
			VRam:      decimal.NewFromFloat(32),
		}

		kernel1Replica1 = createKernelReplica(mockCtrl, kernel1Id, kernel1Key, workloadId, 1, kernel1Spec, kernel1ResourceSpec)
		kernel2Replica2 = createKernelReplica(mockCtrl, kernel2Id, kernel2Key, workloadId, 2, kernel2Spec, kernel2ResourceSpec)
		//kernel3Replica3 = createKernelReplica(mockCtrl, kernel3Id, kernel3Key, workloadId, 3, kernel3Spec, kernel3ResourceSpec)
		resourceManager = resource.NewAllocationManager(hostSpec)

		schedulingPolicy, err := scheduler.GetSchedulingPolicy(&scheduling.SchedulerOptions{
			CommonOptions: configuration.CommonOptions{
				SchedulingPolicy:             string(scheduling.Static),
				IdleSessionReclamationPolicy: string(scheduling.NoIdleSessionReclamation),
			},
		})
		Expect(err).To(BeNil())
		Expect(schedulingPolicy).ToNot(BeNil())

		schedulerDaemon = &SchedulerDaemonImpl{
			transport:                          "tcp",
			kernels:                            hashmap.NewCornelkMap[string, scheduling.KernelReplica](1000),
			closed:                             make(chan struct{}),
			cleaned:                            make(chan struct{}),
			resourceManager:                    resourceManager,
			virtualGpuPluginServer:             vgpuPluginServer,
			schedulingPolicy:                   schedulingPolicy,
			outgoingExecuteRequestQueue:        hashmap.NewCornelkMap[string, chan *enqueuedExecOrYieldRequest](128),
			outgoingExecuteRequestQueueMutexes: hashmap.NewCornelkMap[string, *sync.Mutex](128),
			executeRequestQueueStopChannels:    hashmap.NewCornelkMap[string, chan interface{}](128),
		}
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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

				Expect(allocation.ReplicaId).To(Equal(pendingKernelReplica.ReplicaID()))
				Expect(allocation.KernelId).To(Equal(pendingKernelReplica.ID()))

				Expect(allocation.GPUs.Equal(pendingKernelReplica.ResourceSpec().GPUs)).To(BeTrue())
				Expect(allocation.Millicpus.Equal(pendingKernelReplica.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(allocation.MemoryMB.Equal(pendingKernelReplica.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(allocation.VramGB.Equal(pendingKernelReplica.ResourceSpec().VRam)).To(BeTrue())

				Expect(allocation.IsPending()).To(BeTrue())
				Expect(allocation.IsCommitted()).To(BeFalse())
				Expect(allocation.IsReservation).To(BeFalse())
			}
		}

		It("Should allocate resources to multiple kernels upon receiving 'execute_request' messages", func() {
			kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			kernel2Replica2.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel2Replica2.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(1))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(2))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

			err = resourceManager.KernelReplicaScheduled(kernel2Replica2.ReplicaID(), kernel2Replica2.ID(), kernel2Replica2.ResourceSpec())
			Expect(err).To(BeNil())

			Expect(resourceManager.NumPendingAllocations()).To(Equal(2))
			Expect(resourceManager.NumCommittedAllocations()).To(Equal(0))
			Expect(resourceManager.CommittedGPUs().Equals(decimal.Zero)).To(BeTrue())
			Expect(resourceManager.PendingGPUs().Equals(decimal.NewFromFloat(6))).To(BeTrue())
			Expect(resourceManager.IdleGPUs().Equals(decimal.NewFromFloat(8))).To(BeTrue())

			processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

			By("Committing resources (as a reservation) when an 'execute_request' message is received")

			processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeTrue())

			kernel1Replica1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeFalse())
		})

		It("Should release resources that were fully committed to a kernel replica if that kernel replica is stopped", func() {
			kernel1Replica1.EXPECT().SupposedToYieldNextExecutionRequest().Return(false).AnyTimes()
			kernel1Replica1.EXPECT().YieldedNextExecutionRequest().Return().AnyTimes()

			schedulerDaemon.kernels.Store(kernel1Replica1.ID(), kernel1Replica1)

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

			By("Committing resources (as a reservation) when an 'execute_request' message is received")

			processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeTrue())

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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeTrue())

			kernel1Replica1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeFalse())

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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeTrue())
			Expect(allocation.IsCommitted()).To(BeFalse())
			Expect(allocation.IsReservation).To(BeFalse())

			By("Committing resources (as a reservation) when an 'execute_request' message is received")

			processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeTrue())

			By("Promoting the committed resource reservation to a fully-committed allocation when an 'smr_lead_task' message is received")

			kernel1Replica1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

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

			Expect(allocation.ReplicaId).To(Equal(kernel1Replica1.ReplicaID()))
			Expect(allocation.KernelId).To(Equal(kernel1Replica1.ID()))

			Expect(allocation.GPUs.Equal(kernel1Replica1.ResourceSpec().GPUs)).To(BeTrue())
			Expect(allocation.Millicpus.Equal(kernel1Replica1.ResourceSpec().Millicpus)).To(BeTrue())
			Expect(allocation.MemoryMB.Equal(kernel1Replica1.ResourceSpec().MemoryMb)).To(BeTrue())
			Expect(allocation.VramGB.Equal(kernel1Replica1.ResourceSpec().VRam)).To(BeTrue())

			Expect(allocation.IsPending()).To(BeFalse())
			Expect(allocation.IsCommitted()).To(BeTrue())
			Expect(allocation.IsReservation).To(BeFalse())

			By("Releasing the resources once training ends")

			kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any()).Times(1)
			kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.").Times(1).Return(nil)

			executeReplyContent := map[string]interface{}{"status": "ok"}
			executeReplyMsg := test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent)
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

			err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
			Expect(err).To(BeNil())

			validatePending([]scheduling.KernelReplica{kernel1Replica1})

			err = resourceManager.KernelReplicaScheduled(kernel2Replica2.ReplicaID(), kernel2Replica2.ID(), kernel2Replica2.ResourceSpec())
			Expect(err).To(BeNil())

			validatePending([]scheduling.KernelReplica{kernel1Replica1, kernel2Replica2})
		})

		Context("Adjusting resource specs", func() {
			updateKernelResourceSpec := func(kernelReplica *mock_scheduling.MockKernelReplica, newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
				GinkgoWriter.Printf("Updating resource spec of Kernel 1 Replica 1 from %v to %v.\n", kernelReplica.ResourceSpec(), newSpec)

				Expect(tx).To(BeNil())

				currentSpec := kernelReplica.ResourceSpec()

				currentSpec.Millicpus = decimal.NewFromFloat(newSpec.CPU())
				currentSpec.MemoryMb = decimal.NewFromFloat(newSpec.MemoryMB())
				currentSpec.GPUs = decimal.NewFromFloat(newSpec.GPU())
				currentSpec.VRam = decimal.NewFromFloat(newSpec.VRAM())

				GinkgoWriter.Printf("Kernel 1 Replica 1 resource spec post-modification: %v\n", kernelReplica.ResourceSpec())

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

				Expect(allocation.ReplicaId).To(Equal(kernelReplica.ReplicaID()))
				Expect(allocation.KernelId).To(Equal(kernelReplica.ID()))

				Expect(allocation.GPUs.Equal(kernelReplica.ResourceSpec().GPUs)).To(BeTrue())
				Expect(allocation.Millicpus.Equal(kernelReplica.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(allocation.MemoryMB.Equal(kernelReplica.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(allocation.VramGB.Equal(kernelReplica.ResourceSpec().VRam)).To(BeTrue())

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation).To(BeTrue())

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

				Expect(allocation.ReplicaId).To(Equal(kernelReplica.ReplicaID()))
				Expect(allocation.KernelId).To(Equal(kernelReplica.ID()))

				Expect(allocation.GPUs.Equal(kernelReplica.ResourceSpec().GPUs)).To(BeTrue())
				Expect(allocation.Millicpus.Equal(kernelReplica.ResourceSpec().Millicpus)).To(BeTrue())
				Expect(allocation.MemoryMB.Equal(kernelReplica.ResourceSpec().MemoryMb)).To(BeTrue())
				Expect(allocation.VramGB.Equal(kernelReplica.ResourceSpec().VRam)).To(BeTrue())

				Expect(allocation.IsPending()).To(BeFalse())
				Expect(allocation.IsCommitted()).To(BeTrue())
				Expect(allocation.IsReservation).To(BeFalse())

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

				err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
				Expect(err).To(BeNil())

				validatePending([]scheduling.KernelReplica{kernel1Replica1})

				By("Committing resources (as a reservation) when an 'execute_request' message is received")

				processedMessage := processExecuteRequest(schedulerDaemon, messaging.ShellExecuteRequest, kernel1Replica1)
				Expect(processedMessage).ToNot(BeNil())

				validateCommittedReserved(kernel1Replica1)

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

				By("Promoting the committed resource reservation to a fully-committed allocation when an 'smr_lead_task' message is received")

				kernel1Replica1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

				leadTaskMsg := test_utils.CreateJupyterMessage(messaging.MessageTypeSMRLeadTask, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key)
				err = schedulerDaemon.handleSMRLeadTask(kernel1Replica1, leadTaskMsg.JupyterFrames, leadTaskMsg)
				Expect(err).To(BeNil())

				validateCommittedFully(kernel1Replica1)

				By("Releasing the resources once training ends")

				kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any()).Times(1)
				kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.").Times(1).Return(nil)

				executeReplyContent := map[string]interface{}{"status": "ok"}
				executeReplyMsg := test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent)
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

					kernel1Replica1.EXPECT().KernelStartedTraining().Times(1).Return(nil)

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

					kernel1Replica1.EXPECT().ReceivedExecuteReply(gomock.Any()).Times(1)
					kernel1Replica1.EXPECT().KernelStoppedTraining("Received \"execute_reply\" message, indicating that the training has stopped.").Times(1).Return(nil)
					executeReplyContent = map[string]interface{}{"status": "ok"}
					executeReplyMsg = test_utils.CreateJupyterMessageWithContent(messaging.ShellExecuteReply, kernel1Replica1.ID(), kernel1Replica1.ConnectionInfo().Key, executeReplyContent)
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

				err := resourceManager.KernelReplicaScheduled(kernel1Replica1.ReplicaID(), kernel1Replica1.ID(), kernel1Replica1.ResourceSpec())
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
