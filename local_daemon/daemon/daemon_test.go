package daemon

import (
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/mock_client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"github.com/zhangjyr/distributed-notebook/local_daemon/device"
	"github.com/zhangjyr/distributed-notebook/local_daemon/mock_device"
	"go.uber.org/mock/gomock"
)

const (
	signature_scheme string = "hmac-sha256"
)

var _ = Describe("Local Daemon Tests", func() {
	var (
		schedulerDaemon  *SchedulerDaemon
		vgpuPluginServer device.VirtualGpuPluginServer
		mockCtrl         *gomock.Controller
		kernel           *mock_client.MockKernelReplicaClient
		kernel_key       string = "23d90942-8c3de3a713a5c3611792b7a5"
		gpuManager       *GpuManager

		numGPUs int64 = 8
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		vgpuPluginServer = mock_device.NewMockVirtualGpuPluginServer(mockCtrl)
		kernel = mock_client.NewMockKernelReplicaClient(mockCtrl)
		gpuManager = NewGpuManager(numGPUs)

		schedulerDaemon = &SchedulerDaemon{
			transport:              "tcp",
			kernels:                hashmap.NewCornelkMap[string, client.KernelReplicaClient](1000),
			closed:                 make(chan struct{}),
			cleaned:                make(chan struct{}),
			gpuManager:             gpuManager,
			virtualGpuPluginServer: vgpuPluginServer,
		}
		config.InitLogger(&schedulerDaemon.log, schedulerDaemon)

		kernel.EXPECT().ConnectionInfo().Return(&types.ConnectionInfo{SignatureScheme: signature_scheme, Key: kernel_key}).AnyTimes()
		kernel.EXPECT().KernelSpec().Return(&gateway.KernelSpec{
			Id:              "66902bac-9386-432e-b1b9-21ac853fa1c9",
			Session:         "10cb49c9-b17e-425e-9bc1-ee3ff66e6974",
			SignatureScheme: signature_scheme,
			Key:             "23d90942-8c3de3a713a5c3611792b7a5",
			ResourceSpec: &gateway.ResourceSpec{
				Gpu:    2,
				Cpu:    100,
				Memory: 1000,
			},
		}).AnyTimes()
		kernel.EXPECT().ResourceSpec().Return(&gateway.ResourceSpec{
			Gpu:    2,
			Cpu:    100,
			Memory: 1000,
		}).AnyTimes()
		kernel.EXPECT().ReplicaID().Return(int32(1)).AnyTimes()
		kernel.EXPECT().ID().Return("66902bac-9386-432e-b1b9-21ac853fa1c9").AnyTimes()
	})

	Context("Processing 'execute_request' messages", func() {
		var (
			offset int = 0
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

			kernel.EXPECT().SkipIdentities(gomock.Any()).DoAndReturn(func(arg [][]byte) (types.JupyterFrames, int) {
				return arg, 0
			}).AnyTimes()
		})

		It("Should convert the 'execute_request' message to a 'yeild_request' message if there is a different replica specified as the target", func() {
			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"),
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"),
				[]byte(""), /* Header */
				[]byte(""), /* Parent header*/
				[]byte(fmt.Sprintf("{\"%s\": 2}", TargetReplicaArg)), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"),
			}
			jframes := types.JupyterFrames(unsignedFrames)
			jframes.EncodeHeader(header)
			frames, _ := jframes.Sign(signature_scheme, []byte(kernel_key))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}
			processedMessage := schedulerDaemon.processExecuteRequest(msg, kernel, header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(len(processedMessage.Frames)).To(Equal(len(frames)))

			jframesProcessed := types.JupyterFrames(processedMessage.Frames)
			headerFrame := jframesProcessed.HeaderFrame()
			var header types.MessageHeader
			err := headerFrame.Decode(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType).To(Equal(ShellYieldExecute))
		})
		It("Should convert the 'execute_request' message to a 'yeild_request' message if there are insufficient GPUs available", func() {
			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""), /* Header */
				[]byte(""), /* Parent header */
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jframes := types.JupyterFrames(unsignedFrames)
			jframes.EncodeHeader(header)
			frames, _ := jframes.Sign(signature_scheme, []byte(kernel_key))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}

			// Make it so that there are no idle GPUs available.
			gpuManager.idleGPUs = ZeroDecimal.Copy()

			processedMessage := schedulerDaemon.processExecuteRequest(msg, kernel, header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(len(processedMessage.Frames)).To(Equal(len(frames)))

			jframesProcessed := types.JupyterFrames(processedMessage.Frames)
			headerFrame := jframesProcessed.HeaderFrame()
			var header types.MessageHeader
			err := headerFrame.Decode(&header)

			GinkgoWriter.Printf("Header: %v\n", header)

			Expect(err).To(BeNil())
			Expect(header.MsgType).To(Equal(ShellYieldExecute))
		})

		It("Should correctly process an 'execute_request' message", func() {
			unsignedFrames := [][]byte{
				[]byte("<IDS|MSG>"), /* Frame start */
				[]byte("6c7ab7a8c1671036668a06b199919959cf440d1c6cbada885682a90afd025be8"), /* Signature */
				[]byte(""), /* Header */
				[]byte(""), /* Parent header */
				[]byte(""), /* Metadata */
				[]byte("{\"silent\":false,\"store_history\":true,\"user_expressions\":{},\"allow_stdin\":true,\"stop_on_error\":false,\"code\":\"\"}"), /* Content */
			}
			jframes := types.JupyterFrames(unsignedFrames)
			jframes.EncodeHeader(header)
			frames, _ := jframes.Sign(signature_scheme, []byte(kernel_key))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}

			processedMessage := schedulerDaemon.processExecuteRequest(msg, kernel, header, offset)
			Expect(processedMessage).ToNot(BeNil())
			Expect(len(processedMessage.Frames)).To(Equal(len(frames)))

			By("Embedding the idle GPUs in the metadata of the message")
			processedFrames := types.JupyterFrames(processedMessage.Frames)
			metadataFrame := processedFrames.MetadataFrame()
			var metadata map[string]interface{}
			err := metadataFrame.Decode(&metadata)
			Expect(err).To(BeNil())
			Expect(len(metadata)).To(Equal(1))

			By("Creating a pending allocation for the associated kernel")

			GinkgoWriter.Printf("NumPendingAllocations: %d\n", gpuManager.NumPendingAllocations())
			GinkgoWriter.Printf("PendingGPUs: %s\n", gpuManager.PendingGPUs().StringFixed(0))
			GinkgoWriter.Printf("IdleGPUs: %s\n", gpuManager.IdleGPUs().StringFixed(0))
			GinkgoWriter.Printf("gpuManager.GetPendingGPUsAssociatedWithKernel(%d, %s): %s\n", kernel.ReplicaID(), kernel.ID(), gpuManager.GetPendingGPUsAssociatedWithKernel(kernel.ReplicaID(), kernel.ID()).StringFixed(0))

			Expect(gpuManager.NumPendingAllocations()).To(Equal(1))
			Expect(gpuManager.PendingGPUs()).To(Equal(decimal.NewFromFloat(2)))
			Expect(gpuManager.IdleGPUs()).To(Equal(decimal.NewFromFloat(6)))
			Expect(gpuManager.GetPendingGPUsAssociatedWithKernel(kernel.ReplicaID(), kernel.ID())).To(Equal(decimal.NewFromFloat(2)))
		})
	})
})
