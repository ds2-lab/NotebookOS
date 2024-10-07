package daemon

import (
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/mock_scheduling"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	types2 "github.com/zhangjyr/distributed-notebook/common/types"
	"testing"

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
		cluster        *mock_scheduling.MockCluster
		session        *mock_scheduling.MockAbstractSession
		mockCtrl       *gomock.Controller
		kernel         *mock_client.MockAbstractDistributedKernelClient
		kernelKey      = "23d90942-8c3de3a713a5c3611792b7a5"
	)

	BeforeEach(func() {
		clusterGateway = &ClusterGatewayImpl{
			cluster: cluster,
		}
		config.InitLogger(&clusterGateway.log, clusterGateway)

		mockCtrl = gomock.NewController(GinkgoT())
		kernel = mock_client.NewMockAbstractDistributedKernelClient(mockCtrl)
		cluster = mock_scheduling.NewMockCluster(mockCtrl)
		session = mock_scheduling.NewMockAbstractSession(mockCtrl)

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

			Expect(kernel.NumActiveExecutionOperations()).To(Equal(0))
			err = clusterGateway.processExecuteRequest(jMsg, kernel)
			Expect(err).To(BeNil())
			Expect(kernel.NumActiveExecutionOperations()).To(Equal(1))
		})
	})
})
