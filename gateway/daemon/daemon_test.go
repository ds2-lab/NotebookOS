package daemon

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/mock_client"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	localdaemon "github.com/zhangjyr/distributed-notebook/local_daemon/daemon"
	localdaemondomain "github.com/zhangjyr/distributed-notebook/local_daemon/domain"
	"go.uber.org/mock/gomock"
)

const (
	signature_scheme string = "hmac-sha256"
)

func TestProxy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Daemon Suite")
}

var _ = Describe("Cluster Gateway Tests", func() {
	var (
		clusterGateway *ClusterGatewayImpl
		mockCtrl       *gomock.Controller
		kernel         *mock_client.MockDistributedKernelClient
		kernel_key     string = "23d90942-8c3de3a713a5c3611792b7a5"
	)

	BeforeEach(func() {
		clusterGateway = &ClusterGatewayImpl{
			activeExecutions: hashmap.NewCornelkMap[string, *client.ActiveExecution](64),
		}
		config.InitLogger(&clusterGateway.log, clusterGateway)

		mockCtrl = gomock.NewController(GinkgoT())
		kernel = mock_client.NewMockDistributedKernelClient(mockCtrl)

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
		kernel.EXPECT().ID().Return("66902bac-9386-432e-b1b9-21ac853fa1c9").AnyTimes()
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

			// kernel.EXPECT().SkipIdentities(gomock.Any()).DoAndReturn(func(arg [][]byte) (types.JupyterFrames, int) {
			// 	return arg, 0
			// }).AnyTimes()
			kernel.EXPECT().Size().Return(3).AnyTimes()

			setActiveCall := kernel.EXPECT().SetActiveExecution(gomock.Any())
			kernel.EXPECT().NumActiveAddOperations().Return(0).Times(1)
			kernel.EXPECT().NumActiveAddOperations().After(setActiveCall).Return(1).Times(1)
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
			jframes := types.JupyterFrames(unsignedFrames)
			jframes.EncodeHeader(header)
			frames, _ := jframes.Sign(signature_scheme, []byte(kernel_key))
			msg := &zmq4.Msg{
				Frames: frames,
				Type:   zmq4.UsrMsg,
			}

			Expect(kernel.NumActiveAddOperations()).To(Equal(0))
			clusterGateway.processExecuteRequest(msg, kernel, header)
			Expect(kernel.NumActiveAddOperations()).To(Equal(1))
			Expect(clusterGateway.activeExecutions.Len()).To(Equal(1))
		})
	})

	Context("End-to-End Tests", func() {
		It("Will transmit messages correctly, with ACKs", func() {
			cluster_gateway := New(&types.ConnectionInfo{
				IP:                   "127.0.0.1",
				ControlPort:          11000,
				ShellPort:            11001,
				StdinPort:            11002,
				HBPort:               11003,
				IOPubPort:            11004,
				IOSubPort:            11005,
				AckPort:              11006,
				Transport:            "tcp",
				SignatureScheme:      "hmac-sha256",
				Key:                  "TestKey",
				StartingResourcePort: 11007,
				NumResourcePorts:     64,
			}, &domain.ClusterDaemonOptions{
				ClusterSchedulerOptions: domain.ClusterSchedulerOptions{
					SchedulerHttpPort:             8076,
					GpusPerHost:                   8,
					VirtualGpusPerHost:            72,
					SubscribedRatioUpdateInterval: 1,
					ScalingFactor:                 1,
					ScalingInterval:               1,
					ScalingLimit:                  1,
					MaximumHostsToReleaseAtOnce:   1,
					ScalingOutEnaled:              true,
					ScalingBufferSize:             1,
					MinimumNumNodes:               1,
				},
				LocalDaemonServiceName:        "local-daemon-network",
				LocalDaemonServicePort:        11075,
				GlobalDaemonServicePort:       11075,
				GlobalDaemonServiceName:       "daemon-network",
				SMRPort:                       11080,
				KubeNamespace:                 "default",
				UseStatefulSet:                false,
				HDFSNameNodeEndpoint:          "172.17.0.1:9000",
				SchedulingPolicy:              "static",
				NotebookImageName:             "scusemua/jupyter",
				NotebookImageTag:              "latest",
				DistributedClusterServicePort: 8077,
				DeploymentMode:                "local",
			})
			local_daemon := localdaemon.New(&types.ConnectionInfo{
				IP:                   "127.0.0.1",
				ControlPort:          10000,
				ShellPort:            10001,
				StdinPort:            10002,
				HBPort:               10003,
				IOPubPort:            10004,
				IOSubPort:            10005,
				AckPort:              10006,
				Transport:            "tcp",
				SignatureScheme:      "hmac-sha256",
				Key:                  "TestKey",
				StartingResourcePort: 10007,
				NumResourcePorts:     64,
			}, &localdaemondomain.SchedulerDaemonOptions{
				DirectServer:     false,
				SMRPort:          11080,
				NumGPUs:          8,
				SchedulingPolicy: "static",
				DeploymentMode:   "kubernetes",
			}, 8079, nil, "TestNode")

			go func() {
				err := cluster_gateway.Start()
				Expect(err).To(BeNil())
			}()

			go func() {
				err := local_daemon.Start()
				Expect(err).To(BeNil())
			}()

			time.Sleep(time.Millisecond * 5000)
		})
	})
})
