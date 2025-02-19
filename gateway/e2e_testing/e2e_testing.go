package e2e_testing

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	gatewayDaemon "github.com/scusemua/distributed-notebook/gateway/daemon"
	"github.com/scusemua/distributed-notebook/local_daemon/daemon"
	"github.com/scusemua/distributed-notebook/local_daemon/invoker"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"os"
	"time"
)

const (
	dockerInvokerKernelConnInfoIp = "127.0.0.1"
)

var (
	kernelArgv = []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f",
		"{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"}
)

type components struct {
	ClusterGateway     *gatewayDaemon.ClusterGatewayImpl
	LocalDaemons       []*daemon.LocalScheduler
	JupyterServer      *JupyterServer
	GatewayProvisioner *GatewayProvisioner
}

func createComponents(numLocalDaemons int) *components {
	jupyterGrpcPort := 9998
	provisionerPort := 9999
	initialLocalSchedulerGrpcPort := 10256
	provisionerAddr := fmt.Sprintf("localhost:%d", provisionerPort)

	gatewayConnInfo := GetConnectionInfo(provisionerPort + 1)

	clusterGateway := NewGatewayBuilder(scheduling.Static).
		WithDebugLogging().
		WithoutIdleSessionReclamation().
		WithoutPrewarming().
		WithJupyterPort(jupyterGrpcPort).
		WithProvisionerPort(provisionerPort).
		WithConnectionInfo(gatewayConnInfo).
		Build()

	Expect(clusterGateway).NotTo(BeNil())

	numHosts := 3

	daemons := make([]*daemon.LocalScheduler, 0, numHosts)
	closeConnectionFuncs := make([]func(), 0, numHosts)

	basePort := initialLocalSchedulerGrpcPort
	for i := 0; i < numHosts; i++ {
		connInfo := GetConnectionInfo(basePort + 1)

		localDaemon, closeConnections := NewLocalSchedulerBuilder(scheduling.Static).
			WithDebugLogging().
			WithProvisionerAddress(provisionerAddr).
			WithGrpcPort(basePort).
			WithConnInfo(connInfo).
			Build()

		Expect(localDaemon).NotTo(BeNil())
		Expect(closeConnections).NotTo(BeNil())

		daemons = append(daemons, localDaemon)
		closeConnectionFuncs = append(closeConnectionFuncs, closeConnections)

		basePort += connInfo.NumResourcePorts + 8
	}

	defer func() {
		for _, localDaemon := range daemons {
			_ = localDaemon.Close()
		}

		_ = clusterGateway.Close()
	}()

	cluster := clusterGateway.Cluster()
	index, ok := cluster.GetIndex(scheduling.CategoryClusterIndex, "*")
	Expect(ok).To(BeTrue())
	Expect(index).ToNot(BeNil())

	placer := cluster.Placer()
	Expect(placer).ToNot(BeNil())

	scheduler := cluster.Scheduler()
	Expect(scheduler.Placer()).To(Equal(cluster.Placer()))

	Expect(cluster.Len()).To(Equal(numHosts))

	jupyterServer, err := NewJupyterServer(fmt.Sprintf("localhost:%d", jupyterGrpcPort))
	Expect(err).To(BeNil())
	Expect(jupyterServer).NotTo(BeNil())
	Expect(jupyterServer.GatewayProvisioner).NotTo(BeNil())

	return &components{
		ClusterGateway:     clusterGateway,
		LocalDaemons:       daemons,
		JupyterServer:      jupyterServer,
		GatewayProvisioner: jupyterServer.GatewayProvisioner,
	}
}

var _ = Describe("End-to-End Tests", func() {
	BeforeEach(func() {
		err := os.Setenv(invoker.DisableActualContainerCreationEnv, "1")
		Expect(err).To(BeNil())
		Expect(os.Getenv(invoker.DisableActualContainerCreationEnv)).To(Equal("1"))

		err = os.Setenv(invoker.DockerInvokerKernelConnInfoIp, dockerInvokerKernelConnInfoIp)
		Expect(err).To(BeNil())
		Expect(os.Getenv(invoker.DockerInvokerKernelConnInfoIp)).To(Equal(dockerInvokerKernelConnInfoIp))
	})

	Context("Scheduling Kernels", func() {
		It("Will correctly schedule a kernel.", func() {
			components := createComponents(3)
			Expect(components).NotTo(BeNil())

			time.Sleep(time.Second * 1)

			gatewayProvisioner := components.GatewayProvisioner

			kernelId := uuid.NewString()
			kernelKey := ""
			resourceSpec := proto.NewResourceSpec(128, 4096, 2, 4)

			sem := semaphore.NewWeighted(1)
			Expect(sem.TryAcquire(1)).To(BeTrue())

			go func() {
				resp, err := gatewayProvisioner.LocalGatewayClient.StartKernel(context.Background(), &proto.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            kernelArgv,
					SignatureScheme: jupyter.JupyterSignatureScheme,
					Key:             kernelKey,
					ResourceSpec:    resourceSpec,
				})

				Expect(err).To(BeNil())
				Expect(resp).NotTo(BeNil())

				sem.Release(1)
			}()

			Eventually(sem.Acquire(context.Background(), 1), time.Second*5, time.Millisecond*250).Should(Succeed())
		})
	})
})
