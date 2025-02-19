package e2e_testing

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/local_daemon/daemon"
	"time"
)

var _ = Describe("End-to-End Tests", func() {
	Context("Scheduling Kernels", func() {
		var (
			jupyterGrpcPort               = 9998
			provisionerPort               = 9999
			initialLocalSchedulerGrpcPort = 12000
			provisionerAddr               = fmt.Sprintf("localhost:%d", provisionerPort)
		)

		It("Will schedule kernels", func() {
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

			time.Sleep(time.Second * 2)
		})
	})
})
