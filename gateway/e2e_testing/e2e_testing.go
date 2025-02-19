package e2e_testing

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

var _ = Describe("End-to-End Tests", func() {
	Context("Scheduling Kernels", func() {
		var (
			provisionerPort = 10000
			provisionerAddr = fmt.Sprintf("localhost:%d", provisionerPort)
		)

		It("Will schedule kernels", func() {
			clusterGateway := NewGatewayBuilder(scheduling.Static).
				WithDebugLogging().
				WithoutIdleSessionReclamation().
				WithoutPrewarming().
				WithProvisionerPort(10000).
				Build()

			localDaemon, closeConnections := NewLocalSchedulerBuilder(scheduling.Static).
				WithDebugLogging().
				WithProvisionerAddress(provisionerAddr).
				Build()

			Expect(localDaemon).NotTo(BeNil())
			Expect(clusterGateway).NotTo(BeNil())
			Expect(closeConnections).NotTo(BeNil())

			defer closeConnections()
		})
	})
})
