package prewarm_test

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"go.uber.org/mock/gomock"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPrewarm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Prewarm Suite")
}

var (
	debugLoggingEnabled = false
	globalLogger        = config.GetLogger("")
	hostSpec            = types.NewDecimalSpec(128000, 256000, 8, 40)
)

func init() {
	if os.Getenv("DEBUG") != "" || os.Getenv("VERBOSE") != "" {
		debugLoggingEnabled = true
	}
}

var _ = BeforeSuite(func() {
	if debugLoggingEnabled {
		config.LogLevel = logger.LOG_LEVEL_ALL
	}
})

// createHosts creates n scheduling.Host instances that each use a mocked proto.LocalGatewayClient (i.e., a
// mock_proto.MockLocalGatewayClient).
func createHosts(n, startIdx int, spec types.Spec, cluster scheduling.Cluster, ctrl *gomock.Controller) ([]scheduling.Host, []*mock_proto.MockLocalGatewayClient) {
	hosts := make([]scheduling.Host, 0, n)
	localGatewayClients := make([]*mock_proto.MockLocalGatewayClient, 0, n)

	for i := startIdx; i < startIdx+n; i++ {
		id := fmt.Sprintf("host%d", i)
		name := fmt.Sprintf("host%d", i)

		GinkgoWriter.Printf("Creating host #%d with name=\"%s\", id=\"%s\"\n", i, name, id)

		spoofer := distNbTesting.NewResourceSpoofer(name, id, spec)
		Expect(spoofer).ToNot(BeNil())

		host, localGatewayClient, err := distNbTesting.NewHostWithSpoofedGRPC(ctrl, cluster, id, name, spoofer)
		Expect(err).To(BeNil())
		Expect(host).ToNot(BeNil())

		hosts = append(hosts, host)
		localGatewayClients = append(localGatewayClients, localGatewayClient)
	}

	return hosts, localGatewayClients
}
