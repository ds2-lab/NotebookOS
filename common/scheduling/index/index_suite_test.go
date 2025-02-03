package index_test

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"go.uber.org/mock/gomock"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	debugLoggingEnabled = false
)

func init() {
	if os.Getenv("DEBUG") != "" || os.Getenv("VERBOSE") != "" {
		debugLoggingEnabled = true
	}
}

func TestIndex(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Index Suite")
}

// Un-comment this to enable debug logging for all test files in the suite.
var _ = BeforeSuite(func() {
	if debugLoggingEnabled {
		config.LogLevel = logger.LOG_LEVEL_ALL
	}
})

func createHost(idx int, ctrl *gomock.Controller, cluster scheduling.Cluster, hostSpec *types.DecimalSpec) scheduling.UnitTestingHost {
	hostId := fmt.Sprintf("Host%d", idx)
	nodeName := fmt.Sprintf("Host%d", idx)

	resourceSpoofer := distNbTesting.NewResourceSpoofer(nodeName, hostId, hostSpec)
	Expect(resourceSpoofer).ToNot(BeNil())

	host, _, err := distNbTesting.NewHostWithSpoofedGRPC(ctrl, cluster, hostId, nodeName, resourceSpoofer)
	Expect(err).To(BeNil())
	Expect(host).ToNot(BeNil())

	return host
}
