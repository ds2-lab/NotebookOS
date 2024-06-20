package e2e_testing

import (
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/gateway/daemon"

	clustergateway "github.com/zhangjyr/distributed-notebook/gateway/daemon"
	clustergatewaydomain "github.com/zhangjyr/distributed-notebook/gateway/domain"
	local_daemon "github.com/zhangjyr/distributed-notebook/local_daemon/daemon"
	local_daemon_domain "github.com/zhangjyr/distributed-notebook/local_daemon/domain"
)

var (
	sig = make(chan os.Signal, 1)
)

func validateOptions(opts config.Options, configPath string) {
	flags, err := config.ValidateOptionsWithFlags(opts, "-yaml", configPath, "-debug")
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
}

func getGatewayOptions(configPath string) *clustergatewaydomain.ClusterGatewayOptions {
	options := &clustergatewaydomain.ClusterGatewayOptions{}
	validateOptions(options, configPath)
	return options
}

func getLocalDaemonOptions(configPath string) *local_daemon_domain.LocalDaemonOptions {
	options := &local_daemon_domain.LocalDaemonOptions{}
	validateOptions(options, configPath)
	return options
}

var _ = Describe("End to End (E2E) Tests", func() {
	BeforeEach(func() {
		config.LogLevel = logger.LOG_LEVEL_ALL

		os.Setenv("NODE_NAME", types.LocalNode)
	})

	Context("E2E Connectivity", func() {
		It("Will enable kernels to register", func() {
			var done sync.WaitGroup

			// Create the Cluster Gateway.
			gatewayOptions := getGatewayOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/gateway-daemon.yml")
			gateway, _ := clustergateway.CreateAndStartClusterGatewayComponents(gatewayOptions, &done, finalizeGateway, sig)

			Expect(gateway).ToNot(BeNil())

			time.Sleep(time.Second * 5)

			// Create the Local Daemon(s).
			localDaemon1Options := getLocalDaemonOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/local-daemon0.yml")
			localDaemon1 := local_daemon.CreateAndStartLocalDaemonComponents(localDaemon1Options, &done, finalizeLocalDaemon, sig)

			Expect(localDaemon1).ToNot(BeNil())

			time.Sleep(time.Second * 5)
		})
	})
})

func finalizeLocalDaemon(fix bool) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		fmt.Printf("[ERROR] finalizeLocalDaemon: %v\n", err)
	}

	log.Println("Finalize called. Will be terminating.")

	sig <- syscall.SIGINT
}

func finalizeGateway(fix bool, identity string, distributedCluster *daemon.DistributedCluster) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		fmt.Printf("[ERROR] finalizeGateway: %v\n", err)

		if distributedCluster != nil {
			distributedCluster.HandlePanic(identity, err)
		}
	}

	sig <- syscall.SIGINT
}
