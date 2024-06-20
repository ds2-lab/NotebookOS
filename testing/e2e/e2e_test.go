package e2e_testing

import (
	"context"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
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

func createGrpcGatewayClient() (client gateway.LocalGatewayClient, conn *grpc.ClientConn, err error) {
	conn, err = grpc.Dial("127.0.0.1:18080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).To(BeNil())
	Expect(conn).ToNot(BeNil())

	fmt.Printf("Connected to Gateway via gRPC.\n")

	client = gateway.NewLocalGatewayClient(conn)
	Expect(client).ToNot(BeNil())
	return
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
			clusterGateway, _ := clustergateway.CreateAndStartClusterGatewayComponents(gatewayOptions, &done, finalizeGateway, sig)

			Expect(clusterGateway).ToNot(BeNil())

			// Create the Local Daemon(s).
			localDaemon1Options := getLocalDaemonOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/local-daemon0.yml")
			localDaemon1, closeConnections := local_daemon.CreateAndStartLocalDaemonComponents(localDaemon1Options, &done, finalizeLocalDaemon, sig)

			Expect(localDaemon1).ToNot(BeNil())

			fmt.Printf("Creating ClusterGatewayClient now...\n")

			time.Sleep(time.Millisecond * 500)

			client, conn, err := createGrpcGatewayClient()
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			Expect(client).ToNot(BeNil())

			fmt.Printf("Successfully created new ClusterGatewayClient.\n")

			fmt.Printf("Creating new Kernel now...\n")

			kernelId := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			resp, err := client.StartKernel(context.Background(),
				&gateway.KernelSpec{
					Id:              kernelId,
					Session:         kernelId,
					Argv:            make([]string, 0),
					SignatureScheme: "hmac-sha256",
					Key:             "",
					ResourceSpec: &gateway.ResourceSpec{
						Cpu:    1,
						Memory: 1,
						Gpu:    1,
					},
				})

			Expect(err).To(BeNil())
			Expect(resp).ToNot(BeNil())

			fmt.Printf("Response: %v\n", resp)

			// done.Wait()

			closeConnections()
			conn.Close()
		})
	})
})

func finalizeLocalDaemon(fix bool) {
	log.Printf("finalizeLocalDaemon(%v) called.\n", fix)
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
	log.Printf("finalizeLocalDaemon(%v, %s, %v) called.\n", fix, identity, distributedCluster)

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
