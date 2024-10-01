package e2e_testing

import (
	"context"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
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

	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"github.com/zhangjyr/distributed-notebook/gateway/daemon"
	"github.com/zhangjyr/distributed-notebook/testing/fake_kernel"

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
	if errors.Is(err, config.ErrPrintUsage) {
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

func createGrpcGatewayClient() (client proto.LocalGatewayClient, conn *grpc.ClientConn, err error) {
	conn, err = grpc.Dial("127.0.0.1:18080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).To(BeNil())
	Expect(conn).ToNot(BeNil())

	fmt.Printf("Connected to Gateway via gRPC.\n")

	client = proto.NewLocalGatewayClient(conn)
	Expect(client).ToNot(BeNil())
	return
}

var _ = Describe("End to End (E2E) Tests", func() {
	BeforeEach(func() {
		config.LogLevel = logger.LOG_LEVEL_ALL

		os.Setenv("NODE_NAME", types.LocalNode)
	})

	Context("E2E Connectivity", func() {
		It("Will enable a kernel to register", func() {
			var doneGW sync.WaitGroup
			var doneLD1 sync.WaitGroup
			var doneLD2 sync.WaitGroup
			var doneLD3 sync.WaitGroup

			// Create the Cluster Gateway.
			gatewayOptions := getGatewayOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/gateway-daemon.yml")
			clusterGateway, _ := clustergateway.CreateAndStartClusterGatewayComponents(gatewayOptions, &doneGW, finalizeGateway, sig)

			Expect(clusterGateway).ToNot(BeNil())

			// Create the Local Daemon(s).
			localDaemon1Options := getLocalDaemonOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/local-daemon0.yml")
			localDaemon1, _ := local_daemon.CreateAndStartLocalDaemonComponents(localDaemon1Options, &doneLD1, finalizeLocalDaemon, sig)
			Expect(localDaemon1).ToNot(BeNil())

			localDaemon2Options := getLocalDaemonOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/local-daemon1.yml")
			localDaemon2, _ := local_daemon.CreateAndStartLocalDaemonComponents(localDaemon2Options, &doneLD2, finalizeLocalDaemon, sig)
			Expect(localDaemon2).ToNot(BeNil())

			localDaemon3Options := getLocalDaemonOptions("/home/bcarver2/go/pkg/distributed-notebook/yaml/local/local-daemon2.yml")
			localDaemon3, _ := local_daemon.CreateAndStartLocalDaemonComponents(localDaemon3Options, &doneLD3, finalizeLocalDaemon, sig)
			Expect(localDaemon3).ToNot(BeNil())

			time.Sleep(time.Millisecond * 1250)

			Expect(clusterGateway.NumLocalDaemonsConnected()).To(Equal(3))

			fmt.Printf("Creating ClusterGatewayClient now...\n")

			client, conn, err := createGrpcGatewayClient()
			Expect(err).To(BeNil())
			Expect(conn).ToNot(BeNil())
			Expect(client).ToNot(BeNil())

			fmt.Printf("Successfully created new ClusterGatewayClient.\n")

			fmt.Printf("Creating new Kernel now...\n")

			kernelId := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			go func() {
				resp, err := client.StartKernel(context.Background(),
					&proto.KernelSpec{
						Id:              kernelId,
						Session:         kernelId,
						Argv:            []string{"python3.11", "-m", "distributed_notebook.kernel", "-f", "/home/bcarver2/go/pkg/distributed-notebook/testing/e2e/connection_files/fake_kernel1_connection.json", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
						SignatureScheme: "hmac-sha256",
						Key:             "",
						ResourceSpec: &proto.ResourceSpec{
							Cpu:    1,
							Memory: 1,
							Gpu:    1,
						},
					})

				Expect(err).To(BeNil())
				Expect(resp).ToNot(BeNil())
				fmt.Printf("Response: %v\n", resp)
			}()

			var registerWG sync.WaitGroup
			registerWG.Add(3)

			for i := 0; i < 3; i++ {
				var (
					wg     sync.WaitGroup
					kernel *fake_kernel.FakeKernel
				)

				replicaId := i + 1

				wg.Add(1)
				go func() {
					fmt.Printf("Creating replica #%d now...\n", replicaId)
					kernel = fake_kernel.NewFakeKernel(replicaId, kernelId, "149a41b5-0df54cf013c3035a3084a319", 4000+(1000*(replicaId-1)), 28080+(10000*(replicaId-1)))
					kernel.Start()
					wg.Done()
				}()

				go func() {
					wg.Wait() // Wait until the kernel is started before registering.
					err = kernel.RegisterWithLocalDaemon()

					if err != nil {
						fmt.Printf(utils.RedStyle.Render("[ERROR] Failed to register with local daemon: %v\n"), err)
					}

					Expect(err).To(BeNil())
					registerWG.Done()
				}()
			}

			fmt.Printf("Waiting for registration to complete.\n")

			registerWG.Wait()

			fmt.Printf("Registration complete.\n")

			time.Sleep(time.Millisecond * 1250)

			Expect(clusterGateway.NumKernels()).To(Equal(1))

			// doneGW.Wait()
			// doneLD1.Wait()
			// doneLD2.Wait()
			// doneLD3.Wait()

			// closeConnections1()
			// closeConnections2()
			// closeConnections3()
			// conn.Close()
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
