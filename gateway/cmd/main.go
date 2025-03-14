package main

import (
	"encoding/gob"
	"fmt"
	"github.com/charmbracelet/lipgloss"
	dockerClient "github.com/docker/docker/client"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/muesli/termenv"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/cluster"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	daemon "github.com/scusemua/distributed-notebook/gateway/internal"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel"
	gatewayMetrics "github.com/scusemua/distributed-notebook/gateway/internal/metrics"
	"github.com/scusemua/distributed-notebook/gateway/internal/notifier"
	"github.com/scusemua/distributed-notebook/gateway/internal/routing"
	"github.com/scusemua/distributed-notebook/gateway/internal/rpc"
	"google.golang.org/grpc/keepalive"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/scusemua/distributed-notebook/common/consul"
	"github.com/scusemua/distributed-notebook/common/tracing"
	"google.golang.org/grpc"
)

const (
	ServiceName = "gateway"
)

var (
	options      = domain.ClusterGatewayOptions{}
	globalLogger = config.GetLogger("")
	sig          = make(chan os.Signal, 1)
)

func init() {
	lipgloss.SetColorProfile(termenv.ANSI256)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.JupyterGrpcPort = 8080
	options.ProvisionerPort = 8081
	options.ConnectionInfo.Transport = "tcp"

	gob.Register(metrics.ClusterStatistics{})
	gob.Register(metrics.ClusterEvent{})
	gob.Register(map[string]interface{}{})
	gob.Register(time.Duration(0))
	gob.Register(time.Time{})
}

type PanicHandler interface {
	HandlePanic(identity string, fatalErr interface{})
}

// Create and run the debug HTTP server.
// We don't have any meaningful endpoints that we add directly.
// But we include the following import statement at the top of this file:
//
//	_ "net/http/pprof"
//
// This adds several key debug endpoints.
//
// Important: this should be called from its own goroutine.
func createAndStartDebugHttpServer() {
	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	log.Printf("Received HTTP debug connection to '/'")
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
	// })

	// http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
	// 	log.Printf("Received HTTP debug connection to '/test'")
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
	// })

	var address = fmt.Sprintf(":%d", options.DebugPort)
	log.Printf("Serving debug HTTP server: %s\n", address)

	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// ValidateOptions ensures that the options/configuration is valid.
func ValidateOptions() {
	flags, err := config.ValidateOptions(&options)
	if errors.Is(err, config.ErrPrintUsage) {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
}

func CreateConsulAndTracer(options *domain.ClusterGatewayOptions) (opentracing.Tracer, *consul.Client) {
	var (
		tracer       opentracing.Tracer
		consulClient *consul.Client
		err          error
	)

	if options.JaegerAddr != "" && options.ConsulAddr != "" {
		globalLogger.Info("Initializing jaeger agent [service name: %v | host: %v]...", ServiceName, options.JaegerAddr)

		tracer, err = tracing.Init(ServiceName, options.JaegerAddr)
		if err != nil {
			log.Fatalf("Got error while initializing jaeger agent: %v", err)
		}
		globalLogger.Info("Jaeger agent initialized")

		globalLogger.Info("Initializing consul agent [host: %v]...", options.ConsulAddr)
		consulClient, err = consul.NewClient(options.ConsulAddr)
		if err != nil {
			log.Fatalf("Got error while initializing consul agent: %v", err)
		}
		globalLogger.Info("Consul agent initialized")
	}

	return tracer, consulClient
}

// GetGrpcOptions builds gRPC options for use by the internalCluster Gateway.
func GetGrpcOptions(identity string, tracer opentracing.Tracer, distributedCluster PanicHandler) []grpc.ServerOption {
	gOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(
				recovery.WithRecoveryHandler(
					func(p any) (err error) {
						fmt.Printf("gRPC Recovery Handler called with error: %v\n", err)
						debug.PrintStack()
						// Enable the Distributed internalCluster to handle panics, which ultimately
						// just involves sending a notification of the panic to the Dashboard.
						if distributedCluster != nil {
							distributedCluster.HandlePanic(identity, err)
						}

						return err
					}),
			),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout:           120 * time.Second,
			MaxConnectionAge:  time.Duration(1<<63 - 1),
			MaxConnectionIdle: time.Duration(1<<63 - 1),
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
			MinTime:             time.Minute * 2,
		}),
	}

	if tracer != nil {
		gOpts = append(gOpts, grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)))
	}

	return gOpts
}

func main() {
	defer finalize(false, "Main thread", nil)

	var done sync.WaitGroup

	// Ensure that the options/configuration is valid.
	ValidateOptions()

	options.ClusterDaemonOptions.ValidateClusterDaemonOptions()
	options.SchedulerOptions.ValidateClusterSchedulerOptions()

	if options.PrettyPrintOptions {
		globalLogger.Info("Starting the Global Scheduler with the following options:\n%s\n",
			options.PrettyString(2))
	} else {
		globalLogger.Info("Starting the Global Scheduler.")
	}

	if options.ClusterDaemonOptions.CommonOptions.DebugMode {
		go createAndStartDebugHttpServer()
	}

	tracer, consulClient := CreateConsulAndTracer(&options)

	// Initialize listener
	lisJupyterGrpc, err := net.Listen("tcp", fmt.Sprintf(":%d", options.JupyterGrpcPort))

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	globalLogger.Info("Jupyter server listening at %v", lisJupyterGrpc.Addr())

	clusterGatewayId := uuid.NewString()

	dashboardNotifier := notifier.NewDashboardNotifier(nil)

	metricsProvider := gatewayMetrics.NewManagerBuilder().
		SetID(clusterGatewayId).
		SetPrometheusPort(options.PrometheusPort).
		Build()

	forwarder := routing.NewForwarder(&options.ConnectionInfo, dashboardNotifier, metricsProvider, &options)

	kernelManager, err := kernel.NewManagerBuilder().
		SetID(clusterGatewayId).
		SetCluster(nil).
		SetSchedulingPolicy(nil).
		SetMetricsProvider(metricsProvider).
		SetNotifier(dashboardNotifier).
		SetOptions(&options).
		SetRequestLog(forwarder.RequestLog).
		SetResponseForwarder(forwarder).
		Build()

	if err != nil {
		panic(err)
	}

	forwarder.RegisterKernelForwarder(kernelManager)
	metricsProvider.SetNumActiveKernelProvider(kernelManager)

	globalScheduler := daemon.NewGatewayDaemonBuilder(&options).
		WithId(clusterGatewayId).
		WithNotifier(dashboardNotifier).
		WithForwarder(forwarder).
		WithCluster(nil).
		WithKernelManager(kernelManager).
		WithMetricsManager(metricsProvider).
		WithConnectionOptions(&options.ConnectionInfo).
		WithDistributedClientProvider(&client.DistributedKernelClientProvider{}).
		Build()

	kernelManager.SetNetworkProvider(globalScheduler)

	gatewayGrpcServer := rpc.NewClusterGatewayServer(clusterGatewayId, globalScheduler, dashboardNotifier)
	distributedClusterGrpcServer := rpc.NewDistributedGateway(globalScheduler, dashboardNotifier)

	distributedClusterServiceListener, err := distributedClusterGrpcServer.Listen(
		"tcp", fmt.Sprintf(":%d", options.DistributedClusterServicePort))
	if err != nil {
		log.Fatalf("Failed to listen with Distributed cluster Service server: %v", err)
	}

	globalLogger.Info("DistributedGateway gRPC Server Listening @ %v", distributedClusterServiceListener.Addr())

	// Listen on ClusterGateway Provisioner port
	lisGatewayProvisioner, err := gatewayGrpcServer.Listen("tcp", fmt.Sprintf(":%d", options.ProvisionerPort))
	if err != nil {
		log.Fatalf("Failed to listen on clusterGatewayProvisioner port: %v", err)
	}
	globalLogger.Info("Provisioning server listening at %v", lisGatewayProvisioner.Addr)

	// Initialize internal gRPC server
	clusterGatewayProvisioner := grpc.NewServer(GetGrpcOptions("Provisioner gRPC Server", tracer, distributedClusterGrpcServer)...)
	proto.RegisterClusterGatewayServer(clusterGatewayProvisioner, gatewayGrpcServer)

	// Initialize Jupyter gRPC server
	registrar := grpc.NewServer(GetGrpcOptions("Jupyter gRPC Server", tracer, distributedClusterGrpcServer)...)
	proto.RegisterLocalGatewayServer(registrar, gatewayGrpcServer)

	distributedClusterRpcServer := grpc.NewServer(GetGrpcOptions("Distributed cluster gRPC Server", tracer, distributedClusterGrpcServer)...)
	proto.RegisterDistributedClusterServer(distributedClusterRpcServer, distributedClusterGrpcServer)

	// Register services in consul
	if consulClient != nil {
		err = consulClient.Register(ServiceName, uuid.New().String(), "", options.JupyterGrpcPort)
		if err != nil {
			log.Fatalf("Failed to register in consul: %v", err)
		}
		globalLogger.Info("Successfully registered in consul")
	}

	// Start detecting stop signals
	done.Add(1)
	go func() {
		<-sig
		globalLogger.Info("Shutting down...")
		registrar.Stop()
		clusterGatewayProvisioner.Stop()
		distributedClusterRpcServer.Stop()
		_ = distributedClusterGrpcServer.Close()
		_ = gatewayGrpcServer.Close()
		_ = lisGatewayProvisioner.Close()
		_ = lisJupyterGrpc.Close()
		_ = distributedClusterServiceListener.Close()

		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true, "gRPC Server", distributedClusterGrpcServer)
		if serveErr := registrar.Serve(lisJupyterGrpc); serveErr != nil {

			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			log.Fatalf("Error on serving jupyter connections: %v", serveErr)
		}
	}()

	// Start provisioning server
	go func() {
		defer finalize(true, "Provisioner Server", distributedClusterGrpcServer)
		if serveErr := clusterGatewayProvisioner.Serve(lisGatewayProvisioner); serveErr != nil {
			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				globalLogger.Warn(
					utils.LightOrangeStyle.Render("Error on serving host scheduler connections: %v"), serveErr)
				return
			}

			globalLogger.Error(utils.RedStyle.Render("Error on serving host scheduler connections: %v"), serveErr)
			panic(err)
		}
	}()

	// Start distributed cluster gRPC server.
	go func() {
		defer finalize(true, "Distributed cluster Server", distributedClusterGrpcServer)
		if serveErr := distributedClusterRpcServer.Serve(distributedClusterServiceListener); err != nil {
			globalLogger.Error(utils.RedStyle.Render("Error on serving distributed cluster connections: %v"), serveErr)

			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			panic(serveErr)
		}
	}()

	// Start daemon
	go func() {
		defer finalize(true, "cluster Gateway Daemon", distributedClusterGrpcServer)
		if serveErr := gatewayGrpcServer.Start(); serveErr != nil {
			globalLogger.Error(utils.RedStyle.Render("Error during daemon serving: %v"), serveErr)

			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			panic(serveErr)
		}
	}()

	done.Wait()
}

func initCluster(clusterDaemonOptions *domain.ClusterDaemonOptions, metricsProvider scheduling.MetricsProvider) scheduling.Cluster {
	clusterProvider := func() scheduling.Cluster {
		if clusterGateway == nil {
			return nil
		}

		return clusterGateway.cluster
	}

	schedulingPolicy, policyError := scheduler.GetSchedulingPolicy(&clusterDaemonOptions.SchedulerOptions, clusterProvider)
	if policyError != nil {
		panic(policyError)
	}

	// Note: we don't construct the scheduling.cluster struct within the switch statement below.
	// We construct the scheduling.cluster struct immediately following the switch statement.
	var (
		clusterPlacer  scheduling.Placer
		clusterType    cluster.Type
		deploymentMode types.DeploymentMode
		err            error
	)
	switch clusterDaemonOptions.DeploymentMode {
	case "":
		{
			globalLogger.Info("No 'deployment_mode' specified. Running in default mode: LOCAL mode.")
			panic("Not supported")
		}
	case "local":
		{
			globalLogger.Info("Running in LOCAL mode.")
			deploymentMode = types.LocalMode
			panic("Not supported")
		}
	case "docker":
		{
			globalLogger.Error("\"docker\" mode is no longer a valid deployment mode")
			globalLogger.Error("The supported deployment modes are: ")
			globalLogger.Error("- \"docker-swarm\"")
			globalLogger.Error("- \"docker-compose\"")
			globalLogger.Error("- \"kubernetes\"")
			globalLogger.Error("- \"local\"")
			os.Exit(1)
		}
	case "docker-compose":
		{
			globalLogger.Info("Running in DOCKER COMPOSE mode.")
			deploymentMode = types.DockerComposeMode

			apiClient, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv)
			if err != nil {
				panic(err)
			}

			clusterGateway.dockerApiClient = apiClient

			dockerEventHandler := NewDockerEventHandler()
			clusterGateway.containerEventHandler = dockerEventHandler

			clusterType = cluster.DockerCompose
			break
		}
	case "docker-swarm":
		{
			globalLogger.Info("Running in DOCKER SWARM mode.")
			deploymentMode = types.DockerSwarmMode

			apiClient, err := dockerClient.NewClientWithOpts(dockerClient.FromEnv)
			if err != nil {
				panic(err)
			}

			clusterGateway.dockerApiClient = apiClient

			clusterType = cluster.DockerSwarm

			break
		}
	case "kubernetes":
		{
			globalLogger.Info("Running in KUBERNETES mode.")
			deploymentMode = types.KubernetesMode

			clusterGateway.kubeClient = NewKubeClient(clusterGateway, clusterDaemonOptions)
			clusterGateway.containerEventHandler = clusterGateway.kubeClient

			clusterType = cluster.Kubernetes

			break
		}
	default:
		{
			globalLogger.Error("Unknown/unsupported deployment mode: \"%s\"", clusterDaemonOptions.DeploymentMode)
			globalLogger.Error("The supported deployment modes are: ")
			globalLogger.Error("- \"kubernetes\"")
			globalLogger.Error("- \"docker-swarm\"")
			globalLogger.Error("- \"docker-compose\"")
			globalLogger.Error("- \"local\"")
			os.Exit(1)
		}
	}

	clusterPlacer, err = schedulingPolicy.GetNewPlacer(metricsProvider)
	if err != nil {
		globalLogger.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}

	// This is where we actually construct the scheduling.cluster struct.
	distributedNotebookCluster, err := cluster.NewBuilder(clusterType).
		WithKubeClient(clusterGateway.kubeClient).
		WithHostSpec(clusterGateway.hostSpec).
		WithPlacer(clusterPlacer).
		WithSchedulingPolicy(schedulingPolicy).
		WithHostMapper(clusterGateway).
		WithKernelProvider(clusterGateway).
		WithClusterMetricsProvider(metricsProvider).
		WithNotificationBroker(clusterGateway).
		WithStatisticsUpdateProvider(metricsProvider.UpdateClusterStatistics).
		WithOptions(&clusterSchedulerOptions).
		BuildCluster()

	if err != nil {
		panic(err)
	}

	return distributedNotebookCluster
}

func finalize(fix bool, identity string, handler PanicHandler) {
	if !fix {
		return
	}

	log.Printf("[WARNING] Finalize called with fix=%v and identity=\"%s\"\n", fix, identity)

	if err := recover(); err != nil {
		globalLogger.Error("Called recover() and retrieved the following error: %v", err)

		if handler != nil {
			handler.HandlePanic(identity, err)
		}
	}

	globalLogger.Error("Stack trace of CURRENT goroutine:")
	debug.PrintStack()

	globalLogger.Error("Stack traces of ALL active goroutines:")
	err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	if err != nil {
		globalLogger.Error("Failed to output call stacks of all active goroutines: %v", err)
	}

	sig <- syscall.SIGINT
}
