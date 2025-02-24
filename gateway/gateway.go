package gateway

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/scusemua/distributed-notebook/common/consul"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/tracing"
	"github.com/scusemua/distributed-notebook/common/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"

	"github.com/scusemua/distributed-notebook/gateway/daemon"
	"github.com/scusemua/distributed-notebook/gateway/domain"
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

type Finalizer func(fix bool, identity string, distributedCluster *DistributedCluster)

// CreateConsulAndTracer creates/initializes and return the Tracer and Consul Clients.
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
func GetGrpcOptions(identity string, tracer opentracing.Tracer, distributedCluster *DistributedCluster) []grpc.ServerOption {
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

func createAndStartClusterGatewayComponents(options *domain.ClusterGatewayOptions, done *sync.WaitGroup, finalize Finalizer, sig chan os.Signal) (*ClusterGatewayImpl, *DistributedCluster) {
	if done == nil {
		panic("The provided sync.primarSemaphore cannot be nil.")
	}

	tracer, consulClient := CreateConsulAndTracer(options)

	// Initialize listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", options.JupyterGrpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	globalLogger.Info("Jupyter server listening at %v", lis.Addr())

	options.ClusterDaemonOptions.ValidateClusterDaemonOptions()
	options.SchedulerOptions.ValidateClusterSchedulerOptions()

	globalLogger.Debug("Cluster Gateway SchedulerOptions:\n%s", options.PrettyString(2))

	// Initialize daemon
	srv := New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
		globalLogger.Info("Initializing Cluster Gateway with options: %s", options.PrettyString(2))

		srv.SetClusterOptions(&options.SchedulerOptions)
		srv.SetDistributedClientProvider(&client.DistributedKernelClientProvider{})
	})

	distributedCluster := NewDistributedCluster(srv)

	distributedClusterServiceListener, err := distributedCluster.Listen("tcp", fmt.Sprintf(":%d", options.DistributedClusterServicePort))
	if err != nil {
		log.Fatalf("Failed to listen with Distributed Cluster Service server: %v", err)
	}
	globalLogger.Info("Distributed Cluster Service gRPC server listening at %v", distributedClusterServiceListener.Addr())

	// Listen on provisioner port
	lisHost, err := srv.Listen("tcp", fmt.Sprintf(":%d", options.ProvisionerPort))
	if err != nil {
		log.Fatalf("Failed to listen on provisioner port: %v", err)
	}
	globalLogger.Info("Provisioning server listening at %v", lisHost.Addr)

	// Initialize internal gRPC server
	provisioner := grpc.NewServer(GetGrpcOptions("Provisioner gRPC Server", tracer, distributedCluster)...)
	proto.RegisterClusterGatewayServer(provisioner, srv)

	// Initialize Jupyter gRPC server
	registrar := grpc.NewServer(GetGrpcOptions("Jupyter gRPC Server", tracer, distributedCluster)...)
	proto.RegisterLocalGatewayServer(registrar, srv)

	distributedClusterRpcServer := grpc.NewServer(GetGrpcOptions("Distributed Cluster gRPC Server", tracer, distributedCluster)...)
	proto.RegisterDistributedClusterServer(distributedClusterRpcServer, distributedCluster)

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
		provisioner.Stop()
		distributedClusterRpcServer.Stop()
		_ = distributedCluster.Close()
		_ = srv.Close()
		_ = lisHost.Close()
		_ = lis.Close()
		_ = distributedClusterServiceListener.Close()

		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true, "gRPC Server", distributedCluster)
		if serveErr := registrar.Serve(lis); serveErr != nil {

			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			log.Fatalf("Error on serving jupyter connections: %v", serveErr)
		}
	}()

	// Start provisioning server
	go func() {
		defer finalize(true, "Provisioner Server", distributedCluster)
		if serveErr := provisioner.Serve(lisHost); serveErr != nil {
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
		defer finalize(true, "Distributed Cluster Server", distributedCluster)
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
		defer finalize(true, "Cluster Gateway Daemon", distributedCluster)
		if serveErr := srv.Start(); serveErr != nil {
			globalLogger.Error(utils.RedStyle.Render("Error during daemon serving: %v"), serveErr)

			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			panic(serveErr)
		}
	}()

	return srv, distributedCluster
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

func main() {
	defer finalize(false, "Main thread", nil)

	var done sync.WaitGroup

	// Ensure that the options/configuration is valid.
	ValidateOptions()

	logger.Info("Starting Cluster Gateway with the following options:\n%s\n", options.String())

	if options.PrettyPrintOptions {
		logger.Info("Cluster Gateway ClusterGatewayOptions pretty-printed:\n%s\n", options.PrettyString(2))
	}

	if options.ClusterDaemonOptions.CommonOptions.DebugMode {
		go createAndStartDebugHttpServer()
	}

	daemon.CreateAndStartClusterGatewayComponents(&options, &done, finalize, sig)

	done.Wait()
}

func finalize(fix bool, identity string, distributedCluster *daemon.DistributedCluster) {
	if !fix {
		return
	}

	log.Printf("[WARNING] Finalize called with fix=%v and identity=\"%s\"\n", fix, identity)

	if err := recover(); err != nil {
		logger.Error("Called recover() and retrieved the following error: %v", err)

		if distributedCluster != nil {
			distributedCluster.HandlePanic(identity, err)
		}
	}

	logger.Error("Stack trace of CURRENT goroutine:")
	debug.PrintStack()

	logger.Error("Stack traces of ALL active goroutines:")
	err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	if err != nil {
		logger.Error("Failed to output call stacks of all active goroutines: %v", err)
	}

	sig <- syscall.SIGINT
}
