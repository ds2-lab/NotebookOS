package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/zhangjyr/distributed-notebook/common/consul"
	"github.com/zhangjyr/distributed-notebook/common/core"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/tracing"
	"github.com/zhangjyr/distributed-notebook/gateway/daemon"
)

const (
	ServiceName = "gateway"
)

var (
	options Options = Options{}
	logger          = config.GetLogger("")
	sig             = make(chan os.Signal, 1)
)

type Options struct {
	config.LoggerOptions
	types.ConnectionInfo
	core.CoreOptions
	daemon.ClusterDaemonOptions

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
	// DriverGRPCPort  int    `name:"driver-grpc-port" usage:"Port for the gRPC service that the workload driver connects to"`
}

func init() {
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.Port = 8080
	options.ProvisionerPort = 8081
	options.ConnectionInfo.Transport = "tcp"
}

func main() {
	defer finalize(false, "Main thread", nil)

	var done sync.WaitGroup

	flags, err := config.ValidateOptions(&options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}

	logger.Info("Started gateway with options: %v", options)

	var tracer opentracing.Tracer
	var consulClient *consul.Client
	if options.JaegerAddr != "" && options.Consuladdr != "" {
		logger.Info("Initializing jaeger agent [service name: %v | host: %v]...", ServiceName, options.JaegerAddr)

		tracer, err = tracing.Init(ServiceName, options.JaegerAddr)
		if err != nil {
			log.Fatalf("Got error while initializing jaeger agent: %v", err)
		}
		logger.Info("Jaeger agent initialized")

		logger.Info("Initializing consul agent [host: %v]...", options.Consuladdr)
		consulClient, err = consul.NewClient(options.Consuladdr)
		if err != nil {
			log.Fatalf("Got error while initializing consul agent: %v", err)
		}
		logger.Info("Consul agent initialized")
	}

	// Initialize listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", options.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	logger.Info("Jupyter server listening at %v", lis.Addr())

	// Initialize workload driver listener
	// proxy := gateway.NewWebSocketProxyServer(fmt.Sprintf(":%d", options.DriverGRPCPort))
	// lisDriver, err := proxy.Listen()
	// if err != nil {
	// 	log.Fatalf("Failed to listen with websocket proxy server: %v", err)
	// }
	// logger.Info("Workload Driver gRPC server listening at %v", lisDriver.Addr())

	// Initialize daemon
	srv := daemon.New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv *daemon.GatewayDaemon) {
		srv.ClusterOptions = options.CoreOptions
	})

	distributedCluster := daemon.NewDistributedCluster(srv, &options.ClusterDaemonOptions)

	// Build grpc options
	getGrpcOptions := func(identity string) []grpc.ServerOption {
		gOpts := []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(
				recovery.UnaryServerInterceptor(
					recovery.WithRecoveryHandler(
						func(p any) (err error) {
							if distributedCluster != nil {
								distributedCluster.HandlePanic(identity, err)
							}

							return err
						}),
				),
			),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Timeout: 120 * time.Second,
			}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				PermitWithoutStream: true,
			}),
		}

		if tracer != nil {
			gOpts = append(gOpts, grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)))
		}

		return gOpts
	}

	distributedClusterServiceListener, err := distributedCluster.Listen("tcp", fmt.Sprintf(":%d", options.DistributedClusterServicePort))
	if err != nil {
		log.Fatalf("Failed to listen with Distributed Cluster Service server: %v", err)
	}
	logger.Info("Distributed Cluster Service gRPC server listening at %v", distributedClusterServiceListener.Addr())

	// Listen on provisioner port
	lisHost, err := srv.Listen("tcp", fmt.Sprintf(":%d", options.ProvisionerPort))
	if err != nil {
		log.Fatalf("Failed to listen on provisioner port: %v", err)
	}
	logger.Info("Provisioning server listening at %v", lisHost.Addr())

	// Initialize internel gRPC server
	provisioner := grpc.NewServer(getGrpcOptions("Provisioner gRPC Server")...)
	gateway.RegisterClusterGatewayServer(provisioner, srv)

	// Initialize Jupyter gRPC server
	registrar := grpc.NewServer(getGrpcOptions("Jupyter gRPC Server")...)
	gateway.RegisterLocalGatewayServer(registrar, srv)

	distributedClusterRpcServer := grpc.NewServer(getGrpcOptions("Distributed Cluster gRPC Server")...)
	gateway.RegisterDistributedClusterServer(distributedClusterRpcServer, distributedCluster)

	// Register services in consul
	if consulClient != nil {
		err = consulClient.Register(ServiceName, uuid.New().String(), "", options.Port)
		if err != nil {
			log.Fatalf("Failed to register in consul: %v", err)
		}
		logger.Info("Successfully registered in consul")
	}

	// Start detecting stop signals
	done.Add(1)
	go func() {
		<-sig
		logger.Info("Shutting down...")
		registrar.Stop()
		provisioner.Stop()
		distributedClusterRpcServer.Stop()
		distributedCluster.Close()
		srv.Close()
		lisHost.Close()
		lis.Close()
		distributedClusterServiceListener.Close()

		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true, "gRPC Server", distributedCluster)
		if err := registrar.Serve(lis); err != nil {
			log.Fatalf("Error on serving jupyter connections: %v", err)
		}
	}()

	// Start provisioning server
	go func() {
		defer finalize(true, "Provisioner Server", distributedCluster)
		if err := provisioner.Serve(lisHost); err != nil {
			log.Fatalf("Error on serving host scheduler connections: %v", err)
		}
	}()

	// Start distributed cluster gRPC server.
	go func() {
		defer finalize(true, "Distributed Cluster Server", distributedCluster)
		if err := distributedClusterRpcServer.Serve(distributedClusterServiceListener); err != nil {
			log.Fatalf("Error on serving distributed cluster connections: %v", err)
		}
	}()

	// Start workload driver gRPC server
	// go func() {
	// 	defer finalize(true, "Workload Driver gRPC Server", srv)
	// 	if err := provisioner.Serve(lisDriver); err != nil {
	// 		log.Fatalf("Error on serving workload driver connections: %v", err)
	// 	}
	// }()

	// Start daemon
	go func() {
		defer finalize(true, "Cluster Gateway Daemon", distributedCluster)
		if err := srv.Start(); err != nil {
			log.Fatalf("Error during daemon serving: %v", err)
		}
	}()

	done.Wait()
}

func finalize(fix bool, identity string, distributedCluster *daemon.DistributedCluster) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		logger.Error("%v", err)

		if distributedCluster != nil {
			distributedCluster.HandlePanic(identity, err)
		}
	}

	sig <- syscall.SIGINT
}
