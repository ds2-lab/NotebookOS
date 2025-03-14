package daemon

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/utils"
	"log"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/scusemua/distributed-notebook/common/consul"
	"github.com/scusemua/distributed-notebook/common/tracing"
	"github.com/scusemua/distributed-notebook/gateway/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

//
// This file contains methods that are useful for a driver script for the internalCluster Gateway.
//

const (
	ServiceName = "gateway"
)

var (
	globalLogger = config.GetLogger("")
)

type GatewayFinalizer func(fix bool, identity string, distributedCluster *DistributedCluster)

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

func CreateAndStartClusterGatewayComponents(options *domain.ClusterGatewayOptions, done *sync.WaitGroup, finalize GatewayFinalizer, sig chan os.Signal) (*ClusterGatewayImpl, *DistributedCluster) {
	if done == nil {
		panic("The provided sync.primarSemaphore cannot be nil.")
	}

	tracer, consulClient := CreateConsulAndTracer(options)

	// Initialize listener
	lisJupyterGrpc, err := net.Listen("tcp", fmt.Sprintf(":%d", options.JupyterGrpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	globalLogger.Info("Jupyter server listening at %v", lisJupyterGrpc.Addr())

	options.ClusterDaemonOptions.ValidateClusterDaemonOptions()
	options.SchedulerOptions.ValidateClusterSchedulerOptions()

	globalLogger.Debug("cluster Gateway SchedulerOptions:\n%s", options.PrettyString(2))

	// Initialize daemon
	srv := New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv ClusterGateway) {
		globalLogger.Info("Initializing cluster Gateway with options: %s", options.PrettyString(2))

		srv.SetClusterOptions(&options.SchedulerOptions)
		srv.SetDistributedClientProvider(&client.DistributedKernelClientProvider{})
	})

	distributedCluster := NewDistributedCluster(srv)

	distributedClusterServiceListener, err := distributedCluster.Listen("tcp", fmt.Sprintf(":%d", options.DistributedClusterServicePort))
	if err != nil {
		log.Fatalf("Failed to listen with Distributed cluster Service server: %v", err)
	}
	globalLogger.Info("Distributed cluster Service gRPC server listening at %v", distributedClusterServiceListener.Addr())

	// Listen on ClusterGateway Provisioner port
	lisGatewayProvisioner, err := srv.Listen("tcp", fmt.Sprintf(":%d", options.ProvisionerPort))
	if err != nil {
		log.Fatalf("Failed to listen on clusterGatewayProvisioner port: %v", err)
	}
	globalLogger.Info("Provisioning server listening at %v", lisGatewayProvisioner.Addr)

	// Initialize internal gRPC server
	clusterGatewayProvisioner := grpc.NewServer(GetGrpcOptions("Provisioner gRPC Server", tracer, distributedCluster)...)
	proto.RegisterClusterGatewayServer(clusterGatewayProvisioner, srv)

	// Initialize Jupyter gRPC server
	registrar := grpc.NewServer(GetGrpcOptions("Jupyter gRPC Server", tracer, distributedCluster)...)
	proto.RegisterLocalGatewayServer(registrar, srv)

	distributedClusterRpcServer := grpc.NewServer(GetGrpcOptions("Distributed cluster gRPC Server", tracer, distributedCluster)...)
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
		clusterGatewayProvisioner.Stop()
		distributedClusterRpcServer.Stop()
		_ = distributedCluster.Close()
		_ = srv.Close()
		_ = lisGatewayProvisioner.Close()
		_ = lisJupyterGrpc.Close()
		_ = distributedClusterServiceListener.Close()

		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true, "gRPC Server", distributedCluster)
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
		defer finalize(true, "Provisioner Server", distributedCluster)
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
		defer finalize(true, "Distributed cluster Server", distributedCluster)
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
		defer finalize(true, "cluster Gateway Daemon", distributedCluster)
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
