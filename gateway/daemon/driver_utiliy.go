package daemon

import (
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"log"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/opentracing/opentracing-go"
	"github.com/zhangjyr/distributed-notebook/common/consul"
	"github.com/zhangjyr/distributed-notebook/common/tracing"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

//
// This file contains methods that are useful for a driver script for the Cluster Gateway.
//

const (
	ServiceName = "gateway"
)

var (
	globalLogger = config.GetLogger("")
)

type GatewayFinalizer func(fix bool, identity string, distributedCluster *DistributedCluster)

// Create/initialize and return the Tracer and Consul Clients.
func CreateConsulAndTracer(options *domain.ClusterGatewayOptions) (opentracing.Tracer, *consul.Client) {
	var (
		tracer       opentracing.Tracer
		consulClient *consul.Client
		err          error
	)

	if options.JaegerAddr != "" && options.Consuladdr != "" {
		globalLogger.Info("Initializing jaeger agent [service name: %v | host: %v]...", ServiceName, options.JaegerAddr)

		tracer, err = tracing.Init(ServiceName, options.JaegerAddr)
		if err != nil {
			log.Fatalf("Got error while initializing jaeger agent: %v", err)
		}
		globalLogger.Info("Jaeger agent initialized")

		globalLogger.Info("Initializing consul agent [host: %v]...", options.Consuladdr)
		consulClient, err = consul.NewClient(options.Consuladdr)
		if err != nil {
			log.Fatalf("Got error while initializing consul agent: %v", err)
		}
		globalLogger.Info("Consul agent initialized")
	}

	return tracer, consulClient
}

// Build grpc options
func GetGrpcOptions(identity string, tracer opentracing.Tracer, distributedCluster *DistributedCluster) []grpc.ServerOption {
	gOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(
				recovery.WithRecoveryHandler(
					func(p any) (err error) {
						fmt.Printf("gRPC Recovery Handler called with error: %v\n", err)
						debug.PrintStack()
						// Enable the Distributed Cluster to handle panics, which ultimately
						// just involves sending a notification of the panic to the Dashboard.
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

func CreateAndStartClusterGatewayComponents(options *domain.ClusterGatewayOptions, done *sync.WaitGroup, finalize GatewayFinalizer, sig chan os.Signal) (*ClusterGatewayImpl, *DistributedCluster) {
	if done == nil {
		panic("The provided sync.WaitGroup cannot be nil.")
	}

	tracer, consulClient := CreateConsulAndTracer(options)

	// Initialize listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", options.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	globalLogger.Info("Jupyter server listening at %v", lis.Addr())

	// Initialize daemon
	srv := New(&options.ConnectionInfo, &options.ClusterDaemonOptions, func(srv scheduling.ClusterGateway) {
		srv.SetClusterOptions(&options.ClusterSchedulerOptions)
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
	globalLogger.Info("Provisioning server listening at %v", lisHost.Addr())

	// Initialize internel gRPC server
	provisioner := grpc.NewServer(GetGrpcOptions("Provisioner gRPC Server", tracer, distributedCluster)...)
	proto.RegisterClusterGatewayServer(provisioner, srv)

	// Initialize Jupyter gRPC server
	registrar := grpc.NewServer(GetGrpcOptions("Jupyter gRPC Server", tracer, distributedCluster)...)
	proto.RegisterLocalGatewayServer(registrar, srv)

	distributedClusterRpcServer := grpc.NewServer(GetGrpcOptions("Distributed Cluster gRPC Server", tracer, distributedCluster)...)
	proto.RegisterDistributedClusterServer(distributedClusterRpcServer, distributedCluster)

	// Register services in consul
	if consulClient != nil {
		err = consulClient.Register(ServiceName, uuid.New().String(), "", options.Port)
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

	// Start daemon
	go func() {
		defer finalize(true, "Cluster Gateway Daemon", distributedCluster)
		if err := srv.Start(); err != nil {
			log.Fatalf("Error during daemon serving: %v", err)
		}
	}()

	return srv, distributedCluster
}
