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

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
}

func init() {
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.Port = 8080
	options.ProvisionerPort = 8081
	options.ConnectionInfo.Transport = "tcp"
}

func main() {
	defer finalize(false)

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

	// Build grpc options
	gOpts := []grpc.ServerOption{
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
	// if tlsopt := tls.GetServerOpt(); tlsopt != nil {
	// 	opts = append(opts, tlsopt)
	// }

	// Initialize grpc server
	registrar := grpc.NewServer(gOpts...)
	srv := daemon.New(&options.ConnectionInfo, func(srv *daemon.GatewayDaemon) {
		srv.ClusterOptions = options.CoreOptions
	})
	gateway.RegisterLocalGatewayServer(registrar, srv)
	gateway.RegisterClusterGatewayServer(registrar, srv)
	logger.Info("Server listening at %v", lis.Addr())

	// Listen on provisioner port
	provisioner, err := srv.Listen("tcp", fmt.Sprintf(":%d", options.ProvisionerPort))
	if err != nil {
		log.Fatalf("Failed to listen on provisioner port: %v", err)
	}

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
		srv.Close()
		provisioner.Close()
		lis.Close()

		// logger.Info("listern closed...")
		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true)
		if err := registrar.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Start provisioning server
	go func() {
		defer finalize(true)
		if err := registrar.Serve(provisioner); err != nil {
			log.Fatalf("Failed to serve provisioner: %v", err)
		}
	}()

	// Start daemon
	go func() {
		defer finalize(true)
		if err := srv.Start(); err != nil {
			log.Fatalf("Error during daemon serving: %v", err)
		}
	}()

	done.Wait()
}

func finalize(fix bool) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		logger.Error("%v", err)
	}

	sig <- syscall.SIGINT
}
