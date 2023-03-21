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
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/tracing"
	"github.com/zhangjyr/distributed-notebook/scheduler/daemon"
)

const (
	ServiceName = "scheduler"
)

var (
	options           Options = Options{}
	logger                    = config.GetLogger("")
	sig                       = make(chan os.Signal, 1)
	connectionTimeout         = time.Second
)

type Options struct {
	config.LoggerOptions
	types.ConnectionInfo
	daemon.SchedulerDaemonOptions

	Port            int    `name:"port" usage:"Port the gRPC service listen on."`
	ProvisionerAddr string `name:"provisioner" description:"Provisioner address."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	Consuladdr      string `name:"consul" description:"Consul agent address."`
}

func init() {
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.Port = 8080
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

	logger.Info("Started scheduler with options: %v", options)

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
	srv := grpc.NewServer(gOpts...)
	scheduler := daemon.New(&options.ConnectionInfo, func(scheduler *daemon.SchedulerDaemon) {
		scheduler.Options = options.SchedulerDaemonOptions
	})
	gateway.RegisterLocalGatewayServer(srv, scheduler)

	// Initialize listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", options.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer lis.Close()
	logger.Info("Scheduler listening at %v", lis.Addr())

	// Initialize connection to the provisioner
	provConn, err := net.DialTimeout("tcp", options.ProvisionerAddr, connectionTimeout)
	if err != nil {
		lis.Close()
		log.Fatalf("Failed to connect to provisioner: %v", err)
	}
	defer provConn.Close()

	// Initialize provisioner and wait for ready
	provisioner, err := daemon.NewProvisioner(provConn)
	if err != nil {
		log.Fatalf("Failed to initialize the provisioner: %v", err)
	}
	// Wait for reverse connection
	go func() {
		defer finalize(true)
		if err := srv.Serve(provisioner); err != nil {
			log.Fatalf("Failed to serve provisioner: %v", err)
		}
	}()
	<-provisioner.Ready()
	if err := provisioner.Validate(); err != nil {
		log.Fatalf("Failed to validate reverse provisioner connection: %v", err)
	}
	scheduler.Provisioner = provisioner
	logger.Info("Scheduler connected to %v", provConn.RemoteAddr())

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
		srv.Stop()
		scheduler.Close()
		done.Done()
	}()

	// Start gRPC server
	go func() {
		defer finalize(true)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Start daemon
	go func() {
		defer finalize(true)
		if err := scheduler.Start(); err != nil {
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
