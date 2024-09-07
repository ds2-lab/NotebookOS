package daemon

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/proto"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/opentracing/opentracing-go"
	"github.com/zhangjyr/distributed-notebook/common/consul"
	"github.com/zhangjyr/distributed-notebook/common/tracing"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/local_daemon/device"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

//
// This file contains methods that are useful for a driver script for the Cluster Gateway.
//

const (
	ServiceName       = "gateway"
	connectionTimeout = time.Second
)

var (
	globalLogger = config.GetLogger("")
)

type LocalDaemonFinalizer func(bool)

// Create/initialize and return the Tracer and Consul Clients.
func CreateConsulAndTracer(options *domain.LocalDaemonOptions) (opentracing.Tracer, *consul.Client) {
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
func GetGrpcOptions(tracer opentracing.Tracer) []grpc.ServerOption {
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

	return gOpts
}

func CreateAndStartLocalDaemonComponents(options *domain.LocalDaemonOptions, done *sync.WaitGroup, finalize LocalDaemonFinalizer, sig chan os.Signal) (*SchedulerDaemonImpl, func()) {
	tracer, consulClient := CreateConsulAndTracer(options)

	gOpts := GetGrpcOptions(tracer)

	var nodeName string
	if options.IsLocalMode() {
		nodeName = options.NodeName
	} else if options.IsDockerMode() {
		nodeName = types.DockerNode
	} else {
		nodeName = os.Getenv("NODE_NAME")
	}

	// We largely disable the DevicePlugin server if we're running in LocalMode or if we're not running in Kubernetes mode.
	disableDevicePluginServer := options.DeploymentMode != string(types.KubernetesMode)
	devicePluginServer := device.NewVirtualGpuPluginServer(&options.VirtualGpuPluginServerOptions, nodeName, disableDevicePluginServer)

	// Initialize grpc server
	srv := grpc.NewServer(gOpts...)
	scheduler := New(&options.ConnectionInfo, &options.SchedulerDaemonOptions, options.KernelRegistryPort, devicePluginServer, nodeName)
	proto.RegisterLocalGatewayServer(srv, scheduler)

	// Initialize gRPC listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", options.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	// defer lis.Close()
	globalLogger.Info("Scheduler listening for gRPC at %v", lis.Addr())

	start := time.Now()
	var connectedToProvisioner = false
	var numAttempts = 1
	var provConn net.Conn
	for !connectedToProvisioner && time.Since(start) < (time.Minute*1) {
		globalLogger.Debug("Attempt #%d to connect to Provisioner (Gateway) at %s. Connection timeout: %v.", numAttempts, options.ProvisionerAddr, connectionTimeout)
		provConn, err = net.DialTimeout("tcp", options.ProvisionerAddr, connectionTimeout)

		if err != nil {
			globalLogger.Warn("Failed to connect to provisioner at %s on attempt #%d: %v", options.ProvisionerAddr, numAttempts, err)
			numAttempts += 1
			time.Sleep(time.Second * 3)
		} else {
			connectedToProvisioner = true
		}
	}

	// Initialize connection to the provisioner
	if !connectedToProvisioner {
		lis.Close()
		log.Fatalf("Failed to connect to provisioner after %d attempt(s). Most recent error: %v", numAttempts, err)
	}

	if provConn == nil {
		panic("provConn should not be nil at this point")
	}
	// defer provConn.Close()

	// Initialize provisioner and wait for ready
	provisioner, err := NewProvisioner(provConn)
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
	scheduler.SetProvisioner(provisioner)
	globalLogger.Info("Scheduler connected to %v", provConn.RemoteAddr())

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
		s := <-sig

		globalLogger.Warn("Received signal: \"%v\". Shutting down...", s.String())
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

	// Start device plugin.
	go func() {
		// If we're in local mode, then this will return immediately, but we don't want to shutdown the program (which is what finalize will do).
		// The same holds true if we're running in Docker mode (as opposed to Kubernetes mode).
		if options.DeploymentMode == string(types.KubernetesMode) {
			defer finalize(true)
		}

		for { // Do this forever.
			log.Println("Running the DevicePlugin server now.")
			if err := devicePluginServer.Run(); err != nil {
				if errors.Is(err, device.ErrSocketDeleted) {
					log.Println("DevicePlugin socket has been deleted. Must restart DevicePlugin.")

					// Stop the DevicePlugin server.
					log.Println("Stopping the DevicePlugin server now.")
					devicePluginServer.Stop()

					// Recreate the DevicePlugin server.
					log.Println("Recreating the DevicePlugin server now.")
					devicePluginServer = device.NewVirtualGpuPluginServer(&options.VirtualGpuPluginServerOptions, nodeName, disableDevicePluginServer)
				} else if errors.Is(err, device.ErrNotEnabled) {
					log.Println("DevicePlugin server will not be running as it is disabled. (We're either in Docker mode or Local mode.)")
					return
				} else {
					log.Fatalf("[ERROR] Exception encountered during device plugin serving: %v", err)
				}
			}
		}
	}()

	closeConnections := func() {
		lis.Close()
		provisioner.Close()
	}

	return scheduler, closeConnections
}
