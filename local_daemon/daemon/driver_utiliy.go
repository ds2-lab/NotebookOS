package daemon

import (
	"bytes"
	"context"
	"errors"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

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

// CreateConsulAndTracer creates/initializes and returns the Tracer and Consul Clients.
func CreateConsulAndTracer(options *domain.LocalDaemonOptions) (opentracing.Tracer, *consul.Client) {
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

// GetGrpcOptions builds the grpc.ServerOption slice and returns it.
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

// getNameOfDockerContainer attempts to discover the name of the Docker container in which we are running.
// This should only be called when we are using the Docker Compose or Docker Swarm deployment mode.
func getNameOfDockerContainer() string {
	hostnameEnv := os.Getenv("HOSTNAME")

	if len(hostnameEnv) == 0 {
		globalLogger.Error("Could not retrieve valid value from HOSTNAME environment variable.")
		globalLogger.Error("Returning default value for NodeName: \"%s\"", types.DockerNode)
		return types.DockerNode
	}

	globalLogger.Info("Retrieved value for HOSTNAME environment variable: \"%s\"", hostnameEnv)

	// We will use this command to retrieve the name of this Docker container.
	unformattedCommand := "docker inspect {container_hostname_env} --format='{{.Name}}'"
	formattedCommand := strings.ReplaceAll(unformattedCommand, "{container_hostname_env}", hostnameEnv)
	argv := strings.Split(formattedCommand, " ")

	globalLogger.Info("Executing shell command: %s", utils.LightBlueStyle.Render(formattedCommand))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)

	var stdoutBuffer, stderrBuffer bytes.Buffer
	cmd.Stdout = &stdoutBuffer
	cmd.Stderr = &stderrBuffer

	if err := cmd.Run(); err != nil {
		globalLogger.Error("[Error] Failed to retrieve container name: %v\n", err)
		globalLogger.Error("STDERR: %s", stderrBuffer.String())
		globalLogger.Error("Returning default value for NodeName: \"%s\"", types.DockerNode)
		return types.DockerNode
	}

	containerName := strings.TrimSpace(stdoutBuffer.String())

	if strings.HasPrefix(containerName, "'") {
		containerName = containerName[1:]
	}

	if strings.HasSuffix(containerName, "'") {
		containerName = containerName[0 : len(containerName)-1]
	}

	if strings.HasPrefix(containerName, "/") {
		containerName = containerName[1:]
	}

	globalLogger.Info("Resolved container name: \"%s\"", containerName)
	return containerName
}

func CreateAndStartLocalDaemonComponents(options *domain.LocalDaemonOptions, done *sync.WaitGroup, finalize LocalDaemonFinalizer, sig chan os.Signal) (*SchedulerDaemonImpl, func()) {
	var nodeName string
	if options.IsLocalMode() {
		nodeName = options.NodeName
	} else if options.IsDockerMode() {
		nodeName = getNameOfDockerContainer()
	} else {
		nodeName = os.Getenv("NODE_NAME")
	}

	// We largely disable the DevicePlugin server if we're running in LocalMode or if we're not running in Kubernetes mode.
	disableDevicePluginServer := options.DeploymentMode != string(types.KubernetesMode)
	devicePluginServer := device.NewVirtualGpuPluginServer(&options.VirtualGpuPluginServerOptions, nodeName, disableDevicePluginServer)

	globalLogger.Debug("Local Daemon Options:\n%s", options.PrettyString(2))

	tracer, consulClient := CreateConsulAndTracer(options)

	// Initialize grpc server
	scheduler := New(&options.ConnectionInfo, options, options.KernelRegistryPort, options.Port, devicePluginServer, nodeName)

	srv, lis, err := scheduler.connectToGateway(options.ProvisionerAddr, finalize, tracer)
	if err != nil {
		log.Fatalf(utils.RedStyle.Render("Failed to connect to Cluster Gateway: %v\n"), err)
	}

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
		err := scheduler.Close()
		if err != nil {
			globalLogger.Error("Error while closing scheduler: %v", err)
		}
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
		err := lis.Close()
		if err != nil {
			globalLogger.Error("Error while closing listener: %v", err)
		}

		err = scheduler.provisioner.(*Provisioner).Close()
		if err != nil {
			globalLogger.Error("Error while closing provisioner: %v", err)
		}
	}

	return scheduler, closeConnections
}
