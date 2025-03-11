package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/utils"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/scusemua/distributed-notebook/common/consul"
	"github.com/scusemua/distributed-notebook/common/tracing"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/local_daemon/device"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
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

	errHostnameUnavailable = errors.New("\"HOSTNAME\" environment variable was not set")
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

		globalLogger.Info("Initializing consulClient agent [host: %v]...", options.ConsulAddr)
		consulClient, err = consul.NewClient(options.ConsulAddr)
		if err != nil {
			log.Fatalf("Got error while initializing consulClient agent: %v", err)
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

// getNameOrIdOfDockerContainerNonJson retrieves the container name or full container ID without
// specifying the format to be JSON for the docker command used to retrieve the desired value.
//
// Node that the hostnameEnv is probably the beginning of the docker container ID, but not necessarily
// the full container ID.
func getNameOrIdOfDockerContainerNonJson(hostnameEnv string, getName bool) (string, error) {
	// We will use this command to retrieve the name of this Docker container.
	unformattedCommand := "docker inspect {container_hostname_env} --format='{{.{target_field}}}'"
	formattedCommand := strings.ReplaceAll(unformattedCommand, "{container_hostname_env}", hostnameEnv)

	if getName {
		formattedCommand = strings.ReplaceAll(formattedCommand, "{target_field}", "Name")
	} else {
		formattedCommand = strings.ReplaceAll(formattedCommand, "{target_field}", "Id")
	}

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
		return types.DockerNode, err
	}

	containerName := strings.TrimSpace(stdoutBuffer.String())

	containerName = strings.TrimPrefix(containerName, "'")
	containerName = strings.TrimSuffix(containerName, "'")
	containerName = strings.TrimPrefix(containerName, "/")

	globalLogger.Info("Resolved container name: \"%s\"", containerName)
	return containerName, nil
}

// getNameOfDockerContainerNonJson attempts to retrieve the name and full ID of the Docker container using the command:
// docker inspect {container_hostname_env} --format=json
func getNameAndIdOfDockerContainerNonJson() (string, string, error) {
	hostnameEnv := os.Getenv("HOSTNAME")

	if len(hostnameEnv) == 0 {
		globalLogger.Warn("Could not retrieve valid value from HOSTNAME environment variable.")
		globalLogger.Warn("Returning default value for NodeName: \"%s\"", types.DockerNode)
		return types.DockerNode, "", errHostnameUnavailable
	}

	globalLogger.Info("Retrieved value for HOSTNAME environment variable: \"%s\"", hostnameEnv)

	containerName, err := getNameOrIdOfDockerContainerNonJson(hostnameEnv, true)
	if err != nil {
		return types.DockerNode, "", err
	}

	containerId, err := getNameOrIdOfDockerContainerNonJson(hostnameEnv, false)
	if err != nil {
		return containerName, "", err
	}

	return containerName, containerId, nil
}

func getNameAndIdOfDockerContainer() (string, string, error) {
	hostnameEnv := os.Getenv("HOSTNAME")

	if len(hostnameEnv) == 0 {
		globalLogger.Warn("Could not retrieve valid value from HOSTNAME environment variable.")
		globalLogger.Warn("Returning default value for NodeName: \"%s\"", types.DockerNode)
		return types.DockerNode, "", errHostnameUnavailable
	}

	globalLogger.Info("Retrieved value for HOSTNAME environment variable: \"%s\"", hostnameEnv)

	// We will use this command to retrieve the name of this Docker container.
	unformattedCommand := "docker inspect {container_hostname_env} --format=json"
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
		return types.DockerNode, "", err
	}

	var outputMap []map[string]interface{}
	err := json.Unmarshal(stdoutBuffer.Bytes(), &outputMap)
	if err != nil || len(outputMap) == 0 {
		globalLogger.Error("Failed to unmarshal JSON output of `docker inspect` command: %v", err)

		// We ~might~ be able to get the container name if we go about it a little differently...
		// Probably not, but it is worth a try.
		var containerName, containerId string
		containerName, containerId, err = getNameAndIdOfDockerContainerNonJson()

		return containerName, containerId, err
	}

	containerName := outputMap[0]["Name"].(string)
	containerId := outputMap[0]["Id"].(string)

	containerName = strings.TrimPrefix(containerName, "'")
	containerName = strings.TrimSuffix(containerName, "'")
	containerName = strings.TrimPrefix(containerName, "/")

	globalLogger.Info("Resolved container name: \"%s\"", containerName)
	globalLogger.Info("Resolved container ID: \"%s\"", containerId)
	return containerName, containerId, nil
}

func CreateAndStartLocalDaemonComponents(options *domain.LocalDaemonOptions, done *sync.WaitGroup, finalize LocalDaemonFinalizer, sig chan os.Signal) (*LocalScheduler, func()) {
	var nodeName, dockerContainerId string
	if options.IsLocalMode() {
		nodeName = options.NodeName
	} else if options.IsDockerMode() {
		nodeName, dockerContainerId, _ = getNameAndIdOfDockerContainer()
	} else {
		nodeName = os.Getenv("NODE_NAME")
	}

	// We largely disable the DevicePlugin server if we're running in LocalMode or if we're not running in Kubernetes mode.
	disableDevicePluginServer := options.DeploymentMode != string(types.KubernetesMode)
	devicePluginServer := device.NewVirtualGpuPluginServer(&options.VirtualGpuPluginServerOptions, nodeName,
		disableDevicePluginServer)

	globalLogger.Info("Initializing Local Scheduler with options: %s", options.PrettyString(2))

	// Initialize grpc server
	scheduler := New(&options.ConnectionInfo, options, options.KernelRegistryPort, options.Port, devicePluginServer,
		nodeName, dockerContainerId)

	err := scheduler.connectToGateway(options.ProvisionerAddr, finalize)
	if err != nil {

		// If we're in local mode, then we're running unit tests, so we'll just... return.
		if options.LocalMode {
			fmt.Printf(utils.RedStyle.Render("Failed to connect to Cluster Gateway: %v\n"), err)
			return nil, nil
		}

		log.Fatalf(utils.RedStyle.Render("Failed to connect to Cluster Gateway: %v\n"), err)
	}

	// Start detecting stop signals
	done.Add(1)
	go func() {
		s := <-sig

		globalLogger.Warn("Received signal: \"%v\". Shutting down...", s.String())
		scheduler.grpcServer.Stop()
		err := scheduler.Close()
		if err != nil {
			globalLogger.Error("Error while closing scheduler: %v", err)
		}
		done.Done()
	}()

	// Start daemon
	go func() {
		defer finalize(true)
		if err := scheduler.Start(); err != nil {
			// If we're in local mode, then we're running unit tests, so we'll just... return.
			if options.LocalMode {
				return
			}

			log.Fatalf("Error during daemon serving: %v", err)
		}
	}()

	// Start device plugin.
	go func() {
		// If we're in local mode, then this will return immediately, but we don't want to shut down the program (which is what finalize will do).
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
		err := scheduler.listener.Close()
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
