package invoker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/utils"
)

const (
	DockerNetworkNameEnv     = "DOCKER_NETWORK_NAME"
	DockerNetworkNameDefault = "distributed_cluster_default"

	HostMountDirectory        = "HOST_MOUNT_DIR"
	HostMountDirectoryDefault = "/home/ubuntu/kernel_base"

	TargetMountDirectory        = "TARGET_MOUNT_DIR"
	TargetMountDirectoryDefault = "/kernel_base"

	DockerTempBase        = "KERNEL_TEMP_BASE_IN_CONTAINER"
	DockerTempBaseDefault = ""

	DockerImageName        = "KERNEL_IMAGE"
	DockerImageNameDefault = "scusemua/jupyter:latest"

	DockerStorageVolume        = "STORAGE"
	DockerStorageVolumeDefault = "/kernel_storage"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080

	DockerKernelName                  = "kernel-%s"
	VarContainerImage                 = "{image}"
	VarConnectionFile                 = "{connection_file}"
	VarContainerName                  = "{container_name}"
	VarContainerNewName               = "{container_new_name}"
	VarContainerNetwork               = "{network_name}"
	VarStorageVolume                  = "{storage}"
	VarConfigFile                     = "{config_file}"
	VarKernelId                       = "{kernel_id}"
	VarSessionId                      = "{session_id}"
	VarSpecCpu                        = "{spec_cpu}"
	VarSpecMemory                     = "{spec_memory}"
	VarSpecGpu                        = "{spec_gpu}"
	VarSpecVram                       = "{spec_vram}"
	VarDebugPort                      = "{kernel_debug_port}"
	VarPrometheusMetricsPort          = "{prometheus_metrics_port}"
	VarKernelDebugPyPort              = "{kernel_debugpy_port}"
	VarDeploymentMode                 = "{deployment_mode}"
	VarMaybeFlags                     = "{maybe_flags}"
	VarMaybeGdbFlag                   = "{maybe_gdb}"
	VarMaybeSimCheckPtLatency         = "{maybe_sim_checkpoint_latency}"
	VarMaybeDockerSwarmNodeConstraint = "{docker_swarm_node_constraint}"
	HostMountDir                      = "{host_mount_dir}"
	TargetMountDir                    = "{target_mount_dir}"

	dockerErrorPrefix = "docker: Error response from daemon: "
)

var (
	// dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} -v {storage}:/storage -v {config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} -e CONNECTION_FILE_PATH=\"{target_mount_dir}/{connection_file}\" -e IPYTHON_CONFIG_PATH=\"/home/jovyan/.ipython/profile_default/ipython_config.json\" {image}"
	dockerMaybeFlags  = "{maybe_gdb}{maybe_sim_checkpoint_latency}{docker_swarm_node_constraint}"
	dockerInvokerCmd  = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -p {kernel_debug_port}:{kernel_debug_port} -p {kernel_debugpy_port}:{kernel_debugpy_port} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e PROMETHEUS_METRICS_PORT={prometheus_metrics_port} -e SPEC_CPU={spec_cpu} -e SPEC_MEM={spec_memory} -e SPEC_GPU={spec_gpu} -e SPEC_VRAM={spec_vram} -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} -e DEPLOYMENT_MODE={deployment_mode} {maybe_flags} --security-opt seccomp=unconfined --label component=kernel_replica --label kernel_id={kernel_id} --label prometheus.metrics.port={prometheus_metrics_port} --label logging=promtail --label logging_jobname={kernel_id} --label app=distributed_cluster {image}"
	dockerShutdownCmd = "docker stop {container_name}"
	dockerRenameCmd   = "docker container rename {container_name} {container_new_name}"

	ErrDockerContainerCreationFailed = errors.New("failed to create docker container for new kernel")
	ErrUnexpectedReplicaExpression   = fmt.Errorf("unexpected replica expression, expected url")
	ErrConfigurationFilesDoNotExist  = errors.New("one or more of the necessary configuration files do not exist on the host's file system")
)

type DockerInvoker struct {
	LocalInvoker
	connInfo                     *jupyter.ConnectionInfo
	opts                         *DockerInvokerOptions
	tempBase                     string
	hostMountDir                 string
	targetMountDir               string
	invokerCmd                   string                           // Command used to create the Docker container.
	containerName                string                           // Name of the launched container; this is the empty string before the container is launched.
	dockerNetworkName            string                           // The name of the Docker network that the Local Daemon container is running within.
	smrPort                      int                              // Port used by the SMR cluster.
	closing                      int32                            // Indicates whether the container is closing/shutting down.
	id                           string                           // Uniquely identifies this Invoker instance.
	kernelDebugPort              int                              // Debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	hdfsNameNodeEndpoint         string                           // Endpoint of the HDFS NameNode.
	dockerStorageBase            string                           // Base directory in which the persistent store data is stored.
	containerCreatedAt           time.Time                        // containerCreatedAt is the time at which the DockerInvoker created the kernel container.
	containerCreated             bool                             // containerCreated is a bool indicating whether kernel the container has been created.
	containerMetricsProvider     metrics.ContainerMetricsProvider // containerMetricsProvider enables the DockerInvoker to publish relevant metrics, such as latency of creating containers.
	simulateCheckpointingLatency bool                             // simulateCheckpointingLatency controls whether the kernels will be configured to simulate the latency of performing checkpointing after a migration (read) and after executing code (write).
	runKernelsInGdb              bool                             // If true, then the kernels will be run in GDB.

	// IsInDockerSwarm indicates whether we're running within a Docker Swarm cluster.
	// If IsInDockerSwarm is false, then we're just a regular docker compose application.
	//
	// When in Docker Swarm, we add a constraint when invoking kernel replicas to ensure
	// that they are scheduled onto our node.
	isInDockerSwarm bool
}

type DockerInvokerOptions struct {
	// HdfsNameNodeEndpoint is the endpoint of the HDFS namenode.
	HdfsNameNodeEndpoint string

	// KernelDebugPort is the debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	KernelDebugPort int

	// DockerStorageBase is the base directory in which the persistent store data is stored when running in docker mode.
	DockerStorageBase string

	// UsingWSL indicates whether we're running within WSL (Windows Subsystem for Linux).
	// If we are, then there is some additional configuration required for the kernel containers in order for
	// them to be able to connect to HDFS running in the host (WSL).
	UsingWSL bool

	// RunKernelsInGdb specifies that, if true, then the kernels will be run in GDB.
	RunKernelsInGdb bool `name:"run_kernels_in_gdb" description:"If true, then the kernels will be run in GDB."`

	// SimulateCheckpointingLatency controls whether the kernels will be configured to simulate the latency of
	// performing checkpointing after a migration (read) and after executing code (write).
	SimulateCheckpointingLatency bool

	// IsInDockerSwarm indicates whether we're running within a Docker Swarm cluster.
	// If IsInDockerSwarm is false, then we're just a regular docker compose application.
	//
	// When in Docker Swarm, we add a constraint when invoking kernel replicas to ensure
	// that they are scheduled onto our node.
	IsInDockerSwarm bool

	// PrometheusMetricsPort is the port that the container should serve prometheus metrics on.
	PrometheusMetricsPort int
}

func NewDockerInvoker(connInfo *jupyter.ConnectionInfo, opts *DockerInvokerOptions, containerMetricsProvider metrics.ContainerMetricsProvider) *DockerInvoker {
	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}

	if len(opts.HdfsNameNodeEndpoint) == 0 {
		panic("HDFS NameNode endpoint is empty.")
	}

	var dockerNetworkName = os.Getenv(DockerNetworkNameEnv)

	invoker := &DockerInvoker{
		connInfo:                     connInfo,
		opts:                         opts,
		tempBase:                     utils.GetEnv(DockerTempBase, DockerTempBaseDefault),
		hostMountDir:                 utils.GetEnv(HostMountDirectory, HostMountDirectoryDefault),
		targetMountDir:               utils.GetEnv(TargetMountDirectory, TargetMountDirectoryDefault),
		smrPort:                      smrPort,
		hdfsNameNodeEndpoint:         opts.HdfsNameNodeEndpoint,
		id:                           uuid.NewString(),
		kernelDebugPort:              opts.KernelDebugPort,
		dockerNetworkName:            dockerNetworkName,
		dockerStorageBase:            opts.DockerStorageBase,
		runKernelsInGdb:              opts.RunKernelsInGdb,
		containerMetricsProvider:     containerMetricsProvider,
		simulateCheckpointingLatency: opts.SimulateCheckpointingLatency,
		isInDockerSwarm:              opts.IsInDockerSwarm,
	}

	invoker.LocalInvoker.statusChanged = invoker.defaultStatusChangedHandler
	invoker.invokerCmd = strings.ReplaceAll(dockerInvokerCmd, VarContainerImage, utils.GetEnv(DockerImageName, DockerImageNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarContainerNetwork, utils.GetEnv(DockerNetworkNameEnv, DockerNetworkNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarStorageVolume, utils.GetEnv(DockerStorageVolume, DockerStorageVolumeDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarPrometheusMetricsPort, fmt.Sprintf("%d", opts.PrometheusMetricsPort))

	maybeFlagCmd := dockerMaybeFlags
	if invoker.runKernelsInGdb {
		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeGdbFlag, "-e RUN_IN_GDB=1")
	} else {
		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeGdbFlag, "")
	}

	if invoker.simulateCheckpointingLatency {
		// The leading space is deliberate.
		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeSimCheckPtLatency, " -e SIMULATE_CHECKPOINTING_LATENCY=1")
		maybeFlagCmd = strings.TrimSpace(maybeFlagCmd)
	} else {
		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeSimCheckPtLatency, "")
	}

	if invoker.isInDockerSwarm {
		invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarDeploymentMode, "DOCKER_SWARM")

		schedulingConstraint := fmt.Sprintf(" -e constraint:node==%s", "")
		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeDockerSwarmNodeConstraint, schedulingConstraint)
		maybeFlagCmd = strings.TrimSpace(maybeFlagCmd)
	} else {
		invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarDeploymentMode, "DOCKER_COMPOSE")

		maybeFlagCmd = strings.ReplaceAll(maybeFlagCmd, VarMaybeDockerSwarmNodeConstraint, "")
	}

	maybeFlagCmd = strings.TrimSpace(maybeFlagCmd)
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarMaybeFlags, maybeFlagCmd)

	config.InitLogger(&invoker.log, invoker)

	return invoker
}

// KernelCreated returns a bool indicating whether kernel the container has been created.
func (ivk *DockerInvoker) KernelCreated() bool {
	return ivk.containerCreated
}

// KernelCreatedAt returns the time at which the DockerInvoker created the kernel container.
func (ivk *DockerInvoker) KernelCreatedAt() (time.Time, bool) {
	if !ivk.containerCreated {
		return time.Time{}, false
	}

	return ivk.containerCreatedAt, true
}

// TimeSinceContainerCreated returns the amount of time that has elapsed since the DockerInvoker created the kernel container.
func (ivk *DockerInvoker) TimeSinceContainerCreated() (time.Duration, bool) {
	if !ivk.containerCreated {
		return time.Duration(-1), false
	}

	return time.Since(ivk.containerCreatedAt), true
}

func (ivk *DockerInvoker) InvokeWithContext(ctx context.Context, spec *proto.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec
	ivk.status = jupyter.KernelStatusInitializing

	ivk.log.Debug("[DockerInvoker] Invoking with context now.\n")

	kernelName, port, err := ivk.extractKernelNamePort(spec)
	if err != nil {
		return nil, ivk.reportLaunchError(err)
	}
	if port == 0 {
		port = ivk.smrPort
	}
	if len(spec.Replicas) < int(spec.NumReplicas) {
		// Regenerate replica addresses
		spec.Replicas = make([]string, spec.NumReplicas)
		for i := int32(0); i < spec.NumReplicas; i++ {
			spec.Replicas[i] = fmt.Sprintf("%s:%d", ivk.generateKernelName(spec.Kernel, i+1), port)
		}
	}

	ivk.log.Debug("[DockerInvoker] Kernel Name: \"%s\". Port: %d.\n", kernelName, port)

	// Looking for available port
	connectionInfo, err := ivk.prepareConnectionInfo(spec.Kernel)
	if err != nil {
		ivk.log.Error("Error while preparing connection file: %v.\n", err)
		return nil, ivk.reportLaunchError(err)
	}

	//hostMountDir := os.Getenv("HOST_MOUNT_DIR")
	//targetMountDir := os.Getenv("TARGET_MOUNT_DIR")

	ivk.log.Debug("hostMountDir = \"%v\"\n", ivk.hostMountDir)
	ivk.log.Debug("targetMountDir = \"%v\"\n", ivk.hostMountDir)
	ivk.log.Debug("kernel debug port = %d", ivk.kernelDebugPort)

	ivk.log.Debug("Prepared connection info: %v\n", connectionInfo)

	// Write connection file and replace placeholders within in command line
	connectionFile, err := ivk.writeConnectionFile(ivk.tempBase, kernelName, connectionInfo)
	if err != nil {
		ivk.log.Error("Error while writing connection file: %v.\n", err)
		ivk.log.Error("Connection info: %v\n", connectionInfo)
		return nil, ivk.reportLaunchError(err)
	}

	ivk.log.Debug("Wrote connection file: %v\n", connectionFile)

	configInfo, _ := ivk.prepareConfigFile(spec)
	configInfo.SMRPort = port
	configFile, err := ivk.writeConfigFile(ivk.tempBase, kernelName, configInfo)
	if err != nil {
		ivk.log.Error("Error while writing config file: %v.\n", err)
		ivk.log.Error("Config info: %v\n", configInfo)
		return nil, ivk.reportLaunchError(err)
	}

	ivk.log.Debug("Wrote config file: %v\n", configFile)

	ivk.log.Debug("filepath.Base(connectionFile)=\"%v\"\n", filepath.Base(connectionFile))
	ivk.log.Debug("filepath.Base(configFile)=\"%v\"\n", filepath.Base(configFile))

	ivk.log.Debug("{hostMountDir}/{connectionFile}\"%v\"\n", ivk.hostMountDir+"/"+filepath.Base(connectionFile))
	ivk.log.Debug("{hostMountDir}/{configFile}=\"%v\"\n", ivk.hostMountDir+"/"+filepath.Base(configFile))

	ivk.log.Debug("{targetMountDir}/{connectionFile}\"%v\"\n", ivk.targetMountDir+"/"+filepath.Base(connectionFile))
	ivk.log.Debug("{targetMountDir}/{configFile}=\"%v\"\n", ivk.targetMountDir+"/"+filepath.Base(configFile))

	ivk.containerName = kernelName
	connectionInfo.IP = ivk.containerName // Overwrite IP with container name
	cmd := strings.ReplaceAll(ivk.invokerCmd, VarContainerName, ivk.containerName)
	cmd = strings.ReplaceAll(cmd, TargetMountDir, ivk.targetMountDir)
	cmd = strings.ReplaceAll(cmd, HostMountDir, ivk.hostMountDir)
	cmd = strings.ReplaceAll(cmd, VarConnectionFile, filepath.Base(connectionFile))
	cmd = strings.ReplaceAll(cmd, VarConfigFile, filepath.Base(configFile))
	cmd = strings.ReplaceAll(cmd, VarKernelId, spec.Kernel.Id)
	cmd = strings.ReplaceAll(cmd, VarSessionId, spec.Kernel.Session)
	cmd = strings.ReplaceAll(cmd, VarSpecCpu, fmt.Sprintf("%d", spec.Kernel.ResourceSpec.Cpu))
	cmd = strings.ReplaceAll(cmd, VarSpecMemory, fmt.Sprintf("%f", spec.Kernel.ResourceSpec.Memory))
	cmd = strings.ReplaceAll(cmd, VarSpecGpu, fmt.Sprintf("%d", spec.Kernel.ResourceSpec.Gpu))
	cmd = strings.ReplaceAll(cmd, VarSpecVram, fmt.Sprintf("%f", spec.Kernel.ResourceSpec.Vram))
	cmd = strings.ReplaceAll(cmd, VarDebugPort, fmt.Sprintf("%d", ivk.kernelDebugPort))
	cmd = strings.ReplaceAll(cmd, VarKernelDebugPyPort, fmt.Sprintf("%d", ivk.kernelDebugPort+1000))

	for i, arg := range spec.Kernel.Argv {
		spec.Kernel.Argv[i] = strings.ReplaceAll(arg, VarConnectionFile, connectionFile)
	}

	argv := append(strings.Split(cmd, " "), spec.Kernel.Argv...)

	argv = append(argv, "2>&1")

	configurationFilesExist := ivk.validateConfigFilesExist(connectionFile, configFile)
	if !configurationFilesExist {
		ivk.log.Error("One or both of the configuration files do not exist. Cannot create Docker container for kernel %s.", kernelName)
		return nil, ErrConfigurationFilesDoNotExist
	}

	// Start kernel process
	ivk.log.Debug("Launch kernel: \"%v\"\n", argv)
	if err = ivk.launchKernel(ctx, kernelName, argv); err != nil {
		return nil, ivk.reportLaunchError(err)
	}

	ivk.containerCreated = true
	ivk.containerCreatedAt = time.Now()
	ivk.setStatus(jupyter.KernelStatusRunning)
	return connectionInfo, nil
}

// Validate that the connection file and config file both exist (within the container's file system, which should be mounted to the host's).
// Returns true if both files exist; otherwise, returns false.
func (ivk *DockerInvoker) validateConfigFilesExist(connectionFile string, configFile string) bool {
	var (
		connectionFileExists = false
		configFileExists     = false
	)

	if _, err := os.Stat(connectionFile); err == nil {
		ivk.log.Debug("Connection file \"%s\" exists.", connectionFile)
		connectionFileExists = true
	} else {
		ivk.log.Error("Connection file \"%s\" does NOT exist.", connectionFile)
	}

	if _, err := os.Stat(configFile); err == nil {
		ivk.log.Debug("Config file \"%s\" exists.", configFile)
		configFileExists = true
	} else {
		ivk.log.Error("Config file \"%s\" does NOT exist.", configFile)
	}

	return configFileExists && connectionFileExists
}

func (ivk *DockerInvoker) Status() (jupyter.KernelStatus, error) {
	if ivk.status < jupyter.KernelStatusRunning {
		return 0, jupyter.ErrKernelNotLaunched
	} else {
		return ivk.status, nil
	}
}

func (ivk *DockerInvoker) Shutdown() error {
	return ivk.Close()
}

func (ivk *DockerInvoker) Close() error {
	if ivk.containerName == "" {
		ivk.log.Error("Cannot stop kernel container. The kernel container has not been launched yet. [DockerInvoker ID = \"%s\"]", ivk.id)
		return jupyter.ErrKernelNotLaunched
	}

	if !atomic.CompareAndSwapInt32(&ivk.closing, 0, 1) {
		ivk.log.Debug("Container %s is already closing. Waiting for it to close.", ivk.containerName)
		// Wait for the closing to be done.
		<-ivk.closed
		return nil
	}

	argv := strings.Split(strings.ReplaceAll(dockerShutdownCmd, VarContainerName, ivk.containerName), " ")
	ivk.log.Debug("Stopping container %s via %s.", ivk.containerName, argv)
	cmd := exec.CommandContext(context.Background(), argv[0], argv[1:]...)

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	if err := cmd.Run(); err != nil {
		errrorMessage := errb.String()
		ivk.log.Error("Failed to stop container/kenel %s: %v\n", ivk.containerName, errrorMessage)
		return err
	}

	ivk.closedAt = time.Now()
	close(ivk.closed)
	ivk.closed = nil
	ivk.log.Debug("Closed container/kernel %s.\n", ivk.containerName)
	ivk.setStatus(jupyter.KernelStatusExited)

	// Status will not change anymore, reset the handler.
	ivk.statusChanged = ivk.defaultStatusChangedHandler

	// Rename the stopped Container so that we can create a new one with the same name in its place.
	idx := 0
	for idx < 512 /* This is inefficient and will break if we migrate a container > 512 times. */ {
		renameCmdStr := strings.ReplaceAll(dockerRenameCmd, VarContainerName, ivk.containerName)
		newName := fmt.Sprintf("%s-old-%d", ivk.containerName, idx)
		renameCmdStr = strings.ReplaceAll(renameCmdStr, VarContainerNewName, newName)
		renameArgv := strings.Split(renameCmdStr, " ")
		ivk.log.Debug("Renaming (stopped) container %s via %s.", ivk.containerName, renameArgv)
		renameCmd := exec.CommandContext(context.Background(), renameArgv[0], renameArgv[1:]...)

		var renameStdoutBuffer, renameStderrBuffer bytes.Buffer
		renameCmd.Stdout = &renameStdoutBuffer
		renameCmd.Stderr = &renameStderrBuffer

		if err := renameCmd.Run(); err != nil {
			errorMessage := renameStderrBuffer.String()
			ivk.log.Warn("Failed to rename container %s: %v\n", ivk.containerName, errorMessage)

			// If the error is simply because this container name is already in-use, then we'll retry with another name.
			if strings.Contains(errorMessage, "You have to remove (or rename) that container to be able to reuse that name.") {
				ivk.log.Warn("Will retry using a different name for the old container.")
			} else {
				ivk.log.Error("This Docker error is unexpected. Unsure how to recover.")
				return fmt.Errorf(errorMessage)
			}

			idx += 1
			continue
		} else {
			break
		}
	}

	return nil
}

func (ivk *DockerInvoker) Wait() (jupyter.KernelStatus, error) {
	if ivk.containerName == "" {
		return 0, jupyter.ErrKernelNotLaunched
	}

	closed := ivk.closed
	if closed != nil {
		// Wait for kernel process to exit
		<-ivk.closed
	}

	ivk.closedAt = time.Time{} // Update closedAt to extend expriation time
	return ivk.status, nil
}

func (ivk *DockerInvoker) GetReplicaAddress(kernel *proto.KernelSpec, replicaId int32) string {
	return fmt.Sprintf("%s:%d", ivk.generateKernelName(kernel, replicaId), ivk.smrPort)
}

func (ivk *DockerInvoker) generateKernelName(kernel *proto.KernelSpec, replica_id int32) string {
	return fmt.Sprintf(DockerKernelName, fmt.Sprintf("%s-%d", kernel.Id, replica_id))
}

// extractKernelName extracts kernel name and port from the replica spec
func (ivk *DockerInvoker) extractKernelNamePort(spec *proto.KernelReplicaSpec) (name string, port int, err error) {
	if spec.ReplicaId > int32(len(spec.Replicas)) {
		return ivk.generateKernelName(spec.Kernel, spec.ReplicaId), 0, nil
	}

	addr, err := url.Parse(spec.Replicas[spec.ReplicaId-1]) // ReplicaID starts from 1
	if err != nil {
		return "", 0, ErrUnexpectedReplicaExpression
	}

	port, _ = strconv.Atoi(addr.Port()) // Invalid port will be ignored
	return addr.Hostname(), port, nil
}

func (ivk *DockerInvoker) prepareConnectionInfo(spec *proto.KernelSpec) (*jupyter.ConnectionInfo, error) {
	// Write connection file
	connectionInfo := &jupyter.ConnectionInfo{
		IP:              "0.0.0.0",
		Transport:       "tcp",
		ControlPort:     ivk.connInfo.ControlPort,
		ShellPort:       ivk.connInfo.ShellPort,
		StdinPort:       ivk.connInfo.StdinPort,
		HBPort:          ivk.connInfo.HBPort,
		IOSubPort:       ivk.connInfo.IOSubPort,
		IOPubPort:       ivk.connInfo.IOPubPort,
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
	}

	return connectionInfo, nil
}

func (ivk *DockerInvoker) prepareConfigFile(spec *proto.KernelReplicaSpec) (*jupyter.ConfigFile, error) {
	hostname, err := os.Hostname()
	if err != nil {
		ivk.log.Error("[ERROR] DockerInvoker could not resolve hostname because: %v", err)
		return nil, err
	}

	file := &jupyter.ConfigFile{
		DistributedKernelConfig: jupyter.DistributedKernelConfig{
			StorageBase:             ivk.dockerStorageBase,
			SMRNodeID:               int(spec.ReplicaId),
			SMRNodes:                spec.Replicas,
			SMRJoin:                 spec.Join,
			RegisterWithLocalDaemon: true,
			LocalDaemonAddr:         hostname,
			HdfsNameNodeEndpoint:    ivk.hdfsNameNodeEndpoint,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}

func (ivk *DockerInvoker) launchKernel(ctx context.Context, name string, argv []string) error {
	ivk.log.Debug("Starting Docker container %s now...\n", name)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	startTime := time.Now()
	if err := cmd.Run(); err != nil {
		ivk.log.Error("Failed to launch container/kernel %s: %v", name, err)
		ivk.log.Error("STDOUT: %s", outb.String())

		stderrOutput := errb.String()
		ivk.log.Error("STDERR: %s", stderrOutput)

		// Extract the error message, if possible.
		var errorMessage string
		if strings.HasPrefix(stderrOutput, dockerErrorPrefix) {
			errorMessage = stderrOutput[len(dockerErrorPrefix):]
		} else {
			errorMessage = stderrOutput
		}

		return fmt.Errorf("%w: %s", ErrDockerContainerCreationFailed, errorMessage)
	}

	timeElapsed := time.Since(startTime)
	ivk.log.Debug("Successfully launched container/kernel %s in %v. [DockerInvoker ID = \"%s\"]",
		name, timeElapsed, ivk.id)

	if err := ivk.containerMetricsProvider.AddContainerCreationLatencyObservation(timeElapsed); err != nil {
		ivk.log.Error("Failed to persist container creation latency metric because: %v", err)
	}

	return nil
}
