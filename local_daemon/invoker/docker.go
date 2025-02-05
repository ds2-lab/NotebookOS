package invoker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
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
	DockerImageNameDefault = "scusemua/jupyter-gpu:latest"

	DockerStorageVolume        = "STORAGE"
	DockerStorageVolumeDefault = "/kernel_storage"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080

	DockerKernelName     = "kernel-%s-%s"
	VarContainerImage    = "{image}"
	VarConnectionFile    = "{connection_file}"
	VarContainerName     = "{container_name}"
	VarContainerNewName  = "{container_new_name}"
	VarContainerNetwork  = "{network_name}"
	VarStorageVolume     = "{storage}"
	VarConfigFile        = "{config_file}"
	VarKernelId          = "{kernel_id}"
	VarSessionId         = "{session_id}"
	VarDebugPort         = "{kernel_debug_port}"
	VarKernelDebugPyPort = "{kernel_debugpy_port}"
	HostMountDir         = "{host_mount_dir}"
	TargetMountDir       = "{target_mount_dir}"

	dockerErrorPrefix = "docker: Error response from daemon: "
)

var (
	// dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} -v {storage}:/storage -v {config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} -e CONNECTION_FILE_PATH=\"{target_mount_dir}/{connection_file}\" -e IPYTHON_CONFIG_PATH=\"/home/jovyan/.ipython/profile_default/ipython_config.json\" {image}"

	// This version has debugpy.
	// dockerInvokerCmd  = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -p {kernel_debug_port}:{kernel_debug_port} -p {kernel_debugpy_port}:{kernel_debugpy_port} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} --security-opt seccomp=unconfined --label component=kernel_replica --label kernel_id={kernel_id} --label logging=promtail --label logging_jobname={kernel_id} --label app=distributed_cluster"
	dockerInvokerCmd = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} --security-opt seccomp=unconfined --label component=kernel_replica --label kernel_id={kernel_id} --label logging=promtail --label logging_jobname={kernel_id} --label app=distributed_cluster"

	// dockerShutdownCmd is used to shut down a running kernel container
	dockerShutdownCmd = "docker stop {container_name}"
	// dockerRemoveCmd is used to fully remove a stopped kernel container
	dockerRemoveCmd = "docker rm -f {container_name}"
	// dockerRenameCmd is used to rename a stopped kernel container
	dockerRenameCmd = "docker container rename {container_name} {container_new_name}"

	ErrDockerContainerCreationFailed = errors.New("failed to create docker container for new kernel")
	ErrUnexpectedReplicaExpression   = fmt.Errorf("unexpected replica expression, expected url")
	ErrConfigurationFilesDoNotExist  = errors.New("one or more of the necessary configuration files do not exist on the host's file system")
)

type DockerInvoker struct {
	LocalInvoker
	containerCreatedAt                   time.Time
	containerMetricsProvider             ContainerMetricsProvider
	connInfo                             *jupyter.ConnectionInfo
	opts                                 *DockerInvokerOptions
	invokerCmd                           string
	hostMountDir                         string
	containerName                        string
	kernelId                             string
	dockerNetworkName                    string
	DeploymentMode                       types.DeploymentMode
	WorkloadId                           string
	id                                   string
	tempBase                             string
	remoteStorageEndpoint                string
	remoteStorage                        string
	dockerStorageBase                    string
	targetMountDir                       string
	AssignedGpuDeviceIds                 []int32
	prometheusMetricsPort                int
	KernelDebugPort                      int
	electionTimeoutSeconds               int
	smrPort                              int
	closing                              int32
	containerCreated                     bool
	simulateWriteAfterExec               bool
	simulateWriteAfterExecOnCriticalPath bool
	SimulateTrainingUsingSleep           bool
	BindGPUs                             bool
	RunKernelsInGdb                      bool
	BindAllGpus                          bool
	BindDebugpyPort                      bool
	SaveStoppedKernelContainers          bool
	simulateCheckpointingLatency         bool
	SmrEnabled                           bool
	IsInDockerSwarm                      bool

	// S3Bucket is the AWS S3 bucket name if we're using AWS S3 for our remote storage.
	S3Bucket string

	// AwsRegion is the AWS region in which to create/look for the S3 bucket (if we're using AWS S3 for remote storage).
	AwsRegion string

	// RedisPassword is the password to access Redis (only relevant if using Redis for remote storage).
	RedisPassword string

	// RedisPort is the port of the Redis server (only relevant if using Redis for remote storage).
	RedisPort int

	// RedisDatabase is the database number to use (only relevant if using Redis for remote storage).
	RedisDatabase int
}

// DockerInvokerOptions encapsulates a number of configuration parameters required by the DockerInvoker in order
// to properly configure the kernel replica containers.
type DockerInvokerOptions struct {
	WorkloadId                           string
	RemoteStorage                        string
	DockerStorageBase                    string
	RemoteStorageEndpoint                string
	AssignedGpuDeviceIds                 []int32
	KernelDebugPort                      int
	PrometheusMetricsPort                int
	ElectionTimeoutSeconds               int
	RunKernelsInGdb                      bool `name:"run_kernels_in_gdb" description:"If true, then the kernels will be run in GDB."`
	SimulateWriteAfterExecOnCriticalPath bool
	SimulateCheckpointingLatency         bool
	SmrEnabled                           bool
	SimulateWriteAfterExec               bool
	BindAllGpus                          bool
	IsInDockerSwarm                      bool
	SimulateTrainingUsingSleep           bool
	BindDebugpyPort                      bool
	SaveStoppedKernelContainers          bool
	BindGPUs                             bool
	S3Bucket                             string
	AwsRegion                            string
	RedisPassword                        string
	RedisPort                            int
	RedisDatabase                        int
}

func NewDockerInvoker(connInfo *jupyter.ConnectionInfo, opts *DockerInvokerOptions, containerMetricsProvider ContainerMetricsProvider) *DockerInvoker {
	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}

	if len(opts.RemoteStorageEndpoint) == 0 {
		panic("remote storage endpoint is empty.")
	}

	var dockerNetworkName = os.Getenv(DockerNetworkNameEnv)

	invoker := &DockerInvoker{
		connInfo:                             connInfo,
		opts:                                 opts,
		tempBase:                             utils.GetEnv(DockerTempBase, DockerTempBaseDefault),
		hostMountDir:                         utils.GetEnv(HostMountDirectory, HostMountDirectoryDefault),
		targetMountDir:                       utils.GetEnv(TargetMountDirectory, TargetMountDirectoryDefault),
		smrPort:                              smrPort,
		remoteStorageEndpoint:                opts.RemoteStorageEndpoint,
		remoteStorage:                        opts.RemoteStorage,
		id:                                   uuid.NewString(),
		KernelDebugPort:                      opts.KernelDebugPort,
		dockerNetworkName:                    dockerNetworkName,
		dockerStorageBase:                    opts.DockerStorageBase,
		RunKernelsInGdb:                      opts.RunKernelsInGdb,
		containerMetricsProvider:             containerMetricsProvider,
		simulateCheckpointingLatency:         opts.SimulateCheckpointingLatency,
		IsInDockerSwarm:                      opts.IsInDockerSwarm,
		prometheusMetricsPort:                opts.PrometheusMetricsPort,
		electionTimeoutSeconds:               opts.ElectionTimeoutSeconds,
		simulateWriteAfterExec:               opts.SimulateWriteAfterExec,
		simulateWriteAfterExecOnCriticalPath: opts.SimulateWriteAfterExecOnCriticalPath,
		WorkloadId:                           opts.WorkloadId,
		SmrEnabled:                           opts.SmrEnabled,
		SimulateTrainingUsingSleep:           opts.SimulateTrainingUsingSleep,
		AssignedGpuDeviceIds:                 opts.AssignedGpuDeviceIds,
		BindAllGpus:                          opts.BindAllGpus,
		BindDebugpyPort:                      opts.BindDebugpyPort,
		SaveStoppedKernelContainers:          opts.SaveStoppedKernelContainers,
		BindGPUs:                             opts.BindGPUs,
		S3Bucket:                             opts.S3Bucket,
		AwsRegion:                            opts.AwsRegion,
		RedisPassword:                        opts.RedisPassword,
		RedisPort:                            opts.RedisPort,
		RedisDatabase:                        opts.RedisDatabase,
	}

	config.InitLogger(&invoker.log, invoker)

	invoker.initDeploymentMode(opts)

	invoker.LocalInvoker.statusChanged = invoker.defaultStatusChangedHandler

	invoker.invokerCmd = strings.ReplaceAll(dockerInvokerCmd, VarContainerNetwork, utils.GetEnv(DockerNetworkNameEnv, DockerNetworkNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarStorageVolume, utils.GetEnv(DockerStorageVolume, DockerStorageVolumeDefault))

	if invoker.RunKernelsInGdb {
		invoker.invokerCmd += " -e RUN_IN_GDB=1"
	}

	gpuCommandSnippet := invoker.InitGpuCommand()
	invoker.invokerCmd += gpuCommandSnippet

	if invoker.BindDebugpyPort {
		debugpyPort := invoker.KernelDebugPort + 10000
		invoker.invokerCmd += fmt.Sprintf(" -p %d:%d", debugpyPort, debugpyPort)
	}

	if invoker.KernelDebugPort > 1024 {
		invoker.invokerCmd += fmt.Sprintf(" -p %d:%d", invoker.KernelDebugPort, invoker.KernelDebugPort)
	}

	invoker.invokerCmd += " {image}"
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarContainerImage,
		utils.GetEnv(DockerImageName, DockerImageNameDefault))

	return invoker
}

func (ivk *DockerInvoker) initDeploymentMode(opts *DockerInvokerOptions) {
	// This is a DockerInvoker, so it's one of these two.
	if opts.IsInDockerSwarm {
		ivk.DeploymentMode = types.DockerSwarmMode
	} else {
		ivk.DeploymentMode = types.DockerComposeMode
	}
}

// InitGpuCommand adds/creates the GPU-binding-related part of the docker invoke command.
//
// InitGpuCommand returns the GPU command snippet that was generated.
func (ivk *DockerInvoker) InitGpuCommand() string {
	// If we're simulating training using time.sleep (in Python), then we can just return.
	// Likewise, if we're not supposed to bind GPUs at all, then we just return.
	if ivk.SimulateTrainingUsingSleep || !ivk.BindGPUs {
		return ""
	}

	// If we're binding all available GPUs, then we simply append "--gpus all" to the command.
	if ivk.BindAllGpus {
		return " --gpus all"
	}

	// If no GPU device IDs were specified (and BindAllGpus is false), then we can just return.
	if len(ivk.AssignedGpuDeviceIds) == 0 {
		ivk.log.Warn("The use of real GPUs is enabled; however, no GPU device IDs were specified, " +
			"and we were not instructed to bind all GPUs...")
		return ""
	}

	// We'll need to append something like this to the invocation command:
	//
	// --gpus '"device=0,2"'
	//
	// The snippet above would, as an example, bind GPUs 0 and 2 to the container.
	gpuCommandSnippet := " --gpus 'device={device_ids}'"

	// Iterate over all the assigned GPU device IDs, building out the string to include in the GPU command snippet.
	deviceIdsString := ""
	for i, deviceId := range ivk.AssignedGpuDeviceIds {
		// Append the GPU device ID to the string.
		deviceIdsString += fmt.Sprintf("%d", deviceId)

		// If there is going to be another GPU device ID, then we'll append a comma before continuing with the loop.
		if i < len(ivk.AssignedGpuDeviceIds)-1 {
			deviceIdsString += ","
		}
	}

	// Replace the "{device_ids}" substring in the gpuCommandSnippet with the string that we just built out.
	gpuCommandSnippet = strings.ReplaceAll(gpuCommandSnippet, "{device_ids}", deviceIdsString)

	return gpuCommandSnippet
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
	ivk.kernelId = spec.Kernel.Id

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
	ivk.log.Debug("kernel debug port = %d", ivk.KernelDebugPort)

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
	cmd = strings.ReplaceAll(cmd, VarDebugPort, fmt.Sprintf("%d", ivk.KernelDebugPort))
	// cmd = strings.ReplaceAll(cmd, VarKernelDebugPyPort, fmt.Sprintf("%d", ivk.KernelDebugPort+10000))

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

	if ivk.SaveStoppedKernelContainers {
		return ivk.renameStoppedContainer()
	}

	return ivk.removeStoppedContainer()
}

func (ivk *DockerInvoker) removeStoppedContainer() error {
	removeCommandStr := strings.ReplaceAll(dockerRemoveCmd, VarContainerName, ivk.containerName)
	removeCommandArgv := strings.Split(removeCommandStr, " ")

	removeCommand := exec.CommandContext(context.Background(), removeCommandArgv[0], removeCommandArgv[1:]...)

	var removeStdoutBuffer, removeStderrBuffer bytes.Buffer
	removeCommand.Stdout = &removeStdoutBuffer
	removeCommand.Stderr = &removeStderrBuffer

	if err := removeCommand.Run(); err != nil {
		errorMessage := removeStderrBuffer.String()
		ivk.log.Warn("Failed to remove container %s: %v (%v)\n", ivk.containerName, errorMessage, err)

		return errors.Join(err, fmt.Errorf(errorMessage))
	}

	return nil
}

// renameStoppedContainer will rename the stopped docker container with name <foo>
// to a new name <foo>-old-<suffix>, where <suffix> is a
func (ivk *DockerInvoker) renameStoppedContainer() error {
	renameCmdStr := strings.ReplaceAll(dockerRenameCmd, VarContainerName, ivk.containerName)

	// Generate a timestamp suffix at millisecond granularity
	suffix := time.Now().Format("2006-01-02-15-04-05.000")

	newName := fmt.Sprintf("%s-old-%s", ivk.containerName, suffix)

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

		// Simply remove the container.
		return ivk.removeStoppedContainer()
	}

	return nil
}

func (ivk *DockerInvoker) oldRenameStoppedContainer() error {
	// Rename the stopped Container so that we can create a new one with the same name in its place.
	idx := 0

	for idx < 8192 /* This is both very inefficient and will break if we migrate a container > 8192 times. */ {
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
				return errors.New(errorMessage)
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

	ivk.closedAt = time.Time{} // Update closedAt to extend expiration time
	return ivk.status, nil
}

//func (ivk *DockerInvoker) GetReplicaAddress(kernel *proto.KernelSpec, replicaId int32) string {
//	return fmt.Sprintf("%s:%d", ivk.generateKernelName(kernel, replicaId), ivk.smrPort)
//}

// generateKernelName generates and returns a name for the kernel container based on the kernel ID and replica ID.
func (ivk *DockerInvoker) generateKernelName(kernel *proto.KernelSpec, replicaId int32) string {
	// We append a string of random characters to the end of the name to help mitigate the risk of name collisions
	// when re-running the same workload (with the same kernels/sessions) multiple times on the same cluster.
	return fmt.Sprintf(DockerKernelName, fmt.Sprintf("%s-%d", kernel.Id, replicaId), utils.GenerateRandomString(8))
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
			StorageBase:                  ivk.dockerStorageBase,
			SMRNodeID:                    int(spec.ReplicaId),
			SMRNodes:                     spec.Replicas,
			SMRJoin:                      spec.Join,
			RegisterWithLocalDaemon:      true,
			LocalDaemonAddr:              hostname,
			RemoteStorageEndpoint:        ivk.remoteStorageEndpoint,
			RemoteStorage:                ivk.remoteStorage,
			PrometheusServerPort:         ivk.prometheusMetricsPort,
			SpecCpus:                     float64(spec.Kernel.ResourceSpec.Cpu),
			SpecMemoryMb:                 float64(spec.Kernel.ResourceSpec.Memory),
			SpecGpus:                     int(spec.Kernel.ResourceSpec.Gpu),
			SpecVramGb:                   float64(spec.Kernel.ResourceSpec.Vram),
			DeploymentMode:               ivk.DeploymentMode,
			SimulateCheckpointingLatency: ivk.simulateCheckpointingLatency,
			ElectionTimeoutSeconds:       ivk.electionTimeoutSeconds,
			WorkloadId:                   ivk.WorkloadId,
			SmrEnabled:                   ivk.SmrEnabled,
			SimulateTrainingUsingSleep:   ivk.SimulateTrainingUsingSleep,
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
