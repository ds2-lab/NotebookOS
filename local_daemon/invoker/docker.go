package invoker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

	PrewarmKernelName    = "prewarm-%s-%s"
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
	VarAwsRegion         = "{aws_region}"
	VarKernelDebugPyPort = "{kernel_debugpy_port}"
	HostMountDir         = "{host_mount_dir}"
	TargetMountDir       = "{target_mount_dir}"

	dockerErrorPrefix = "docker: Error response from daemon: "

	// DisableActualContainerCreationEnv is an environment variable used during unit tests
	// that prevents the DockerInvoker from actually creating any Docker containers.
	DisableActualContainerCreationEnv = "DISABLE_CONTAINERS"

	// DockerInvokerKernelConnInfoIp is used to modify what IP address is placed into the jupyter.ConnectionInfo
	// structs created by the InvokeWithContext method of the DockerInvoker.
	//
	// DockerInvokerKernelConnInfoIp is intended to be used during unit testing.
	DockerInvokerKernelConnInfoIp = "DOCKER_INVOKER_KERNEL_CONN_IP"

	// DockerInvokerKernelConnInfoIpDefault is the default IP address is placed into the jupyter.ConnectionInfo
	// structs created by the InvokeWithContext method of the DockerInvoker.
	//
	// This can be changed by setting a value for the DockerInvokerKernelConnInfoIp environment variable.
	DockerInvokerKernelConnInfoIpDefault = "0.0.0.0"
)

var (
	// dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} -v {storage}:/storage -v {config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} -e CONNECTION_FILE_PATH=\"{target_mount_dir}/{connection_file}\" -e IPYTHON_CONFIG_PATH=\"/home/jovyan/.ipython/profile_default/ipython_config.json\" {image}"

	// This version has debugpy.
	// dockerInvokerCmd  = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -p {kernel_debug_port}:{kernel_debug_port} -p {kernel_debugpy_port}:{kernel_debugpy_port} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} --security-opt seccomp=unconfined --label component=kernel_replica --label kernel_id={kernel_id} --label logging=promtail --label logging_jobname={kernel_id} --label app=distributed_cluster"
	dockerInvokerCmd = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e AWS_REGION={aws_region} -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} --security-opt seccomp=unconfined --label component=kernel_replica --label kernel_id={kernel_id} --label logging=promtail --label logging_jobname={kernel_id} --label app=distributed_cluster"

	// dockerShutdownCmd is used to shut down a running kernel container
	dockerShutdownCmd = "docker stop {container_name}"
	// dockerRemoveCmd is used to fully remove a stopped kernel container
	dockerRemoveCmd = "docker rm -f {container_name}"
	// dockerRenameCmd is used to rename a stopped kernel container
	dockerRenameCmd = "docker container rename {container_name} {container_new_name}"

	ErrDockerContainerCreationFailed = errors.New("failed to create docker container for new kernel")
	ErrUnexpectedReplicaExpression   = fmt.Errorf("unexpected replica expression, expected url")
	ErrConfigurationFilesDoNotExist  = errors.New("one or more of the necessary configuration files do not exist on the host's file system")
	ErrIllegalFieldMutation          = errors.New("cannot mutate specified field of non-prewarm container")
)

type DockerInvoker struct {
	LocalInvoker

	// opts are the options specified when first creating the DockerInvoker.
	opts *DockerInvokerOptions

	// containerMetricsProvider enables the DockerInvoker to publish relevant metrics,
	// such as latency of creating containers.
	containerMetricsProvider ContainerMetricsProvider

	tempBase       string
	hostMountDir   string
	targetMountDir string

	// invokerCmd is the command used to create the Docker container.
	invokerCmd string

	// containerName is the name of the launched container; this is the empty string before the container is launched.
	containerName string

	// dockerNetworkName is the name of the Docker network that the Local Daemon container is running within.
	dockerNetworkName string

	// dockerStorageBase is the base directory in which the persistent store data is stored.
	dockerStorageBase string

	// originalContainerType is the original type of the container created by this DockerInvoker
	originalContainerType scheduling.ContainerType

	// currentContainerType is the current type of the container created by this DockerInvoker
	currentContainerType scheduling.ContainerType

	// closing indicates whether the container is closing/shutting down.
	closing int32

	// IsInDockerSwarm indicates whether we're running within a Docker Swarm cluster.
	// If IsInDockerSwarm is false, then we're just a regular docker compose application.
	//
	// When in Docker Swarm, we add a constraint when invoking kernel replicas to ensure
	// that they are scheduled onto our node.
	IsInDockerSwarm bool

	// RunKernelsInGdb indicates whether the kernels should be run in GDB.
	RunKernelsInGdb bool

	// DockerContainersDisabled is a flag that is set when the DisableActualContainerCreationEnv environment variable
	// is set. When DockerContainersDisabled is true, the DockerInvoker will not actually create any Docker
	// containers. This is useful for unit tests.
	DockerContainersDisabled bool

	// containerCreatedWg is a wait group that is set to 0 when the container is created.
	// If there is an error while creating the container, then the wait group's counter will still be decremented,
	// but the containerCreated variable will still hold the value false.
	containerCreatedWg sync.WaitGroup

	// containerCreated is a flag that is set to true if the container creation is successful.
	containerCreated atomic.Bool

	// kernelConnInfoIp is the IP address stored in the jupyter.ConnectionInfo structs created and returned
	// by the DockerInvoker's InvokeWithContext method.
	//
	// The value of InfoIp is initialized to "0.0.0.0" by default; however, this can be changed/overridden by
	// setting the DockerInvokerKernelConnInfoIp environment variable.
	//
	// Default: "0.0.0.0"
	kernelConnInfoIp string

	// overwriteIpWithContainerName instructs the DockerInvoker to overwrite the IP address in the
	// jupyter.ConnectionInfo returned by the InvokeWithContext method with the name of the Docker container (when
	// true). This is so the LocalScheduler can properly connect with the Docker container using its container name
	// as its host name. However, during unit tests, if we change the value of kernelConnInfoIp by setting the
	// DockerInvokerKernelConnInfoIp environment variable to something, then we will flip the
	// overwriteIpWithContainerName flag to false so that the IP specified in the DockerInvokerKernelConnInfoIp
	// environment variable is used.
	overwriteIpWithContainerName bool
}

// DockerInvokerOptions encapsulates a number of configuration parameters required by the DockerInvoker in order
// to properly configure the kernel replica containers.
type DockerInvokerOptions struct {
	// RemoteStorageEndpoint is the endpoint of the remote storage.
	RemoteStorageEndpoint string

	// RemoteStorage is the type of remote storage, either 'hdfs' or 'redis'
	RemoteStorage string

	// DockerStorageBase is the base directory in which the persistent store data is stored when running in docker mode.
	DockerStorageBase string

	WorkloadId string

	AwsRegion     string
	RedisPassword string

	// AssignedGpuDeviceIds is the list of GPU device IDs that are being assigned to the kernel replica that
	// the DockerInvoker will be invoking.
	AssignedGpuDeviceIds []int32

	// KernelDebugPort is the debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	KernelDebugPort int32

	// PrometheusMetricsPort is the port that the container should serve prometheus metrics on.
	PrometheusMetricsPort int

	// ElectionTimeoutSeconds is how long kernel leader elections wait to receive all proposals before
	// deciding on a leader
	ElectionTimeoutSeconds int

	RedisPort     int
	RedisDatabase int

	// RunKernelsInGdb specifies that, if true, then the kernels will be run in GDB.
	RunKernelsInGdb bool `name:"run_kernels_in_gdb" description:"If true, then the kernels will be run in GDB."`

	// IsInDockerSwarm indicates whether we're running within a Docker Swarm cluster.
	// If IsInDockerSwarm is false, then we're just a regular docker compose application.
	//
	// When in Docker Swarm, we add a constraint when invoking kernel replicas to ensure
	// that they are scheduled onto our node.
	IsInDockerSwarm bool

	// SimulateWriteAfterExecOnCriticalPath is a flag indicating whether the kernel should perform a simulated network
	// write after executing code.
	SimulateWriteAfterExec bool

	// SimulateWriteAfterExecOnCriticalPath is a flag indicating whether the simulated network write after executing
	// code should be on the critical path.
	SimulateWriteAfterExecOnCriticalPath bool

	// SimulateCheckpointingLatency controls whether the kernels will be configured to simulate the latency of
	// performing checkpointing after a migration (read) and after executing code (write).
	SimulateCheckpointingLatency bool

	SmrEnabled bool

	// BindAllGpus instructs the DockerInvoker to bind ALL GPUs to the container when creating it
	// (if SimulateTrainingUsingSleep is false). Note that if SimulateTrainingUsingSleep is true,
	// then this option is ultimately ignored.
	BindAllGpus bool

	// SimulateTrainingUsingSleep controls whether we tell the kernels to train using real GPUs and real PyTorch code or not.
	SimulateTrainingUsingSleep bool

	// BindDebugpyPort specifies whether to bind a port to kernel containers for DebugPy
	BindDebugpyPort bool

	// If true, then do not fully remove stopped kernel containers.
	SaveStoppedKernelContainers bool

	// BindGPUs indicates whether we should bind GPUs to the container or not.
	// We can still train with CPU-PyTorch, so we only want to bind GPUs if we are going to be using real GPUs.
	BindGPUs bool

	// RetrieveDatasetsFromS3 is a bool flag that, when true, instructs the KernelInvoker to configure the kernels to retrieve datasets from an S3 bucket.
	RetrieveDatasetsFromS3 bool

	// DatasetsS3Bucket is the S3 bucket from which the kernels retrieve the datasets when RetrieveDatasetsFromS3 is set to true.
	DatasetsS3Bucket string
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
		LocalInvoker: LocalInvoker{
			workloadId:                           opts.WorkloadId,
			remoteStorageEndpoint:                opts.RemoteStorageEndpoint,
			remoteStorage:                        opts.RemoteStorage,
			id:                                   uuid.NewString(),
			SMRPort:                              smrPort,
			connInfo:                             connInfo,
			AwsRegion:                            opts.AwsRegion,
			RedisPassword:                        opts.RedisPassword,
			prometheusMetricsPort:                opts.PrometheusMetricsPort,
			electionTimeoutSeconds:               opts.ElectionTimeoutSeconds,
			simulateWriteAfterExec:               opts.SimulateWriteAfterExec,
			simulateWriteAfterExecOnCriticalPath: opts.SimulateWriteAfterExecOnCriticalPath,
			SmrEnabled:                           opts.SmrEnabled,
			SimulateTrainingUsingSleep:           opts.SimulateTrainingUsingSleep,
			AssignedGpuDeviceIds:                 opts.AssignedGpuDeviceIds,
			BindAllGpus:                          opts.BindAllGpus,
			BindDebugpyPort:                      opts.BindDebugpyPort,
			SaveStoppedKernelContainers:          opts.SaveStoppedKernelContainers,
			BindGPUs:                             opts.BindGPUs,
			RedisPort:                            opts.RedisPort,
			RedisDatabase:                        opts.RedisDatabase,
			KernelDebugPort:                      opts.KernelDebugPort,
			simulateCheckpointingLatency:         opts.SimulateCheckpointingLatency,
			RetrieveDatasetsFromS3:               opts.RetrieveDatasetsFromS3,
			DatasetsS3Bucket:                     opts.DatasetsS3Bucket,
		},
		opts:                         opts,
		RunKernelsInGdb:              opts.RunKernelsInGdb,
		tempBase:                     utils.GetEnv(DockerTempBase, DockerTempBaseDefault),
		hostMountDir:                 utils.GetEnv(HostMountDirectory, HostMountDirectoryDefault),
		targetMountDir:               utils.GetEnv(TargetMountDirectory, TargetMountDirectoryDefault),
		dockerNetworkName:            dockerNetworkName,
		dockerStorageBase:            opts.DockerStorageBase,
		containerMetricsProvider:     containerMetricsProvider,
		IsInDockerSwarm:              opts.IsInDockerSwarm,
		kernelConnInfoIp:             DockerInvokerKernelConnInfoIpDefault,
		overwriteIpWithContainerName: true,
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

	val := os.Getenv(DisableActualContainerCreationEnv)
	if val != "" {
		invoker.log.Warn("Docker containers are DISABLED.")
		invoker.DockerContainersDisabled = true
	}

	val = os.Getenv(DockerInvokerKernelConnInfoIp)
	if val != "" {
		invoker.log.Warn("Kernel ConnectionInfo IP address overridden: \"%s\"", val)
		invoker.kernelConnInfoIp = val
		invoker.overwriteIpWithContainerName = false
	}

	invoker.containerCreatedWg.Add(1)

	return invoker
}

// WaitForContainerToBeCreated will block until the target DockerInvoker has created its container.
//
// If DockerContainersDisabled is set to true, then WaitForContainerToBeCreated will return whenever the DockerInvoker
// would have created its container.
func (ivk *DockerInvoker) WaitForContainerToBeCreated() {
	ivk.containerCreatedWg.Wait()
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
	return ivk.containerCreated.Load()
}

// KernelCreatedAt returns the time at which the DockerInvoker created the kernel container.
func (ivk *DockerInvoker) KernelCreatedAt() (time.Time, bool) {
	if !ivk.KernelCreated() {
		return time.Time{}, false
	}

	return ivk.createdAt, true
}

// TimeSinceContainerCreated returns the amount of time that has elapsed since the DockerInvoker created the kernel container.
func (ivk *DockerInvoker) TimeSinceContainerCreated() (time.Duration, bool) {
	if !ivk.KernelCreated() {
		return time.Duration(-1), false
	}

	return time.Since(ivk.createdAt), true
}

func (ivk *DockerInvoker) InvokeWithContext(ctx context.Context, spec *proto.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec
	ivk.status = jupyter.KernelStatusInitializing
	ivk.kernelId = spec.Kernel.Id

	if spec.PrewarmContainer {
		ivk.originalContainerType = scheduling.PrewarmContainer
	} else {
		ivk.originalContainerType = scheduling.StandardContainer
	}

	ivk.currentContainerType = ivk.originalContainerType

	ivk.log.Debug("[DockerInvoker] Invoking with context now.\n")

	var (
		kernelName string
		port       int
		err        error
	)

	kernelName, port, err = ivk.extractKernelNamePort(spec)
	if err != nil {
		return nil, ivk.reportLaunchError(err)
	}
	if port == 0 {
		port = ivk.SMRPort
	}
	if len(spec.Replicas) < int(spec.NumReplicas) {
		// Regenerate replica addresses
		spec.Replicas = make([]string, spec.NumReplicas)
		for i := int32(0); i < spec.NumReplicas; i++ {
			spec.Replicas[i] = fmt.Sprintf("%s:%d", ivk.generateKernelName(spec.Kernel, i+1), port)
		}
	}

	ivk.log.Debug("[DockerInvoker] kernel Name: \"%s\". Port: %d.\n", kernelName, port)

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

	//ivk.log.Debug("filepath.Base(connectionFile)=\"%v\"\n", filepath.Base(connectionFile))
	//ivk.log.Debug("filepath.Base(configFile)=\"%v\"\n", filepath.Base(configFile))
	//
	//ivk.log.Debug("{hostMountDir}/{connectionFile}\"%v\"\n", ivk.hostMountDir+"/"+filepath.Base(connectionFile))
	//ivk.log.Debug("{hostMountDir}/{configFile}=\"%v\"\n", ivk.hostMountDir+"/"+filepath.Base(configFile))
	//
	//ivk.log.Debug("{targetMountDir}/{connectionFile}\"%v\"\n", ivk.targetMountDir+"/"+filepath.Base(connectionFile))
	//ivk.log.Debug("{targetMountDir}/{configFile}=\"%v\"\n", ivk.targetMountDir+"/"+filepath.Base(configFile))

	ivk.containerName = kernelName

	if ivk.overwriteIpWithContainerName {
		connectionInfo.IP = ivk.containerName // Overwrite IP with container name
	}

	cmd := strings.ReplaceAll(ivk.invokerCmd, VarContainerName, ivk.containerName)
	cmd = strings.ReplaceAll(cmd, TargetMountDir, ivk.targetMountDir)
	cmd = strings.ReplaceAll(cmd, HostMountDir, ivk.hostMountDir)
	cmd = strings.ReplaceAll(cmd, VarConnectionFile, filepath.Base(connectionFile))
	cmd = strings.ReplaceAll(cmd, VarConfigFile, filepath.Base(configFile))
	cmd = strings.ReplaceAll(cmd, VarKernelId, spec.Kernel.Id)
	cmd = strings.ReplaceAll(cmd, VarSessionId, spec.Kernel.Session)
	cmd = strings.ReplaceAll(cmd, VarAwsRegion, ivk.AwsRegion)
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

	ivk.containerCreated.Store(true)
	ivk.createdAt = time.Now()
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
	ivk.log.Debug("Closing DockerInvoker for %v container \"%s\" [currentContainerType=%v].",
		ivk.currentContainerType, ivk.containerName, ivk.currentContainerType)

	if ivk.containerName == "" {
		ivk.log.Error("Cannot stop %v kernel container. The kernel container has not been launched yet. [DockerInvoker ID = \"%s\"]",
			ivk.currentContainerType, ivk.id)
		return jupyter.ErrKernelNotLaunched
	}

	if !atomic.CompareAndSwapInt32(&ivk.closing, 0, 1) {
		ivk.log.Debug("%v container %s is already closing. Waiting for it to close.",
			ivk.currentContainerType, ivk.containerName)

		// Wait for the closing to be done.
		<-ivk.closed
		return nil
	}

	argv := strings.Split(strings.ReplaceAll(dockerShutdownCmd, VarContainerName, ivk.containerName), " ")
	ivk.log.Debug("Stopping %v container %s via %s.", ivk.currentContainerType, ivk.containerName, argv)

	// Only execute the actual Docker command if containers are not disabled (which they are during unit tests).
	if !ivk.DockerContainersDisabled {
		cmd := exec.CommandContext(context.Background(), argv[0], argv[1:]...)

		var outb, errb bytes.Buffer
		cmd.Stdout = &outb
		cmd.Stderr = &errb

		if err := cmd.Run(); err != nil {
			ivk.log.Error("Failed to stop %v container/kernel %s: %v\n",
				ivk.currentContainerType, ivk.containerName, errb.String())
			return err
		}
	}

	ivk.closedAt = time.Now()
	close(ivk.closed)
	// TODO: Commented out because it can cause deadlocks (receiving on a nil channel leads to indefinite blocking).
	//		 Why did we do this in the first place...?
	//ivk.closed = nil
	ivk.log.Debug("Closed %v container/kernel %s.\n", ivk.currentContainerType, ivk.containerName)
	ivk.setStatus(jupyter.KernelStatusExited)

	// Status will not change anymore, reset the handler.
	ivk.statusChanged = ivk.defaultStatusChangedHandler

	// If Docker containers are disabled, then return now. Don't try to rename/remove anything.
	if ivk.DockerContainersDisabled {
		return nil
	}

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

// RenameContainer renames the container.
func (ivk *DockerInvoker) RenameContainer(name string) error {
	panic("Not implemented")
}

// generateKernelName generates and returns a name for the kernel container based on the kernel ID and replica ID.
func (ivk *DockerInvoker) generateKernelName(kernel *proto.KernelSpec, replicaId int32) string {
	// We append a string of random characters to the end of the name to help mitigate the risk of name collisions
	// when re-running the same workload (with the same kernels/sessions) multiple times on the same cluster.
	return fmt.Sprintf(DockerKernelName, fmt.Sprintf("%s-%d", kernel.Id, replicaId), utils.GenerateRandomString(8))
}

// generatePrewarmedContainerName generates a name to use for a new pre-warmed container that is not associated
// with a real/actual kernel (yet).
func (ivk *DockerInvoker) generatePrewarmedContainerName(spec *proto.KernelReplicaSpec) string {
	// We append a string of random characters to the end of the name to help mitigate the risk of name collisions
	// when re-running the same workload (with the same kernels/sessions) multiple times on the same cluster.
	return fmt.Sprintf(PrewarmKernelName, fmt.Sprintf("%s", spec.Kernel.Id), utils.GenerateRandomString(8))
}

// extractKernelName extracts kernel name and port from the replica spec
func (ivk *DockerInvoker) extractKernelNamePort(spec *proto.KernelReplicaSpec) (name string, port int, err error) {
	if spec.PrewarmContainer {
		return ivk.generatePrewarmedContainerName(spec), 0, nil
	}

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
		IP:              ivk.kernelConnInfoIp,
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
			WorkloadId:                   ivk.workloadId,
			SmrEnabled:                   ivk.SmrEnabled,
			SimulateTrainingUsingSleep:   ivk.SimulateTrainingUsingSleep,
			PrewarmContainer:             spec.PrewarmContainer,
			RetrieveDatasetsFromS3:       ivk.RetrieveDatasetsFromS3,
			DatasetsS3Bucket:             ivk.DatasetsS3Bucket,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}

func (ivk *DockerInvoker) launchKernel(ctx context.Context, name string, argv []string) error {
	if ivk.DockerContainersDisabled {
		ivk.log.Warn("Docker containers are disabled. Skipping kernel invocation.")
		ivk.containerCreatedWg.Done()
		return nil
	}

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

		ivk.containerCreatedWg.Done()
		return fmt.Errorf("%w: %s", ErrDockerContainerCreationFailed, errorMessage)
	}

	timeElapsed := time.Since(startTime)
	ivk.log.Debug("Successfully launched container/kernel %s in %v. [DockerInvoker ID = \"%s\"]",
		name, timeElapsed, ivk.id)

	if err := ivk.containerMetricsProvider.AddContainerCreationLatencyObservation(timeElapsed); err != nil {
		ivk.log.Error("Failed to persist container creation latency metric because: %v", err)
	}

	ivk.containerCreatedWg.Done()
	return nil
}

// ContainerIsPrewarm returns true if the CurrentContainerType of the target DockerInvoker is scheduling.PrewarmContainer.
func (ivk *DockerInvoker) ContainerIsPrewarm() bool {
	return ivk.currentContainerType == scheduling.PrewarmContainer
}

// CurrentContainerType is the current scheduling.ContainerType of the container created by the target DockerInvoker.
func (ivk *DockerInvoker) CurrentContainerType() scheduling.ContainerType {
	return ivk.currentContainerType
}

// OriginalContainerType is the original scheduling.ContainerType of the container created by the target DockerInvoker.
//
// OriginalContainerType can be used to determine if the container created by the target DockerInvoker was originally
// a scheduling.PrewarmContainer that has since been promoted to a scheduling.StandardContainer.
func (ivk *DockerInvoker) OriginalContainerType() scheduling.ContainerType {
	return ivk.originalContainerType
}

// PromotePrewarmedContainer records within the target KernelInvoker that its container, which must originally have
// been a scheduling.PrewarmContainer, is now a scheduling.StandardContainer.
//
// If the promotion is successful, then PromotePrewarmedContainer returns true.
//
// If the OriginalContainerType of the target KernelInvoker is KernelInvoker,
// then PromotePrewarmedContainer returns false.
func (ivk *DockerInvoker) PromotePrewarmedContainer() bool {
	// If the container is already a standard container (and therefore must have already been promoted), return false.
	if ivk.currentContainerType == scheduling.StandardContainer {
		ivk.log.Error("Cannot promote container \"%s\"; it is currently a %s container.",
			ivk.containerName, scheduling.StandardContainer)
		return false
	}

	// Update the current container type and return true.
	ivk.currentContainerType = scheduling.StandardContainer

	ivk.log.Debug("Promoted container \"%s\" to a %s container.",
		ivk.containerName, scheduling.StandardContainer)

	return true
}

// DemoteStandardContainer records within the target KernelInvoker that its container is now of type
// scheduling.PrewarmContainer.
//
// PRECONDITION: The container of the target KernelInvoker must be of type scheduling.StandardContainer when
// DemoteStandardContainer is called.
//
// If the demotion is successful, then PromotePrewarmedContainer returns nil.
//
// DemoteStandardContainer is the inverse of PromotePrewarmedContainer.
func (ivk *DockerInvoker) DemoteStandardContainer() error {
	// If the container is already a standard container (and therefore must have already been promoted), return false.
	if ivk.currentContainerType == scheduling.PrewarmContainer {
		return fmt.Errorf("%w: container is already of type \"%s\"",
			scheduling.ErrInvalidContainerType, scheduling.PrewarmContainer)
	}

	// Update the current container type and return true.
	ivk.currentContainerType = scheduling.PrewarmContainer
	return nil
}

// SetAssignedGpuDeviceIds will panic if the CurrentContainerType of the target DockerInvoker is
// scheduling.StandardContainer.
//
// You can only mutate the GetAssignedGpuDeviceIds field of a DockerInvoker struct if the CurrentContainerType of the
// target DockerInvoker struct is scheduling.PrewarmContainer.
func (ivk *DockerInvoker) SetAssignedGpuDeviceIds(assignedGpuDeviceIds []int32) {
	if !ivk.ContainerIsPrewarm() {
		panic("Cannot mutate the GetAssignedGpuDeviceIds field of a DockerInvoker a non-prewarm container.")
	}

	ivk.LocalInvoker.SetAssignedGpuDeviceIds(assignedGpuDeviceIds)
}

// SetDebugPort will panic if the CurrentContainerType of the target DockerInvoker is scheduling.StandardContainer.
//
// You can only mutate the DebugPort field of a DockerInvoker struct if the CurrentContainerType of the target
// DockerInvoker struct is scheduling.PrewarmContainer.
func (ivk *DockerInvoker) SetDebugPort(kernelDebugPort int32) {
	if !ivk.ContainerIsPrewarm() {
		panic("Cannot mutate the DebugPort field of a DockerInvoker a non-prewarm container.")
	}

	ivk.LocalInvoker.SetDebugPort(kernelDebugPort)
}

// SetKernelId will panic if the CurrentContainerType of the target DockerInvoker is scheduling.StandardContainer.
//
// You can only mutate the KernelId field of a DockerInvoker struct if the CurrentContainerType of the target
// DockerInvoker struct is scheduling.PrewarmContainer.
func (ivk *DockerInvoker) SetKernelId(kernelId string) {
	if !ivk.ContainerIsPrewarm() {
		panic("Cannot mutate the KernelId field of a DockerInvoker a non-prewarm container.")
	}

	ivk.LocalInvoker.SetKernelId(kernelId)
}

// SetWorkloadId will panic if the CurrentContainerType of the target DockerInvoker is scheduling.StandardContainer.
//
// You can only mutate the WorkloadId field of a DockerInvoker struct if the CurrentContainerType of the target
// DockerInvoker struct is scheduling.PrewarmContainer.
func (ivk *DockerInvoker) SetWorkloadId(workloadId string) {
	if !ivk.ContainerIsPrewarm() {
		panic("Cannot mutate the WorkloadId field of a DockerInvoker a non-prewarm container.")
	}

	ivk.LocalInvoker.SetWorkloadId(workloadId)
}
