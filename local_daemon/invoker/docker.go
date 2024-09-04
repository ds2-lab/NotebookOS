package invoker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	DockerNetworkNameEnv     = "DOCKER_NETWORK_NAME"
	DockerNetworkNameDefault = "distributed_cluster_default"

	DockerTempBase        = "KERNEL_TEMP_BASE"
	DockerTempBaseDefault = ""

	DockerImageName        = "KERNEL_IMAGE"
	DockerImageNameDefault = "scusemua/jupyter:latest"

	DockerStorageVolume        = "STORAGE"
	DockerStorageVolumeDefault = "/kernel_storage"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080

	DockerKernelName     = "kernel-%s"
	VarContainerImage    = "{image}"
	VarConnectionFile    = "{connection_file}"
	VarContainerName     = "{container_name}"
	VarContainerNewName  = "{container_new_name}"
	VarContainerNetwork  = "{network_name}"
	VarStorageVolume     = "{storage}"
	VarConfigFile        = "{config_file}"
	VarKernelId          = "{kernel_id}"
	VarSessionId         = "{session_id}"
	VarSpecCpu           = "{spec_cpu}"
	VarSpecMemory        = "{spec_memory}"
	VarSpecGpu           = "{spec_gpu}"
	VarDebugPort         = "{kernel_debug_port}"
	VarKernelDebugPyPort = "{kernel_debugpy_port}"
	HostMountDir         = "{host_mount_dir}"
	TargetMountDir       = "{target_mount_dir}"
)

var (
	// dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} -v {storage}:/storage -v {config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} -e CONNECTION_FILE_PATH=\"{target_mount_dir}/{connection_file}\" -e IPYTHON_CONFIG_PATH=\"/home/jovyan/.ipython/profile_default/ipython_config.json\" {image}"
	dockerInvokerCmd  = "docker run -d -t --name {container_name} --ulimit core=-1 --mount source=coredumps_volume,target=/cores --network-alias {container_name} --network {network_name} -p {kernel_debug_port}:{kernel_debug_port} -p {kernel_debugpy_port}:{kernel_debugpy_port} -v {storage}:/storage -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json -e CONNECTION_FILE_PATH={target_mount_dir}/{connection_file} -e IPYTHON_CONFIG_PATH=/home/jovyan/.ipython/profile_default/ipython_config.json -e SPEC_CPU={spec_cpu} -e SPEC_MEM={spec_memory} -e SPEC_GPU={spec_gpu} -e SESSION_ID={session_id} -e KERNEL_ID={kernel_id} -e DEPLOYMENT_MODE=docker --security-opt seccomp=unconfined --label kernel_id={kernel_id} --label app=distributed_cluster {image}"
	dockerShutdownCmd = "docker stop {container_name}"
	dockerRenameCmd   = "docker container rename {container_name} {container_new_name}"

	ErrUnexpectedReplicaExpression  = fmt.Errorf("unexpected replica expression, expected url")
	ErrConfigurationFilesDoNotExist = errors.New("one or more of the necessary configuration files do not exist on the host's file system")
)

type DockerInvoker struct {
	LocalInvoker
	connInfo             *jupyter.ConnectionInfo
	opts                 *DockerInvokerOptions
	tempBase             string
	invokerCmd           string // Command used to create the Docker container.
	containerName        string // Name of the launched container; this is the empty string before the container is launched.
	dockerNetworkName    string // The name of the Docker network that the Local Daemon container is running within.
	smrPort              int    // Port used by the SMR cluster.
	closing              int32  // Indicates whether the container is closing/shutting down.
	id                   string // Uniquely identifies this Invoker instance.
	kernelDebugPort      int    // Debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	hdfsNameNodeEndpoint string // Endpoint of the HDFS namenode.
	dockerStorageBase    string // Base directory in which the persistent store data is stored.
}

type DockerInvokerOptions struct {
	// Endpoint of the HDFS namenode.
	HdfsNameNodeEndpoint string

	// Debug port used within the kernel to expose an HTTP server and the go net/pprof debug server.
	KernelDebugPort int

	// Base directory in which the persistent store data is stored when running in docker mode.
	DockerStorageBase string

	// Indicates whether we're running within WSL (Windows Subsystem for Linux).
	// If we are, then there is some additional configuration required for the kernel containers in order for
	// them to be able to connect to HDFS running in the host (WSL).
	UsingWSL bool
}

func NewDockerInvoker(connInfo *jupyter.ConnectionInfo, opts *DockerInvokerOptions) *DockerInvoker {
	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}

	if len(opts.HdfsNameNodeEndpoint) == 0 {
		panic("HDFS NameNode endpoint is empty.")
	}

	var dockerNetworkName = os.Getenv(DockerNetworkNameEnv)

	invoker := &DockerInvoker{
		connInfo:             connInfo,
		opts:                 opts,
		tempBase:             utils.GetEnv(DockerTempBase, DockerTempBaseDefault),
		smrPort:              smrPort,
		hdfsNameNodeEndpoint: opts.HdfsNameNodeEndpoint,
		id:                   uuid.NewString(),
		kernelDebugPort:      opts.KernelDebugPort,
		dockerNetworkName:    dockerNetworkName,
		dockerStorageBase:    opts.DockerStorageBase,
	}
	invoker.LocalInvoker.statusChanged = invoker.defaultStatusChangedHandler
	invoker.invokerCmd = strings.ReplaceAll(dockerInvokerCmd, VarContainerImage, utils.GetEnv(DockerImageName, DockerImageNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarContainerNetwork, utils.GetEnv(DockerNetworkNameEnv, DockerNetworkNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarStorageVolume, utils.GetEnv(DockerStorageVolume, DockerStorageVolumeDefault))

	config.InitLogger(&invoker.log, invoker)

	// invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarLocalDaemonAddr, localDaemonAddr)
	return invoker
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

	hostMountDir := os.Getenv("HOST_MOUNT_DIR")
	targetMountDir := os.Getenv("TARGET_MOUNT_DIR")

	ivk.log.Debug("hostMountDir = \"%v\"\n", hostMountDir)
	ivk.log.Debug("targetMountDir = \"%v\"\n", hostMountDir)
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

	ivk.log.Debug("{hostMountDir}/{connectionFile}\"%v\"\n", hostMountDir+"/"+filepath.Base(connectionFile))
	ivk.log.Debug("{hostMountDir}/{configFile}=\"%v\"\n", hostMountDir+"/"+filepath.Base(configFile))

	ivk.log.Debug("{targetMountDir}/{connectionFile}\"%v\"\n", targetMountDir+"/"+filepath.Base(connectionFile))
	ivk.log.Debug("{targetMountDir}/{configFile}=\"%v\"\n", targetMountDir+"/"+filepath.Base(configFile))

	ivk.containerName = kernelName
	connectionInfo.IP = ivk.containerName // Overwrite IP with container name
	cmd := strings.ReplaceAll(ivk.invokerCmd, VarContainerName, ivk.containerName)
	cmd = strings.ReplaceAll(cmd, TargetMountDir, targetMountDir)
	cmd = strings.ReplaceAll(cmd, HostMountDir, hostMountDir)
	cmd = strings.ReplaceAll(cmd, VarConnectionFile, filepath.Base(connectionFile))
	cmd = strings.ReplaceAll(cmd, VarConfigFile, filepath.Base(configFile))
	cmd = strings.ReplaceAll(cmd, VarKernelId, spec.Kernel.Id)
	cmd = strings.ReplaceAll(cmd, VarSessionId, spec.Kernel.Session)
	cmd = strings.ReplaceAll(cmd, VarSpecCpu, fmt.Sprintf("%d", spec.Kernel.ResourceSpec.Cpu))
	cmd = strings.ReplaceAll(cmd, VarSpecMemory, fmt.Sprintf("%d", spec.Kernel.ResourceSpec.Memory))
	cmd = strings.ReplaceAll(cmd, VarSpecGpu, fmt.Sprintf("%d", spec.Kernel.ResourceSpec.Gpu))
	cmd = strings.ReplaceAll(cmd, VarDebugPort, fmt.Sprintf("%d", ivk.kernelDebugPort))
	cmd = strings.ReplaceAll(cmd, VarKernelDebugPyPort, fmt.Sprintf("%d", ivk.kernelDebugPort+1000))

	for i, arg := range spec.Kernel.Argv {
		spec.Kernel.Argv[i] = strings.ReplaceAll(arg, VarConnectionFile, connectionFile)
	}

	argv := append(strings.Split(cmd, " "), spec.Kernel.Argv...)

	// argv = append(argv, ">")
	// argv = append(argv, "kernel_output.log")
	argv = append(argv, "2>&1")

	configurationFilesExist := ivk.validateConfigFilesExist(connectionFile, configFile)
	if !configurationFilesExist {
		ivk.log.Error("One or both of the configuration files do not exist. Cannot create Docker container for kernel %s.", kernelName)
		return nil, ErrConfigurationFilesDoNotExist
	}

	// Start kernel process
	ivk.log.Debug("Launch kernel: \"%v\"\n", argv)
	if err := ivk.launchKernel(ctx, kernelName, argv); err != nil {
		return nil, ivk.reportLaunchError(err)
	}

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
		ivk.log.Error("[Error] Failed to stop container/kenel %s: %v\n", ivk.containerName, err)
		ivk.log.Error("STDOUT: %s", outb.String())
		ivk.log.Error("STDERR: %s", errb.String())
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
	for idx < 50 /* TODO: This is bad/highly inefficient and will also break if a Container is migrated more than 50 times. */ {
		rename_cmd_str := strings.ReplaceAll(dockerRenameCmd, VarContainerName, ivk.containerName)
		newName := fmt.Sprintf("%s-old-%d", ivk.containerName, idx)
		rename_cmd_str = strings.ReplaceAll(rename_cmd_str, VarContainerNewName, newName)
		rename_argv := strings.Split(rename_cmd_str, " ")
		ivk.log.Debug("Renaming (stopped) container %s via %s.", ivk.containerName, rename_argv)
		renameCmd := exec.CommandContext(context.Background(), rename_argv[0], rename_argv[1:]...)

		var renameOutb, renameErrb bytes.Buffer
		renameCmd.Stdout = &renameOutb
		renameCmd.Stderr = &renameErrb

		if err := renameCmd.Run(); err != nil {
			ivk.log.Error("[Error] Failed to rename container %s: %v\n", ivk.containerName, err)
			ivk.log.Error("STDOUT: %s", renameOutb.String())
			ivk.log.Error("STDERR: %s", renameErrb.String())
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

	addr, err := url.Parse(spec.Replicas[spec.ReplicaId-1]) // ReplicaId starts from 1
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

	if err := cmd.Run(); err != nil {
		ivk.log.Debug("Failed to launch container/kernel %s: %v", name, err)
		ivk.log.Error("STDOUT: %s", outb.String())
		ivk.log.Error("STDERR: %s", errb.String())
		return err
	}

	ivk.log.Debug("Successfully launched container/kernel %s. [DockerInvoker ID = \"%s\"]", name, ivk.id)
	return nil
}
