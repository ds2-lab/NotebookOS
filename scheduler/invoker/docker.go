package invoker

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	DockerTempBase        = "KERNEL_TEMP_BASE"
	DockerTempBaseDefault = ""

	DockerImageName        = "KERNEL_IMAGE"
	DockerImageNameDefault = "scusemua/jupyter:latest"

	DockerNetworkName        = "KERNEL_NETWORK"
	DockerNetworkNameDefault = "local_daemon_default"

	DockerStorageVolume        = "STORAGE"
	DockerStorageVolumeDefault = "storage"

	KernelSMRPort        = "SMR_PORT"
	KernelSMRPortDefault = 8080

	DockerKernelName    = "kernel-%s"
	VarContainerImage   = "{image}"
	VarConnectionFile   = "{connection_file}"
	VarContainerName    = "{container_name}"
	VarContainerNetwork = "{network}"
	VarStorageVolume    = "{storage}"
	VarConfigFile       = "{config_file}"
	HostMountDir        = "{host_mount_dir}"
	TargetMountDir      = "{target_mount_dir}"
)

var (
	dockerStorageBase = "/storage"
	// dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} -v {storage}:/storage -v {config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	// dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}/{connection_file}:{target_mount_dir}/{connection_file} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	dockerInvokerCmd  = "docker run -d --name {container_name} -v {host_mount_dir}:{target_mount_dir} -v {storage}:/storage -v {host_mount_dir}/{config_file}:/home/jovyan/.ipython/profile_default/ipython_config.json --net {network} {image}"
	dockerShutdownCmd = "docker stop {container_name}"

	ErrUnexpectedReplicaExpression = fmt.Errorf("unexpected replica expression, expected url")
)

type DockerInvoker struct {
	LocalInvoker
	dockerOpts    *jupyter.ConnectionInfo
	tempBase      string
	invokerCmd    string
	containerName string
	smrPort       int
	closing       int32
}

func NewDockerInvoker(opts *jupyter.ConnectionInfo) *DockerInvoker {
	smrPort, _ := strconv.Atoi(utils.GetEnv(KernelSMRPort, strconv.Itoa(KernelSMRPortDefault)))
	if smrPort == 0 {
		smrPort = KernelSMRPortDefault
	}
	invoker := &DockerInvoker{
		dockerOpts: opts,
		tempBase:   utils.GetEnv(DockerTempBase, DockerTempBaseDefault),
		smrPort:    smrPort,
	}
	invoker.LocalInvoker.statusChanged = invoker.defaultStatusChangedHandler
	invoker.invokerCmd = strings.ReplaceAll(dockerInvokerCmd, VarContainerImage, utils.GetEnv(DockerImageName, DockerImageNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarContainerNetwork, utils.GetEnv(DockerNetworkName, DockerNetworkNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarStorageVolume, utils.GetEnv(DockerStorageVolume, DockerStorageVolumeDefault))
	return invoker
}

func (ivk *DockerInvoker) InvokeWithContext(ctx context.Context, spec *gateway.KernelReplicaSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec
	ivk.status = jupyter.KernelStatusInitializing

	log.Printf("[DockerInvoker] Invoking with context now.\n")

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

	log.Printf("[DockerInvoker] Kernel Name: \"%s\". Port: %d.\n", kernelName, port)

	// Looking for available port
	connectionInfo, err := ivk.prepareConnectionFile(spec.Kernel)
	if err != nil {
		log.Printf("Error while preparing connection file: %v.\n", err)
		return nil, ivk.reportLaunchError(err)
	}

	hostMountDir := os.Getenv("HOST_MOUNT_DIR")
	targetMountDir := os.Getenv("TARGET_MOUNT_DIR")

	log.Printf("hostMountDir = \"%v\"\n", hostMountDir)
	log.Printf("targetMountDir = \"%v\"\n", hostMountDir)

	log.Printf("Prepared connection info: %v\n", connectionInfo)

	// Write connection file and replace placeholders within in command line
	connectionFile, err := ivk.writeConnectionFile(ivk.tempBase, kernelName, connectionInfo)
	if err != nil {
		log.Printf("Error while writing connection file: %v.\n", err)
		log.Printf("Connection info: %v\n", connectionInfo)
		return nil, ivk.reportLaunchError(err)
	}

	log.Printf("Wrote connection file: %v\n", connectionFile)

	configInfo, _ := ivk.prepareConfigFile(spec)
	configInfo.SMRPort = port
	configFile, err := ivk.writeConfigFile(ivk.tempBase, kernelName, configInfo)
	if err != nil {
		log.Printf("Error while writing config file: %v.\n", err)
		log.Printf("Config info: %v\n", configInfo)
		return nil, ivk.reportLaunchError(err)
	}

	log.Printf("Wrote config file: %v\n", configFile)

	log.Printf("filepath.Base(connectionFile)=\"%v\"\n", filepath.Base(connectionFile))
	log.Printf("filepath.Base(configFile)=\"%v\"\n", filepath.Base(configFile))

	log.Printf("{hostMountDir}/{connectionFile}\"%v\"\n", hostMountDir+"/"+filepath.Base(connectionFile))
	log.Printf("{hostMountDir}/{configFile}=\"%v\"\n", hostMountDir+"/"+filepath.Base(configFile))

	log.Printf("{targetMountDir}/{connectionFile}\"%v\"\n", targetMountDir+"/"+filepath.Base(connectionFile))
	log.Printf("{targetMountDir}/{configFile}=\"%v\"\n", targetMountDir+"/"+filepath.Base(configFile))

	ivk.containerName = kernelName
	connectionInfo.IP = ivk.containerName // Overwrite IP with container name
	cmd := strings.ReplaceAll(ivk.invokerCmd, VarContainerName, ivk.containerName)
	cmd = strings.ReplaceAll(cmd, TargetMountDir, targetMountDir)
	cmd = strings.ReplaceAll(cmd, HostMountDir, hostMountDir)
	cmd = strings.ReplaceAll(cmd, VarConnectionFile, filepath.Base(connectionFile))
	cmd = strings.ReplaceAll(cmd, VarConfigFile, filepath.Base(configFile))
	for i, arg := range spec.Kernel.Argv {
		spec.Kernel.Argv[i] = strings.ReplaceAll(arg, VarConnectionFile, connectionFile)
	}
	argv := append(strings.Split(cmd, " "), spec.Kernel.Argv...)

	// Start kernel process
	log.Printf("Launch kernel: \"%v\"\n", argv)
	if err := ivk.launchKernel(ctx, kernelName, argv); err != nil {
		return nil, ivk.reportLaunchError(err)
	}

	ivk.setStatus(jupyter.KernelStatusRunning)
	return connectionInfo, nil
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
		return jupyter.ErrKernelNotLaunched
	}

	if !atomic.CompareAndSwapInt32(&ivk.closing, 0, 1) {
		// Wait for the closing to be done.
		<-ivk.closed
		return nil
	}

	argv := strings.Split(strings.ReplaceAll(dockerShutdownCmd, VarContainerName, ivk.containerName), " ")
	fmt.Printf("Stopping kernel %s......", argv)
	cmd := exec.CommandContext(context.Background(), argv[0], argv[1:]...)
	if err := cmd.Run(); err != nil {
		fmt.Printf("[Error]: %v\n", err)
		return err
	}

	ivk.closedAt = time.Now()
	close(ivk.closed)
	ivk.closed = nil
	fmt.Printf("[Done]\n")
	ivk.setStatus(jupyter.KernelStatusExited)
	// Status will not change anymore, reset the handler.
	ivk.statusChanged = ivk.defaultStatusChangedHandler
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

func (ivk *DockerInvoker) GetReplicaAddress(kernel *gateway.KernelSpec, replicaId int32) string {
	return fmt.Sprintf("%s:%d", ivk.generateKernelName(kernel, replicaId), ivk.smrPort)
}

func (ivk *DockerInvoker) generateKernelName(kernel *gateway.KernelSpec, replica_id int32) string {
	return fmt.Sprintf(DockerKernelName, fmt.Sprintf("%s-%d", kernel.Id, replica_id))
}

// extractKernelName extracts kernel name and port from the replica spec
func (ivk *DockerInvoker) extractKernelNamePort(spec *gateway.KernelReplicaSpec) (name string, port int, err error) {
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

func (ivk *DockerInvoker) prepareConnectionFile(spec *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	// Write connection file
	connectionInfo := &jupyter.ConnectionInfo{
		IP:              "0.0.0.0",
		Transport:       "tcp",
		ControlPort:     ivk.dockerOpts.ControlPort,
		ShellPort:       ivk.dockerOpts.ShellPort,
		StdinPort:       ivk.dockerOpts.StdinPort,
		HBPort:          ivk.dockerOpts.HBPort,
		IOPubPort:       ivk.dockerOpts.IOPubPort,
		SignatureScheme: spec.SignatureScheme,
		Key:             spec.Key,
	}

	return connectionInfo, nil
}

func (ivk *DockerInvoker) prepareConfigFile(spec *gateway.KernelReplicaSpec) (*jupyter.ConfigFile, error) {
	file := &jupyter.ConfigFile{
		DistributedKernelConfig: jupyter.DistributedKernelConfig{
			StorageBase: dockerStorageBase,
			SMRNodeID:   int(spec.ReplicaId),
			SMRNodes:    spec.Replicas,
			SMRJoin:     spec.Join,
		},
	}
	if spec.PersistentId != nil {
		file.DistributedKernelConfig.PersistentID = *spec.PersistentId
	}
	return file, nil
}

func (ivk *DockerInvoker) launchKernel(ctx context.Context, name string, argv []string) error {
	fmt.Printf("Starting %s......", name)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if err := cmd.Run(); err != nil {
		fmt.Printf("[Error]: %v\n", err)
		return err
	}

	fmt.Printf("[Done]\n")
	return nil
}
