package invoker

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	DockerTempBase        = "KERNEL_TEMP_BASE"
	DockerTempBaseDefault = ""

	DockerImageName        = "KERNEL_IMAGE"
	DockerImageNameDefault = "zhangjyr/jupyter:latest"

	DockerNetworkName        = "KERNEL_NETWORK"
	DockerNetworkNameDefault = "local_daemon_default"

	DockerKernelName    = "kernel-%s"
	VarContainerImage   = "{image}"
	VarConnectionFile   = "{connection_file}"
	VarContainerName    = "{container_name}"
	VarContainerNetwork = "{network}"
)

var (
	dockerInvokerCmd  = "docker run -d --rm --name {container_name} -v {connection_file}:{connection_file} --net {network} {image}"
	dockerShutdownCmd = "docker stop {container_name}"
)

type DockerInvoker struct {
	LocalInvoker
	dockerOpts    *jupyter.ConnectionInfo
	tempBase      string
	invokerCmd    string
	containerName string
	status        jupyter.KernelStatus
}

func NewDockerInvoker(opts *jupyter.ConnectionInfo) *DockerInvoker {
	invoker := &DockerInvoker{
		dockerOpts: opts,
		tempBase:   GetEnv(DockerTempBase, DockerTempBaseDefault),
	}
	invoker.invokerCmd = strings.ReplaceAll(dockerInvokerCmd, VarContainerImage, GetEnv(DockerImageName, DockerImageNameDefault))
	invoker.invokerCmd = strings.ReplaceAll(invoker.invokerCmd, VarContainerNetwork, GetEnv(DockerNetworkName, DockerNetworkNameDefault))
	return invoker
}

func (ivk *DockerInvoker) InvokeWithContext(ctx context.Context, spec *gateway.KernelSpec) (*jupyter.ConnectionInfo, error) {
	ivk.closed = make(chan struct{})
	ivk.spec = spec
	ivk.status = jupyter.KernelStatusInitializing

	// Looking for available port
	connectionInfo, err := ivk.prepareConnectionFile(spec)
	if err != nil {
		return nil, ivk.reportLaucnError(err)
	}

	// Write connection file and replace placeholders within in command line
	path, err := ivk.writeConnectionFile(ivk.tempBase, spec.Id, connectionInfo)
	if err != nil {
		return nil, ivk.reportLaucnError(err)
	}

	ivk.containerName = fmt.Sprintf(DockerKernelName, spec.Id)
	connectionInfo.IP = ivk.containerName // Overwrite IP with container name
	cmd := strings.ReplaceAll(ivk.invokerCmd, VarContainerName, ivk.containerName)
	cmd = strings.ReplaceAll(cmd, VarConnectionFile, path)
	for i, arg := range spec.Argv {
		spec.Argv[i] = strings.ReplaceAll(arg, VarConnectionFile, path)
	}
	argv := append(strings.Split(cmd, " "), spec.Argv...)

	// Start kernel process
	log.Printf("Launch kernel \"%v\"\n", argv)
	if err := ivk.launchKernel(ctx, spec.Id, argv); err != nil {
		return nil, ivk.reportLaucnError(err)
	}
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

	argv := strings.Split(strings.ReplaceAll(dockerShutdownCmd, VarContainerName, ivk.containerName), " ")
	fmt.Printf("Stopping kernel %s......", argv)
	cmd := exec.CommandContext(context.Background(), argv[0], argv[1:]...)
	if err := cmd.Run(); err != nil {
		fmt.Printf("[Error]: %v\n", err)
		return err
	}

	ivk.closedAt = time.Now()
	ivk.status = jupyter.KernelStatusExited
	close(ivk.closed)
	ivk.closed = nil
	fmt.Printf("[Done]\n")
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

func (ivk *DockerInvoker) launchKernel(ctx context.Context, id string, argv []string) error {
	fmt.Printf("Starting kernel %s......", id)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if err := cmd.Run(); err != nil {
		fmt.Printf("[Error]: %v\n", err)
		return err
	}

	ivk.status = jupyter.KernelStatusRunning
	fmt.Printf("[Done]\n")
	return nil
}

func (ivk *DockerInvoker) reportLaucnError(err error) error {
	ivk.status = jupyter.KernelStatusAbnormal
	return err
}
