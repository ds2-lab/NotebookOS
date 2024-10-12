package domain

import (
	"encoding/json"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/configuration"
	"strings"

	"github.com/mason-leap-lab/go-utils/config"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

type LocalDaemonOptions struct {
	config.LoggerOptions
	jupyter.ConnectionInfo
	SchedulerDaemonOptions
	VirtualGpuPluginServerOptions

	Port               int    `name:"port" usage:"Port that the gRPC service listens on."`
	KernelRegistryPort int    `name:"kernel-registry-port" usage:"Port on which the Kernel Registry Server listens."`
	ProvisionerAddr    string `name:"provisioner" description:"Provisioner address."`
	JaegerAddr         string `name:"jaeger" description:"Jaeger agent address."`
	ConsulAddr         string `name:"consul" description:"Consul agent address."`
	NodeName           string `name:"node_name" description:"Node name used only for debugging in local mode."`
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (o *LocalDaemonOptions) PrettyString(indentSize int) string {
	indentBuilder := strings.Builder{}
	for i := 0; i < indentSize; i++ {
		indentBuilder.WriteString(" ")
	}

	m, err := json.MarshalIndent(o, "", indentBuilder.String())
	if err != nil {
		panic(err)
	}

	return string(m)
}

func (o *LocalDaemonOptions) String() string {
	m, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}

	return string(m)
}

type SchedulerDaemonConfig func(SchedulerDaemon)

type SchedulerDaemonOptions struct {
	configuration.CommonOptions `yaml:",inline"`

	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer      bool   `name:"direct" description:"True if the scheduler serves jupyter notebook directly."`
	NumGPUs           int64  `name:"max-actual-gpu-per-node" json:"max-actual-gpu-per-node" yaml:"max-actual-gpu-per-node" description:"The total number of GPUs that should be available on each node."`
	DockerStorageBase string `name:"docker-storage-base" description:"Base directory in which the persistent store data is stored when running in docker mode."`
	RunKernelsInGdb   bool   `name:"run_kernels_in_gdb" description:"If true, then the kernels will be run in GDB."`
}

// IsKubernetesMode returns true if the deployment mode is specified as "kubernetes".
func (o SchedulerDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

// IsLocalMode returns true if the deployment mode is specified as "local".
func (o SchedulerDaemonOptions) IsLocalMode() bool {
	return o.DeploymentMode == string(types.LocalMode)
}

// IsDockerMode returns true if the deployment mode is specified as either "docker-swarm" or "docker-compose".
func (o SchedulerDaemonOptions) IsDockerMode() bool {
	return o.IsDockerComposeMode() || o.IsDockerSwarmMode()
}

// IsDockerSwarmMode returns true if the deployment mode is specified as "docker-swarm".
func (o SchedulerDaemonOptions) IsDockerSwarmMode() bool {
	return o.DeploymentMode == string(types.DockerSwarmMode)
}

// IsDockerComposeMode returns true if the deployment mode is specified as "docker-compose".
func (o SchedulerDaemonOptions) IsDockerComposeMode() bool {
	return o.DeploymentMode == string(types.DockerComposeMode)
}

func (o SchedulerDaemonOptions) String() string {
	return fmt.Sprintf("DirectServer: %v", o.DirectServer)
}

type VirtualGpuPluginServerOptions struct {
	DevicePluginPath string `name:"device-plugin-path" description:"The path to the socket used by the kubelet to receive our DevicePlugin registration."`
	NumVirtualGPUs   int    `name:"num-virtual-gpus-per-node" description:"The number of virtual GPUs to be made available on each Kubernetes node."`
	// DevicePluginRpcPort string `name:"device-plugin-port" description:"The port that the gRPC server for the DevicePlugin interface listens on."`
}
