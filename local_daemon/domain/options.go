package domain

import (
	"fmt"

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
	Consuladdr         string `name:"consul" description:"Consul agent address."`
	NodeName           string `name:"node_name" description:"Node name used only for debugging in local mode."`
	DebugMode          bool   `name:"debug_mode" description:"Enable the debug HTTP server."`
	DebugPort          int    `name:"debug_port" description:"The port for the debug HTTP server."`
}

func (o LocalDaemonOptions) String() string {
	return fmt.Sprintf("Port: %d, KernelRegistryPort: %d, ProvisionerAddr: %s, JaegerAddr: %s, ConsulAddr: %s, %s, %s", o.Port, o.KernelRegistryPort, o.ProvisionerAddr, o.JaegerAddr, o.Consuladdr, o.ConnectionInfo.String(), o.SchedulerDaemonOptions.String())
}

type SchedulerDaemonConfig func(SchedulerDaemon)

type SchedulerDaemonOptions struct {
	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer         bool   `name:"direct" description:"True if the scheduler serves jupyter notebook directly."`
	SMRPort              int    `name:"smr_port" description:"Port used by the SMR protocol."`
	NumGPUs              int64  `name:"max-actual-gpu-per-node" json:"max-actual-gpu-per-node" yaml:"max-actual-gpu-per-node" description:"The total number of GPUs that should be available on each node."`
	SchedulingPolicy     string `name:"scheduling-policy" description:"The scheduling policy to use. Options are 'default, 'static', and 'dynamic'."`
	DeploymentMode       string `name:"deployment_mode" description:"Options are 'docker' and 'kubernetes'."`
	HdfsNameNodeEndpoint string `name:"hdfs-namenode-endpoint" description:"Hostname of the HDFS NameNode. The SyncLog's HDFS client will connect to this."`
	DockerStorageBase    string `name:"docker-storage-base" description:"Base directory in which the persistent store data is stored when running in docker mode."`
	UsingWSL             bool   `name:"using-wsl" description:"Flag indicating whether we're running within WSL2 (Windows Subsystem for Linux). Requires additional networking configuring for the Docker containers."`
	PrometheusInterval   int    `name:"prometheus_interval" description:"Frequency in seconds of how often to publish metrics to Prometheus. So, setting this to 5 means we publish metrics roughly every 5 seconds."`
	PrometheusPort       int    `name:"prometheus_port" description:"The port on which this local daemon will serve Prometheus metrics. Default/suggested: 8089."`
}

// IsKubernetesMode returns true if the deployment mode is specified as "kubernetes".
func (o SchedulerDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

// IsLocalMode returns true if the deployment mode is specified as "local".
func (o SchedulerDaemonOptions) IsLocalMode() bool {
	return o.DeploymentMode == string(types.LocalMode)
}

// IsDockerSwarmMode returns true if the deployment mode is specified as either "docker-swarm" or "docker-compose".
func (o SchedulerDaemonOptions) IsDockerMode() bool {
	return o.IsDockerComposeMode() || o.IsDockerSwarmMode()
}

// IsDockerSwarmMode returns true if the deployment mode is specified as "docker-swarm".
func (o SchedulerDaemonOptions) IsDockerSwarmMode() bool {
	return o.DeploymentMode == string(types.DockerSwarmMode)
}

// IsDockerSwarmMode returns true if the deployment mode is specified as "docker-compose".
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
