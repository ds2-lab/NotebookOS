package domain

import (
	"encoding/json"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"strings"

	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/types"
)

const (
	DefaultS3Bucket      = "bcarver-distributed-notebook-storage"
	DefaultAwsRegion     = "us-east-1"
	DefaultRedisPassword = ""
	DefaultRedisPort     = 6379
	DefaultRedisDatabase = 0
)

func getDefaultRemoteStorageEndpoint(remoteStorage string) string {
	switch remoteStorage {
	case "s3":
		return DefaultS3Bucket
	case "redis":
		return "redis"
	case "local":
		return ""
	default:
		panic(fmt.Sprintf("Cannot determine likely default remote storage endpoint for remote storage \"%s\"",
			remoteStorage))
	}
}

type LocalDaemonOptions struct {
	config.LoggerOptions          `yaml:",inline" json:"logger_options"`
	VirtualGpuPluginServerOptions `yaml:",inline" json:"virtual_gpu_plugin_server_options"`
	jupyter.ConnectionInfo        `yaml:",inline" json:"connection_info"`
	SchedulerDaemonOptions        `yaml:",inline" json:"scheduler_daemon_options"`
	ProvisionerAddr               string `name:"provisioner" description:"Provisioner address." yaml:"provisioner" json:"provisioner"`
	JaegerAddr                    string `name:"jaeger" description:"Jaeger agent address." yaml:"jaeger" json:"jaeger"`
	ConsulAddr                    string `name:"consul" description:"Consul agent address." yaml:"consul" json:"consul"`
	NodeName                      string `name:"node_name" description:"Node name used only for debugging in local mode." yaml:"node_name" json:"node_name"`
	AwsRegion                     string `name:"aws_region" json:"aws_region" yaml:"aws_region"`             // AwsRegion is the AWS region in which to create/look for the S3 bucket (if we're using AWS S3 for remote storage).
	RedisPassword                 string `name:"redis_password" json:"redis_password" yaml:"redis_password"` // RedisPassword is the password to access Redis (only relevant if using Redis for remote storage).
	Port                          int    `name:"port" json:"port" yaml:"port" usage:"Port that the gRPC service listens on."`
	KernelRegistryPort            int    `name:"kernel-registry-port" usage:"Port on which the kernel Registry Server listens."`
	RedisPort                     int    `name:"redis_port" json:"redis_port" yaml:"redis_port"`             // RedisPort is the port of the Redis server (only relevant if using Redis for remote storage).
	RedisDatabase                 int    `name:"redis_database" json:"redis_database" yaml:"redis_database"` // RedisDatabase is the database number to use (only relevant if using Redis for remote storage).
}

func (o *LocalDaemonOptions) Validate() error {
	if o.RemoteStorage != "" && o.RemoteStorageEndpoint == "" {
		defaultValue := getDefaultRemoteStorageEndpoint(o.RemoteStorage)
		fmt.Printf("[WARNING] \"remote-storage-endpoint\" configuration is not set while using remote_storage=\"%s\". Using default value: \"%s\".\n",
			o.RemoteStorage, defaultValue)
		o.RemoteStorageEndpoint = defaultValue
	}

	if o.AwsRegion == "" {
		fmt.Printf("[WARNING] AwsRegion configuration is not set. Using default value: \"%s\".\n",
			DefaultAwsRegion)
		o.AwsRegion = DefaultAwsRegion
	}

	if o.RedisDatabase < 0 {
		fmt.Printf("[WARNING] RedisDatabase configuration is not set. Using default value: '%d'.\n",
			DefaultRedisDatabase)
		o.RedisDatabase = DefaultRedisDatabase
	}

	if o.RedisPort <= 0 {
		fmt.Printf("[WARNING] AwsReRedisPort configuration is not set. Using default value: '%d'.\n",
			DefaultRedisPort)
		o.RedisPort = DefaultRedisPort
	}

	return nil
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
	DockerStorageBase           string `name:"docker-storage-base" json:"docker-storage-base" yaml:"docker-storage-base" description:"Base directory in which the persistent store data is stored when running in docker mode."`
	scheduling.SchedulerOptions `yaml:",inline" json:"cluster_scheduler_options"`

	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer    bool `name:"direct" yaml:"direct" json:"direct" description:"True if the scheduler serves jupyter notebook directly."`
	RunKernelsInGdb bool `name:"run_kernels_in_gdb" yaml:"run_kernels_in_gdb" json:"run_kernels_in_gdb" description:"If true, then the kernels will be run in GDB."`
}

// IsKubernetesMode returns true if the deployment mode is specified as "kubernetes".
func (o SchedulerDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

// IsLocalMode returns true if the deployment mode is specified as "local".
func (o SchedulerDaemonOptions) IsLocalMode() bool {
	return o.LocalMode
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
	DevicePluginPath string `name:"device-plugin-path" yaml:"device-plugin-path" json:"device-plugin-path" description:"The path to the socket used by the kubelet to receive our DevicePlugin registration."`
	NumVirtualGPUs   int    `name:"num-virtual-gpus-per-node" yaml:"num-virtual-gpus-per-node" json:"num-virtual-gpus-per-node" description:"The number of virtual GPUs to be made available on each Kubernetes node."`
}
