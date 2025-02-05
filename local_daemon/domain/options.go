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
	DefaultS3Bucket      = "distributed-notebook-storage"
	DefaultAwsRegion     = "us-east-1"
	DefaultRedisPassword = ""
	DefaultRedisPort     = 6379
	DefaultRedisDatabase = 0
)

type LocalDaemonOptions struct {
	config.LoggerOptions
	ProvisionerAddr string `name:"provisioner" description:"Provisioner address."`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address."`
	ConsulAddr      string `name:"consul" description:"Consul agent address."`
	NodeName        string `name:"node_name" description:"Node name used only for debugging in local mode."`
	SchedulerDaemonOptions
	VirtualGpuPluginServerOptions
	jupyter.ConnectionInfo
	Port               int    `name:"port" usage:"Port that the gRPC service listens on."`
	KernelRegistryPort int    `name:"kernel-registry-port" usage:"Port on which the Kernel Registry Server listens."`
	S3Bucket           string `name:"s3_bucket" json:"s3_bucket" yaml:"s3_bucket"`                // S3Bucket is the AWS S3 bucket name if we're using AWS S3 for our remote storage.
	AwsRegion          string `name:"aws_region" json:"aws_region" yaml:"aws_region"`             // AwsRegion is the AWS region in which to create/look for the S3 bucket (if we're using AWS S3 for remote storage).
	RedisPassword      string `name:"redis_password" json:"redis_password" yaml:"redis_password"` // RedisPassword is the password to access Redis (only relevant if using Redis for remote storage).
	RedisPort          int    `name:"redis_port" json:"redis_port" yaml:"redis_port"`             // RedisPort is the port of the Redis server (only relevant if using Redis for remote storage).
	RedisDatabase      int    `name:"redis_database" json:"redis_database" yaml:"redis_database"` // RedisDatabase is the database number to use (only relevant if using Redis for remote storage).
}

func (o *LocalDaemonOptions) Validate() error {
	if o.S3Bucket == "" {
		o.S3Bucket = DefaultS3Bucket
	}

	if o.AwsRegion == "" {
		o.AwsRegion = DefaultAwsRegion
	}

	if o.RedisDatabase < 0 {
		o.RedisDatabase = DefaultRedisDatabase
	}

	if o.RedisPort <= 0 {
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
	scheduling.SchedulerOptions `yaml:",inline" json:"cluster_scheduler_options"`

	// If the scheduler serves jupyter notebook directly, set this to true.
	DirectServer      bool   `name:"direct" description:"True if the scheduler serves jupyter notebook directly."`
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
