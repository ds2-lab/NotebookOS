package domain

import (
	"encoding/json"
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"strings"
)

const (
	// DockerProjectName is used to monitor only for Docker container-created events corresponding to this particular project.
	// TODO: Don't hardcode this. If the "name" field in "deploy/docker/docker-compose.yml" is changed,
	// then the value of this const must be updated so that it matches the "name" field.
	DockerProjectName = "distributed_cluster"

	//DockerContainerFullId  MetadataKey = "docker-container-full-id"
	//DockerContainerShortId MetadataKey = "docker-container-short-id"

	// DefaultNumResendAttempts is the default number of attempts we'll resend a message before giving up.
	DefaultNumResendAttempts = 3

	// DefaultPrometheusPort is the default port on which the Local Daemon will serve Prometheus metrics.
	DefaultPrometheusPort int = 8089
	// DefaultPrometheusIntervalSeconds is the default interval, in seconds, on which the Local Daemon will push new Prometheus metrics.
	DefaultPrometheusIntervalSeconds = 15
)

type ClusterDaemonOptions struct {
	scheduling.SchedulerOptions `yaml:",inline" json:"cluster_scheduler_options"`

	LocalDaemonServiceName            string `name:"local-daemon-service-name"        json:"local-daemon-service-name"         yaml:"local-daemon-service-name"           description:"Name of the Kubernetes service that manages the local-only networking of local daemons."`
	LocalDaemonServicePort            int    `name:"local-daemon-service-port"        json:"local-daemon-service-port"         yaml:"local-daemon-service-port"           description:"Port exposed by the Kubernetes service that manages the local-only  networking of local daemons."`
	GlobalDaemonServiceName           string `name:"global-daemon-service-name"       json:"global-daemon-service-name"        yaml:"global-daemon-service-name"          description:"Name of the Kubernetes service that manages the global networking of local daemons."`
	GlobalDaemonServicePort           int    `name:"global-daemon-service-port"       json:"global-daemon-service-port"        yaml:"global-daemon-service-port"          description:"Port exposed by the Kubernetes service that manages the global networking of local daemons."`
	KubeNamespace                     string `name:"kubernetes-namespace"             json:"kubernetes-namespace"              yaml:"kubernetes-namespace"                description:"Kubernetes namespace that all of these components reside in."`
	UseStatefulSet                    bool   `name:"use-stateful-set"                 json:"use-stateful-set"                  yaml:"use-stateful-set"                    description:"If true, use StatefulSet for the distributed kernel Pods; if false, use CloneSet."`
	NotebookImageName                 string `name:"notebook-image-name"              json:"notebook-image-name"               yaml:"notebook-image-name"                 description:"Name of the docker image to use for the jupyter notebook/kernel image"` // Name of the docker image to use for the jupyter notebook/kernel image
	NotebookImageTag                  string `name:"notebook-image-tag"               json:"notebook-image-tag"                yaml:"notebook-image-tag"                  description:"Name of the docker image to use for the jupyter notebook/kernel image"` // Tag to use for the jupyter notebook/kernel image
	DistributedClusterServicePort     int    `name:"distributed-cluster-service-port" json:"distributed-cluster-service-port"  yaml:"distributed-cluster-service-port"    description:"Port to use for the 'distributed cluster' service, which is used by the Dashboard."`
	RemoteDockerEventAggregatorPort   int    `name:"remote-docker-event-aggregator-port" json:"remote-docker-event-aggregator-port" yaml:"remote-docker-event-aggregator-port" description:"The port to be used by the Docker Remote Event Aggregator when running in Docker Swarm mode."`
	InitialClusterSize                int    `name:"initial-cluster-size" json:"initial-cluster-size" yaml:"initial-cluster-size" description:"The initial size of the cluster. If more than this many Local Daemons connect during the 'initial connection period', then the extra nodes will be disabled until a scale-out event occurs."`
	InitialClusterConnectionPeriodSec int    `name:"initial-connection-period" json:"initial-connection-period" yaml:"initial-connection-period" description:"The initial connection period is the time immediately after the Cluster Gateway begins running during which it expects all Local Daemons to connect. If greater than N local daemons connect during this period, where N is the initial cluster size, then those extra daemons will be disabled."`
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (o *ClusterDaemonOptions) PrettyString(indentSize int) string {
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

// ValidateClusterDaemonOptions ensures that the values of certain configuration parameters are consistent with respect
// to one another, and/or with respect to certain requirements/constraints on their values
// (unrelated of other configuration parameters).
func (o *ClusterDaemonOptions) ValidateClusterDaemonOptions() {
	if o.NumResendAttempts <= 0 {
		log.Printf("[WARNING] Invalid number of message resend attempts specified: %d. Defaulting to %d.\n",
			o.NumResendAttempts, DefaultNumResendAttempts)
		o.NumResendAttempts = DefaultNumResendAttempts
	}

	if o.PrometheusInterval <= 0 {
		log.Printf("[WARNING] Using default Prometheus interval: %v.\n", DefaultPrometheusIntervalSeconds)
		o.PrometheusInterval = DefaultPrometheusIntervalSeconds
	}

	if o.PrometheusPort <= 0 {
		log.Printf("[WARNING] Using default Prometheus port: %d.\n", DefaultPrometheusPort)
		o.PrometheusPort = DefaultPrometheusPort
	}

	if len(o.RemoteStorageEndpoint) == 0 {
		panic("remote storage endpoint is empty.")
	}
}

// IsLocalMode returns true if the deployment mode is specified as "local".
func (o *ClusterDaemonOptions) IsLocalMode() bool {
	return o.DeploymentMode == string(types.LocalMode)
}

// IsDockerMode returns true if the deployment mode is specified as either "docker-swarm" or "docker-compose".
func (o *ClusterDaemonOptions) IsDockerMode() bool {
	return o.IsDockerComposeMode() || o.IsDockerSwarmMode()
}

// IsDockerSwarmMode returns true if the deployment mode is specified as "docker-swarm".
func (o *ClusterDaemonOptions) IsDockerSwarmMode() bool {
	return o.DeploymentMode == string(types.DockerSwarmMode)
}

// IsDockerComposeMode returns true if the deployment mode is specified as "docker-compose".
func (o *ClusterDaemonOptions) IsDockerComposeMode() bool {
	return o.DeploymentMode == string(types.DockerComposeMode)
}

// IsKubernetesMode returns true if the deployment mode is specified as "kubernetes".
func (o *ClusterDaemonOptions) IsKubernetesMode() bool {
	return o.DeploymentMode == string(types.KubernetesMode)
}

func (o *ClusterDaemonOptions) String() string {
	out, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}

	return string(out)
}

type ClusterGatewayOptions struct {
	config.LoggerOptions   `yaml:",inline" json:"logger_options"`
	jupyter.ConnectionInfo `yaml:",inline" json:"connection_info"`
	ClusterDaemonOptions   `yaml:",inline" json:"cluster_daemon_options"`

	Port            int    `name:"port" usage:"Port the gRPC service listen on." json:"port"`
	ProvisionerPort int    `name:"provisioner-port" usage:"Port for provisioning host schedulers." json:"provisioner_port"`
	JaegerAddr      string `name:"jaeger" description:"Jaeger agent address." json:"jaeger_addr"`
	ConsulAddr      string `name:"consul" description:"Consul agent address." json:"consul_addr"`
}

func (opts *ClusterGatewayOptions) String() string {
	m, err := json.Marshal(opts)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (opts *ClusterGatewayOptions) PrettyString(indentSize int) string {
	indentBuilder := strings.Builder{}
	for i := 0; i < indentSize; i++ {
		indentBuilder.WriteString(" ")
	}

	m, err := json.MarshalIndent(opts, "", indentBuilder.String())
	if err != nil {
		panic(err)
	}

	return string(m)
}
