package configuration

import (
	"github.com/goccy/go-json"
	"strings"
)

// CommonOptions includes all configuration parameters that are common to both the Cluster Gateway
// and the Local Daemon components.
type CommonOptions struct {
	DeploymentMode                     string `name:"deployment_mode"                  json:"deployment_mode"                   yaml:"deployment_mode"                     description:"SchedulerOptions are 'docker-compose', 'docker-swarm', and 'kubernetes'."`
	DockerAppName                      string `name:"docker_app_name" json:"docker_app_name" yaml:"docker_app_name" description:"The name of the Docker application (or Docker Stack) that we're deployed within (only relevant when in Docker mode)."`
	DockerNetworkName                  string `name:"docker_network_name"              json:"docker_network_name"               yaml:"docker_network_name"                 description:"The name of the Docker network that the container is running within. Only used in Docker mode."`
	SchedulingPolicy                   string `name:"scheduling-policy"                json:"scheduling-policy"                 yaml:"scheduling-policy"                   description:"The scheduling policy to use. SchedulerOptions are 'default, 'static', and 'dynamic'."`
	IdleSessionReclamationPolicy       string `name:"idle-session-reclamation-policy" json:"idle-session-reclamation-policy" csv:"idle-session-reclamation-policy" yaml:"idle-session-reclamation-policy"`
	RemoteStorageEndpoint              string `name:"remote-storage-endpoint"           json:"remote-storage-endpoint"            yaml:"remote-storage-endpoint"              description:"Hostname of the remote storage. The SyncLog's remote storage client will connect to this."`
	RemoteStorage                      string `name:"remote-storage" json:"remote-storage" yaml:"remote-storage" description:"The type of remote storage we're using, either 'hdfs' or 'redis'"`
	GpusPerHost                        int    `name:"gpus-per-host"                    json:"gpus-per-host"                     yaml:"gpus-per-host" description:"The number of actual GPUs that are available for use on each node/host."`
	PrometheusInterval                 int    `name:"prometheus_interval"              json:"prometheus_interval"               yaml:"prometheus_interval"                 description:"Frequency in seconds of how often to publish metrics to Prometheus. So, setting this to 5 means we publish metrics roughly every 5 seconds."`
	PrometheusPort                     int    `name:"prometheus_port"                  json:"prometheus_port"                   yaml:"prometheus_port"                     description:"The port on which this local daemon will serve Prometheus metrics. Default/suggested: 8089."`
	NumResendAttempts                  int    `name:"num_resend_attempts"              json:"num_resend_attempts"               yaml:"num_resend_attempts"                 description:"The number of times to attempt to resend a message before giving up."`
	SMRPort                            int    `name:"smr-port"                         json:"smr-port"                          yaml:"smr-port"                            description:"JupyterGrpcPort used by the state machine replication (SMR) protocol."`
	DebugPort                          int    `name:"debug_port"                       json:"debug_port"                        yaml:"debug_port"                          description:"The port for the debug HTTP server."`
	ElectionTimeoutSeconds             int    `name:"election_timeout_seconds" json:"election_timeout_seconds" yaml:"election_timeout_seconds" description:"How long kernel leader elections wait to receive all proposals before electing a leader"`
	LocalMode                          bool   `name:"local_mode" json:"local_mode" yaml:"local_mode" description:"Local mode is set to true during unit tests and changes how certain information is resolved, such as how Local Schedulers determine their name (normally from an environment variable)"`
	RealGpusAvailable                  bool   `name:"use_real_gpus" json:"use_real_gpus" yaml:"use_real_gpus" description:"Indicates whether there are real GPUs available that should be bound to kernel containers or not."`
	MessageAcknowledgementsEnabled     bool   `name:"acks_enabled"                     json:"acks_enabled"                      yaml:"acks_enabled"                        description:"MessageAcknowledgementsEnabled indicates whether we send/expect to receive message acknowledgements for the ZMQ messages that we're forwarding back and forth between the various cluster components."`
	DebugMode                          bool   `name:"debug_mode"                       json:"debug_mode"                        yaml:"debug_mode"                          description:"Enable the debug HTTP server."`
	SimulateCheckpointingLatency       bool   `name:"simulate_checkpointing_latency"   json:"simulate_checkpointing_latency"    yaml:"simulate_checkpointing_latency"      description:"If enabled, then kernels will simulate the latency of performing checkpointing after executing code (write) and after a migration (read)."`
	DisablePrometheusMetricsPublishing bool   `name:"disable_prometheus_metrics_publishing" json:"disable_prometheus_metrics_publishing"    yaml:"disable_prometheus_metrics_publishing" description:"If passed as true, then the goroutine that publishes Prometheus metrics on an interval will not be created."`
	SimulateTrainingUsingSleep         bool   `name:"simulate_training_using_sleep" json:"simulate_training_using_sleep" yaml:"simulate_training_using_sleep" description:"Flag which informs system whether to use real GPUs for training or not."`
	BindDebugPyPort                    bool   `name:"bind_debugpy_port" json:"bind_debugpy_port" yaml:"bind_debugpy_port" description:"If true, bind a port to the kernel for debugpy."`
	SaveStoppedKernelContainers        bool   `name:"save_stopped_kernel_containers" json:"save_stopped_kernel_containers" yaml:"save_stopped_kernel_containers" description:"If true, rename stopped kernel containers to save/persist them."`
	RetrieveDatasetsFromS3             bool   `name:"retrieve_datasets_from_s3" json:"retrieve_datasets_from_s3" json:"retrieve_datasets_from_s3"`
	DatasetsS3Bucket                   string `name:"datasets_s3_bucket" json:"datasets_s3_bucket" yaml:"datasets_s3_bucket"`

	// PrettyPrintOptions, when true, instructs the Cluster Gateway's driver script to pretty-print
	// the ClusterGatewayOptions struct when the program first begins running.
	PrettyPrintOptions bool `name:"pretty_print_options" json:"pretty_print_options" yaml:"pretty_print_options"`
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (opts *CommonOptions) PrettyString(indentSize int) string {
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

func (opts *CommonOptions) Clone() *CommonOptions {
	clone := *opts
	return &clone
}

func (opts *CommonOptions) String() string {
	m, err := json.Marshal(opts)
	if err != nil {
		panic(err)
	}

	return string(m)
}
