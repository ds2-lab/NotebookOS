package scheduling

import (
	"encoding/json"
	"github.com/scusemua/distributed-notebook/common/configuration"
	"log"
	"math"
	"strings"
)

var (
	DefaultStaticSchedulerOptions = &SchedulerOptions{
		CommonOptions: configuration.CommonOptions{
			GpusPerHost:                        8,
			DeploymentMode:                     "docker-swarm",
			DockerAppName:                      "distributed_notebook",
			DockerNetworkName:                  "distributed_cluster_default",
			PrometheusInterval:                 15,
			PrometheusPort:                     8089,
			NumResendAttempts:                  1,
			MessageAcknowledgementsEnabled:     false,
			SchedulingPolicy:                   "static",
			IdleSessionReclamationPolicy:       "none",
			RemoteStorageEndpoint:              "redis:6379",
			RemoteStorage:                      "redis",
			SMRPort:                            8080,
			DebugMode:                          true,
			DebugPort:                          9996,
			SimulateCheckpointingLatency:       true,
			DisablePrometheusMetricsPublishing: true,
			ElectionTimeoutSeconds:             3,
			SimulateTrainingUsingSleep:         false,
			BindDebugPyPort:                    false,
			SaveStoppedKernelContainers:        false,
		},
		SubscribedRatioUpdateInterval: 1,
		ScalingFactor:                 1.05,
		ScalingIntervalSec:            30,
		ScalingLimit:                  1.1,
		MaximumHostsToReleaseAtOnce:   2,
		PredictiveAutoscalingEnabled:  true,
		ScalingBufferSize:             3,
		MinimumNumNodes:               12,
		MaximumNumNodes:               48,
		GpuPollIntervalSeconds:        5,
		MaxSubscribedRatio:            7.0,
		ExecutionTimeSamplingWindow:   10,
		MigrationTimeSamplingWindow:   10,
		SchedulerHttpPort:             8078,
		MeanScaleOutPerHostSec:        15,
		StdDevScaleOutPerHostSec:      2,
		MeanScaleInPerHostSec:         10,
		StdDevScaleInPerHostSec:       1,
		AssignKernelDebugPorts:        false,
		PrewarmingEnabled:             true,
		MaxPrewarmContainers:          -1,
	}

	DefaultFcfsSchedulerOptions = &SchedulerOptions{
		CommonOptions: configuration.CommonOptions{
			GpusPerHost:                        8,
			DeploymentMode:                     "docker-swarm",
			DockerAppName:                      "distributed_notebook",
			DockerNetworkName:                  "distributed_cluster_default",
			PrometheusInterval:                 15,
			PrometheusPort:                     8089,
			NumResendAttempts:                  1,
			MessageAcknowledgementsEnabled:     false,
			SchedulingPolicy:                   "fcfs-batch",
			IdleSessionReclamationPolicy:       "none",
			RemoteStorageEndpoint:              "redis:6379",
			RemoteStorage:                      "redis",
			SMRPort:                            8080,
			DebugMode:                          true,
			DebugPort:                          9996,
			SimulateCheckpointingLatency:       true,
			DisablePrometheusMetricsPublishing: true,
			ElectionTimeoutSeconds:             3,
			SimulateTrainingUsingSleep:         false,
			BindDebugPyPort:                    false,
			SaveStoppedKernelContainers:        false,
		},
		SubscribedRatioUpdateInterval: 1,
		ScalingFactor:                 1.05,
		ScalingIntervalSec:            30,
		ScalingLimit:                  1.1,
		MaximumHostsToReleaseAtOnce:   2,
		PredictiveAutoscalingEnabled:  true,
		ScalingBufferSize:             3,
		MinimumNumNodes:               12,
		MaximumNumNodes:               48,
		GpuPollIntervalSeconds:        5,
		MaxSubscribedRatio:            7.0,
		ExecutionTimeSamplingWindow:   10,
		MigrationTimeSamplingWindow:   10,
		SchedulerHttpPort:             8078,
		MeanScaleOutPerHostSec:        15,
		StdDevScaleOutPerHostSec:      2,
		MeanScaleInPerHostSec:         10,
		StdDevScaleInPerHostSec:       1,
		AssignKernelDebugPorts:        false,
		PrewarmingEnabled:             false,
		MaxPrewarmContainers:          -1,
	}
)

// CustomIdleSessionReclamationOptions defines options that apply only when using the CustomIdleSessionReclamationPolicy
// (i.e., "custom") IdleSessionReclamationPolicy. These options will NOT be used if a different policy is being used.
type CustomIdleSessionReclamationOptions struct {
	ReplayAllCells     bool  `name:"idle_session_replay_all_cells" json:"idle_session_replay_all_cells" csv:"idle_session_replay_all_cells" yaml:"idle_session_replay_all_cells"`
	TimeoutIntervalSec int64 `name:"idle_session_timeout_interval_sec" json:"idle_session_timeout_interval_sec" csv:"idle_session_timeout_interval_sec" yaml:"idle_session_timeout_interval_sec"`
}

type SchedulerOptions struct {
	configuration.CommonOptions         `yaml:",inline" name:"common_options" json:"common_options"`
	CustomIdleSessionReclamationOptions `yaml:",inline" name:"custom_idle_session_reclamation_options" json:"custom_idle_session_reclamation_options"`
	SubscribedRatioUpdateInterval       float64 `name:"subscribed-ratio-update-interval"  json:"subscribed-ratio-update-interval" yaml:"subscribed-ratio-update-interval"                        description:"The interval to update the subscribed ratio."`
	ScalingFactor                       float64 `name:"scaling-factor"                    json:"scaling-factor"                   yaml:"scaling-factor"                        description:"Defines how many hosts the Cluster will provision based on busy TransactionResources"`
	ScalingIntervalSec                  float64 `name:"scaling-interval"                  json:"scaling-interval"                 yaml:"scaling-interval"                        description:"Interval to call validateCapacity, in seconds. Set to 0 to disable routing scaling."`
	ScalingLimit                        float64 `name:"scaling-limit"                     json:"scaling-limit"                    yaml:"scaling-limit"                        description:"Defines how many hosts the Cluster will provision at maximum based on busy TransactionResources"`
	MaximumHostsToReleaseAtOnce         int     `name:"scaling-in-limit"                  json:"scaling-in-limit"                 yaml:"scaling-in-limit"                        description:"Sort of the inverse of the ScalingLimit parameter (maybe?)"`
	ScalingBufferSize                   int     `name:"scaling-buffer-size"               json:"scaling-buffer-size"              yaml:"scaling-buffer-size"                        description:"Buffer size is how many extra hosts we provision so that we can quickly scale if needed."`
	MinimumNumNodes                     int     `name:"min_cluster_nodes"                 json:"min_cluster_nodes"                yaml:"min_cluster_nodes"                        description:"The minimum number of Cluster nodes we must have available at any time."`
	MaximumNumNodes                     int     `name:"max_cluster_nodes"                 json:"max_cluster_nodes"                yaml:"max_cluster_nodes"                        description:"The maximum number of Cluster nodes we must have available at any time. If this is < 0, then it is unbounded."`
	InitialClusterSize                  int     `name:"initial-cluster-size" json:"initial-cluster-size" yaml:"initial-cluster-size" description:"The initial size of the cluster. If more than this many Local Daemons connect during the 'initial connection period', then the extra nodes will be disabled until a scale-out event occurs."`
	GpuPollIntervalSeconds              int     `name:"gpu_poll_interval"                 json:"gpu_poll_interval"                yaml:"gpu_poll_interval"                        description:"How frequently the Cluster Gateway should poll the Local Daemons for updated GPU information."`
	MaxSubscribedRatio                  float64 `name:"max-subscribed-ratio"              json:"max-subscribed-ratio"             yaml:"max-subscribed-ratio"                        description:"Maximum subscribed ratio."`
	ExecutionTimeSamplingWindow         int64   `name:"execution-time-sampling-window"    json:"execution-time-sampling-window"   yaml:"execution-time-sampling-window"                        description:"Window size for moving average of training time. Specify a negative value to compute the average as the average of ALL execution times."`
	MigrationTimeSamplingWindow         int64   `name:"migration-time-sampling-window"    json:"migration-time-sampling-window"   yaml:"migration-time-sampling-window"                        description:"Window size for moving average of migration time. Specify a negative value to compute the average as the average of ALL migration times."`
	SchedulerHttpPort                   int     `name:"scheduler-http-port"               json:"scheduler-http-port"              yaml:"scheduler-http-port"                        description:"Port that the Cluster Gateway's kubernetes scheduler API server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender."`
	MeanScaleOutPerHostSec              float64 `name:"mean_scale_out_per_host_sec" json:"mean_scale_out_per_host_sec" yaml:"mean_scale_out_per_host_sec"`
	StdDevScaleOutPerHostSec            float64 `name:"std_dev_scale_out_per_host_sec" json:"std_dev_scale_out_per_host_sec" yaml:"std_dev_scale_out_per_host_sec"`
	MeanScaleInPerHostSec               float64 `name:"mean_scale_in_per_host_sec" json:"mean_scale_in_per_host_sec" yaml:"mean_scale_in_per_host_sec"`
	StdDevScaleInPerHostSec             float64 `name:"std_dev_scale_in_per_host_sec" json:"std_dev_scale_in_per_host_sec" yaml:"std_dev_scale_in_per_host_sec"`

	MillicpusPerHost int     `name:"millicpus_per_host" json:"millicpus_per_host" yaml:"millicpus_per_host" description:"Amount of allocatable CPU available on each host, in millicpus (1 millicpu = 1/1000 vCPU)."`
	MemoryMbPerHost  float64 `name:"memory_mb_per_host" json:"memory_mb_per_host" yaml:"memory_mb_per_host" description:"Amount of allocatable main memory (RAM) available on each host, in megabytes."`
	VramGbPerHost    float64 `name:"vram_gb_per_host" json:"vram_gb_per_host" yaml:"vram_gb_per_host" description:"Amount of allocatable VRAM (GPU/video memory) available on each host, in gigabytes."`

	PrewarmingEnabled                 bool `name:"prewarming_enabled" json:"prewarming_enabled" yaml:"prewarming_enabled"`
	MaxPrewarmContainers              int  `name:"max_prewarm_containers" json:"max_prewarm_containers" yaml:"max_prewarm_containers"`
	InitialNumContainersPerHost       int  `name:"initial_num_containers_per_host" json:"initial_num_containers_per_host" yaml:"initial_num_containers_per_host"`
	InitialClusterConnectionPeriodSec int  `name:"initial-connection-period" json:"initial-connection-period" yaml:"initial-connection-period" description:"The initial connection period is the time immediately after the Cluster Gateway begins running during which it expects all Local Daemons to connect. If greater than N local daemons connect during this period, where N is the initial cluster size, then those extra daemons will be disabled."`

	PredictiveAutoscalingEnabled bool `name:"predictive_autoscaling"            json:"predictive_autoscaling"           yaml:"predictive_autoscaling"                        description:"If enabled, the scaling manager will attempt to over-provision hosts slightly to leave room for fluctuation, and will also scale-in if we are over-provisioned relative to the current request load. If this is disabled, the Cluster can still provision new hosts if demand surges, but it will not scale-down, nor will it automatically scale to leave room for fluctuation."`

	// If true, then assign debug ports to kernel containers that will be passed to their Golang backend to start a net/pprof debug server.
	AssignKernelDebugPorts bool `name:"assign_kernel_debug_ports" json:"assign_kernel_debug_ports" yaml:"assign_kernel_debug_ports" description:"If true, then assign debug ports to kernel containers that will be passed to their Golang backend to start a net/pprof debug server."`
}

func (opts *SchedulerOptions) Validate() error {
	opts.ValidateClusterSchedulerOptions()

	return nil
}

// GetGpusPerHost returns the number of allocatable GPUs on each entity.Host.
func (opts *SchedulerOptions) GetGpusPerHost() int {
	return opts.GpusPerHost
}

// GetScalingIntervalSec returns the interval at which the Cluster will attempt to auto-scale.
func (opts *SchedulerOptions) GetScalingIntervalSec() float64 {
	if opts.ScalingIntervalSec <= 0 {
		opts.ScalingIntervalSec = DefaultScalingIntervalSeconds
	}

	return opts.ScalingIntervalSec
}

// PrettyString is the same as String, except that PrettyString calls json.MarshalIndent instead of json.Marshal.
func (opts *SchedulerOptions) PrettyString(indentSize int) string {
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

func (opts *SchedulerOptions) Clone() *SchedulerOptions {
	clone := *opts
	return &clone
}

func (opts *SchedulerOptions) String() string {
	m, err := json.Marshal(opts)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// ValidateClusterSchedulerOptions ensures that the values of certain configuration parameters are consistent with
// respect to one another, and/or with respect to certain requirements/constraints on their values (unrelated of
// other configuration parameters).
func (opts *SchedulerOptions) ValidateClusterSchedulerOptions() {
	// Validate the maximum capacity.
	// It must be at least equal to the number of replicas per kernel.
	// It also cannot be less than the minimum capacity (it must be at least equal).
	if opts.MaximumNumNodes < 0 {
		opts.MaximumNumNodes = math.MaxInt // Essentially unbounded.
	}

	if opts.MaximumNumNodes < opts.MinimumNumNodes {
		log.Printf("[WARNING] Invalid maximum number of nodes specified (%d). Value is less than the configured minimum number of nodes (%d). Setting maximum nodes to %d.\n",
			opts.MaximumNumNodes, opts.MinimumNumNodes, opts.MinimumNumNodes)
		opts.MaximumNumNodes = opts.MinimumNumNodes
	}

	// Validate the maximum subscribed/subscription ratio, which must be greater than zero (i.e., strictly positive).
	if opts.MaxSubscribedRatio <= 0 {
		opts.MaxSubscribedRatio = DefaultMaxSubscribedRatio
	}

	// Validate the scaling interval, which must be strictly positive
	if opts.ScalingIntervalSec <= 0 {
		opts.ScalingIntervalSec = DefaultScalingIntervalSeconds
	}

	if opts.MillicpusPerHost <= 0 {
		opts.MillicpusPerHost = DefaultMillicpusPerHost
	}

	if opts.MemoryMbPerHost <= 0 {
		opts.MemoryMbPerHost = DefaultMemoryMbPerHost
	}

	if opts.VramGbPerHost <= 0 {
		opts.VramGbPerHost = DefaultVramPerHostGb
	}
}
