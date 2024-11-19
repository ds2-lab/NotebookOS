package scheduling

import (
	"encoding/json"
	"github.com/scusemua/distributed-notebook/common/configuration"
	"log"
	"math"
	"strings"
)

const (
	DefaultScalingIntervalSeconds = 30
	DefaultMaxSubscribedRatio     = 7.0
)

type SchedulerOptions struct {
	configuration.CommonOptions   `yaml:",inline" name:"common_options" json:"common_options"`
	VirtualGpusPerHost            int     `name:"num-virtual-gpus-per-node"         json:"num-virtual-gpus-per-node"        yaml:"num-virtual-gpus-per-node"                        description:"The number of virtual GPUs per host."`
	SubscribedRatioUpdateInterval float64 `name:"subscribed-ratio-update-interval"  json:"subscribed-ratio-update-interval" yaml:"subscribed-ratio-update-interval"                        description:"The interval to update the subscribed ratio."`
	ScalingFactor                 float64 `name:"scaling-factor"                    json:"scaling-factor"                   yaml:"scaling-factor"                        description:"Defines how many hosts the Cluster will provision based on busy Resources"`
	ScalingInterval               int     `name:"scaling-interval"                  json:"scaling-interval"                 yaml:"scaling-interval"                        description:"Interval to call validateCapacity, 0 to disable routing scaling."`
	ScalingLimit                  float64 `name:"scaling-limit"                     json:"scaling-limit"                    yaml:"scaling-limit"                        description:"Defines how many hosts the Cluster will provision at maximum based on busy Resources"`
	MaximumHostsToReleaseAtOnce   int     `name:"scaling-in-limit"                  json:"scaling-in-limit"                 yaml:"scaling-in-limit"                        description:"Sort of the inverse of the ScalingLimit parameter (maybe?)"`
	PredictiveAutoscalingEnabled  bool    `name:"predictive_autoscaling"            json:"predictive_autoscaling"           yaml:"predictive_autoscaling"                        description:"If enabled, the scaling manager will attempt to over-provision hosts slightly to leave room for fluctuation, and will also scale-in if we are over-provisioned relative to the current request load. If this is disabled, the Cluster can still provision new hosts if demand surges, but it will not scale-down, nor will it automatically scale to leave room for fluctuation."`
	ScalingBufferSize             int     `name:"scaling-buffer-size"               json:"scaling-buffer-size"              yaml:"scaling-buffer-size"                        description:"Buffer size is how many extra hosts we provision so that we can quickly scale if needed."`
	MinimumNumNodes               int     `name:"min_cluster_nodes"                 json:"min_cluster_nodes"                yaml:"min_cluster_nodes"                        description:"The minimum number of Cluster nodes we must have available at any time."`
	MaximumNumNodes               int     `name:"max_cluster_nodes"                 json:"max_cluster_nodes"                yaml:"max_cluster_nodes"                        description:"The maximum number of Cluster nodes we must have available at any time. If this is < 0, then it is unbounded."`
	GpuPollIntervalSeconds        int     `name:"gpu_poll_interval"                 json:"gpu_poll_interval"                yaml:"gpu_poll_interval"                        description:"How frequently the Cluster Gateway should poll the DefaultSchedulingPolicy Daemons for updated GPU information."`
	NumReplicas                   int     `name:"num-replicas"                      json:"num-replicas"                     yaml:"num-replicas"                        description:"Number of kernel replicas."`
	MaxSubscribedRatio            float64 `name:"max-subscribed-ratio"              json:"max-subscribed-ratio"             yaml:"max-subscribed-ratio"                        description:"Maximum subscribed ratio."`
	ExecutionTimeSamplingWindow   int64   `name:"execution-time-sampling-window"    json:"execution-time-sampling-window"   yaml:"execution-time-sampling-window"                        description:"Window size for moving average of training time. Specify a negative value to compute the average as the average of ALL execution times."`
	MigrationTimeSamplingWindow   int64   `name:"migration-time-sampling-window"    json:"migration-time-sampling-window"   yaml:"migration-time-sampling-window"                        description:"Window size for moving average of migration time. Specify a negative value to compute the average as the average of ALL migration times."`
	SchedulerHttpPort             int     `name:"scheduler-http-port"               json:"scheduler-http-port"              yaml:"scheduler-http-port"                        description:"Port that the Cluster Gateway's kubernetes scheduler API server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender."`
}

// GetNumReplicas returns the number of replicas per kernel.
func (opts *SchedulerOptions) GetNumReplicas() int {
	return opts.NumReplicas
}

// GetGpusPerHost returns the number of allocatable GPUs on each entity.Host.
func (opts *SchedulerOptions) GetGpusPerHost() int {
	return opts.GpusPerHost
}

// GetScalingInterval returns the interval at which the Cluster will attempt to auto-scale.
func (opts *SchedulerOptions) GetScalingInterval() int {
	return opts.ScalingInterval
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
	// Validate the minimum capacity.
	// It must be at least equal to the number of replicas per kernel.
	if opts.MinimumNumNodes < opts.NumReplicas {
		log.Printf("[WARNING] minimum number of nodes specified (%d). Value is less than the configured number of replicas (%d). Setting minimum nodes to %d.\n",
			opts.MinimumNumNodes, opts.NumReplicas, opts.NumReplicas)
		opts.MinimumNumNodes = opts.NumReplicas
	}

	// Validate the maximum capacity.
	// It must be at least equal to the number of replicas per kernel.
	// It also cannot be less than the minimum capacity (it must be at least equal).
	if opts.MaximumNumNodes < 0 {
		opts.MaximumNumNodes = math.MaxInt // Essentially unbounded.
	}
	if opts.MaximumNumNodes < opts.NumReplicas {
		log.Printf("[WARNING] Invalid maximum number of nodes specified (%d). Value is less than the configured number of replicas (%d). Setting maximum nodes to %d.\n",
			opts.MaximumNumNodes, opts.NumReplicas, opts.NumReplicas)
		opts.MaximumNumNodes = opts.NumReplicas
	}
	if opts.MaximumNumNodes < opts.MinimumNumNodes {
		log.Printf("[WARNING] Invalid maximum number of nodes specified (%d). Value is less than the configured minimum number of nodes (%d). Setting maximum nodes to %d.\n",
			opts.MaximumNumNodes, opts.MinimumNumNodes, opts.MinimumNumNodes)
		opts.MaximumNumNodes = opts.MinimumNumNodes
	}

	// Validate the maximum subscribed/subscription ratio, which must be greater than zero (i.e., strictly positive).
	if opts.MaxSubscribedRatio <= 0 {
		log.Printf("[WARNING] Invalid maximum \"subscribed\" ratio specified: %.2f. Defaulting to %.2f.\n",
			opts.MaxSubscribedRatio, DefaultMaxSubscribedRatio)
		opts.MaxSubscribedRatio = DefaultMaxSubscribedRatio
	}

	// Validate the scaling interval, which must be strictly positive
	if opts.ScalingInterval <= 0 {
		log.Printf("[WARNING] Invalid scaling interval specified: %d. Defaulting to every %d seconds.",
			opts.ScalingInterval, DefaultScalingIntervalSeconds)
		opts.ScalingInterval = DefaultScalingIntervalSeconds
	}
}
