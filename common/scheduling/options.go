package scheduling

type ClusterSchedulerOptions struct {
	GpusPerHost                   int     `name:"gpus-per-host" description:"The number of actual GPUs that are available for use on each node/host."`
	VirtualGpusPerHost            int     `name:"num-virtual-gpus-per-node" description:"The number of virtual GPUs per host."`
	SubscribedRatioUpdateInterval float64 `name:"subscribed-ratio-update-interval" description:"The interval to update the subscribed ratio."`
	ScalingFactor                 float64 `name:"scaling-factor" description:"Defines how many hosts the cluster will provision based on busy resources"`
	ScalingInterval               int     `name:"scaling-interval" description:"Interval to call validateCapacity, 0 to disable routing scaling."`
	ScalingLimit                  float64 `name:"scaling-limit" description:"Defines how many hosts the cluster will provision at maximum based on busy resources"`
	MaximumHostsToReleaseAtOnce   int     `name:"scaling-in-limit" description:"Sort of the inverse of the ScalingLimit parameter (maybe?)"`
	ScalingOutEnabled             bool    `name:"scaling-out-enabled" description:"If enabled, the scaling manager will attempt to over-provision hosts slightly so as to leave room for fluctuation. If disabled, then the Cluster will exclusively scale-out in response to real-time demand, rather than attempt to have some hosts available in the case that demand surges."`
	ScalingBufferSize             int     `name:"scaling-buffer-size" description:"Buffer size is how many extra hosts we provision so that we can quickly scale if needed."`
	MinimumNumNodes               int     `name:"min-kubernetes-nodes" description:"The minimum number of kubernetes nodes we must have available at any time."`
	GpuPollIntervalSeconds        int     `name:"gpu_poll_interval" description:"How frequently the Cluster Gateway should poll the Local Daemons for updated GPU information."`
	NumReplicas                   int     `name:"num-replicas" description:"Number of kernel replicas."`
	MaxSubscribedRatio            float64 `name:"max-subscribed-ratio" description:"Maximum subscribed ratio."`
	ExecutionTimeSamplingWindow   int64   `name:"execution-time-sampling-window" description:"Window size for moving average of training time. Specify a negative value to compute the average as the average of ALL execution times."`
	MigrationTimeSamplingWindow   int64   `name:"migration-time-sampling-window" description:"Window size for moving average of migration time. Specify a negative value to compute the average as the average of ALL migration times."`
	SchedulerHttpPort             int     `name:"scheduler-http-port" description:"Port that the Cluster Gateway's kubernetes scheduler API server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender."`
}
