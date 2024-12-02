package statistics

type ClusterStatistics struct {
	///////////
	// Hosts //
	///////////

	Hosts            int `json:"hosts" csv:"hosts"`
	NumDisabledHosts int `json:"num_disabled_hosts" csv:"num_disabled_hosts"`
	NumEmptyHosts    int `csv:"NumEmptyHosts" json:"NumEmptyHosts"` // The number of Hosts with 0 sessions/containers scheduled on them.

	// The amount of time hosts have spent not idling throughout the entire simulation
	CumulativeHostActiveTime float64 `csv:"CumulativeHostActiveTimeSec" json:"CumulativeHostActiveTimeSec"`
	// The amount of time hosts have spent idling throughout the entire simulation.
	CumulativeHostIdleTime float64 `csv:"CumulativeHostIdleTimeSec" json:"CumulativeHostIdleTimeSec"`
	// The aggregate, cumulative lifetime of ALL hosts provisioned at some point during the simulation.
	AggregateHostLifetime float64 `csv:"AggregateHostLifetimeSec" json:"AggregateHostLifetimeSec"`
	// The aggregate, cumulative lifetime of the hosts that are currently running.
	AggregateHostLifetimeOfRunningHosts float64 `csv:"AggregateHostLifetimeOfRunningHostsSec" json:"AggregateHostLifetimeOfRunningHostsSec"`

	// The total (cumulative) number of hosts provisioned during the simulation run.
	CumulativeNumHostsProvisioned int `csv:"CumulativeNumHostsProvisioned" json:"CumulativeNumHostsProvisioned"`
	// The total amount of time spent provisioning hosts.
	CumulativeTimeProvisioningHosts float64 `csv:"CumulativeTimeProvisioningHostsSec" json:"CumulativeTimeProvisioningHostsSec"`

	///////////////
	// Messaging //
	///////////////

	NumJupyterMessagesReceivedByClusterGateway int64 `json:"num_jupyter_messages_received_by_cluster_gateway" csv:"num_jupyter_messages_received_by_cluster_gateway"`
	NumJupyterRepliesSentByClusterGateway      int64 `json:"num_jupyter_replies_sent_by_cluster_gateway" csv:"num_jupyter_replies_sent_by_cluster_gateway"`

	// CumulativeRequestProcessingTimeClusterGateway is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeClusterGateway int64 `json:"cumulative_request_processing_time_cluster_gateway" csv:"cumulative_request_processing_time_cluster_gateway"`
	// CumulativeRequestProcessingTimeLocalDaemon is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeLocalDaemon int64 `json:"cumulative_request_processing_time_local_daemon" csv:"cumulative_request_processing_time_local_daemon"`

	// CumulativeRequestProcessingTimeKernel is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeKernel int64

	// CumulativeRequestProcessingTimeClusterGateway is calculated using the RequestTrace proto message.
	CumulativeResponseProcessingTimeClusterGateway int64 `json:"cumulative_response_processing_time_cluster_gateway" csv:"cumulative_response_processing_time_cluster_gateway"`
	// CumulativeRequestProcessingTimeLocalDaemon is calculated using the RequestTrace proto message.
	CumulativeResponseProcessingTimeLocalDaemon int64 `json:"cumulative_response_processing_time_local_daemon" csv:"cumulative_response_processing_time_local_daemon"`
	// CumulativeRequestProcessingTimeKernel is calculated using the RequestTrace proto message.

	////////////////////////////////////////
	// Execution/Kernel-Related Overheads //
	////////////////////////////////////////

	// CumulativeCudaInitMicroseconds is the cumulative, aggregate time spent initializing CUDA runtimes by all kernels.
	CumulativeCudaInitMicroseconds int64 `json:"cumulative_cuda_init_microseconds" csv:"cumulative_cuda_init_microseconds"`
	// NumCudaRuntimesInitialized is the number of times a CUDA runtime was initialized.
	NumCudaRuntimesInitialized int64 `json:"num_cuda_runtimes_initialized" csv:"num_cuda_runtimes_initialized"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent downloading
	// runtime/library/module dependencies by all kernels.
	CumulativeTimeDownloadingDependenciesMicroseconds int64 `json:"cumulative_time_downloading_dependencies_microseconds" csv:"cumulative_time_downloading_dependencies_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel downloaded dependencies.
	NumTimesDownloadedDependencies int64 `json:"num_times_downloaded_dependencies" csv:"num_times_downloaded_dependencies"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent downloading the model
	// and training data by all kernels.
	CumulativeTimeDownloadModelAndTrainingDataMicroseconds int64 `json:"cumulative_time_download_model_and_training_data_microseconds" csv:"cumulative_time_download_model_and_training_data_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel downloaded the model and training data.
	NumTimesDownloadModelAndTrainingDataMicroseconds int64 `json:"num_times_download_model_and_training_data_microseconds" csv:"num_times_download_model_and_training_data_microseconds"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent uploading the model
	// and training data by all kernels.
	CumulativeTimeUploadModelAndTrainingDataMicroseconds int64 `json:"cumulative_time_upload_model_and_training_data_microseconds" csv:"cumulative_time_upload_model_and_training_data_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel uploaded the model and training data.
	NumTimesUploadModelAndTrainingDataMicroseconds int64 `json:"num_times_upload_model_and_training_data_microseconds" csv:"num_times_upload_model_and_training_data_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory) by all kernels.
	CumulativeTimeCopyDataHostToDeviceMicroseconds int64 `json:"cumulative_time_copy_data_host_to_device_microseconds" csv:"cumulative_time_copy_data_host_to_device_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory).
	NumTimesCopyDataHostToDeviceMicroseconds int64 `json:"num_times_copy_data_host_to_device_microseconds" csv:"num_times_copy_data_host_to_device_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from the GPU
	// (i.e., device memory) to main memory (i.e., host memory).
	CumulativeTimeCopyDataDeviceToHostMicroseconds int64 `json:"cumulative_time_copy_data_device_to_host_microseconds" csv:"cumulative_time_copy_data_device_to_host_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from the GPU
	// (i.e., device memory) to main memory (i.e., device memory).
	NumTimesCopyDataDeviceToHostMicroseconds int64 `json:"num_times_copy_data_device_to_host_microseconds" csv:"num_times_copy_data_device_to_host_microseconds"`

	// CumulativeExecutionTimeMicroseconds is the cumulative, aggregate time spent executing user code, excluding any
	// related overheads, by all kernels.
	CumulativeExecutionTimeMicroseconds int64 `json:"cumulative_execution_time_microseconds" csv:"cumulative_execution_time_microseconds"`

	// CumulativeReplayTimeMicroseconds is the cumulative, aggregate time spent replaying cells, excluding any
	// related overheads, by all kernels.
	CumulativeReplayTimeMicroseconds int64 `json:"cumulative_replay_time_microseconds" csv:"cumulative_replay_time_microseconds"`
	// TotalNumReplays is the total number of times that one or more cells had to be replayed by a kernel.
	TotalNumReplays int64 `json:"total_num_replays" csv:"total_num_replays"`
	// TotalNumCellsReplayed is the total number of cells that were replayed by all kernels.
	TotalNumCellsReplayed int64 `json:"total_num_cells_replayed" csv:"total_num_cells_replayed"`

	///////////////
	// Resources //
	///////////////

	SpecCPUs        float64 `csv:"SpecCPUs" json:"SpecCPUs"`
	SpecGPUs        float64 `csv:"SpecGPUs" json:"SpecGPUs"`
	SpecMemory      float64 `csv:"SpecMemory" json:"SpecMemory"`
	SpecVRAM        float64 `csv:"SpecVRAM" json:"SpecVRAM"`
	IdleCPUs        float64 `csv:"IdleCPUs" json:"IdleCPUs"`
	IdleGPUs        float64 `csv:"IdleGPUs" json:"IdleGPUs"`
	IdleMemory      float64 `csv:"IdleMemory" json:"IdleMemory"`
	IdleVRAM        float64 `csv:"IdleVRAM" json:"IdleVRAM"`
	PendingCPUs     float64 `csv:"PendingCPUs" json:"PendingCPUs"`
	PendingGPUs     float64 `csv:"PendingGPUs" json:"PendingGPUs"`
	PendingMemory   float64 `csv:"PendingMemory" json:"PendingMemory"`
	PendingVRAM     float64 `csv:"PendingVRAM" json:"PendingVRAM"`
	CommittedCPUs   float64 `csv:"CommittedCPUs" json:"CommittedCPUs"`
	CommittedGPUs   float64 `csv:"CommittedGPUs" json:"CommittedGPUs"`
	CommittedMemory float64 `csv:"CommittedMemory" json:"CommittedMemory"`
	CommittedVRAM   float64 `csv:"CommittedVRAM" json:"CommittedVRAM"`

	DemandCPUs   float64 `csv:"DemandCPUs" json:"DemandCPUs"`
	DemandMemMb  float64 `csv:"DemandMemMb" json:"DemandMemMb"`
	DemandGPUs   float64 `csv:"DemandGPUs" json:"DemandGPUs"`
	DemandVRAMGb float64 `csv:"DemandVRAMGb" json:"DemandVRAMGb"`
	//GPUUtil    float64 `csv:"GPUUtil" json:"GPUUtil"`
	//CPUUtil    float64 `csv:"CPUUtil" json:"CPUUtil"`
	//MemUtil    float64 `csv:"MemUtil" json:"MemUtil"`
	//VRAMUtil   float64 `csv:"VRAMUtil" json:"VRAMUtil"`
	//CPUOverload int `csv:"CPUOverload" json:"CPUOverload"`

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////

	SubscriptionRatio float64 `csv:"SubscriptionRatio" json:"SubscriptionRatio"`

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	Rescheduled       int32 `csv:"Rescheduled" json:"Rescheduled"`
	Resched2Ready     int32 `csv:"Resched2Ready" json:"Resched2Ready"`
	Migrated          int32 `csv:"Migrated" json:"Migrated"`
	Preempted         int32 `csv:"Preempted" json:"Preempted"`
	OnDemandContainer int   `csv:"OnDemandContainers" json:"OnDemandContainers"`
	IdleHostsPerClass int32 `csv:"IdleHosts" json:"IdleHosts"`

	//////////////
	// Sessions //
	//////////////

	CompletedTrainings int32 `csv:"CompletedTrainings" json:"CompletedTrainings"`
	// The Len of Cluster::Sessions (which is of type *SessionManager).
	// This includes all Sessions that have not been permanently stopped.
	NumNonTerminatedSessions int `csv:"NumNonTerminatedSessions" json:"NumNonTerminatedSessions"`
	// The number of Sessions that are presently idle, not training.
	NumIdleSessions int `csv:"NumIdleSessions" json:"NumIdleSessions"`
	// The number of Sessions that are presently actively-training.
	NumTrainingSessions int `csv:"NumTrainingSessions" json:"NumTrainingSessions"`
	// The number of Sessions in the STOPPED state.
	NumStoppedSessions int `csv:"NumStoppedSessions" json:"NumStoppedSessions"`
	// The number of Sessions that are actively running (but not necessarily training), so includes idle sessions.
	// Does not include evicted, init, or stopped sessions.
	NumRunningSessions int `csv:"NumRunningSessions" json:"NumRunningSessions"`

	// The amount of time that Sessions have spent idling throughout the entire simulation.
	CumulativeSessionIdleTime float64 `csv:"CumulativeSessionIdleTimeSec" json:"CumulativeSessionIdleTimeSec"`
	// The amount of time that Sessions have spent training throughout the entire simulation. This does NOT include replaying events.
	CumulativeSessionTrainingTime float64 `csv:"CumulativeSessionTrainingTimeSec" json:"CumulativeSessionTrainingTimeSec"`
	// The aggregate lifetime of all sessions created during the simulation (before being suspended).
	AggregateSessionLifetimeSec  float64   `csv:"AggregateSessionLifetimeSec" json:"AggregateSessionLifetimeSec"`
	AggregateSessionLifetimesSec []float64 `csv:"-" json:"AggregateSessionLifetimesSec"`
	// Delay between when client submits "execute_request" and when kernel begins executing.
	JupyterTrainingStartLatencyMillis   float64   `json:"jupyter_training_start_latency_millis" csv:"jupyter_training_start_latency_millis"`
	JupyterTrainingStartLatenciesMillis []float64 `json:"jupyter_training_start_latencies_millis" csv:"-"`
}

func NewClusterStatistics() *ClusterStatistics {
	return &ClusterStatistics{
		JupyterTrainingStartLatenciesMillis: make([]float64, 0),
		AggregateSessionLifetimesSec:        make([]float64, 0),
	}
}
