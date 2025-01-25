package statistics

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"time"
)

const (
	KernelReplicaRegistered ClusterEventName = "kernel_replica_registered"
	KernelCreationStarted   ClusterEventName = "kernel_creation_started"
	KernelCreationComplete  ClusterEventName = "kernel_creation_complete"
	KernelMigrationStarted  ClusterEventName = "kernel_migration_started"
	KernelMigrationComplete ClusterEventName = "kernel_migration_complete"
	KernelTrainingStarted   ClusterEventName = "kernel_training_started"
	KernelTrainingEnded     ClusterEventName = "kernel_training_ended"
	KernelStopped           ClusterEventName = "kernel_stopped"
	ScaleOutStarted         ClusterEventName = "scale_out_started"
	ScaleOutEnded           ClusterEventName = "scale_out_ended"
	ScaleInStarted          ClusterEventName = "scale_in_started"
	ScaleInEnded            ClusterEventName = "scale_in_ended"
)

type ClusterEventName string

func (n ClusterEventName) String() string {
	return string(n)
}

type ClusterEvent struct {
	Name                ClusterEventName       `json:"name" csv:"name"`
	KernelId            string                 `json:"kernel_id" csv:"kernel_id"`
	ReplicaId           int32                  `json:"replica_id" csv:"replica_id"`
	Timestamp           time.Time              `json:"timestamp" csv:"timestamp"`
	Duration            time.Duration          `json:"duration" csv:"duration"`
	DurationMillis      int64                  `json:"duration_millis" csv:"duration_millis"`
	TimestampUnixMillis int64                  `json:"timestamp_unix_millis" csv:"timestamp_unix_millis"`
	Metadata            map[string]interface{} `json:"metadata" csv:"-"`
}

/*
	d.clusterStatisticsMutex.Lock()
	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &statistics.ClusterEvent{
		Name:                statistics.KernelCreationStarted,
		KernelId:            in.Id,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()
*/

type ClusterStatistics struct {
	///////////
	// Hosts //
	///////////

	Hosts            int `json:"hosts" csv:"hosts"`
	NumDisabledHosts int `json:"num_disabled_hosts" csv:"num_disabled_hosts"`
	NumEmptyHosts    int `csv:"NumEmptyHosts" json:"NumEmptyHosts"` // The number of Hosts with 0 sessions/containers scheduled on them.

	ClusterEvents []*ClusterEvent `json:"cluster_events" csv:"-"`

	ExecuteRequestTraces []*proto.RequestTrace `json:"execute_request_traces" csv:"-"`

	// The amount of time hosts have spent not idling throughout the entire simulation
	CumulativeHostActiveTime float64 `csv:"CumulativeHostActiveTimeSec" json:"CumulativeHostActiveTimeSec"`
	// The amount of time hosts have spent idling throughout the entire simulation.
	CumulativeHostIdleTime float64 `csv:"CumulativeHostIdleTimeSec" json:"CumulativeHostIdleTimeSec"`
	// The aggregate, cumulative lifetime of ALL hosts provisioned at some point during the simulation.
	AggregateHostLifetime float64 `csv:"AggregateHostLifetimeSec" json:"AggregateHostLifetimeSec"`
	// The aggregate, cumulative lifetime of the hosts that are currently running.
	AggregateHostLifetimeOfRunningHosts float64 `csv:"AggregateHostLifetimeOfRunningHostsSec" json:"AggregateHostLifetimeOfRunningHostsSec"`

	// The total (cumulative) number of hosts provisioned during.
	CumulativeNumHostsProvisioned int `csv:"CumulativeNumHostsProvisioned" json:"CumulativeNumHostsProvisioned"`
	// The total (cumulative) number of hosts released during.
	CumulativeNumHostsReleased int `json:"cumulative_num_hosts_released" csv:"cumulative_num_hosts_released"`
	// The total amount of time spent provisioning hosts.
	CumulativeTimeProvisioningHosts float64 `csv:"CumulativeTimeProvisioningHostsSec" json:"CumulativeTimeProvisioningHostsSec"`

	NumActiveScaleOutEvents     int `json:"num_active_scale_out_events" csv:"num_active_scale_out_events"`
	NumSuccessfulScaleOutEvents int `json:"num_successful_scale_out_events" csv:"num_successful_scale_out_events"`
	NumFailedScaleOutEvents     int `json:"num_failed_scale_out_events" csv:"num_failed_scale_out_events"`

	NumActiveScaleInEvents     int `json:"num_active_scale_in_events" csv:"num_active_scale_in_events"`
	NumSuccessfulScaleInEvents int `json:"num_successful_scale_in_events" csv:"num_successful_scale_in_events"`
	NumFailedScaleInEvents     int `json:"num_failed_scale_in_events" csv:"num_failed_scale_in_events"`

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
	CumulativeCudaInitMicroseconds float64 `json:"cumulative_cuda_init_microseconds" csv:"cumulative_cuda_init_microseconds"`
	// NumCudaRuntimesInitialized is the number of times a CUDA runtime was initialized.
	NumCudaRuntimesInitialized float64 `json:"num_cuda_runtimes_initialized" csv:"num_cuda_runtimes_initialized"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent downloading
	// runtime/library/module dependencies by all kernels.
	// CumulativeTimeDownloadingDependenciesMicroseconds float64 `json:"cumulative_time_downloading_dependencies_microseconds" csv:"cumulative_time_downloading_dependencies_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel downloaded dependencies.
	// NumTimesDownloadedDependencies float64 `json:"num_times_downloaded_dependencies" csv:"num_times_downloaded_dependencies"`

	// CumulativeTimeDownloadTrainingDataMicroseconds is the cumulative, aggregate time spent downloading the
	// training data by all kernels.
	CumulativeTimeDownloadTrainingDataMicroseconds float64 `json:"cumulative_time_download_training_data_microseconds" csv:"cumulative_time_download_training_data_microseconds"`
	// NumTimesDownloadTrainingDataMicroseconds is the total number of times that a kernel downloaded the training data.
	NumTimesDownloadTrainingDataMicroseconds float64 `json:"num_times_download_training_data_microseconds" csv:"num_times_download_training_data_microseconds"`

	CumulativeTokenizeDatasetMicroseconds float64 `json:"cumulative_tokenize_dataset_microseconds" csv:"cumulative_tokenize_dataset_microseconds"`
	NumTimesTokenizeDatasetMicroseconds   float64 `json:"num_times_tokenize_dataset_microseconds" csv:"num_times_tokenize_dataset_microseconds"`

	// CumulativeTimeDownloadModelMicroseconds is the cumulative, aggregate time spent downloading the model by all kernels.
	CumulativeTimeDownloadModelMicroseconds float64 `json:"cumulative_time_download_model_microseconds" csv:"cumulative_time_download_model_microseconds"`
	// NumTimesDownloadModelMicroseconds is the total number of times that a kernel downloaded the model.
	NumTimesDownloadModelMicroseconds float64 `json:"num_times_download_model_microseconds" csv:"num_times_download_model_microseconds"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent uploading the model
	// and training data by all kernels.
	CumulativeTimeUploadModelAndTrainingDataMicroseconds float64 `json:"cumulative_time_upload_model_and_training_data_microseconds" csv:"cumulative_time_upload_model_and_training_data_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel uploaded the model and training data.
	NumTimesUploadModelAndTrainingDataMicroseconds float64 `json:"num_times_upload_model_and_training_data_microseconds" csv:"num_times_upload_model_and_training_data_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory) by all kernels.
	CumulativeTimeCopyDataHostToDeviceMicroseconds float64 `json:"cumulative_time_copy_data_host_to_device_microseconds" csv:"cumulative_time_copy_data_host_to_device_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory).
	NumTimesCopyDataHostToDeviceMicroseconds float64 `json:"num_times_copy_data_host_to_device_microseconds" csv:"num_times_copy_data_host_to_device_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from the GPU
	// (i.e., device memory) to main memory (i.e., host memory).
	CumulativeTimeCopyDataDeviceToHostMicroseconds float64 `json:"cumulative_time_copy_data_device_to_host_microseconds" csv:"cumulative_time_copy_data_device_to_host_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from the GPU
	// (i.e., device memory) to main memory (i.e., device memory).
	NumTimesCopyDataDeviceToHostMicroseconds float64 `json:"num_times_copy_data_device_to_host_microseconds" csv:"num_times_copy_data_device_to_host_microseconds"`

	// CumulativeExecutionTimeMicroseconds is the cumulative, aggregate time spent executing user code, excluding any
	// related overheads, by all kernels.
	CumulativeExecutionTimeMicroseconds float64 `json:"cumulative_execution_time_microseconds" csv:"cumulative_execution_time_microseconds"`

	// CumulativeLeaderElectionTimeMicroseconds is the cumulative, aggregate time spent handling leader elections.
	CumulativeLeaderElectionTimeMicroseconds float64 `json:"cumulative_leader_election_time_microseconds" csv:"cumulative_leader_election_time_microseconds"`

	// CumulativeKernelPreprocessRequestMillis is the time between when a kernel receives a request and when it begins handling the leader election.
	CumulativeKernelPreprocessRequestMillis float64 `json:"cumulative_kernel_preprocess_request_millis" csv:"cumulative_kernel_preprocess_request_millis"`
	// CumulativeKernelCreateElectionMillis is the time the kernels spent creating an election.
	CumulativeKernelCreateElectionMillis float64 `json:"cumulative_kernel_create_election_millis" csv:"cumulative_kernel_create_election_millis"`
	// CumulativeKernelProposalVotePhaseMillis is the cumulative duration of the proposal + voting phase of elections.
	CumulativeKernelProposalVotePhaseMillis float64 `json:"cumulative_kernel_proposal_vote_phase_millis" csv:"cumulative_kernel_proposal_vote_phase_millis"`
	// CumulativeKernelPostprocessMillis is the cumulative time after the kernels finish executing code before they send their response to their Local Scheduler.
	CumulativeKernelPostprocessMillis float64 `json:"cumulative_kernel_postprocess_millis" csv:"cumulative_kernel_postprocess_millis"`

	// CumulativeReplayTimeMicroseconds is the cumulative, aggregate time spent replaying cells, excluding any
	// related overheads, by all kernels.
	CumulativeReplayTimeMicroseconds float64 `json:"cumulative_replay_time_microseconds" csv:"cumulative_replay_time_microseconds"`
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

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////

	SubscriptionRatio float64 `csv:"SubscriptionRatio" json:"SubscriptionRatio"`

	// NumTimesKernelReplicaAvailableImmediately is the number of times that a kernel replica was available
	// immediately when an "execute_request" message was received by the cluster (as opposed to having to
	// migrate some replicas around in order to serve the "execute_request").
	NumTimesKernelReplicaAvailableImmediately float64 `csv:"NumTimesKernelReplicaAvailableImmediately" json:"NumTimesKernelReplicaAvailableImmediately"`

	// NumTimesKernelReplicaNotAvailableImmediately is the number of times that a kernel replica was NOT available
	// immediately when an "execute_request" message was received by the cluster, and we had to migrate some replicas
	// around in order to serve the "execute_request" (as opposed to there being a replica available and able to
	// serve the "execute_request" immediately, with no migrations required).
	NumTimesKernelReplicaNotAvailableImmediately float64 `csv:"NumTimesKernelReplicaNotAvailableImmediately" json:"NumTimesKernelReplicaNotAvailableImmediately"`

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
	// NumSeenSessions is the total number of sessions seen/ever created.
	NumSeenSessions int `csv:"NumSeenSessions" json:"NumSeenSessions"`

	NumSuccessfulMigrations int `json:"num_successful_migrations" csv:"num_successful_migrations"`
	NumFailedMigrations     int `json:"num_failed_migrations" csv:"num_failed_migrations"`

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
		ClusterEvents:                       make([]*ClusterEvent, 0),
		ExecuteRequestTraces:                make([]*proto.RequestTrace, 0),
	}
}
