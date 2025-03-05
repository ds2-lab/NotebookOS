package metrics

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

const (
	KernelReplicaRegistered  ClusterEventName = "kernel_replica_registered"
	KernelCreationStarted    ClusterEventName = "kernel_creation_started"
	KernelCreationComplete   ClusterEventName = "kernel_creation_complete"
	ScheduleReplicasStarted  ClusterEventName = "schedule_replicas_started"
	ScheduleReplicasComplete ClusterEventName = "schedule_replicas_complete"
	KernelMigrationStarted   ClusterEventName = "kernel_migration_started"
	KernelMigrationComplete  ClusterEventName = "kernel_migration_complete"
	KernelTrainingStarted    ClusterEventName = "kernel_training_started"
	KernelTrainingEnded      ClusterEventName = "kernel_training_complete"
	KernelStopped            ClusterEventName = "kernel_stopped"
	ScaleOutStarted          ClusterEventName = "scale_out_started"
	ScaleOutEnded            ClusterEventName = "scale_out_complete"
	ScaleInStarted           ClusterEventName = "scale_in_started"
	ScaleInEnded             ClusterEventName = "scale_in_complete"
)

type ClusterEventName string

func (n ClusterEventName) String() string {
	return string(n)
}

type ClusterEvent struct {
	Timestamp           time.Time              `json:"timestamp" csv:"timestamp"`
	Metadata            map[string]interface{} `json:"metadata" csv:"-"`
	Name                ClusterEventName       `json:"name" csv:"name"`
	KernelId            string                 `json:"kernel_id" csv:"kernel_id"`
	EventId             string                 `json:"event_id" csv:"event_id"`
	Duration            time.Duration          `json:"duration" csv:"duration"`
	DurationMillis      int64                  `json:"duration_millis" csv:"duration_millis"`
	TimestampUnixMillis int64                  `json:"timestamp_unix_millis" csv:"timestamp_unix_millis"`
	ReplicaId           int32                  `json:"replica_id" csv:"replica_id"`
}

/*
	d.clusterStatisticsMutex.Lock()
	now := time.Now()
	d.ClusterStatistics.ClusterEvents = append(d.ClusterStatistics.ClusterEvents, &statistics.ClusterEvent{
		Name:                statistics.KernelCreationStarted,
		kernelId:            in.Id,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()
*/

// SerializableClusterStatistics is a version of the ClusterStatistics struct that is serializable using
// the encoding/gob package/module/mechanism.
type SerializableClusterStatistics struct {
	ClusterEvents []*ClusterEvent `json:"cluster_events" csv:"-"`

	ExecuteRequestTraces []*proto.RequestTrace `json:"execute_request_traces" csv:"-"`

	AggregateSessionLifetimesSec        []float64 `csv:"-" json:"AggregateSessionLifetimesSec"`
	JupyterTrainingStartLatenciesMillis []float64 `json:"jupyter_training_start_latencies_millis" csv:"-"`

	AggregateSessionLifetimeSec       float64 `csv:"AggregateSessionLifetimeSec" json:"AggregateSessionLifetimeSec"`
	CompletedTrainings                int32   `csv:"CompletedTrainings" json:"CompletedTrainings"`
	CumulativeSessionIdleTime         float64 `csv:"CumulativeSessionIdleTimeSec" json:"CumulativeSessionIdleTimeSec"`
	CumulativeSessionTrainingTime     float64 `csv:"CumulativeSessionTrainingTimeSec" json:"CumulativeSessionTrainingTimeSec"`
	IdleHostsPerClass                 int32   `csv:"IdleHosts" json:"IdleHosts"`
	Migrated                          int32   `csv:"Migrated" json:"Migrated"`
	JupyterTrainingStartLatencyMillis float64 `json:"jupyter_training_start_latency_millis" csv:"jupyter_training_start_latency_millis"`
	NumFailedMigrations               int32   `json:"num_failed_migrations" csv:"num_failed_migrations"`
	OnDemandContainer                 int32   `csv:"OnDemandContainers" json:"OnDemandContainers"`
	NumNonTerminatedSessions          int32   `csv:"NumNonTerminatedSessions" json:"NumNonTerminatedSessions"`
	NumIdleSessions                   int32   `csv:"NumIdleSessions" json:"NumIdleSessions"`
	NumTrainingSessions               int32   `csv:"NumTrainingSessions" json:"NumTrainingSessions"`
	NumStoppedSessions                int32   `csv:"NumStoppedSessions" json:"NumStoppedSessions"`
	NumRunningSessions                int32   `csv:"NumRunningSessions" json:"NumRunningSessions"`
	NumSeenSessions                   int32   `csv:"NumSeenSessions" json:"NumSeenSessions"`

	Hosts                                                int32   `csv:"hosts" json:"hosts"`
	NumDisabledHosts                                     int32   `csv:"num_disabled_hosts" json:"num_disabled_hosts"`
	NumEmptyHosts                                        int32   `csv:"NumEmptyHosts" json:"NumEmptyHosts"`
	CumulativeHostActiveTime                             float64 `csv:"CumulativeHostActiveTimeSec" json:"CumulativeHostActiveTimeSec"`
	CumulativeHostIdleTime                               float64 `csv:"CumulativeHostIdleTimeSec" json:"CumulativeHostIdleTimeSec"`
	AggregateHostLifetime                                float64 `csv:"AggregateHostLifetimeSec" json:"AggregateHostLifetimeSec"`
	AggregateHostLifetimeOfRunningHosts                  float64 `csv:"AggregateHostLifetimeOfRunningHostsSec" json:"AggregateHostLifetimeOfRunningHostsSec"`
	CumulativeNumHostsProvisioned                        int32   `csv:"CumulativeNumHostsProvisioned" json:"CumulativeNumHostsProvisioned"`
	CumulativeNumHostsReleased                           int32   `csv:"cumulative_num_hosts_released" json:"cumulative_num_hosts_released"`
	CumulativeTimeProvisioningHosts                      float64 `csv:"CumulativeTimeProvisioningHostsSec" json:"CumulativeTimeProvisioningHostsSec"`
	NumActiveScaleOutEvents                              int32   `csv:"num_active_scale_out_events" json:"num_active_scale_out_events"`
	NumSuccessfulScaleOutEvents                          int32   `csv:"num_successful_scale_out_events" json:"num_successful_scale_out_events"`
	NumFailedScaleOutEvents                              int32   `csv:"num_failed_scale_out_events" json:"num_failed_scale_out_events"`
	NumActiveScaleInEvents                               int32   `csv:"num_active_scale_in_events" json:"num_active_scale_in_events"`
	NumSuccessfulScaleInEvents                           int32   `csv:"num_successful_scale_in_events" json:"num_successful_scale_in_events"`
	NumFailedScaleInEvents                               int32   `csv:"num_failed_scale_in_events" json:"num_failed_scale_in_events"`
	NumJupyterMessagesReceivedByClusterGateway           int64   `csv:"num_jupyter_messages_received_by_cluster_gateway" json:"num_jupyter_messages_received_by_cluster_gateway"`
	NumJupyterRepliesSentByClusterGateway                int64   `csv:"num_jupyter_replies_sent_by_cluster_gateway" json:"num_jupyter_replies_sent_by_cluster_gateway"`
	CumulativeRequestProcessingTimeClusterGateway        int64   `csv:"cumulative_request_processing_time_cluster_gateway" json:"cumulative_request_processing_time_cluster_gateway"`
	CumulativeRequestProcessingTimeLocalDaemon           int64   `csv:"cumulative_request_processing_time_local_daemon" json:"cumulative_request_processing_time_local_daemon"`
	CumulativeRequestProcessingTimeKernel                int64   `csv:"cumulative_request_processing_time_kernel" json:"cumulative_request_processing_time_kernel"`
	CumulativeResponseProcessingTimeClusterGateway       int64   `csv:"cumulative_response_processing_time_cluster_gateway" json:"cumulative_response_processing_time_cluster_gateway"`
	CumulativeResponseProcessingTimeLocalDaemon          int64   `csv:"cumulative_response_processing_time_local_daemon" json:"cumulative_response_processing_time_local_daemon"`
	CumulativeCudaInitMicroseconds                       float64 `csv:"cumulative_cuda_init_microseconds" json:"cumulative_cuda_init_microseconds"`
	NumCudaRuntimesInitialized                           float64 `csv:"num_cuda_runtimes_initialized" json:"num_cuda_runtimes_initialized"`
	CumulativeTimeDownloadTrainingDataMicroseconds       float64 `csv:"cumulative_time_download_training_data_microseconds" json:"cumulative_time_download_training_data_microseconds"`
	NumTimesDownloadTrainingDataMicroseconds             float64 `csv:"num_times_download_training_data_microseconds" json:"num_times_download_training_data_microseconds"`
	CumulativeTokenizeDatasetMicroseconds                float64 `csv:"cumulative_tokenize_dataset_microseconds" json:"cumulative_tokenize_dataset_microseconds"`
	NumTimesTokenizeDatasetMicroseconds                  float64 `csv:"num_times_tokenize_dataset_microseconds" json:"num_times_tokenize_dataset_microseconds"`
	CumulativeTimeDownloadModelMicroseconds              float64 `csv:"cumulative_time_download_model_microseconds" json:"cumulative_time_download_model_microseconds"`
	NumTimesDownloadModelMicroseconds                    float64 `csv:"num_times_download_model_microseconds" json:"num_times_download_model_microseconds"`
	CumulativeTimeUploadModelAndTrainingDataMicroseconds float64 `csv:"cumulative_time_upload_model_and_training_data_microseconds" json:"cumulative_time_upload_model_and_training_data_microseconds"`
	NumTimesUploadModelAndTrainingDataMicroseconds       float64 `csv:"num_times_upload_model_and_training_data_microseconds" json:"num_times_upload_model_and_training_data_microseconds"`
	CumulativeTimeCopyDataHostToDeviceMicroseconds       float64 `csv:"cumulative_time_copy_data_host_to_device_microseconds" json:"cumulative_time_copy_data_host_to_device_microseconds"`
	NumTimesCopyDataHostToDeviceMicroseconds             float64 `csv:"num_times_copy_data_host_to_device_microseconds" json:"num_times_copy_data_host_to_device_microseconds"`
	CumulativeTimeCopyDataDeviceToHostMicroseconds       float64 `csv:"cumulative_time_copy_data_device_to_host_microseconds" json:"cumulative_time_copy_data_device_to_host_microseconds"`
	NumTimesCopyDataDeviceToHostMicroseconds             float64 `csv:"num_times_copy_data_device_to_host_microseconds" json:"num_times_copy_data_device_to_host_microseconds"`
	CumulativeExecutionTimeMicroseconds                  float64 `csv:"cumulative_execution_time_microseconds" json:"cumulative_execution_time_microseconds"`
	CumulativeLeaderElectionTimeMicroseconds             float64 `csv:"cumulative_leader_election_time_microseconds" json:"cumulative_leader_election_time_microseconds"`
	CumulativeKernelPreprocessRequestMillis              float64 `csv:"cumulative_kernel_preprocess_request_millis" json:"cumulative_kernel_preprocess_request_millis"`
	CumulativeKernelCreateElectionMillis                 float64 `csv:"cumulative_kernel_create_election_millis" json:"cumulative_kernel_create_election_millis"`
	CumulativeKernelProposalVotePhaseMillis              float64 `csv:"cumulative_kernel_proposal_vote_phase_millis" json:"cumulative_kernel_proposal_vote_phase_millis"`
	CumulativeKernelPostprocessMillis                    float64 `csv:"cumulative_kernel_postprocess_millis" json:"cumulative_kernel_postprocess_millis"`
	CumulativeReplayTimeMicroseconds                     float64 `csv:"cumulative_replay_time_microseconds" json:"cumulative_replay_time_microseconds"`

	// Spec is the amount that is available/allocatable.
	// It only changes when adding or removing hosts to/from the cluster.

	SpecCPUs   float64 `csv:"SpecCPUs" json:"SpecCPUs"`
	SpecGPUs   float64 `csv:"SpecGPUs" json:"SpecGPUs"`
	SpecMemory float64 `csv:"SpecMemory" json:"SpecMemory"`
	SpecVRAM   float64 `csv:"SpecVRAM" json:"SpecVRAM"`

	// Idle means that they're not bound exclusively to a kernel replica.

	IdleCPUs   float64 `csv:"IdleCPUs" json:"IdleCPUs"`
	IdleGPUs   float64 `csv:"IdleGPUs" json:"IdleGPUs"`
	IdleMemory float64 `csv:"IdleMemory" json:"IdleMemory"`
	IdleVRAM   float64 `csv:"IdleVRAM" json:"IdleVRAM"`

	// Pending is the sum of the resources requested by all kernel replicas scheduled on a host
	// if they were to begin training at the same time.

	PendingCPUs   float64 `csv:"PendingCPUs" json:"PendingCPUs"`
	PendingGPUs   float64 `csv:"PendingGPUs" json:"PendingGPUs"`
	PendingMemory float64 `csv:"PendingMemory" json:"PendingMemory"`
	PendingVRAM   float64 `csv:"PendingVRAM" json:"PendingVRAM"`

	// Commited means that they're bound exclusively to a kernel replica.

	CommittedCPUs   float64 `csv:"CommittedCPUs" json:"CommittedCPUs"`
	CommittedGPUs   float64 `csv:"CommittedGPUs" json:"CommittedGPUs"`
	CommittedMemory float64 `csv:"CommittedMemory" json:"CommittedMemory"`
	CommittedVRAM   float64 `csv:"CommittedVRAM" json:"CommittedVRAM"`

	// Demand is the sum of the resources requested by all active sessions/kernels.

	DemandCPUs   float64 `csv:"DemandCPUs" json:"DemandCPUs"`
	DemandMemMb  float64 `csv:"DemandMemMb" json:"DemandMemMb"`
	DemandGPUs   float64 `csv:"DemandGPUs" json:"DemandGPUs"`
	DemandVRAMGb float64 `csv:"DemandVRAMGb" json:"DemandVRAMGb"`

	// Busy are what are actively being used by training kernels.
	// Only committed resources can be considered busy.
	// Committed resources are not considered to be busy until the associated kernel replica begins training.

	BusyCPUs   float64 `csv:"BusyCPUs" json:"BusyCPUs"`
	BusyGPUs   float64 `csv:"BusyGPUs" json:"BusyGPUs"`
	BusyMemory float64 `csv:"BusyMemory" json:"BusyMemory"`
	BusyVRAM   float64 `csv:"BusyVRAM" json:"BusyVRAM"`

	TotalNumReplays       int64 `json:"total_num_replays" csv:"total_num_replays"`
	TotalNumCellsReplayed int64 `json:"total_num_cells_replayed" csv:"total_num_cells_replayed"`

	SubscriptionRatio float64 `csv:"SubscriptionRatio" json:"SubscriptionRatio"`

	NumTimesKernelReplicaAvailableImmediately float64 `csv:"NumTimesKernelReplicaAvailableImmediately" json:"NumTimesKernelReplicaAvailableImmediately"`

	NumTimesKernelReplicaNotAvailableImmediately float64 `csv:"NumTimesKernelReplicaNotAvailableImmediately" json:"NumTimesKernelReplicaNotAvailableImmediately"`

	NumTimesPreviousPrimaryReplicaSelectedConsecutively int64 `csv:"NumTimesPreviousPrimaryReplicaSelectedConsecutively" json:"NumTimesPreviousPrimaryReplicaSelectedConsecutively"`

	NumTimesPreviousPrimaryReplicaUnavailable int64 `csv:"num_times_previous_primary_replica_unavailable" json:"num_times_previous_primary_replica_unavailable"`

	NumSuccessfulMigrations int32 `json:"num_successful_migrations" csv:"num_successful_migrations"`

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	Rescheduled   int32 `csv:"Rescheduled" json:"Rescheduled"`
	Resched2Ready int32 `csv:"Resched2Ready" json:"Resched2Ready"`
	Preempted     int32 `csv:"Preempted" json:"Preempted"`
}

type ClusterStatistics struct {
	///////////
	// Hosts //
	///////////

	ClusterEvents []*ClusterEvent `json:"cluster_events" csv:"-"`

	ExecuteRequestTraces []*proto.RequestTrace `json:"execute_request_traces" csv:"-"`

	AggregateSessionLifetimesSec        []float64 `csv:"-" json:"AggregateSessionLifetimesSec"`
	JupyterTrainingStartLatenciesMillis []float64 `json:"jupyter_training_start_latencies_millis" csv:"-"`

	Hosts            types.StatInt32 `json:"hosts" csv:"hosts"`
	NumDisabledHosts types.StatInt32 `json:"num_disabled_hosts" csv:"num_disabled_hosts"`
	NumEmptyHosts    types.StatInt32 `csv:"NumEmptyHosts" json:"NumEmptyHosts"` // The number of Hosts with 0 sessions/containers scheduled on them.

	// The amount of time hosts have spent not idling throughout the entire simulation
	CumulativeHostActiveTime types.StatFloat64 `csv:"CumulativeHostActiveTimeSec" json:"CumulativeHostActiveTimeSec"`
	// The amount of time hosts have spent idling throughout the entire simulation.
	CumulativeHostIdleTime types.StatFloat64 `csv:"CumulativeHostIdleTimeSec" json:"CumulativeHostIdleTimeSec"`
	// The aggregate, cumulative lifetime of ALL hosts provisioned at some point during the simulation.
	AggregateHostLifetime types.StatFloat64 `csv:"AggregateHostLifetimeSec" json:"AggregateHostLifetimeSec"`
	// The aggregate, cumulative lifetime of the hosts that are currently running.
	AggregateHostLifetimeOfRunningHosts types.StatFloat64 `csv:"AggregateHostLifetimeOfRunningHostsSec" json:"AggregateHostLifetimeOfRunningHostsSec"`

	// The total (cumulative) number of hosts provisioned during.
	CumulativeNumHostsProvisioned types.StatInt32 `csv:"CumulativeNumHostsProvisioned" json:"CumulativeNumHostsProvisioned"`
	// The total (cumulative) number of hosts released during.
	CumulativeNumHostsReleased types.StatInt32 `json:"cumulative_num_hosts_released" csv:"cumulative_num_hosts_released"`
	// The total amount of time spent provisioning hosts.
	CumulativeTimeProvisioningHosts types.StatFloat64 `csv:"CumulativeTimeProvisioningHostsSec" json:"CumulativeTimeProvisioningHostsSec"`

	NumActiveScaleOutEvents     types.StatInt32 `json:"num_active_scale_out_events" csv:"num_active_scale_out_events"`
	NumSuccessfulScaleOutEvents types.StatInt32 `json:"num_successful_scale_out_events" csv:"num_successful_scale_out_events"`
	NumFailedScaleOutEvents     types.StatInt32 `json:"num_failed_scale_out_events" csv:"num_failed_scale_out_events"`

	NumActiveScaleInEvents     types.StatInt32 `json:"num_active_scale_in_events" csv:"num_active_scale_in_events"`
	NumSuccessfulScaleInEvents types.StatInt32 `json:"num_successful_scale_in_events" csv:"num_successful_scale_in_events"`
	NumFailedScaleInEvents     types.StatInt32 `json:"num_failed_scale_in_events" csv:"num_failed_scale_in_events"`

	///////////////
	// Messaging //
	///////////////

	NumJupyterMessagesReceivedByClusterGateway types.StatInt64 `json:"num_jupyter_messages_received_by_cluster_gateway" csv:"num_jupyter_messages_received_by_cluster_gateway"`
	NumJupyterRepliesSentByClusterGateway      types.StatInt64 `json:"num_jupyter_replies_sent_by_cluster_gateway" csv:"num_jupyter_replies_sent_by_cluster_gateway"`

	// CumulativeRequestProcessingTimeClusterGateway is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeClusterGateway types.StatInt64 `json:"cumulative_request_processing_time_cluster_gateway" csv:"cumulative_request_processing_time_cluster_gateway"`
	// CumulativeRequestProcessingTimeLocalDaemon is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeLocalDaemon types.StatInt64 `json:"cumulative_request_processing_time_local_daemon" csv:"cumulative_request_processing_time_local_daemon"`

	// CumulativeRequestProcessingTimeKernel is calculated using the RequestTrace proto message.
	CumulativeRequestProcessingTimeKernel types.StatInt64 `csv:"cumulative_request_processing_time_kernel" json:"cumulative_request_processing_time_kernel"`

	// CumulativeRequestProcessingTimeClusterGateway is calculated using the RequestTrace proto message.
	CumulativeResponseProcessingTimeClusterGateway types.StatInt64 `json:"cumulative_response_processing_time_cluster_gateway" csv:"cumulative_response_processing_time_cluster_gateway"`
	// CumulativeRequestProcessingTimeLocalDaemon is calculated using the RequestTrace proto message.
	CumulativeResponseProcessingTimeLocalDaemon types.StatInt64 `json:"cumulative_response_processing_time_local_daemon" csv:"cumulative_response_processing_time_local_daemon"`
	// CumulativeRequestProcessingTimeKernel is calculated using the RequestTrace proto message.

	////////////////////////////////////////
	// Execution/kernel-Related Overheads //
	////////////////////////////////////////

	// CumulativeCudaInitMicroseconds is the cumulative, aggregate time spent initializing CUDA runtimes by all kernels.
	CumulativeCudaInitMicroseconds types.StatFloat64 `json:"cumulative_cuda_init_microseconds" csv:"cumulative_cuda_init_microseconds"`
	// NumCudaRuntimesInitialized is the number of times a CUDA runtime was initialized.
	NumCudaRuntimesInitialized types.StatFloat64 `json:"num_cuda_runtimes_initialized" csv:"num_cuda_runtimes_initialized"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent downloading
	// runtime/library/module dependencies by all kernels.
	// CumulativeTimeDownloadingDependenciesMicroseconds types.StatFloat64 `json:"cumulative_time_downloading_dependencies_microseconds" csv:"cumulative_time_downloading_dependencies_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel downloaded dependencies.
	// NumTimesDownloadedDependencies types.StatFloat64 `json:"num_times_downloaded_dependencies" csv:"num_times_downloaded_dependencies"`

	// CumulativeTimeDownloadTrainingDataMicroseconds is the cumulative, aggregate time spent downloading the
	// training data by all kernels.
	CumulativeTimeDownloadTrainingDataMicroseconds types.StatFloat64 `json:"cumulative_time_download_training_data_microseconds" csv:"cumulative_time_download_training_data_microseconds"`
	// NumTimesDownloadTrainingDataMicroseconds is the total number of times that a kernel downloaded the training data.
	NumTimesDownloadTrainingDataMicroseconds types.StatFloat64 `json:"num_times_download_training_data_microseconds" csv:"num_times_download_training_data_microseconds"`

	CumulativeTokenizeDatasetMicroseconds types.StatFloat64 `json:"cumulative_tokenize_dataset_microseconds" csv:"cumulative_tokenize_dataset_microseconds"`
	NumTimesTokenizeDatasetMicroseconds   types.StatFloat64 `json:"num_times_tokenize_dataset_microseconds" csv:"num_times_tokenize_dataset_microseconds"`

	// CumulativeTimeDownloadModelMicroseconds is the cumulative, aggregate time spent downloading the model by all kernels.
	CumulativeTimeDownloadModelMicroseconds types.StatFloat64 `json:"cumulative_time_download_model_microseconds" csv:"cumulative_time_download_model_microseconds"`
	// NumTimesDownloadModelMicroseconds is the total number of times that a kernel downloaded the model.
	NumTimesDownloadModelMicroseconds types.StatFloat64 `json:"num_times_download_model_microseconds" csv:"num_times_download_model_microseconds"`

	// CumulativeTimeDownloadingDependenciesMicroseconds is the cumulative, aggregate time spent uploading the model
	// and training data by all kernels.
	CumulativeTimeUploadModelAndTrainingDataMicroseconds types.StatFloat64 `json:"cumulative_time_upload_model_and_training_data_microseconds" csv:"cumulative_time_upload_model_and_training_data_microseconds"`
	// NumTimesDownloadedDependencies is the total number of times that a kernel uploaded the model and training data.
	NumTimesUploadModelAndTrainingDataMicroseconds types.StatFloat64 `json:"num_times_upload_model_and_training_data_microseconds" csv:"num_times_upload_model_and_training_data_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory) by all kernels.
	CumulativeTimeCopyDataHostToDeviceMicroseconds types.StatFloat64 `json:"cumulative_time_copy_data_host_to_device_microseconds" csv:"cumulative_time_copy_data_host_to_device_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from main
	// memory (i.e., host memory) to the GPU (i.e., device memory).
	NumTimesCopyDataHostToDeviceMicroseconds types.StatFloat64 `json:"num_times_copy_data_host_to_device_microseconds" csv:"num_times_copy_data_host_to_device_microseconds"`

	// CumulativeTimeCopyDataHostToDeviceMicroseconds is the cumulative, aggregate time spent copying data from the GPU
	// (i.e., device memory) to main memory (i.e., host memory).
	CumulativeTimeCopyDataDeviceToHostMicroseconds types.StatFloat64 `json:"cumulative_time_copy_data_device_to_host_microseconds" csv:"cumulative_time_copy_data_device_to_host_microseconds"`
	// NumTimesCopyDataHostToDeviceMicroseconds is the total number of times that a kernel copied data from the GPU
	// (i.e., device memory) to main memory (i.e., device memory).
	NumTimesCopyDataDeviceToHostMicroseconds types.StatFloat64 `json:"num_times_copy_data_device_to_host_microseconds" csv:"num_times_copy_data_device_to_host_microseconds"`

	// CumulativeExecutionTimeMicroseconds is the cumulative, aggregate time spent executing user code, excluding any
	// related overheads, by all kernels.
	CumulativeExecutionTimeMicroseconds types.StatFloat64 `json:"cumulative_execution_time_microseconds" csv:"cumulative_execution_time_microseconds"`

	// CumulativeLeaderElectionTimeMicroseconds is the cumulative, aggregate time spent handling leader elections.
	CumulativeLeaderElectionTimeMicroseconds types.StatFloat64 `json:"cumulative_leader_election_time_microseconds" csv:"cumulative_leader_election_time_microseconds"`

	// CumulativeKernelPreprocessRequestMillis is the time between when a kernel receives a request and when it begins handling the leader election.
	CumulativeKernelPreprocessRequestMillis types.StatFloat64 `json:"cumulative_kernel_preprocess_request_millis" csv:"cumulative_kernel_preprocess_request_millis"`
	// CumulativeKernelCreateElectionMillis is the time the kernels spent creating an election.
	CumulativeKernelCreateElectionMillis types.StatFloat64 `json:"cumulative_kernel_create_election_millis" csv:"cumulative_kernel_create_election_millis"`
	// CumulativeKernelProposalVotePhaseMillis is the cumulative duration of the proposal + voting phase of elections.
	CumulativeKernelProposalVotePhaseMillis types.StatFloat64 `json:"cumulative_kernel_proposal_vote_phase_millis" csv:"cumulative_kernel_proposal_vote_phase_millis"`
	// CumulativeKernelPostprocessMillis is the cumulative time after the kernels finish executing code before they send their response to their Local Scheduler.
	CumulativeKernelPostprocessMillis types.StatFloat64 `json:"cumulative_kernel_postprocess_millis" csv:"cumulative_kernel_postprocess_millis"`

	// CumulativeReplayTimeMicroseconds is the cumulative, aggregate time spent replaying cells, excluding any
	// related overheads, by all kernels.
	CumulativeReplayTimeMicroseconds types.StatFloat64 `json:"cumulative_replay_time_microseconds" csv:"cumulative_replay_time_microseconds"`
	// TotalNumReplays is the total number of times that one or more cells had to be replayed by a kernel.
	TotalNumReplays types.StatInt64 `json:"total_num_replays" csv:"total_num_replays"`
	// TotalNumCellsReplayed is the total number of cells that were replayed by all kernels.
	TotalNumCellsReplayed types.StatInt64 `json:"total_num_cells_replayed" csv:"total_num_cells_replayed"`

	///////////////
	// Resources //
	///////////////

	// Spec is the amount that is available/allocatable.
	// It only changes when adding or removing hosts to/from the cluster.

	SpecCPUs   types.StatFloat64 `csv:"SpecCPUs" json:"SpecCPUs"`
	SpecGPUs   types.StatFloat64 `csv:"SpecGPUs" json:"SpecGPUs"`
	SpecMemory types.StatFloat64 `csv:"SpecMemory" json:"SpecMemory"`
	SpecVRAM   types.StatFloat64 `csv:"SpecVRAM" json:"SpecVRAM"`

	// Idle means that they're not bound exclusively to a kernel replica.

	IdleCPUs   types.StatFloat64 `csv:"IdleCPUs" json:"IdleCPUs"`
	IdleGPUs   types.StatFloat64 `csv:"IdleGPUs" json:"IdleGPUs"`
	IdleMemory types.StatFloat64 `csv:"IdleMemory" json:"IdleMemory"`
	IdleVRAM   types.StatFloat64 `csv:"IdleVRAM" json:"IdleVRAM"`

	// Pending is the sum of the resources requested by all kernel replicas scheduled on a host
	// if they were to begin training at the same time.

	PendingCPUs   types.StatFloat64 `csv:"PendingCPUs" json:"PendingCPUs"`
	PendingGPUs   types.StatFloat64 `csv:"PendingGPUs" json:"PendingGPUs"`
	PendingMemory types.StatFloat64 `csv:"PendingMemory" json:"PendingMemory"`
	PendingVRAM   types.StatFloat64 `csv:"PendingVRAM" json:"PendingVRAM"`

	// Commited means that they're bound exclusively to a kernel replica.

	CommittedCPUs   types.StatFloat64 `csv:"CommittedCPUs" json:"CommittedCPUs"`
	CommittedGPUs   types.StatFloat64 `csv:"CommittedGPUs" json:"CommittedGPUs"`
	CommittedMemory types.StatFloat64 `csv:"CommittedMemory" json:"CommittedMemory"`
	CommittedVRAM   types.StatFloat64 `csv:"CommittedVRAM" json:"CommittedVRAM"`

	// Demand is the sum of the resources requested by all active sessions/kernels.

	DemandCPUs   types.StatFloat64 `csv:"DemandCPUs" json:"DemandCPUs"`
	DemandMemMb  types.StatFloat64 `csv:"DemandMemMb" json:"DemandMemMb"`
	DemandGPUs   types.StatFloat64 `csv:"DemandGPUs" json:"DemandGPUs"`
	DemandVRAMGb types.StatFloat64 `csv:"DemandVRAMGb" json:"DemandVRAMGb"`

	// Busy are what are actively being used by training kernels.
	// Only committed resources can be considered busy.
	// Committed resources are not considered to be busy until the associated kernel replica begins training.

	BusyCPUs   types.StatFloat64 `csv:"BusyCPUs" json:"BusyCPUs"`
	BusyGPUs   types.StatFloat64 `csv:"BusyGPUs" json:"BusyGPUs"`
	BusyMemory types.StatFloat64 `csv:"BusyMemory" json:"BusyMemory"`
	BusyVRAM   types.StatFloat64 `csv:"BusyVRAM" json:"BusyVRAM"`

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////

	SubscriptionRatio types.StatFloat64 `csv:"SubscriptionRatio" json:"SubscriptionRatio"`

	// NumTimesKernelReplicaAvailableImmediately is the number of times that a kernel replica was available
	// immediately when an "execute_request" message was received by the cluster (as opposed to having to
	// migrate some replicas around in order to serve the "execute_request").
	NumTimesKernelReplicaAvailableImmediately types.StatFloat64 `csv:"NumTimesKernelReplicaAvailableImmediately" json:"NumTimesKernelReplicaAvailableImmediately"`

	// NumTimesKernelReplicaNotAvailableImmediately is the number of times that a kernel replica was NOT available
	// immediately when an "execute_request" message was received by the cluster, and we had to migrate some replicas
	// around in order to serve the "execute_request" (as opposed to there being a replica available and able to
	// serve the "execute_request" immediately, with no migrations required).
	NumTimesKernelReplicaNotAvailableImmediately types.StatFloat64 `csv:"NumTimesKernelReplicaNotAvailableImmediately" json:"NumTimesKernelReplicaNotAvailableImmediately"`

	// NumTimesPreviousPrimaryReplicaSelectedConsecutively refers to the number of times that the previous primary replica is
	// selected again for the next consecutive user-submitted code execution.
	NumTimesPreviousPrimaryReplicaSelectedConsecutively types.StatInt64 `csv:"NumTimesPreviousPrimaryReplicaSelectedConsecutively" json:"NumTimesPreviousPrimaryReplicaSelectedConsecutively"`

	// NumTimesPreviousPrimaryReplicaUnavailable refers to the number of times that the previous primary replica is
	// NOT selected again for the next consecutive user-submitted code execution, due to it being unavailable (i.e.,
	// insufficient resources available on that replica's host).
	NumTimesPreviousPrimaryReplicaUnavailable types.StatInt64 `csv:"num_times_previous_primary_replica_unavailable" json:"num_times_previous_primary_replica_unavailable"`

	OnDemandContainer types.StatInt32 `csv:"OnDemandContainers" json:"OnDemandContainers"`
	// The Len of Cluster::Sessions (which is of type *SessionManager).
	// This includes all Sessions that have not been permanently stopped.
	NumNonTerminatedSessions types.StatInt32 `csv:"NumNonTerminatedSessions" json:"NumNonTerminatedSessions"`
	// The number of Sessions that are presently idle, not training.
	NumIdleSessions types.StatInt32 `csv:"NumIdleSessions" json:"NumIdleSessions"`
	// The number of Sessions that are presently actively-training.
	NumTrainingSessions types.StatInt32 `csv:"NumTrainingSessions" json:"NumTrainingSessions"`
	// The number of Sessions in the STOPPED state.
	NumStoppedSessions types.StatInt32 `csv:"NumStoppedSessions" json:"NumStoppedSessions"`
	// The number of Sessions that are actively running (but not necessarily training), so includes idle sessions.
	// Does not include evicted, init, or stopped sessions.
	NumRunningSessions types.StatInt32 `csv:"NumRunningSessions" json:"NumRunningSessions"`
	// NumSeenSessions is the total number of sessions seen/ever created.
	NumSeenSessions types.StatInt32 `csv:"NumSeenSessions" json:"NumSeenSessions"`

	NumSuccessfulMigrations types.StatInt32 `json:"num_successful_migrations" csv:"num_successful_migrations"`
	NumFailedMigrations     types.StatInt32 `json:"num_failed_migrations" csv:"num_failed_migrations"`

	// The amount of time that Sessions have spent idling throughout the entire simulation.
	CumulativeSessionIdleTime types.StatFloat64 `csv:"CumulativeSessionIdleTimeSec" json:"CumulativeSessionIdleTimeSec"`
	// The amount of time that Sessions have spent training throughout the entire simulation. This does NOT include replaying events.
	CumulativeSessionTrainingTime types.StatFloat64 `csv:"CumulativeSessionTrainingTimeSec" json:"CumulativeSessionTrainingTimeSec"`
	// The aggregate lifetime of all sessions created during the simulation (before being suspended).
	AggregateSessionLifetimeSec types.StatFloat64 `csv:"AggregateSessionLifetimeSec" json:"AggregateSessionLifetimeSec"`
	// Delay between when client submits "execute_request" and when kernel begins executing.
	JupyterTrainingStartLatencyMillis types.StatFloat64 `json:"jupyter_training_start_latency_millis" csv:"jupyter_training_start_latency_millis"`

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	Rescheduled       types.StatInt32 `csv:"Rescheduled" json:"Rescheduled"`
	Resched2Ready     types.StatInt32 `csv:"Resched2Ready" json:"Resched2Ready"`
	Migrated          types.StatInt32 `csv:"Migrated" json:"Migrated"`
	Preempted         types.StatInt32 `csv:"Preempted" json:"Preempted"`
	IdleHostsPerClass types.StatInt32 `csv:"IdleHosts" json:"IdleHosts"`

	//////////////
	// Sessions //
	//////////////

	CompletedTrainings types.StatInt32 `csv:"CompletedTrainings" json:"CompletedTrainings"`
}

func NewClusterStatistics() *ClusterStatistics {
	clusterStatistics := &ClusterStatistics{
		JupyterTrainingStartLatenciesMillis: make([]float64, 0),
		AggregateSessionLifetimesSec:        make([]float64, 0),
		ClusterEvents:                       make([]*ClusterEvent, 0),
		ExecuteRequestTraces:                make([]*proto.RequestTrace, 0),
	}

	// Initialize StatInt32 fields
	clusterStatistics.Hosts.Store(0)
	clusterStatistics.NumDisabledHosts.Store(0)
	clusterStatistics.NumEmptyHosts.Store(0)
	clusterStatistics.CumulativeNumHostsProvisioned.Store(0)
	clusterStatistics.CumulativeNumHostsReleased.Store(0)
	clusterStatistics.NumActiveScaleOutEvents.Store(0)
	clusterStatistics.NumSuccessfulScaleOutEvents.Store(0)
	clusterStatistics.NumFailedScaleOutEvents.Store(0)
	clusterStatistics.NumActiveScaleInEvents.Store(0)
	clusterStatistics.NumSuccessfulScaleInEvents.Store(0)
	clusterStatistics.NumFailedScaleInEvents.Store(0)

	// Initialize StatInt64 fields
	clusterStatistics.NumJupyterMessagesReceivedByClusterGateway.Store(0)
	clusterStatistics.NumJupyterRepliesSentByClusterGateway.Store(0)
	clusterStatistics.CumulativeRequestProcessingTimeClusterGateway.Store(0)
	clusterStatistics.CumulativeRequestProcessingTimeLocalDaemon.Store(0)
	clusterStatistics.CumulativeRequestProcessingTimeKernel.Store(0)
	clusterStatistics.CumulativeResponseProcessingTimeClusterGateway.Store(0)
	clusterStatistics.CumulativeResponseProcessingTimeLocalDaemon.Store(0)

	// Initialize StatFloat64 fields
	clusterStatistics.CumulativeHostActiveTime.Store(0.0)
	clusterStatistics.CumulativeHostIdleTime.Store(0.0)
	clusterStatistics.AggregateHostLifetime.Store(0.0)
	clusterStatistics.AggregateHostLifetimeOfRunningHosts.Store(0.0)
	clusterStatistics.CumulativeTimeProvisioningHosts.Store(0.0)
	clusterStatistics.CumulativeCudaInitMicroseconds.Store(0.0)
	clusterStatistics.NumCudaRuntimesInitialized.Store(0.0)
	clusterStatistics.CumulativeTimeDownloadTrainingDataMicroseconds.Store(0.0)
	clusterStatistics.NumTimesDownloadTrainingDataMicroseconds.Store(0.0)
	clusterStatistics.CumulativeTokenizeDatasetMicroseconds.Store(0.0)
	clusterStatistics.NumTimesTokenizeDatasetMicroseconds.Store(0.0)
	clusterStatistics.CumulativeTimeDownloadModelMicroseconds.Store(0.0)
	clusterStatistics.NumTimesDownloadModelMicroseconds.Store(0.0)
	clusterStatistics.CumulativeTimeUploadModelAndTrainingDataMicroseconds.Store(0.0)
	clusterStatistics.NumTimesUploadModelAndTrainingDataMicroseconds.Store(0.0)
	clusterStatistics.CumulativeTimeCopyDataHostToDeviceMicroseconds.Store(0.0)
	clusterStatistics.NumTimesCopyDataHostToDeviceMicroseconds.Store(0.0)
	clusterStatistics.CumulativeTimeCopyDataDeviceToHostMicroseconds.Store(0.0)
	clusterStatistics.NumTimesCopyDataDeviceToHostMicroseconds.Store(0.0)
	clusterStatistics.CumulativeExecutionTimeMicroseconds.Store(0.0)
	clusterStatistics.CumulativeLeaderElectionTimeMicroseconds.Store(0.0)
	clusterStatistics.CumulativeKernelPreprocessRequestMillis.Store(0.0)
	clusterStatistics.CumulativeKernelCreateElectionMillis.Store(0.0)
	clusterStatistics.CumulativeKernelProposalVotePhaseMillis.Store(0.0)
	clusterStatistics.CumulativeKernelPostprocessMillis.Store(0.0)

	clusterStatistics.SpecCPUs.Store(0)
	clusterStatistics.SpecGPUs.Store(0)
	clusterStatistics.SpecMemory.Store(0)
	clusterStatistics.SpecVRAM.Store(0)

	clusterStatistics.IdleCPUs.Store(0)
	clusterStatistics.IdleGPUs.Store(0)
	clusterStatistics.IdleMemory.Store(0)
	clusterStatistics.IdleVRAM.Store(0)

	clusterStatistics.PendingCPUs.Store(0)
	clusterStatistics.PendingGPUs.Store(0)
	clusterStatistics.PendingMemory.Store(0)
	clusterStatistics.PendingVRAM.Store(0)

	clusterStatistics.CommittedCPUs.Store(0)
	clusterStatistics.CommittedGPUs.Store(0)
	clusterStatistics.CommittedMemory.Store(0)
	clusterStatistics.CommittedVRAM.Store(0)

	clusterStatistics.DemandCPUs.Store(0)
	clusterStatistics.DemandMemMb.Store(0)
	clusterStatistics.DemandGPUs.Store(0)
	clusterStatistics.DemandVRAMGb.Store(0)

	clusterStatistics.BusyCPUs.Store(0)
	clusterStatistics.BusyGPUs.Store(0)
	clusterStatistics.BusyMemory.Store(0)
	clusterStatistics.BusyVRAM.Store(0)

	return clusterStatistics
}

func (stats *ClusterStatistics) ConvertToSerializable() *SerializableClusterStatistics {
	return &SerializableClusterStatistics{
		AggregateHostLifetime:                                stats.AggregateHostLifetime.Load(),
		AggregateHostLifetimeOfRunningHosts:                  stats.AggregateHostLifetimeOfRunningHosts.Load(),
		AggregateSessionLifetimeSec:                          stats.AggregateSessionLifetimeSec.Load(),
		AggregateSessionLifetimesSec:                         stats.AggregateSessionLifetimesSec,
		ClusterEvents:                                        stats.ClusterEvents,
		CommittedCPUs:                                        stats.CommittedCPUs.Load(),
		CommittedGPUs:                                        stats.CommittedGPUs.Load(),
		CommittedMemory:                                      stats.CommittedMemory.Load(),
		CommittedVRAM:                                        stats.CommittedVRAM.Load(),
		CompletedTrainings:                                   stats.CompletedTrainings.Load(),
		CumulativeCudaInitMicroseconds:                       stats.CumulativeCudaInitMicroseconds.Load(),
		CumulativeExecutionTimeMicroseconds:                  stats.CumulativeExecutionTimeMicroseconds.Load(),
		CumulativeHostActiveTime:                             stats.CumulativeHostActiveTime.Load(),
		CumulativeHostIdleTime:                               stats.CumulativeHostIdleTime.Load(),
		CumulativeKernelCreateElectionMillis:                 stats.CumulativeKernelCreateElectionMillis.Load(),
		CumulativeKernelPostprocessMillis:                    stats.CumulativeKernelPostprocessMillis.Load(),
		CumulativeKernelPreprocessRequestMillis:              stats.CumulativeKernelPreprocessRequestMillis.Load(),
		CumulativeKernelProposalVotePhaseMillis:              stats.CumulativeKernelProposalVotePhaseMillis.Load(),
		CumulativeLeaderElectionTimeMicroseconds:             stats.CumulativeLeaderElectionTimeMicroseconds.Load(),
		CumulativeNumHostsProvisioned:                        stats.CumulativeNumHostsProvisioned.Load(),
		CumulativeNumHostsReleased:                           stats.CumulativeNumHostsReleased.Load(),
		CumulativeReplayTimeMicroseconds:                     stats.CumulativeReplayTimeMicroseconds.Load(),
		CumulativeRequestProcessingTimeClusterGateway:        stats.CumulativeRequestProcessingTimeClusterGateway.Load(),
		CumulativeRequestProcessingTimeKernel:                stats.CumulativeRequestProcessingTimeKernel.Load(),
		CumulativeRequestProcessingTimeLocalDaemon:           stats.CumulativeRequestProcessingTimeLocalDaemon.Load(),
		CumulativeResponseProcessingTimeClusterGateway:       stats.CumulativeResponseProcessingTimeClusterGateway.Load(),
		CumulativeResponseProcessingTimeLocalDaemon:          stats.CumulativeResponseProcessingTimeLocalDaemon.Load(),
		CumulativeSessionIdleTime:                            stats.CumulativeSessionIdleTime.Load(),
		CumulativeSessionTrainingTime:                        stats.CumulativeSessionTrainingTime.Load(),
		CumulativeTimeCopyDataDeviceToHostMicroseconds:       stats.CumulativeTimeCopyDataDeviceToHostMicroseconds.Load(),
		CumulativeTimeCopyDataHostToDeviceMicroseconds:       stats.CumulativeTimeCopyDataHostToDeviceMicroseconds.Load(),
		CumulativeTimeDownloadModelMicroseconds:              stats.CumulativeTimeDownloadModelMicroseconds.Load(),
		CumulativeTimeDownloadTrainingDataMicroseconds:       stats.CumulativeTimeDownloadTrainingDataMicroseconds.Load(),
		CumulativeTimeProvisioningHosts:                      stats.CumulativeTimeProvisioningHosts.Load(),
		CumulativeTimeUploadModelAndTrainingDataMicroseconds: stats.CumulativeTimeUploadModelAndTrainingDataMicroseconds.Load(),
		CumulativeTokenizeDatasetMicroseconds:                stats.CumulativeTokenizeDatasetMicroseconds.Load(),
		DemandCPUs:                                           stats.DemandCPUs.Load(),
		DemandGPUs:                                           stats.DemandGPUs.Load(),
		DemandMemMb:                                          stats.DemandMemMb.Load(),
		DemandVRAMGb:                                         stats.DemandVRAMGb.Load(),
		BusyCPUs:                                             stats.BusyCPUs.Load(),
		BusyMemory:                                           stats.BusyMemory.Load(),
		BusyGPUs:                                             stats.BusyGPUs.Load(),
		BusyVRAM:                                             stats.BusyVRAM.Load(),
		ExecuteRequestTraces:                                 stats.ExecuteRequestTraces,
		Hosts:                                                stats.Hosts.Load(),
		IdleCPUs:                                             stats.IdleCPUs.Load(),
		IdleGPUs:                                             stats.IdleGPUs.Load(),
		IdleHostsPerClass:                                    stats.IdleHostsPerClass.Load(),
		IdleMemory:                                           stats.IdleMemory.Load(),
		IdleVRAM:                                             stats.IdleVRAM.Load(),
		JupyterTrainingStartLatenciesMillis:                  stats.JupyterTrainingStartLatenciesMillis,
		JupyterTrainingStartLatencyMillis:                    stats.JupyterTrainingStartLatencyMillis.Load(),
		Migrated:                                             stats.Migrated.Load(),
		NumActiveScaleInEvents:                               stats.NumActiveScaleInEvents.Load(),
		NumActiveScaleOutEvents:                              stats.NumActiveScaleOutEvents.Load(),
		NumCudaRuntimesInitialized:                           stats.NumCudaRuntimesInitialized.Load(),
		NumDisabledHosts:                                     stats.NumDisabledHosts.Load(),
		NumFailedMigrations:                                  stats.NumFailedMigrations.Load(),
		NumFailedScaleInEvents:                               stats.NumFailedScaleInEvents.Load(),
		NumFailedScaleOutEvents:                              stats.NumFailedScaleOutEvents.Load(),
		NumIdleSessions:                                      stats.NumIdleSessions.Load(),
		NumJupyterMessagesReceivedByClusterGateway:           stats.NumJupyterMessagesReceivedByClusterGateway.Load(),
		NumJupyterRepliesSentByClusterGateway:                stats.NumJupyterRepliesSentByClusterGateway.Load(),
		NumNonTerminatedSessions:                             stats.NumNonTerminatedSessions.Load(),
		NumRunningSessions:                                   stats.NumRunningSessions.Load(),
		NumSeenSessions:                                      stats.NumSeenSessions.Load(),
		NumStoppedSessions:                                   stats.NumStoppedSessions.Load(),
		NumSuccessfulMigrations:                              stats.NumSuccessfulMigrations.Load(),
		NumSuccessfulScaleInEvents:                           stats.NumSuccessfulScaleInEvents.Load(),
		NumSuccessfulScaleOutEvents:                          stats.NumSuccessfulScaleOutEvents.Load(),
		NumTimesCopyDataDeviceToHostMicroseconds:             stats.NumTimesCopyDataDeviceToHostMicroseconds.Load(),
		NumTimesCopyDataHostToDeviceMicroseconds:             stats.NumTimesCopyDataHostToDeviceMicroseconds.Load(),
		NumTimesDownloadModelMicroseconds:                    stats.NumTimesDownloadModelMicroseconds.Load(),
		NumTimesDownloadTrainingDataMicroseconds:             stats.NumTimesDownloadTrainingDataMicroseconds.Load(),
		NumTimesKernelReplicaAvailableImmediately:            stats.NumTimesKernelReplicaAvailableImmediately.Load(),
		NumTimesKernelReplicaNotAvailableImmediately:         stats.NumTimesKernelReplicaNotAvailableImmediately.Load(),
		NumTimesPreviousPrimaryReplicaSelectedConsecutively:  stats.NumTimesPreviousPrimaryReplicaSelectedConsecutively.Load(),
		NumTimesPreviousPrimaryReplicaUnavailable:            stats.NumTimesPreviousPrimaryReplicaUnavailable.Load(),
		NumTimesTokenizeDatasetMicroseconds:                  stats.NumTimesTokenizeDatasetMicroseconds.Load(),
		NumTimesUploadModelAndTrainingDataMicroseconds:       stats.NumTimesUploadModelAndTrainingDataMicroseconds.Load(),
		NumTrainingSessions:                                  stats.NumTrainingSessions.Load(),
		OnDemandContainer:                                    stats.OnDemandContainer.Load(),
		PendingCPUs:                                          stats.PendingCPUs.Load(),
		PendingGPUs:                                          stats.PendingGPUs.Load(),
		PendingMemory:                                        stats.PendingMemory.Load(),
		PendingVRAM:                                          stats.PendingVRAM.Load(),
		Preempted:                                            stats.Preempted.Load(),
		Resched2Ready:                                        stats.Resched2Ready.Load(),
		Rescheduled:                                          stats.Rescheduled.Load(),
		SpecCPUs:                                             stats.SpecCPUs.Load(),
		SpecGPUs:                                             stats.SpecGPUs.Load(),
		SpecMemory:                                           stats.SpecMemory.Load(),
		SpecVRAM:                                             stats.SpecVRAM.Load(),
		SubscriptionRatio:                                    stats.SubscriptionRatio.Load(),
		TotalNumCellsReplayed:                                stats.TotalNumCellsReplayed.Load(),
		TotalNumReplays:                                      stats.TotalNumReplays.Load(),
	}
}
