package metrics

import (
	"github.com/scusemua/distributed-notebook/common/proto"
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
	KernelTrainingEnded      ClusterEventName = "kernel_training_ended"
	KernelStopped            ClusterEventName = "kernel_stopped"
	ScaleOutStarted          ClusterEventName = "scale_out_started"
	ScaleOutEnded            ClusterEventName = "scale_out_ended"
	ScaleInStarted           ClusterEventName = "scale_in_started"
	ScaleInEnded             ClusterEventName = "scale_in_ended"
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
		KernelId:            in.Id,
		ReplicaId:           -1,
		Timestamp:           now,
		TimestampUnixMillis: now.UnixMilli(),
	})
	d.clusterStatisticsMutex.Unlock()
*/

type ClusterStatistics struct {
	ClusterEvents                                        []*ClusterEvent       `json:"cluster_events" csv:"-"`
	ExecuteRequestTraces                                 []*proto.RequestTrace `json:"execute_request_traces" csv:"-"`
	JupyterTrainingStartLatenciesMillis                  []float64             `json:"jupyter_training_start_latencies_millis" csv:"-"`
	AggregateSessionLifetimesSec                         []float64             `csv:"-" json:"AggregateSessionLifetimesSec"`
	TotalNumReplays                                      int64                 `json:"total_num_replays" csv:"total_num_replays"`
	CumulativeTimeDownloadTrainingDataMicroseconds       float64               `json:"cumulative_time_download_training_data_microseconds" csv:"cumulative_time_download_training_data_microseconds"`
	CumulativeHostIdleTime                               float64               `csv:"CumulativeHostIdleTimeSec" json:"CumulativeHostIdleTimeSec"`
	AggregateHostLifetime                                float64               `csv:"AggregateHostLifetimeSec" json:"AggregateHostLifetimeSec"`
	SpecCPUs                                             float64               `csv:"SpecCPUs" json:"SpecCPUs"`
	CumulativeNumHostsProvisioned                        int                   `csv:"CumulativeNumHostsProvisioned" json:"CumulativeNumHostsProvisioned"`
	CumulativeNumHostsReleased                           int                   `json:"cumulative_num_hosts_released" csv:"cumulative_num_hosts_released"`
	CumulativeTimeProvisioningHosts                      float64               `csv:"CumulativeTimeProvisioningHostsSec" json:"CumulativeTimeProvisioningHostsSec"`
	NumActiveScaleOutEvents                              int                   `json:"num_active_scale_out_events" csv:"num_active_scale_out_events"`
	NumSuccessfulScaleOutEvents                          int                   `json:"num_successful_scale_out_events" csv:"num_successful_scale_out_events"`
	NumFailedScaleOutEvents                              int                   `json:"num_failed_scale_out_events" csv:"num_failed_scale_out_events"`
	SpecMemory                                           float64               `csv:"SpecMemory" json:"SpecMemory"`
	NumSuccessfulScaleInEvents                           int                   `json:"num_successful_scale_in_events" csv:"num_successful_scale_in_events"`
	NumFailedScaleInEvents                               int                   `json:"num_failed_scale_in_events" csv:"num_failed_scale_in_events"`
	NumJupyterMessagesReceivedByClusterGateway           int64                 `json:"num_jupyter_messages_received_by_cluster_gateway" csv:"num_jupyter_messages_received_by_cluster_gateway"`
	NumJupyterRepliesSentByClusterGateway                int64                 `json:"num_jupyter_replies_sent_by_cluster_gateway" csv:"num_jupyter_replies_sent_by_cluster_gateway"`
	CumulativeRequestProcessingTimeClusterGateway        int64                 `json:"cumulative_request_processing_time_cluster_gateway" csv:"cumulative_request_processing_time_cluster_gateway"`
	CumulativeRequestProcessingTimeLocalDaemon           int64                 `json:"cumulative_request_processing_time_local_daemon" csv:"cumulative_request_processing_time_local_daemon"`
	CumulativeRequestProcessingTimeKernel                int64
	CumulativeResponseProcessingTimeClusterGateway       int64   `json:"cumulative_response_processing_time_cluster_gateway" csv:"cumulative_response_processing_time_cluster_gateway"`
	CumulativeResponseProcessingTimeLocalDaemon          int64   `json:"cumulative_response_processing_time_local_daemon" csv:"cumulative_response_processing_time_local_daemon"`
	CumulativeCudaInitMicroseconds                       float64 `json:"cumulative_cuda_init_microseconds" csv:"cumulative_cuda_init_microseconds"`
	NumCudaRuntimesInitialized                           float64 `json:"num_cuda_runtimes_initialized" csv:"num_cuda_runtimes_initialized"`
	SpecGPUs                                             float64 `csv:"SpecGPUs" json:"SpecGPUs"`
	NumTimesDownloadTrainingDataMicroseconds             float64 `json:"num_times_download_training_data_microseconds" csv:"num_times_download_training_data_microseconds"`
	CumulativeTokenizeDatasetMicroseconds                float64 `json:"cumulative_tokenize_dataset_microseconds" csv:"cumulative_tokenize_dataset_microseconds"`
	NumTimesTokenizeDatasetMicroseconds                  float64 `json:"num_times_tokenize_dataset_microseconds" csv:"num_times_tokenize_dataset_microseconds"`
	CumulativeTimeDownloadModelMicroseconds              float64 `json:"cumulative_time_download_model_microseconds" csv:"cumulative_time_download_model_microseconds"`
	NumTimesDownloadModelMicroseconds                    float64 `json:"num_times_download_model_microseconds" csv:"num_times_download_model_microseconds"`
	CumulativeTimeUploadModelAndTrainingDataMicroseconds float64 `json:"cumulative_time_upload_model_and_training_data_microseconds" csv:"cumulative_time_upload_model_and_training_data_microseconds"`
	NumTimesUploadModelAndTrainingDataMicroseconds       float64 `json:"num_times_upload_model_and_training_data_microseconds" csv:"num_times_upload_model_and_training_data_microseconds"`
	CumulativeTimeCopyDataHostToDeviceMicroseconds       float64 `json:"cumulative_time_copy_data_host_to_device_microseconds" csv:"cumulative_time_copy_data_host_to_device_microseconds"`
	NumTimesCopyDataHostToDeviceMicroseconds             float64 `json:"num_times_copy_data_host_to_device_microseconds" csv:"num_times_copy_data_host_to_device_microseconds"`
	CumulativeTimeCopyDataDeviceToHostMicroseconds       float64 `json:"cumulative_time_copy_data_device_to_host_microseconds" csv:"cumulative_time_copy_data_device_to_host_microseconds"`
	NumTimesCopyDataDeviceToHostMicroseconds             float64 `json:"num_times_copy_data_device_to_host_microseconds" csv:"num_times_copy_data_device_to_host_microseconds"`
	CumulativeExecutionTimeMicroseconds                  float64 `json:"cumulative_execution_time_microseconds" csv:"cumulative_execution_time_microseconds"`
	CumulativeLeaderElectionTimeMicroseconds             float64 `json:"cumulative_leader_election_time_microseconds" csv:"cumulative_leader_election_time_microseconds"`
	CumulativeKernelPreprocessRequestMillis              float64 `json:"cumulative_kernel_preprocess_request_millis" csv:"cumulative_kernel_preprocess_request_millis"`
	CumulativeKernelCreateElectionMillis                 float64 `json:"cumulative_kernel_create_election_millis" csv:"cumulative_kernel_create_election_millis"`
	CumulativeKernelProposalVotePhaseMillis              float64 `json:"cumulative_kernel_proposal_vote_phase_millis" csv:"cumulative_kernel_proposal_vote_phase_millis"`
	CumulativeKernelPostprocessMillis                    float64 `json:"cumulative_kernel_postprocess_millis" csv:"cumulative_kernel_postprocess_millis"`
	CumulativeReplayTimeMicroseconds                     float64 `json:"cumulative_replay_time_microseconds" csv:"cumulative_replay_time_microseconds"`
	NumEmptyHosts                                        int     `csv:"NumEmptyHosts" json:"NumEmptyHosts"`
	TotalNumCellsReplayed                                int64   `json:"total_num_cells_replayed" csv:"total_num_cells_replayed"`
	AggregateHostLifetimeOfRunningHosts                  float64 `csv:"AggregateHostLifetimeOfRunningHostsSec" json:"AggregateHostLifetimeOfRunningHostsSec"`
	CumulativeHostActiveTime                             float64 `csv:"CumulativeHostActiveTimeSec" json:"CumulativeHostActiveTimeSec"`
	NumActiveScaleInEvents                               int     `json:"num_active_scale_in_events" csv:"num_active_scale_in_events"`
	SpecVRAM                                             float64 `csv:"SpecVRAM" json:"SpecVRAM"`
	IdleCPUs                                             float64 `csv:"IdleCPUs" json:"IdleCPUs"`
	IdleGPUs                                             float64 `csv:"IdleGPUs" json:"IdleGPUs"`
	IdleMemory                                           float64 `csv:"IdleMemory" json:"IdleMemory"`
	IdleVRAM                                             float64 `csv:"IdleVRAM" json:"IdleVRAM"`
	PendingCPUs                                          float64 `csv:"PendingCPUs" json:"PendingCPUs"`
	PendingGPUs                                          float64 `csv:"PendingGPUs" json:"PendingGPUs"`
	PendingMemory                                        float64 `csv:"PendingMemory" json:"PendingMemory"`
	PendingVRAM                                          float64 `csv:"PendingVRAM" json:"PendingVRAM"`
	CommittedCPUs                                        float64 `csv:"CommittedCPUs" json:"CommittedCPUs"`
	CommittedGPUs                                        float64 `csv:"CommittedGPUs" json:"CommittedGPUs"`
	CommittedMemory                                      float64 `csv:"CommittedMemory" json:"CommittedMemory"`
	CommittedVRAM                                        float64 `csv:"CommittedVRAM" json:"CommittedVRAM"`
	DemandCPUs                                           float64 `csv:"DemandCPUs" json:"DemandCPUs"`
	DemandMemMb                                          float64 `csv:"DemandMemMb" json:"DemandMemMb"`
	DemandGPUs                                           float64 `csv:"DemandGPUs" json:"DemandGPUs"`
	DemandVRAMGb                                         float64 `csv:"DemandVRAMGb" json:"DemandVRAMGb"`
	SubscriptionRatio                                    float64 `csv:"SubscriptionRatio" json:"SubscriptionRatio"`
	NumTimesKernelReplicaAvailableImmediately            float64 `csv:"NumTimesKernelReplicaAvailableImmediately" json:"NumTimesKernelReplicaAvailableImmediately"`
	NumTimesKernelReplicaNotAvailableImmediately         float64 `csv:"NumTimesKernelReplicaNotAvailableImmediately" json:"NumTimesKernelReplicaNotAvailableImmediately"`
	NumTimesPreviousPrimaryReplicaSelectedConsecutively  int64   `csv:"NumTimesPreviousPrimaryReplicaSelectedConsecutively" json:"NumTimesPreviousPrimaryReplicaSelectedConsecutively"`
	NumTimesPreviousPrimaryReplicaUnavailable            int64   `csv:"" json:""`
	OnDemandContainer                                    int     `csv:"OnDemandContainers" json:"OnDemandContainers"`
	NumNonTerminatedSessions                             int     `csv:"NumNonTerminatedSessions" json:"NumNonTerminatedSessions"`
	NumIdleSessions                                      int     `csv:"NumIdleSessions" json:"NumIdleSessions"`
	Hosts                                                int     `json:"hosts" csv:"hosts"`
	JupyterTrainingStartLatencyMillis                    float64 `json:"jupyter_training_start_latency_millis" csv:"jupyter_training_start_latency_millis"`
	NumDisabledHosts                                     int     `json:"num_disabled_hosts" csv:"num_disabled_hosts"`
	AggregateSessionLifetimeSec                          float64 `csv:"AggregateSessionLifetimeSec" json:"AggregateSessionLifetimeSec"`
	NumTrainingSessions                                  int     `csv:"NumTrainingSessions" json:"NumTrainingSessions"`
	NumStoppedSessions                                   int     `csv:"NumStoppedSessions" json:"NumStoppedSessions"`
	NumRunningSessions                                   int     `csv:"NumRunningSessions" json:"NumRunningSessions"`
	NumSeenSessions                                      int     `csv:"NumSeenSessions" json:"NumSeenSessions"`
	NumSuccessfulMigrations                              int     `json:"num_successful_migrations" csv:"num_successful_migrations"`
	NumFailedMigrations                                  int     `json:"num_failed_migrations" csv:"num_failed_migrations"`
	CumulativeSessionIdleTime                            float64 `csv:"CumulativeSessionIdleTimeSec" json:"CumulativeSessionIdleTimeSec"`
	CumulativeSessionTrainingTime                        float64 `csv:"CumulativeSessionTrainingTimeSec" json:"CumulativeSessionTrainingTimeSec"`
	Preempted                                            int32   `csv:"Preempted" json:"Preempted"`
	Migrated                                             int32   `csv:"Migrated" json:"Migrated"`
	Resched2Ready                                        int32   `csv:"Resched2Ready" json:"Resched2Ready"`
	Rescheduled                                          int32   `csv:"Rescheduled" json:"Rescheduled"`
	IdleHostsPerClass                                    int32   `csv:"IdleHosts" json:"IdleHosts"`
	CompletedTrainings                                   int32   `csv:"CompletedTrainings" json:"CompletedTrainings"`
}

func NewClusterStatistics() *ClusterStatistics {
	return &ClusterStatistics{
		JupyterTrainingStartLatenciesMillis: make([]float64, 0),
		AggregateSessionLifetimesSec:        make([]float64, 0),
		ClusterEvents:                       make([]*ClusterEvent, 0),
		ExecuteRequestTraces:                make([]*proto.RequestTrace, 0),
	}
}
