package statistics

type ClusterStatistics struct {
	Rescheduled       int32   `csv:"Rescheduled"`
	Resched2Ready     int32   `csv:"Resched2Ready"`
	Migrated          int32   `csv:"Migrated"`
	Preempted         int32   `csv:"Preempted"`
	OnDemandContainer int     `csv:"OnDemandContainers"`
	IdleHostsPerClass int32   `csv:"IdleHosts"`
	Trainings         int32   `csv:"Trainings"`
	SubscriptionRatio float64 `csv:"SubscriptionRatio"`
	DemandGPUs        float64 `csv:"DemandGPUs"`      // The number of GPUs required by all the actively-running Sessions.
	AvailableCPUs     float64 `csv:"AvailableCPUs"`   // The total number of vCPUs that the Cluster has at its disposal. This is NOT the number of currently-available vCPUs, meaning it does not take into account the current resource usage of the VirtualizedComputeResource.
	AvailableGPUs     float64 `csv:"AvailableGPUs"`   // The total number of GPUs that the Cluster has at its disposal. This is NOT the number of currently-available GPUs, meaning it does not take into account the current resource usage of the VirtualizedComputeResource.
	AvailableMemory   float64 `csv:"AvailableMemory"` // The total amount of RAM (in GB) that the Cluster has at its disposal. This is NOT the number of currently-available main memory, meaning it does not take into account the current resource usage of the VirtualizedComputeResource.
	AvailableVRAM     float64 `csv:"AvailableVRAM"`   // The total amount of VRAM available (in GB). This is NOT the number of currently-available VRAM, meaning it does not take into account the current resource usage of the VirtualizedComputeResource.
	IdleCPUs          float64 `csv:"IdleCPUs"`        // The total number of vCPUs that are uncommitted and therefore available within this Cluster. This quantity is equal to AllocatableCPUs - CommittedCPUs.
	IdleGPUs          float64 `csv:"IdleGPUs"`        // The total number of GPUs that are uncommitted and therefore available within this Cluster. This quantity is equal to AllocatableGPUs - CommittedGPUs.
	IdleMemory        float64 `csv:"IdleMemory"`      // The total amount of memory (i.e., RAM) in GB that is uncommitted and therefore available within this Cluster. This quantity is equal to AllocatableMemory - CommittedMemory.
	IdleVRAM          float64 `csv:"IdleVRAM"`        // The total amount of VRAM (in GB) that is uncommitted and therefore available within this Cluster. This quantity is equal to AllocatableVRAM - CommittedVRAM.
	PendingCPUs       float64 `csv:"PendingCPUs"`     // The sum of the outstanding CPUs (in vCPUs) of all Containers scheduled within this Cluster. Pending CPUs are not allocated or committed to a particular Container yet. The time at which resources are actually committed to a Container depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the Container is actively training.
	PendingGPUs       float64 `csv:"PendingGPUs"`     // The sum of the outstanding GPUs of all Containers scheduled within this Cluster. Pending CPUs are not allocated or committed to a particular Container yet. The time at which resources are actually committed to a Container depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the Container is actively training.
	PendingMemory     float64 `csv:"PendingMemory"`   // The sum of the outstanding RAM (in GB) of all Containers scheduled within this Cluster. Pending CPUs are not allocated or committed to a particular Container yet. The time at which resources are actually committed to a Container depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the Container is actively training.
	PendingVRAM       float64 `csv:"PendingVRAM"`     // The sum of the outstanding VRAM (in GB) of all Containers scheduled within this Cluster. Pending CPUs are not allocated or committed to a particular Container yet. The time at which resources are actually committed to a Container depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the Container is actively training.
	CommittedCPUs     float64 `csv:"CommittedCPUs"`   // The total number of vCPUs that are actively committed and allocated to Containers that are scheduled within this Cluster.
	CommittedGPUs     float64 `csv:"CommittedGPUs"`   // The total number of GPUs that are actively committed and allocated to Containers that are scheduled within this Cluster. IMPORTANT: This field is used by the ScaleManager.
	CommittedMemory   float64 `csv:"CommittedMemory"` // The total amount of memory (i.e., RAM) in GB that is actively committed and allocated to Containers that are scheduled within this Cluster.
	CommittedVRAM     float64 `csv:"CommittedVRAM"`   // The total amount of VRAM in GB that is actively committed and allocated to Containers that are scheduled within this Cluster.
	PlacedCPUs        float64 `csv:"PlacedCPUs"`      // The total number of vCPUs scheduled within this cluster. This is equal to the sum of PendingCPUs + CommittedCPUs.
	PlacedGPUs        float64 `csv:"PlacedGPUs"`      // The total number of GPUs scheduled within this cluster. This is equal to the sum of PendingGPUs + CommittedGPUs.
	PlacedMemory      float64 `csv:"PlacedMemory"`    // The total amount of memory (in GB) scheduled within this cluster. This is equal to the sum of PendingMemory + CommittedMemory.
	PlacedVRAM        float64 `csv:"PlacedVRAM"`      // The total number of VRAM scheduled within this cluster. This is equal to the sum of PendingVRAM + CommittedVRAM.
	GPUUtil           float64 `csv:"GPUUtil"`         // The current aggregate GPU utilization across all actively-running hosts within the cluster. This is a sum of percentages, so a value of 5,000 means that 50 GPUs are being fully-utilized.
	CPUUtil           float64 `csv:"CPUUtil"`         // The current aggregate CPU utilization across all actively-running hosts within the cluster. This is a sum of percentages, so a value of 5,000 means that 50 vCPUs are being fully-utilized.
	MemUtil           float64 `csv:"MemUtil"`         // Memory utilization, real-time.
	CPUOverload       int     `csv:"CPUOverload"`
	// The Len of Cluster::Sessions (which is of type *SessionManager).
	// This includes all Sessions that have not been permanently stopped.
	NumNonTerminatedSessions int `csv:"NumNonTerminatedSessions"`
	// The maximum number of all non-terminated Sessions at any point in the simulation.
	MaxNumNonTerminatedSessions int `csv:"MaxNumNonTerminatedSessions"`
	// The number of idle sessions that have been reclaimed via the keep-alive mechanism.
	// If the same session is reclaimed via keep-alive for being idle more than once, then it's counted multiple times.
	NumIdleSessionsReclaimed int `csv:"NumIdleSessionsReclaimed"`
	// The number of Hosts with 0 sessions/containers scheduled on them.
	NumEmptyHosts int `csv:"NumEmptyHosts"`
	// The number of Sessions that are presently idle, not training.
	NumIdleSessions int `csv:"NumIdleSessions"`
	// The number of Sessions that are presently actively-training.
	NumTrainingSessions int `csv:"NumTrainingSessions"`
	// The number of sessions that are replaying events.
	NumReplayingSessions int `csv:"NumReplayingSessions"`
	// The number of sessions in the evicted, needing-scheduling, or awaiting-start states.
	NumDescheduledSessions int `csv:"NumDescheduledSessions"`
	// The number of Sessions in the STOPPED state.
	NumStoppedSessions int `csv:"NumStoppedSessions"`
	// The amount of time hosts have spent not idling throughout the entire simulation
	CumulativeHostActiveTime float64 `csv:"CumulativeHostIdleTimeSec"`
	// The amount of time hosts have spent idling throughout the entire simulation.
	CumulativeHostIdleTime float64 `csv:"CumulativeHostIdleTimeSec"`
	// The amount of time that Sessions have spent idling throughout the entire simulation.
	CumulativeSessionIdleTime float64 `csv:"CumulativeSessionIdleTimeSec"`
	// The amount of time that Sessions have spent training throughout the entire simulation. This does NOT include replaying events.
	CumulativeSessionTrainingTime float64 `csv:"CumulativeSessionTrainingTimeSec"`
	// The amount of time Sessions spend replaying events following spot reclamations.
	// This will only be positive when checkpointing is disabled.
	CumulativeTimeReplayingEvents float64 `csv:"CumulativeTimeReplayingEventsSec"`
	// The amount of time that Sessions have spent training AND replaying events throughout the entire simulation.
	CumulativeSessionTrainingAndReplayingTime float64 `csv:"CumulativeSessionTrainingAndReplayingTimeSec"`
	// The number of Sessions that are actively running (but not necessarily training), so includes idle sessions.
	// Does not include evicted, init, or stopped sessions.
	NumRunningSessions int `csv:"NumRunningSessions"`
	// The aggregate, cumulative lifetime of ALL hosts provisioned at some point during the simulation.
	AggregateHostLifetime float64 `csv:"AggregateHostLifetimeSec"`
	// The aggregate, cumulative lifetime of the hosts that are currently running.
	AggregateHostLifetimeOfRunningHosts float64 `csv:"AggregateHostLifetimeOfRunningHostsSec"`
	// The aggregate, cumulative lifetime of all spot VMs to ever run.
	AggregateSpotInstanceLifetime float64 `csv:"AggregateSpotLifetimeSec"`
	// The aggregate, cumulative lifetime of all currently-running spot VMs.
	AggregateActiveSpotInstanceLifetime float64 `csv:"AggregateActiveSpotLifetimeSec"`
	// The aggregate lifetime of all sessions created during the simulation (before being suspended).
	AggregateSessionLifetime float64 `csv:"AggregateSessionLifetimeSec"`
	// The aggregate lifetime of all sessions before being fully/permanently terminated. This includes evicted/descheduled sessions.
	AggregateNotStoppedSessionLifetime float64 `csv:"AggregateNotStoppedSessionLifetimeSec"`
	// The number of spot reclamations that have been triggered.
	NumSpotReclamations int `csv:"NumSpotReclamations"`
	// The total number of sessions that have been reclaimed by a spot reclamation.
	// So, if a session has been reclaimed from spot reclamations 5 times, it's counted 5 times in this metric.
	NumSessionsSpotReclaimed int `csv:"NumSessionsSpotReclaimed"`
	// The total number of unique sessions that have been reclaimed by a spot reclamation.
	// So, if a session has been reclaimed from spot reclamations 5 times, it's only counted once in this metric.
	NumUniqueSessionsSpotReclaimed int `csv:"NumUniqueSessionsSpotReclaimed"`
	// The total (cumulative) number of hosts provisioned during the simulation run.
	CumulativeNumHostsProvisioned int `csv:"CumulativeNumHostsProvisioned"`
	// The total amount of time spent provisioning hosts.
	CumulativeTimeProvisioningHosts float64 `csv:"CumulativeTimeProvisioningHostsSec"`
	// The total amount of time spent provisioning serverless functions.
	CumulativeTimeProvisioningServerlessFunctions float64 `csv:"CumulativeTimeProvisioningServerlessFunctionsSec"`
	// The aggregate, cumulative `totalDelay` field of the currently-running sessions.
	AggregateActiveSessionTotalDelay float64 `csv:"AggregateSessionTotalDelaySec"`
	// The average `totalDelay` of the currently-running Sessions.
	AverageTotalDelay float64 `csv:"AverageSessionTotalDelaySec"`
	// The running, cumulative provider-side cost of all of the requests so far.
	CumulativeProviderCost float64 `csv:"CumulativeProviderCost"`
	// The running, cumulative tenant-side cost of all of the requests so far.
	CumulativeTenantCost float64 `csv:"CumulativeTenantCost"`
	// The average time that a Session spends alive. Only calculated once a Session stops permanently.
	AverageSessionLifetime float64 `csv:"AverageSessionLifetimeSec"`
	// The average time that a Session spends actively training. Only calculated once a Session stops permanently.
	AverageSessionTrainingTime float64 `csv:"AverageSessionTrainingTimeSec"`
	// Cumulative amount of money (in USD) spent by the provider, providing the checkpointing & recovery service to users.
	CumulativeCheckpointCostProvider float64 `csv:"CumulativeCheckpointCostProvider"`
	// Cumulative amount of money (in USD) spent by the users to utilize the checkpointing & recovery service.
	CumulativeCheckpointCostUser float64 `csv:"CumulativeCheckpointCostUser"`
	// The sum of all the session lifetimes.
	SumSessionLifetimes float64 `csv:"SumSessionLifetimesSec"`
	// The sum of all the session training times.
	SumSessionTrainingTimes float64 `csv:"SumSessionTrainingTimesSec"`
	// The cumulative tenant-side cost minus the cumulative provider-side cost.
	// If negative, then that indicates that the notebook provider is losing money.
	Profit float64 `csv:"Profit"`
	// The change in cumulative user-side cost for the current tick. Should always be non-negative.
	UserCostDelta float64 `csv:"UserCostDelta"`
	// The change in cumulative provider-side cost for the current tick. Should always be non-negative.
	ProviderCostDelta float64 `csv:"ProviderCostDelta"`
	// The change in profit (maybe positive or negative) for the current tick.
	ProfitDelta float64 `csv:"ProfitDelta"`
	// Collect all the resource requests used during the simulation.
	// ResourceRequests []ResourceRequest `csv:"-"`
	// The total, cumulative number of training events successfully completed.
	CumulativeNumTrainingsCompleted     int `csv:"CumulativeNumTrainingsCompleted"`
	CurrentNumTrainingSessionsFaaS      int `csv:"CurrentNumTrainingSessionsFaaS"`
	CurrentNumTrainingSessionsServerful int `csv:"CurrentNumTrainingSessionsServerful"`
}
