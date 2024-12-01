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
