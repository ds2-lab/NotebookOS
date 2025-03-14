package metrics

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"
)

type NumActiveKernelProvider interface {
	NumActiveKernels() int32
}

type Manager struct {
	id string

	lastFullStatisticsUpdate time.Time

	metricsProvider *metrics.ClusterMetricsProvider

	clusterStatistics *metrics.ClusterStatistics

	localDaemonProvider metrics.LocalDaemonNodeProvider

	cluster scheduling.Cluster

	numActiveKernelProvider NumActiveKernelProvider

	log logger.Logger

	mu sync.RWMutex
}

func NewManager(id string, localDaemonProvider metrics.LocalDaemonNodeProvider, prometheusPort int,
	cluster scheduling.Cluster, numActiveTrainings *atomic.Int32, provider NumActiveKernelProvider) *Manager {

	mgr := &Manager{
		id:                      id,
		cluster:                 cluster,
		localDaemonProvider:     localDaemonProvider,
		numActiveKernelProvider: provider,
	}

	metricsProvider := metrics.NewClusterMetricsProvider(prometheusPort, mgr, numActiveTrainings)
	mgr.metricsProvider = metricsProvider

	config.InitLogger(&mgr.log, mgr)

	return mgr
}

// ClearClusterStatistics clears the current ClusterStatistics struct.
func (m *Manager) ClearClusterStatistics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.clusterStatistics = metrics.NewClusterStatistics()
}

func (m *Manager) GetClusterStatistics() *metrics.ClusterStatistics {
	return m.clusterStatistics
}

func (m *Manager) LastFullStatisticsUpdate() time.Time {
	return m.lastFullStatisticsUpdate
}

func (m *Manager) GetLocalDaemonNodeIDs(ctx context.Context, in *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error) {
	return m.localDaemonProvider.GetLocalDaemonNodeIDs(ctx, in)
}

func (m *Manager) GetId() string {
	return m.id
}

// UpdateClusterStatistics is passed to Distributed kernel Clients so that they may atomically update statistics.
func (m *Manager) UpdateClusterStatistics(updaterFunc func(statistics *metrics.ClusterStatistics)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	updaterFunc(m.clusterStatistics)
}

func (m *Manager) IncrementResourceCountsForNewHost(host metrics.Host) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.incrIdleResourcesForHost(host)
	m.incrSpecResourcesForHost(host)
}

func (m *Manager) DecrementResourceCountsForRemovedHost(host metrics.Host) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.decrIdleResourcesForHost(host)
	m.decrSpecResourcesForHost(host)
}

// GatherClusterStatistics updates all the values in the ClusterStatistics field.
//
// GatherClusterStatistics is thread-safe.
func (m *Manager) GatherClusterStatistics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var lastTime time.Time // Last update time

	if m.lastFullStatisticsUpdate.IsZero() {
		lastTime = now // We're doing the first update
	} else {
		lastTime = m.lastFullStatisticsUpdate
	}

	//var cpuUtil, gpuUtil, memUtil, vramUtil, demandGpus float64
	var demandCpus, demandMem, demandGpus, demandVram float64

	numNonEmptyHosts, numEmptyHosts := m.RecomputeResourceCounts()

	activeTime := time.Since(lastTime) * time.Duration(numNonEmptyHosts)
	idleTime := time.Since(lastTime) * time.Duration(numEmptyHosts)

	///////////
	// Hosts //
	///////////

	m.clusterStatistics.Hosts.Store(int32(m.cluster.Len()))
	m.clusterStatistics.NumDisabledHosts.Store(int32(m.cluster.NumDisabledHosts()))
	m.clusterStatistics.NumEmptyHosts.Store(int32(numEmptyHosts))

	m.clusterStatistics.CumulativeHostActiveTime.Add(activeTime.Seconds())
	m.clusterStatistics.CumulativeHostIdleTime.Add(idleTime.Seconds())
	m.clusterStatistics.AggregateHostLifetime.Add(time.Since(lastTime).Seconds() * float64(m.cluster.Len()))

	var numRunning, numIdle, numTraining, numStopped int
	m.cluster.RangeOverSessions(func(key string, value scheduling.UserSession) bool {
		if value.IsIdle() {
			numIdle += 1
			numRunning += 1
		} else if value.IsTraining() {
			numTraining += 1
			numRunning += 1
		} else if value.IsMigrating() {
			numRunning += 1
		} else if value.IsStopped() {
			numStopped += 1
			return true // Return here so that we don't increment the demand values for stopped sessions.
		}

		demandCpus += value.ResourceSpec().CPU()
		demandMem += value.ResourceSpec().MemoryMB()
		demandGpus += value.ResourceSpec().GPU()
		demandVram += value.ResourceSpec().VRAM()

		return true
	})

	m.clusterStatistics.NumSeenSessions.Store(int32(m.cluster.Sessions().Len()))
	m.clusterStatistics.NumRunningSessions.Store(int32(numRunning))
	m.clusterStatistics.NumIdleSessions.Store(int32(numIdle))
	m.clusterStatistics.NumTrainingSessions.Store(int32(numTraining))
	m.clusterStatistics.NumStoppedSessions.Store(int32(numStopped))

	m.clusterStatistics.DemandGPUs.Store(demandCpus)
	m.clusterStatistics.DemandMemMb.Store(demandMem)
	m.clusterStatistics.DemandGPUs.Store(demandGpus)
	m.clusterStatistics.DemandVRAMGb.Store(demandVram)

	///////////
	// Hosts //
	///////////

	m.clusterStatistics.Hosts.Store(int32(m.cluster.Len()))
	m.clusterStatistics.NumDisabledHosts.Store(int32(m.cluster.NumDisabledHosts()))

	/////////////////////////////////
	// Static & Dynamic Scheduling //
	/////////////////////////////////
	m.clusterStatistics.SubscriptionRatio.Store(m.cluster.Scheduler().SubscriptionRatio())

	////////////////////////
	// Dynamic Scheduling //
	////////////////////////

	//////////////
	// sessions //
	//////////////
	m.clusterStatistics.NumNonTerminatedSessions.Store(m.numActiveKernelProvider.NumActiveKernels())
	m.clusterStatistics.NumRunningSessions.Store(int32(m.cluster.Sessions().Len()))

	m.lastFullStatisticsUpdate = time.Now()

	m.log.Debug("=== Updated cluster Statistics ===")
	m.log.Debug("Idle CPUs: %.0f, Idle Mem: %.0f, Idle GPUs: %.0f, Idle VRAM: %.0f",
		m.clusterStatistics.IdleCPUs.Load(), m.clusterStatistics.IdleMemory.Load(), m.clusterStatistics.IdleGPUs.Load(), m.clusterStatistics.IdleVRAM.Load())
	m.log.Debug("Pending CPUs: %.0f, Pending Mem: %.0f, Pending GPUs: %.0f, Pending VRAM: %.0f",
		m.clusterStatistics.PendingCPUs.Load(), m.clusterStatistics.PendingMemory.Load(), m.clusterStatistics.PendingGPUs.Load(), m.clusterStatistics.PendingVRAM.Load())
	m.log.Debug("Committed CPUs: %.0f, Committed Mem: %.0f, Committed GPUs: %.0f, Committed VRAM: %.0f",
		m.clusterStatistics.CommittedCPUs.Load(), m.clusterStatistics.CommittedMemory.Load(), m.clusterStatistics.CommittedGPUs.Load(), m.clusterStatistics.CommittedVRAM.Load())
	m.log.Debug("Spec CPUs: %.0f, Spec Mem: %.0f, Spec GPUs: %.0f, Spec VRAM: %.0f",
		m.clusterStatistics.SpecCPUs.Load(), m.clusterStatistics.SpecMemory.Load(), m.clusterStatistics.SpecGPUs.Load(), m.clusterStatistics.SpecVRAM.Load())
	m.log.Debug("NumSeenSessions: %d, NumRunningSessions: %d, NumNonTerminatedSessions: %d, NumTraining: %d, NumIdle: %d, NumStopped: %m.",
		m.clusterStatistics.NumSeenSessions.Load(), m.clusterStatistics.NumRunningSessions.Load(), m.clusterStatistics.NumNonTerminatedSessions.Load(),
		m.clusterStatistics.NumTrainingSessions.Load(), m.clusterStatistics.NumIdleSessions.Load(), m.clusterStatistics.NumStoppedSessions.Load())
	m.log.Debug("NumHosts: %d, NumDisabledHosts: %d, NumEmptyHosts: %d",
		m.clusterStatistics.Hosts.Load(), m.clusterStatistics.NumDisabledHosts.Load(), m.clusterStatistics.NumEmptyHosts.Load())
}

// resetResourceCounts sets all resource counts in the ClusterStatistics to 0.
//
// Important: resetResourceCounts is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) resetResourceCounts() {
	m.clusterStatistics.IdleCPUs.Store(0.0)
	m.clusterStatistics.IdleMemory.Store(0.0)
	m.clusterStatistics.IdleGPUs.Store(0.0)
	m.clusterStatistics.IdleVRAM.Store(0.0)

	m.clusterStatistics.PendingCPUs.Store(0.0)
	m.clusterStatistics.PendingMemory.Store(0.0)
	m.clusterStatistics.PendingGPUs.Store(0.0)
	m.clusterStatistics.PendingVRAM.Store(0.0)

	m.clusterStatistics.CommittedCPUs.Store(0.0)
	m.clusterStatistics.CommittedMemory.Store(0.0)
	m.clusterStatistics.CommittedGPUs.Store(0.0)
	m.clusterStatistics.CommittedVRAM.Store(0.0)

	m.clusterStatistics.SpecCPUs.Store(0.0)
	m.clusterStatistics.SpecMemory.Store(0.0)
	m.clusterStatistics.SpecGPUs.Store(0.0)
	m.clusterStatistics.SpecVRAM.Store(0.0)
}

// incrIdleResourcesForHost increments the idle resource counts of the ClusterStatistics for a particular host.
//
// Important: incrIdleResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) incrIdleResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		m.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing idle resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	m.clusterStatistics.IdleCPUs.Add(host.IdleCPUs())
	m.clusterStatistics.IdleMemory.Add(host.IdleMemoryMb())
	m.clusterStatistics.IdleGPUs.Add(host.IdleGPUs())
	m.clusterStatistics.IdleVRAM.Add(host.IdleVRAM())
}

// incrPendingResourcesForHost increments the pending resource counts of the ClusterStatistics for a particular host.
//
// Important: incrPendingResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) incrPendingResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		m.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing pending resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	m.clusterStatistics.PendingCPUs.Add(host.PendingCPUs())
	m.clusterStatistics.PendingMemory.Add(host.PendingMemoryMb())
	m.clusterStatistics.PendingGPUs.Add(host.PendingGPUs())
	m.clusterStatistics.PendingVRAM.Add(host.PendingVRAM())
}

// incrCommittedResourcesForHost increments the committed resource counts of the ClusterStatistics for a particular host.
//
// Important: incrCommittedResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) incrCommittedResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		m.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing committed resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	m.clusterStatistics.CommittedCPUs.Add(host.CommittedCPUs())
	m.clusterStatistics.CommittedMemory.Add(host.CommittedMemoryMb())
	m.clusterStatistics.CommittedGPUs.Add(host.CommittedGPUs())
	m.clusterStatistics.CommittedVRAM.Add(host.CommittedVRAM())
}

// incrSpecResourcesForHost increments the spec resource counts of the ClusterStatistics for a particular host.
//
// Important: incrSpecResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) incrSpecResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		m.log.Warn("Host %s (ID=%s) is not enabled. Will not be incrementing spec resource counts.",
			host.GetNodeName(), host.GetID())
		return
	}

	m.clusterStatistics.SpecCPUs.Add(host.ResourceSpec().CPU())
	m.clusterStatistics.SpecMemory.Add(host.ResourceSpec().MemoryMB())
	m.clusterStatistics.SpecGPUs.Add(host.ResourceSpec().GPU())
	m.clusterStatistics.SpecVRAM.Add(host.ResourceSpec().VRAM())
}

// incrementResourceCountsForHost increments the resource counts of the ClusterStatistics for a particular host.
//
// Important: incrementResourceCountsForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) incrementResourceCountsForHost(host scheduling.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.incrIdleResourcesForHost(host)
	m.incrPendingResourcesForHost(host)
	m.incrCommittedResourcesForHost(host)
	m.incrSpecResourcesForHost(host)
}

// decrIdleResourcesForHost decrements the idle resource counts of the ClusterStatistics for a particular host.
//
// Important: decrIdleResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) decrIdleResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.clusterStatistics.IdleCPUs.Sub(host.IdleCPUs())
	m.clusterStatistics.IdleMemory.Sub(host.IdleMemoryMb())
	m.clusterStatistics.IdleGPUs.Sub(host.IdleGPUs())
	m.clusterStatistics.IdleVRAM.Sub(host.IdleVRAM())
}

// decrPendingResourcesForHost decrements the pending resource counts of the ClusterStatistics for a particular host.
//
// Important: decrPendingResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) decrPendingResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.clusterStatistics.PendingCPUs.Sub(host.PendingCPUs())
	m.clusterStatistics.PendingMemory.Sub(host.PendingMemoryMb())
	m.clusterStatistics.PendingGPUs.Sub(host.PendingGPUs())
	m.clusterStatistics.PendingVRAM.Sub(host.PendingVRAM())
}

// decrCommittedResourcesForHost decrements the committed resource counts of the ClusterStatistics for a particular host.
//
// Important: decrCommittedResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) decrCommittedResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.clusterStatistics.CommittedCPUs.Sub(host.CommittedCPUs())
	m.clusterStatistics.CommittedMemory.Sub(host.CommittedMemoryMb())
	m.clusterStatistics.CommittedGPUs.Sub(host.CommittedGPUs())
	m.clusterStatistics.CommittedVRAM.Sub(host.CommittedVRAM())
}

// decrSpecResourcesForHost decrements the spec resource counts of the ClusterStatistics for a particular host.
//
// Important: decrSpecResourcesForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) decrSpecResourcesForHost(host metrics.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.clusterStatistics.SpecCPUs.Sub(host.ResourceSpec().CPU())
	m.clusterStatistics.SpecMemory.Sub(host.ResourceSpec().MemoryMB())
	m.clusterStatistics.SpecGPUs.Sub(host.ResourceSpec().GPU())
	m.clusterStatistics.SpecVRAM.Sub(host.ResourceSpec().VRAM())
}

// decrementResourceCountsForHost decrements the resource counts of the ClusterStatistics for a particular host.
//
// Important: decrementResourceCountsForHost is NOT thread safe. The cluster statistics mutex must be acquired first.
func (m *Manager) decrementResourceCountsForHost(host scheduling.Host) {
	if !host.Enabled() {
		// If the host is not enabled, then just return.
		return
	}

	m.decrIdleResourcesForHost(host)
	m.decrPendingResourcesForHost(host)
	m.decrCommittedResourcesForHost(host)
	m.decrSpecResourcesForHost(host)
}

// RecomputeResourceCounts iterates over all the hosts in the cluster and updates the related resource count stats.
//
// Important: RecomputeResourceCounts is NOT thread safe. The cluster statistics mutex must be acquired first.
//
// RecomputeResourceCounts returns a tuple such that:
// - 1st element is the number of non-empty hosts
// - 2nd element is the number of empty hosts
func (m *Manager) RecomputeResourceCounts() (int, int) {
	m.resetResourceCounts()

	var numNonEmptyHosts, numEmptyHosts int

	// The aggregate, cumulative lifetime of the hosts that are currently running.
	var aggregateHostLifetimeOfRunningHosts float64

	m.cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		if !host.Enabled() {
			// If the host is not enabled, then just continue to the next host.
			return true
		}

		m.incrementResourceCountsForHost(host)

		if host.NumContainers() == 0 {
			numEmptyHosts += 1
		} else {
			numNonEmptyHosts += 1
		}

		aggregateHostLifetimeOfRunningHosts += time.Since(host.GetCreatedAt()).Seconds()

		return true
	})

	m.clusterStatistics.AggregateHostLifetimeOfRunningHosts.Store(aggregateHostLifetimeOfRunningHosts)

	return numNonEmptyHosts, numEmptyHosts
}
