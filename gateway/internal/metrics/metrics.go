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

type Manager struct {
	id string

	metricsProvider *metrics.ClusterMetricsProvider

	clusterStatistics *metrics.ClusterStatistics

	localDaemonProvider metrics.LocalDaemonNodeProvider

	cluster scheduling.Cluster

	log logger.Logger

	mu sync.RWMutex
}

func NewManager(id string, localDaemonProvider metrics.LocalDaemonNodeProvider, prometheusPort int,
	cluster scheduling.Cluster, numActiveTrainings *atomic.Int32) *Manager {

	mgr := &Manager{
		id:                  id,
		cluster:             cluster,
		localDaemonProvider: localDaemonProvider,
	}

	metricsProvider := metrics.NewClusterMetricsProvider(prometheusPort, mgr, numActiveTrainings)
	mgr.metricsProvider = metricsProvider

	config.InitLogger(&mgr.log, mgr)

	return mgr
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

// recomputeResourceCounts iterates over all the hosts in the cluster and updates the related resource count stats.
//
// Important: recomputeResourceCounts is NOT thread safe. The cluster statistics mutex must be acquired first.
//
// recomputeResourceCounts returns a tuple such that:
// - 1st element is the number of non-empty hosts
// - 2nd element is the number of empty hosts
func (m *Manager) recomputeResourceCounts() (int, int) {
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
