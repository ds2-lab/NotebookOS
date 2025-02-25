package metrics

import (
	"github.com/scusemua/distributed-notebook/common/metrics"
	"sync"
)

type Manager struct {
	metricsProvider *metrics.ClusterMetricsProvider

	mu sync.RWMutex
}

func NewManager(prometheusPort int) *Manager {
	mgr := &Manager{}

	metricsProvider := metrics.NewClusterMetricsProvider(
		prometheusPort, gatewayDaemon, clusterGateway.UpdateClusterStatistics,
		clusterGateway.IncrementResourceCountsForNewHost, clusterGateway.DecrementResourceCountsForRemovedHost,
		&clusterGateway.numActiveTrainings)

	gatewayDaemon.metricsProvider = metricsProvider

	return mgr
}

// UpdateClusterStatistics is passed to Distributed kernel Clients so that they may atomically update statistics.
func (m *Manager) UpdateClusterStatistics(updaterFunc func(statistics *metrics.ClusterStatistics)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	updaterFunc(m.ClusterStatistics)
}
