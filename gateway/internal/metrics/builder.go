package metrics

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"sync/atomic"
)

type ManagerBuilder struct {
	id                      string
	localDaemonProvider     metrics.LocalDaemonNodeProvider
	prometheusPort          int
	numActiveTrainings      *atomic.Int32
	numActiveKernelProvider NumActiveKernelProvider
}

// NewManagerBuilder initializes a new builder instance.
func NewManagerBuilder() *ManagerBuilder {
	return &ManagerBuilder{}
}

// SetID sets the ID.
func (b *ManagerBuilder) SetID(id string) *ManagerBuilder {
	b.id = id
	return b
}

// SetLocalDaemonProvider sets the LocalDaemonProvider.
func (b *ManagerBuilder) SetLocalDaemonProvider(provider metrics.LocalDaemonNodeProvider) *ManagerBuilder {
	b.localDaemonProvider = provider
	return b
}

// SetPrometheusPort sets the Prometheus port.
func (b *ManagerBuilder) SetPrometheusPort(port int) *ManagerBuilder {
	b.prometheusPort = port
	return b
}

// SetNumActiveTrainings sets the numActiveTrainings.
func (b *ManagerBuilder) SetNumActiveTrainings(numActiveTrainings *atomic.Int32) *ManagerBuilder {
	b.numActiveTrainings = numActiveTrainings
	return b
}

// SetNumActiveKernelProvider sets the NumActiveKernelProvider.
func (b *ManagerBuilder) SetNumActiveKernelProvider(provider NumActiveKernelProvider) *ManagerBuilder {
	b.numActiveKernelProvider = provider
	return b
}

// Build constructs the Manager object with validation.
func (b *ManagerBuilder) Build() *Manager {
	manager := &Manager{
		id:                       b.id,
		localDaemonProvider:      b.localDaemonProvider,
		kernelStatisticsProvider: b.numActiveKernelProvider,
	}

	metricsProvider := metrics.NewClusterMetricsProvider(b.prometheusPort, manager, b.numActiveTrainings)
	manager.metricsProvider = metricsProvider

	config.InitLogger(&manager.log, manager)

	return manager
}
