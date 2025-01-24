package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/types"
)

type Host interface {
	Enabled() bool
	IdleGPUs() float64
	PendingGPUs() float64
	CommittedGPUs() float64
	IdleCPUs() float64
	PendingCPUs() float64
	CommittedCPUs() float64
	IdleMemoryMb() float64
	PendingMemoryMb() float64
	CommittedMemoryMb() float64
	IdleVRAM() float64
	PendingVRAM() float64
	CommittedVRAM() float64
	ResourceSpec() types.ValidatableResourceSpec
	GetNodeName() string
	GetID() string
}

type ClusterMetricsProvider struct {
	promProvider                                  *GatewayPrometheusManager
	incrementResourceCountsForNewHostCallback     func(Host)
	decrementResourceCountsForRemovedHostCallback func(Host)
}

func NewClusterMetricsProvider(promProvider *GatewayPrometheusManager, incrCallback func(Host), decrCallback func(Host)) *ClusterMetricsProvider {
	return &ClusterMetricsProvider{
		promProvider: promProvider,
		incrementResourceCountsForNewHostCallback:     incrCallback,
		decrementResourceCountsForRemovedHostCallback: decrCallback,
	}
}

func (p *ClusterMetricsProvider) GetClusterMetricsProvider() *ClusterMetricsProvider {
	return p
}

// IncrementResourceCountsForNewHost is intended to be called when a Host is added to the Cluster.
// IncrementResourceCountsForNewHost will increment the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (p *ClusterMetricsProvider) IncrementResourceCountsForNewHost(host Host) {
	if p.incrementResourceCountsForNewHostCallback != nil {
		p.incrementResourceCountsForNewHostCallback(host)
	}
}

// DecrementResourceCountsForRemovedHost is intended to be called when a Host is removed from the Cluster.
// DecrementResourceCountsForRemovedHost will decrement the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (p *ClusterMetricsProvider) DecrementResourceCountsForRemovedHost(host Host) {
	if p.decrementResourceCountsForRemovedHostCallback != nil {
		p.decrementResourceCountsForRemovedHostCallback(host)
	}
}

func (p *ClusterMetricsProvider) GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetScaleOutLatencyMillisecondsHistogram()
}

func (p *ClusterMetricsProvider) GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetScaleInLatencyMillisecondsHistogram()
}

func (p *ClusterMetricsProvider) GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetPlacerFindHostLatencyMicrosecondsHistogram()
}

func (p *ClusterMetricsProvider) GetNumDisabledHostsGauge() prometheus.Gauge {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetNumDisabledHostsGauge()
}

func (p *ClusterMetricsProvider) GetNumHostsGauge() prometheus.Gauge {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetNumHostsGauge()
}

func (p *ClusterMetricsProvider) GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram {
	if p.promProvider == nil {
		return nil
	}

	return p.promProvider.GetHostRemoteSyncLatencyMicrosecondsHistogram()
}
