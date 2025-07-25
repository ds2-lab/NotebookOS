package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/metrics"
)

type PrometheusMetricsProvider interface {
	SpecGpuGaugeVec() *prometheus.GaugeVec
	CommittedGpuGaugeVec() *prometheus.GaugeVec
	PendingGpuGaugeVec() *prometheus.GaugeVec
	IdleGpuGaugeVec() *prometheus.GaugeVec
	SpecCpuGaugeVec() *prometheus.GaugeVec
	CommittedCpuGaugeVec() *prometheus.GaugeVec
	PendingCpuGaugeVec() *prometheus.GaugeVec
	IdleCpuGaugeVec() *prometheus.GaugeVec
	SpecMemoryGaugeVec() *prometheus.GaugeVec
	CommittedMemoryGaugeVec() *prometheus.GaugeVec
	PendingMemoryGaugeVec() *prometheus.GaugeVec
	IdleMemoryGaugeVec() *prometheus.GaugeVec

	GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram
	GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram
	GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec
	GetNumDisabledHostsGauge() prometheus.Gauge
	GetNumHostsGauge() prometheus.Gauge
	GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram

	// IncrementNumTrainingEventsCompletedCounterVec increments the NumTrainingEventsCompletedCounterVec Prometheus vec.
	IncrementNumTrainingEventsCompletedCounterVec()

	// PrometheusMetricsEnabled returns true if Prometheus metrics are enabled/available.
	PrometheusMetricsEnabled() bool
}

type ActiveExecutionProvider interface {
	// IncrementNumActiveExecutions increments the global counter of the number of active executions.
	IncrementNumActiveExecutions()

	// DecrementNumActiveExecutions decrements the global counter of the number of active executions.
	DecrementNumActiveExecutions()

	// NumActiveExecutions returns the global number of active executions.
	NumActiveExecutions() int32
}

type ClusterProvider func() Cluster

type MetricsProvider interface {
	PrometheusMetricsProvider
	server.MessagingMetricsProvider
	ActiveExecutionProvider

	// IncrementResourceCountsForNewHost is intended to be called when a Host is added to the Cluster.
	// IncrementResourceCountsForNewHost will increment the ClusterStatistics' resource counts
	// based on the resources available on the Host in question.
	IncrementResourceCountsForNewHost(host metrics.Host)

	// DecrementResourceCountsForRemovedHost is intended to be called when a Host is removed from the Cluster.
	// DecrementResourceCountsForRemovedHost will decrement the ClusterStatistics' resource counts
	// based on the resources available on the Host in question.
	DecrementResourceCountsForRemovedHost(host metrics.Host)
}
