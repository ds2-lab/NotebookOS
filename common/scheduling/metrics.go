package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/statistics"
)

// StatisticsUpdater is a function that updates the fields of the given *statistics.ClusterStatistics.
type StatisticsUpdater func(statistics *statistics.ClusterStatistics)

// StatisticsUpdaterProvider is a function that accepts another function as an argument.
type StatisticsUpdaterProvider func(StatisticsUpdater)

type MetricsProvider interface {
	GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram
	GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram
	GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec
	GetNumDisabledHostsGauge() prometheus.Gauge
	GetNumHostsGauge() prometheus.Gauge
	GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram
}
