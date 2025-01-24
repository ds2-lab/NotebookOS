package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/metrics"
)

type PrometheusMetricsProvider interface {
	GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram
	GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram
	GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec
	GetNumDisabledHostsGauge() prometheus.Gauge
	GetNumHostsGauge() prometheus.Gauge
	GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram
}

type MetricsProvider interface {
	PrometheusMetricsProvider
	GetClusterMetricsProvider() *metrics.ClusterMetricsProvider
}
