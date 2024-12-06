package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsProvider interface {
	GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram
	GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram
	GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec
	GetNumDisabledHostsGauge() prometheus.Gauge
	GetNumHostsGauge() prometheus.Gauge
	GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram
}
