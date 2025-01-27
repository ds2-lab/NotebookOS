package invoker

import (
	"time"
)

// ContainerMetricsProvider is an exported interface that exposes an API for publishing container-related metrics.
type ContainerMetricsProvider interface {
	// AddContainerCreationLatencyObservation records the latency of a container-creation event.
	//
	// If the target ContainerMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddContainerCreationLatencyObservation(latency time.Duration) error
}
