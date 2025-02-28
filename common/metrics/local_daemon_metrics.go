package metrics

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"time"
)

var (
	ErrLocalDaemonPrometheusManagerAlreadyRunning = errors.New("LocalDaemonPrometheusManager is already running")
	ErrLocalDaemonPrometheusManagerNotRunning     = errors.New("LocalDaemonPrometheusManager is not running")
)

// LocalDaemonPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This to be used by Local Daemons. The Cluster Gateway uses the ClusterGatewayPrometheusManager struct.
type LocalDaemonPrometheusManager struct {
	*basePrometheusManager

	//SpecGpuGauge      prometheus.Gauge // SpecGpuGauge is a cached return of SpecGpuGaugeVec.With(<label for the local daemon on this node>)
	//CommittedGpuGauge prometheus.Gauge // CommittedGpuGauge is a cached return of CommittedGpuGaugeVec.With(<label for the local daemon on this node>)
	//PendingGpuGauge   prometheus.Gauge // PendingGpuGauge is a cached return of PendingGpuGaugeVec.With(<label for the local daemon on this node>)
	//IdleGpuGauge      prometheus.Gauge // IdleGpuGauge is a cached return of IdleGpuGaugeVec.With(<label for the local daemon on this node>)
	//
	//SpecCpuGauge      prometheus.Gauge // SpecCpuGauge is a cached return of SpecCpuGaugeVec.With(<label for the local daemon on this node>)
	//CommittedCpuGauge prometheus.Gauge // CommittedCpuGauge is a cached return of CommittedCpuGaugeVec.With(<label for the local daemon on this node>)
	//PendingCpuGauge   prometheus.Gauge // PendingCpuGauge is a cached return of PendingCpuGaugeVec.With(<label for the local daemon on this node>)
	//IdleCpuGauge      prometheus.Gauge // IdleCpuGauge is a cached return of IdleCpuGaugeVec.With(<label for the local daemon on this node>)
	//
	//SpecMemoryGauge      prometheus.Gauge // SpecMemoryGauge is a cached return of SpecMemoryGaugeVec.With(<label for the local daemon on this node>)
	//CommittedMemoryGauge prometheus.Gauge // CommittedMemoryGauge is a cached return of CommittedMemoryGaugeVec.With(<label for the local daemon on this node>)
	//PendingMemoryGauge   prometheus.Gauge // PendingMemoryGauge is a cached return of PendingMemoryGaugeVec.With(<label for the local daemon on this node>)
	//IdleMemoryGauge      prometheus.Gauge // IdleMemoryGauge is a cached return of IdleMemoryGaugeVec.With(<label for the local daemon on this node>)

	TrainingTimeGaugeVec *prometheus.GaugeVec // TrainingTimeGaugeVec is the total, collective time that all kernels have spent executing user-submitted code.

	// DockerContainerCreationLatencyHistogramVec is a *prometheus.HistogramVec of the latencies of creating kernel replica Containers with Docker.
	// The units of the observations recorded by DockerContainerCreationLatencyHistogramVec are milliseconds.
	DockerContainerCreationLatencyHistogramVec *prometheus.HistogramVec

	//TotalNumPrewarmContainersUsed            prometheus.Counter
	//TotalNumPrewarmContainersCreatedCounter  prometheus.Counter
	//TotalNumStandardContainersCreatedCounter prometheus.Counter
	//TotalNumKernelsCounter                   prometheus.Counter  // TotalNumKernelsCounter is a cached return of TotalNumKernelsCounterVec.With(<label for the local daemon on this node>)
	//NumTrainingEventsCompletedCounter        prometheus.Counter  // NumTrainingEventsCompletedCounter is a cached return of NumTrainingEventsCompletedCounterVec.With(<label for the local daemon on this node>)
	//NumActiveKernelReplicasGauge             prometheus.Gauge    // NumActiveKernelReplicasGauge is a cached return of NumActiveKernelReplicasGaugeVec.With(<label for the local daemon on this node>)
	//DockerContainerCreationLatencyHistogram prometheus.Observer // DockerContainerCreationLatencyHistogram is a cached return of DockerContainerCreationLatencyHistogramVec.With(<label for the local daemon on this node>)
	//AckLatencyMicrosecondsHistogram         prometheus.Observer // AckLatencyMicrosecondsHistogram is a cached return of AckLatencyMicrosecondsVec.With(<label for the local daemon on this node>)
}

// UpdateClusterStatistics isn't directly supported by LocalDaemonPrometheusManager.
func (m *LocalDaemonPrometheusManager) UpdateClusterStatistics(_ func(statistics *ClusterStatistics)) {
	// Not supported. We can just ignore it. No-op.
	return
}

// NewLocalDaemonPrometheusManager creates a new LocalDaemonPrometheusManager struct and returns a pointer to it.
func NewLocalDaemonPrometheusManager(port int, nodeId string) *LocalDaemonPrometheusManager {
	baseManager := newBasePrometheusManager(port, nodeId)
	config.InitLogger(&baseManager.log, baseManager)

	manager := &LocalDaemonPrometheusManager{
		basePrometheusManager: baseManager,
	}
	baseManager.instance = manager
	baseManager.initializeInstanceMetrics = manager.initMetrics

	return manager
}

// AddContainerCreationLatencyObservation records the latency of a container-creation event.
//
// If the target ContainerMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
// error is returned.
func (m *LocalDaemonPrometheusManager) AddContainerCreationLatencyObservation(latency time.Duration, nodeId string) error {
	if !m.metricsInitialized {
		m.log.Error("Cannot record \"NumSendAttemptsRequired\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.DockerContainerCreationLatencyHistogramVec.With(prometheus.Labels{
		"node_id": nodeId,
	}).Observe(float64(latency.Milliseconds()))

	return nil
}

// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
func (m *LocalDaemonPrometheusManager) HandleVariablesRequest(c *gin.Context) {
	m.log.Error("LocalDaemonPrometheusManager is not supposed to receive 'variables' requests.")

	_ = c.AbortWithError(http.StatusNotFound, fmt.Errorf("LocalDaemon nodes cannot serve 'variables' requests"))
}

// InitMetrics creates a Prometheus endpoint and
func (m *LocalDaemonPrometheusManager) initMetrics() error {
	// Miscellaneous metrics.

	m.TrainingTimeGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "training_time_seconds",
		Help:      "The total, collective time that all kernels have spent executing user-submitted code.",
	}, []string{"node_id", "kernel_id", "workload_id"})

	m.DockerContainerCreationLatencyHistogramVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "docker_container_creation_latency_milliseconds",
		Help:      "The latency, in milliseconds, of creating kernel replica Containers via Docker.",
		Buckets: []float64{250, 500, 1000, 2500, 5000, 7500, 10000, 12500, 15000, 17500, 20000, 30000, 45000, 60000,
			90000, 120000, 180000, 240000, 300000, 450000, 600000, 900000},
	}, []string{"node_id"})

	// Register miscellaneous metrics.
	if err := prometheus.Register(m.TrainingTimeGaugeVec); err != nil {
		m.log.Error("Failed to register 'Training Time' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.DockerContainerCreationLatencyHistogramVec); err != nil {
		m.log.Error("Failed to register 'Docker Container Creation Latency' metric because: %v", err)
		return err
	}

	// We'll be publishing these metrics with the same label every single time on this node.
	// So, we can just cache the Gauge returned when calling <GaugeVec>.With(...).
	//m.SpecGpuGauge = m.SpecGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.CommittedGpuGauge = m.CommittedGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.PendingGpuGauge = m.PendingGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.IdleGpuGauge = m.IdleGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//
	//m.SpecCpuGauge = m.SpecCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.CommittedCpuGauge = m.CommittedCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.PendingCpuGauge = m.PendingCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.IdleCpuGauge = m.IdleCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//
	//m.SpecMemoryGauge = m.SpecMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.CommittedMemoryGauge = m.CommittedMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.PendingMemoryGauge = m.PendingMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	//m.IdleMemoryGauge = m.IdleMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})

	//m.NumActiveKernelReplicasGauge = m.NumActiveKernelReplicasGaugeVec.With(
	//	prometheus.Labels{"node_id": m.nodeId, "node_type": string(LocalDaemon)})
	//m.TotalNumKernelsCounter = m.TotalNumKernelsCounterVec.
	//	With(prometheus.Labels{"node_id": m.nodeId, "node_type": string(LocalDaemon)})
	//m.TotalNumPrewarmContainersUsed = m.TotalNumPrewarmContainersUsedVec.
	//	With(prometheus.Labels{"node_id": m.nodeId})
	//m.TotalNumPrewarmContainersCreatedCounter = m.TotalNumPrewarmContainersCreatedCounterVec.
	//	With(prometheus.Labels{"node_id": m.nodeId})
	//m.TotalNumStandardContainersCreatedCounter = m.TotalNumStandardContainersCreatedCounterVec.
	//	With(prometheus.Labels{"node_id": m.nodeId})
	//m.NumTrainingEventsCompletedCounter = m.NumTrainingEventsCompletedCounterVec.
	//	With(prometheus.Labels{"node_id": m.nodeId, "node_type": string(LocalDaemon)})
	//m.DockerContainerCreationLatencyHistogram = m.DockerContainerCreationLatencyHistogramVec.With(
	//	prometheus.Labels{"node_id": m.nodeId})

	m.metricsInitialized = true
	return nil
}
