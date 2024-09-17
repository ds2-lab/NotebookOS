package metrics

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
)

var (
	ErrLocalDaemonPrometheusManagerAlreadyRunning = errors.New("LocalDaemonPrometheusManager is already running")
	ErrLocalDaemonPrometheusManagerNotRunning     = errors.New("LocalDaemonPrometheusManager is not running")
)

// LocalDaemonPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This to be used by Local Daemons. The Cluster Gateway uses the ClusterGatewayPrometheusManager struct.
type LocalDaemonPrometheusManager struct {
	*basePrometheusManager

	SpecGpuGaugeVec      *prometheus.GaugeVec
	CommittedGpuGaugeVec *prometheus.GaugeVec
	PendingGpuGaugeVec   *prometheus.GaugeVec
	IdleGpuGaugeVec      *prometheus.GaugeVec

	SpecCpuGaugeVec      *prometheus.GaugeVec
	CommittedCpuGaugeVec *prometheus.GaugeVec
	PendingCpuGaugeVec   *prometheus.GaugeVec
	IdleCpuGaugeVec      *prometheus.GaugeVec

	SpecMemoryGaugeVec      *prometheus.GaugeVec
	CommittedMemoryGaugeVec *prometheus.GaugeVec
	PendingMemoryGaugeVec   *prometheus.GaugeVec
	IdleMemoryGaugeVec      *prometheus.GaugeVec

	SpecGpuGauge      prometheus.Gauge // SpecGpuGauge is a cached return of SpecGpuGaugeVec.With(<label for the local daemon on this node>)
	CommittedGpuGauge prometheus.Gauge // CommittedGpuGauge is a cached return of CommittedGpuGaugeVec.With(<label for the local daemon on this node>)
	PendingGpuGauge   prometheus.Gauge // PendingGpuGauge is a cached return of PendingGpuGaugeVec.With(<label for the local daemon on this node>)
	IdleGpuGauge      prometheus.Gauge // IdleGpuGauge is a cached return of IdleGpuGaugeVec.With(<label for the local daemon on this node>)

	SpecCpuGauge      prometheus.Gauge // SpecCpuGauge is a cached return of SpecCpuGaugeVec.With(<label for the local daemon on this node>)
	CommittedCpuGauge prometheus.Gauge // CommittedCpuGauge is a cached return of CommittedCpuGaugeVec.With(<label for the local daemon on this node>)
	PendingCpuGauge   prometheus.Gauge // PendingCpuGauge is a cached return of PendingCpuGaugeVec.With(<label for the local daemon on this node>)
	IdleCpuGauge      prometheus.Gauge // IdleCpuGauge is a cached return of IdleCpuGaugeVec.With(<label for the local daemon on this node>)

	SpecMemoryGauge      prometheus.Gauge // SpecMemoryGauge is a cached return of SpecMemoryGaugeVec.With(<label for the local daemon on this node>)
	CommittedMemoryGauge prometheus.Gauge // CommittedMemoryGauge is a cached return of CommittedMemoryGaugeVec.With(<label for the local daemon on this node>)
	PendingMemoryGauge   prometheus.Gauge // PendingMemoryGauge is a cached return of PendingMemoryGaugeVec.With(<label for the local daemon on this node>)
	IdleMemoryGauge      prometheus.Gauge // IdleMemoryGauge is a cached return of IdleMemoryGaugeVec.With(<label for the local daemon on this node>)

	TrainingTimeGaugeVec *prometheus.GaugeVec // TrainingTimeGaugeVec is the total, collective time that all kernels have spent executing user-submitted code.

	NumActiveKernelReplicasGaugeVec *prometheus.GaugeVec // NumActiveKernelReplicasGaugeVec is a Prometheus Gauge Vector for how many replicas are scheduled on a particular Local Daemon.
	NumActiveKernelReplicasGauge    prometheus.Gauge     // NumActiveKernelReplicasGauge is a cached return of NumActiveKernelReplicasGaugeVec.With(<label for the local daemon on this node>)

	TotalNumKernelsCounterVec *prometheus.CounterVec
	TotalNumKernelsCounter    prometheus.Counter // TotalNumKernelsCounter is a cached return of TotalNumKernelsCounterVec.With(<label for the local daemon on this node>)

	NumTrainingEventsCompletedCounterVec *prometheus.CounterVec // NumTrainingEventsCompletedCounterVec is the number of training events that have completed successfully.
	NumTrainingEventsCompletedCounter    prometheus.Counter     // NumTrainingEventsCompletedCounter is a cached return of NumTrainingEventsCompletedCounterVec.With(<label for the local daemon on this node>)

	/////////////////////////////
	// Message latency metrics //
	/////////////////////////////

	// DaemonShellMessageLatencyVec is the end-to-end latency of Shell messages forwarded by the Local Daemon.
	// The end-to-end latency is measured from the time the message is forwarded by the Local Daemon to the time
	// at which the Gateway receives the associated response.
	DaemonShellMessageLatencyVec *prometheus.HistogramVec
	DaemonShellMessageLatency    prometheus.Observer

	// DaemonControlMessageLatencyVec is the end-to-end latency of Shell messages forwarded by the Local Daemon.
	// The end-to-end latency is measured from the time the message is forwarded by the Local Daemon to the time
	// at which the Gateway receives the associated response.
	DaemonControlMessageLatencyVec *prometheus.HistogramVec
	DaemonControlMessageLatency    prometheus.Observer
}

// NewLocalDaemonPrometheusManager creates a new LocalDaemonPrometheusManager struct and returns a pointer to it.
func NewLocalDaemonPrometheusManager(port int, nodeId string) *LocalDaemonPrometheusManager {
	baseManager := newBasePrometheusManager(port, nodeId)
	config.InitLogger(&baseManager.log, baseManager)

	manager := &LocalDaemonPrometheusManager{
		basePrometheusManager: baseManager,
	}
	baseManager.instance = manager
	baseManager.initMetrics = manager.initMetrics

	return manager
}

// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
func (m *LocalDaemonPrometheusManager) HandleVariablesRequest(c *gin.Context) {
	m.log.Error("LocalDaemonPrometheusManager is not supposed to receive 'variables' requests.")

	_ = c.AbortWithError(http.StatusNotFound, fmt.Errorf("LocalDaemon nodes cannot serve 'variables' requests"))
}

// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
//func (m *LocalDaemonPrometheusManager) Start() error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	if m.serving {
//		m.log.Warn("LocalDaemonPrometheusManager for Local Daemon %s is already running.", m.nodeId)
//		return ErrLocalDaemonPrometheusManagerAlreadyRunning
//	}
//
//	if !m.metricsInitialized {
//		err := m.initMetrics()
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

// Stop instructs the LocalDaemonPrometheusManager to shut down its HTTP server.
//func (m *LocalDaemonPrometheusManager) Stop() error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	if !m.isRunningUnsafe() /* we already have the lock */ {
//		m.log.Warn("LocalDaemonPrometheusManager for Local Daemon %s is already running.", m.nodeId)
//		return ErrLocalDaemonPrometheusManagerNotRunning
//	}
//
//	m.serving = false
//	if err := m.httpServer.Shutdown(context.Background()); err != nil {
//		m.log.Error("Failed to cleanly shutdown the HTTP server: %v", err)
//
//		// TODO: Can we safely assume that we're no longer serving at this point?
//		// We already set 'serving' to false.
//		return err
//	}
//
//	return nil
//}

// InitMetrics creates a Prometheus endpoint and
func (m *LocalDaemonPrometheusManager) initMetrics() error {
	// CPU resource metrics.
	m.IdleCpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_millicpus",
		Help:      "Idle CPUs available on a Local Daemon",
	}, []string{"node_id"})
	m.SpecCpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_millicpus",
		Help:      "Total CPUs available for use on a Local Daemon",
	}, []string{"node_id"})
	m.CommittedCpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_millicpus",
		Help:      "Allocated/committed CPUs on a Local Daemon",
	}, []string{"node_id"})
	m.PendingCpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_millicpus",
		Help:      "Pending CPUs on a Local Daemon",
	}, []string{"node_id"})

	// Memory resource metrics.
	m.IdleMemoryGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_memory_megabytes",
		Help:      "Idle memory available on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.SpecMemoryGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_memory_megabytes",
		Help:      "Total memory available for use on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.CommittedMemoryGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_memory_megabytes",
		Help:      "Allocated/committed memory on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.PendingMemoryGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_memory_megabytes",
		Help:      "Pending memory on a Local Daemon in megabytes",
	}, []string{"node_id"})

	// GPU resource metrics.
	m.IdleGpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_gpus",
		Help:      "Idle GPUs available on a Local Daemon",
	}, []string{"node_id"})
	m.SpecGpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_gpus",
		Help:      "Total GPUs available for use on a Local Daemon",
	}, []string{"node_id"})
	m.CommittedGpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_gpus",
		Help:      "Allocated/committed GPUs on a Local Daemon",
	}, []string{"node_id"})
	m.PendingGpuGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_gpus",
		Help:      "Pending GPUs on a Local Daemon",
	}, []string{"node_id"})

	// Miscellaneous metrics.
	m.NumActiveKernelReplicasGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "active_sessions",
		Help:      "Number of actively-running kernels",
	}, []string{"node_id"})
	m.TrainingTimeGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "training_time_seconds",
		Help:      "The total, collective time that all kernels have spent executing user-submitted code.",
	}, []string{"node_id", "kernel_id", "workload_id"})
	m.TotalNumKernelsCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "sessions_total",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})
	m.NumTrainingEventsCompletedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "training_events_completed_total",
		Help:      "The number of training events that have completed successfully",
	}, []string{"node_id"})

	// Message latency metrics.
	m.DaemonShellMessageLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "shell_message_latency_milliseconds",
		Help:      "End-to-end latency of Shell messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
	}, []string{"node_id"})

	m.DaemonControlMessageLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "control_message_latency_milliseconds",
		Help:      "End-to-end latency of Control messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
	}, []string{"node_id"})

	// Register GPU resource metrics.
	if err := prometheus.Register(m.IdleGpuGaugeVec); err != nil {
		m.log.Error("Failed to register Idle GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.SpecGpuGaugeVec); err != nil {
		m.log.Error("Failed to register Spec GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.CommittedGpuGaugeVec); err != nil {
		m.log.Error("Failed to register Committed GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.PendingGpuGaugeVec); err != nil {
		m.log.Error("Failed to register Pending GPUs metric because: %v", err)
		return err
	}

	// Register CPU resource metrics.
	if err := prometheus.Register(m.IdleCpuGaugeVec); err != nil {
		m.log.Error("Failed to register Idle GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.SpecCpuGaugeVec); err != nil {
		m.log.Error("Failed to register Spec GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.CommittedCpuGaugeVec); err != nil {
		m.log.Error("Failed to register Committed GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.PendingCpuGaugeVec); err != nil {
		m.log.Error("Failed to register Pending GPUs metric because: %v", err)
		return err
	}

	// Register memory resource metrics.
	if err := prometheus.Register(m.IdleMemoryGaugeVec); err != nil {
		m.log.Error("Failed to register Idle GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.SpecMemoryGaugeVec); err != nil {
		m.log.Error("Failed to register Spec GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.CommittedMemoryGaugeVec); err != nil {
		m.log.Error("Failed to register Committed GPUs metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.PendingMemoryGaugeVec); err != nil {
		m.log.Error("Failed to register Pending GPUs metric because: %v", err)
		return err
	}

	// Register miscellaneous metrics.
	if err := prometheus.Register(m.NumActiveKernelReplicasGaugeVec); err != nil {
		m.log.Error("Failed to register 'Number of Active Kernel Replicas' metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.TrainingTimeGaugeVec); err != nil {
		m.log.Error("Failed to register 'Training Time' metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.TotalNumKernelsCounterVec); err != nil {
		m.log.Error("Failed to register 'Total Number of Kernels' metric because: %v", err)
		return err
	}
	if err := prometheus.Register(m.NumTrainingEventsCompletedCounterVec); err != nil {
		m.log.Error("Failed to register 'Training Events Completed' metric because: %v", err)
		return err
	}

	// Message latency metrics.
	if err := prometheus.Register(m.NumTrainingEventsCompletedCounterVec); err != nil {
		m.log.Error("Failed to register 'Daemon Shell Message Latency' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.DaemonControlMessageLatencyVec); err != nil {
		m.log.Error("Failed to register 'Daemon Control Message Latency' metric because: %v", err)
		return err
	}

	// We'll be publishing these metrics with the same label every single time on this node.
	// So, we can just cache the Gauge returned when calling <GaugeVec>.With(...).
	m.SpecGpuGauge = m.SpecGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.CommittedGpuGauge = m.CommittedGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.PendingGpuGauge = m.PendingGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.IdleGpuGauge = m.IdleGpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})

	m.SpecCpuGauge = m.SpecCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.CommittedCpuGauge = m.CommittedCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.PendingCpuGauge = m.PendingCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.IdleCpuGauge = m.IdleCpuGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})

	m.SpecMemoryGauge = m.SpecMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.CommittedMemoryGauge = m.CommittedMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.PendingMemoryGauge = m.PendingMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.IdleMemoryGauge = m.IdleMemoryGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})

	m.NumActiveKernelReplicasGauge = m.NumActiveKernelReplicasGaugeVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.TotalNumKernelsCounter = m.TotalNumKernelsCounterVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.NumTrainingEventsCompletedCounter = m.NumTrainingEventsCompletedCounterVec.With(prometheus.Labels{"node_id": m.nodeId})

	m.DaemonShellMessageLatency = m.DaemonShellMessageLatencyVec.With(prometheus.Labels{"node_id": m.nodeId})
	m.DaemonControlMessageLatency = m.DaemonControlMessageLatencyVec.With(prometheus.Labels{"node_id": m.nodeId})

	m.metricsInitialized = true
	return nil
}
