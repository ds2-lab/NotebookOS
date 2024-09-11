package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

var (
	ErrLocalDaemonPrometheusManagerAlreadyRunning = errors.New("LocalDaemonPrometheusManager is already running")
	ErrLocalDaemonPrometheusManagerNotRunning     = errors.New("LocalDaemonPrometheusManager is not running")
)

// LocalDaemonPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This to be used by Local Daemons. The Cluster Gateway uses the ClusterGatewayPrometheusManager struct.
type LocalDaemonPrometheusManager struct {
	log    logger.Logger
	nodeId string

	// serving indicates whether the manager has been started and is serving requests.
	serving            bool
	metricsInitialized bool
	mu                 sync.Mutex
	port               int
	engine             *gin.Engine
	httpServer         *http.Server
	prometheusHandler  http.Handler

	SpecGpuGauge      *prometheus.GaugeVec
	CommittedGpuGauge *prometheus.GaugeVec
	PendingGpuGauge   *prometheus.GaugeVec
	IdleGpuGauge      *prometheus.GaugeVec

	SpecCpuGauge      *prometheus.GaugeVec
	CommittedCpuGauge *prometheus.GaugeVec
	PendingCpuGauge   *prometheus.GaugeVec
	IdleCpuGauge      *prometheus.GaugeVec

	SpecMemoryGauge      *prometheus.GaugeVec
	CommittedMemoryGauge *prometheus.GaugeVec
	PendingMemoryGauge   *prometheus.GaugeVec
	IdleMemoryGauge      *prometheus.GaugeVec

	// NumActiveKernelReplicasGauge is a Prometheus Gauge Vector for how many replicas are scheduled on a particular Local Daemon.
	NumActiveKernelReplicasGauge *prometheus.GaugeVec
	TotalNumKernels              *prometheus.CounterVec
	// NumTrainingEventsCompleted is the number of training events that have completed successfully.
	NumTrainingEventsCompleted *prometheus.CounterVec
}

func NewLocalDaemonPrometheusManager(port int, nodeId string) *LocalDaemonPrometheusManager {
	manager := &LocalDaemonPrometheusManager{
		port:              port,
		prometheusHandler: promhttp.Handler(),
		nodeId:            nodeId,
	}
	config.InitLogger(&manager.log, manager)
	return manager
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func NewResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
func (m *LocalDaemonPrometheusManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serving {
		m.log.Warn("LocalDaemonPrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrLocalDaemonPrometheusManagerAlreadyRunning
	}

	if !m.metricsInitialized {
		err := m.initMetrics()
		if err != nil {
			return err
		}
	}
	m.initializeHttpServer()

	return nil
}

// IsRunning returns true if the LocalDaemonPrometheusManager has been started and is serving metrics.
func (m *LocalDaemonPrometheusManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isRunningUnsafe()
}

// isRunningUnsafe returns true if the LocalDaemonPrometheusManager has been started and is serving metrics.
// This does not acquire the mutex and is intended for file-internal use only.
func (m *LocalDaemonPrometheusManager) isRunningUnsafe() bool {
	return m.serving
}

// Stop instructs the LocalDaemonPrometheusManager to shut down its HTTP server.
func (m *LocalDaemonPrometheusManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunningUnsafe() /* we already have the lock */ {
		m.log.Warn("LocalDaemonPrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrLocalDaemonPrometheusManagerNotRunning
	}

	m.serving = false
	if err := m.httpServer.Shutdown(context.Background()); err != nil {
		m.log.Error("Failed to cleanly shutdown the HTTP server: %v", err)

		// TODO: Can we safely assume that we're no longer serving at this point?
		// We already set 'serving' to false.
		return err
	}

	return nil
}

// InitMetrics creates a Prometheus endpoint and
func (m *LocalDaemonPrometheusManager) initMetrics() error {
	// CPU resource metrics.
	m.IdleCpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_millicpus_total",
		Help:      "Idle CPUs available on a Local Daemon",
	}, []string{"node_id"})
	m.SpecCpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_millicpus",
		Help:      "Total CPUs available for use on a Local Daemon",
	}, []string{"node_id"})
	m.CommittedCpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_millicpus",
		Help:      "Allocated/committed CPUs on a Local Daemon",
	}, []string{"node_id"})
	m.PendingCpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_millicpus",
		Help:      "Pending CPUs on a Local Daemon",
	}, []string{"node_id"})

	// Memory resource metrics.
	m.IdleMemoryGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_memory_megabytes",
		Help:      "Idle memory available on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.SpecMemoryGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_memory_megabytes",
		Help:      "Total memory available for use on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.CommittedMemoryGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_memory_megabytes",
		Help:      "Allocated/committed memory on a Local Daemon in megabytes",
	}, []string{"node_id"})
	m.PendingMemoryGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_memory_megabytes",
		Help:      "Pending memory on a Local Daemon in megabytes",
	}, []string{"node_id"})

	// GPU resource metrics.
	m.IdleGpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "idle_gpus",
		Help:      "Idle GPUs available on a Local Daemon",
	}, []string{"node_id"})
	m.SpecGpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "spec_gpus",
		Help:      "Total GPUs available for use on a Local Daemon",
	}, []string{"node_id"})
	m.CommittedGpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "committed_gpus",
		Help:      "Allocated/committed GPUs on a Local Daemon",
	}, []string{"node_id"})
	m.PendingGpuGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "pending_gpus",
		Help:      "Pending GPUs on a Local Daemon",
	}, []string{"node_id"})

	// Miscellaneous metrics.
	m.NumActiveKernelReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "num_active_kernels",
		Help:      "Number of kernel replicas scheduled on a Local Daemon",
	}, []string{"node_id"})
	m.TotalNumKernels = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "num_kernels_total",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})
	m.NumTrainingEventsCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "num_training_events_completed_total",
		Help:      "The number of training events that have completed successfully",
	}, []string{"node_id"})

	if err := prometheus.Register(m.IdleGpuGauge); err != nil {
		m.log.Error("Failed to register Idle GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.SpecGpuGauge); err != nil {
		m.log.Error("Failed to register Spec GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.CommittedGpuGauge); err != nil {
		m.log.Error("Failed to register Committed GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.PendingGpuGauge); err != nil {
		m.log.Error("Failed to register Pending GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumActiveKernelReplicasGauge); err != nil {
		m.log.Error("Failed to register 'Number of Active Kernel Replicas' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.TotalNumKernels); err != nil {
		m.log.Error("Failed to register 'Total Number of Kernels' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumTrainingEventsCompleted); err != nil {
		m.log.Error("Failed to register 'Training Events Completed' metric because: %v", err)
		return err
	}

	m.metricsInitialized = true
	return nil
}

func (m *LocalDaemonPrometheusManager) HandleRequest(c *gin.Context) {
	m.prometheusHandler.ServeHTTP(c.Writer, c.Request)
}

func (m *LocalDaemonPrometheusManager) initializeHttpServer() {
	m.engine = gin.New()

	m.engine.Use(gin.Logger())
	m.engine.Use(cors.Default())

	m.engine.GET("/prometheus", m.HandleRequest)

	address := fmt.Sprintf("0.0.0.0:%d", m.port)
	m.httpServer = &http.Server{
		Addr:    address,
		Handler: m.engine,
	}

	go func() {
		m.log.Debug("Serving Prometheus metrics at %s", address)
		if err := m.httpServer.ListenAndServe(); err != nil {
			m.log.Error(utils.RedStyle.Render("HTTP Server failed to listen on '%s'. Error: %v"), address, err)
			panic(err)
		}
	}()
}
