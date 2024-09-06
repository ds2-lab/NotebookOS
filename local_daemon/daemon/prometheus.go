package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	ErrPrometheusManagerAlreadyRunning = errors.New("PrometheusManager is already running")
	ErrPrometheusManagerNotRunning     = errors.New("PrometheusManager is not running")
)

// PrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
type PrometheusManager struct {
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

	SpecGpuGuage      prometheus.Gauge
	CommittedGpuGauge prometheus.Gauge
	PendingGpuGuage   prometheus.Gauge
	IdleGpuGuage      prometheus.Gauge
}

func NewPrometheusManager(port int, nodeId string) *PrometheusManager {
	manager := &PrometheusManager{
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
func (m *PrometheusManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serving {
		m.log.Warn("PrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrPrometheusManagerAlreadyRunning
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

// IsRunning returns true if the PrometheusManager has been started and is serving metrics.
func (m *PrometheusManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isRunningUnsafe()
}

// isRunningUnsafe returns true if the PrometheusManager has been started and is serving metrics.
// This does not acquire the mutex and is intended for file-internal use only.
func (m *PrometheusManager) isRunningUnsafe() bool {
	return m.serving
}

// Stop instructs the PrometheusManager to shutdown its HTTP server.
func (m *PrometheusManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunningUnsafe() /* we already have the lock */ {
		m.log.Warn("PrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrPrometheusManagerNotRunning
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
func (m *PrometheusManager) initMetrics() error {
	nodeId := strings.ReplaceAll(m.nodeId, "-", "_")

	m.IdleGpuGuage = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      fmt.Sprintf("ld_%s_idle_gpus", nodeId),
		Help:      fmt.Sprintf("Idle GPUs available on Local Daemon %s", m.nodeId),
	})

	m.SpecGpuGuage = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      fmt.Sprintf("ld_%s_spec_gpus", nodeId),
		Help:      fmt.Sprintf("Total GPUs available for use on Local Daemon %s", m.nodeId),
	})

	m.CommittedGpuGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      fmt.Sprintf("ld_%s_committed_gpus", nodeId),
		Help:      fmt.Sprintf("Allocated/committed GPUs on Local Daemon %s", m.nodeId),
	})

	m.PendingGpuGuage = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      fmt.Sprintf("ld_%s_pending_gpus", nodeId),
		Help:      fmt.Sprintf("Pending GPUs on Local Daemon %s", m.nodeId),
	})

	if err := prometheus.Register(m.IdleGpuGuage); err != nil {
		m.log.Error("Failed to register Idle GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.SpecGpuGuage); err != nil {
		m.log.Error("Failed to register Spec GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.CommittedGpuGauge); err != nil {
		m.log.Error("Failed to register Committed GPUs metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.PendingGpuGuage); err != nil {
		m.log.Error("Failed to register Pending GPUs metric because: %v", err)
		return err
	}

	m.metricsInitialized = true
	return nil
}

func (m *PrometheusManager) HandleRequest(c *gin.Context) {
	m.prometheusHandler.ServeHTTP(c.Writer, c.Request)
}

func (m *PrometheusManager) initializeHttpServer() {
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
