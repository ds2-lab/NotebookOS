package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"sync"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

var (
	ErrGatewayPrometheusManagerAlreadyRunning = errors.New("GatewayPrometheusManager is already running")
	ErrGatewayPrometheusManagerNotRunning     = errors.New("GatewayPrometheusManager is not running")
)

// PrometheusManager defines the general interface of Prometheus metric managers.
type PrometheusManager interface {
	HandleRequest(*gin.Context)

	// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
	HandleVariablesRequest(*gin.Context)

	// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
	Start() error

	// NodeId returns the node ID associated with the metrics manager.
	NodeId() string

	// IsRunning returns true if the PrometheusManager has been started and is serving metrics.
	IsRunning() bool

	// Stop instructs the PrometheusManager to shut down its HTTP server.
	Stop() error
}

// BasePrometheusManager contains the common state and infrastructure required
// by both the GatewayPrometheusManager and the DaemonPrometheusManager.
type basePrometheusManager struct {
	log logger.Logger

	instance PrometheusManager

	// serving indicates whether the manager has been started and is serving requests.
	serving            bool
	metricsInitialized bool
	mu                 sync.Mutex
	engine             *gin.Engine
	httpServer         *http.Server
	prometheusHandler  http.Handler
	port               int
	nodeId             string

	// initializeInstanceMetrics is a field that is to be assigned by "child" structs in their "constructors".
	// Specifically, this function is assigned by the 'instance' to initialize the instance's metrics.
	initializeInstanceMetrics func() error

	NumActiveKernelReplicasGaugeVec *prometheus.GaugeVec // NumActiveKernelReplicasGaugeVec is a Prometheus Gauge Vector for how many replicas are scheduled on a particular Local Daemon.

	TotalNumKernelsCounterVec            *prometheus.CounterVec
	NumTrainingEventsCompletedCounterVec *prometheus.CounterVec // NumTrainingEventsCompletedCounterVec is the number of training events that have completed successfully.

	/////////////////////////////
	// Message latency metrics //
	/////////////////////////////

	// ShellMessageLatencyVec is the end-to-end latency of Shell messages forwarded by the Local Daemon.
	// The end-to-end latency is measured from the time the message is forwarded by the Local Daemon to the time
	// at which the Gateway receives the associated response.
	ShellMessageLatencyVec *prometheus.HistogramVec

	// ControlMessageLatencyVec is the end-to-end latency of Shell messages forwarded by the Local Daemon.
	// The end-to-end latency is measured from the time the message is forwarded by the Local Daemon to the time
	// at which the Gateway receives the associated response.
	ControlMessageLatencyVec *prometheus.HistogramVec
}

// newBasePrometheusManager creates a new basePrometheusManager and returns a pointer to it.
func newBasePrometheusManager(port int, nodeId string) *basePrometheusManager {
	manager := &basePrometheusManager{
		port:              port,
		prometheusHandler: promhttp.Handler(),
		nodeId:            nodeId,
		serving:           false,
	}
	config.InitLogger(&manager.log, manager)
	return manager
}

// isRunningUnsafe returns true if the basePrometheusManager has been started and is serving metrics.
// This does not acquire the mutex and is intended for file-internal use only.
func (m *basePrometheusManager) isRunningUnsafe() bool {
	return m.serving
}

// IsRunning returns true if the basePrometheusManager has been started and is serving metrics.
func (m *basePrometheusManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isRunningUnsafe()
}

// NodeId returns the node ID associated with the metrics manager.
func (m *basePrometheusManager) NodeId() string {
	return m.nodeId
}

// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
func (m *basePrometheusManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serving {
		m.log.Warn("GatewayPrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrGatewayPrometheusManagerAlreadyRunning
	}

	m.serving = true
	if !m.metricsInitialized {
		err := m.initializeMetrics()
		if err != nil {
			return err
		}
	}
	m.initializeHttpServer()

	return nil
}

// Stop instructs the PrometheusManager to shut down its HTTP server.
func (m *basePrometheusManager) Stop() error {
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

// HandleRequest handles Prometheus HTTP requests (when Prometheus is scraping for metrics).
func (m *basePrometheusManager) HandleRequest(c *gin.Context) {
	m.prometheusHandler.ServeHTTP(c.Writer, c.Request)
}

func (m *basePrometheusManager) initializeHttpServer() {
	m.engine = gin.New()

	m.engine.Use(gin.Logger())
	m.engine.Use(cors.Default())

	m.engine.GET("/variables/:variable_name", m.HandleVariablesRequest)
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

// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
func (m *basePrometheusManager) HandleVariablesRequest(c *gin.Context) {
	m.instance.HandleVariablesRequest(c)
}

func (m *basePrometheusManager) initializeMetrics() error {
	if m.initializeInstanceMetrics == nil {
		panic("Base Prometheus Manager's `initializeInstanceMetrics` field cannot be nil when initializing metrics.")
	}

	// Miscellaneous metrics.

	m.NumActiveKernelReplicasGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "active_sessions",
		Help:      "Number of actively-running kernels",
	}, []string{"node_id"})
	m.NumTrainingEventsCompletedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "training_events_completed_total",
		Help:      "The number of training events that have completed successfully",
	}, []string{"node_id"})
	m.TotalNumKernelsCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "sessions_total",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})

	// Create/define message latency metrics.

	m.ShellMessageLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "shell_message_latency_milliseconds",
		Help:      "End-to-end latency of Shell messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
	}, []string{"node_id"})

	m.ControlMessageLatencyVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "control_message_latency_milliseconds",
		Help:      "End-to-end latency of Control messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
	}, []string{"node_id"})

	if err := prometheus.Register(m.NumTrainingEventsCompletedCounterVec); err != nil {
		m.log.Error("Failed to register 'Training Events Completed' metric because: %v", err)
		return err
	}

	// Register message latency metrics.

	if err := prometheus.Register(m.ShellMessageLatencyVec); err != nil {
		m.log.Error("Failed to register 'Gateway Shell Message Latency' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.ControlMessageLatencyVec); err != nil {
		m.log.Error("Failed to register 'Gateway Control Message Latency' metric because: %v", err)
		return err
	}
	
	// Register miscellaneous metrics.

	if err := prometheus.Register(m.NumActiveKernelReplicasGaugeVec); err != nil {
		m.log.Error("Failed to register 'Number of Active Kernel Replicas' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.TotalNumKernelsCounterVec); err != nil {
		m.log.Error("Failed to register 'Total Number of Kernels' metric because: %v", err)
		return err
	}

	return m.initializeInstanceMetrics()
}
