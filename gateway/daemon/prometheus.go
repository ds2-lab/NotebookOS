package daemon

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"net/http"
	"sync"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

var (
	ErrGatewayPrometheusManagerAlreadyRunning = errors.New("GatewayPrometheusManager is already running")
	ErrGatewayPrometheusManagerNotRunning     = errors.New("GatewayPrometheusManager is not running")
)

// GatewayPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This is to be used by the Cluster Gateway. Local Daemons use the LocalDaemonPrometheusManager struct.
type GatewayPrometheusManager struct {
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

	// NumActiveKernelReplicasGauge is the number of actively-running kernels.
	NumActiveKernelReplicasGauge *prometheus.GaugeVec

	// TotalNumKernels is the total number of kernels that have been created, including kernels that have since stopped.
	TotalNumKernels *prometheus.CounterVec

	// NumTrainingEventsCompleted is the number of training events that have completed successfully.
	NumTrainingEventsCompleted *prometheus.CounterVec

	// JupyterTrainingStartLatency is a metric tracking the latency between when an
	// "execute_request" message is sent and when the first "execute_reply" is received.
	//
	// The latency is observed from the Golang-based Jupyter client, and the units
	// of the metric are seconds.
	JupyterTrainingStartLatency *prometheus.HistogramVec

	gatewayDaemon *ClusterGatewayImpl
}

func NewGatewayPrometheusManager(port int, gatewayDaemon *ClusterGatewayImpl) *GatewayPrometheusManager {
	manager := &GatewayPrometheusManager{
		port:              port,
		prometheusHandler: promhttp.Handler(),
		nodeId:            gatewayDaemon.id,
		gatewayDaemon:     gatewayDaemon,
		serving:           false,
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
func (m *GatewayPrometheusManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serving {
		m.log.Warn("GatewayPrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrGatewayPrometheusManagerAlreadyRunning
	}

	m.serving = true
	if !m.metricsInitialized {
		err := m.initMetrics()
		if err != nil {
			return err
		}
	}
	m.initializeHttpServer()

	return nil
}

// IsRunning returns true if the GatewayPrometheusManager has been started and is serving metrics.
func (m *GatewayPrometheusManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isRunningUnsafe()
}

// isRunningUnsafe returns true if the GatewayPrometheusManager has been started and is serving metrics.
// This does not acquire the mutex and is intended for file-internal use only.
func (m *GatewayPrometheusManager) isRunningUnsafe() bool {
	return m.serving
}

// Stop instructs the GatewayPrometheusManager to shut down its HTTP server.
func (m *GatewayPrometheusManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunningUnsafe() /* we already have the lock */ {
		m.log.Warn("GatewayPrometheusManager for Local Daemon %s is already running.", m.nodeId)
		return ErrGatewayPrometheusManagerNotRunning
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
func (m *GatewayPrometheusManager) initMetrics() error {
	m.NumActiveKernelReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "active_sessions",
		Help:      "Number of actively-running kernels",
	}, []string{"node_id"})

	m.TotalNumKernels = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "sessions_total",
		Help:      "Total number of kernels to have ever been created within the cluster",
	}, []string{"node_id"})

	m.NumTrainingEventsCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "training_events_completed_total",
		Help:      "The number of training events that have completed successfully",
	}, []string{"node_id"})

	m.JupyterTrainingStartLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Subsystem: "jupyter",
		Name:      "session_training_start_latency_seconds",
	}, []string{"workload_id"})

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

	if err := prometheus.Register(m.JupyterTrainingStartLatency); err != nil {
		m.log.Error("Failed to register 'Jupyter Training Start Latency' metric because: %v", err)
		return err
	}

	m.metricsInitialized = true
	return nil
}

// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
func (m *GatewayPrometheusManager) HandleVariablesRequest(c *gin.Context) {
	variable := c.Param("variable_name")
	m.log.Debug("Received query for variable: \"%s\"", variable)

	response := make(map[string]interface{})
	switch variable {
	case "num_nodes":
		{
			// Call the GetLocalDaemonNodeIDs gRPC handler directly. This is a local call.
			resp, err := m.gatewayDaemon.GetLocalDaemonNodeIDs(context.Background(), &proto.Void{})
			if err != nil {
				m.log.Error("Failed to retrieve Local Daemon IDs because: %v", err)
				_ = c.AbortWithError(http.StatusInternalServerError, err)
				return
			}
			response["num_nodes"] = len(resp.HostIds)
			m.log.Debug("Returning number of nodes: %d", len(resp.HostIds))
			break
		}
	case "local_daemon_ids":
		{
			// Call the GetLocalDaemonNodeIDs gRPC handler directly. This is a local call.
			resp, err := m.gatewayDaemon.GetLocalDaemonNodeIDs(context.Background(), &proto.Void{})
			if err != nil {
				m.log.Error("Failed to retrieve Local Daemon IDs because: %v", err)
				_ = c.AbortWithError(http.StatusInternalServerError, err)
				return
			}
			response["local_daemon_ids"] = resp.HostIds
			m.log.Debug("Returning Local Daemon host IDs: %v", resp.HostIds)
			break
		}
	case "default":
		{
			m.log.Error("Received variable query for unknown variable \"%s\".", variable)
			_ = c.AbortWithError(http.StatusBadRequest, fmt.Errorf("unknown or unsupported variable: \"%s\"", variable))
			return
		}
	}

	c.JSON(http.StatusOK, response)
}

func (m *GatewayPrometheusManager) HandleRequest(c *gin.Context) {
	m.prometheusHandler.ServeHTTP(c.Writer, c.Request)
}

func (m *GatewayPrometheusManager) initializeHttpServer() {
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
