package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/logger"
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

	// initMetrics is a field that is to be assigned by "child" structs in their "constructors".
	initMetrics func() error
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
		err := m.initMetrics()
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
