package metrics

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"net/http"
)

type LocalDaemonNodeProvider interface {
	// GetLocalDaemonNodeIDs returns the IDs of the active Local Daemon nodes.
	GetLocalDaemonNodeIDs(_ context.Context, _ *proto.Void) (*proto.GetLocalDaemonNodeIDsResponse, error)

	// GetId returns the node ID of the entity providing the local daemon nodes.
	GetId() string
}

// GatewayPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This is to be used by the Cluster Gateway. Local Daemons use the LocalDaemonPrometheusManager struct.
type GatewayPrometheusManager struct {
	*basePrometheusManager

	// JupyterTrainingStartLatency is a metric tracking the latency, in milliseconds, between when an
	// "execute_request" message is sent and when the first "execute_reply" is received.
	//
	// The latency is observed from the Golang-based Jupyter client, and the units
	// of the metric are seconds.
	JupyterTrainingStartLatency *prometheus.HistogramVec

	// NumSuccessfulMigrations keeps track of the number of times we successfully migrated a kernel from
	// one node to another.
	NumSuccessfulMigrations prometheus.Counter

	// NumFailedMigrations keeps track of the number of times we failed to migrate a kernel from one node
	// to another for any reason.
	NumFailedMigrations prometheus.Counter

	// KernelMigrationLatencyHistogram records the latencies of migrating kernel replicas from one node to another.
	KernelMigrationLatencyHistogram prometheus.Histogram

	// KernelCreationLatencyHistogram records the latency of creating a new kernel from the perspective of
	// the Cluster Gateway. There are separate metrics for tracking how long it takes to create new sessions
	// from the perspective of Jupyter Clients.
	KernelCreationLatencyHistogram prometheus.Histogram

	localDaemonNodeProvider LocalDaemonNodeProvider
}

func NewGatewayPrometheusManager(port int, localDaemonNodeProvider LocalDaemonNodeProvider) *GatewayPrometheusManager {
	baseManager := newBasePrometheusManager(port, localDaemonNodeProvider.GetId())
	config.InitLogger(&baseManager.log, baseManager)

	manager := &GatewayPrometheusManager{
		basePrometheusManager:   baseManager,
		localDaemonNodeProvider: localDaemonNodeProvider,
	}
	baseManager.instance = manager
	baseManager.initializeInstanceMetrics = manager.initMetrics

	return manager
}

// InitMetrics creates a Prometheus endpoint and
func (m *GatewayPrometheusManager) initMetrics() error {
	m.JupyterTrainingStartLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Subsystem: "jupyter",
		Name:      "session_training_start_latency_milliseconds",
		Buckets: []float64{1, 5, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300, 400, 500, 750, 1e3, 1.5e3, 2e3, 3e3, 4e3,
			5e3, 7.5e3, 1e4, 1.5e4, 2e4, 3e4, 4.5e4, 6e4, 9e4, 1.2e5},
	}, []string{"workload_id"})

	m.KernelMigrationLatencyHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "kernel_migration_latency_milliseconds",
		Help:      "The latency of migrating kernel replicas from one node to another.",
		Buckets: []float64{10, 1e3, 2e3, 3e3, 4e3, 5e3, 6e3, 7e3, 8e3, 9e3, 1e4, 1.5e4, 2e4, 2.5e4, 3e4, 4.5e4, 6e4,
			9e4, 1.2e5, 1.8e5, 2.4e5, 3e5},
	})

	m.KernelCreationLatencyHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "gateway_kernel_creation_latency_milliseconds",
		Help:      "The latency of creating a new kernel from the perspective of the Cluster Gateway.",
		Buckets: []float64{10, 1e3, 2e3, 3e3, 4e3, 5e3, 6e3, 7e3, 8e3, 9e3, 1e4, 1.5e4, 2e4, 3e4, 4.5e4, 6e4, 9e4,
			1.2e5, 1.8e5, 2.4e5, 3e5},
	})

	m.NumSuccessfulMigrations = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "successful_migrations_total",
		Help:      "The total number of times we've successfully migrated a kernel.",
	})

	m.NumFailedMigrations = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "failed_migrations_total",
		Help:      "The total number of times we've failed to migrate a kernel for any reason.",
	})

	if err := prometheus.Register(m.JupyterTrainingStartLatency); err != nil {
		m.log.Error("Failed to register 'Jupyter Training Start Latency' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.KernelMigrationLatencyHistogram); err != nil {
		m.log.Error("Failed to register 'Kernel Migration Latency Histogram' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.KernelCreationLatencyHistogram); err != nil {
		m.log.Error("Failed to register 'Kernel Creation Latency Histogram' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumSuccessfulMigrations); err != nil {
		m.log.Error("Failed to register 'Num Successful Migrations' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumFailedMigrations); err != nil {
		m.log.Error("Failed to register 'Num Failed Migrations' metric because: %v", err)
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
			resp, err := m.localDaemonNodeProvider.GetLocalDaemonNodeIDs(context.Background(), &proto.Void{})
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
			resp, err := m.localDaemonNodeProvider.GetLocalDaemonNodeIDs(context.Background(), &proto.Void{})
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

//func (m *GatewayPrometheusManager) HandleRequest(c *gin.Context) {
//	m.prometheusHandler.ServeHTTP(c.Writer, c.Request)
//}
//
//func (m *GatewayPrometheusManager) initializeHttpServer() {
//	m.engine = gin.New()
//
//	m.engine.Use(gin.Logger())
//	m.engine.Use(cors.Default())
//
//	m.engine.GET("/variables/:variable_name", m.HandleVariablesRequest)
//	m.engine.GET("/prometheus", m.HandleRequest)
//
//	address := fmt.Sprintf("0.0.0.0:%d", m.port)
//	m.httpServer = &http.Server{
//		Addr:    address,
//		Handler: m.engine,
//	}
//
//	go func() {
//		m.log.Debug("Serving Prometheus metrics at %s", address)
//		if err := m.httpServer.ListenAndServe(); err != nil {
//			m.log.Error(utils.RedStyle.Render("HTTP Server failed to listen on '%s'. Error: %v"), address, err)
//			panic(err)
//		}
//	}()
//}
