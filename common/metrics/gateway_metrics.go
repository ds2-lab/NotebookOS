package metrics

import (
	"context"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/gin-gonic/gin"
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

// ClusterMetricsProvider provides access to Cluster-related Prometheus metrics.
type ClusterMetricsProvider interface {
	GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram
	GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram
	GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec
	GetNumDisabledHostsGauge() prometheus.Gauge
	GetNumHostsGauge() prometheus.Gauge
	GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram
}

// GatewayPrometheusManager is responsible for registering metrics with Prometheus and serving them via HTTP.
// This is to be used by the Cluster Gateway. Local Daemons use the LocalDaemonPrometheusManager struct.
type GatewayPrometheusManager struct {
	*basePrometheusManager

	localDaemonNodeProvider LocalDaemonNodeProvider

	// JupyterTrainingStartLatency is a metric tracking the latency, in milliseconds, between when an
	// "execute_request" message is sent and when the first "execute_reply" is received.
	//
	// The latency is observed from the Golang-based Jupyter client, and the units
	// of the metric are seconds.
	JupyterTrainingStartLatency *prometheus.HistogramVec

	// HostRemoteSyncLatencyMicrosecondsHistogram is a Histogram of the latencies for Hosts to synchronize
	// their local view of their resources with the "ground truth" resource values on their remote host.
	HostRemoteSyncLatencyMicrosecondsHistogram prometheus.Histogram

	//////////////////////////
	// Node Scaling Metrics //
	//////////////////////////

	// ScaleOutLatencyMillisecondsHistogram is a prometheus.Histogram of the latency, in milliseconds, of scaling-out
	// (i.e., increasing the number of nodes available within the cluster).
	ScaleOutLatencyMillisecondsHistogram prometheus.Histogram

	// ScaleInLatencyMillisecondsHistogram is a prometheus.Histogram of the latency, in milliseconds, of scaling-in
	// (i.e., decreasing the number of nodes available within the cluster).
	ScaleInLatencyMillisecondsHistogram prometheus.Histogram

	///////////////////////////////////////
	// Kernel Replica Scheduling Metrics //
	///////////////////////////////////////

	// KernelCreationLatencyHistogram records the latency of creating a new kernel from the perspective of
	// the Cluster Gateway. There are separate metrics for tracking how long it takes to create new sessions
	// from the perspective of Jupyter Clients.
	KernelCreationLatencyHistogram prometheus.Histogram

	// PlacerFindHostLatencyMicrosecondsHistogramVec tracks the latency of each call to a scheduling.Placer's FindHosts method.
	PlacerFindHostLatencyMicrosecondsHistogramVec *prometheus.HistogramVec

	// ClusterSubscriptionRatioGauge is a gauge of the subscription ratio metric of the Cluster.
	ClusterSubscriptionRatioGauge prometheus.Gauge

	// DemandGpusGauge is a prometheus.Gauge metric that tracks the total GPU demand within the cluster.
	// The "demand" is the number of GPUs that are required by all actively-running Sessions, if all
	// GPUs were to be committed at the same time.
	DemandGpusGauge prometheus.Gauge

	// BusyGpusGauge is a prometheus.Gauge metric that tracks the total number of "busy" GPUs
	// within the entire cluster, where "busy" GPUs are those actively committed to kernel replicas.
	BusyGpusGauge prometheus.Gauge

	// NumHostsGauge is a prometheus.Gauge metric that tracks the total number of active/enabled hosts provisioned
	// within the Cluster.
	NumHostsGauge prometheus.Gauge

	// NumDisabledHostsGauge is a prometheus.Gauge metric that tracks the total number of disabled hosts within
	// the Cluster.
	NumDisabledHostsGauge prometheus.Gauge

	//////////////////////////////
	// Kernel Migration Metrics //
	//////////////////////////////

	// NumSuccessfulMigrations keeps track of the number of times we successfully migrated a kernel from
	// one node to another.
	NumSuccessfulMigrations prometheus.Counter

	// NumFailedMigrations keeps track of the number of times we failed to migrate a kernel from one node
	// to another for any reason.
	NumFailedMigrations prometheus.Counter

	// KernelMigrationLatencyHistogram records the latencies of migrating kernel replicas from one node to another.
	KernelMigrationLatencyHistogram prometheus.Histogram
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
		Buckets:   []float64{50, 100, 200, 300, 400, 500, 750, 1e3, 2.5e3, 5e3, 10e3, 30e3, 60e3, 120e3, 300e3},
	}, []string{"workload_id", "kernel_id"})

	m.KernelMigrationLatencyHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "kernel_migration_latency_milliseconds",
		Help:      "The latency of migrating kernel replicas from one node to another.",
		Buckets:   []float64{100, 500, 1e3, 5e3, 10e3, 30e3, 45e3, 60e3, 120e3, 300e3},
	})

	m.KernelCreationLatencyHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "gateway_kernel_creation_latency_milliseconds",
		Help:      "The latency of creating a new kernel from the perspective of the Cluster Gateway.",
		Buckets:   []float64{100, 500, 1e3, 5e3, 10e3, 30e3, 45e3, 60e3, 120e3, 300e3},
	})

	m.PlacerFindHostLatencyMicrosecondsHistogramVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "placer_find_host_latency_microseconds",
		Help:      "The latency, in microseconds, of finding candidate hosts when scheduling a kernel for the first time.",
		Buckets:   []float64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1e3},
	}, []string{"successful"})

	m.HostRemoteSyncLatencyMicrosecondsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "host_remote_sync_latency_microseconds",
		Help:      "The latency, in microseconds, of finding candidate hosts when scheduling a kernel for the first time.",
		Buckets:   []float64{1e3, 10e3, 50e3, 100e3, 250e3, 500e3, 1e6, 2.5e6, 5e6, 10e6},
	})

	m.ClusterSubscriptionRatioGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "cluster_subscription_ratio",
		Help:      "The subscription ratio, which is the ratio of demand to committed GPUs within the cluster.",
	})

	m.DemandGpusGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "cluster_demand_gpus_total",
		Help:      "The total GPU demand within the cluster (i.e., the total number of GPUs required by all actively-running sessions.",
	})

	m.BusyGpusGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "cluster_busy_gpus_total",
		Help:      "The total number of GPUs that are actively committed to training kernel replicas within the Cluster.",
	})

	m.NumHostsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "hosts_total",
		Help:      "The total number of active/enabled hosts provisioned within the Cluster.",
	})

	m.NumDisabledHostsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distributed_cluster",
		Name:      "disabled_hosts_total",
		Help:      "The total number of disabled hosts provisioned within the Cluster.",
	})

	m.ScaleOutLatencyMillisecondsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "scale_out_latency_milliseconds",
		Help:      "The latency of scaling-out (i.e., increasing the number of nodes available within the cluster).",
		Buckets: []float64{1000, 5000, 10000, 15000, 20000, 30000, 45000, 60000, 90000, 120000, 180000, 240000,
			300000, 450000, 600000, 900000, 1200000, 1800000, 2400000, 3000000},
	})

	m.ScaleInLatencyMillisecondsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "scale_out_in_milliseconds",
		Help:      "The latency of scaling-in (i.e., decreasing the number of nodes available within the cluster).",
		Buckets: []float64{1000, 5000, 10000, 15000, 20000, 30000, 45000, 60000, 90000, 120000, 180000, 240000,
			300000, 450000, 600000, 900000, 1200000},
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

	if err := prometheus.Register(m.HostRemoteSyncLatencyMicrosecondsHistogram); err != nil {
		m.log.Error("Failed to register 'Host Remote Sync Latency Microseconds Histogram' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumDisabledHostsGauge); err != nil {
		m.log.Error("Failed to register 'Num Disabled Hosts Gauge' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumHostsGauge); err != nil {
		m.log.Error("Failed to register 'Num Hosts Gauge' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.KernelMigrationLatencyHistogram); err != nil {
		m.log.Error("Failed to register 'Kernel Migration Latency Histogram' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.ClusterSubscriptionRatioGauge); err != nil {
		m.log.Error("Failed to register 'Cluster Subscription Ratio Gauge' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.DemandGpusGauge); err != nil {
		m.log.Error("Failed to register 'Demand Gpus Gauge' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.BusyGpusGauge); err != nil {
		m.log.Error("Failed to register 'Busy Gpus Gauge' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.KernelCreationLatencyHistogram); err != nil {
		m.log.Error("Failed to register 'Kernel Creation Latency Histogram' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.PlacerFindHostLatencyMicrosecondsHistogramVec); err != nil {
		m.log.Error("Failed to register 'Placer FindHosts Latency Histogram' metric because: %v", err)
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

// GetContainerMetricsProvider returns nil in the case of a GatewayPrometheusManager struct,
// as GatewayPrometheusManager does not provide/implement this interface.
func (m *GatewayPrometheusManager) GetContainerMetricsProvider() ContainerMetricsProvider {
	m.log.Warn("Someone is attempting to retrieve a ContainerMetricsProvider from a GatewayPrometheusManager. " +
		"GatewayPrometheusManager does not implement the ContainerMetricsProvider interface, so this is going to fail.")
	return nil
}

func (m *GatewayPrometheusManager) GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram {
	return m.ScaleOutLatencyMillisecondsHistogram
}

func (m *GatewayPrometheusManager) GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram {
	return m.ScaleInLatencyMillisecondsHistogram
}

func (m *GatewayPrometheusManager) GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec {
	return m.PlacerFindHostLatencyMicrosecondsHistogramVec
}

func (m *GatewayPrometheusManager) GetNumDisabledHostsGauge() prometheus.Gauge {
	return m.NumDisabledHostsGauge
}

func (m *GatewayPrometheusManager) GetNumHostsGauge() prometheus.Gauge {
	return m.NumHostsGauge
}

func (m *GatewayPrometheusManager) GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram {
	return m.HostRemoteSyncLatencyMicrosecondsHistogram
}
