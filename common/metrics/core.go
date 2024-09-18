package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	jupyterTypes "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

const (
	ClusterGateway NodeType = "cluster_gateway"
	LocalDaemon    NodeType = "local_daemon"
	JupyterKernel  NodeType = "jupyter_kernel"
)

var (
	ErrGatewayPrometheusManagerAlreadyRunning = errors.New("GatewayPrometheusManager is already running")
	ErrMetricsNotInitialized                  = errors.New("the MessagingMetricsProvider has not been initialized yet")
	//ErrGatewayPrometheusManagerNotRunning     = errors.New("GatewayPrometheusManager is not running")
)

// NodeType indicates whether a node is a Cluster Gateway ("cluster_gateway"), a Local Daemon ("local_daemon"),
// or a Jupyter Kernel Replica ("jupyter_kernel").
type NodeType string

func (t NodeType) String() string {
	return string(t)
}

// prometheusHandler is an internal interface that defines important, internal functionality of the
// Prometheus managers. This functionality is required for their correct operation, but entities that use
// Prometheus managers need not be concerned with these details, so they're defined in a non-exported interface.
type prometheusHandler interface {
	PrometheusMetricsProvider

	// HandleRequest is an HTTP handler to serve Prometheus metric-scraping requests.
	HandleRequest(*gin.Context)

	// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
	HandleVariablesRequest(*gin.Context)
}

// MessagingMetricsProvider is an interface intended to be embedded within the PrometheusMetricsProvider interface.
//
// This interface will allow servers to record observations of metrics like end-to-end latency without knowing
// the actual names of the fields within concrete structs that implement the PrometheusMetricsProvider interface.
type MessagingMetricsProvider interface {
	// AddMessageE2ELatencyObservation records an observation of end-to-end latency, in microseconds, for a single message.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddMessageE2ELatencyObservation(latency time.Duration, nodeId string, nodeType NodeType,
		socketType jupyterTypes.MessageType, jupyterMessageType string) error

	// AddNumSendAttemptsRequiredObservation enables the caller to record an observation of the number of times a
	// message had to be (re)sent before an ACK was received from the recipient.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddNumSendAttemptsRequiredObservation(acksRequired float64, nodeId string, nodeType NodeType,
		socketType jupyterTypes.MessageType, jupyterMessageType string) error

	// AddFailedSendAttempt records that a message was never acknowledged by the target recipient.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddFailedSendAttempt(nodeId string, nodeType NodeType, socketType jupyterTypes.MessageType, jupyterMessageType string) error
}

// ContainerMetricsProvider is an exported interface that exposes an API for publishing container-related metrics.
type ContainerMetricsProvider interface {
	// AddContainerCreationLatencyObservation records the latency of a container-creation event.
	//
	// If the target ContainerMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddContainerCreationLatencyObservation(latency time.Duration) error
}

// PrometheusMetricsProvider defines the general interface of Prometheus metric managers.
//
// This interface is designed to be used by external entities who need to store/publish metrics.
type PrometheusMetricsProvider interface {
	// GetMessagingMetricsProvider returns a MessagingMetricsProvider, or nil if this particular
	// PrometheusMetricsProvider is incapable of providing a reference to a MessagingMetricsProvider instance.
	GetMessagingMetricsProvider() MessagingMetricsProvider

	// GetContainerMetricsProvider returns a ContainerMetricsProvider, or nil if this particular
	// PrometheusMetricsProvider is incapable of providing a reference to a ContainerMetricsProvider instance.
	GetContainerMetricsProvider() ContainerMetricsProvider

	// Start registers metrics with Prometheus and begins serving the metrics via an HTTP endpoint.
	Start() error

	// NodeId returns the node ID associated with the metrics manager.
	NodeId() string

	// IsRunning returns true if the PrometheusMetricsProvider has been started and is serving metrics.
	IsRunning() bool

	// Stop instructs the PrometheusMetricsProvider to shut down its HTTP server.
	Stop() error
}

// BasePrometheusManager contains the common state and infrastructure required
// by both the GatewayPrometheusManager and the DaemonPrometheusManager.
type basePrometheusManager struct {
	log logger.Logger

	instance prometheusHandler

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

	// NumActiveKernelReplicasGaugeVec is a Prometheus Gauge Vector for how many replicas are scheduled on
	// a particular Local Daemon.
	NumActiveKernelReplicasGaugeVec *prometheus.GaugeVec

	// TotalNumKernelsCounterVec tracks the total number of kernels ever created. Kernels that have stopped
	// running are still counted in this metric.
	TotalNumKernelsCounterVec *prometheus.CounterVec

	// NumTrainingEventsCompletedCounterVec is the number of training events that have completed successfully.
	NumTrainingEventsCompletedCounterVec *prometheus.CounterVec

	///////////////////////
	// Messaging metrics //
	///////////////////////

	// NumFailedSendsCounterVec counts the number of messages that were never acknowledged by the target
	// recipient, thus constituting a "failed send".
	NumFailedSendsCounterVec *prometheus.CounterVec

	// MessageLatencyMicrosecondsVec is the end-to-end latency, in microseconds, of messages forwarded by the Gateway or Local Daemon.
	// The end-to-end latency is measured from the time the message is forwarded by the Gateway or Local Daemon to the
	// time at which the Gateway receives the associated response.
	//
	// This metric requires the following labels:
	//
	// - "node_id": the ID of the node observing the latency.
	//
	// - "node_type": the "type" of the node observing the latency (i.e., "local_daemon" or "cluster_gateway").
	//
	// - "socket_type": the socket type of the message whose latency is being observed.
	//
	// - "jupyter_message_type": the Jupyter message type of the message whose latency is being observed.
	MessageLatencyMicrosecondsVec *prometheus.HistogramVec

	// NumSendsBeforeAckReceived is a prometheus.HistogramVec tracking observations of the number of times a given
	// message was sent before an ACK was received for that message.
	//
	// This metric requires the following labels:
	//
	// - "node_id": the ID of the node sending the message and receiving ACKs for the message.
	//
	// - "node_type": the "type" of the node sending the message and receiving ACKs for the message.
	//
	// - "socket_type": the socket type of the associated message.
	//
	// - "jupyter_message_type": the Jupyter message type of the associated message.
	NumSendsBeforeAckReceived *prometheus.HistogramVec
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

// GetMessagingMetricsProvider returns a MessagingMetricsProvider, a role that is fulfilled directly
// by the basePrometheusManager.
func (m *basePrometheusManager) GetMessagingMetricsProvider() MessagingMetricsProvider {
	return m
}

// GetContainerMetricsProvider returns a ContainerMetricsProvider if the basePrometheusManager's instance
// field is capable of producing a ContainerMetricsProvider. Otherwise, this returns nil.
func (m *basePrometheusManager) GetContainerMetricsProvider() ContainerMetricsProvider {
	return m.instance.GetContainerMetricsProvider()
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

// Stop instructs the PrometheusMetricsProvider to shut down its HTTP server.
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
	}, []string{"node_type", "node_id"})
	m.NumTrainingEventsCompletedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "training_events_completed_total",
		Help:      "The number of training events that have completed successfully",
	}, []string{"node_type", "node_id"})
	m.TotalNumKernelsCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "sessions_total",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_type", "node_id"})

	// Create/define message latency metrics.

	m.NumFailedSendsCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "num_failed_sends_total",
		Help:      "The number of messages that were never acknowledged by the target  recipient, thus constituting a \"failed send\".",
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.MessageLatencyMicrosecondsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "message_latency_microseconds",
		Help:      "End-to-end latency in microseconds of messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
		Buckets: []float64{1, 10, 100, 1e3, 5e3, 10e3, 15e3, 20e3, 30e3, 50e3, 75e3, 100e3, 150e3, 200e3, 300e3,
			400e3, 500e3, 750e3, 1e6, 1.5e6, 2e6, 3e6, 4.5e6, 6e6, 9e6, 1.2e7, 1.8e7, 2.4e7, 3e7, 4.5e7, 6e7, 9e7, 1.2e8},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.NumSendsBeforeAckReceived = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "num_resends_required",
		Help:      "The number of times a message had to be sent before an acknowledgement was received.",
		Buckets:   []float64{0, 1, 2, 3, 4, 5},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})
	// Register message latency metrics.

	if err := prometheus.Register(m.MessageLatencyMicrosecondsVec); err != nil {
		m.log.Error("Failed to register 'Gateway Shell Message Latency' metric because: %v", err)
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

	if err := prometheus.Register(m.NumFailedSendsCounterVec); err != nil {
		m.log.Error("Failed to register 'Training Events Completed' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumSendsBeforeAckReceived); err != nil {
		m.log.Error("Failed to register 'Num Sends Before Ack Received' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.NumTrainingEventsCompletedCounterVec); err != nil {
		m.log.Error("Failed to register 'Training Events Completed' metric because: %v", err)
		return err
	}

	return m.initializeInstanceMetrics()
}

////////////////////////////////////////////////
// Messaging Metrics interface implementation //
////////////////////////////////////////////////

// AddMessageE2ELatencyObservation records an observation of end-to-end latency for a single Shell message.
//
// If the target Prometheus Manager has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
// error is returned.
func (m *basePrometheusManager) AddMessageE2ELatencyObservation(latency time.Duration, nodeId string, nodeType NodeType,
	socketType jupyterTypes.MessageType, jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Error("Cannot record message E2E latency observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.MessageLatencyMicrosecondsVec.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Observe(float64(latency.Microseconds()))

	return nil
}

// AddNumSendAttemptsRequiredObservation enables the caller to record an observation of the number of times a
// message had to be (re)sent before an ACK was received from the recipient.
//
// If the target Prometheus Manager has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
// error is returned.
func (m *basePrometheusManager) AddNumSendAttemptsRequiredObservation(sendsRequired float64, nodeId string,
	nodeType NodeType, socketType jupyterTypes.MessageType, jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Error("Cannot record \"NumSendAttemptsRequired\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.NumSendsBeforeAckReceived.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Observe(sendsRequired)

	return nil
}

// AddFailedSendAttempt records that a message was never acknowledged by the target recipient.
func (m *basePrometheusManager) AddFailedSendAttempt(nodeId string, nodeType NodeType, socketType jupyterTypes.MessageType,
	jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Error("Cannot record \"NumSendAttemptsRequired\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.NumFailedSendsCounterVec.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Inc()

	return nil
}
