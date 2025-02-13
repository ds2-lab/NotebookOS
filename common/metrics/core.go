package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"net/http"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scusemua/distributed-notebook/common/utils"
)

const (
	ClusterGateway NodeType = "cluster_gateway"
	LocalDaemon    NodeType = "local_daemon"
	JupyterKernel  NodeType = "jupyter_kernel"
)

var (
	ErrGatewayPrometheusManagerAlreadyRunning = errors.New("GatewayPrometheusManager is already running")
	ErrMetricsNotInitialized                  = errors.New("the StatisticsAndMetricsProvider has not been initialized yet")
	//ErrGatewayPrometheusManagerNotRunning     = errors.New("GatewayPrometheusManager is not running")
)

// NodeType indicates whether a node is a Cluster Gateway ("cluster_gateway"), a Local Daemon ("local_daemon"),
// or a Jupyter kernel Replica ("jupyter_kernel").
type NodeType string

func (t NodeType) String() string {
	return string(t)
}

// prometheusHandler is an internal interface that defines important, internal functionality of the
// Prometheus managers. This functionality is required for their correct operation, but entities that use
// Prometheus managers need not be concerned with these details, so they're defined in a non-exported interface.
type prometheusHandler interface {
	// HandleRequest is an HTTP handler to serve Prometheus metric-scraping requests.
	HandleRequest(*gin.Context)

	// HandleVariablesRequest handles query requests from Grafana for variables that are required to create Dashboards.
	HandleVariablesRequest(*gin.Context)
}

// BasePrometheusManager contains the common state and infrastructure required
// by both the GatewayPrometheusManager and the DaemonPrometheusManager.
type basePrometheusManager struct {
	log logger.Logger

	instance prometheusHandler

	prometheusHandler http.Handler
	engine            *gin.Engine
	httpServer        *http.Server

	// initializeInstanceMetrics is a field that is to be assigned by "child" structs in their "constructors".
	// Specifically, this function is assigned by the 'instance' to initialize the instance's metrics.
	initializeInstanceMetrics func() error

	// NumActiveKernelReplicasGaugeVec is a Prometheus Gauge Vector for how many replicas are scheduled on
	// a particular Local Daemon.
	NumActiveKernelReplicasGaugeVec *prometheus.GaugeVec

	// TotalNumKernelsCounterVec tracks the total number of kernels ever created. Kernels that have stopped
	// running are still counted in this metric.
	TotalNumKernelsCounterVec *prometheus.CounterVec

	TotalNumPrewarmContainersUsedVec            *prometheus.CounterVec
	TotalNumPrewarmContainersCreatedCounterVec  *prometheus.CounterVec
	TotalNumStandardContainersCreatedCounterVec *prometheus.CounterVec

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

	// MessageSendLatencyMicrosecondsVec is a histogram of the time taken to send a ZMQ message in microseconds.
	MessageSendLatencyMicrosecondsVec *prometheus.HistogramVec

	// AckLatencyMicrosecondsVec is a histogram of the amount of time that passes before an ACK is received.
	AckLatencyMicrosecondsVec *prometheus.HistogramVec

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

	// MessagesSent simply counts the number of Jupyter messages that are sent, including resubmissions.
	JupyterMessagesSent *prometheus.CounterVec

	// MessagesSent simply counts the number of Jupyter messages that are sent, NOT including resubmission
	JupyterUniqueMessagesSent *prometheus.CounterVec
	nodeId                    string

	port int
	mu   sync.Mutex

	// serving indicates whether the manager has been started and is serving requests.
	serving            bool
	metricsInitialized bool
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

	if m.port <= 0 {
		m.log.Debug("Prometheus Port is set to %d. Not serving HTTP server.", m.port)
		return
	}

	// Commented-out for now as I don't want the log messages for Prometheus requests.
	// m.engine.Use(gin.Logger())
	m.engine.Use(gin.Recovery())
	m.engine.Use(cors.Default())

	m.engine.GET("/variables/:variable_name", m.HandleVariablesRequest)
	m.engine.GET("/metrics", m.HandleRequest)

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
	m.TotalNumPrewarmContainersUsedVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "prewarm_containers_used",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})
	m.TotalNumPrewarmContainersCreatedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "prewarm_containers_created",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})
	m.TotalNumStandardContainersCreatedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "standard_containers_created",
		Help:      "Total number of kernel replicas to have ever been scheduled/created",
	}, []string{"node_id"})

	// Create/define message-related metrics.

	m.NumFailedSendsCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "num_failed_sends_total",
		Help:      "The number of messages that were never acknowledged by the target  recipient, thus constituting a \"failed send\".",
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.MessageLatencyMicrosecondsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "message_latency_microseconds",
		Help:      "End-to-end latency in microseconds of messages. The end-to-end latency is measured from the time the message is forwarded by the node to the time at which the node receives the associated response.",
		Buckets:   []float64{500, 5000, 10e3, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, 5e6, 30e6, 60e6, 300e6},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.AckLatencyMicrosecondsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "ack_received_latency_microseconds",
		Help:      "The amount of time that passes before an acknowledgement message is received from the recipient of another (non-acknowledgement) message.",
		Buckets:   []float64{100, 500, 1000, 2500, 5000, 10e3, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, 5e6, 30e6, 60e6, 300e6},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.MessageSendLatencyMicrosecondsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "message_send_latency_microseconds",
		Help:      "The latency, in microseconds, to send a ZMQ message. This does not include receiving an explicit ACK or a response.",
		Buckets:   []float64{100, 250, 500, 1000, 2500, 5000, 10e3, 25e3, 50e3, 100e3, 250e3, 500e3, 1e6, 5e6, 30e6, 60e6},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.NumSendsBeforeAckReceived = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "distributed_cluster",
		Name:      "num_resends_required",
		Help:      "The number of times a message had to be sent before an acknowledgement was received.",
		Buckets:   []float64{0, 1, 2, 3, 4, 5},
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.JupyterMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "messages_sent_total",
		Help:      "The number of times a message was sent, including resubmissions.",
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	m.JupyterUniqueMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "distributed_cluster",
		Name:      "unique_messages_sent_total",
		Help:      "The number of times a message was sent, not including resubmissions.",
	}, []string{"node_id", "node_type", "socket_type", "jupyter_message_type"})

	// Register message-related metrics.

	if err := prometheus.Register(m.MessageLatencyMicrosecondsVec); err != nil {
		m.log.Error("Failed to register 'Gateway Shell Message Latency' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.JupyterMessagesSent); err != nil {
		m.log.Error("Failed to register 'Jupyter Messages Sent' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.JupyterUniqueMessagesSent); err != nil {
		m.log.Error("Failed to register 'Jupyter Unique Messages Sent' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.MessageSendLatencyMicrosecondsVec); err != nil {
		m.log.Error("Failed to register 'Message Send Latency Microseconds' metric because: %v", err)
		return err
	}

	if err := prometheus.Register(m.AckLatencyMicrosecondsVec); err != nil {
		m.log.Error("Failed to register 'Ack Latency Microseconds Vec' metric because: %v", err)
		return err
	}

	// Register miscellaneous metrics.

	if err := prometheus.Register(m.NumActiveKernelReplicasGaugeVec); err != nil {
		m.log.Error("Failed to register 'Number of Active kernel Replicas' metric because: %v", err)
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
	socketType messaging.MessageType, jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Warn("Cannot record message E2E latency observation as metrics have not yet been initialized...")
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
	nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Warn("Cannot record \"NumSendAttemptsRequired\" observation as metrics have not yet been initialized...")
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
func (m *basePrometheusManager) AddFailedSendAttempt(nodeId string, nodeType NodeType, socketType messaging.MessageType,
	jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Warn("Cannot record \"NumSendAttemptsRequired\" observation as metrics have not yet been initialized...")
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

// SentMessage record that a message was sent (including cases where the message sent was resubmitted and not
// sent for the very first time).
func (m *basePrometheusManager) SentMessage(nodeId string, sendLatency time.Duration, nodeType NodeType, socketType messaging.MessageType,
	jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Warn("Cannot record \"JupyterMessagesSent\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.JupyterMessagesSent.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Inc()

	m.MessageSendLatencyMicrosecondsVec.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Observe(float64(sendLatency.Microseconds()))

	return nil
}

// SentMessageUnique records that a message was sent. This should not be incremented for resubmitted messages.
func (m *basePrometheusManager) SentMessageUnique(nodeId string, nodeType NodeType, socketType messaging.MessageType,
	jupyterMessageType string) error {

	if !m.metricsInitialized {
		m.log.Warn("Cannot record \"JupyterUniqueMessagesSent\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.JupyterUniqueMessagesSent.
		With(prometheus.Labels{
			"node_id":              nodeId,
			"node_type":            nodeType.String(),
			"socket_type":          socketType.String(),
			"jupyter_message_type": jupyterMessageType,
		}).Inc()

	return nil
}

// AddAckReceivedLatency is used to record an observation for the "ack_received_latency_microseconds" metric.
func (m *basePrometheusManager) AddAckReceivedLatency(latency time.Duration, nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !m.metricsInitialized {
		m.log.Warn("Cannot record \"AckLatencyMicroseconds\" observation as metrics have not yet been initialized...")
		return ErrMetricsNotInitialized
	}

	m.AckLatencyMicrosecondsVec.With(prometheus.Labels{
		"node_id":              nodeId,
		"node_type":            nodeType.String(),
		"socket_type":          socketType.String(),
		"jupyter_message_type": jupyterMessageType,
	}).Observe(float64(latency.Microseconds()))

	return nil
}
