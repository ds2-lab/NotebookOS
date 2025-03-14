package metrics

import (
	"errors"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync/atomic"
	"time"
)

var (
	ErrPrometheusMetricsDisabled = errors.New("cannot start GatewayPrometheusManager as Prometheus metrics are disabled")
)

type Host interface {
	Enabled() bool
	IdleGPUs() float64
	PendingGPUs() float64
	CommittedGPUs() float64
	IdleCPUs() float64
	PendingCPUs() float64
	CommittedCPUs() float64
	IdleMemoryMb() float64
	PendingMemoryMb() float64
	CommittedMemoryMb() float64
	IdleVRAM() float64
	PendingVRAM() float64
	CommittedVRAM() float64
	ResourceSpec() types.ValidatableResourceSpec
	GetNodeName() string
	GetID() string
}

type ClusterMetricsProvider struct {
	gatewayPrometheusManager *GatewayPrometheusManager

	incrementResourceCountsForNewHostCallback     func(Host)
	decrementResourceCountsForRemovedHostCallback func(Host)

	updateClusterStatsCallback func(updater func(statistics *ClusterStatistics))
	prometheusMetricsEnabled   bool

	log logger.Logger

	numActiveExecutions *atomic.Int32
}

type CallbackProvider interface {
	LocalDaemonNodeProvider

	IncrementResourceCountsForNewHost(host Host)
	DecrementResourceCountsForRemovedHost(host Host)
	UpdateClusterStatistics(updater func(statistics *ClusterStatistics))
}

func NewClusterMetricsProvider(port int, callbackProvider CallbackProvider, numActiveExecutions *atomic.Int32) *ClusterMetricsProvider {
	provider := &ClusterMetricsProvider{
		gatewayPrometheusManager:                      nil,
		prometheusMetricsEnabled:                      false,
		updateClusterStatsCallback:                    callbackProvider.UpdateClusterStatistics,
		incrementResourceCountsForNewHostCallback:     callbackProvider.IncrementResourceCountsForNewHost,
		decrementResourceCountsForRemovedHostCallback: callbackProvider.DecrementResourceCountsForRemovedHost,
		numActiveExecutions:                           numActiveExecutions,
	}

	config.InitLogger(&provider.log, provider)

	if port > 0 {
		provider.log.Debug("Creating GatewayPrometheusManager with port=%d.", port)
		provider.gatewayPrometheusManager = NewGatewayPrometheusManager(port, callbackProvider)
		provider.prometheusMetricsEnabled = true
	}

	return provider
}

func (p *ClusterMetricsProvider) SetNumActiveTrainingsPointer(numActiveTrainings *atomic.Int32) {
	p.numActiveExecutions = numActiveTrainings
}

func (p *ClusterMetricsProvider) IncrementNumActiveExecutions() {
	p.numActiveExecutions.Add(1)
}

func (p *ClusterMetricsProvider) DecrementNumActiveExecutions() {
	p.numActiveExecutions.Add(-1)
}

func (p *ClusterMetricsProvider) NumActiveExecutions() int32 {
	return p.numActiveExecutions.Load()
}

func (p *ClusterMetricsProvider) StartGatewayPrometheusManager() error {
	if p.prometheusMetricsEnabled == false || p.gatewayPrometheusManager == nil {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.Start()
}

// PrometheusMetricsEnabled returns true if Prometheus metrics are enabled/available.
func (p *ClusterMetricsProvider) PrometheusMetricsEnabled() bool {
	return p.prometheusMetricsEnabled && p.gatewayPrometheusManager != nil
}

// UpdateClusterStatistics accepts a function as an argument.
//
// The parameter function accepts one parameter of type *statistics.ClusterStatistics and
// is used to update cluster-level metrics and statistics.
func (p *ClusterMetricsProvider) UpdateClusterStatistics(updater func(statistics *ClusterStatistics)) {
	p.updateClusterStatsCallback(updater)
}

func (p *ClusterMetricsProvider) GetGatewayPrometheusManager() *GatewayPrometheusManager {
	return p.gatewayPrometheusManager
}

// IncrementResourceCountsForNewHost is intended to be called when a Host is added to the Cluster.
// IncrementResourceCountsForNewHost will increment the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (p *ClusterMetricsProvider) IncrementResourceCountsForNewHost(host Host) {
	if p.incrementResourceCountsForNewHostCallback != nil {
		p.incrementResourceCountsForNewHostCallback(host)
	}
}

// DecrementResourceCountsForRemovedHost is intended to be called when a Host is removed from the Cluster.
// DecrementResourceCountsForRemovedHost will decrement the ClusterStatistics' resource counts
// based on the resources available on the Host in question.
func (p *ClusterMetricsProvider) DecrementResourceCountsForRemovedHost(host Host) {
	if p.decrementResourceCountsForRemovedHostCallback != nil {
		p.decrementResourceCountsForRemovedHostCallback(host)
	}
}

func (p *ClusterMetricsProvider) GetScaleOutLatencyMillisecondsHistogram() prometheus.Histogram {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetScaleOutLatencyMillisecondsHistogram()
}

func (p *ClusterMetricsProvider) GetScaleInLatencyMillisecondsHistogram() prometheus.Histogram {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetScaleInLatencyMillisecondsHistogram()
}

func (p *ClusterMetricsProvider) GetPlacerFindHostLatencyMicrosecondsHistogram() *prometheus.HistogramVec {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetPlacerFindHostLatencyMicrosecondsHistogram()
}

func (p *ClusterMetricsProvider) GetNumDisabledHostsGauge() prometheus.Gauge {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetNumDisabledHostsGauge()
}

func (p *ClusterMetricsProvider) GetNumHostsGauge() prometheus.Gauge {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetNumHostsGauge()
}

func (p *ClusterMetricsProvider) GetHostRemoteSyncLatencyMicrosecondsHistogram() prometheus.Histogram {
	if p.gatewayPrometheusManager == nil {
		return nil
	}

	return p.gatewayPrometheusManager.GetHostRemoteSyncLatencyMicrosecondsHistogram()
}

func (p *ClusterMetricsProvider) IncrementNumTrainingEventsCompletedCounterVec() {
	if p.prometheusMetricsEnabled && p.gatewayPrometheusManager != nil {
		p.gatewayPrometheusManager.IncrementNumTrainingEventsCompletedCounterVec()
	}
}

func (p *ClusterMetricsProvider) AddMessageE2ELatencyObservation(latency time.Duration, nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.AddMessageE2ELatencyObservation(latency, nodeId, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) AddNumSendAttemptsRequiredObservation(acksRequired float64, nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.AddNumSendAttemptsRequiredObservation(acksRequired, nodeId, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) AddAckReceivedLatency(latency time.Duration, nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.AddAckReceivedLatency(latency, nodeId, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) AddFailedSendAttempt(nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.AddFailedSendAttempt(nodeId, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) SentMessage(nodeId string, sendLatency time.Duration, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.SentMessage(nodeId, sendLatency, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) SentMessageUnique(nodeId string, nodeType NodeType, socketType messaging.MessageType, jupyterMessageType string) error {
	if !p.PrometheusMetricsEnabled() {
		return ErrPrometheusMetricsDisabled
	}

	return p.gatewayPrometheusManager.SentMessageUnique(nodeId, nodeType, socketType, jupyterMessageType)
}

func (p *ClusterMetricsProvider) SpecGpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.SpecGpuGaugeVec
}

func (p *ClusterMetricsProvider) CommittedGpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.CommittedGpuGaugeVec
}

func (p *ClusterMetricsProvider) PendingGpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.PendingGpuGaugeVec
}

func (p *ClusterMetricsProvider) IdleGpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.IdleGpuGaugeVec
}

func (p *ClusterMetricsProvider) SpecCpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.SpecCpuGaugeVec
}

func (p *ClusterMetricsProvider) CommittedCpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.CommittedCpuGaugeVec
}

func (p *ClusterMetricsProvider) PendingCpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.PendingCpuGaugeVec
}

func (p *ClusterMetricsProvider) IdleCpuGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.IdleCpuGaugeVec
}

func (p *ClusterMetricsProvider) SpecMemoryGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.SpecMemoryGaugeVec
}

func (p *ClusterMetricsProvider) CommittedMemoryGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.CommittedMemoryGaugeVec
}

func (p *ClusterMetricsProvider) PendingMemoryGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.PendingMemoryGaugeVec
}

func (p *ClusterMetricsProvider) IdleMemoryGaugeVec() *prometheus.GaugeVec {
	return p.gatewayPrometheusManager.IdleMemoryGaugeVec
}
