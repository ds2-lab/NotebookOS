package server

import (
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"time"
)

// MessagingMetricsProvider is an interface intended to be embedded within the PrometheusMetricsProvider interface.
//
// This interface will allow servers to record observations of metrics like end-to-end latency without knowing
// the actual names of the fields within concrete structs that implement the PrometheusMetricsProvider interface.
type MessagingMetricsProvider interface {
	// UpdateClusterStatistics accepts a function as an argument.
	//
	// The parameter function accepts one parameter of type *statistics.ClusterStatistics and
	// is used to update cluster-level metrics and statistics.
	UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics))

	// AddMessageE2ELatencyObservation records an observation of end-to-end latency, in microseconds, for a single message.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddMessageE2ELatencyObservation(latency time.Duration, nodeId string, nodeType metrics.NodeType,
		socketType messaging.MessageType, jupyterMessageType string) error

	// AddNumSendAttemptsRequiredObservation enables the caller to record an observation of the number of times a
	// message had to be (re)sent before an ACK was received from the recipient.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddNumSendAttemptsRequiredObservation(acksRequired float64, nodeId string, nodeType metrics.NodeType,
		socketType messaging.MessageType, jupyterMessageType string) error

	// AddAckReceivedLatency is used to record an observation for the "ack_received_latency_microseconds" metric.
	AddAckReceivedLatency(latency time.Duration, nodeId string, nodeType metrics.NodeType,
		socketType messaging.MessageType, jupyterMessageType string) error

	// AddFailedSendAttempt records that a message was never acknowledged by the target recipient.
	//
	// If the target MessagingMetricsProvider has not yet initialized its metrics yet, then an ErrMetricsNotInitialized
	// error is returned.
	AddFailedSendAttempt(nodeId string, nodeType metrics.NodeType, socketType messaging.MessageType, jupyterMessageType string) error

	// SentMessage record that a message was sent (including cases where the message sent was resubmitted and not
	// sent for the very first time).
	SentMessage(nodeId string, sendLatency time.Duration, nodeType metrics.NodeType, socketType messaging.MessageType, jupyterMessageType string) error

	// SentMessageUnique records that a message was sent. This should not be incremented for resubmitted messages.
	SentMessageUnique(nodeId string, nodeType metrics.NodeType, socketType messaging.MessageType, jupyterMessageType string) error
}
