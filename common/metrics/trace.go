package metrics

import (
	"encoding/json"
	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	// DefaultTraceTimingValue is the value that RequestTrace timing fields are initialized to.
	DefaultTraceTimingValue int64 = -1

	// RequestTraceMetadataKey is the key at which a RequestTrace is included in the metadata mapping/frame
	// of a types.JupyterMessage.
	RequestTraceMetadataKey string = "request_trace"
)

// JupyterRequestTraceFrame is a wrapper around a *RequestTrace that allows us to deserialize the
// associated frame of a types.JupyterMessage when it contains a serialized/JSON-encoded *RequestTrace.
type JupyterRequestTraceFrame struct {
	RequestTrace *RequestTrace `json:"request_trace" mapstructure:"request_trace"`
}

func (f *JupyterRequestTraceFrame) String() string {
	m, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}

	return string(m)
}

type RequestTrace struct {
	// MessageId is the Jupyter message ID of the associated request.
	MessageId string `json:"msg_id"`

	// MessageType is the Jupyter message type of the associated request.
	MessageType string `json:"msg_type"`

	// KernelId is the ID of the kernel that is the target recipient of the associated request.
	KernelId string `json:"kernel_id"`

	// ReplicaId is the ID of the particular kernel replica that received this trace.
	// ReplicaId int32 `json:"replica_id"`

	// RequestReceivedByGateway is the time at which the associated Request was received by the Cluster Gateway.
	RequestReceivedByGateway int64 `json:"request_received_by_gateway"`

	// RequestReceivedByGateway is the time at which the associated Request was forwarded to the Local Daemons
	// by the Cluster Gateway.
	RequestSentByGateway int64 `json:"request_sent_by_gateway"`

	RequestReceivedByLocalDaemon   int64 `json:"request_received_by_local_daemon"`
	RequestSentByLocalDaemon       int64 `json:"request_sent_by_local_daemon"`
	RequestReceivedByKernelReplica int64 `json:"request_received_by_kernel_replica"`
	ReplySentByKernelReplica       int64 `json:"reply_sent_by_kernel_replica"`
	ReplyReceivedByLocalDaemon     int64 `json:"reply_received_by_local_daemon"`
	ReplySentByLocalDaemon         int64 `json:"reply_sent_by_local_daemon"`
	ReplyReceivedByGateway         int64 `json:"reply_received_by_gateway"`
	ReplySentByGateway             int64 `json:"reply_sent_by_gateway"`
}

// NewRequestTrace creates a new RequestTrace and returns a pointer to it.
//
// The RequestTrace struct has all of its timing fields initialized to -1.
func NewRequestTrace() *RequestTrace {
	return &RequestTrace{
		RequestReceivedByGateway:       DefaultTraceTimingValue,
		RequestSentByGateway:           DefaultTraceTimingValue,
		RequestReceivedByLocalDaemon:   DefaultTraceTimingValue,
		RequestSentByLocalDaemon:       DefaultTraceTimingValue,
		RequestReceivedByKernelReplica: DefaultTraceTimingValue,
		ReplySentByKernelReplica:       DefaultTraceTimingValue,
		ReplySentByGateway:             DefaultTraceTimingValue,
		ReplyReceivedByGateway:         DefaultTraceTimingValue,
		ReplySentByLocalDaemon:         DefaultTraceTimingValue,
		ReplyReceivedByLocalDaemon:     DefaultTraceTimingValue,
	}
}

func (rt *RequestTrace) String() string {
	m, err := json.Marshal(rt)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// PopulateNextField populates the next field with the given unix milliseconds timestamp and returns true.
//
// If all fields of the RequestTrace are already populated, then GetNextFieldToPopulate will return false.
//
// The fields are (generally) populated in the following order:
// - 1:   RequestReceivedByGateway
//
// - 2:   RequestSentByGateway
//
// - 3:   RequestReceivedByLocalDaemon
//
// - 4:   RequestSentByLocalDaemon
//
// - 5:   RequestReceivedByKernelReplica
//
// - 6:   ReplySentByKernelReplica
//
// - 7:   ReplySentByGateway
//
// - 8:   ReplyReceivedByGateway
//
// - 9:   ReplySentByLocalDaemon
//
// - 10:  ReplyReceivedByLocalDaemon
func (rt *RequestTrace) PopulateNextField(unixMilliseconds int64, log logger.Logger) bool {
	if rt.RequestReceivedByGateway == DefaultTraceTimingValue {
		rt.RequestReceivedByGateway = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByGateway\" field.")
		return true
	} else if rt.RequestSentByGateway == DefaultTraceTimingValue {
		rt.RequestSentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByGateway\" field.")
		return true
	} else if rt.RequestReceivedByLocalDaemon == DefaultTraceTimingValue {
		rt.RequestReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByLocalDaemon\" field.")
		return true
	} else if rt.RequestSentByLocalDaemon == DefaultTraceTimingValue {
		rt.RequestSentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByLocalDaemon\" field.")
		return true
	} else if rt.RequestReceivedByKernelReplica == DefaultTraceTimingValue {
		rt.RequestReceivedByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByKernelReplica\" field.")
		return true
	} else if rt.ReplySentByKernelReplica == DefaultTraceTimingValue {
		rt.ReplySentByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByKernelReplica\" field.")
		return true
	} else if rt.ReplyReceivedByLocalDaemon == DefaultTraceTimingValue {
		rt.ReplyReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByLocalDaemon\" field.")
		return true
	} else if rt.ReplySentByLocalDaemon == DefaultTraceTimingValue {
		rt.ReplySentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByLocalDaemon\" field.")
		return true
	} else if rt.ReplyReceivedByGateway == DefaultTraceTimingValue {
		rt.ReplyReceivedByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByGateway\" field.")
		return true
	} else if rt.ReplySentByGateway == DefaultTraceTimingValue {
		rt.ReplySentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByGateway\" field.")
		return true
	} else {
		return false
	}
}
