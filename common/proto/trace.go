package proto

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

// NewRequestTrace creates a new RequestTrace and returns a pointer to it.
//
// The RequestTrace struct has all of its timing fields initialized to -1.
func NewRequestTrace() *RequestTrace {
	return &RequestTrace{
		ReplicaId:                      -1,
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
		log.Debug("Assigned value to \"RequestReceivedByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.RequestSentByGateway == DefaultTraceTimingValue {
		rt.RequestSentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.RequestReceivedByLocalDaemon == DefaultTraceTimingValue {
		rt.RequestReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.RequestSentByLocalDaemon == DefaultTraceTimingValue {
		rt.RequestSentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.RequestReceivedByKernelReplica == DefaultTraceTimingValue {
		rt.RequestReceivedByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByKernelReplica\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.ReplySentByKernelReplica == DefaultTraceTimingValue {
		rt.ReplySentByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByKernelReplica\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.ReplyReceivedByLocalDaemon == DefaultTraceTimingValue {
		rt.ReplyReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.ReplySentByLocalDaemon == DefaultTraceTimingValue {
		rt.ReplySentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.ReplyReceivedByGateway == DefaultTraceTimingValue {
		rt.ReplyReceivedByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else if rt.ReplySentByGateway == DefaultTraceTimingValue {
		rt.ReplySentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			rt.MessageType, rt.MessageId, rt.KernelId)
		return true
	} else {
		return false
	}
}
