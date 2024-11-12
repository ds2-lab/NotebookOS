package proto

import (
	"encoding/json"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
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
		RequestTraceUuid:               uuid.NewString(),
	}
}

// Clone creates a copy of RequestTrace with a DIFFERENT UUID.
func (x *RequestTrace) Clone() *RequestTrace {
	return &RequestTrace{
		MessageId:                      x.MessageId,
		MessageType:                    x.MessageType,
		KernelId:                       x.KernelId,
		ReplicaId:                      x.ReplicaId,
		RequestReceivedByGateway:       x.RequestReceivedByGateway,
		RequestSentByGateway:           x.RequestSentByGateway,
		RequestReceivedByLocalDaemon:   x.RequestReceivedByLocalDaemon,
		RequestSentByLocalDaemon:       x.RequestSentByLocalDaemon,
		RequestReceivedByKernelReplica: x.RequestReceivedByKernelReplica,
		ReplySentByKernelReplica:       x.ReplySentByKernelReplica,
		ReplyReceivedByLocalDaemon:     x.ReplyReceivedByLocalDaemon,
		ReplySentByLocalDaemon:         x.ReplySentByLocalDaemon,
		ReplyReceivedByGateway:         x.ReplyReceivedByGateway,
		ReplySentByGateway:             x.ReplySentByGateway,
		RequestTraceUuid:               uuid.NewString(),
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
func (x *RequestTrace) PopulateNextField(unixMilliseconds int64, log logger.Logger) bool {
	if x.RequestReceivedByGateway == DefaultTraceTimingValue {
		x.RequestReceivedByGateway = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.RequestSentByGateway == DefaultTraceTimingValue {
		x.RequestSentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.RequestReceivedByLocalDaemon == DefaultTraceTimingValue {
		x.RequestReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.RequestSentByLocalDaemon == DefaultTraceTimingValue {
		x.RequestSentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"RequestSentByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.RequestReceivedByKernelReplica == DefaultTraceTimingValue {
		x.RequestReceivedByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"RequestReceivedByKernelReplica\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.ReplySentByKernelReplica == DefaultTraceTimingValue {
		x.ReplySentByKernelReplica = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByKernelReplica\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.ReplyReceivedByLocalDaemon == DefaultTraceTimingValue {
		x.ReplyReceivedByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.ReplySentByLocalDaemon == DefaultTraceTimingValue {
		x.ReplySentByLocalDaemon = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByLocalDaemon\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.ReplyReceivedByGateway == DefaultTraceTimingValue {
		x.ReplyReceivedByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplyReceivedByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else if x.ReplySentByGateway == DefaultTraceTimingValue {
		x.ReplySentByGateway = unixMilliseconds
		log.Debug("Assigned value to \"ReplySentByGateway\" field of request trace for \"%s\" message \"%s\" targeting kernel \"%s\"",
			x.MessageType, x.MessageId, x.KernelId)
		return true
	} else {
		return false
	}
}
