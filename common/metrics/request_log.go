package metrics

import (
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"sync"
)

var (
	ErrRequestLogEntryExists = errors.New("cannot add entry for specified request")
)

// RequestLog is used in DebugMode to trace the history/progress of a particular request/ZMQ message.
//
// Each AbstractServer maintains its own RequestLog in DebugMode.
type RequestLog struct {
	// mu is the main mutex of the RequestLog.
	mu  sync.Mutex
	log logger.Logger

	// EntriesByJupyterMsgId is a map from Jupyter Message ID to RequestLogEntry.
	EntriesByJupyterMsgId hashmap.HashMap[string, *RequestLogEntryWrapper]
}

// NewRequestLog creates and initializes a new RequestLog struct and returns a pointer to it.
func NewRequestLog() *RequestLog {
	requestLog := &RequestLog{
		EntriesByJupyterMsgId: hashmap.NewCornelkMap[string, *RequestLogEntryWrapper](64),
	}
	config.InitLogger(&requestLog.log, requestLog)

	return requestLog
}

func (l *RequestLog) Lock() {
	l.mu.Lock()
}

func (l *RequestLog) Unlock() {
	l.mu.Unlock()
}

func (l *RequestLog) TryLock() bool {
	return l.mu.TryLock()
}

// Len returns the number of entries in the RequestLog.
//
// This method is thread-safe.
func (l *RequestLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.unsafeLen()
}

// Size is an alias for the RequestLog's Len method.
//
// This method is thread-safe.
func (l *RequestLog) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.unsafeLen()
}

// unsafeLen returns the number of entries in the RequestLog.
//
// This method is NOT thread safe. It is meant to be called by Len and Size.
func (l *RequestLog) unsafeLen() int {
	return l.EntriesByJupyterMsgId.Len()
}

// AddEntry adds a RequestLogEntry to the RequestLog for the specified JupyterMessage.
func (l *RequestLog) AddEntry(msg *types.JupyterMessage, messageType types.MessageType, trace *proto.RequestTrace) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	msgId := msg.JupyterMessageId()

	var (
		wrapper  *RequestLogEntryWrapper
		existing *RequestLogEntry
		loaded   bool
	)
	if wrapper, loaded = l.EntriesByJupyterMsgId.Load(msgId); !loaded {
		wrapper = &RequestLogEntryWrapper{
			EntriesByNodeId:    hashmap.NewCornelkMap[int32, *RequestLogEntry](3),
			RequestId:          msg.RequestId,
			JupyterMessageId:   msg.JupyterMessageId(),
			JupyterMessageType: msg.JupyterMessageType(),
			MessageType:        messageType,
			KernelId:           msg.DestinationId,
		}
	}

	entry := NewRequestLogEntry(msg, messageType, trace)
	if existing, loaded = wrapper.EntriesByNodeId.LoadOrStore(trace.ReplicaId, entry); loaded {
		if existing.RequestTrace.RequestTraceUuid == trace.RequestTraceUuid && trace.ReplicaId != -1 {
			// If the existing trace is an old one with replica -1, and we now have the same trace updated
			// with its replica ID, overwrite it.
			wrapper.EntriesByNodeId.Store(trace.ReplicaId, entry)
		} else {
			return fmt.Errorf("already have an entry for message \"%s\" for replica %d", msgId, trace.ReplicaId)
		}
	}

	l.EntriesByJupyterMsgId.Store(msgId, wrapper)

	return nil
}

// RequestLogEntryWrapper is an entry for a single message in the
// RequestLog, broken up into separate individual entries by SMR node ID.
type RequestLogEntryWrapper struct {
	EntriesByNodeId    hashmap.HashMap[int32, *RequestLogEntry]
	RequestId          string
	JupyterMessageId   string
	JupyterMessageType string
	MessageType        types.MessageType
	KernelId           string
}

// RequestLogEntry is an entry for a single message in the RequestLog.
type RequestLogEntry struct {
	RequestId          string
	JupyterMessageId   string
	JupyterMessageType string
	MessageType        types.MessageType
	KernelId           string

	RequestTrace *proto.RequestTrace
}

// NewRequestLogEntry creates a new RequestLogEntry struct and returns a pointer to it.
func NewRequestLogEntry(msg *types.JupyterMessage, messageType types.MessageType, trace *proto.RequestTrace) *RequestLogEntry {
	return &RequestLogEntry{
		RequestId:          msg.RequestId,
		JupyterMessageId:   msg.JupyterMessageId(),
		JupyterMessageType: msg.JupyterMessageType(),
		RequestTrace:       trace,
		MessageType:        messageType,
		KernelId:           msg.DestinationId,
	}
}
