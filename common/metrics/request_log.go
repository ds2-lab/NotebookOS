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

	// EntriesByRequestId is a map from RequestID to RequestLogEntry.
	EntriesByRequestId hashmap.HashMap[string, *RequestLogEntry]

	// EntriesByJupyterMsgId is a map from Jupyter Message ID to RequestLogEntry.
	EntriesByJupyterMsgId hashmap.HashMap[string, *RequestLogEntry]

	// RequestsPeKernel is a map from kernel ID to an inner map.
	// The inner mapping is from Jupyter Message ID to RequestLogEntry.
	RequestsPerKernel hashmap.HashMap[string, hashmap.HashMap[string, *RequestLogEntry]]
}

// NewRequestLog creates and initializes a new RequestLog struct and returns a pointer to it.
func NewRequestLog() *RequestLog {
	requestLog := &RequestLog{
		EntriesByRequestId:    hashmap.NewCornelkMap[string, *RequestLogEntry](64),
		EntriesByJupyterMsgId: hashmap.NewCornelkMap[string, *RequestLogEntry](64),
		RequestsPerKernel:     hashmap.NewCornelkMap[string, hashmap.HashMap[string, *RequestLogEntry]](64),
	}
	config.InitLogger(&requestLog.log, requestLog)

	return requestLog
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
	return l.EntriesByRequestId.Len()
}

// AddEntry adds a RequestLogEntry to the RequestLog for the specified JupyterMessage.
func (l *RequestLog) AddEntry(msg *types.JupyterMessage, socket *types.Socket, trace *proto.RequestTrace) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	msgId := msg.JupyterMessageId()
	entry := &RequestLogEntry{
		RequestId:          msg.RequestId,
		JupyterMessageId:   msgId,
		JupyterMessageType: msg.JupyterMessageType(),
		RequestTrace:       trace,
		MessageType:        socket.Type,
		KernelId:           msg.DestinationId,
	}

	if _, loaded := l.EntriesByJupyterMsgId.LoadOrStore(msgId, entry); loaded {
		return fmt.Errorf("%w: entry already exists for request with jupyter message ID \"%s\"",
			ErrRequestLogEntryExists, msgId)
	}

	if _, loaded := l.EntriesByRequestId.LoadOrStore(msg.RequestId, entry); loaded {
		return fmt.Errorf("%w: entry already exists for request with request ID \"%s\"",
			ErrRequestLogEntryExists, msg.RequestId)
	}

	var (
		requestsForKernel hashmap.HashMap[string, *RequestLogEntry] // Jupyter Message ID to RequestLogEntry.
		loaded            bool
	)
	requestsForKernel, loaded = l.RequestsPerKernel.Load(msg.DestinationId)
	if !loaded {
		requestsForKernel = hashmap.NewCornelkMap[string, *RequestLogEntry](64)
		l.RequestsPerKernel.Store(msg.DestinationId, requestsForKernel)
	}

	if _, loaded := requestsForKernel.LoadOrStore(msgId, entry); loaded {
		return fmt.Errorf("%w: entry for request with jupyter message ID \"%s\" already registered under kernel \"%s\"",
			ErrRequestLogEntryExists, msgId, msg.DestinationId)
	}

	return nil
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
