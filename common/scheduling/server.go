package scheduling

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/jupyter/types"
	"github.com/scusemua/distributed-notebook/common/metrics"
)

type Server interface {
	SendRequest(request types.Request, socket *types.Socket) error
	SetComponentId(id string)
	AssignMessagingMetricsProvider(messagingMetricsProvider metrics.MessagingMetricsProvider)
	RegisterAck(msg *types.JupyterMessage) (chan struct{}, bool)
	RegisterAckForRequest(req types.Request) (chan struct{}, bool)
	Socket(typ types.MessageType) *types.Socket
	GetSocketPort(typ types.MessageType) int
	SetIOPubSocket(iopub *types.Socket) error
	MessageAcknowledgementsEnabled() bool
	Context() context.Context
	SetContext(ctx context.Context)
	Close() error
}
