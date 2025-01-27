package scheduling

import (
	"context"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
)

type Server interface {
	SendRequest(request messaging.Request, socket *messaging.Socket) error
	SetComponentId(id string)
	AssignMessagingMetricsProvider(messagingMetricsProvider server.MessagingMetricsProvider)
	RegisterAck(msg *messaging.JupyterMessage) (chan struct{}, bool)
	RegisterAckForRequest(req messaging.Request) (chan struct{}, bool)
	Socket(typ messaging.MessageType) *messaging.Socket
	GetSocketPort(typ messaging.MessageType) int
	SetIOPubSocket(iopub *messaging.Socket) error
	MessageAcknowledgementsEnabled() bool
	Context() context.Context
	SetContext(ctx context.Context)
	Close() error
}
