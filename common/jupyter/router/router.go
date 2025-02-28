package router

import (
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/types"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
)

const (
	ClusterGatewayRouter    string = "ClusterGatewayRouter"
	LocalDaemonRouterPrefix string = "LocalDaemon_"

	GatewayRetrySleepInterval     = time.Millisecond * 675
	LocalDaemonRetrySleepInterval = time.Millisecond * 550
)

// MessageHandler defines the interface of messages that a JupyterRouter can intercept and handle.
type MessageHandler func(Info, *messaging.JupyterMessage) error

type Info interface {
	messaging.JupyterServerInfo
}

// Provider defines the interface to provide handlers for a JupyterRouter.
type Provider interface {
	ControlHandler(Info, *messaging.JupyterMessage) error

	ShellHandler(Info, *messaging.JupyterMessage) error

	StdinHandler(Info, *messaging.JupyterMessage) error

	HBHandler(Info, *messaging.JupyterMessage) error
}

type Router struct {
	*server.BaseServer
	server *server.AbstractServer

	name string // Identifies the router server.

	log logger.Logger

	// handlers
	handlers []MessageHandler
}

func New(ctx context.Context, id string, opts *jupyter.ConnectionInfo, provider Provider,
	messageAcknowledgementsEnabled bool, name string, shouldAckMessages bool, nodeType metrics.NodeType, debugMode bool,
	metricsProvider server.MessagingMetricsProvider) *Router {

	router := &Router{
		name: name,
		server: server.New(ctx, opts, nodeType, func(s *server.AbstractServer) {
			var remoteComponentName string
			if name == ClusterGatewayRouter {
				remoteComponentName = "JupyterServer"
				s.RetrySleepInterval = GatewayRetrySleepInterval
			} else if strings.HasPrefix(name, LocalDaemonRouterPrefix) {
				remoteComponentName = "CGKC" // ClusterGatewayKernelClient
				s.RetrySleepInterval = LocalDaemonRetrySleepInterval
			} else {
				panic(fmt.Sprintf("Unrecognized name for router sockets: \"%s\"", name))
			}

			// We do not set handlers of the sockets here. Server routine will be started using a shared handler.
			s.Sockets.HB = messaging.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx, zmq4.WithTimeout(time.Millisecond*3500), zmq4.WithDialerMaxRetries(3), zmq4.WithDialerRetry(time.Millisecond*500), zmq4.WithDialerTimeout(time.Millisecond*5000)), opts.HBPort, messaging.HBMessage, fmt.Sprintf("Router-Router-HB[%s]", name), fmt.Sprintf("Remote-%s-HB", remoteComponentName))
			s.Sockets.Control = messaging.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx, zmq4.WithTimeout(time.Millisecond*3500), zmq4.WithDialerMaxRetries(3), zmq4.WithDialerRetry(time.Millisecond*500), zmq4.WithDialerTimeout(time.Millisecond*5000)), opts.ControlPort, messaging.ControlMessage, fmt.Sprintf("Router-Router-Ctrl[%s]", name), fmt.Sprintf("Remote-%s-Ctrl", remoteComponentName))
			s.Sockets.Shell = messaging.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx, zmq4.WithTimeout(time.Millisecond*3500), zmq4.WithDialerMaxRetries(3), zmq4.WithDialerRetry(time.Millisecond*500), zmq4.WithDialerTimeout(time.Millisecond*5000)), opts.ShellPort, messaging.ShellMessage, fmt.Sprintf("Router-Router-Shell[%s]", name), fmt.Sprintf("Remote-%s-Shell", remoteComponentName))
			s.Sockets.Stdin = messaging.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx, zmq4.WithTimeout(time.Millisecond*3500), zmq4.WithDialerMaxRetries(3), zmq4.WithDialerRetry(time.Millisecond*500), zmq4.WithDialerTimeout(time.Millisecond*5000)), opts.StdinPort, messaging.StdinMessage, fmt.Sprintf("Router-Router-Stdin[%s]", name), fmt.Sprintf("Remote-%s-Stdin", remoteComponentName))
			s.PrependId = true
			s.ReconnectOnAckFailure = false
			s.ShouldAckMessages = shouldAckMessages
			s.DebugMode = debugMode
			s.StatisticsAndMetricsProvider = metricsProvider
			s.MessageAcknowledgementsEnabled = messageAcknowledgementsEnabled
			s.Name = fmt.Sprintf("Router[%s] ", name)
			config.InitLogger(&s.Log, s.Name)
		}),
	}
	router.BaseServer = router.server.Server()
	router.handlers = make([]MessageHandler, len(router.server.Sockets.All))
	if provider != nil {
		router.AddHandler(messaging.ControlMessage, provider.ControlHandler)
		router.AddHandler(messaging.ShellMessage, provider.ShellHandler)
		router.AddHandler(messaging.StdinMessage, provider.StdinHandler)
		router.AddHandler(messaging.HBMessage, provider.HBHandler)
	}

	if id != "" {
		router.SetComponentId(id)
	}

	config.InitLogger(&router.log, router.name)
	return router
}

// AssignPrometheusManager sets the StatisticsAndMetricsProvider on the server(s) encapsulated by the Router.
func (g *Router) AssignPrometheusManager(messagingMetricsProvider server.MessagingMetricsProvider) {
	g.server.StatisticsAndMetricsProvider = messagingMetricsProvider

	// I think this is actually essentially changing the same field, as I think the two structs/fields here
	// are actually the same variable (via pointers), but nevertheless...
	g.BaseServer.AssignMessagingMetricsProvider(messagingMetricsProvider)
}

// SetComponentId sets the ComponentId of the underlying AbstractServer (and BaseServer, but I think they
// are actually one and the same?)
func (g *Router) SetComponentId(id string) {
	g.server.ComponentId = id

	// I think this is actually essentially changing the same field, as I think the two structs/fields here
	// are actually the same variable (via pointers), but nevertheless...
	g.BaseServer.SetComponentId(id)
}

func (g *Router) ShouldAckMessages() bool {
	return g.server.ShouldAckMessages
}

func (g *Router) ConnectionInfo() *jupyter.ConnectionInfo {
	return g.server.Meta
}

//func (g *Router) RequestLog() *metrics.RequestLog {
//	return g.server.RequestLog
//}

// String returns the information for logging.
func (g *Router) String() string {
	return "router"
}

// Start initializes the zmq sockets and starts the service.
func (g *Router) Start() error {
	// Start listening on all sockets.
	for _, socket := range g.server.Sockets.All {
		if socket == nil {
			continue
		}

		g.server.Log.Debug("Listening on %v socket now.", socket.Type.String())

		err := g.server.Listen(socket)
		if err != nil {
			g.server.Log.Error("Error while trying to listen on %v socket %s (port=%d): %v", socket.Type, socket.Name, socket.Port, err)
			return fmt.Errorf("could not listen on router socket (port:%d): %w", socket.Port, err)
		}

		// defer socket.Socket.Close()
	}

	// Now listeners are ready, start serving.
	for _, socket := range g.server.Sockets.All {
		if socket == nil {
			continue
		}

		g.server.Log.Debug("Serving %v socket with shared handler (Router::handleMsg) now.", socket.Type.String())

		// socket.Handler has not been set, use shared handler.
		go g.server.Serve(g, socket, g.handleMsg)
	}

	<-g.server.Ctx.Done()

	// Close all the sockets.
	for idx, socket := range g.server.Sockets.All {
		if socket == nil {
			g.server.Log.Warn("Router found nil socket (socket #%d) when attempting to close all sockets...", idx)
			continue
		}
		if socket.Socket != nil {
			_ = socket.Socket.Close()
		}
	}

	return nil
}

func (g *Router) ID() string { return g.Name() }

func (g *Router) Name() string {
	return g.name
}

func (g *Router) AddHandler(typ messaging.MessageType, handler MessageHandler) {
	if g.handlers[typ] != nil {
		handler = func(oldHandler MessageHandler, newHandler MessageHandler) MessageHandler {
			return func(sockets Info, msg *messaging.JupyterMessage) error {
				err := newHandler(sockets, msg)
				if err == nil {
					return oldHandler(sockets, msg)
				} else if errors.Is(err, types.ErrStopPropagation) {
					return nil
				} else {
					return err
				}
			}
		}(g.handlers[typ], handler)
	}
	g.handlers[typ] = handler
}

func (g *Router) Close() error {
	if g.BaseServer != nil {
		err := g.BaseServer.Close()
		if err != nil {
			g.log.Warn("Error while closing BaseServer of router '%s': %v", g.name, err)
		}
	}

	// Sockets will be closed on Start() existing.
	return nil
}

func (g *Router) handleMsg(_ messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	handler := g.handlers[typ]
	if handler != nil {
		return handler(g, msg)
	}

	return nil
}
