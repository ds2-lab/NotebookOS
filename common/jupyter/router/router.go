package router

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	commonTypes "github.com/zhangjyr/distributed-notebook/common/types"
)

const (
	ClusterGatewayRouter    string = "ClusterGatewayRouter"
	LocalDaemonRouterPrefix string = "LocalDaemon_"

	GatewayRetrySleepInterval     = time.Millisecond * 675
	LocalDaemonRetrySleepInterval = time.Millisecond * 550
)

type Router struct {
	*server.BaseServer
	server *server.AbstractServer

	name string // Identifies the router server.

	// handlers
	handlers []RouterMessageHandler
}

func New(ctx context.Context, opts *types.ConnectionInfo, provider RouterProvider, messageAcknowledgementsEnabled bool,
	name string, shouldAckMessages bool, nodeType metrics.NodeType, debugMode bool) *Router {

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
			s.Sockets.HB = types.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx), opts.HBPort, types.HBMessage, fmt.Sprintf("Router-Router-HB[%s]", name), fmt.Sprintf("Remote-%s-HB", remoteComponentName))
			s.Sockets.Control = types.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx), opts.ControlPort, types.ControlMessage, fmt.Sprintf("Router-Router-Ctrl[%s]", name), fmt.Sprintf("Remote-%s-Ctrl", remoteComponentName))
			s.Sockets.Shell = types.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx), opts.ShellPort, types.ShellMessage, fmt.Sprintf("Router-Router-Shell[%s]", name), fmt.Sprintf("Remote-%s-Shell", remoteComponentName))
			s.Sockets.Stdin = types.NewSocketWithRemoteName(zmq4.NewRouter(s.Ctx), opts.StdinPort, types.StdinMessage, fmt.Sprintf("Router-Router-Stdin[%s]", name), fmt.Sprintf("Remote-%s-Stdin", remoteComponentName))
			s.PrependId = true
			s.ReconnectOnAckFailure = false
			s.ShouldAckMessages = shouldAckMessages
			s.DebugMode = debugMode
			s.MessageAcknowledgementsEnabled = messageAcknowledgementsEnabled
			s.Name = fmt.Sprintf("Router-%s", name)
			config.InitLogger(&s.Log, s.Name)
		}),
	}
	router.BaseServer = router.server.Server()
	router.handlers = make([]RouterMessageHandler, len(router.server.Sockets.All))
	if provider != nil {
		router.AddHandler(types.ControlMessage, provider.ControlHandler)
		router.AddHandler(types.ShellMessage, provider.ShellHandler)
		router.AddHandler(types.StdinMessage, provider.StdinHandler)
		router.AddHandler(types.HBMessage, provider.HBHandler)
	}
	return router
}

// AssignPrometheusManager sets the MessagingMetricsProvider on the server(s) encapsulated by the Router.
func (g *Router) AssignPrometheusManager(messagingMetricsProvider metrics.MessagingMetricsProvider) {
	g.server.MessagingMetricsProvider = messagingMetricsProvider

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

func (g *Router) ConnectionInfo() *types.ConnectionInfo {
	return g.server.Meta
}

func (g *Router) RequestLog() *metrics.RequestLog {
	return g.server.RequestLog
}

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
	for _, socket := range g.server.Sockets.All {
		if socket == nil {
			g.server.Log.Warn("Router found nil socket when attempting to close all sockets...")
			continue
		}
		if socket.Socket != nil {
			_ = socket.Socket.Close()
		}
	}

	return nil
}

func (g *Router) Name() string {
	return g.name
}

func (g *Router) AddHandler(typ types.MessageType, handler RouterMessageHandler) {
	if g.handlers[typ] != nil {
		handler = func(oldHandler RouterMessageHandler, newHandler RouterMessageHandler) RouterMessageHandler {
			return func(sockets RouterInfo, msg *types.JupyterMessage) error {
				err := newHandler(sockets, msg)
				if err == nil {
					return oldHandler(sockets, msg)
				} else if errors.Is(err, commonTypes.ErrStopPropagation) {
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
	g.BaseServer.Close()
	// Sockets will be closed on Start() existing.
	return nil
}

func (g *Router) handleMsg(_ types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
	handler := g.handlers[typ]
	if handler != nil {
		return handler(g, msg)
	}

	return nil
}
