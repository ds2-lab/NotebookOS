package router

import (
	"context"
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type Router struct {
	*server.BaseServer
	server *server.AbstractServer

	name string // Identifies the router server.

	// handlers
	handlers []RouterMessageHandler
}

func New(ctx context.Context, opts *types.ConnectionInfo, provider RouterProvider, name string) *Router {
	router := &Router{
		name: name,
		server: server.New(ctx, opts, true, func(s *server.AbstractServer) {
			// We do not set handlers of the sockets here. Server routine will be started using a shared handler.
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.HBPort}
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.StdinPort}
			// s.Sockets.Ack = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.AckPort}
			// IOPub is a session specific socket, so it is not initialized here.
		}),
	}
	router.BaseServer = router.server.Server()
	router.handlers = make([]RouterMessageHandler, len(router.server.Sockets.All))
	config.InitLogger(&router.server.Log, router)
	if provider != nil {
		router.AddHandler(types.ControlMessage, provider.ControlHandler)
		router.AddHandler(types.ShellMessage, provider.ShellHandler)
		router.AddHandler(types.StdinMessage, provider.StdinHandler)
		router.AddHandler(types.HBMessage, provider.HBHandler)
		router.AddHandler(types.AckMessage, provider.AckHandler)
	}
	return router
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
			return fmt.Errorf("could not listen on router socket(port:%d): %w", socket.Port, err)
		}

		defer socket.Socket.Close()
	}

	// Now listeners are ready, start servering.
	for _, socket := range g.server.Sockets.All {
		if socket == nil {
			continue
		}

		g.server.Log.Debug("Serving %v socket with shared handler (Router::handleMsg) now.", socket.Type.String())

		// socket.Handler has not been set, use shared handler.
		go g.server.Serve(g, socket, g, g.handleMsg)
	}

	<-g.server.Ctx.Done()
	return nil
}

func (g *Router) Name() string {
	return g.name
}

func (g *Router) RequestDestID() string {
	return g.name
}

func (g *Router) AddHandler(typ types.MessageType, handler RouterMessageHandler) {
	if g.handlers[typ] != nil {
		handler = func(oldHandler RouterMessageHandler, newHandler RouterMessageHandler) RouterMessageHandler {
			return func(sockets RouterInfo, msg *zmq4.Msg) error {
				err := newHandler(sockets, msg)
				if err == nil {
					return oldHandler(sockets, msg)
				} else if err == types.ErrStopPropagation {
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

func (g *Router) handleMsg(_ types.JupyterServerInfo, typ types.MessageType, msg *zmq4.Msg) error {
	handler := g.handlers[typ]
	if handler != nil {
		return handler(g, msg)
	}

	return nil
}
