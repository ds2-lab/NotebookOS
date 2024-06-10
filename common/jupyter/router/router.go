package router

import (
	"context"
	"fmt"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/pebbe/zmq4"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type Router struct {
	*server.BaseServer
	server *server.AbstractServer

	// destMutex sync.Mutex

	name string // Identifies the router server.

	// handlers
	handlers []RouterMessageHandler
}

func New(ctx context.Context, opts *types.ConnectionInfo, provider RouterProvider, name string, shouldAckMessages bool) *Router {
	router := &Router{
		name: name,
		server: server.New(ctx, opts, func(s *server.AbstractServer) {
			// We do not set handlers of the sockets here. Server routine will be started using a shared handler.
			hb_socket, err := zmq4.NewSocket(zmq4.ROUTER)
			if err != nil {
				panic(err)
			}
			err = hb_socket.SetRouterMandatory(1)
			if err != nil {
				panic(err)
			}
			s.Sockets.HB = &types.Socket{Socket: hb_socket, Port: opts.HBPort, Name: fmt.Sprintf("Router-Router-HB[%s]", name)}

			ctrl_socket, err := zmq4.NewSocket(zmq4.ROUTER)
			if err != nil {
				panic(err)
			}
			err = ctrl_socket.SetRouterMandatory(1)
			if err != nil {
				panic(err)
			}
			s.Sockets.Control = &types.Socket{Socket: ctrl_socket, Port: opts.ControlPort, Name: fmt.Sprintf("Router-Router-Ctrl[%s]", name)}

			shell_socket, err := zmq4.NewSocket(zmq4.ROUTER)
			if err != nil {
				panic(err)
			}
			err = shell_socket.SetRouterMandatory(1)
			if err != nil {
				panic(err)
			}
			s.Sockets.Shell = &types.Socket{Socket: shell_socket, Port: opts.ShellPort, Name: fmt.Sprintf("Router-Router-Shell[%s]", name)}

			stdin_socket, err := zmq4.NewSocket(zmq4.ROUTER)
			if err != nil {
				panic(err)
			}
			err = stdin_socket.SetRouterMandatory(1)
			if err != nil {
				panic(err)
			}
			s.Sockets.Stdin = &types.Socket{Socket: stdin_socket, Port: opts.StdinPort, Name: fmt.Sprintf("Router-Router-Stdin[%s]", name)}

			s.PrependId = true
			s.ShouldAckMessages = shouldAckMessages
			s.Name = fmt.Sprintf("Router-%s", name)
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
	}
	return router
}

// String returns the information for logging.
func (g *Router) String() string {
	return "router"
}

// func (g *Router) Unlock() {
// 	g.destMutex.Unlock()
// }

// func (g *Router) Lock() {
// 	g.destMutex.Lock()
// }

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
		go g.server.Serve(g, socket, g, g.handleMsg, g.server.ShouldAckMessages)
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
			return func(sockets RouterInfo, msg [][]byte) error {
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

func (g *Router) handleMsg(_ types.JupyterServerInfo, typ types.MessageType, msg [][]byte) error {
	handler := g.handlers[typ]
	if handler != nil {
		return handler(g, msg)
	}

	return nil
}
