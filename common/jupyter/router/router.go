package router

import (
	"context"
	"fmt"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	AllInterfaces = "0.0.0.0"
)

type Router struct {
	*server.BaseServer
	server *server.AbstractServer

	// handlers
	handlers []MessageHandler
}

func New(ctx context.Context, opts *types.ConnectionInfo, provider RouterProvider) *Router {
	router := &Router{
		server: server.New(ctx, opts, func(s *server.AbstractServer) {
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.HBPort}
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewRouter(s.Ctx), Port: opts.StdinPort}
			// IOPub is a session specific socket, so it is not initialized here.
		}),
	}
	router.BaseServer = router.server.Server()
	router.handlers = make([]MessageHandler, len(router.server.Sockets.All))
	config.InitLogger(&router.server.Log, router)
	if provider != nil {
		router.AddHandler(types.ControlMessage, provider.ControlHandler)
		router.AddHandler(types.ShellMessage, provider.ShellHandler)
		router.AddHandler(types.StdinMessage, provider.StdinHandler)
		router.AddHandler(types.HBMessage, provider.HBHandler)
	}
	return router
}

// start initializes the zmq sockets and starts the service.
func (g *Router) Start() error {
	// Start listening on all sockets.
	address := fmt.Sprintf("%v://%v:%%v", g.server.Meta.Transport, AllInterfaces)
	for _, socket := range g.server.Sockets.All {
		if socket == nil {
			continue
		}

		err := socket.Socket.Listen(fmt.Sprintf(address, socket.Port))
		if err != nil {
			return fmt.Errorf("could not listen on router socket(port:%d): %w", socket.Port, err)
		}

		defer socket.Socket.Close()
	}

	// Now listeners are ready, start servering.
	for i, socket := range g.server.Sockets.All {
		if socket == nil {
			continue
		}

		go g.server.Serve(types.MessageType(i), g.handleMsg)
	}

	<-g.server.Ctx.Done()
	return nil
}

func (g *Router) AddHandler(typ types.MessageType, handler MessageHandler) {
	if g.handlers[typ] != nil {
		handler = func(oldHandler MessageHandler, newHandler MessageHandler) MessageHandler {
			return func(sockets RouterInfo, msg *zmq4.Msg) error {
				err := newHandler(sockets, msg)
				if err != nil {
					return oldHandler(sockets, msg)
				} else if err == ErrStopPropagation {
					return nil
				} else {
					return err
				}
			}
		}(g.handlers[typ], handler)
	}
	g.handlers[typ] = handler
}

func (g *Router) handleMsg(typ types.MessageType, msg *zmq4.Msg) error {
	handler := g.handlers[typ]
	if handler != nil {
		return handler(g, msg)
	}

	return nil
}
