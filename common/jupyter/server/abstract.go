package server

import (
	"context"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/logger"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

type MessageHandler func(types.MessageType, *zmq4.Msg) error

// AbstractServer implements the basic socket serving useful for a Jupyter server. Embed this struct in your server implementation.
type AbstractServer struct {
	Meta *types.ConnectionInfo

	// ctx of this server and a func to cancel it.
	Ctx       context.Context
	CancelCtx func()

	// ZMQ sockets
	Sockets *types.JupyterSocket

	// logger
	Log logger.Logger
}

func New(ctx context.Context, info *types.ConnectionInfo, init func(server *AbstractServer)) *AbstractServer {
	var cancelCtx func()
	ctx, cancelCtx = context.WithCancel(ctx)

	server := &AbstractServer{
		Meta:      info,
		Ctx:       ctx,
		CancelCtx: cancelCtx,
		Sockets:   &types.JupyterSocket{},
	}
	init(server)
	server.Sockets.All = [5]*types.Socket{server.Sockets.HB, server.Sockets.Control, server.Sockets.Shell, server.Sockets.Stdin, server.Sockets.IOPub}

	return server
}

func (s *AbstractServer) Server() *BaseServer {
	return &BaseServer{s}
}

func (s *AbstractServer) Serve(typ types.MessageType, socket *types.Socket, handler MessageHandler) {
	chMsg := make(chan interface{})
	s.Log.Debug("Start serving %v messages", typ)
	go s.poll(typ, socket, chMsg)

	for {
		select {
		case <-s.Ctx.Done():
			return
		case msg := <-chMsg:
			if msg == nil {
				return
			}

			var err error
			switch v := msg.(type) {
			case error:
				err = v
			case *zmq4.Msg:
				err = handler(typ, v)
			}
			if err != nil {
				s.Log.Warn("Error on handle %v message: %v", typ, err)
			}
		}
	}
}

func (s *AbstractServer) ServeOnce(typ types.MessageType, socket *types.Socket, handler MessageHandler) {
	msg, err := socket.Recv()
	if err == nil {
		err = handler(typ, &msg)
	}

	if err != nil {
		s.Log.Warn("Error on handle %v message: %v", typ, err)
	}
}

func (s *AbstractServer) poll(typ types.MessageType, socket zmq4.Socket, chMsg chan<- interface{}) {
	defer close(chMsg)

	var msg interface{}
	for {
		got, err := socket.Recv()
		s.Log.Debug("Incoming %v message: %v.", typ, got)
		if err != nil {
			msg = err
		} else {
			msg = &got
		}
		select {
		case chMsg <- msg:
		// Quit on router closed.
		case <-s.Ctx.Done():
			return
		}
	}
}
