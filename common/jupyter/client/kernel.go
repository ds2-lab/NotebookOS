package client

import (
	"context"
	"fmt"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	heartbeatInterval = time.Second
)

// KernelClient offers a simple interface to communicate with a kernel.
// All sockets except IOPub are connected on dialing.
type KernelClient struct {
	*server.BaseServer
	server *server.AbstractServer
	id     string
	status types.KernelStatus

	echoHandler MessageHandler
}

func NewKernelClient(id string, ctx context.Context, info *types.ConnectionInfo) *KernelClient {
	client := &KernelClient{
		id: id,
		server: server.New(ctx, info, func(s *server.AbstractServer) {
			s.Sockets.Control = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ControlPort}
			s.Sockets.Shell = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.ShellPort}
			s.Sockets.Stdin = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.StdinPort}
			s.Sockets.HB = &types.Socket{Socket: zmq4.NewReq(s.Ctx), Port: info.HBPort}
			// IOPub is lazily initialized for different subclasses.
		}),
		status: types.KernelStatusInitializing,
	}
	client.BaseServer = client.server.Server()
	client.server.Log = config.GetLogger(fmt.Sprintf("Kernel %s ", id))
	client.echoHandler = client.defaultEchoHandler

	go client.validate()

	return client
}

func (c *KernelClient) ID() string {
	return c.id
}

func (c *KernelClient) Status() types.KernelStatus {
	return c.status
}

func (c *KernelClient) SetEchoHandler(handler MessageHandler) (old MessageHandler) {
	old = c.echoHandler
	c.echoHandler = handler
	return
}

// Close closes the zmq sockets.
func (c *KernelClient) Close() {
	c.BaseServer.Close()
	for _, socket := range c.server.Sockets.All {
		if socket != nil {
			socket.Close()
		}
	}
}

// dial connects to specified sockets
func (c *KernelClient) dial(sockets ...*types.Socket) error {
	// Start listening on all sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.server.Meta.Transport, c.server.Meta.IP)
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		err := socket.Socket.Dial(fmt.Sprintf(address, socket.Port))
		if err != nil {
			return fmt.Errorf("could not connect to kernel socket(port:%d): %w", socket.Port, err)
		}
	}
	return nil
}

func (c *KernelClient) validate() {
	timer := time.NewTimer(0)

	for {
		select {
		case <-c.server.Ctx.Done():
			c.status = types.KernelStatusExited
			return
		case <-timer.C:
			// Wait for heartbeat connection to
			if err := c.dial(c.server.Sockets.HB); err != nil {
				timer.Reset(heartbeatInterval)
				continue
			}

			if err := c.dial(c.server.Sockets.All[types.HBMessage:]...); err != nil {
				c.server.Log.Warn(err.Error())
				c.Close()
				c.status = types.KernelStatusExited
			}

			go c.server.Serve(types.HBMessage, c.handleMsg)

			c.status = types.KernelStatusRunning
			return
		}
	}
}

func (c *KernelClient) defaultEchoHandler(_ ClientInfo, _ *zmq4.Msg) error {
	return nil
}

func (c *KernelClient) handleMsg(typ types.MessageType, msg *zmq4.Msg) error {
	switch typ {
	case types.HBMessage:
		return c.echoHandler(c, msg)
	}

	return ErrHandlerNotImplemented
}
