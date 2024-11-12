package forwarder

import (
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"net"
	"time"
)

type EventForwarder struct {
	// events are the events observed/collected by an ContainerCreatedEventCollector and delivered to us
	// via our ConsumeDockerEvent method.
	events chan map[string]interface{}

	// log is a simple logger.
	log logger.Logger

	// remoteHost is the IP of the server to which we're forwarding the events.
	remoteHost string

	// remotePort is the port on which the remote server to which we're forwarding the events is listening.
	remotePort int
}

func NewEventForwarder(remoteHost string, remotePort int) *EventForwarder {
	forwarder := &EventForwarder{
		events:     make(chan map[string]interface{}, 5),
		remoteHost: remoteHost,
		remotePort: remotePort,
	}

	config.InitLogger(&forwarder.log, forwarder)

	return forwarder
}

func (f *EventForwarder) ConsumeDockerEvent(evt map[string]interface{}) {
	f.events <- evt
}

// ForwardEvents to the configured remote server/consumer.
//
// This should be called from its own Goroutine unless it is intended to serve as the main thread of the program.
func (f *EventForwarder) ForwardEvents() {
	// Connect to the remote server.

	var (
		conn net.Conn
		err  error
	)

	attempts := 0
	connected := false

	address := fmt.Sprintf("%s:%d", f.remoteHost, f.remotePort)
	for conn == nil || !connected {
		f.log.Info("Connecting to remote server at address \"%s\" [attempt #%d]", address, attempts+1)

		conn, err = net.Dial("tcp", address)

		if err == nil {
			connected = true
			break
		}

		f.log.Error("Failed to connect to remote server at address \"%s\" on attempt %d because: %v",
			address, attempts+1, err)

		attempts += 1

		// Sleep for a bit before trying again.
		time.Sleep(time.Millisecond * 2500)
	}

	f.log.Info("Successfully connected to remote server at address \"%s\" [attempt #%d]", address, attempts+1)

	for {
		evt := <-f.events

		f.log.Info("Forwarding docker event now: %v", evt)

		encoded, err := json.Marshal(evt)
		if err != nil {
			f.log.Error("Failed to encode Docker event: %v", err)
			f.log.Error("Problematic event: %v", evt)
			continue
		}

		_, err = conn.Write(encoded)
		if err != nil {
			f.log.Error("Failed to write event to remote because: %v", err)
			continue
		}
	}
}
