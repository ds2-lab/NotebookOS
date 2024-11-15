package daemon

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/docker_events/observer"
	"net"
)

// RemoteDockerEventAggregator runs a TCP server to receive docker events that occur on remote nodes
// when running in Docker Swarm mode.
type RemoteDockerEventAggregator struct {
	log logger.Logger

	// consumer consumes the events received by the RemoteDockerEventAggregator.
	consumer observer.EventConsumer

	port int
}

func NewRemoteDockerEventAggregator(port int, consumer observer.EventConsumer) *RemoteDockerEventAggregator {
	aggregator := &RemoteDockerEventAggregator{
		port:     port,
		consumer: consumer,
	}
	config.InitLogger(&aggregator.log, aggregator)

	return aggregator
}

// Start starts the TCP server. Should be called from its own Goroutine.
func (a *RemoteDockerEventAggregator) Start() {
	a.log.Debug("Launching Remote Docker Event Aggregator TCP server on port %d.", a.port)

	// listen on all interfaces
	ln, _ := net.Listen("tcp", fmt.Sprintf(":%d", a.port))

	for {
		// Accept a remote connection.
		conn, _ := ln.Accept()

		a.log.Debug("Accepted remote connection from %s", conn.RemoteAddr().String())

		// Spawn a goroutine to handle messages from that connection.
		go func() {
			for {
				reader := bufio.NewReader(conn)
				decoder := json.NewDecoder(reader)

				for {
					var evt map[string]interface{}
					if err := decoder.Decode(&evt); err != nil {
						a.log.Error("Error encountered while trying to decode docker event: %v", err)
						break
					}

					m, _ := json.MarshalIndent(evt, "", "  ")
					a.log.Debug("Received docker event:\n%s", m)

					a.consumer.ConsumeDockerEvent(evt)
				}
			}
		}()
	}
}
