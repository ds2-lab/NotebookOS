package main

import (
	"flag"
	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/common/docker_events/forwarder"
	"github.com/zhangjyr/distributed-notebook/common/docker_events/observer"
)

func main() {
	// Define the project-name flag with a default value and a description
	projectName := flag.String("project-name", "distributed_notebook", "The name of the Docker Swarm stack for the distributed notebook cluster.")
	networkName := flag.String("network-name", "traefik-public", "The name of the overlay network used in your Docker Swarm cluster.")
	remoteHost := flag.String("remote-host", "gateway", "IP of server to forward events to")
	remotePort := flag.Int("remote-port", 5821, "Port on which target server is listening")

	// Parse the flags
	flag.Parse()

	watcher := observer.NewEventObserver(*projectName, *networkName)
	eventForwarder := forwarder.NewEventForwarder(*remoteHost, *remotePort)

	watcher.RegisterEventConsumer(uuid.NewString(), eventForwarder)

	eventForwarder.ForwardEvents()
}
