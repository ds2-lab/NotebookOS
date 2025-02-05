package observer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
)

const (
	url string = "http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"],\"label\":[\"app=distributed_cluster\"]}"
)

type ContainerStartedNotification struct {
	FullContainerId  string `json:"full-container-id"`  // FullContainerId is the full, non-truncated Docker container ID.
	ShortContainerId string `json:"short-container-id"` // ShortContainerId is the first 12 characters of the FullContainerId.
	KernelId         string `json:"kernel_id"`          // KernelId is the associated KernelId.
}

// EventConsumer defines the interface for an entity which consumes the events collected/observed by a EventObserver.
type EventConsumer interface {
	// ConsumeDockerEvent consumes an event collected/observed by a EventObserver.
	// Any processing of the event should be quick and non-blocking.
	ConsumeDockerEvent(event map[string]interface{})
}

// EventObserver monitors for "container created" events by dialing
// the Docker socket and listening for events that are emitted.
//
// This is only used for Docker Compose clusters.
type EventObserver struct {
	log            logger.Logger
	eventConsumers map[string]EventConsumer

	// Docker Swarm / Docker Compose project name.
	projectName string
	// Docker network name.
	networkName string

	mu sync.Mutex
}

func NewEventObserver(projectName string, networkName string) *EventObserver {
	if networkName == "" {
		networkName = fmt.Sprintf("%s_default", projectName)
	}

	watcher := &EventObserver{
		projectName:    projectName,
		networkName:    networkName,
		eventConsumers: make(map[string]EventConsumer),
	}

	config.InitLogger(&watcher.log, watcher)

	return watcher
}

// Start initiates the monitoring procedure in its own goroutine.
func (w *EventObserver) Start() {
	go w.monitor()
}

// RegisterEventConsumer registers an EventConsumer to which observed/collected events will be delivered via the
// EventConsumer's ConsumeDockerEvent function.
func (w *EventObserver) RegisterEventConsumer(id string, consumer EventConsumer) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.eventConsumers[id] = consumer
}

// UnregisterEventConsumer unregisters an EventConsumer such that it will no longer receive any events
// collected/consumed by the EventObserver.
func (w *EventObserver) UnregisterEventConsumer(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.eventConsumers, id)
}

// monitor is the main loop of the EventObserver and is automatically
// called in a separate goroutine in the  NewEventObserver function.
func (w *EventObserver) monitor() {
	ctx := context.Background()
	dialerFunc := func(ctx context.Context, proto string, addr string) (conn net.Conn, err error) {
		return net.Dial("unix", "/var/run/docker.sock")
	}
	transport := &http.Transport{
		DialContext: dialerFunc,
	}
	client := &http.Client{Transport: transport}

	w.log.Debug("Monitoring for Docker container-start events for project \"%s\", network \"%s\"", w.projectName, w.networkName)

	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	w.log.Debug("Successfully issued HTTP GET request. Processing response(s) now.")

	decoder := json.NewDecoder(resp.Body)

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				panic(err)
			}
			w.log.Debug("ctx.Done, but no error. Exiting now.")
			return
		default:
			var containerCreationEvent map[string]interface{}
			if err := decoder.Decode(&containerCreationEvent); err != nil {
				w.log.Error("Error encountered while trying to decode container creation event: %v", err)
				time.Sleep(time.Millisecond * 250)
				continue
			}

			w.log.Debug("Received container-creation event: %v", containerCreationEvent)

			w.mu.Lock()

			toRemove := make([]string, 0)
			for id, consumer := range w.eventConsumers {
				if consumer == nil {
					w.log.Warn("Found nil consumer in EventConsumers map with ID=%s", id)
					toRemove = append(toRemove, id)
					continue
				}

				consumer.ConsumeDockerEvent(containerCreationEvent)
			}

			for _, id := range toRemove {
				w.log.Warn("Removing nil entry from EventConsumers map with ID=%s", id)
				delete(w.eventConsumers, id)
			}

			w.mu.Unlock()
		}
	}
}
