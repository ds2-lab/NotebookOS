package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	cmap "github.com/orcaman/concurrent-map/v2"
)

const (
	url string = "http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"],\"network\":[\"%s\"]}"
)

type DockerContainerWatcher struct {
	channels *cmap.ConcurrentMap[string, []chan string] // Mapping from Kernel ID to a slice of channels, each of which would correspond to a scale-up operation.

	projectName string
	networkName string
	url         string

	log logger.Logger
}

func NewDockerContainerWatcher(projectName string) *DockerContainerWatcher {
	channels := cmap.New[[]chan string]()

	networkName := fmt.Sprintf("%s_default", projectName)
	watcher := &DockerContainerWatcher{
		projectName: projectName,
		networkName: networkName,
		channels:    &channels,
		url:         fmt.Sprintf(url, networkName),
	}

	config.InitLogger(&watcher.log, watcher)

	go watcher.monitor()

	return watcher
}

// Register a channel that is used to notify waiting goroutines that the Pod/Container has started.
func (w *DockerContainerWatcher) RegisterChannel(kernelId string, startedChan chan string) {
	// Store the new channel in the mapping.
	channels, ok := w.channels.Get(kernelId)
	if !ok {
		channels = make([]chan string, 0, 4)
	}
	channels = append(channels, startedChan)
	w.channels.Set(kernelId, channels)
}

func (w *DockerContainerWatcher) monitor() {
	ctx := context.Background()

	dialerFunc := func(proto, addr string) (conn net.Conn, err error) {
		return net.Dial("unix", "/var/run/docker.sock")
	}
	transport := &http.Transport{
		Dial: dialerFunc,
	}
	client := &http.Client{Transport: transport}

	w.log.Debug("Monitoring for Docker container-start events for project \"%s\", network \"%s\"", w.projectName, w.networkName)

	resp, err := client.Get(w.url)
	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(resp.Body)

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				panic(err)
			}
			return
		default:
			var containerCreationEvent map[string]interface{}
			if err := decoder.Decode(&containerCreationEvent); err != nil {
				w.log.Error("Error encountered while trying to decode container creation event: %v", err)
				time.Sleep(time.Millisecond * 250)
				continue
			}

			fullContainerId := containerCreationEvent["id"].(string)
			shortContainerId := fullContainerId[0:12]
			attributes := containerCreationEvent["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})
			kernelId := attributes["kernel_id"].(string)

			w.log.Debug("Docker Container %s for kernel %s has started running.", shortContainerId, kernelId)

			channels, ok := w.channels.Get(kernelId)

			if !ok || len(channels) == 0 {
				w.log.Debug("No scale-up waiters for kernel %s", kernelId)
				continue
			}

			w.log.Debug("Notifying waiters that new container %s for kernel %s has been created.", shortContainerId, kernelId)

			// Notify the first wait group that a Pod has started.
			// We only notify one of the wait groups, as each wait group corresponds to
			// a different scale-up operation and thus requires a unique Pod to have been created.
			// We treat the slice of wait groups as FIFO queue.
			var channel chan string
			channel, channels = channels[0], channels[1:]
			channel <- shortContainerId // Notify that the Pod has been created by sending its name over the channel.

			w.channels.Set(kernelId, channels)
		}
	}
}
