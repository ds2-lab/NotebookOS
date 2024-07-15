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
	url string = "http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"],\"label\":[\"app=distributed_cluster\"]}"
)

type DockerContainerWatcher struct {
	channels *cmap.ConcurrentMap[string, []chan string] // Mapping from Kernel ID to a slice of channels, each of which would correspond to a scale-up operation.

	projectName string
	networkName string

	log logger.Logger
}

func NewDockerContainerWatcher(projectName string) *DockerContainerWatcher {
	channels := cmap.New[[]chan string]()

	networkName := fmt.Sprintf("%s_default", projectName)
	watcher := &DockerContainerWatcher{
		projectName: projectName,
		networkName: networkName,
		channels:    &channels,
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

			fullContainerId := containerCreationEvent["id"].(string)
			shortContainerId := fullContainerId[0:12]
			attributes := containerCreationEvent["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})

			var kernelId string
			if val, ok := attributes["kernel_id"]; ok {
				kernelId = val.(string)
			} else {
				w.log.Debug("Docker Container %s related to the distributed cluster has started.", shortContainerId)
				continue
			}

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

// type dockerEvent struct {
// 	Action       string            `json:"Action"`
// 	Actor        *dockerEventActor `json:"Actor"`
// 	Type         string            `json:"Type"`
// 	From         string            `json:"from"`
// 	Id           string            `json:"id"`
// 	Scope        string            `json:"scope"`
// 	Status       string            `json:"create"`
// 	TimestampStr string            `json:"time"`
// 	TimeNanoStr  string            `json:"timeNano"`

// 	// cachedTimestamp    time.Time `json:"-"`
// 	// timestampConverted bool      `json:"-"`
// }

// func (e *dockerEvent) String() string {
// 	out, err := json.Marshal(e)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return string(out)
// }

// type dockerEventActor struct {
// 	Attributes map[string]interface{} `json:"Attributes"`
// 	Id         string                 `json:"ID"`
// }

// func (a *dockerEventActor) String() string {
// 	out, err := json.Marshal(a)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return string(out)
// }
