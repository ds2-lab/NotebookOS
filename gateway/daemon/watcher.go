package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	url string = "http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"],\"label\":[\"app=distributed_cluster\"]}"
)

type DockerContainerStartedNotification struct {
	FullContainerId  string `json:"full-container-id"`  // FullContainerId is the full, non-truncated Docker container ID.
	ShortContainerId string `json:"short-container-id"` // ShortContainerId is the first 12 characters of the FullContainerId.
	KernelId         string `json:"kernel-id"`          // KernelId is the associated KernelId.
}

// DockerContainerWatcher monitors for "container created" events by dialing
// the Docker socket and listening for events that are emitted.
type DockerContainerWatcher struct {
	// Mapping from Kernel ID to a slice of channels, each of which would correspond to a scale-up operation.
	channels hashmap.BaseHashMap[string, []chan string]

	// Mapping from Kernel ID to []*DockerContainerStartedNotification structs for which there was no channel registered
	// when the notification was received. This exists so the Docker container ID can be retrieved for the initial
	// kernel replica containers that are created when a kernel is created for the first time.
	//
	// Note that we use a slice of *DockerContainerStartedNotification here because there will be N unwatched
	// notifications for a kernel, where N is the number of replicas of that kernel.
	unwatchedNotifications hashmap.BaseHashMap[string, []*DockerContainerStartedNotification]

	// Docker Swarm / Docker Compose project name.
	projectName string
	// Docker network name.
	networkName string

	mu sync.Mutex

	log logger.Logger
}

func NewDockerContainerWatcher(projectName string) *DockerContainerWatcher {
	networkName := fmt.Sprintf("%s_default", projectName)
	watcher := &DockerContainerWatcher{
		projectName:            projectName,
		networkName:            networkName,
		channels:               hashmap.NewCornelkMap[string, []chan string](64),
		unwatchedNotifications: hashmap.NewCornelkMap[string, []*DockerContainerStartedNotification](64),
	}

	config.InitLogger(&watcher.log, watcher)

	go watcher.monitor()

	return watcher
}

// RegisterChannel registers a channel that is used to notify waiting goroutines that the Pod/Container has started.
func (w *DockerContainerWatcher) RegisterChannel(kernelId string, startedChan chan string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Store the new channel in the mapping.
	channels, ok := w.channels.Load(kernelId)
	if !ok {
		channels = make([]chan string, 0, 4)
	}
	channels = append(channels, startedChan)
	w.channels.Store(kernelId, channels)
}

// LoadAndDeleteUnwatchedNotifications attempts to load a slice of *DockerContainerStartedNotification structs for
// the specified kernel from the mapping of unwatched notifications.
//
// If this slice exists and contains at least `expected` *DockerContainerStartedNotification structs, then the
// slice is removed from the mapping and returned.
//
// If the slice either does not exist, or it exists but contains less than expected entries,
// then this method simply returns nil.
//
// If the slice exists and contains more than `expected` entries, then this method will panic.
//
// TODO: How to determine which scheduling.Container instance corresponds to which Container ID...?
func (w *DockerContainerWatcher) LoadAndDeleteUnwatchedNotifications(kernelId string, expected int) []*DockerContainerStartedNotification {
	w.mu.Lock()
	defer w.mu.Unlock()

	notifications, loaded := w.unwatchedNotifications.Load(kernelId)
	if !loaded {
		// Entry for specified kernel does not exist --> return nil.
		return nil
	}

	if len(notifications) < expected {
		// Exists but contains less than expected entries --> return nil.
		return nil
	} else if len(notifications) > expected {
		// Exists and contains more than `expected` entries --> panic.
		panic(fmt.Sprintf("Expected %d notification(s) for kernel \"%s\", but we have %d.", expected, kernelId, len(notifications)))
	} else {
		// Exists and contains at least `expected` entries --> delete entry and return slice.
		w.unwatchedNotifications.Delete(kernelId)
		return notifications
	}
}

// monitor is the main loop of the DockerContainerWatcher and is automatically
// called in a separate goroutine in the  NewDockerContainerWatcher function.
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

			// Notify that the Container has been created by sending its name over the channel.
			notification := &DockerContainerStartedNotification{
				FullContainerId:  fullContainerId,
				ShortContainerId: shortContainerId,
				KernelId:         kernelId,
			}

			////////////////////////////
			// Start Critical Section //
			////////////////////////////
			w.mu.Lock()

			channels, ok := w.channels.Load(kernelId)

			if !ok || len(channels) == 0 {
				w.log.Debug("No scale-up waiters for kernel %s", kernelId)

				var unwatchedNotificationsForKernel []*DockerContainerStartedNotification
				// Check if we already have an entry for unwatched notifications for this kernel.
				if unwatchedNotificationsForKernel, ok = w.unwatchedNotifications.Load(kernelId); !ok {
					// If we do not have such an entry, then we'll create one. First, instantiate the slice.
					unwatchedNotificationsForKernel = make([]*DockerContainerStartedNotification, 0, 3)
				}

				// Add the notification to the slice.
				unwatchedNotificationsForKernel = append(unwatchedNotificationsForKernel, notification)

				// Store the slice back to the unwatched notifications map.
				w.unwatchedNotifications.Store(kernelId, unwatchedNotificationsForKernel)

				w.mu.Unlock()
				continue
			}

			w.log.Debug("Notifying waiters that new container %s for kernel %s has been created.", shortContainerId, kernelId)

			// Notify the first wait group that a Container has started.
			// We only notify one of the wait groups, as each wait group corresponds to
			// a different scale-up operation and thus requires a unique Container to have been created.
			// We treat the slice of wait groups as FIFO queue.
			var channel chan string
			channel, channels = channels[0], channels[1:]

			// Add the remaining channels back.
			w.channels.Store(kernelId, channels)

			//////////////////////////
			// End Critical Section //
			//////////////////////////
			w.mu.Unlock()

			marshalled, err := json.Marshal(notification)
			if err != nil {
				panic(err)
			}

			channel <- string(marshalled)
		}
	}
}
