package daemon

import (
	"encoding/json"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"sync"
)

type ContainerStartedNotification struct {
	FullContainerId  string `json:"full-container-id"`  // FullContainerId is the full, non-truncated Docker container ID.
	ShortContainerId string `json:"short-container-id"` // ShortContainerId is the first 12 characters of the FullContainerId.
	KernelId         string `json:"kernel_id"`          // KernelId is the associated KernelId.
}

type DockerEventHandler struct {
	log logger.Logger
	mu  sync.Mutex

	// Mapping from Kernel ID to a slice of channels, each of which would correspond to a scale-up operation.
	channels hashmap.BaseHashMap[string, []chan string]

	// Mapping from Kernel ID to []*ContainerStartedNotification structs for which there was no channel registered
	// when the notification was received. This exists so the Docker container ID can be retrieved for the initial
	// kernel replica containers that are created when a kernel is created for the first time.
	//
	// Note that we use a slice of *ContainerStartedNotification here because there will be N unwatched
	// notifications for a kernel, where N is the number of replicas of that kernel.
	unwatchedNotifications hashmap.BaseHashMap[string, []*ContainerStartedNotification]
}

func NewDockerEventHandler() *DockerEventHandler {
	handler := &DockerEventHandler{
		channels:               hashmap.NewCornelkMap[string, []chan string](64),
		unwatchedNotifications: hashmap.NewCornelkMap[string, []*ContainerStartedNotification](64),
	}
	config.InitLogger(&handler.log, handler)

	return handler
}

// RegisterChannel registers a channel that is used to notify waiting goroutines that the Pod/Container has started.
func (h *DockerEventHandler) RegisterChannel(kernelId string, startedChan chan string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Store the new channel in the mapping.
	channels, ok := h.channels.Load(kernelId)
	if !ok {
		channels = make([]chan string, 0, 4)
	}
	channels = append(channels, startedChan)
	h.channels.Store(kernelId, channels)
}

// LoadAndDeleteUnwatchedNotifications attempts to load a slice of *ContainerStartedNotification structs for
// the specified kernel from the mapping of unwatched notifications.
//
// If this slice exists and contains at least `expected` *ContainerStartedNotification structs, then the
// slice is removed from the mapping and returned.
//
// If the slice either does not exist, or it exists but contains less than expected entries,
// then this method simply returns nil.
//
// If the slice exists and contains more than `expected` entries, then this method will panic.
//
// TODO: How to determine which scheduling.Container instance corresponds to which Container ID...?
func (h *DockerEventHandler) LoadAndDeleteUnwatchedNotifications(kernelId string, expected int) []*ContainerStartedNotification {
	h.mu.Lock()
	defer h.mu.Unlock()

	notifications, loaded := h.unwatchedNotifications.Load(kernelId)
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
		h.unwatchedNotifications.Delete(kernelId)
		return notifications
	}
}

// ConsumeDockerEvent consumes an event collected/observed by a EventObserver.
// Any processing of the event should be quick and non-blocking.
func (h *DockerEventHandler) ConsumeDockerEvent(event map[string]interface{}) {
	fullContainerId := event["id"].(string)
	shortContainerId := fullContainerId[0:12]
	attributes := event["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})

	var kernelId string
	if val, ok := attributes["kernel_id"]; ok {
		kernelId = val.(string)
	} else {
		h.log.Debug("Docker Container %s related to the distributed cluster has started.", shortContainerId)
		return
	}

	h.log.Debug("Docker Container %s for kernel %s has started running.", shortContainerId, kernelId)

	// Notify that the Container has been created by sending its name over the channel.
	notification := &ContainerStartedNotification{
		FullContainerId:  fullContainerId,
		ShortContainerId: shortContainerId,
		KernelId:         kernelId,
	}

	////////////////////////////
	// Start Critical Section //
	////////////////////////////
	h.mu.Lock()

	channels, ok := h.channels.Load(kernelId)

	if !ok || len(channels) == 0 {
		h.log.Debug("No scale-up waiters for kernel %s", kernelId)

		var unwatchedNotificationsForKernel []*ContainerStartedNotification
		// Check if we already have an entry for unwatched notifications for this kernel.
		if unwatchedNotificationsForKernel, ok = h.unwatchedNotifications.Load(kernelId); !ok {
			// If we do not have such an entry, then we'll create one. First, instantiate the slice.
			unwatchedNotificationsForKernel = make([]*ContainerStartedNotification, 0, 3)
		}

		// Add the notification to the slice.
		unwatchedNotificationsForKernel = append(unwatchedNotificationsForKernel, notification)

		// Store the slice back to the unwatched notifications map.
		h.unwatchedNotifications.Store(kernelId, unwatchedNotificationsForKernel)

		h.mu.Unlock()
		return
	}

	h.log.Debug("Notifying waiters that new container %s for kernel %s has been created.", shortContainerId, kernelId)

	// Notify the first wait group that a Container has started.
	// We only notify one of the wait groups, as each wait group corresponds to
	// a different scale-up operation and thus requires a unique Container to have been created.
	// We treat the slice of wait groups as FIFO queue.
	var channel chan string
	channel, channels = channels[0], channels[1:]

	// Add the remaining channels back.
	h.channels.Store(kernelId, channels)

	//////////////////////////
	// End Critical Section //
	//////////////////////////
	h.mu.Unlock()

	marshalled, err := json.Marshal(notification)
	if err != nil {
		panic(err)
	}

	channel <- string(marshalled)
}
