package daemon

import (
	"github.com/zhangjyr/distributed-notebook/common/docker_events/observer"
	"sync"
)

// ContainerStartedNotificationManager keeps track of observer.ContainerStartedNotification structs associated with
// various kernels. Specifically, it maintains the most recent notification associated with a particular kernel.
//
// In order to ensure that the current notification maintained by a ContainerStartedNotificationManager is up to date,
// the Local Daemon must clear any existing notifications within its ContainerStartedNotificationManager before
// launching a new kernel. Failure to do so could cause errors, such as if the Local Daemon receives a registration
// request from the kernel before the new "container created" notification is received and stored in the
// ContainerStartedNotificationManager.
type ContainerStartedNotificationManager struct {
	mu sync.Mutex

	// notifications is a map from kernel ID to the latest *observer.ContainerStartedNotification received for
	// a replica/container associated with that kernel.
	notifications map[string]*observer.ContainerStartedNotification

	// notificationChannels is used to deliver incoming *observer.ContainerStartedNotification structs to awaiting
	// goroutines when there are no notifications available at the time that the GetAndDeleteNotification method
	// is called.
	notificationChannels map[string]chan *observer.ContainerStartedNotification
}

func NewContainerStartedNotificationManager() *ContainerStartedNotificationManager {
	return &ContainerStartedNotificationManager{
		notifications:        make(map[string]*observer.ContainerStartedNotification),
		notificationChannels: make(map[string]chan *observer.ContainerStartedNotification),
	}
}

// GetNotification returns the *observer.ContainerStartedNotification associated with the specified kernel,
// if there is one.
func (m *ContainerStartedNotificationManager) GetNotification(kernelId string) (*observer.ContainerStartedNotification, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	notification, loaded := m.notifications[kernelId]
	return notification, loaded
}

// GetAndDeleteNotification returns the *observer.ContainerStartedNotification associated with the specified kernel,
// if there is one. The notification is deleted from the mapping maintained by the ContainerStartedNotificationManager
// before the GetAndDeleteNotification method returns.
//
// If there is no notification at the time that the GetAndDeleteNotification is called, then this will wait for the
// next notification associated with the specified kernel to be delivered (i.e., it will block).
func (m *ContainerStartedNotificationManager) GetAndDeleteNotification(kernelId string) *observer.ContainerStartedNotification {
	m.mu.Lock()
	defer m.mu.Unlock()

	notification, loaded := m.notifications[kernelId]

	if loaded {
		// Delete the notification.
		delete(m.notifications, kernelId)
	} else {
		// Create a channel and put it in the channel mapping so that, when we receive the notification,
		// the channel will be there, and it can be delivered to us.
		channel := make(chan *observer.ContainerStartedNotification)
		m.notificationChannels[kernelId] = channel

		// Unlock and wait for the notification to come over the channel.
		m.mu.Unlock()
		notification = <-channel

		// Lock again, close the channel, and remove it from the mapping.
		// We do not need to remove the notification from the mapping, as it was never added in the first place.
		m.mu.Lock()
		close(channel)
		m.notificationChannels[kernelId] = nil
	}

	return notification
}

// DeliverNotification sets the notification associated with a particular kernel. The kernel ID is retrieved from the
// notification itself.
//
// This first checks if there is a goroutine blocked waiting for the notification. If so, then the notification
// is delivered directly to that goroutine, and it is never added to the internal mapping. The internal mapping
// will be set to nil, in case there was an old notification there.
func (m *ContainerStartedNotificationManager) DeliverNotification(notification *observer.ContainerStartedNotification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If there is a channel -- which indicates that some goroutine is blocked waiting for this notification,
	// then deliver the notification directly to that goroutine (via the channel) and nullify the mapping.
	if channel, channelLoaded := m.notificationChannels[notification.KernelId]; channelLoaded {
		channel <- notification
		m.notifications[notification.KernelId] = nil
		return
	}

	// Update the mapping.
	m.notifications[notification.KernelId] = notification
}

// DeleteNotification deletes the latest notification associated with the specified kernel.
//
// If there was a notification associated with the specified kernel, then DeleteNotification returns true.
// If there was no notification associated with the specified kernel, then DeleteNotification returns false.
func (m *ContainerStartedNotificationManager) DeleteNotification(kernelId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.notifications[kernelId]
	if !ok {
		return false /* Notification was not deleted */
	}

	delete(m.notifications, kernelId)
	return true /* Notification was deleted */
}
