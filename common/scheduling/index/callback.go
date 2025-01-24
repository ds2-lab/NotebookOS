package index

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"sync"
)

// ModificationCallback is a callback that can be registered on an index.
// Registered IndexChangedCallback instances are invoked when their associated action occurs,
// such as adding or removing a Host to/from the associated index.
type ModificationCallback func(scheduling.Host)

type CallbackManager struct {
	HostAddedCallbacks   map[string]ModificationCallback
	HostRemovedCallbacks map[string]ModificationCallback
	HostUpdatedCallbacks map[string]ModificationCallback

	callbackMutex sync.Mutex
}

func NewCallbackManager() *CallbackManager {
	return &CallbackManager{
		HostAddedCallbacks:   make(map[string]ModificationCallback),
		HostRemovedCallbacks: make(map[string]ModificationCallback),
		HostUpdatedCallbacks: make(map[string]ModificationCallback),
	}
}

func (m *CallbackManager) InvokeHostAddedCallbacks(host scheduling.Host) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	for _, callback := range m.HostAddedCallbacks {
		callback(host)
	}
}

func (m *CallbackManager) InvokeHostRemovedCallbacks(host scheduling.Host) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	for _, callback := range m.HostRemovedCallbacks {
		callback(host)
	}
}

func (m *CallbackManager) InvokeHostUpdatedCallbacks(host scheduling.Host) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	for _, callback := range m.HostUpdatedCallbacks {
		callback(host)
	}
}

func (m *CallbackManager) RegisterHostAddedCallback(id string, callback ModificationCallback) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	m.HostAddedCallbacks[id] = callback
}

func (m *CallbackManager) RegisterHostRemovedCallback(id string, callback ModificationCallback) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	m.HostRemovedCallbacks[id] = callback
}

func (m *CallbackManager) RegisterHostUpdatedCallback(id string, callback ModificationCallback) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	m.HostUpdatedCallbacks[id] = callback
}

func (m *CallbackManager) UnregisterHostAddedCallback(id string) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	delete(m.HostAddedCallbacks, id)
}

func (m *CallbackManager) UnregisterHostRemovedCallback(id string) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	delete(m.HostRemovedCallbacks, id)
}

func (m *CallbackManager) UnregisterHostUpdatedCallback(id string) {
	m.callbackMutex.Lock()
	defer m.callbackMutex.Unlock()

	delete(m.HostUpdatedCallbacks, id)
}
