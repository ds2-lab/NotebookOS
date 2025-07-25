package provisioner

import (
	"fmt"
	"sync"
)

type RegistrationWaitGroups struct {

	// The SMR node replicas in order according to their registration IDs.
	replicas map[int32]string

	onReplicaRegisteredCallbacks []func(replicaId int32)
	// Decremented each time a kernel registers.
	registered sync.WaitGroup

	// Decremented each time we've notified a kernel of its ID.
	notified      sync.WaitGroup
	numRegistered int

	numNotified int

	onReplicaRegisteredCallbacksMutex sync.Mutex

	// Synchronizes access to the `replicas` slice.
	replicasMutex sync.Mutex
}

// NewRegistrationWaitGroups creates and return a pointer to a new RegistrationWaitGroups struct.
//
// Parameters:
// - numReplicas (int): Value to be added to the "notified" and "registered" sync.WaitGroups of the RegistrationWaitGroups.
func NewRegistrationWaitGroups(numReplicas int) *RegistrationWaitGroups {
	wg := &RegistrationWaitGroups{
		replicas:                     make(map[int32]string),
		onReplicaRegisteredCallbacks: make([]func(replicaId int32), 0),
	}

	wg.notified.Add(numReplicas)
	wg.registered.Add(numReplicas)

	return wg
}

func (wg *RegistrationWaitGroups) AddOnReplicaRegisteredCallback(f func(int32)) {
	wg.onReplicaRegisteredCallbacksMutex.Lock()
	defer wg.onReplicaRegisteredCallbacksMutex.Unlock()

	wg.onReplicaRegisteredCallbacks = append(wg.onReplicaRegisteredCallbacks, f)
}

func (wg *RegistrationWaitGroups) String() string {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()
	return fmt.Sprintf("RegistrationWaitGroups[NumRegistered=%d, NumNotified=%d]", wg.numRegistered, wg.numNotified)
}

// Notify calls `SetDone()` on the "notified" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) Notify() {
	wg.notified.Done()

	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()
	wg.numNotified += 1
}

// Register calls `SetDone()` on the "registered" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) Register(replicaId int32) {
	wg.registered.Done()

	wg.replicasMutex.Lock()
	wg.numRegistered += 1
	wg.replicasMutex.Unlock()

	wg.onReplicaRegisteredCallbacksMutex.Lock()
	for _, callback := range wg.onReplicaRegisteredCallbacks {
		callback(replicaId)
	}
	wg.onReplicaRegisteredCallbacksMutex.Unlock()
}

func (wg *RegistrationWaitGroups) SetReplica(idx int32, hostname string) {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	wg.replicas[idx] = hostname
}

func (wg *RegistrationWaitGroups) GetReplicas() map[int32]string {
	return wg.replicas
}

func (wg *RegistrationWaitGroups) NumReplicas() int {
	return len(wg.replicas)
}

// RemoveReplica returns true if the node with the given ID was actually removed.
// If the node with the given ID was not present in the primarSemaphore, then returns false.
func (wg *RegistrationWaitGroups) RemoveReplica(nodeId int32) bool {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	if _, ok := wg.replicas[nodeId]; !ok {
		return false
	}

	delete(wg.replicas, nodeId)

	return true
}

func (wg *RegistrationWaitGroups) AddReplica(nodeId int32, hostname string) map[int32]string {
	wg.replicasMutex.Lock()
	defer wg.replicasMutex.Unlock()

	if _, ok := wg.replicas[nodeId]; ok {
		fmt.Printf("WARNING: Replacing replica %d (%s) with new replica %s.\n", nodeId, wg.replicas[nodeId], hostname)
	}

	wg.replicas[nodeId] = hostname

	return wg.replicas
}

// GetNotified returns the "notified" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) GetNotified() *sync.WaitGroup {
	return &wg.notified
}

// GetRegistered returns the "registered" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) GetRegistered() *sync.WaitGroup {
	return &wg.registered
}

// WaitNotified calls `Wait()` on the "notified" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) WaitNotified() {
	wg.notified.Wait()
}

// WaitRegistered calls `Wait()` on the "registered" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) WaitRegistered() {
	wg.registered.Wait()
}

// Wait first calls `Wait()` on the "registered" sync.primarSemaphore.
// Then, Wait calls `Wait()` on the "notified" sync.primarSemaphore.
func (wg *RegistrationWaitGroups) Wait() {
	wg.WaitRegistered()
	wg.WaitNotified()
}
