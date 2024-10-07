package client

import (
	"context"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

const (
	KernelStatusFrameTemplate = "{\"execution_state\": \"%s\"}"
)

var (
	KernelStatusMaxTimeout = 1 * time.Minute

	ErrReplicaOutOfRange = fmt.Errorf("replica id out of range")
	ErrCollected         = fmt.Errorf("collected")
)

// private global variables
var (
	emptyCollectedMap []int32
	mu                sync.Mutex
)

// Recreate the global emptyCollectedMap variable with a particular size based on the number of replicas.
// As far as I can tell, this is just used to efficiently clear the contents of the `collected` field of
// an AggregateKernelStatus struct.
func resetEmptyCollectedMap(size int) {
	if int(size) > len(emptyCollectedMap) {
		mu.Lock()
		defer mu.Unlock()

		emptyCollectedMap = make([]int32, int(math.Pow(2, math.Ceil(math.Log2(float64(size)))))) // Round up to the nearest power of 2.
	}
}

type KernelStatusPublisher func(msg *types.JupyterMessage, status string, how string) error

type StatusMsg struct {
	*types.JupyterMessage
	Status string
	How    string
}

// AggregateKernelStatus is the aggregated Jupyter kernel status of a set of kernel replicas.
type AggregateKernelStatus struct {
	promise.Promise

	mu sync.Mutex

	kernel           *DistributedKernelClient // The client that manages all the replicas.
	expectingStatus  string                   // Expected status.
	expectingMatches int32                    // The number of collected status that matches the expected status.
	allowViolation   bool                     // If true, the status collection will stop on status dismatch.
	collecting       int32                    // The number of replicas that are collecting status.
	collectedMap     map[int32]int32          // Map from replica ID to its status.
	numCollected     int32                    // Number of statuses we've collected.
	matches          int32                    // The number of collected status that matches the expected status.
	status           string                   // The last resolved status.
	sampleMsg        *types.JupyterMessage
	lastErr          error

	// collected        []int32                  // A map that tracks the progress of the status collection. The first element is the number of collected status.
}

// NewAggregateKernelStatus creates a new aggregate status with configured number of replicas.
func NewAggregateKernelStatus(kernel *DistributedKernelClient, numReplicas int) *AggregateKernelStatus {
	resetEmptyCollectedMap(numReplicas + 1) // + 1, as the first slot is the current number of statuses collected.
	return &AggregateKernelStatus{
		Promise:      promise.Resolved(),
		kernel:       kernel,
		status:       types.MessageKernelStatusIdle,
		collectedMap: make(map[int32]int32, numReplicas),
		numCollected: 0,
		// collected: make([]int32, num_replicas+1),
	}
}

// Status returns the last resolved status.
func (s *AggregateKernelStatus) Status() string {
	return s.status
}

// Collect initiate the status collection with actual number of replicas.
func (s *AggregateKernelStatus) Collect(ctx context.Context, numReplicas int, replica_slots int, expecting string, publish KernelStatusPublisher) {
	// s.kernel.log.Debug("Collecting aggregate status for kernel %s, %d replicas.", s.kernel.id, num_replicas)
	s.expectingStatus = expecting
	if expecting == types.MessageKernelStatusIdle {
		// Idle status is special, it requires all replicas to be idle.
		s.expectingMatches = int32(numReplicas)
		s.allowViolation = false
	} else {
		// Other status changes on any replica reaches the status.
		s.expectingMatches = 1
		s.allowViolation = true
	}
	// Reset the collected map. Note we need replice_slots+1 because the first element is the number of collected status.
	s.collecting = int32(numReplicas)

	// if replica_slots >= cap(s.collected) {
	// 	resetEmptyCollectedMap(replica_slots + 1)
	// 	s.collected = make([]int32, int(math.Pow(2, math.Ceil(math.Log2(float64(replica_slots+1)))))) // Round up to the nearest power of 2.
	// }
	// s.collected = s.collected[:replica_slots+1]
	// copy(s.collected, emptyCollectedMap[:replica_slots+1]) // Reset the collected map.
	s.collectedMap = make(map[int32]int32, numReplicas)
	s.numCollected = 0

	// s.status will remain the same.
	s.matches = 0
	s.sampleMsg = nil
	s.lastErr = nil
	s.ResetWithOptions(s)
	go s.waitForStatus(ctx, s.status, publish)
}

// Reduce reduces the received status against the expected status and called the handler if last collect has timed out.
func (s *AggregateKernelStatus) Reduce(replicaId int32, status string, msg *types.JupyterMessage, publish KernelStatusPublisher) {
	// s.kernel.log.Debug("Reducing status \"%s\" for replica %d of kernel %s.", status, replicaId, s.kernel.id)

	if s.IsResolved() && s.lastErr == nil {
		// Ignore if the status has been collected without error.
		return
	}

	// If the collection has been timeout, we continue handle late messages and trigger the handler if necessary.
	how, status, resolved := s.match(replicaId, status, msg)

	// Trigger handler for late messages.
	if resolved && s.lastErr != nil {
		s.lastErr = nil
		err := publish(msg, status, fmt.Sprintf("Late resolution: %s", how))
		if err != nil {
			fmt.Printf(utils.RedStyle.Render("[ERROR] Error while publishing AggregateKernelStatus: %v\n"), err)
			return
		}
	}
}

// waitForStatus waits for the status to be collected and called the handler.
func (s *AggregateKernelStatus) waitForStatus(ctx context.Context, defaultStatus string, publish KernelStatusPublisher) {
	status := defaultStatus
	var err error
	if deadline, ok := ctx.Deadline(); ok {
		err = s.Timeout(time.Until(deadline))
	} else {
		err = s.Timeout(KernelStatusMaxTimeout)
	}
	s.lastErr = err

	if err == nil {
		ret, err := s.Result()
		// Double-check the promise error. Possibilities are the promise can be reset with promise.ErrReset.
		if err != nil || ret == nil {
			// s.kernel.log.Warn("Failed to obtain status result. Error: %v", err)
			return
		}
		statusMsg := ret.(*StatusMsg)
		s.status = statusMsg.Status
		// s.kernel.log.Debug("Publishing status \"%v\" for kernel %s; how \"%v\"", statusMsg.Status, s.kernel.id, statusMsg.How)
		err = publish(statusMsg.JupyterMessage, statusMsg.Status, statusMsg.How)
		if err != nil {
			fmt.Printf(utils.RedStyle.Render("[ERROR] Error while publishing AggregateKernelStatus: %v\n"), err)
			return
		}
	} else if s.sampleMsg != nil {
		// TODO: Not working here, need to regenerate the signature.
		s.sampleMsg.JupyterFrames.ContentFrame().Set([]byte(fmt.Sprintf(KernelStatusFrameTemplate, status)))
		_, signError := s.sampleMsg.JupyterFrames.Sign(s.kernel.ConnectionInfo().SignatureScheme, []byte(s.kernel.ConnectionInfo().Key)) // Ignore the error, log it if necessary.
		if signError != nil {
			fmt.Printf(utils.RedStyle.Render("[ERROR] Error while publishing AggregateKernelStatus: %v\n"), err)
			return
		}
		err := publish(s.sampleMsg, status, "Synthesized status")
		if err != nil {
			fmt.Printf(utils.RedStyle.Render("[ERROR] Error while publishing AggregateKernelStatus: %v\n"), err)
			return
		}
	}

	// Or, do nothing.
}

// match returns true if the number of status reaches expected number.
// replicaID starts from 1. status update from duplicated replica will be ignored.
// New match call after the resolution of the promise will still be proceeded.
// The function is thread-safe and can be called concurrently.
func (s *AggregateKernelStatus) match(replicaId int32, status string, msg *types.JupyterMessage) (how string, retStatus string, resolved bool) {
	// Check if the status has been collected.
	// ReplicaID should not exceed the size of the collected map, ignore if it does.
	// if replicaId >= int32(len(s.collected)) || !atomic.CompareAndSwapInt32(&s.collected[replicaId], 0, 1) {
	// 	return
	// }

	// Check if the status has already been collected. If so, then we'll just return.
	s.mu.Lock()
	if val, ok := s.collectedMap[replicaId]; ok && val == 1 {
		// If there's already a 1 stored for the particular replica ID, then we already have its status, so we'll just return.
		s.mu.Unlock() // Make sure to unlock before returning.
		return
	}

	// There was not already a 1 stored for the particular replica ID, so we'll store a 1 now.
	s.collectedMap[replicaId] = 1
	s.mu.Unlock() // Make sure to unlock before proceeding.

	collected := atomic.AddInt32(&s.numCollected, 1)
	s.sampleMsg = msg // Update the sample message.
	if status != s.expectingStatus {
		if !s.allowViolation {
			how = fmt.Sprintf("violated %s status", s.expectingStatus)
			retStatus = status
			_, _ = s.Resolve(&StatusMsg{Status: retStatus, JupyterMessage: msg, How: how})
			return how, retStatus, true
		} else if collected >= s.collecting {
			// We've collected all status without violation or reaching expected number of matches. Stop without changing status.
			how = fmt.Sprintf("not collected sufficient(%d/%d) %s status", atomic.LoadInt32(&s.matches), s.expectingMatches, s.expectingStatus)
			retStatus = s.status
			_, _ = s.Resolve(&StatusMsg{Status: retStatus, JupyterMessage: msg, How: how})
			return how, retStatus, true
		}
	} else if atomic.AddInt32(&s.matches, 1) >= s.expectingMatches {
		// We've reached expected number of matches, update status and stop.
		how = fmt.Sprintf("collected sufficient(%d/%d) %s status", atomic.LoadInt32(&s.matches), s.expectingMatches, s.expectingStatus)
		retStatus = s.expectingStatus
		_, _ = s.Resolve(&StatusMsg{Status: retStatus, JupyterMessage: msg, How: how})
		return how, retStatus, true
	}

	return
}
