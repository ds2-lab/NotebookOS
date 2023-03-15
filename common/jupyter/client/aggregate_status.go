package client

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
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

func resetEmptyCollectedMap(size int) {
	if int(size) > len(emptyCollectedMap) {
		mu.Lock()
		defer mu.Unlock()

		emptyCollectedMap = make([]int32, int(math.Pow(2, math.Ceil(math.Log2(float64(size)))))) // Round up to the nearest power of 2.
	}
}

type KernelStatusPublisher func(msg *zmq4.Msg, status string, how string) error

type StatusMsg struct {
	*zmq4.Msg
	Status string
	How    string
}

type AggregateKernelStatus struct {
	promise.Promise

	kernel           *DistributedKernelClient
	expectingStatus  string  // Expected status.
	expectingMatches int32   // The number of collected status that matches the expected status.
	allowViolation   bool    // If true, the status collection will stop on status dismatch.
	collecting       int32   // The number of replicas that are collecting status.
	collected        []int32 // A map that tracks the progress of the status collection. The first element is the number of collected status.
	matches          int32   // The number of collected status that matches the expected status.
	sampleMsg        *zmq4.Msg
	status           string // The last resolved status.
	lastErr          error
}

// NewAggregateKernelStatus creates a new aggregate status with configured number of replicas.
func NewAggregateKernelStatus(kernel *DistributedKernelClient, num_replicas int) *AggregateKernelStatus {
	resetEmptyCollectedMap(num_replicas + 1)
	return &AggregateKernelStatus{
		Promise:   promise.Resolved(),
		kernel:    kernel,
		status:    types.MessageKernelStatusIdle,
		collected: make([]int32, num_replicas+1),
	}
}

// Status returns the last resolved status.
func (s *AggregateKernelStatus) Status() string {
	return s.status
}

// Collect initiate the status collection with actual number of replicas.
func (s *AggregateKernelStatus) Collect(ctx context.Context, num_replicas int, replica_slots int, expecting string, publish KernelStatusPublisher) {
	s.expectingStatus = expecting
	if expecting == types.MessageKernelStatusIdle {
		// Idle status is special, it requires all replicas to be idle.
		s.expectingMatches = int32(num_replicas)
		s.allowViolation = false
	} else {
		// Other status changes on any replica reaches the status.
		s.expectingMatches = 1
		s.allowViolation = true
	}
	// Reset the collected map.
	s.collecting = int32(num_replicas)
	if replica_slots > cap(s.collected) {
		resetEmptyCollectedMap(replica_slots + 1)
		s.collected = make([]int32, int(math.Pow(2, math.Ceil(math.Log2(float64(replica_slots+1)))))) // Round up to the nearest power of 2.
	}
	s.collected = s.collected[:replica_slots+1]
	copy(s.collected, emptyCollectedMap[:replica_slots+1]) // Reset the collected map.
	// s.status will remain the same.
	s.matches = 0
	s.sampleMsg = nil
	s.lastErr = nil
	s.ResetWithOptions(s)
	go s.waitForStatus(ctx, s.status, publish)
}

// Reduce reduces the received status against the expected status and called the handler if last collect has timed out.
func (s *AggregateKernelStatus) Reduce(replicaId int32, status string, msg *zmq4.Msg, publish KernelStatusPublisher) {
	if s.IsResolved() && s.lastErr == nil {
		// Ignore if the status has been collected without error.
		return
	}

	// If the collection has been timeout, we continue handle late messages and trigger the handler if necessary.
	how, status, resolved := s.match(replicaId, status, msg)

	// Trigger handler for late messages.
	if resolved && s.lastErr != nil {
		s.lastErr = nil
		publish(msg, status, fmt.Sprintf("Late resolution: %s", how))
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
		// Double check the promise error. Possibilities are the promise can be reseted with promise.ErrReset.
		if err != nil || ret == nil {
			return
		}
		statusMsg := ret.(*StatusMsg)
		s.status = statusMsg.Status
		publish(statusMsg.Msg, statusMsg.Status, statusMsg.How)
	} else if s.sampleMsg != nil {
		// TODO: Not working here, need to regenerate the signature.
		jFrames, _ := s.kernel.SkipIdentities(s.sampleMsg.Frames)
		jFrames[4] = []byte(fmt.Sprintf(KernelStatusFrameTemplate, status))
		publish(s.sampleMsg, status, "Synthesized status")
	}

	// Or, do nothing.
}

// match returns true if the number of status reaches expected number.
// replicaID starts from 1. status update from duplicated replica will be ignored.
// New match call after the resolution of the promise will still be proceeded.
// The function is thread-safe and can be called concurrently.
func (s *AggregateKernelStatus) match(replicaId int32, status string, msg *zmq4.Msg) (how string, retStatus string, resolved bool) {
	// Check if the status has been collected.
	// ReplicaID should not exceed the size of the collected map, ignore if it does.
	if replicaId > int32(len(s.collected)) || !atomic.CompareAndSwapInt32(&s.collected[replicaId], 0, 1) {
		return
	}

	collected := atomic.AddInt32(&s.collected[0], 1)
	s.sampleMsg = msg // Update the sample message.
	if status != s.expectingStatus {
		if !s.allowViolation {
			how = fmt.Sprintf("violated %s status", s.expectingStatus)
			retStatus = status
			s.Resolve(&StatusMsg{Status: retStatus, Msg: msg, How: how})
			return how, retStatus, true
		} else if collected >= s.collecting {
			// We've collected all status without violation or reaching expected number of matches. Stop without changing status.
			how = fmt.Sprintf("not collected sufficient(%d/%d) %s status", atomic.LoadInt32(&s.matches), s.expectingMatches, s.expectingStatus)
			retStatus = s.status
			s.Resolve(&StatusMsg{Status: retStatus, Msg: msg, How: how})
			return how, retStatus, true
		}
	} else if atomic.AddInt32(&s.matches, 1) >= s.expectingMatches {
		// We've reached expected number of matches, update status and stop.
		how = fmt.Sprintf("collected sufficient(%d/%d) %s status", atomic.LoadInt32(&s.matches), s.expectingMatches, s.expectingStatus)
		retStatus = s.expectingStatus
		s.Resolve(&StatusMsg{Status: retStatus, Msg: msg, How: how})
		return how, retStatus, true
	}

	return
}
