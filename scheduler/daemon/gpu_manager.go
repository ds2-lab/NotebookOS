package daemon

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

var (
	ErrInsufficientGPUs            = errors.New("there are insufficient GPUs available to satisfy the allocation request")
	ErrAllocationNotFound          = errors.New("could not find the requested GPU allocation")
	ErrAllocationPartiallyNotFound = errors.New("the requested GPU allocation was found only in one of the various internal mappings (rather than within all of the mappings)")

	ZeroDecimal decimal.Decimal = decimal.NewFromFloat(0.0)
)

// Represents an allocation of GPU resources to a particular replica.
type gpuAllocation struct {
	id          string          // Unique ID of the allocation.
	numGPUs     decimal.Decimal // Number of GPUs that were allocated.
	replicaId   int32           // The SMR node ID of the replica to which the GPUs were allocated.
	kernelId    string          // The ID of the kernel whose replica was allocated GPUs.
	allocatedAt time.Time       // The time at which the GPUs were allocated to the replica.
	pending     bool            // If true, then this request corresponds to pending GPUs rather than "actual" GPUs.
}

func newGpuAllocation(numGPUs decimal.Decimal, replicaId int32, kernelId string, pending bool) *gpuAllocation {
	return &gpuAllocation{
		id:          uuid.NewString(),
		numGPUs:     numGPUs,
		replicaId:   replicaId,
		kernelId:    kernelId,
		allocatedAt: time.Now(),
		pending:     pending,
	}
}

func (a *gpuAllocation) String() string {
	return fmt.Sprintf("GpuAllocation(ID=%s,NumGPUs=%s,ReplicaID=%d,KernelID=%s,AllocatedAt=%s,Pending=%v)", a.id, a.numGPUs.StringFixed(0), a.replicaId, a.kernelId, a.allocatedAt.String(), a.pending)
}

// Manages the "actual" GPUs that are allocated to kernel replicas at training-time.
type GpuManager struct {
	id  string        // Unique ID of the scheduler.
	log logger.Logger // Logger.
	mu  sync.Mutex    // Mutex.

	/* Allocation Maps. */
	/* We maintain several maps of allocations to facilitate retrieving allocations */

	allocationIdMap            hashmap.HashMap[string, *gpuAllocation] // AllocationID -> *gpuAllocation; mapping in which keys are strings -- the allocation ID -- and values are the associated *gpuAllocation (i.e., the *gpuAllocation whose ID is the key).
	allocationKernelReplicaMap hashmap.HashMap[string, *gpuAllocation] // "<KernelID>-<ReplicaID>" -> *gpuAllocation; mapping in which keys are strings of the form "<KernelID>-<ReplicaID>" and values are *gpuAllocation.

	pendingAllocIdMap            hashmap.HashMap[string, *gpuAllocation] // AllocationID -> *gpuAllocation; mapping in which keys are strings -- the allocation ID -- and values are the associated *gpuAllocation (i.e., the *gpuAllocation whose ID is the key).
	pendingAllocKernelReplicaMap hashmap.HashMap[string, *gpuAllocation] // "<KernelID>-<ReplicaID>" -> *gpuAllocation; mapping in which keys are strings of the form "<KernelID>-<ReplicaID>" and values are *gpuAllocation.

	specGPUs      decimal.Decimal // The total number of GPUs configured/present on this node.
	idleGPUs      decimal.Decimal // The number of GPUs that are uncommitted and therefore available on this node. This quantity is equal to specGPUs - committedGPUs.
	committedGPUs decimal.Decimal // The number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
	pendingGPUs   decimal.Decimal // The sum of the outstanding GPUs of all replicas scheduled onto this node. Pending GPUs are not allocated or committed to a particular replica yet. The time at which resources are actually committed to a replica depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
}

// Create and return a new GPU Manager.
func NewGpuManager(gpus int64) *GpuManager {
	manager := &GpuManager{
		id:                           uuid.NewString(),
		allocationIdMap:              hashmap.NewCornelkMap[string, *gpuAllocation](16),
		allocationKernelReplicaMap:   hashmap.NewCornelkMap[string, *gpuAllocation](16),
		pendingAllocIdMap:            hashmap.NewCornelkMap[string, *gpuAllocation](16),
		pendingAllocKernelReplicaMap: hashmap.NewCornelkMap[string, *gpuAllocation](16),
		specGPUs:                     decimal.NewFromInt(gpus),
		idleGPUs:                     decimal.NewFromInt(gpus), // Initially, all GPUs are idle.
		committedGPUs:                ZeroDecimal.Copy(),       // Initially, there are 0 committed GPUs.
		pendingGPUs:                  ZeroDecimal.Copy(),       // Initially, there are 0 pending GPUs.
	}

	return manager
}

// The total number of GPUs configured/present on this node.
func (m *GpuManager) SpecGPUs() decimal.Decimal {
	return m.specGPUs
}

// The number of GPUs that are uncommitted and therefore available on this node.
// This quantity is equal to specGPUs - committedGPUs.
func (m *GpuManager) IdleGPUs() decimal.Decimal {
	return m.idleGPUs
}

// The number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
func (m *GpuManager) CommittedGPUs() decimal.Decimal {
	return m.committedGPUs
}

// The sum of the outstanding GPUs of all replicas scheduled onto this node.
// Pending GPUs are not allocated or committed to a particular replica yet.
// The time at which resources are actually committed to a replica depends upon the policy being used.
// In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
func (m *GpuManager) PendingGPUs() decimal.Decimal {
	return m.pendingGPUs
}

// Try to allocate the requested number of GPUs for the specified replica of the specified kernel.
// This will upgrade an existing Pending GPU request, if one exists. Otherwise, this will create a new GPU request.
//
// Returns:
// - nil on success.
// - ErrInsufficientGPUs if there are not enough GPUs available to satisfy the allocation request.
//
// NOTE: This function will acquire the mutex; the mutex should not be held when this function is called.
func (m *GpuManager) AllocateGPUs(numGPUs decimal.Decimal, replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If the request is for more GPUs than we have available (at all or just what isn't already allocated), then we'll return an error indicating that this is the case.
	if numGPUs.GreaterThan(m.specGPUs) || numGPUs.GreaterThan(m.idleGPUs) {
		return ErrInsufficientGPUs
	}

	// TODO(Ben): Is it strictly the case that there should already be an associated pending allocation?
	// We'll see as we continue with the implementation.
	allocation, exists := m.tryDeallocatePendingGPUs(replicaId, kernelId)

	// If the allocation does not already exist, then we'll just create a new one.
	// If it does, then we'll reuse it after first flipping its pending flag to false.
	if !exists {
		allocation = newGpuAllocation(numGPUs, replicaId, kernelId, false)
	} else {
		// Allocation already existed.
		m.assertPending(allocation)
		allocation.pending = false
	}

	// Update resource counts.
	m.idleGPUs = m.idleGPUs.Sub(numGPUs)
	m.committedGPUs = m.committedGPUs.Add(numGPUs)

	// Store allocation in the relevant mappings.
	key := m.getKey(replicaId, kernelId)
	m.allocationKernelReplicaMap.Store(key, allocation)
	m.allocationIdMap.Store(allocation.id, allocation)

	m.log.Debug("Allocated %d actual GPU(s) to replica %d of kernel %s.", numGPUs.StringFixed(0), replicaId, kernelId)

	return nil
}

// Try to allocate the requested number of GPUs for the specified replica of the specified kernel.
//
// Returns:
// - nil on success.
// - ErrInsufficientGPUs if there are not enough GPUs available to satisfy the allocation request.
//
// NOTE: This function will acquire the mutex; the mutex should not be held when this function is called.
func (m *GpuManager) AllocatePendingGPUs(numGPUs decimal.Decimal, replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If the request is for more GPUs than we have available at all, then we'll return an error indicating that this is the case.
	if numGPUs.GreaterThan(m.specGPUs) {
		return ErrInsufficientGPUs
	}

	allocation := newGpuAllocation(numGPUs, replicaId, kernelId, true)

	// Update resource counts.
	m.pendingGPUs = m.pendingGPUs.Add(numGPUs)

	// Store allocation in the relevant mappings.
	key := m.getKey(replicaId, kernelId)
	m.pendingAllocKernelReplicaMap.Store(key, allocation)
	m.pendingAllocIdMap.Store(allocation.id, allocation)

	m.log.Debug("Allocated %d pending GPU(s) to replica %d of kernel %s.", numGPUs.StringFixed(0), replicaId, kernelId)

	return nil
}

// Demote an existing, non-pending GPU allocation to a pending GPU allocation for the specified kernel replica.
//
// Returns nil on success.
// Returns ErrAllocationNotFound if there is no "actual" GPU allocation (as opposed to a "pending" GPU allocation) for the specified kernel replica.
//
// NOTE: This function will acquire the mutex; the mutex should not be held when this function is called.
func (m *GpuManager) ReleaseAllocatedGPUs(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getKey(replicaId, kernelId)

	allocation, exists := m.allocationKernelReplicaMap.Load(key)

	if !exists {
		return ErrAllocationNotFound
	}

	m.assertNotPending(allocation)
	allocation.pending = true

	// Update resource counts.
	m.idleGPUs = m.idleGPUs.Add(allocation.numGPUs)
	m.committedGPUs = m.committedGPUs.Sub(allocation.numGPUs)
	m.pendingGPUs = m.pendingGPUs.Add(allocation.numGPUs)

	// Update mappings.
	m.allocationKernelReplicaMap.Delete(key)
	m.allocationIdMap.Delete(allocation.id)

	m.pendingAllocKernelReplicaMap.Store(key, allocation)
	m.pendingAllocIdMap.Store(allocation.id, allocation)

	return nil
}

// Returns nil on success.
// Returns ErrAllocationNotFound if there is no "actual" GPU allocation (as opposed to a "pending" GPU allocation) for the specified kernel replica.
//
// NOTE: This function will acquire the mutex; the mutex should not be held when this function is called.
func (m *GpuManager) ReleasePendingGPUs(replicaId int32, kernelId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// We just call the helper method for this.
	_, existed := m.tryDeallocatePendingGPUs(replicaId, kernelId)

	if !existed {
		return ErrAllocationNotFound
	}

	return nil
}

// Attempt to release pending GPUs from the specified kernel replica.
//
// If there are no pending GPUs assigned to the specified kernel replica, then this simply returns false.
// True is returned if we found and deallocated pending GPUs for the specified kernel replica.
//
// This is used in multiple places, including:
// - GpuManager::AllocateGPUs
// - GpuManager::ReleasePendingGPUs
//
// IMPORTANT: This function must be called with the mutex already held!
func (m *GpuManager) tryDeallocatePendingGPUs(replicaId int32, kernelId string) (*gpuAllocation, bool) {
	key := m.getKey(replicaId, kernelId)
	pendingAllocation, pendingAllocationExists := m.pendingAllocKernelReplicaMap.Load(key)

	// If there was a pending allocation, then deallocate it now that we've "actually" allocated the GPUs.
	if pendingAllocationExists {
		m.log.Debug("Deallocating all pending GPUs for replica %d of kernel %s.", replicaId, kernelId)

		m.assertPending(pendingAllocation)
		m.pendingAllocKernelReplicaMap.Delete(key)
		m.pendingAllocIdMap.Delete(pendingAllocation.id)

		// Decrement the pending GPU count by the number of GPUs specified in the allocation.
		m.pendingGPUs.Sub(pendingAllocation.numGPUs)

		m.log.Debug("Removed %d pending GPU(s) from replica %d of kernel %s.", pendingAllocation.numGPUs.StringFixed(0), replicaId, kernelId)

		return pendingAllocation, true
	}

	return nil, false
}

// Create and return a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the GPU Manager.
func (m *GpuManager) getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

// Return true if the given *gpuAllocation IS pending.
// If the given *gpuAllocation is NOT pending, then this panics.
func (m *GpuManager) assertPending(allocation *gpuAllocation) bool {
	if allocation.pending {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation is NOT pending: %v", allocation))
}

// Return true if the given *gpuAllocation is NOT pending.
// If the given *gpuAllocation IS pending, then this panics.
func (m *GpuManager) assertNotPending(allocation *gpuAllocation) bool {
	if !allocation.pending {
		return true
	}

	panic(fmt.Sprintf("GPU Allocation IS pending: %v", allocation))
}

// Create and return a GPU allocation.
// The allocation is stored within the GPU Manager's various maps before being returned.
func (m *GpuManager) createGpuAllocation(numGPUs decimal.Decimal, replicaId int32, kernelId string) *gpuAllocation {
	allocation := newGpuAllocation(numGPUs, replicaId, kernelId, false)

	/* Store the allocation in the various maps. */

	key := m.getKey(replicaId, kernelId)
	m.allocationKernelReplicaMap.Store(key, allocation)

	m.allocationIdMap.Store(allocation.id, allocation)

	return allocation
}

func (m *GpuManager) removeGpuAllocation(replicaId int32, kernelId string) error {
	// First, load and delete the allocation from the `allocationKernelReplicaMap` map.
	key := m.getKey(replicaId, kernelId)
	allocation, exists := m.allocationKernelReplicaMap.LoadAndDelete(key)

	if !exists {
		m.log.Error("Could not find allocation associated with replica %d of kernel %s.", replicaId, kernelId)
		return ErrAllocationNotFound
	}

	// Next, load and delete the allocation from the `allocationIdMap`, using the id of the allocation we just loaded above.
	_, exists = m.allocationIdMap.LoadAndDelete(allocation.id)
	if !exists {
		m.log.Error("Allocation %v was found only in the `allocationKernelReplicaMap` map, but not in the `allocationIdMap`.", allocation)
		return ErrAllocationPartiallyNotFound
	}

	return nil
}

// Return the number of active allocations.
func (m *GpuManager) NumAllocations() int {
	return m.allocationIdMap.Len()
}

// Return the number of pending allocations.
func (m *GpuManager) NumPendingAllocations() int {
	return m.pendingAllocIdMap.Len()
}
