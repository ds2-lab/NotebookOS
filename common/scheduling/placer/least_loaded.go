package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// LeastLoadedPlacer is a placer that places sessions onto the least-loaded hosts in its index.
type LeastLoadedPlacer struct {
	*AbstractPlacer

	index *index.LeastLoadedIndex
}

// NewLeastLoadedPlacer creates a new LeastLoadedPlacer.
func NewLeastLoadedPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int,
	schedulingPolicy scheduling.Policy) (*LeastLoadedPlacer, error) {

	basePlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)
	leastLoadedPlacer := &LeastLoadedPlacer{
		AbstractPlacer: basePlacer,
		index:          index.NewLeastLoadedIndex(index.LeastLoadedIndexMetadataKey),
	}

	basePlacer.instance = leastLoadedPlacer
	return leastLoadedPlacer, nil
}

// Len returns the number of scheduling.Host instances in the underlying least-loaded index.
// Len is equivalent to Size.
func (placer *LeastLoadedPlacer) Len() int {
	return placer.index.Len()
}

// Size returns the number of scheduling.Host instances in the underlying least-loaded index.
// Size is equivalent to Len.
func (placer *LeastLoadedPlacer) Size() int {
	return placer.index.Len()
}

func (placer *LeastLoadedPlacer) UpdateIndex(host scheduling.Host) {
	placer.index.Update(host)
}

func (placer *LeastLoadedPlacer) UpdateIndexMultiple(hosts []scheduling.Host) {
	placer.index.UpdateMultiple(hosts)
}

// GetIndex returns the ClusterIndex of the specific Placer implementation.
func (placer *LeastLoadedPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// NumHostsInIndex returns the length of the LeastLoadedPlacer's index.
func (placer *LeastLoadedPlacer) NumHostsInIndex() int {
	return placer.index.Len()
}

// tryReserveResourcesOnHost attempts to reserve resources for a future replica of the specified kernel
// on the specified host, returning true if the reservation was created successfully.
//
// Returns: true if the reservation was created successfully.
func (placer *LeastLoadedPlacer) tryReserveResourcesOnHost(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool {
	var usePendingReservation bool

	if forTraining {
		// If we are migrating a replica that needs to begin training right away, then we should not use a pending reservation.
		// The container will need resources committed to it immediately.
		usePendingReservation = false
	} else {
		usePendingReservation = placer.reservationShouldUsePendingResources()
	}

	reserved, err := candidateHost.ReserveResources(kernelSpec, usePendingReservation)
	if err != nil {
		placer.log.Error("Error while attempting to reserve resources for replica of kernel %s on host %s (ID=%s): %v",
			kernelSpec.Id, candidateHost.GetNodeName(), candidateHost.GetID(), err)

		// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
		if reserved {
			panic("We successfully reserved resources on a Host despite ReserveResources also returning an error...")
		}
	}

	return reserved
}

// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
// on each Host until either the requested number of Host instances has been found, or until all Host
// instances have been checked.
func (placer *LeastLoadedPlacer) findHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	var (
		pos   interface{}       = nil
		hosts []scheduling.Host = nil
	)

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numHosts, func(candidateHost scheduling.Host) bool {
		success := placer.tryReserveResourcesOnHost(candidateHost, kernelSpec, false)

		return success
	}, make([]interface{}, 0))
	placer.mu.Unlock()

	if len(hosts) > 0 {
		return hosts
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *LeastLoadedPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) scheduling.Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost scheduling.Host) bool {
		success := placer.tryReserveResourcesOnHost(candidateHost, kernelSpec, forTraining)

		return success
	}, blacklist)

	if len(hosts) > 0 {
		return hosts[0]
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}
