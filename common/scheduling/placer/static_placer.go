package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	*AbstractPlacer

	index *index.StaticClusterIndex
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) (*StaticPlacer, error) {
	basePlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	staticPlacer := &StaticPlacer{
		AbstractPlacer: basePlacer,
		index:          index.NewStaticClusterIndex(),
	}

	basePlacer.instance = staticPlacer
	return staticPlacer, nil
}

// GetIndex returns the ClusterIndex of the specific Placer implementation.
func (placer *StaticPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	var (
		pos   interface{}       = nil
		hosts []scheduling.Host = nil
	)

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numHosts, func(candidateHost scheduling.Host) bool {
		return placer.tryReserveResourcesOnHost(candidateHost, kernelSpec, false)
	}, make([]interface{}, 0))
	placer.mu.Unlock()

	if len(hosts) > 0 {
		return hosts
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

func (placer *StaticPlacer) UpdateIndex(host scheduling.Host) {
	placer.index.Update(host)
}

func (placer *StaticPlacer) UpdateIndexMultiple(hosts []scheduling.Host) {
	placer.index.UpdateMultiple(hosts)
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) scheduling.Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost scheduling.Host) bool {
		return placer.tryReserveResourcesOnHost(candidateHost, kernelSpec, forTraining)
	}, blacklist)

	if len(hosts) > 0 {
		return hosts[0]
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

// NumHostsInIndex returns the length of the StaticPlacer's index.
func (placer *StaticPlacer) NumHostsInIndex() int {
	return placer.index.Len()
}

// tryReserveResourcesOnHost attempts to reserve resources for a future replica of the specified kernel
// on the specified host, returning true if the reservation was created successfully.
func (placer *StaticPlacer) tryReserveResourcesOnHost(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool {
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
		placer.log.Debug("Failed to reserve resources for replica of kernel %s on host %s (ID=%s): %v",
			kernelSpec.Id, candidateHost.GetNodeName(), candidateHost.GetID(), err)

		// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
		if reserved {
			panic("We successfully reserved resources on a Host despite ReserveResources also returning an error...")
		}
	}

	return reserved
}
