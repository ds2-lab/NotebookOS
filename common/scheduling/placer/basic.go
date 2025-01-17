package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

type BasicPlacer struct {
	*AbstractPlacer

	index scheduling.ClusterIndex
}

// NewBasicPlacer creates a new BasicPlacer.
func NewBasicPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) (*BasicPlacer, error) {
	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          index.GetIndex(schedulingPolicy.PolicyKey(), schedulingPolicy.GetGpusPerHost()),
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer, nil
}

// Len returns the number of scheduling.Host instances in the underlying least-loaded index.
// Len is equivalent to Size.
func (placer *BasicPlacer) Len() int {
	return placer.index.Len()
}

// Size returns the number of scheduling.Host instances in the underlying least-loaded index.
// Size is equivalent to Len.
func (placer *BasicPlacer) Size() int {
	return placer.index.Len()
}

// GetIndex returns the ClusterIndex of the specific Placer implementation.
func (placer *BasicPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *BasicPlacer) findHosts(blacklist []interface{}, kernelSpec *proto.KernelSpec, numHosts int, forTraining bool) []scheduling.Host {
	var (
		pos   interface{}       = nil
		hosts []scheduling.Host = nil
	)

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numHosts, func(candidateHost scheduling.Host) bool {
		return placer.tryReserveResourcesOnHost(candidateHost, kernelSpec, forTraining)
	}, blacklist)

	if len(hosts) > 0 {
		return hosts
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *BasicPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) scheduling.Host {
	hosts := placer.findHosts(blacklist, kernelSpec, 1, forTraining)

	if hosts == nil || len(hosts) == 0 {
		return nil
	}

	return hosts[0]
}

func (placer *BasicPlacer) UpdateIndex(host scheduling.Host) {
	placer.index.Update(host)
}

func (placer *BasicPlacer) UpdateIndexMultiple(hosts []scheduling.Host) {
	placer.index.UpdateMultiple(hosts)
}

// NumHostsInIndex returns the length of the BasicPlacer's index.
func (placer *BasicPlacer) NumHostsInIndex() int {
	return placer.index.Len()
}

// tryReserveResourcesOnHost attempts to reserve resources for a future replica of the specified kernel
// on the specified host, returning true if the reservation was created successfully.
func (placer *BasicPlacer) tryReserveResourcesOnHost(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool {
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
