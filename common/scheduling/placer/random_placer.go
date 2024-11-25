package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// RandomPlacer is a simple placer that places sessions randomly.
type RandomPlacer struct {
	*AbstractPlacer

	index *index.RandomClusterIndex
}

// NewRandomPlacer creates a new RandomPlacer.
func NewRandomPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) (*RandomPlacer, error) {
	basePlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)
	randomPlacer := &RandomPlacer{
		AbstractPlacer: basePlacer,
		index:          index.NewRandomClusterIndex(100),
	}

	basePlacer.instance = randomPlacer
	return randomPlacer, nil
}

// GetIndex returns the ClusterIndex of the specific Placer implementation.
func (placer *RandomPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// NumHostsInIndex returns the length of the RandomPlacer's index.
func (placer *RandomPlacer) NumHostsInIndex() int {
	return placer.index.Len()
}

// tryReserveResourcesOnHost attempts to reserve resources for a future replica of the specified kernel
// on the specified host, returning true if the reservation was created successfully.
func (placer *RandomPlacer) tryReserveResourcesOnHost(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec) bool {
	reserved, err := candidateHost.ReserveResources(kernelSpec, placer.reservationShouldUsePendingResources())

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
func (placer *RandomPlacer) findHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	var (
		pos   interface{}       = nil
		hosts []scheduling.Host = nil
	)

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numHosts, func(candidateHost scheduling.Host) bool {
		return placer.tryReserveResourcesOnHost(candidateHost, kernelSpec)
	}, make([]interface{}, 0))
	placer.mu.Unlock()

	if len(hosts) > 0 {
		return hosts
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *RandomPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec) scheduling.Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost scheduling.Host) bool {
		return placer.tryReserveResourcesOnHost(candidateHost, kernelSpec)
	}, blacklist)

	if len(hosts) > 0 {
		return hosts[0]
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}
