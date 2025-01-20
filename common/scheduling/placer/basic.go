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
func (placer *BasicPlacer) findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool,
	metrics ...[]float64) []scheduling.Host {
	var (
		pos   interface{}       = nil
		hosts []scheduling.Host = nil
	)

	// Create a wrapper around the 'resourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) bool {
		return placer.resourceReserver(candidateHost, spec, forTraining)
	}

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _ = placer.index.SeekMultipleFrom(pos, numHosts, reserveResources, blacklist, metrics...)

	if len(hosts) > 0 {
		return hosts
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return nil
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *BasicPlacer) findHost(blacklist []interface{}, spec *proto.KernelSpec, forTraining bool,
	metrics ...[]float64) scheduling.Host {
	
	hosts := placer.findHosts(blacklist, spec, 1, forTraining, metrics...)

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
