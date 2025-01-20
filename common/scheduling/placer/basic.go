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

// NewBasicPlacerWithSpecificMultiIndex creates a new BasicPlacer backed by a *index.MultiIndex.
// The underlying *index.MultiIndex manages a pool of scheduling.ClusterIndex instances whose concrete type is
// the type parameter T.
//
// NewBasicPlacerWithSpecificMultiIndex enables the creation of an arbitrary MultiPlacer (which contains a promoted
// BasicPlacer, of course) that is backed by the *index.MultiIndex[T].
func NewBasicPlacerWithSpecificMultiIndex[T scheduling.ClusterIndex](metricsProvider scheduling.MetricsProvider, numReplicas int,
	schedulingPolicy scheduling.Policy, indexProvider index.Provider[T], numPools int32) (*BasicPlacer, error) {

	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	multiIndex, err := index.NewMultiIndex[T](numPools, indexProvider)
	if err != nil {
		return nil, err
	}

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          multiIndex,
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer, nil
}

// NewBasicPlacerWithSpecificIndex creates a new BasicPlacer backed by a scheduling.ClusterIndex
// whose type is determined by whatever the given indexProvider returns, rather than by the
// scheduling.Policy configured for the scheduling.Cluster.
//
// NewBasicPlacerWithSpecificIndex enables the creation of an arbitrary BasicPlacer (or an arbitrary MultiPlacer)
// backed by an arbitrary scheduling.ClusterIndex.
func NewBasicPlacerWithSpecificIndex[T scheduling.ClusterIndex](metricsProvider scheduling.MetricsProvider, numReplicas int,
	schedulingPolicy scheduling.Policy, indexProvider index.Provider[T]) (*BasicPlacer, error) {

	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          indexProvider(int32(schedulingPolicy.GetGpusPerHost())),
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer, nil
}

// NewBasicPlacer creates a new BasicPlacer backed by a scheduling.ClusterIndex whose type is determined by the
// specified scheduling.Policy.
func NewBasicPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) (*BasicPlacer, error) {
	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          index.GetIndex(schedulingPolicy.PolicyKey(), schedulingPolicy.GetGpusPerHost()+1 /* for Gandiva */),
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
