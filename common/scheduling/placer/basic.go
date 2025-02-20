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
	schedulingPolicy scheduling.Policy, indexProvider index.Provider[T], numPools int32) *BasicPlacer {

	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	multiIndex := index.NewMultiIndex[T](numPools, indexProvider)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          multiIndex,
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer
}

// NewBasicPlacerWithSpecificIndex creates a new BasicPlacer backed by a scheduling.ClusterIndex
// whose type is determined by whatever the given indexProvider returns, rather than by the
// scheduling.Policy configured for the scheduling.Cluster.
//
// NewBasicPlacerWithSpecificIndex enables the creation of an arbitrary BasicPlacer (or an arbitrary MultiPlacer)
// backed by an arbitrary scheduling.ClusterIndex.
func NewBasicPlacerWithSpecificIndex[T scheduling.ClusterIndex](metricsProvider scheduling.MetricsProvider, numReplicas int,
	schedulingPolicy scheduling.Policy, indexProvider index.Provider[T]) *BasicPlacer {

	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          indexProvider(int32(schedulingPolicy.GetGpusPerHost())),
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer
}

// NewBasicPlacer creates a new BasicPlacer backed by a scheduling.ClusterIndex whose type is determined by the
// specified scheduling.Policy.
func NewBasicPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) *BasicPlacer {
	abstractPlacer := NewAbstractPlacer(metricsProvider, numReplicas, schedulingPolicy)

	basicPlacer := &BasicPlacer{
		AbstractPlacer: abstractPlacer,
		index:          index.GetIndex(schedulingPolicy.PolicyKey(), schedulingPolicy.GetGpusPerHost()+1 /* for Gandiva */),
	}

	abstractPlacer.instance = basicPlacer
	return basicPlacer
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
	metrics ...[]float64) ([]scheduling.Host, error) {
	var (
		pos   interface{}
		hosts []scheduling.Host
		err   error
	)

	// Create a wrapper around the 'kernelResourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) error {
		_, reservationError := placer.reserveResourcesForKernel(candidateHost, spec, forTraining)
		return reservationError
	}

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, pos, err = placer.index.SeekMultipleFrom(pos, numHosts, reserveResources, blacklist, metrics...)

	return hosts, err
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *BasicPlacer) findHost(blacklist []interface{}, replicaSpec *proto.KernelReplicaSpec, forTraining bool,
	metrics ...[]float64) (scheduling.Host, error) {

	// Our index will expect the first metric to be the number of GPUs.
	metrics = append([][]float64{{replicaSpec.ResourceSpec().GPU()}}, metrics...)

	var (
		pos   interface{}
		hosts []scheduling.Host
		// err   error
	)

	// Create a wrapper around the 'kernelResourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) error {
		_, reservationError := placer.reserveResourcesForReplica(candidateHost, replicaSpec, forTraining)
		return reservationError
	}

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _, _ /* err */ = placer.index.SeekMultipleFrom(pos, 1, reserveResources, blacklist, metrics...)

	if len(hosts) == 0 {
		// TODO: Why don't we return an error?
		return nil, nil
	}

	// TODO: Why don't we return an error?
	return hosts[0], nil
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
