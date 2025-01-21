package placer

import (
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// MultiIndexProvider is a function that returns an *index.MultiIndex[T] struct, where T is some other concrete
// implementation of scheduling.ClusterIndex (i.e., another struct that implements scheduling.ClusterIndex).
//
// For example, a MultiIndexProvider[*index.LeastLoadedIndex, *index.MultiIndex[*index.LeastLoadedIndex]] would
// return *index.MultiIndex structs that manage collections of *index.LeastLoadedIndex structs as their individual
// host pools.
type MultiIndexProvider[T scheduling.ClusterIndex, V *index.MultiIndex[T]] func(gpusPerHost int32) V

//func GetMultiIndexProvider[T scheduling.ClusterIndex, V *index.MultiIndex[T]](indexProvider index.Provider[T]) MultiIndexProvider[T, V] {
//	provider := func(gpusPerHost int32) V {
//		multiIndex, err := index.NewMultiIndex[T](gpusPerHost, indexProvider)
//		if err != nil {
//			panic(err)
//		}
//
//		return multiIndex
//	}
//
//	return provider
//}

// MultiPlacer manages a collection of underlying Gandiva placers.
type MultiPlacer[T scheduling.ClusterIndex] struct {
	*BasicPlacer

	placerId string
}

// NewMultiPlacerWithSpecificIndex creates a new MultiPlacer backed by a specific scheduling.ClusterIndex,
// rather than whatever scheduling.ClusterIndex is used according to the scheduling.Policy.
//
// NewMultiPlacerWithSpecificIndex enables the creation of an arbitrary BasicPlacer (or an arbitrary MultiPlacer)
// backed by an arbitrary scheduling.ClusterIndex.
func NewMultiPlacerWithSpecificIndex[T scheduling.ClusterIndex](metrics scheduling.MetricsProvider, numReplicas int,
	policy scheduling.Policy, provider index.Provider[T], numPools int32) (*MultiPlacer[T], error) {

	basePlacer, err := NewBasicPlacerWithSpecificMultiIndex[T](metrics, numReplicas, policy, provider, numPools)
	if err != nil {
		return nil, err
	}

	multiPlacer := &MultiPlacer[T]{
		BasicPlacer: basePlacer,
		placerId:    uuid.NewString(),
	}

	basePlacer.instance = multiPlacer

	return multiPlacer, nil
}

// NewMultiPlacer creates a new MultiPlacer.
func NewMultiPlacer[T scheduling.ClusterIndex](metrics scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy) (*MultiPlacer[T], error) {
	basePlacer, err := NewBasicPlacer(metrics, numReplicas, policy)
	if err != nil {
		return nil, err
	}

	multiPlacer := &MultiPlacer[T]{
		BasicPlacer: basePlacer,
		placerId:    uuid.NewString(),
	}

	basePlacer.instance = multiPlacer

	return multiPlacer, nil
}

// getIndex returns the target MultiPlacer's index field with a type assertion
// so that it is returned as a *index.MultiIndex[T].
func (placer *MultiPlacer[T]) getIndex() *index.MultiIndex[T] {
	return placer.index.(*index.MultiIndex[T])
}

// Len returns the number of scheduling.Host instances in the underlying least-loaded index.
// Len is equivalent to Size.
func (placer *MultiPlacer[T]) Len() int {
	return placer.index.Len()
}

// Size returns the number of scheduling.Host instances in the underlying least-loaded index.
// Size is equivalent to Len.
func (placer *MultiPlacer[T]) Size() int {
	return placer.index.Len()
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiPlacer's index.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (placer *MultiPlacer[T]) NumFreeHosts() int {
	return placer.getIndex().NumFreeHosts()
}

// HasHostPool returns true if the MultiPlacer's underlying MultiIndex has a host pool for the specified
// number of GPUs.
func (placer *MultiPlacer[T]) HasHostPool(gpus int32) bool {
	return placer.getIndex().HasHostPool(gpus)
}

// NumHostsInPool returns the number of hosts in the specified host pool.
func (placer *MultiPlacer[T]) NumHostsInPool(gpus int32) int {
	return placer.getIndex().NumHostsInPool(gpus)
}

// GetHostPool returns the index.HostPool for the specified number of GPUs.
//
// The index.HostPool will be the one responsible for containing scheduling.Host instances that serve
// sessions/kernels/jobs requiring `gpus` number of GPUs.
func (placer *MultiPlacer[T]) GetHostPool(gpus int32) (*index.HostPool[T], bool) {
	return placer.getIndex().GetHostPool(gpus)
}

func (placer *MultiPlacer[T]) UpdateIndex(host scheduling.Host) {
	// First, figure out which index the host is contained within.
	placer.index.Update(host)
}

func (placer *MultiPlacer[T]) UpdateIndexMultiple(hosts []scheduling.Host) {
	placer.index.UpdateMultiple(hosts)
}

// GetIndex returns the index containing all hosts managed by the MultiPlacer.
func (placer *MultiPlacer[T]) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// NumHostsInIndex returns the length of the MultiPlacer's index.
func (placer *MultiPlacer[T]) NumHostsInIndex() int {
	return placer.Len()
}

// logFindHosts simply logs a message about how many hosts were found during a part of findCandidateHosts.
func (placer *MultiPlacer[T]) logFindHosts(numHosts int, numGpus int32, hosts []scheduling.Host) {
	// We did not find all the hosts that we need.
	if hosts == nil || len(hosts) == 0 {
		placer.log.Debug("Failed to find any candidate hosts from %d-GPU pool. We need %d host(s).", numGpus, numHosts)
	} else {
		placer.log.Debug("Found %d/%d candidate hosts from %d-GPU pool.", len(hosts), numHosts, numGpus)
	}
}

// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
// on each Host until either the requested number of Host instances has been found, or until all Host
// instances have been checked.
func (placer *MultiPlacer[T]) findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool,
	metrics ...[]float64) []scheduling.Host {

	// Our index will expect the first metric to be the number of GPUs.
	metrics = append([][]float64{{spec.ResourceSpec.GPU()}}, metrics...)

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

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *MultiPlacer[T]) findHost(blacklist []interface{}, spec *proto.KernelSpec, training bool,
	metrics ...[]float64) scheduling.Host {

	// Our index will expect the first metric to be the number of GPUs.
	// findHosts will handle this, however, so we can just call findHosts immediately.
	hosts := placer.findHosts(blacklist, spec, 1, training, metrics...)

	if hosts == nil || len(hosts) == 0 {
		return nil
	}

	return hosts[0]
}
