package placer

import (
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// StaticPlacer is a particular type of MultiPlacer that implements the logic for the static scheduling policy.
type StaticPlacer struct {
	*BasicPlacer

	PlacerId string
}

// NewStaticPlacer creates and returns a StaticPlacer struct.
func NewStaticPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy) *StaticPlacer {
	basePlacer := NewBasicPlacerWithSpecificIndex[*index.StaticMultiIndex](metricsProvider, numReplicas, policy, index.NewStaticMultiIndex)

	staticPlacer := &StaticPlacer{
		BasicPlacer: basePlacer,
		PlacerId:    uuid.NewString(),
	}

	basePlacer.instance = staticPlacer
	staticPlacer.instance = staticPlacer

	return staticPlacer
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool,
	metrics ...[]float64) ([]scheduling.Host, error) {
	var (
		pos   interface{}
		hosts []scheduling.Host
		err   error
	)

	// Our index will expect the first metric to be the number of GPUs.
	metrics = append([][]float64{{spec.ResourceSpec.GPU()}}, metrics...)

	// Create a wrapper around the 'kernelResourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) error {
		_, reservationError := placer.reserveResourcesForKernel(candidateHost, spec, forTraining)
		return reservationError
	}

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, pos, err = placer.index.SeekMultipleFrom(pos, numHosts, reserveResources, blacklist, metrics...)

	return hosts, err
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHost(blacklist []interface{}, replicaSpec *proto.KernelReplicaSpec, forTraining bool,
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

// getIndex returns the target MultiPlacer's index field with a type assertion
// so that it is returned as a *index.MultiIndex[T].
func (placer *StaticPlacer) getIndex() *index.StaticMultiIndex {
	return placer.index.(*index.StaticMultiIndex)
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiPlacer's index.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (placer *StaticPlacer) NumFreeHosts() int {
	return placer.getIndex().NumFreeHosts()
}

// NumHostPools returns the number of HostPools managed by the StaticPlacer.
func (placer *StaticPlacer) NumHostPools() int {
	return int(placer.getIndex().NumPools)
}

// HostPoolIDs returns the valid IDs of each HostPool managed by the target StaticPlacer.
func (placer *StaticPlacer) HostPoolIDs() []int32 {
	return placer.getIndex().HostPoolIDs()
}

// HasHostPool returns true if the MultiPlacer's underlying MultiIndex has a host pool for the specified
// number of GPUs.
//
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (placer *StaticPlacer) HasHostPool(poolNumber int32) bool {
	return placer.getIndex().HasHostPool(poolNumber)
}

// HasHostPoolByIndex returns true if the MultiIndex has a host pool for the specified pool index.
func (placer *StaticPlacer) HasHostPoolByIndex(poolNumber int32) bool {
	return placer.getIndex().HasHostPoolByIndex(poolNumber)
}

// NumHostsInPool returns the number of hosts in the specified host pool.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (placer *StaticPlacer) NumHostsInPool(gpus int32) int {
	return placer.getIndex().NumHostsInPool(gpus)
}

// NumHostsInPoolByIndex returns the number of hosts in the specified host pool.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (placer *StaticPlacer) NumHostsInPoolByIndex(poolIndex int32) int {
	return placer.getIndex().NumHostsInPoolByIndex(poolIndex)
}

// GetHostPool returns the index.HostPool for the specified index.
//
// The index.HostPool will be the one responsible for containing scheduling.Host instances that serve
// sessions/kernels/jobs requiring `gpus` number of GPUs.
func (placer *StaticPlacer) GetHostPool(poolNumber int32) (*index.HostPool[*index.LeastLoadedIndex], bool) {
	return placer.getIndex().GetHostPool(poolNumber)
}
