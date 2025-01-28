package placer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// GandivaPlacer manages a collection of underlying Gandiva placers.
//
// GandivaPlacer implements the scheduling logic of Gandiva, presented at NSDI'19 in the paper
// ["Gandiva: Introspective Cluster Scheduling for Deep Learning"].
//
// ["Gandiva: Introspective Cluster Scheduling for Deep Learning"]: https://www.usenix.org/system/files/nsdi19-gu.pdf
type GandivaPlacer struct {
	*BasicPlacer

	placerId string
}

// NewGandivaPlacer creates a new GandivaPlacer.
func NewGandivaPlacer(metrics scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy) (*GandivaPlacer, error) {
	basePlacer := NewBasicPlacer(metrics, numReplicas, policy)

	clusterIndex := basePlacer.GetIndex()
	_, ok := clusterIndex.(*index.MultiIndex[*index.LeastLoadedIndex])
	if !ok {
		return nil, fmt.Errorf("incorrect/unexpected index type")
	}

	gandivaPlacer := &GandivaPlacer{
		BasicPlacer: basePlacer,
		placerId:    uuid.NewString(),
	}

	basePlacer.instance = gandivaPlacer

	return gandivaPlacer, nil
}

// getIndex returns the target GandivaPlacer's index field with a type assertion
// so that it is returned as a *index.MultiIndex[*index.LeastLoadedIndex].
func (placer *GandivaPlacer) getIndex() *index.MultiIndex[*index.LeastLoadedIndex] {
	return placer.index.(*index.MultiIndex[*index.LeastLoadedIndex])
}

// Len returns the number of scheduling.Host instances in the underlying least-loaded index.
// Len is equivalent to Size.
func (placer *GandivaPlacer) Len() int {
	return placer.index.Len()
}

// Size returns the number of scheduling.Host instances in the underlying least-loaded index.
// Size is equivalent to Len.
func (placer *GandivaPlacer) Size() int {
	return placer.index.Len()
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target GandivaPlacer's index.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (placer *GandivaPlacer) NumFreeHosts() int {
	return placer.getIndex().NumFreeHosts()
}

// HasHostPool returns true if the GandivaPlacer's underlying MultiIndex has a host pool for the specified
// number of GPUs.
func (placer *GandivaPlacer) HasHostPool(gpus int32) bool {
	return placer.getIndex().HasHostPool(gpus)
}

// NumHostsInPool returns the number of hosts in the specified host pool.
func (placer *GandivaPlacer) NumHostsInPool(gpus int32) int {
	return placer.getIndex().NumHostsInPool(gpus)
}

// GetHostPool returns the index.HostPool for the specified number of GPUs.
//
// The index.HostPool will be the one responsible for containing scheduling.Host instances that serve
// sessions/kernels/jobs requiring `gpus` number of GPUs.
func (placer *GandivaPlacer) GetHostPool(gpus int32) (*index.HostPool[*index.LeastLoadedIndex], bool) {
	return placer.getIndex().GetHostPool(gpus)
}

func (placer *GandivaPlacer) UpdateIndex(host scheduling.Host) {
	// First, figure out which index the host is contained within.
	placer.index.Update(host)
}

func (placer *GandivaPlacer) UpdateIndexMultiple(hosts []scheduling.Host) {
	placer.index.UpdateMultiple(hosts)
}

// GetIndex returns the index containing all hosts managed by the GandivaPlacer.
func (placer *GandivaPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// NumHostsInIndex returns the length of the GandivaPlacer's index.
func (placer *GandivaPlacer) NumHostsInIndex() int {
	return placer.Len()
}

// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
// on each Host until either the requested number of Host instances has been found, or until all Host
// instances have been checked.
func (placer *GandivaPlacer) findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool,
	metrics ...[]float64) ([]scheduling.Host, error) {

	// Our index will expect the first metric to be the number of GPUs.
	metrics = append([][]float64{{spec.ResourceSpec.GPU()}}, metrics...)

	var (
		pos   interface{}
		hosts []scheduling.Host
		err   error
	)

	// Create a wrapper around the 'kernelResourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) bool {
		reserved, _ := placer.kernelResourceReserver(candidateHost, spec, forTraining)
		return reserved
	}

	// Seek `numHosts` Hosts from the Placer's index.
	hosts, _, err = placer.index.SeekMultipleFrom(pos, numHosts, reserveResources, blacklist, metrics...)

	return hosts, err
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *GandivaPlacer) findHost(blacklist []interface{}, replicaSpec *proto.KernelReplicaSpec, forTraining bool,
	metrics ...[]float64) (scheduling.Host, error) {

	// Our index will expect the first metric to be the number of GPUs.
	metrics = append([][]float64{{replicaSpec.ResourceSpec().GPU()}}, metrics...)

	var (
		pos   interface{}
		hosts []scheduling.Host
		// err   error
	)

	// Create a wrapper around the 'kernelResourceReserver' field so that it can be called by the index.
	reserveResources := func(candidateHost scheduling.Host) bool {
		reserved, _ := placer.replicaResourceReserver(candidateHost, replicaSpec, forTraining)
		return reserved
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
