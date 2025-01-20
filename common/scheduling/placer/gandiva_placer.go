package placer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// GandivaPlacer manages a collection of underlying Gandiva placers.
type GandivaPlacer struct {
	*BasicPlacer

	placerId string
}

// NewGandivaPlacer creates a new GandivaPlacer.
func NewGandivaPlacer(metrics scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy, gpusPerHost int) (*GandivaPlacer, error) {
	basePlacer, err := NewBasicPlacer(metrics, numReplicas, policy)
	if err != nil {
		return nil, err
	}

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

// logFindHosts simply logs a message about how many hosts were found during a part of findCandidateHosts.
func (placer *GandivaPlacer) logFindHosts(numHosts int, numGpus int32, hosts []scheduling.Host) {
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
func (placer *GandivaPlacer) findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool,
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
func (placer *GandivaPlacer) findHost(blacklist []interface{}, spec *proto.KernelSpec, training bool,
	metrics ...[]float64) scheduling.Host {

	// Our index will expect the first metric to be the number of GPUs.
	// findHosts will handle this, however, so we can just call findHosts immediately.
	hosts := placer.findHosts(blacklist, spec, 1, training, metrics...)

	if hosts == nil || len(hosts) == 0 {
		return nil
	}

	return hosts[0]
}
