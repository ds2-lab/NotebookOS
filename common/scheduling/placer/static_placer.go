package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	*AbstractPlacer

	index *index.StaticClusterIndex

	mu sync.Mutex
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(cluster scheduling.Cluster, numReplicas int) (*StaticPlacer, error) {
	basePlacer := NewAbstractPlacer(cluster, numReplicas)

	staticPlacer := &StaticPlacer{
		AbstractPlacer: basePlacer,
		index:          index.NewStaticClusterIndex(),
	}

	if err := cluster.AddIndex(staticPlacer.index); err != nil {
		return nil, err
	}

	basePlacer.instance = staticPlacer
	return staticPlacer, nil
}

func (placer *StaticPlacer) getIndex() index.ClusterIndex {
	return placer.index
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	var (
		pos   interface{}
		host  scheduling.Host
		hosts = make([]scheduling.Host, placer.numReplicas)
	)
	for i := 0; i < len(hosts); i++ {
		host, pos = placer.index.SeekFrom(pos)

		if host.ResourceSpec().Validate(kernelSpec.DecimalSpecFromKernelSpec()) {
			// The Host can satisfy the resourceSpec, so append it to the slice.
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHost(blacklist []interface{}, spec types.Spec) scheduling.Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost scheduling.Host) bool {
		viable, _ := placer.hostIsViable(candidateHost, spec)
		return viable
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
