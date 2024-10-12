package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/types"
	"sync"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	*AbstractPlacer

	index *StaticClusterIndex

	mu sync.Mutex
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(cluster clusterInternal, opts *ClusterSchedulerOptions) (*StaticPlacer, error) {
	basePlacer := newAbstractPlacer(cluster, opts)

	staticPlacer := &StaticPlacer{
		AbstractPlacer: basePlacer,
		index:          NewStaticClusterIndex(),
	}

	if err := cluster.AddIndex(staticPlacer.index); err != nil {
		return nil, err
	}

	basePlacer.instance = staticPlacer
	return staticPlacer, nil
}

func (placer *StaticPlacer) getIndex() ClusterIndex {
	return placer.index
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHosts(spec types.Spec) []*Host {
	var (
		pos   interface{}
		host  *Host
		hosts = make([]*Host, placer.opts.NumReplicas)
	)
	for i := 0; i < len(hosts); i++ {
		host, pos = placer.index.SeekFrom(pos)

		if host.ResourceSpec().Validate(spec) {
			// The Host can satisfy the resourceSpec, so append it to the slice.
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHost(blacklist []interface{}, spec types.Spec) *Host {
	hosts, _ := placer.index.SeekMultipleFrom(nil, 1, func(candidateHost *Host) bool {
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
