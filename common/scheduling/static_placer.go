package scheduling

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"sync"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	AbstractPlacer

	cluster Cluster
	index   *StaticClusterIndex
	opts    *CoreOptions

	mu sync.Mutex
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(cluster Cluster, opts *CoreOptions) (*StaticPlacer, error) {
	placer := &StaticPlacer{
		cluster: cluster,
		index:   NewStaticClusterIndex(),
		opts:    opts,
	}

	if err := cluster.AddIndex(placer.index); err != nil {
		return nil, err
	}

	config.InitLogger(&placer.log, placer)
	return placer, nil
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) FindHosts(spec types.Spec) []Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	numReplicas := placer.opts.NumReplicas
	if placer.index.Len() < numReplicas {
		numReplicas = placer.index.Len()
	}
	if numReplicas == 0 {
		return nil
	}
	var (
		pos   interface{}
		host  Host
		hosts = make([]Host, numReplicas)
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
func (placer *StaticPlacer) FindHost(blacklist []interface{}, spec types.Spec) Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	host, _ := placer.index.Seek(blacklist)

	if host.ResourceSpec().Validate(spec) {
		// The Host can satisfy the resourceSpec, so return it.
		return host
	} else {
		// The Host could not satisfy the resourceSpec, so return nil.
		return nil
	}
}
