package scheduling

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"sync"
)

// RandomPlacer is a simple placer that places sessions randomly.
type RandomPlacer struct {
	AbstractPlacer

	cluster Cluster
	index   *RandomClusterIndex
	opts    *CoreOptions

	mu sync.Mutex
}

// NewRandomPlacer creates a new RandomPlacer.
func NewRandomPlacer(cluster Cluster, opts *CoreOptions) (*RandomPlacer, error) {
	placer := &RandomPlacer{
		cluster: cluster,
		index:   NewRandomClusterIndex(100),
		opts:    opts,
	}

	if err := cluster.AddIndex(placer.index); err != nil {
		return nil, err
	}

	config.InitLogger(&placer.log, placer)
	return placer, nil
}

// FindHosts returns a slice of Host instances that can satisfy the resourceSpec.
func (placer *RandomPlacer) FindHosts(spec types.Spec) []*Host {
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
		host  *Host
		hosts = make([]*Host, numReplicas)
	)
	for i := 0; i < len(hosts); i++ {
		host, pos = placer.index.SeekFrom(pos)

		// If the Host can satisfy the resourceSpec, then add it to the slice of Host instances being returned.
		if host.ResourceSpec().Validate(spec) {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *RandomPlacer) FindHost(blacklist []interface{}, spec types.Spec) *Host {
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
