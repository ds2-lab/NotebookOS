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

	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", placer.opts.NumReplicas, spec.String(), placer.index.Len())

	numReplicas := placer.opts.NumReplicas
	if placer.index.Len() == 0 {
		placer.log.Warn("Index is empty... returning empty slice of Hosts.")
		return make([]*Host, 0)
	} else if placer.index.Len() < numReplicas {
		placer.log.Warn("Index has just %d hosts (%d are required).", placer.index.Len(), placer.opts.NumReplicas)
		numReplicas = placer.index.Len()
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
			hosts[i] = host
			placer.log.Debug("Found viable host: %v. Identified hosts: %d.", host, len(hosts))
		} else {
			placer.log.Debug("Host %v cannot satisfy request %v. (Host resources: %v.)", host, host.ResourceSpec().String(), spec.String())
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
