package core

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

// RandomPlacer is a simple placer that places sessions randomly.
type RandomPlacer struct {
	AbstractPlacer

	cluster Cluster
	index   *RandomClusterIndex
	opts    *CoreOptions
}

// NewRandomPlacer creates a new RandomPlacer.
func NewRandomPlacer(cluster Cluster, opts *CoreOptions) *RandomPlacer {
	placer := &RandomPlacer{
		cluster: cluster,
		index:   NewRandomClusterIndex(100),
		opts:    opts,
	}
	cluster.AddIndex(placer.index)
	config.InitLogger(&placer.log, placer)
	return placer
}

// FindHost returns a single host that can satisfy the spec.
func (placer *RandomPlacer) FindHosts(spec types.Spec) []Host {
	numReplicas := placer.opts.NumReplicas
	if placer.index.Len() < numReplicas {
		numReplicas = placer.index.Len()
	}
	if numReplicas == 0 {
		return nil
	}
	var pos interface{}
	hosts := make([]Host, numReplicas)
	for i := 0; i < len(hosts); i++ {
		hosts[i], pos = placer.index.SeekFrom(pos)
	}
	return hosts
}

// FindHost returns a single host that can satisfy the spec.
func (placer *RandomPlacer) FindHost(blacklist []interface{}, spec types.Spec) Host {
	host, _ := placer.index.Seek(blacklist)
	return host
}
