package scheduling

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	AbstractPlacer

	cluster Cluster
	opts    *CoreOptions
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(cluster Cluster, opts *CoreOptions) (*StaticPlacer, error) {
	placer := &StaticPlacer{
		cluster: cluster,
		opts:    opts,
	}

	//if err := cluster.AddIndex(placer.index); err != nil {
	//	return nil, err
	//}

	config.InitLogger(&placer.log, placer)
	return placer, nil
}

// FindHosts returns a single host that can satisfy the spec.
func (placer *StaticPlacer) FindHosts(spec types.Spec) []Host {
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
func (placer *StaticPlacer) FindHost(blacklist []interface{}, spec types.Spec) Host {
	host, _ := placer.index.Seek(blacklist)
	return host
}
