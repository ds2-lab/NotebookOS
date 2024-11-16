package placer

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"sync"
)

// StaticPlacer is a placer that implements the Static Scheduling algorithm.
type StaticPlacer struct {
	*AbstractPlacer

	index *index.StaticClusterIndex

	mu sync.Mutex
}

// NewStaticPlacer creates a new StaticPlacer.
func NewStaticPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int) (*StaticPlacer, error) {
	basePlacer := NewAbstractPlacer(metricsProvider, numReplicas)

	staticPlacer := &StaticPlacer{
		AbstractPlacer: basePlacer,
		index:          index.NewStaticClusterIndex(),
	}

	basePlacer.instance = staticPlacer
	return staticPlacer, nil
}

// GetIndex returns the ClusterIndex of the specific Placer implementation.
func (placer *StaticPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.index
}

// FindHosts returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	panic("Not implemented")
}

// FindHost returns a single host that can satisfy the resourceSpec.
func (placer *StaticPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec) scheduling.Host {
	panic("Not implemented")
}

// NumHostsInIndex returns the length of the StaticPlacer's index.
func (placer *StaticPlacer) NumHostsInIndex() int {
	return placer.index.Len()
}
