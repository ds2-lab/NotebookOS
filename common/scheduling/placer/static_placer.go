package placer

import (
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

// StaticPlacer is a particular type of MultiPlacer that implements the logic for the static scheduling policy.
type StaticPlacer MultiPlacer[*index.StaticIndex]

// NewStaticPlacer creates and returns a StaticPlacer struct.
func NewStaticPlacer(metrics scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy) (*StaticPlacer, error) {
	provider := func(gpusPerHost int32) *index.StaticIndex {
		staticIndex, err := index.NewStaticIndex(gpusPerHost)
		if err != nil {
			panic(err)
		}

		return staticIndex
	}

	basePlacer, err := NewBasicPlacerWithSpecificIndex[*index.StaticIndex](metrics, numReplicas, policy, provider)
	if err != nil {
		return nil, err
	}

	staticPlacer := &StaticPlacer{
		BasicPlacer: basePlacer,
		placerId:    uuid.NewString(),
	}

	basePlacer.instance = staticPlacer

	return staticPlacer, nil
}

// getIndex returns the target MultiPlacer's index field with a type assertion
// so that it is returned as a *index.MultiIndex[T].
func (placer *StaticPlacer) getIndex() *index.StaticIndex {
	return placer.index.(*index.StaticIndex)
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiPlacer's index.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (placer *StaticPlacer) NumFreeHosts() int {
	return placer.getIndex().NumFreeHosts()
}

// HasHostPool returns true if the MultiPlacer's underlying MultiIndex has a host pool for the specified
// number of GPUs.
func (placer *StaticPlacer) HasHostPool(gpus int32) bool {
	return placer.getIndex().HasHostPool(gpus)
}

// NumHostsInPool returns the number of hosts in the specified host pool.
func (placer *StaticPlacer) NumHostsInPool(gpus int32) int {
	return placer.getIndex().NumHostsInPool(gpus)
}

// GetHostPool returns the index.HostPool for the specified number of GPUs.
//
// The index.HostPool will be the one responsible for containing scheduling.Host instances that serve
// sessions/kernels/jobs requiring `gpus` number of GPUs.
func (placer *StaticPlacer) GetHostPool(gpus int32) (*index.HostPool[*index.LeastLoadedIndex], bool) {
	return placer.getIndex().GetHostPool(gpus)
}
