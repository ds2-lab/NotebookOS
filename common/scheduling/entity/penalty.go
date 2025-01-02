package entity

import "github.com/scusemua/distributed-notebook/common/scheduling"

type cachedPenalty struct {
	penalty     float64
	explain     string
	preemptions scheduling.ContainerList
	valid       bool
}

func (p *cachedPenalty) Penalty() float64 {
	return p.penalty
}

func (p *cachedPenalty) String() string {
	return p.explain
}

func (p *cachedPenalty) Candidates() scheduling.ContainerList {
	return p.preemptions[:]
}
