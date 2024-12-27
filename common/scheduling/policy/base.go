package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type baseSchedulingPolicy struct {
	scalingConfiguration         *scheduling.ScalingConfiguration
	idleSessionReclamationPolicy scheduling.IdleSessionReclamationPolicy
}

func newBaseSchedulingPolicy(opts *scheduling.SchedulerOptions) (*baseSchedulingPolicy, error) {
	idleSessionReclamationPolicy, err := getIdleSessionReclamationPolicy(opts)
	if err != nil {
		return nil, err
	}

	basePolicy := &baseSchedulingPolicy{
		scalingConfiguration:         scheduling.NewScalingConfiguration(opts),
		idleSessionReclamationPolicy: idleSessionReclamationPolicy,
	}

	return basePolicy, nil
}

func (p *baseSchedulingPolicy) IdleSessionReclamationPolicy() scheduling.IdleSessionReclamationPolicy {
	return p.idleSessionReclamationPolicy
}
