package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type baseSchedulingPolicy struct {
	scalingConfiguration         *scheduling.ScalingConfiguration
	idleSessionReclamationPolicy scheduling.IdleSessionReclamationPolicy

	scalingOutEnabled bool

	// GpusPerHost is the number of GPUs available on each host.
	GpusPerHost int
}

func newBaseSchedulingPolicy(opts *scheduling.SchedulerOptions, scalingOutEnabled bool) (*baseSchedulingPolicy, error) {
	idleSessionReclamationPolicy, err := getIdleSessionReclamationPolicy(opts)
	if err != nil {
		return nil, err
	}

	basePolicy := &baseSchedulingPolicy{
		scalingConfiguration:         scheduling.NewScalingConfiguration(opts),
		idleSessionReclamationPolicy: idleSessionReclamationPolicy,
		scalingOutEnabled:            scalingOutEnabled,
		GpusPerHost:                  opts.GpusPerHost,
	}

	return basePolicy, nil
}

// GetGpusPerHost returns the number of GPUs available on each host.
func (p *baseSchedulingPolicy) GetGpusPerHost() int {
	return p.GpusPerHost
}

func (p *baseSchedulingPolicy) IdleSessionReclamationPolicy() scheduling.IdleSessionReclamationPolicy {
	return p.idleSessionReclamationPolicy
}

// DisableScalingOut modifies the scaling policy to disallow scaling-out, even if the policy isn't
// supposed to support scaling out. This is only intended to be used for unit tests.
func (p *baseSchedulingPolicy) DisableScalingOut() {
	p.scalingOutEnabled = false
}

// EnableScalingOut modifies the scaling policy to enable scaling-out, even if the policy isn't
// supposed to support scaling out. This is only intended to be used for unit tests.
func (p *baseSchedulingPolicy) EnableScalingOut() {
	p.scalingOutEnabled = true
}
