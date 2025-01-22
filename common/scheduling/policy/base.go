package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type baseSchedulingPolicy struct {
	scalingConfiguration         *scheduling.ScalingConfiguration
	idleSessionReclamationPolicy scheduling.IdleSessionReclamationPolicy

	scalingOutEnabled bool
	supportsMigration bool

	// GpusPerHost is the number of GPUs available on each host.
	GpusPerHost int
}

func newBaseSchedulingPolicy(opts *scheduling.SchedulerOptions, scalingOutEnabled bool, supportsMigration bool) (*baseSchedulingPolicy, error) {
	idleSessionReclamationPolicy, err := getIdleSessionReclamationPolicy(opts)
	if err != nil {
		return nil, err
	}

	basePolicy := &baseSchedulingPolicy{
		scalingConfiguration:         scheduling.NewScalingConfiguration(opts),
		idleSessionReclamationPolicy: idleSessionReclamationPolicy,
		scalingOutEnabled:            scalingOutEnabled,
		GpusPerHost:                  opts.GpusPerHost,
		supportsMigration:            supportsMigration,
	}

	return basePolicy, nil
}

// SupportsMigration returns true if the Policy allows for the migration of one or more replicas of
// a kernel when no replicas are able to serve a code execution request.
//
// If SupportsMigration returns false, then it is up to the client to resubmit the request.
func (p *baseSchedulingPolicy) SupportsMigration() bool {
	return p.supportsMigration
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
