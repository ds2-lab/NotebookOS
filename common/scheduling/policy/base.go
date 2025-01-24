package policy

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

var (
	ErrMigrationNotSupported = errors.New("migration is not supported by this scheduling policy")
)

// GetIdleSessionReclamationPolicy returns the configured scheduling.IdleSessionReclamationPolicy, based on the
// associated parameter in the specified scheduling.SchedulerOptions struct.
//
// This is just used internally by the "constructors" of the various policy structs.
func GetIdleSessionReclamationPolicy(opts *scheduling.SchedulerOptions) (scheduling.IdleSessionReclamationPolicy, error) {
	if opts.IdleSessionReclamationPolicy == "" {
		return nil, fmt.Errorf("%w: unspecified (you did not specify one)", scheduling.ErrInvalidIdleSessionReclamationPolicy)
	}

	switch opts.IdleSessionReclamationPolicy {
	case string(scheduling.NoIdleSessionReclamation):
		{
			return &NoIdleSessionReclamationPolicy{Opts: opts}, nil
		}
	case string(scheduling.GoogleColabIdleSessionReclamationPolicy):
		{
			return &GoogleColabReclamationPolicy{Opts: opts}, nil
		}
	case string(scheduling.AdobeSenseiIdleSessionReclamationPolicy):
		{
			return &AdobeSenseiReclamationPolicy{Opts: opts}, nil
		}
	case string(scheduling.CustomIdleSessionReclamationPolicy):
		{
			return NewCustomColabReclamationPolicy(opts)
		}
	}

	return nil, fmt.Errorf("%w: \"%s\"", scheduling.ErrInvalidIdleSessionReclamationPolicy, opts.IdleSessionReclamationPolicy)
}

type baseSchedulingPolicy struct {
	scalingConfiguration         *scheduling.ScalingConfiguration
	idleSessionReclamationPolicy scheduling.IdleSessionReclamationPolicy

	scalingOutEnabled bool
	supportsMigration bool

	// GpusPerHost is the number of GPUs available on each host.
	GpusPerHost int

	log logger.Logger
}

func newBaseSchedulingPolicy(opts *scheduling.SchedulerOptions, scalingOutEnabled bool, supportsMigration bool) (*baseSchedulingPolicy, error) {
	idleSessionReclamationPolicy, err := GetIdleSessionReclamationPolicy(opts)
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

	config.InitLogger(&basePolicy.log, basePolicy)

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
