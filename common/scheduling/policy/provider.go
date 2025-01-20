package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// GetSchedulingPolicy returns the appropriate scheduling policy based on the provided configuration.
//
// If it is unclear which scheduling.Policy should be returned, then a scheduling.ErrInvalidSchedulingPolicy will be
// returned.
func GetSchedulingPolicy(opts *scheduling.SchedulerOptions) (scheduling.Policy, error) {
	if opts.SchedulingPolicy == "" {
		return nil, fmt.Errorf("%w: unspecified (you did not specify one)", scheduling.ErrInvalidSchedulingPolicy)
	}

	switch opts.SchedulingPolicy {
	case string(scheduling.Reservation):
		{
			return NewReservationPolicy(opts)
		}
	case string(scheduling.FcfsBatch):
		{
			return NewFcfsBatchSchedulingPolicy(opts)
		}
	case string(scheduling.AutoScalingFcfsBatch):
		{
			return NewAutoScalingFcfsBatchSchedulingPolicy(opts)
		}
	case string(scheduling.Static):
		{
			return NewStaticPolicy(opts)
		}
	case string(scheduling.DynamicV3):
		{
			return NewDynamicV3Policy(opts)
		}
	case string(scheduling.DynamicV4):
		{
			return NewDynamicV4Policy(opts)
		}
	case string(scheduling.Gandiva):
		{
			return NewGandivaPolicy(opts)
		}
	}
	return nil, fmt.Errorf("%w: \"%s\"", scheduling.ErrInvalidSchedulingPolicy, opts.SchedulingPolicy)
}

// getIdleSessionReclamationPolicy returns the configured scheduling.IdleSessionReclamationPolicy, based on the
// associated parameter in the specified scheduling.SchedulerOptions struct.
//
// This is just used internally by the "constructors" of the various policy structs.
func getIdleSessionReclamationPolicy(opts *scheduling.SchedulerOptions) (scheduling.IdleSessionReclamationPolicy, error) {
	if opts.IdleSessionReclamationPolicy == "" {
		return nil, fmt.Errorf("%w: unspecified (you did not specify one)", scheduling.ErrInvalidIdleSessionReclamationPolicy)
	}

	switch opts.IdleSessionReclamationPolicy {
	case string(scheduling.NoIdleSessionReclamation):
		{
			return &noIdleSessionReclamationPolicy{opts: opts}, nil
		}
	case string(scheduling.GoogleColabIdleSessionReclamationPolicy):
		{
			return &googleColabReclamationPolicy{opts: opts}, nil
		}
	case string(scheduling.AdobeSenseiIdleSessionReclamationPolicy):
		{
			return &adobeSenseiReclamationPolicy{opts: opts}, nil
		}
	case string(scheduling.CustomIdleSessionReclamationPolicy):
		{
			return newCustomColabReclamationPolicy(opts)
		}
	}

	return nil, fmt.Errorf("%w: \"%s\"", scheduling.ErrInvalidIdleSessionReclamationPolicy, opts.IdleSessionReclamationPolicy)
}
