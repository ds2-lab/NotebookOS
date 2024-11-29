package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// GetSchedulingPolicy returns the appropriate scheduling policy based on the provided configuration.
//
// If it is unclear which scheduling.Policy should be returned, then a scheduling.ErrInvalidSchedulingPolicy will be
// returned.
func GetSchedulingPolicy(opts *scheduling.SchedulerOptions) (scheduling.Policy, error) {
	switch opts.SchedulingPolicy {
	case string(scheduling.Reservation):
		{
			return NewReservationPolicy(opts), nil
		}
	case string(scheduling.FcfsBatch):
		{
			return NewFcfsBatchSchedulingPolicy(opts), nil
		}
	case string(scheduling.AutoScalingFcfsBatch):
		{
			return NewAutoScalingFcfsBatchSchedulingPolicy(opts), nil
		}
	case string(scheduling.Static):
		{
			return NewStaticPolicy(opts), nil
		}
	case string(scheduling.DynamicV3):
		{
			return NewDynamicV3Policy(opts), nil
		}
	case string(scheduling.DynamicV4):
		{
			return NewDynamicV4Policy(opts), nil
		}
	}
	return nil, scheduling.ErrInvalidSchedulingPolicy
}
