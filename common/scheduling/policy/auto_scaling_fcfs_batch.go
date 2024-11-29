package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

// AutoScalingFcfsBatchSchedulingPolicy is an extension of FcfsBatchSchedulingPolicy scheduling.Policy.
// Like FcfsBatchSchedulingPolicy, AutoScalingFcfsBatchSchedulingPolicy is modeled after Slurm-like first-come,
// first-serve batch schedulers.
//
// AutoScalingFcfsBatchSchedulingPolicy uses short-lived scheduling.KernelContainer instances that are created
// reactively each time a user submits a training task, and that are reclaimed when the training task finishes.
//
// The FcfsBatchSchedulingPolicy employs auto-scaling and dynamic resource management.
type AutoScalingFcfsBatchSchedulingPolicy struct {
	*FcfsBatchSchedulingPolicy
}

func NewAutoScalingFcfsBatchSchedulingPolicy(opts *scheduling.SchedulerOptions) *AutoScalingFcfsBatchSchedulingPolicy {
	return &AutoScalingFcfsBatchSchedulingPolicy{
		FcfsBatchSchedulingPolicy: NewFcfsBatchSchedulingPolicy(opts),
	}
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	// AutoScalingFcfsBatchSchedulingPolicy implements scheduling.ResourceScalingPolicy directly, so we
	// just return the target AutoScalingFcfsBatchSchedulingPolicy struct.
	return p
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) SmrEnabled() bool {
	return false
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *AutoScalingFcfsBatchSchedulingPolicy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *AutoScalingFcfsBatchSchedulingPolicy) AutomaticScalingOutEnabled() bool {
	return true
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) AutomaticScalingInEnabled() bool {
	return true
}

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *AutoScalingFcfsBatchSchedulingPolicy) ManualScalingOutEnabled() bool {
	return true
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) ManualScalingInEnabled() bool {
	return true
}
