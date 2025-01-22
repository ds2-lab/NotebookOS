package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

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

func NewAutoScalingFcfsBatchSchedulingPolicy(opts *scheduling.SchedulerOptions) (*AutoScalingFcfsBatchSchedulingPolicy, error) {
	basePolicy, err := NewFcfsBatchSchedulingPolicy(opts)
	if err != nil {
		return nil, err
	}

	policy := &AutoScalingFcfsBatchSchedulingPolicy{
		FcfsBatchSchedulingPolicy: basePolicy,
	}

	return policy, nil
}

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *AutoScalingFcfsBatchSchedulingPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if p.SupportsMigration() {
		panic("AutoScalingFcfsBatchSchedulingPolicy isn't supposed to support migration, yet apparently it does?")
	}

	return nil
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *AutoScalingFcfsBatchSchedulingPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	// AutoScalingFcfsBatchSchedulingPolicy implements scheduling.ResourceScalingPolicy directly, so we
	// just return the target AutoScalingFcfsBatchSchedulingPolicy struct.
	return p
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) SmrEnabled() bool {
	return false
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *AutoScalingFcfsBatchSchedulingPolicy) ScalingOutEnabled() bool {
	return true
}

func (p *AutoScalingFcfsBatchSchedulingPolicy) ScalingInEnabled() bool {
	return true
}
