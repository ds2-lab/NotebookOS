package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type StaticPolicy struct {
	*baseSchedulingPolicy
}

func NewStaticPolicy(opts *scheduling.SchedulerOptions) (*StaticPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true)
	if err != nil {
		return nil, err
	}

	policy := &StaticPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	return policy, nil
}

func (p *StaticPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	return p
}

func (p *StaticPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	return p
}

func (p *StaticPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	return p
}

func (p *StaticPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.Static
}

func (p *StaticPolicy) Name() string {
	return "Static Scheduling"
}

func (p *StaticPolicy) NumReplicas() int {
	return 3
}

func (p *StaticPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesAtTrainingStart
}

func (p *StaticPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.LongRunning
}

func (p *StaticPolicy) SmrEnabled() bool {
	return true
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *StaticPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p)
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *StaticPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *StaticPolicy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *StaticPolicy) ScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *StaticPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *StaticPolicy) WriteOperationIsOnCriticalPath() bool {
	return false
}

/////////////////////////////////////////////
// PreExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformReadOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network read operation before executing user-submitted code.
//
// Such a read operation would be to retrieve the current or latest model state/parameters and any required
// training data.
func (p *StaticPolicy) ShouldPerformReadOperation() bool {
	return false
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *StaticPolicy) ReadOperationIsOnCriticalPath() bool {
	return false
}
