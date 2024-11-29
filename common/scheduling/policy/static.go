package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type StaticPolicy struct {
	scalingConfiguration *scheduling.ScalingConfiguration
}

func NewStaticPolicy(opts *scheduling.SchedulerOptions) *StaticPolicy {
	return &StaticPolicy{
		scalingConfiguration: scheduling.NewScalingConfiguration(opts),
	}
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

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *StaticPolicy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *StaticPolicy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
}

func (p *StaticPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *StaticPolicy) AutomaticScalingOutEnabled() bool {
	return true
}

func (p *StaticPolicy) AutomaticScalingInEnabled() bool {
	return true
}

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *StaticPolicy) ManualScalingOutEnabled() bool {
	return true
}

func (p *StaticPolicy) ManualScalingInEnabled() bool {
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
