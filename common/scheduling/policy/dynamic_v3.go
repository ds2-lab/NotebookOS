package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type DynamicV3Policy struct {
	scalingConfiguration *scheduling.ScalingConfiguration
}

func NewDynamicV3Policy(opts *scheduling.SchedulerOptions) *DynamicV3Policy {
	return &DynamicV3Policy{
		scalingConfiguration: scheduling.NewScalingConfiguration(opts),
	}
}

func (p *DynamicV3Policy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	return p
}

func (p *DynamicV3Policy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	return p
}

func (p *DynamicV3Policy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	return p
}

func (p *DynamicV3Policy) PolicyKey() scheduling.PolicyKey {
	return scheduling.DynamicV3
}

func (p *DynamicV3Policy) Name() string {
	return "Dynamic Scheduling v3"
}

func (p *DynamicV3Policy) NumReplicas() int {
	return 3
}

func (p *DynamicV3Policy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesAtTrainingStart
}

func (p *DynamicV3Policy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.LongRunning
}

func (p *DynamicV3Policy) SmrEnabled() bool {
	return true
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *DynamicV3Policy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *DynamicV3Policy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *DynamicV3Policy) AutomaticScalingOutEnabled() bool {
	return true
}

func (p *DynamicV3Policy) AutomaticScalingInEnabled() bool {
	return true
}

func (p *DynamicV3Policy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *DynamicV3Policy) ManualScalingOutEnabled() bool {
	return true
}

func (p *DynamicV3Policy) ManualScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *DynamicV3Policy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *DynamicV3Policy) WriteOperationIsOnCriticalPath() bool {
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
func (p *DynamicV3Policy) ShouldPerformReadOperation() bool {
	return false
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *DynamicV3Policy) ReadOperationIsOnCriticalPath() bool {
	return false
}
