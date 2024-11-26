package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

type DynamicV4Policy struct{}

func (p *DynamicV4Policy) PolicyKey() scheduling.PolicyKey {
	return scheduling.DynamicV3
}

func (p *DynamicV4Policy) Name() string {
	return "Dynamic Scheduling v3"
}

func (p *DynamicV4Policy) NumReplicas() int {
	return 3
}

func (p *DynamicV4Policy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesAtTrainingStart
}

func (p *DynamicV4Policy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.LongRunning
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *DynamicV4Policy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *DynamicV4Policy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *DynamicV4Policy) AutomaticScalingOutEnabled() bool {
	return true
}

func (p *DynamicV4Policy) AutomaticScalingInEnabled() bool {
	return true
}

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *DynamicV4Policy) ManualScalingOutEnabled() bool {
	return true
}

func (p *DynamicV4Policy) ManualScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *DynamicV4Policy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *DynamicV4Policy) WriteOperationIsOnCriticalPath() bool {
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
func (p *DynamicV4Policy) ShouldPerformReadOperation() bool {
	return false
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *DynamicV4Policy) ReadOperationIsOnCriticalPath() bool {
	return false
}
