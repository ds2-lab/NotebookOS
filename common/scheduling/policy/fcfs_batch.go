package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

// FcfsBatchSchedulingPolicy is a scheduling.Policy modeled after Slurm-like first-come, first-serve batch schedulers.
// FcfsBatchSchedulingPolicy uses short-lived scheduling.KernelContainer instances that are created reactively each
// time a user submits a training task, and that are reclaimed when the training task finishes.
//
// The FcfsBatchSchedulingPolicy does not employ auto-scaling.
//
// Because KernelContainer instances are short-lived, FcfsBatchSchedulingPolicy effectively uses dynamic resource
// management.
type FcfsBatchSchedulingPolicy struct {
}

func NewFcfsBatchSchedulingPolicy() *FcfsBatchSchedulingPolicy {
	return &FcfsBatchSchedulingPolicy{}
}

func (p *FcfsBatchSchedulingPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.FcfsBatch
}

func (p *FcfsBatchSchedulingPolicy) Name() string {
	return "First-Come, First-Serve Batch Scheduling"
}

func (p *FcfsBatchSchedulingPolicy) NumReplicas() int {
	return 1
}

func (p *FcfsBatchSchedulingPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesAtTrainingStart
}

func (p *FcfsBatchSchedulingPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.SingleTrainingEvent
}

func (p *FcfsBatchSchedulingPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.PostExecutionStatePolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

func (p *FcfsBatchSchedulingPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.PreExecutionStatePolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

func (p *FcfsBatchSchedulingPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.ResourceScalingPolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *FcfsBatchSchedulingPolicy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *FcfsBatchSchedulingPolicy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *FcfsBatchSchedulingPolicy) AutomaticScalingOutEnabled() bool {
	return false
}

func (p *FcfsBatchSchedulingPolicy) AutomaticScalingInEnabled() bool {
	return false
}

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *FcfsBatchSchedulingPolicy) ManualScalingOutEnabled() bool {
	return false
}

func (p *FcfsBatchSchedulingPolicy) ManualScalingInEnabled() bool {
	return false
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *FcfsBatchSchedulingPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *FcfsBatchSchedulingPolicy) WriteOperationIsOnCriticalPath() bool {
	return true
}

/////////////////////////////////////////////
// PreExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformReadOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network read operation before executing user-submitted code.
//
// Such a read operation would be to retrieve the current or latest model state/parameters and any required
// training data.
func (p *FcfsBatchSchedulingPolicy) ShouldPerformReadOperation() bool {
	return true
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *FcfsBatchSchedulingPolicy) ReadOperationIsOnCriticalPath() bool {
	return true
}
