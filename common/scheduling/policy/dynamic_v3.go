package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type DynamicV3Policy struct {
	*baseSchedulingPolicy
}

func NewDynamicV3Policy(opts *scheduling.SchedulerOptions) (*DynamicV3Policy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true)
	if err != nil {
		return nil, err
	}

	policy := &DynamicV3Policy{
		baseSchedulingPolicy: basePolicy,
	}

	return policy, nil
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

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *DynamicV3Policy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

func (p *DynamicV3Policy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *DynamicV3Policy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *DynamicV3Policy) ScalingInEnabled() bool {
	return p.scalingOutEnabled
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
