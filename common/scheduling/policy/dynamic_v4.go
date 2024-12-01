package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type DynamicV4Policy struct {
	scalingConfiguration *scheduling.ScalingConfiguration
}

func NewDynamicV4Policy(opts *scheduling.SchedulerOptions) *DynamicV4Policy {
	return &DynamicV4Policy{
		scalingConfiguration: scheduling.NewScalingConfiguration(opts),
	}
}

func (p *DynamicV4Policy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	return p
}

func (p *DynamicV4Policy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	return p
}

func (p *DynamicV4Policy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	return p
}

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

func (p *DynamicV4Policy) SmrEnabled() bool {
	return true
}

func (p *DynamicV4Policy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.LongRunning
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *DynamicV4Policy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewStaticPlacer(metricsProvider, p.NumReplicas(), p)
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

func (p *DynamicV4Policy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
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
