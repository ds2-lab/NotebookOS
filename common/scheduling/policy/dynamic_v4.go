package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type DynamicV4Policy struct {
	*baseSchedulingPolicy
}

func NewDynamicV4Policy(opts *scheduling.SchedulerOptions) (*DynamicV4Policy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true)
	if err != nil {
		return nil, err
	}

	policy := &DynamicV4Policy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.SchedulingPolicy != scheduling.DynamicV4.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of DynamicV4Policy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
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
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

func (p *DynamicV4Policy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *DynamicV4Policy) SelectReplicaForMigration(_ scheduling.Kernel) (scheduling.KernelReplica, error) {
	if !p.SupportsMigration() {
		panic("DynamicV4Policy is supposed to support migration, yet apparently it doesn't?")
	}

	// TODO: Implement me.
	panic("Not implemented.")
}

// FindReadyReplica (optionally) selects a KernelReplica of the specified Kernel to be
// pre-designated as the leader of a code execution.
//
// If the returned KernelReplica is nil and the returned error is nil, then that indicates
// that no KernelReplica is being pre-designated as the leader, and the KernelReplicas
// will fight amongst themselves to determine the leader.
//
// If a non-nil KernelReplica is returned, then the "execute_request" messages that are
// forwarded to that KernelReplica's peers should first be converted to "yield_request"
// messages, thereby ensuring that the selected KernelReplica becomes the leader.
//
// FindReadyReplica also returns a map of ineligible replicas, or replicas that have already
// been ruled out.
//
// PRECONDITION: The resource spec of the specified scheduling.Kernel should already be
// updated (in cases where dynamic resource requests are supported) such that the current
// resource spec reflects the requirements for this code execution. That is, the logic of
// selecting a replica now depends upon the kernel's resource request correctly specifying
// the requirements. If the requirements were to change after selection a replica, then
// that could invalidate the selection.
func (p *DynamicV4Policy) FindReadyReplica(_ scheduling.Kernel, _ string) (scheduling.KernelReplica, error) {
	// TODO: Implement me.
	panic("Not implemented.")
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *DynamicV4Policy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *DynamicV4Policy) ScalingInEnabled() bool {
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
