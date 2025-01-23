package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type DynamicV3Policy struct {
	*baseSchedulingPolicy
}

func NewDynamicV3Policy(opts *scheduling.SchedulerOptions) (*DynamicV3Policy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true)
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

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *DynamicV3Policy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if !p.SupportsMigration() {
		panic("DynamicV3Policy is supposed to support migration, yet apparently it doesn't?")
	}

	// Identify containers that are eligible for preemption.
	//penalties := &scheduling.PenaltyContainers{}
	//maxBenefit := 0.0
	//preemptionBuffer := make(scheduling.ContainerList, p.GpusPerHost)
	//requiredGPUs := kernel.ResourceSpec().GPU()
	//replicas := kernel.Replicas()
	//
	//var candidate scheduling.KernelContainer
	//for _, replica := range replicas {
	//	pp, pl, err := replica.Host().Penalty(requiredGPUs)
	//	if err != nil {
	//		p.log.Debug("%v: required %f GPUs, found %v", err, requiredGPUs, pl)
	//		continue
	//	}
	//
	//	container := replica.Container()
	//	ip := container.InteractivePriority()
	//	benefit := ip - pp
	//
	//	p.log.Debug("Got benefit of preempting from %v: %f = %f(%s) - %f(%v)", container.ContainerID(), benefit,
	//		ip, container.Explain(scheduling.ExplainInteractivePriority), pp, pl)
	//
	//	if benefit > maxBenefit {
	//		maxBenefit = benefit
	//		preemptionBuffer = preemptionBuffer[:len(pl.Candidates())]
	//		// make a copy to avoid changes due to preemption
	//		copy(preemptionBuffer, pl.Candidates())
	//		candidate = container
	//	}
	//}
	//
	//// TODO: The logic here -- from dynamic_v3 FindReadyContainer -- is also identifying co-located containers
	//// to migrate (colocated with one of the replicas).
	//
	//if len(preemptionBuffer) == 0 {
	//	return nil, fmt.Errorf("could not find eligible replica for preemption/migration")
	//}
	//
	//penalties.ContainerList = preemptionBuffer

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
func (p *DynamicV3Policy) FindReadyReplica(_ scheduling.Kernel) (scheduling.KernelReplica, error) {
	// TODO: Implement me.
	panic("Not implemented.")
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
