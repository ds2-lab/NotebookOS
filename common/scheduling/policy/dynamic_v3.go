package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"golang.org/x/net/context"
)

type DynamicV3Policy struct {
	*baseSchedulingPolicy
}

func NewDynamicV3Policy(opts *scheduling.SchedulerOptions, clusterProvider scheduling.ClusterProvider) (*DynamicV3Policy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true, clusterProvider)
	if err != nil {
		return nil, err
	}

	policy := &DynamicV3Policy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.DynamicV3.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of DynamicV3Policy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *DynamicV3Policy) ValidateCapacity(cluster scheduling.Cluster) {
	// Ensure we don't double-up on capacity validations. Only one at a time.
	if !p.isValidatingCapacity.CompareAndSwap(0, 1) {
		return
	}

	multiReplicaValidateCapacity(p, cluster, p.log)

	if !p.isValidatingCapacity.CompareAndSwap(1, 0) {
		panic("Failed to swap isValidatingCapacity 1 → 0 after finishing call to DynamicV3Policy::ValidateCapacity")
	}
}

// HandleFailedAttemptToGetViableHosts is called when the Scheduler fails to find the requested number of Host
// instances to serve the KernelReplica instance(s) of a particular Kernel.
func (p *DynamicV3Policy) HandleFailedAttemptToGetViableHosts(ctx context.Context, kernelSpec *proto.KernelSpec,
	numHosts int32, hosts []scheduling.Host) (bool, error) {

	shouldContinue := handleFailedAttemptToFindCandidateHosts(ctx, kernelSpec, numHosts, hosts, p.log, p)

	return shouldContinue, nil
}

// RequirePrewarmContainer indicates whether a new kernel replica must be placed within a prewarm container.
func (p *DynamicV3Policy) RequirePrewarmContainer() bool {
	return false
}

// PrioritizePrewarmContainers indicates whether the host selection process should prioritize hosts with
// a prewarm container available or not factor that into the placement decision.
func (p *DynamicV3Policy) PrioritizePrewarmContainers() bool {
	return false
}

// ReuseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
// placed back into the warm KernelContainer pool, or if it should simply be terminated.
//
// ReuseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
//
// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
func (p *DynamicV3Policy) ReuseWarmContainers() bool {
	return true
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

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *DynamicV3Policy) SupportsDynamicResourceAdjustments() bool {
	return true
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

// SelectReplicaForMigration selects a KernelReplica of the specified kernel to be migrated.
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

// FindReadyReplica (optionally) selects a KernelReplica of the specified kernel to be
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
func (p *DynamicV3Policy) FindReadyReplica(_ scheduling.Kernel, _ string) (scheduling.KernelReplica, error) { // TODO: Implement me.
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

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *DynamicV3Policy) SupportsPredictiveAutoscaling() bool {
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
