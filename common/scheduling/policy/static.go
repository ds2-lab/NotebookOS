package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/utils"
	"math/rand"
	"slices"
)

type StaticPolicy struct {
	*baseSchedulingPolicy
}

func NewStaticPolicy(opts *scheduling.SchedulerOptions) (*StaticPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true)
	if err != nil {
		return nil, err
	}

	policy := &StaticPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.Static.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of StaticPolicy.",
			opts.SchedulingPolicy))
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
	return placer.NewBasicPlacerWithSpecificIndex[*index.StaticIndex](metricsProvider, p.NumReplicas(), p, index.NewStaticIndex), nil
}

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *StaticPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if !p.SupportsMigration() {
		panic("StaticPolicy is supposed to support migration, yet apparently it doesn't?")
	}

	// Select a replica at random.
	targetReplicaId := int32(rand.Intn(kernel.Size()) + 1 /* IDs start at 1.  */)

	return kernel.GetReplicaByID(targetReplicaId)
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
//
// IMPORTANT NOTE:
//
// This method will only ever be called by one of the scheduling.Scheduler implementations, and they will
// always use a lock around the call to SelectReadyReplica, so the execution of this method is atomic.
func (p *StaticPolicy) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	p.log.Debug("Searching for ready replica of kernel \"%s\" for execution \"%s\"", kernel.ID(), executionId)

	replicas := kernel.Replicas()

	// First, we try to select the same primary replica as last time, if possible.
	lastPrimaryReplica := kernel.LastPrimaryReplica()
	if lastPrimaryReplica != nil {
		p.log.Debug("Attempting to reuse previous primary replica %d for new execution \"%s\" for kernel \"%s\"",
			lastPrimaryReplica.ReplicaID(), executionId, kernel.ID())

		allocationError := lastPrimaryReplica.Host().PreCommitResources(lastPrimaryReplica.Container(), executionId)
		if allocationError == nil {
			p.log.Debug(
				utils.LightGreenStyle.Render(
					"Resource pre-commitment succeeded. Previous primary replica %d of kernel %s will lead new execution \"%s\"."),
				lastPrimaryReplica.ReplicaID(), kernel.ID(), executionId)

			return lastPrimaryReplica, nil // Migration is permitted, so we never return an error.
		}

		// Failed to commit resources. Continue.
		p.log.Debug("Resource pre-commitment %s. Previous primary replica %d of kernel %s is not viable for next execution \"%s\".",
			utils.LightOrangeStyle.Render("failed"), lastPrimaryReplica.ReplicaID(), kernel.ID(), executionId)
	}

	// Sort the replicas from most to least idle GPUs available on each replica's respective host.
	slices.SortFunc(replicas, func(r1, r2 scheduling.KernelReplica) int {
		// cmp(r1, r2) should return:
		// - r1 negative number when r1 < r2,
		// - r1 positive number when r1 > r2,
		// - and zero when r1 == r2.

		// SortFunc sorts in ascending order. Normally, we'd do r1 - r2.
		// But we want to sort largest to smallest, so we flip the variables and do r2 - r1.
		return int(r2.Host().IdleGPUs() - r1.Host().IdleGPUs())
	})

	// For each replica (in order of most to least idle GPUs available on the host)...
	for _, candidateReplica := range replicas {
		p.log.Debug("Try pre-commit resources %v to replica %d of kernel %s whose host has %d idle GPUs",
			candidateReplica.ResourceSpec(), candidateReplica.ReplicaID(), candidateReplica.ID(),
			int(candidateReplica.Host().IdleGPUs()))
		// Try to commit resources to the candidateReplica replica.
		allocationError := candidateReplica.Host().PreCommitResources(candidateReplica.Container(), executionId)
		if allocationError != nil {
			// Failed to commit resources. Continue.
			p.log.Debug("Resource pre-commitment %s. Replica %d of kernel %s is not viable for execution \"%s\".",
				utils.LightOrangeStyle.Render("failed"), candidateReplica.ReplicaID(), kernel.ID(), executionId)
			continue
		}

		p.log.Debug(
			utils.LightGreenStyle.Render(
				"Resource pre-commitment succeeded. Identified viable replica %d of kernel %s for execution \"%s\"."),
			candidateReplica.ReplicaID(), kernel.ID(), executionId)

		return candidateReplica, nil // Migration is permitted, so we never return an error.
	}

	// If we've made it to this point, then we tried all replicas and none of them worked.
	// (That is, we were unable to commit resources to any of the replicas.)
	p.log.Debug(utils.YellowStyle.Render("Could not find eligible replica of kernel %s with resource request %v."),
		kernel.ID(), kernel.ResourceSpec().String())

	return nil, nil // Migration is permitted, so we never return an error.
}

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *StaticPolicy) SupportsDynamicResourceAdjustments() bool {
	return true
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *StaticPolicy) ValidateCapacity(cluster scheduling.Cluster) {
	// Ensure we don't double-up on capacity validations. Only one at a time.
	if !p.isValidatingCapacity.CompareAndSwap(0, 1) {
		return
	}

	multiReplicaValidateCapacity(p, cluster, p.log)

	if !p.isValidatingCapacity.CompareAndSwap(1, 0) {
		panic("Failed to swap isValidatingCapacity 1 → 0 after finishing call to StaticPolicy::ValidateCapacity")
	}
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

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *StaticPolicy) SupportsPredictiveAutoscaling() bool {
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
