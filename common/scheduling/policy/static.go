package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
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
	replicas := kernel.Replicas()

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
		p.log.Debug("Attempting to pre-commit resources to replica %d of kernel %s whose host has %d idle GPUs",
			candidateReplica.ReplicaID(), candidateReplica.ID(), int(candidateReplica.Host().IdleGPUs()))
		// Try to commit resources to the candidateReplica replica.
		allocationError := candidateReplica.Host().PreCommitResources(candidateReplica.Container(), executionId)
		if allocationError != nil {
			// Failed to commit resources. Continue.
			continue
		}

		// Successfully committed resources.
		p.log.Debug("Identified viable candidate replica of kernel %s with resource request %v: replica %d.",
			kernel.ID(), kernel.ResourceSpec().String(), candidateReplica.ReplicaID())

		return candidateReplica, nil // Migration is permitted, so we never return an error.
	}

	// If we've made it to this point, then we tried all replicas and none of them worked.
	// (That is, we were unable to commit resources to any of the replicas.)
	p.log.Debug("Could not find eligible replica of kernel %s with resource request %v.",
		kernel.ID(), kernel.ResourceSpec().String())

	return nil, nil // Migration is permitted, so we never return an error.
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
