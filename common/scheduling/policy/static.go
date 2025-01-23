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

// SelectReadyReplica (optionally) selects a KernelReplica of the specified Kernel to be
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
// PRECONDITION: The resource spec of the specified scheduling.Kernel should already be
// updated (in cases where dynamic resource requests are supported) such that the current
// resource spec reflects the requirements for this code execution. That is, the logic of
// selecting a replica now depends upon the kernel's resource request correctly specifying
// the requirements. If the requirements were to change after selection a replica, then
// that could invalidate the selection.
func (p *StaticPolicy) SelectReadyReplica(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	replicas := kernel.Replicas()

	// Sort the replicas from most to least idle GPUs available on each replica's respective host.
	slices.SortFunc(replicas, func(r1, r2 scheduling.KernelReplica) int {
		// cmp(r1, r2) should return:
		// - r1 negative number when r1 < r2,
		// - r1 positive number when r1 > r2,
		// - and zero when r1 == r2.

		return int(r1.Host().IdleGPUs() - r2.Host().IdleGPUs())
	})

	// Check if first replica in the slice, which will now be the one whose Host has
	// the most idle GPUs, is able to serve as the leader for this code execution.
	//
	// If so, then we'll return this replica.
	//
	// TODO: This needs to be performed atomically.
	candidate := replicas[0]

	// TODO: This step needs to be performed atomically.
	if candidate.Host().CanCommitResources(candidate.ResourceSpec()) {
		return candidate, nil
	}

	// TODO: If we know that the replica on the least-loaded host cannot acquire the
	// 		 resources that it needs to begin training, then we should take action
	// 		 at this point to either migrate one of replicas of the specified kernel
	//		 OR preempt [actively-training] containers on the hosts of one of the
	// 		 replicas of the target kernel so that there are sufficient resources
	//		 available for the associated replica of the target kernel to begin training.
	return nil, nil
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
