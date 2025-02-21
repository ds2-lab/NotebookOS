package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type GandivaPolicy struct {
	*baseSchedulingPolicy
}

func NewGandivaPolicy(opts *scheduling.SchedulerOptions, clusterProvider scheduling.ClusterProvider) (*GandivaPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true, clusterProvider)
	if err != nil {
		return nil, err
	}

	policy := &GandivaPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.Gandiva.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of GandivaPolicy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
}

// ReuseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
// placed back into the warm KernelContainer pool, or if it should simply be terminated.
//
// ReuseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
//
// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
func (p *GandivaPolicy) ReuseWarmContainers() bool {
	return false
}

// RequirePrewarmContainer indicates whether a new kernel replica must be placed within a prewarm container.
func (p *GandivaPolicy) RequirePrewarmContainer() bool {
	return false
}

// PrioritizePrewarmContainers indicates whether the host selection process should prioritize hosts with
// a prewarm container available or not factor that into the placement decision.
func (p *GandivaPolicy) PrioritizePrewarmContainers() bool {
	return false
}

func (p *GandivaPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	return p
}

func (p *GandivaPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	return p
}

func (p *GandivaPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	return p
}

func (p *GandivaPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.Gandiva
}

func (p *GandivaPolicy) Name() string {
	return "Gandiva"
}

func (p *GandivaPolicy) NumReplicas() int {
	return 1
}

func (p *GandivaPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesWhenContainerScheduled
}

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *GandivaPolicy) SupportsDynamicResourceAdjustments() bool {
	// TODO: Should this return true or false?
	return true
}

func (p *GandivaPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.SingleTrainingEvent
}

func (p *GandivaPolicy) SmrEnabled() bool {
	return true
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *GandivaPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewGandivaPlacer(metricsProvider, p.NumReplicas(), p)
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *GandivaPolicy) ValidateCapacity(_ scheduling.Cluster) {
	// Ensure we don't double-up on capacity validations. Only one at a time.
	if !p.isValidatingCapacity.CompareAndSwap(0, 1) {
		return
	}

	// No-Op. Do nothing.

	if !p.isValidatingCapacity.CompareAndSwap(1, 0) {
		panic("Failed to swap isValidatingCapacity 1 → 0 after finishing call to ReservationPolicy::ValidateCapacity")
	}
}

// SelectReplicaForMigration selects a KernelReplica of the specified kernel to be migrated.
func (p *GandivaPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if !p.SupportsMigration() {
		panic("GandivaPolicy is supposed to support migration, yet apparently it doesn't?")
	}

	// There should only be one replica, so return the one replica.
	return kernel.GetReplicaByID(1) // IDs start at 1.
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
func (p *GandivaPolicy) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	return defaultFindReadyReplicaSingleReplicaPolicy(p, kernel, executionId)
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *GandivaPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *GandivaPolicy) SupportsPredictiveAutoscaling() bool {
	return false
}

func (p *GandivaPolicy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *GandivaPolicy) ScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *GandivaPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *GandivaPolicy) WriteOperationIsOnCriticalPath() bool {
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
func (p *GandivaPolicy) ShouldPerformReadOperation() bool {
	return false
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *GandivaPolicy) ReadOperationIsOnCriticalPath() bool {
	return false
}
