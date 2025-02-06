package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

// ReservationPolicy represents a very simple reservation-based scheduling policy that uses long-running
// scheduling.KernelContainer instances.
//
// ReservationPolicy employs auto-scaling, but ReservationPolicy does not employ dynamic resource management.
// Under ReservationPolicy, resources are bound to scheduling.KernelContainer instances for the duration of the
// lifetime of the associated scheduling.UserSession.
type ReservationPolicy struct {
	*baseSchedulingPolicy
}

func NewReservationPolicy(opts *scheduling.SchedulerOptions) (*ReservationPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true)
	if err != nil {
		return nil, err
	}

	policy := &ReservationPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.Reservation.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of ReservationPolicy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
}

// UseWarmContainers returns a boolean indicating whether a warm KernelContainer should be re-used, such as being
// placed back into the warm KernelContainer pool, or if it should simply be terminated.
//
// UseWarmContainers is used in conjunction with ContainerLifetime to determine what to do with the container of a
// Kernel when the Policy specifies the ContainerLifetime as SingleTrainingEvent. Specifically, for policies like
// FCFS Batch Scheduling, the warm KernelContainer will simply be destroyed.
//
// But for the "middle ground" approach, a warm KernelContainer will be returned to the warm KernelContainer pool.
func (p *ReservationPolicy) UseWarmContainers() bool {
	return false
}

// SelectReplicaForMigration selects a KernelReplica of the specified kernel to be migrated.
func (p *ReservationPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	replicas := kernel.Replicas()

	if len(replicas) == 0 {
		return nil, fmt.Errorf("kernel '%s' does not have a replica", kernel.ID())
	}

	return replicas[0], nil
}

func (p *ReservationPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.Reservation
}

func (p *ReservationPolicy) Name() string {
	return "Reservation-Based"
}

func (p *ReservationPolicy) NumReplicas() int {
	return 1
}

func (p *ReservationPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	// The reservation-based policy uses a single long-running replica that remains scheduled for the
	// duration of its lifetime.
	return scheduling.BindResourcesWhenContainerScheduled
}

func (p *ReservationPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.LongRunning
}

func (p *ReservationPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	return p
}

func (p *ReservationPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	return p
}

func (p *ReservationPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	return p
}

func (p *ReservationPolicy) SmrEnabled() bool {
	return false
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *ReservationPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *ReservationPolicy) ValidateCapacity(cluster scheduling.Cluster) {
	// Ensure we don't double-up on capacity validations. Only one at a time.
	if !p.isValidatingCapacity.CompareAndSwap(0, 1) {
		return
	}

	singleReplicaValidateCapacity(p, cluster, p.log)

	if !p.isValidatingCapacity.CompareAndSwap(1, 0) {
		panic("Failed to swap isValidatingCapacity 1 → 0 after finishing call to ReservationPolicy::ValidateCapacity")
	}
}

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *ReservationPolicy) SupportsDynamicResourceAdjustments() bool {
	return false
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
func (p *ReservationPolicy) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	return defaultFindReadyReplicaSingleReplicaPolicy(p, kernel, executionId)
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *ReservationPolicy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *ReservationPolicy) ScalingInEnabled() bool {
	return true
}

func (p *ReservationPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *ReservationPolicy) SupportsPredictiveAutoscaling() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *ReservationPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *ReservationPolicy) WriteOperationIsOnCriticalPath() bool {
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
func (p *ReservationPolicy) ShouldPerformReadOperation() bool {
	return true
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *ReservationPolicy) ReadOperationIsOnCriticalPath() bool {
	return true
}
