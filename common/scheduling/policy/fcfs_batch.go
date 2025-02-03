package policy

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

// FcfsBatchSchedulingPolicy is a scheduling.Policy modeled after Slurm-like first-come, first-serve batch schedulers.
// FcfsBatchSchedulingPolicy uses short-lived scheduling.KernelContainer instances that are created reactively each
// time a user submits a training task, and that are reclaimed when the training task finishes.
//
// The FcfsBatchSchedulingPolicy does not employ auto-scaling.
//
// Because KernelContainer instances are short-lived, FcfsBatchSchedulingPolicy effectively uses dynamic resource
// management.
type FcfsBatchSchedulingPolicy struct {
	*baseSchedulingPolicy
}

func NewFcfsBatchSchedulingPolicy(opts *scheduling.SchedulerOptions) (*FcfsBatchSchedulingPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, false)
	if err != nil {
		return nil, err
	}

	policy := &FcfsBatchSchedulingPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	if opts.MinimumNumNodes < policy.NumReplicas() {
		panic(fmt.Sprintf("Minimum number of nodes (%d) is incompatible with number of replicas (%d). Minimum number of nodes must be >= number of replicas.",
			opts.MinimumNumNodes, policy.NumReplicas()))
	}

	if opts.SchedulingPolicy != scheduling.FcfsBatch.String() {
		panic(fmt.Sprintf("Configured scheduling policy is \"%s\"; cannot create instance of FcfsBatchSchedulingPolicy.",
			opts.SchedulingPolicy))
	}

	return policy, nil
}

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *FcfsBatchSchedulingPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if p.SupportsMigration() {
		panic("FcfsBatchSchedulingPolicy isn't supposed to support migration, yet apparently it does?")
	}

	return nil, ErrMigrationNotSupported
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
func (p *FcfsBatchSchedulingPolicy) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	return defaultFindReadyReplicaSingleReplicaPolicy(kernel, p.supportsMigration, executionId)
}

// ValidateCapacity validates the Cluster's capacity according to the configured scheduling / scaling policy.
// Adjust the Cluster's capacity as directed by scaling policy.
func (p *FcfsBatchSchedulingPolicy) ValidateCapacity(cluster scheduling.Cluster) {
	singleReplicaValidateCapacity(p, cluster, p.log)
}

// SupportsPredictiveAutoscaling returns true if the Policy supports "predictive auto-scaling", in which
// the cluster attempts to adaptively resize itself in anticipation of request load fluctuations.
func (p *FcfsBatchSchedulingPolicy) SupportsPredictiveAutoscaling() bool {
	return true
}

// SupportsDynamicResourceAdjustments returns true if the Policy allows for dynamically altering the
// resource request of an existing/scheduled kernel after it has already been created, or if the
// initial resource request/allocation is static and cannot be changed after the kernel is created.
func (p *FcfsBatchSchedulingPolicy) SupportsDynamicResourceAdjustments() bool {
	// TODO: Should this return true or false?
	return true
}

func (p *FcfsBatchSchedulingPolicy) SmrEnabled() bool {
	return false
}

func (p *FcfsBatchSchedulingPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.FcfsBatch
}

func (p *FcfsBatchSchedulingPolicy) Name() string {
	return "First-Come, First-Serve Batch Scheduling"
}

func (p *FcfsBatchSchedulingPolicy) NumReplicas() int {
	return 1
}

func (p *FcfsBatchSchedulingPolicy) ResourceBindingMode() scheduling.ResourceBindingMode {
	return scheduling.BindResourcesWhenContainerScheduled
}

func (p *FcfsBatchSchedulingPolicy) ContainerLifetime() scheduling.ContainerLifetime {
	return scheduling.SingleTrainingEvent
}

func (p *FcfsBatchSchedulingPolicy) PostExecutionStatePolicy() scheduling.PostExecutionStatePolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.PostExecutionStatePolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

func (p *FcfsBatchSchedulingPolicy) PreExecutionStatePolicy() scheduling.PreExecutionStatePolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.PreExecutionStatePolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

func (p *FcfsBatchSchedulingPolicy) ResourceScalingPolicy() scheduling.ResourceScalingPolicy {
	// FcfsBatchSchedulingPolicy implements scheduling.ResourceScalingPolicy directly, so we
	// just return the target FcfsBatchSchedulingPolicy struct.
	return p
}

// GetNewPlacer returns a concrete Placer implementation based on the Policy.
func (p *FcfsBatchSchedulingPolicy) GetNewPlacer(metricsProvider scheduling.MetricsProvider) (scheduling.Placer, error) {
	return placer.NewBasicPlacer(metricsProvider, p.NumReplicas(), p), nil
}

func (p *FcfsBatchSchedulingPolicy) ScalingConfiguration() *scheduling.ScalingConfiguration {
	return p.scalingConfiguration
}

//////////////////////////////////
// ScalingPolicy implementation //
//////////////////////////////////

func (p *FcfsBatchSchedulingPolicy) ScalingOutEnabled() bool {
	return p.scalingOutEnabled
}

func (p *FcfsBatchSchedulingPolicy) ScalingInEnabled() bool {
	return true
}

/////////////////////////////////////////////
// PostExecutionStatePolicy implementation //
/////////////////////////////////////////////

// ShouldPerformWriteOperation returns a bool flag indicating whether the kernel should perform a (simulated)
// network write operation after a successful code execution.
func (p *FcfsBatchSchedulingPolicy) ShouldPerformWriteOperation() bool {
	return true
}

// WriteOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network write operation
// performed after a successful code execution should be on the critical path.
//
// If the ShouldPerformWriteOperation method of the target PostExecutionStatePolicy returns false, then
// the WriteOperationIsOnCriticalPath method will also return false.
func (p *FcfsBatchSchedulingPolicy) WriteOperationIsOnCriticalPath() bool {
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
func (p *FcfsBatchSchedulingPolicy) ShouldPerformReadOperation() bool {
	return true
}

// ReadOperationIsOnCriticalPath returns a bool flag indicating whether the (simulated) network read operation
// performed before executing user-submitted code should be on the critical path.
//
// If the ShouldPerformReadOperation method of the target PostExecutionStatePolicy returns false, then
// the ReadOperationIsOnCriticalPath method will also return false.
func (p *FcfsBatchSchedulingPolicy) ReadOperationIsOnCriticalPath() bool {
	return true
}
