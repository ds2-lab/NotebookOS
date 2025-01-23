package policy

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
)

type GandivaPolicy struct {
	*baseSchedulingPolicy
}

func NewGandivaPolicy(opts *scheduling.SchedulerOptions) (*GandivaPolicy, error) {
	basePolicy, err := newBaseSchedulingPolicy(opts, true, true)
	if err != nil {
		return nil, err
	}

	policy := &GandivaPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	return policy, nil
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

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *GandivaPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if !p.SupportsMigration() {
		panic("GandivaPolicy is supposed to support migration, yet apparently it doesn't?")
	}

	// There should only be one replica, so return the one replica.
	return kernel.GetReplicaByID(1) // IDs start at 1.
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
func (p *GandivaPolicy) SelectReadyReplica(_ scheduling.Kernel) (scheduling.KernelReplica, error) {
	// There will only be one replica under GandivaPolicy.
	// Rather than return that replica, GandivaPolicy simply returns nil.
	// The default behavior will just be to forward the "execute_request" to all replicas,
	// or in this case, the single replica of the specified scheduling.Kernel.
	return nil, nil
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
