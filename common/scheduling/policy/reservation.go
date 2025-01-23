package policy

import (
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
	basePolicy, err := newBaseSchedulingPolicy(opts, true, false)
	if err != nil {
		return nil, err
	}

	policy := &ReservationPolicy{
		baseSchedulingPolicy: basePolicy,
	}

	return policy, nil
}

// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
func (p *ReservationPolicy) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	if p.SupportsMigration() {
		panic("ReservationPolicy isn't supposed to support migration, yet apparently it does?")
	}

	return nil, ErrMigrationNotSupported
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
func (p *ReservationPolicy) SelectReadyReplica(_ scheduling.Kernel) (scheduling.KernelReplica, error) {
	// There will only be one replica under ReservationPolicy.
	// Rather than return that replica, ReservationPolicy simply returns nil.
	// The default behavior will just be to forward the "execute_request" to all replicas,
	// or in this case, the single replica of the specified scheduling.Kernel.
	return nil, nil
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
