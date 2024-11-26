package policy

import "github.com/scusemua/distributed-notebook/common/scheduling"

// ReservationPolicy represents a very simple reservation-based scheduling policy that uses long-running
// scheduling.KernelContainer instances.
//
// ReservationPolicy employs auto-scaling, but ReservationPolicy does not employ dynamic resource management.
// Under ReservationPolicy, resources are bound to scheduling.KernelContainer instances for the duration of the
// lifetime of the associated scheduling.UserSession.
type ReservationPolicy struct{}

func (p *ReservationPolicy) PolicyKey() scheduling.PolicyKey {
	return scheduling.Reservation
}

func (p *ReservationPolicy) Name() string {
	return "Reservation"
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

////////////////////////////////////////
// ManualScalingPolicy implementation //
////////////////////////////////////////

func (p *ReservationPolicy) ManualScalingOutEnabled() bool {
	return true
}

func (p *ReservationPolicy) ManualScalingInEnabled() bool {
	return true
}

//////////////////////////////////////
// AutoscalingPolicy implementation //
//////////////////////////////////////

func (p *ReservationPolicy) AutomaticScalingOutEnabled() bool {
	return true
}

func (p *ReservationPolicy) AutomaticScalingInEnabled() bool {
	return true
}

//////////////////////////////////////////
// ResourceScalingPolicy implementation //
//////////////////////////////////////////

func (p *ReservationPolicy) AutoscalingPolicy() scheduling.AutoscalingPolicy {
	return p
}

func (p *ReservationPolicy) ManualScalingPolicy() scheduling.ManualScalingPolicy {
	return p
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
