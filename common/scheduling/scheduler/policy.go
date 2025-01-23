package scheduler

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
)

// SchedulingPolicy defines an interface used more-or-less exclusively by the scheduler package.
//
// Specifically, all scheduling.Policy implementations will need to implement these, but they're only
// intended to be used internally within the scheduling package.
//
// That is, they should only be invoked by going through the versions of these methods exposed on the
// scheduling.Scheduler interface, as those guarantee atomicity.
type SchedulingPolicy interface {
	scheduling.Policy

	// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
	SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error)

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
	FindReadyReplica(kernel scheduling.Kernel) (scheduling.KernelReplica, error)
}

// GetSchedulingPolicy returns the appropriate scheduling policy based on the provided configuration.
//
// If it is unclear which scheduling.Policy should be returned, then a scheduling.ErrInvalidSchedulingPolicy will be
// returned.
func GetSchedulingPolicy(opts *scheduling.SchedulerOptions) (SchedulingPolicy, error) {
	if opts.SchedulingPolicy == "" {
		return nil, fmt.Errorf("%w: unspecified (you did not specify one)", scheduling.ErrInvalidSchedulingPolicy)
	}

	switch opts.SchedulingPolicy {
	case string(scheduling.Reservation):
		{
			return policy.NewReservationPolicy(opts)
		}
	case string(scheduling.FcfsBatch):
		{
			return policy.NewFcfsBatchSchedulingPolicy(opts)
		}
	case string(scheduling.AutoScalingFcfsBatch):
		{
			return policy.NewAutoScalingFcfsBatchSchedulingPolicy(opts)
		}
	case string(scheduling.Static):
		{
			return policy.NewStaticPolicy(opts)
		}
	case string(scheduling.DynamicV3):
		{
			return policy.NewDynamicV3Policy(opts)
		}
	case string(scheduling.DynamicV4):
		{
			return policy.NewDynamicV4Policy(opts)
		}
	case string(scheduling.Gandiva):
		{
			return policy.NewGandivaPolicy(opts)
		}
	}
	return nil, fmt.Errorf("%w: \"%s\"", scheduling.ErrInvalidSchedulingPolicy, opts.SchedulingPolicy)
}
