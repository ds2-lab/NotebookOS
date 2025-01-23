package cluster

import "github.com/scusemua/distributed-notebook/common/scheduling"

// internalSchedulingPolicy defines an interface used more-or-less exclusively by the scheduler package.
//
// Specifically, all scheduling.Policy implementations will need to implement these, but they're only
// intended to be used internally within the scheduling package.
//
// That is, they should only be invoked by going through the versions of these methods exposed on the
// scheduling.Scheduler interface, as those guarantee atomicity.
type internalSchedulingPolicy interface {
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
	FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error)
}
