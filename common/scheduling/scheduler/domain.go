package scheduler

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type clusterSchedulerInternal interface {
	scheduling.Scheduler

	getHost(hostId string) (scheduling.Host, bool)

	// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
	addReplicaSetup(kernelId string, addReplicaOp *scheduling.AddReplicaOperation)

	// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
	postScheduleKernelReplica(kernelId string, addReplicaOp *scheduling.AddReplicaOperation)

	// selectViableHostForReplica identifies a viable scheduling.Host to serve the given scheduling.KernelContainer.
	//
	// selectViableHostForReplica is most often called for kernels that need to begin training immediately.
	//
	// Important: selectViableHostForReplica will reserve resources on the Host.
	selectViableHostForReplica(replicaSpec *proto.KernelReplicaSpec, blacklistedHosts []scheduling.Host, forTraining bool) (scheduling.Host, error)

	// findViableHostForReplica is called at scheduling-time (rather than before we get to the point of scheduling, such
	// as searching for viable hosts before trying to schedule the container).
	//
	// PRECONDITION: If we're finding a viable host for an existing replica, then the blacklisted hosts argument should
	// be non-nil and should contain the replica's current/original host (in order to prevent the replica from being
	// scheduled back onto the same host).
	//
	// This method searches for a viable training host and, if one is found, then that host is returned.
	// Otherwise, an error is returned.
	//
	// If we fail to find a host, then we'll try to scale-out (if we're allowed).
	findViableHostForReplica(replicaSpec scheduling.KernelReplica, blacklistedHosts []scheduling.Host, forTraining bool,
		createNewHostPermitted bool) (host scheduling.Host, failureReason error)
}
