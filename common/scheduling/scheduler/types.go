package scheduler

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type clusterSchedulerInternal interface {
	scheduling.Scheduler

	// findCandidateHosts is a scheduler-specific implementation for finding candidate hosts for the given kernel.
	//
	// If findCandidateHosts returns nil, rather than an empty slice, then that indicates that an error occurred.
	findCandidateHosts(numToFind int, kernelSpec *proto.KernelSpec) ([]scheduling.Host, error)

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
}
