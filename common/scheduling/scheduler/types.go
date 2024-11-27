package scheduler

import "github.com/scusemua/distributed-notebook/common/scheduling"

type clusterSchedulerInternal interface {
	scheduling.Scheduler

	// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
	addReplicaSetup(kernelId string, addReplicaOp *scheduling.AddReplicaOperation)

	// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
	postScheduleKernelReplica(kernelId string, addReplicaOp *scheduling.AddReplicaOperation)
}
