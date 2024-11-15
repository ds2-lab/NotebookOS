package scheduler

import "github.com/zhangjyr/distributed-notebook/common/scheduling"

type clusterSchedulerInternal interface {
	scheduling.Scheduler

	// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
	addReplicaSetup(kernelId string, addReplicaOp *AddReplicaOperation)

	// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
	postScheduleKernelReplica(kernelId string, addReplicaOp *AddReplicaOperation)
}

type clusterInternal interface {
	scheduling.Cluster
}
