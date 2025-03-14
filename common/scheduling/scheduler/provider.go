package scheduler

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
)

func GetDockerScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper scheduling.HostMapper,
	hostSpec types.Spec, kernelProvider scheduling.KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy SchedulingPolicy, opts *scheduling.SchedulerOptions) scheduling.Scheduler {

	var (
		clusterScheduler scheduling.Scheduler
		err              error
	)

	clusterScheduler, err = NewDockerScheduler(cluster, placer, hostMapper, hostSpec,
		kernelProvider, notificationBroker, schedulingPolicy, opts)
	if err != nil {
		fmt.Printf("[ERROR] Failed to create Docker Compose Scheduler: %v", err)
		panic(err)
	}

	return clusterScheduler
}
