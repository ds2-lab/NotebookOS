package scheduler

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
)

func GetDockerComposeScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	hostSpec types.Spec, kernelProvider KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy scheduling.Policy, opts *scheduling.SchedulerOptions) scheduling.Scheduler {

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

func GetDockerSwarmScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	hostSpec types.Spec, kernelProvider KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy scheduling.Policy, opts *scheduling.SchedulerOptions) scheduling.Scheduler {

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
