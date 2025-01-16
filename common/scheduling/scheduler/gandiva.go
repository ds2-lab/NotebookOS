package scheduler

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/types"
)

type HostGroup struct {
	NumGpus int
	Placer  *placer.LeastLoadedPlacer
}

func NewHostGroup(gpus int, numReplicas int, metricsProvider scheduling.MetricsProvider, schedulingPolicy scheduling.Policy) (*HostGroup, error) {
	placer, err := placer.NewLeastLoadedPlacer(metricsProvider, numReplicas, schedulingPolicy)
	if err != nil {
		return nil, err
	}

	hostGroup := &HostGroup{
		NumGpus: gpus,
		Placer:  placer,
	}

	return hostGroup, nil
}

type GandivaScheduler struct {
	*DockerScheduler

	hostGroupsInitialized bool
	hostGroups            map[int]*HostGroup
}

func NewGandivaScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper, hostSpec types.Spec,
	kernelProvider KernelProvider, notificationBroker NotificationBroker, schedulingPolicy scheduling.Policy,
	opts *scheduling.SchedulerOptions) (*GandivaScheduler, error) {

	baseScheduler, err := NewDockerScheduler(cluster, placer, hostMapper, hostSpec, kernelProvider, notificationBroker, schedulingPolicy, opts)
	if err != nil {
		return nil, err
	}

	gandivaScheduler := &GandivaScheduler{
		DockerScheduler: baseScheduler,
		hostGroups:      make(map[int]*HostGroup),
	}

	gandivaScheduler.DockerScheduler.instance = gandivaScheduler

	err = gandivaScheduler.initHostGroups()
	if err != nil {
		gandivaScheduler.log.Error("Failed to initialize Host Groups: %v", err)
		return nil, err
	}

	err = gandivaScheduler.refreshClusterNodes()
	if err != nil {
		gandivaScheduler.log.Error("Initial retrieval of Docker nodes failed: %v", err)
	}

	return gandivaScheduler, nil
}

// initHostGroups initializes the hostGroups map of the target GandivaScheduler,
// creating a HostGroup for 1, 2, 4, and 8 GPUs.
func (s *GandivaScheduler) initHostGroups() error {
	if s.hostGroupsInitialized {
		return nil
	}

	gpus := 1
	for gpus <= 8 {
		hostGroup, err := NewHostGroup(gpus, s.schedulingPolicy.NumReplicas(), s.cluster.MetricsProvider(), s.schedulingPolicy)
		if err != nil {
			return err
		}

		s.hostGroups[gpus] = hostGroup
		gpus = gpus * 2
	}

	return nil
}
