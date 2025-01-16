package scheduler

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/placer"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
)

type HostPool struct {
	NumGpus    int32
	Identifier string
	Placer     *placer.LeastLoadedPlacer
}

func GetHostGroupIdentifier(gpus int32) string {
	return fmt.Sprintf("GpuPool[%d]", gpus)
}

func NewHostGroup(gpus int32, numReplicas int, metricsProvider scheduling.MetricsProvider, schedulingPolicy scheduling.Policy) (*HostPool, error) {
	identifier := GetHostGroupIdentifier(gpus)
	leastLoadedPlacer, err := placer.NewLeastLoadedPlacer(metricsProvider, numReplicas, schedulingPolicy, identifier)
	if err != nil {
		return nil, err
	}

	hostGroup := &HostPool{
		NumGpus:    gpus,
		Placer:     leastLoadedPlacer,
		Identifier: identifier,
	}

	return hostGroup, nil
}

type GandivaScheduler struct {
	*DockerScheduler

	hostGroupsInitialized bool
	hostPools             map[int32]*HostPool

	// unpooledHosts contains scheduling.Host instances that have been added to the scheduling.Cluster, but that have
	// not yet been placed into a Host/GPU pool.
	//
	// (In Gandiva, hosts are placed into groups corresponding to the number of GPUs required
	// by jobs that are scheduled on hosts of that pool.)
	unpooledHosts *queue.Fifo[scheduling.Host]

	mu sync.Mutex
}

func NewGandivaScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	hostSpec types.Spec, kernelProvider KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy scheduling.Policy, opts *scheduling.SchedulerOptions) (*GandivaScheduler, error) {

	dockerScheduler, err := newDockerScheduler(cluster, placer, hostMapper, hostSpec, kernelProvider,
		notificationBroker, schedulingPolicy, opts)
	if err != nil {
		return nil, err
	}

	gandivaScheduler := &GandivaScheduler{
		DockerScheduler: dockerScheduler,
		hostPools:       make(map[int32]*HostPool),
		unpooledHosts:   queue.NewFifo[scheduling.Host](8),
	}

	gandivaScheduler.instance = gandivaScheduler
	dockerScheduler.instance = gandivaScheduler
	dockerScheduler.BaseScheduler.instance = gandivaScheduler

	err = gandivaScheduler.initHostPools()
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

func (s *GandivaScheduler) NumUnpooledHosts() int {
	return s.unpooledHosts.Len()
}

func (s *GandivaScheduler) NumHostsInPool(gpus int32) int {
	pool, loaded := s.hostPools[gpus]
	if !loaded {
		return -1
	}

	return pool.Placer.Len()
}

func (s *GandivaScheduler) GetHostPool(gpus int32) *HostPool {
	return s.hostPools[gpus]
}

func (s *GandivaScheduler) Instance() scheduling.Scheduler {
	return s.DockerScheduler.BaseScheduler.instance
}

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *GandivaScheduler) HostAdded(host scheduling.Host) {
	s.log.Debug("Host %s (ID=%s) was added to the Cluster. Adding new host to 'unpooled hosts' queue.",
		host.GetNodeName(), host.GetID(), host.GetNodeName(), host.GetID())

	s.unpooledHosts.Enqueue(host)
}

// logFindCandidateHosts simply logs a message about how many hosts were found during a part of findCandidateHosts.
func (s *GandivaScheduler) logFindCandidateHosts(numHosts int, numGpus int32, hosts []scheduling.Host) {
	// We did not find all the hosts that we need.
	if hosts == nil || len(hosts) == 0 {
		s.log.Debug("Failed to find any candidate hosts from %d-GPU pool. We need %d host(s).", numGpus, numHosts)
	} else {
		s.log.Debug("Found %d/%d candidate hosts from %d-GPU pool.", len(hosts), numHosts, numGpus)
	}
}

// findCandidateHosts is a scheduler-specific implementation for finding candidate hosts for the given kernel.
// GandivaScheduler has several pools of hosts from which it selects candidates.
func (s *GandivaScheduler) findCandidateHosts(numHostsRequired int, kernelSpec *proto.KernelSpec) []scheduling.Host {
	// There should already be a lock around this from the caller -- but just to be safe, we have our own lock here.
	s.mu.Lock()
	defer s.mu.Unlock()

	// If for whatever reason, we were instructed to find zero hosts, then just return immediately.
	if numHostsRequired == 0 {
		s.log.Warn("Instructed to find candidate hosts; however, numHostsRequired=%d...", numHostsRequired)
		return []scheduling.Host{}
	}

	numGpus := kernelSpec.ResourceSpec.Gpu
	pool := s.hostPools[numGpus]

	if pool == nil {
		s.log.Error("No pool found for specified number of GPUs: %d", numGpus)
		return []scheduling.Host{}
	}

	var hosts []scheduling.Host

	// If there is at least one valid host available, then we'll try to see if it is viable.
	if pool.Placer.Len() > 0 {
		// Attempt to find some candidate hosts.
		hosts = pool.Placer.FindHosts(kernelSpec, numHostsRequired)

		// Check if we found all the hosts that we need.
		if hosts != nil && len(hosts) == numHostsRequired {
			s.log.Debug("Successfully identified all %d required host(s) from %d-GPU pool.", numHostsRequired, numGpus)
			return hosts
		}

		s.logFindCandidateHosts(numHostsRequired, numGpus, hosts)
	} else {
		// There were no viable hosts.
		s.log.Debug("Need host from %d-GPU pool; however %d-GPU pool is empty.", numGpus, numGpus)
	}

	// Create the host slice if it has not already been created.
	if hosts == nil {
		hosts = make([]scheduling.Host, 0)
	}

	// Update the number of hosts that we need.
	numHostsRequired -= len(hosts)

	// Sanity check.
	// - numHostsRequired should be non-zero, because if it were zero, then we would've returned up above.
	// - numHostsRequired should not be negative, because that would mean we found more hosts than we need.
	if numHostsRequired <= 0 {
		panic(fmt.Sprintf("Number of required hosts is invalid: %d", numHostsRequired))
	}

	if s.unpooledHosts.Len() == 0 {
		s.log.Debug("There are no unpooled nodes available. Cannot find %d remaining host(s).", numHostsRequired)
		return hosts
	}

	numHostsAddedToPool := s.unsafeUpdatePool(numHostsRequired, numGpus)

	if numHostsAddedToPool < numHostsRequired {
		s.log.Debug("Insufficient unpooled hosts available. Will only be able to find at most %d/%d host(s).",
			numHostsAddedToPool, numHostsRequired)
	}

	hostBatch := pool.Placer.FindHosts(kernelSpec, numHostsRequired)
	hosts = append(hosts, hostBatch...)

	s.log.Debug("Found %d host(s) after adding %d host(s) to %d-GPU pool. Found total of %d/%d host(s).",
		len(hostBatch), numHostsAddedToPool, numGpus, len(hosts), numHostsRequired)

	return hosts
}

// unsafeUpdatePool attempts to add up to 'numHostsRequired' scheduling.Host instances from
// the unpooledHosts to the HostPool for the specified number of GPUs, 'numGPUs'.
//
// unsafeUpdatePool returns the number of hosts that were added to the specified HostPool.
func (s *GandivaScheduler) unsafeUpdatePool(numHostsRequired int, numGpus int32) int {
	// If we get to this point, then we did not find all the hosts that we need.
	// Let's first see if we have any "free" hosts that we can allocate to the pool.
	numHostsAddedToPool := 0

	pool := s.hostPools[numGpus]

	// As long as we've not yet added enough new hosts to satisfy the request, and there are still unpooled hosts
	// that we can add to the pool, continue adding unpooled hosts to the pool.
	for numHostsAddedToPool < numHostsRequired && s.unpooledHosts.Len() > 0 {
		unpooledHost, ok := s.unpooledHosts.Dequeue()
		if !ok {
			s.log.Error("Expected to have at least one more unpooled host; however, there are none left...")
			break
		}

		pool.Placer.GetIndex().Add(unpooledHost)
		numHostsAddedToPool += 1
	}

	s.log.Debug("Added %d/%d unpooled host(s) to the %d-GPU pool. Remaining unpooled hosts: %d.",
		numHostsAddedToPool, numHostsRequired, numGpus, s.unpooledHosts.Len())

	return numHostsAddedToPool
}

// initHostPools initializes the hostPools map of the target GandivaScheduler,
// creating a HostPool for 1, 2, 3, ..., 'GpusPerHost' GPUs.
func (s *GandivaScheduler) initHostPools() error {
	if s.hostGroupsInitialized {
		return nil
	}

	for gpus := 0; gpus <= s.opts.GpusPerHost; gpus++ {
		hostGroup, err := NewHostGroup(int32(gpus), s.schedulingPolicy.NumReplicas(), s.cluster.MetricsProvider(), s.schedulingPolicy)
		if err != nil {
			return err
		}

		s.hostPools[int32(gpus)] = hostGroup

		err = s.cluster.AddIndex(hostGroup.Placer.GetIndex())
		if err != nil {
			s.log.Error("Failed to add index for %d-GPU pool: %v", gpus, err)
			panic(err)
		}
	}

	return nil
}
