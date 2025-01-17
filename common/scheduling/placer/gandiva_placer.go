package placer

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
)

type HostPool struct {
	NumGpus int32
	Placer  *BasicPlacer
}

func NewHostGroup(gpus int32, numReplicas int, metricsProvider scheduling.MetricsProvider, schedulingPolicy scheduling.Policy) (*HostPool, error) {
	basicPlacer, err := NewBasicPlacer(metricsProvider, numReplicas, schedulingPolicy)
	if err != nil {
		return nil, err
	}

	hostGroup := &HostPool{
		NumGpus: gpus,
		Placer:  basicPlacer,
	}

	return hostGroup, nil
}

// GandivaPlacer manages a collection of underlying Gandiva placers.
type GandivaPlacer struct {
	*AbstractPlacer

	// allHosts is a LeastLoadedIndex that contains all the hosts managed by this GandivaPlacer.
	allHosts *index.LeastLoadedIndex

	// hostGroupsInitialized indicates whether the host pools have been initialized.
	hostGroupsInitialized bool

	// hostPools maps GPUs to the associated HostPool.
	hostPools map[int32]*HostPool
	// hostPoolsStr contains the same mapping as hostPools, except the keys are converted to strings, as that's
	// how the metadata is stored on hosts. (The metadata stored on hosts identifies which GPU pool the host is in.)
	hostPoolsStr map[string]*HostPool

	// unpooledHosts contains scheduling.Host instances that have been added to the scheduling.Cluster, but that have
	// not yet been placed into a Host/GPU pool.
	//
	// (In Gandiva, hosts are placed into groups corresponding to the number of GPUs required
	// by jobs that are scheduled on hosts of that pool.)
	unpooledHosts *queue.Fifo[scheduling.Host]

	// totalGpusPerHost is the number of GPUs that are available for use on each Host within the cluster.
	totalGpusPerHost int
}

// NewGandivaPlacer creates a new GandivaPlacer.
func NewGandivaPlacer(metrics scheduling.MetricsProvider, numReplicas int, policy scheduling.Policy, gpusPerHost int) (*GandivaPlacer, error) {
	basePlacer := NewAbstractPlacer(metrics, numReplicas, policy)
	groupedGandivaPlacer := &GandivaPlacer{
		AbstractPlacer:   basePlacer,
		totalGpusPerHost: gpusPerHost,
		hostPools:        make(map[int32]*HostPool),
		hostPoolsStr:     make(map[string]*HostPool),
		allHosts:         index.NewLeastLoadedIndex(),
		unpooledHosts:    queue.NewFifo[scheduling.Host](16),
	}

	basePlacer.instance = groupedGandivaPlacer

	err := groupedGandivaPlacer.initHostPools()
	if err != nil {
		groupedGandivaPlacer.log.Error("Failed to initialize host pools: %v", err)
		return nil, err
	}

	return groupedGandivaPlacer, nil
}

// Len returns the number of scheduling.Host instances in the underlying least-loaded index.
// Len is equivalent to Size.
func (placer *GandivaPlacer) Len() int {
	return placer.allHosts.Len()
}

// Size returns the number of scheduling.Host instances in the underlying least-loaded index.
// Size is equivalent to Len.
func (placer *GandivaPlacer) Size() int {
	return placer.allHosts.Len()
}

func (placer *GandivaPlacer) NumUnpooledHosts() int {
	return placer.unpooledHosts.Len()
}

func (placer *GandivaPlacer) NumHostsInPool(gpus int32) int {
	pool, loaded := placer.hostPools[gpus]
	if !loaded {
		return -1
	}

	return pool.Placer.Len()
}

func (placer *GandivaPlacer) GetHostPool(gpus int32) *HostPool {
	return placer.hostPools[gpus]
}

func (placer *GandivaPlacer) UpdateIndex(host scheduling.Host) {
	// First, figure out which index the host is contained within.
	keyMetadata := host.GetMeta(scheduling.HostIndexKeyMetadata)
	if keyMetadata == nil {
		placer.log.Warn("Host %s (ID=%s) does not have a HostIndexKeyMetadata ('%s') metadata entry",
			host.GetNodeName(), host.GetID(), scheduling.HostIndexKeyMetadata)
		return
	}

	pool, loaded := placer.hostPoolsStr[keyMetadata.(string)]
	if !loaded {
		placer.log.Warn("Could not load GPU pool from key \"%s\" for host %s (ID=%s). Cannot update host.",
			keyMetadata, host.GetNodeName(), host.GetID())
		return
	}

	pool.Placer.GetIndex().Update(host)
}

func (placer *GandivaPlacer) UpdateIndexMultiple(hosts []scheduling.Host) {
	for _, host := range hosts {
		placer.UpdateIndex(host)
	}
}

// GetIndex returns the index containing all hosts managed by the GandivaPlacer.
func (placer *GandivaPlacer) GetIndex() scheduling.ClusterIndex {
	return placer.allHosts
}

// GetGpuPool returns the host pool for the specified number of GPUs.
func (placer *GandivaPlacer) GetGpuPool(gpus int32) scheduling.ClusterIndex {
	pool := placer.hostPools[gpus]
	if pool != nil {
		return pool.Placer.GetIndex()
	}

	return nil
}

// NumHostsInIndex returns the length of the GandivaPlacer's index.
func (placer *GandivaPlacer) NumHostsInIndex() int {
	return placer.Len()
}

// tryReserveResourcesOnHost attempts to reserve resources for a future replica of the specified kernel
// on the specified host, returning true if the reservation was created successfully.
//
// Returns: true if the reservation was created successfully.
func (placer *GandivaPlacer) tryReserveResourcesOnHost(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool {
	var usePendingReservation bool

	if forTraining {
		// If we are migrating a replica that needs to begin training right away, then we should not use a pending reservation.
		// The container will need resources committed to it immediately.
		usePendingReservation = false
	} else {
		usePendingReservation = placer.reservationShouldUsePendingResources()
	}

	reserved, err := candidateHost.ReserveResources(kernelSpec, usePendingReservation)
	if err != nil {
		placer.log.Error("Error while attempting to reserve resources for replica of kernel %s on host %s (ID=%s): %v",
			kernelSpec.Id, candidateHost.GetNodeName(), candidateHost.GetID(), err)

		// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
		if reserved {
			panic("We successfully reserved resources on a Host despite ReserveResources also returning an error...")
		}
	}

	return reserved
}

// logFindHosts simply logs a message about how many hosts were found during a part of findCandidateHosts.
func (placer *GandivaPlacer) logFindHosts(numHosts int, numGpus int32, hosts []scheduling.Host) {
	// We did not find all the hosts that we need.
	if hosts == nil || len(hosts) == 0 {
		placer.log.Debug("Failed to find any candidate hosts from %d-GPU pool. We need %d host(s).", numGpus, numHosts)
	} else {
		placer.log.Debug("Found %d/%d candidate hosts from %d-GPU pool.", len(hosts), numHosts, numGpus)
	}
}

// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
// on each Host until either the requested number of Host instances has been found, or until all Host
// instances have been checked.
func (placer *GandivaPlacer) findHosts(blacklist []interface{}, kernelSpec *proto.KernelSpec, numHosts int, forTraining bool) []scheduling.Host {
	// If for whatever reason, we were instructed to find zero hosts, then just return immediately.
	if numHosts == 0 {
		placer.log.Warn("Instructed to find candidate hosts; however, numHosts=%d...", numHosts)
		return []scheduling.Host{}
	}

	numGpus := kernelSpec.ResourceSpec.Gpu
	pool := placer.hostPools[numGpus]

	if pool == nil {
		placer.log.Error("No pool found for specified number of GPUs: %d", numGpus)
		return []scheduling.Host{}
	}

	var hosts []scheduling.Host

	// If there is at least one valid host available, then we'll try to see if it is viable.
	if pool.Placer.Len() > 0 {
		// Attempt to find some candidate hosts.
		hosts = pool.Placer.FindHosts(blacklist, kernelSpec, numHosts, forTraining)

		// Check if we found all the hosts that we need.
		if hosts != nil && len(hosts) == numHosts {
			placer.log.Debug("Successfully identified all %d required host(s) from %d-GPU pool.", numHosts, numGpus)
			return hosts
		}

		placer.logFindHosts(numHosts, numGpus, hosts)
	} else {
		// There were no viable hosts.
		placer.log.Debug("Need host from %d-GPU pool; however %d-GPU pool is empty.", numGpus, numGpus)
	}

	// Create the host slice if it has not already been created.
	if hosts == nil {
		hosts = make([]scheduling.Host, 0)
	}

	// Update the number of hosts that we need.
	numHosts -= len(hosts)

	// Sanity check.
	// - numHosts should be non-zero, because if it were zero, then we would've returned up above.
	// - numHosts should not be negative, because that would mean we found more hosts than we need.
	if numHosts <= 0 {
		panic(fmt.Sprintf("Number of required hosts is invalid: %d", numHosts))
	}

	if placer.unpooledHosts.Len() == 0 {
		placer.log.Debug("There are no unpooled nodes available. Cannot find %d remaining host(s).", numHosts)
		return hosts
	}

	numHostsAddedToPool := placer.unsafeUpdatePool(numHosts, numGpus)

	if numHostsAddedToPool < numHosts {
		placer.log.Debug("Insufficient unpooled hosts available. Will only be able to find at most %d/%d host(s).",
			numHostsAddedToPool, numHosts)
	}

	hostBatch := pool.Placer.FindHosts([]interface{}{}, kernelSpec, numHosts, false)
	hosts = append(hosts, hostBatch...)

	placer.log.Debug("Found %d host(s) after adding %d host(s) to %d-GPU pool. Found total of %d/%d host(s).",
		len(hostBatch), numHostsAddedToPool, numGpus, len(hosts), numHosts)

	return hosts
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *GandivaPlacer) findHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) scheduling.Host {
	hosts := placer.findHosts(blacklist, kernelSpec, 1, forTraining)

	if hosts == nil || len(hosts) == 0 {
		return nil
	}

	return hosts[0]
}

// initHostPools initializes the hostPools map of the target GandivaScheduler,
// creating a HostPool for 1, 2, 3, ..., 'GpusPerHost' GPUs.
func (placer *GandivaPlacer) initHostPools() error {
	if placer.hostGroupsInitialized {
		return nil
	}

	for gpus := 0; gpus <= placer.totalGpusPerHost; gpus++ {
		hostGroup, err := NewHostGroup(int32(gpus), placer.schedulingPolicy.NumReplicas(),
			placer.metricsProvider, placer.schedulingPolicy)

		if err != nil {
			return err
		}

		placer.hostPools[int32(gpus)] = hostGroup
		placer.hostPoolsStr[fmt.Sprintf("%d-GPU Pool", gpus)] = hostGroup

		placer.log.Debug("Initialized %d-GPU pool.", gpus)
	}

	return nil
}

// unsafeUpdatePool attempts to add up to 'numHosts' scheduling.Host instances from
// the unpooledHosts to the HostPool for the specified number of GPUs, 'numGPUs'.
//
// unsafeUpdatePool returns the number of hosts that were added to the specified HostPool.
func (placer *GandivaPlacer) unsafeUpdatePool(numHosts int, numGpus int32) int {
	// If we get to this point, then we did not find all the hosts that we need.
	// Let's first see if we have any "free" hosts that we can allocate to the pool.
	numHostsAddedToPool := 0

	pool := placer.hostPools[numGpus]

	// As long as we've not yet added enough new hosts to satisfy the request, and there are still unpooled hosts
	// that we can add to the pool, continue adding unpooled hosts to the pool.
	for numHostsAddedToPool < numHosts && placer.unpooledHosts.Len() > 0 {
		unpooledHost, ok := placer.unpooledHosts.Dequeue()
		if !ok {
			placer.log.Error("Expected to have at least one more unpooled host; however, there are none left...")
			break
		}

		pool.Placer.GetIndex().Add(unpooledHost)
		numHostsAddedToPool += 1
	}

	placer.log.Debug("Added %d/%d unpooled host(s) to the %d-GPU pool. Remaining unpooled hosts: %d.",
		numHostsAddedToPool, numHosts, numGpus, placer.unpooledHosts.Len())

	return numHostsAddedToPool
}
