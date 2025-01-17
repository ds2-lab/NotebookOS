package index

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"sync"
)

const (
	expectedMultiIndexValue string = "*"
)

type HostPool[T scheduling.ClusterIndex] struct {
	Pool       T
	NumGPUs    int32
	Identifier string
}

func NewHostPool[T scheduling.ClusterIndex](pool T, gpus int32) *HostPool[T] {
	return &HostPool[T]{
		Pool:       pool,
		NumGPUs:    gpus,
		Identifier: fmt.Sprintf("%d-GPU Pool", gpus),
	}
}

// SeekMultipleFrom simply forwards the call to the target HostPool's Pool field (which is a scheduling.ClusterIndex).
func (p *HostPool[T]) SeekMultipleFrom(pos interface{}, numHosts int, criteria scheduling.HostCriteriaFunction,
	blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {
	return p.Pool.SeekMultipleFrom(pos, numHosts, criteria, blacklist, metrics...)
}

// Len returns the number of scheduling.Host instances in the target HostPool.
//
// Len is equivalent to Size.
func (p *HostPool[T]) Len() int {
	return p.Pool.Len()
}

// Size returns the number of scheduling.Host instances in the target HostPool.
//
// Size is equivalent to Len.
func (p *HostPool[T]) Size() int {
	return p.Pool.Len()
}

// AddHost adds a scheduling.Host to the target HostPool.
//
// AddHost simply forwards the call directly to the target AddHost's Pool field (which is a scheduling.ClusterIndex).
func (p *HostPool[T]) AddHost(host scheduling.Host) {
	p.Pool.Add(host)
}

// MultiIndexProvider provides the individual indices used by a MultiIndex.
type MultiIndexProvider[T scheduling.ClusterIndex] func() T

// MultiIndex manages a collection of sub-indices organized by number of GPUs.
//
// The type parameter is the concrete type of the "sub-indices" or the "host pools" managed by the MultiIndex.
// For example, LeastLoadedIndex, StaticClusterIndex, RandomClusterIndex, etc.
type MultiIndex[T scheduling.ClusterIndex] struct {
	// FreeHosts are scheduling.Host instances that have not been placed into a particular HostPool yet.
	FreeHosts *queue.Fifo[scheduling.Host]

	// FreeHostsMap is used to keep track of the scheduling.Host instances that are in the FreeHosts queue.
	FreeHostsMap map[string]scheduling.Host

	// HostPools is a map from GPUs to a scheduling.ClusterIndex containing scheduling.Host instances
	// that serve sessions/kernels that require that number of GPUs.
	HostPools map[int32]*HostPool[T]

	// IndexProvider provides the individual indices used by a MultiIndex.
	IndexProvider MultiIndexProvider[T]

	// HostGroupsInitialized indicates whether the host pools have been initialized.
	HostGroupsInitialized bool

	// HostIdToHostPool is a mapping from Host ID to the Host Pool in which the Host is contained.
	HostIdToHostPool map[string]*HostPool[T]

	// Size encodes the total number of scheduling.Host instances contained within this MutliIndex.
	// Size includes scheduling.Host instances in FreeHosts as well as those in the HostPools.
	Size int

	log logger.Logger
	mu  sync.Mutex
}

// NewMultiIndex creates and returns a new MultiIndex.
//
// The type parameter is the concrete type of the "sub-indices" or the "host pools" managed by the MultiIndex.
//
// The MultiIndexProvider is a function that returns concrete instantiations of the type parameter.
// It will typically just be the "constructor" (i.e., the NewX function, such as NewLeastLoadedIndex).
//
// If the constructor accepts parameters, then a closure of the constructor could be passed, assuming the
// values of those parameters can accetably remain the same for the program's execution.
func NewMultiIndex[T scheduling.ClusterIndex](maxGpus int32, provider MultiIndexProvider[T]) (*MultiIndex[T], error) {
	index := &MultiIndex[T]{
		FreeHosts:        queue.NewFifo[scheduling.Host](16),
		FreeHostsMap:     make(map[string]scheduling.Host),
		HostPools:        make(map[int32]*HostPool[T]),
		HostIdToHostPool: make(map[string]*HostPool[T]),
		IndexProvider:    provider,
		Size:             0,
	}

	config.InitLogger(&index.log, fmt.Sprintf("MultiIndex[%d] ", maxGpus))

	err := index.initializeHostPools(maxGpus, provider)
	if err != nil {
		index.log.Error("Failed to initialize host pools: %v", err)
		return nil, err
	}

	return index, nil
}

// initializeHostPools creates all the HostPool instances to be managed by the target MultiIndex.
//
// It uses the given MultiIndexProvider to create each of the "sub-indices"/HostPool instances.
func (index *MultiIndex[T]) initializeHostPools(maxGPUs int32, indexProvider MultiIndexProvider[T]) error {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.log.Debug("Initializing %d host pools now.", maxGPUs+1)

	if index.HostGroupsInitialized {
		return nil
	}

	var gpus int32
	for gpus = 0; gpus <= maxGPUs; gpus++ {
		index.HostPools[gpus] = NewHostPool(indexProvider(), gpus)
		index.log.Debug("Initialized HostPool #%d: %d-GPU pool.", len(index.HostPools), gpus)
	}

	return nil
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiIndex.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (index *MultiIndex[T]) NumFreeHosts() int {
	index.mu.Lock()
	defer index.mu.Unlock()

	return index.FreeHosts.Len()
}

// NumHostsInPool returns the number of hosts in the specified host pool.
func (index *MultiIndex[T]) NumHostsInPool(gpus int32) int {
	pool, loaded := index.HostPools[gpus]
	if !loaded {
		index.log.Warn("Size of %d-GPU pool requested; however, no such pool exists...", gpus)

		return -1
	}

	return pool.Len()
}

// GetHostPool returns the HostPool for the specified number of GPUs.
//
// The HostPool will be the one responsible for containing Hosts that serve sessions/kernels/jobs
// requiring `gpus` number of GPUs.
func (index *MultiIndex[T]) GetHostPool(gpus int32) (*HostPool[T], bool) {
	pool, loaded := index.HostPools[gpus]
	if loaded {
		return pool, true
	}

	return nil, false
}

func (index *MultiIndex[T]) Len() int {
	index.mu.Lock()
	defer index.mu.Unlock()

	return index.Size
}

func (index *MultiIndex[T]) Add(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.FreeHosts.Enqueue(host)
	index.FreeHostsMap[host.GetID()] = host

	index.Size += 1
}

func (index *MultiIndex[T]) Update(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.unsafeUpdate(host)
}

func (index *MultiIndex[T]) unsafeUpdate(host scheduling.Host) {
	hostPool, loaded := index.HostIdToHostPool[host.GetID()]
	if !loaded {
		index.log.Warn("Could not load GPU pool for host %s (ID=%s). Cannot update host.",
			host.GetNodeName(), host.GetID())
		return
	}

	hostPool.Pool.Update(host)
}

func (index *MultiIndex[T]) UpdateMultiple(hosts []scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	for _, host := range hosts {
		index.unsafeUpdate(host)
	}
}

func (index *MultiIndex[T]) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	if _, loaded := index.FreeHostsMap[host.GetID()]; loaded {
		_, removed := index.FreeHosts.Remove(host, func(h1 scheduling.Host, h2 scheduling.Host) bool {
			return h1 == h2 || h1.GetID() == h2.GetID()
		})

		// We want to remove the entry from the FreeHostsMap either way.
		delete(index.FreeHostsMap, host.GetID())

		// If the host was found and removed from the unpooled hosts queue, then we're done.
		if removed {
			index.Size -= 1
			return
		}

		index.log.Error("Expected Host %s (ID=%s) to be in the FreeHosts queue; "+
			"however, the host was not found when we tried to remove it...",
			host.GetNodeName(), host.GetID())
	}

	hostPool, loaded := index.HostIdToHostPool[host.GetID()]
	if !loaded {
		index.log.Warn("Could not load GPU pool for host %s (ID=%s). Cannot remove host.",
			host.GetNodeName(), host.GetID())
		return
	}

	hostPool.Pool.Remove(host)
	index.Size -= 1
}

func (index *MultiIndex[T]) GetMetrics(host scheduling.Host) []float64 {
	index.mu.Lock()
	defer index.mu.Unlock()

	hostPool, loaded := index.HostIdToHostPool[host.GetID()]
	if !loaded {
		index.log.Warn("Could not load GPU pool for host %s (ID=%s). Cannot get metrics for host.",
			host.GetNodeName(), host.GetID())
		return []float64{}
	}

	return hostPool.Pool.GetMetrics(host)
}

func (index *MultiIndex[T]) Category() (string, interface{}) {
	return scheduling.CategoryGandivaPoolIndex, "*"
}

func (index *MultiIndex[T]) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	_, hostIsFree := index.FreeHostsMap[host.GetID()]
	if hostIsFree {
		// The host is already present in the index.
		// It has not yet been assigned to a particular host pool.
		return expectedMultiIndexValue, scheduling.IndexQualified
	}

	_, hostIsInPool := index.HostIdToHostPool[host.GetID()]
	if hostIsInPool {
		// The host is already present in the index and has already been assigned to a particular host pool.
		return expectedMultiIndexValue, scheduling.IndexQualified
	}

	return expectedMultiIndexValue, scheduling.IndexUnqualified
}

// seekCriteria is used by the Seek method.
//
// Seek calls SeekMultipleFrom, passing 1 as the number of hosts. SeekMultipleFrom requires a "criteria"
// function, whereas Seek doesn't. Thus, Seek passes seekCriteria to SeekMultipleFrom, and seekCriteria
// always returns true.
func (index *MultiIndex[T]) seekCriteria(_ scheduling.Host) bool {
	return true
}

func (index *MultiIndex[T]) Seek(blacklist []interface{}, metrics ...[]float64) (scheduling.Host, interface{}) {
	if len(metrics) == 0 {
		index.log.Warn("No metrics received in call to SeekMultipleFrom for Multi-Index...")
		return nil, nil
	}

	index.log.Debug("Seeking single host with metrics: %v", metrics)

	hosts, _ := index.SeekMultipleFrom(nil, 1, index.seekCriteria, blacklist, metrics...)

	if hosts == nil || len(hosts) == 0 {
		return nil, nil
	}

	return hosts[0], nil
}

// logSeekMultiple simply logs a message about how many hosts were found during a part of findCandidateHosts.
func (index *MultiIndex[T]) logSeekMultiple(numHosts int, numGpus int32, hosts []scheduling.Host) {
	// We did not find all the hosts that we need.
	if hosts == nil || len(hosts) == 0 {
		index.log.Debug("Failed to find any candidate hosts from %d-GPU pool. We need %d host(s).", numGpus, numHosts)
	} else {
		index.log.Debug("Found %d/%d candidate hosts from %d-GPU pool.", len(hosts), numHosts, numGpus)
	}
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *MultiIndex[T]) SeekMultipleFrom(pos interface{}, numHosts int, criteria scheduling.HostCriteriaFunction,
	blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {

	index.mu.Lock()
	defer index.mu.Unlock()

	// If for whatever reason, we were instructed to find zero hosts, then just return immediately.
	if numHosts == 0 {
		index.log.Warn("Instructed to find candidate hosts; however, NumHosts=%d...", numHosts)
		return []scheduling.Host{}, nil
	}

	if len(metrics) == 0 {
		panic("No metrics received in call to SeekMultipleFrom for MutliIndex...")
	}

	numGpus := int32(metrics[0][0])
	pool := index.HostPools[numGpus]
	if pool == nil {
		index.log.Error("No pool found for specified number of GPUs: %d", numGpus)
		return []scheduling.Host{}, nil
	}

	// If there is at least one valid host available, then we'll try to see if it is viable.
	var hosts []scheduling.Host
	if pool.Len() > 0 {
		// Attempt to find some candidate hosts.
		hosts, _ = pool.SeekMultipleFrom(pos, numHosts, criteria, blacklist)

		// Check if we found all the hosts that we need.
		if hosts != nil && len(hosts) == numHosts {
			index.log.Debug("Successfully identified all %d required host(s) from %d-GPU pool.", numHosts, numGpus)
			return hosts, nil
		}

		index.logSeekMultiple(numHosts, numGpus, hosts)
	} else {
		// There were no viable hosts.
		index.log.Debug("Need host from %d-GPU pool; however %d-GPU pool is empty.", numGpus, numGpus)
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

	if index.FreeHosts.Len() == 0 {
		index.log.Debug("There are no unpooled nodes available. Cannot find %d remaining host(s).", numHosts)
		return hosts, nil
	}

	numHostsAddedToPool := index.unsafeUpdatePool(numHosts, numGpus)

	if numHostsAddedToPool < numHosts {
		index.log.Debug("Insufficient unpooled hosts available. Will only be able to find at most %d/%d host(s).",
			numHostsAddedToPool, numHosts)
	}

	hostBatch, _ := pool.SeekMultipleFrom(pos, numHosts, criteria, blacklist)
	hosts = append(hosts, hostBatch...)

	index.log.Debug("Found %d host(s) after adding %d host(s) to %d-GPU pool. Found total of %d/%d host(s).",
		len(hostBatch), numHostsAddedToPool, numGpus, len(hosts), numHosts)

	return hosts, nil
}

// unsafeUpdatePool attempts to add up to 'numHosts' scheduling.Host instances from
// the unpooledHosts to the HostPool for the specified number of GPUs, 'numGPUs'.
//
// unsafeUpdatePool returns the number of hosts that were added to the specified HostPool.
func (index *MultiIndex[T]) unsafeUpdatePool(numHosts int, numGpus int32) int {
	// If we get to this point, then we did not find all the hosts that we need.
	// Let's first see if we have any "free" hosts that we can allocate to the pool.
	numHostsAddedToPool := 0

	pool := index.HostPools[numGpus]

	// As long as we've not yet added enough new hosts to satisfy the request, and there are still unpooled hosts
	// that we can add to the pool, continue adding unpooled hosts to the pool.
	for numHostsAddedToPool < numHosts && index.FreeHosts.Len() > 0 {
		freeHost, ok := index.FreeHosts.Dequeue()
		if !ok {
			index.log.Error("Expected to have at least one more free host; however, there are none left...")
			break
		}

		// Add the host to the pool.
		pool.AddHost(freeHost)

		// Update mappings.
		index.HostIdToHostPool[freeHost.GetID()] = pool
		delete(index.FreeHostsMap, freeHost.GetID())

		numHostsAddedToPool += 1
	}

	index.log.Debug("Added %d/%d unpooled host(s) to the %d-GPU pool. Remaining free hosts: %d.",
		numHostsAddedToPool, numHosts, numGpus, index.FreeHosts.Len())

	return numHostsAddedToPool
}
