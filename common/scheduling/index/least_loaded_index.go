package index

import (
	"container/heap"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"slices"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
)

const (
	expectedLeastLoadedIndex                        = "*"
	HostMetaLeastLoadedIndex scheduling.HostMetaKey = "least_loaded_index"
)

// LeastLoadedIndex is a simple Cluster that seeks the least-loaded hosts.
// LeastLoadedIndex uses CategoryClusterIndex and all hosts are qualified.
type LeastLoadedIndex struct {
	hosts types.Heap // The Host instances contained within the LeastLoadedIndex.
	mu    sync.Mutex
	log   logger.Logger
}

func NewLeastLoadedIndex(size int) *LeastLoadedIndex {
	index := &LeastLoadedIndex{
		hosts: make(types.Heap, 0, size),
	}

	config.InitLogger(&index.log, index)

	return index
}

func (index *LeastLoadedIndex) Category() (string, interface{}) {
	return scheduling.CategoryClusterIndex, expectedLeastLoadedIndex
}

func (index *LeastLoadedIndex) GetMetadataKey() scheduling.HostMetaKey {
	return HostMetaLeastLoadedIndex
}

func (index *LeastLoadedIndex) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	val := host.GetMeta(HostMetaLeastLoadedIndex)
	if val == nil {
		return expectedLeastLoadedIndex, scheduling.IndexNewQualified
	}

	if _, ok := val.(int32); ok {
		return expectedLeastLoadedIndex, scheduling.IndexQualified
	} else {
		return expectedLeastLoadedIndex, scheduling.IndexNewQualified
	}
}

func (index *LeastLoadedIndex) Len() int {
	return len(index.hosts)
}

func (index *LeastLoadedIndex) Add(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.unsafeAdd(host)
}

func (index *LeastLoadedIndex) unsafeAdd(host scheduling.Host) {
	heap.Push(&index.hosts, host)
	idx := host.GetIdx()
	host.SetMeta(HostMetaLeastLoadedIndex, int32(idx))
	host.SetContainedWithinIndex(true)
	index.log.Debug("Added Host %s to LeastLoadedIndex at position %d.", host.GetID(), idx)
}

func (index *LeastLoadedIndex) unsafeAddBack(host scheduling.Host) {
	heap.Push(&index.hosts, host)
	idx := host.GetIdx()
	host.SetMeta(HostMetaLeastLoadedIndex, int32(idx))
	host.SetContainedWithinIndex(true)
}

func (index *LeastLoadedIndex) Update(host scheduling.Host) {
	heap.Fix(&index.hosts, host.GetIdx())
}

func (index *LeastLoadedIndex) UpdateMultiple(_ []scheduling.Host) {
	heap.Init(&index.hosts)
}

func (index *LeastLoadedIndex) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaLeastLoadedIndex).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within LeastLoadedIndex", host.GetID())
		return
	}

	if !host.IsContainedWithinIndex() {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.GetID(), HostMetaLeastLoadedIndex, i)
	}

	index.log.Debug("Removing host %s from LeastLoadedIndex, position=%d", host.GetID(), i)

	heap.Remove(&index.hosts, int(i))

	host.SetMeta(HostMetaLeastLoadedIndex, nil)
	host.SetContainedWithinIndex(false)
}

func (index *LeastLoadedIndex) GetMetrics(_ scheduling.Host) []float64 {
	return nil
}

// getBlacklist converts the list of interface{} to a list of []int32 containing
// the indices of blacklisted Host instances within a LeastLoadedIndex.
func (index *LeastLoadedIndex) getBlacklist(blacklist []interface{}) []int32 {
	__blacklist := make([]int32, 0)
	for i, meta := range blacklist {
		if meta == nil {
			index.log.Error("Blacklist contains nil entry at index %d.", i)
			continue
		}

		__blacklist = append(__blacklist, meta.(int32))
	}

	return __blacklist
}

// unsafeSeek does the actual work of the Seek method.
// unsafeSeek does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *LeastLoadedIndex) unsafeSeek(blacklistArg []interface{}, metrics ...[]float64) (scheduling.Host, interface{}) {
	if len(index.hosts) == 0 {
		return nil, nil
	}

	// Convert the blacklistArg parameter into a slice of a concrete type; in this case, []int32.
	blacklist := index.getBlacklist(blacklistArg)
	hostsSeen := 0
	var host scheduling.Host

	// Keep iterating as long as:
	// (a) we have not found a Host, and
	// (b) we've not yet looked at every slot in the index and found that it is blacklisted.
	index.log.Debug("Searching for host. Len of blacklist: %d. Number of hosts in index: %d.", len(blacklist), index.Len())

	hostsToBeAddedBack := make([]scheduling.Host, 0)
	for host == nil && hostsSeen < index.Len() {
		nextHost := heap.Pop(&index.hosts)
		host = nextHost.(scheduling.Host)
		hostsToBeAddedBack = append(hostsToBeAddedBack, host)

		if nextHost != nil {
			hostsSeen += 1

			// If the given host is blacklisted, then look for a different host.
			if slices.Contains(blacklist, host.GetMeta(HostMetaLeastLoadedIndex).(int32)) {
				// Set to nil so that we have to continue searching.
				host = nil
			}
		}
	}

	if host == nil {
		index.log.Warn("Exhausted remaining hosts in index; failed to find non-blacklisted host.")
	}

	for _, hostToBeAddedBack := range hostsToBeAddedBack {
		index.unsafeAddBack(hostToBeAddedBack)
	}

	return host, -1
}

func (index *LeastLoadedIndex) Seek(blacklist []interface{}, metrics ...[]float64) (ret scheduling.Host, pos interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	return index.unsafeSeek(blacklist, metrics...)
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *LeastLoadedIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	st := time.Now()

	var (
		candidateHost scheduling.Host
		nextPos       interface{}
	)

	// We use a map in case we generate a new permutation and begin examining hosts that we've already seen before.
	hostsMap := make(map[string]scheduling.Host)
	hosts := make([]scheduling.Host, 0, n)
	hostsToBeAddedBack := make([]scheduling.Host, 0)

	for len(hosts) < n {
		candidateHost, nextPos = index.unsafeSeek(blacklist, metrics...)

		if candidateHost == nil {
			index.log.Warn("Index returned nil host.")
			return hosts, nextPos
		}

		// In case we reshuffled, make sure we haven't already received this host.
		// If indeed it is new, then we'll add it to the host map.
		if _, loaded := hostsMap[candidateHost.GetID()]; !loaded {
			// Check that the host is not outright excluded from scheduling right now and
			// that it satisfies whatever scheduling criteria was specified by the user.
			hostSatisfiesSchedulingCriteria := criteriaFunc == nil || criteriaFunc(candidateHost)

			// Note: ConsiderForScheduling will atomically check if the host is excluded from consideration
			// before marking it as being considered.
			if hostSatisfiesSchedulingCriteria && candidateHost.ConsiderForScheduling() {
				index.log.Debug("Found candidate: host %s (ID=%s)", candidateHost.GetNodeName(), candidateHost.GetID())
				hostsMap[candidateHost.GetID()] = candidateHost
			} else {
				index.log.Debug("Host %s (ID=%s) failed supplied criteria function. Rejecting.", candidateHost.GetNodeName(), candidateHost.GetID())
				hostsToBeAddedBack = append(hostsToBeAddedBack, candidateHost)
			}
		} else {
			index.log.Warn("Found duplicate: host %s (ID=%s) (we must've generated a new permutation)", candidateHost.GetNodeName(), candidateHost.GetID())
		}
	}

	// Put all the hosts from the map into the slice and return it.
	for _, host := range hostsMap {
		hosts = append(hosts, host)
	}

	for _, hostToBeAddedBack := range hostsToBeAddedBack {
		index.unsafeAddBack(hostToBeAddedBack)
	}

	if len(hosts) < n {
		index.log.Warn("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	} else {
		index.log.Debug("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	}

	return hosts, nextPos
}
