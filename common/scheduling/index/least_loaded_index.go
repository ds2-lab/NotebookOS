package index

import (
	"container/heap"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"slices"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
)

const (
	HostMetaLeastLoadedIndexPos scheduling.HostMetaKey = "least_loaded_index_position"
)

// LeastLoadedIndex is a simple Cluster that seeks the least-loaded hosts.
// LeastLoadedIndex uses CategoryClusterIndex and all hosts are qualified.
type LeastLoadedIndex struct {
	hosts      types.Heap // The Host instances contained within the LeastLoadedIndex.
	mu         sync.Mutex
	log        logger.Logger
	identifier string
}

func NewLeastLoadedIndex(size int, identifier string) *LeastLoadedIndex {
	index := &LeastLoadedIndex{
		hosts:      make(types.Heap, 0, size),
		identifier: identifier,
	}

	config.InitLogger(&index.log, index)

	return index
}

func (index *LeastLoadedIndex) Category() (string, interface{}) {
	return scheduling.CategoryClusterIndex, index.identifier
}

func (index *LeastLoadedIndex) GetMetadataKey() scheduling.HostMetaKey {
	return HostMetaLeastLoadedIndexPos
}

func (index *LeastLoadedIndex) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	val := host.GetMeta(HostMetaLeastLoadedIndexPos)
	if val == nil {
		return index.identifier, scheduling.IndexNewQualified
	}

	if _, ok := val.(int32); ok {
		return index.identifier, scheduling.IndexQualified
	} else {
		return index.identifier, scheduling.IndexNewQualified
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
	host.SetMeta(HostMetaLeastLoadedIndexPos, int32(idx))
	host.SetMeta(scheduling.HostIndexIdentifier, index.identifier)
	host.SetContainedWithinIndex(true)
	index.log.Debug("Added Host %s (ID=%s) to LeastLoadedIndex at position %d.",
		host.GetNodeName(), host.GetID(), idx)
}

func (index *LeastLoadedIndex) unsafeAddBack(host scheduling.Host) {
	heap.Push(&index.hosts, host)
	idx := host.GetIdx()
	host.SetMeta(HostMetaLeastLoadedIndexPos, int32(idx))
	host.SetContainedWithinIndex(true)
}

func (index *LeastLoadedIndex) Update(host scheduling.Host) {
	oldIdx := host.GetIdx()
	index.log.Debug("Fixing position of Host %s (ID=%s) in LeastLoadedIndex '%s' (old index of host: %d)",
		host.GetNodeName(), host.GetID(), index.identifier, oldIdx)

	heap.Fix(&index.hosts, oldIdx)
	newIdx := host.GetIdx()

	if oldIdx != newIdx {
		index.log.Debug("Updated position of Host %s (ID=%s) from %d to %d in LeastLoadedIndex '%s'",
			host.GetNodeName(), host.GetID(), oldIdx, newIdx, index.identifier)
	} else {
		index.log.Debug("Position of Host %s (ID=%s) in LeastLoadedIndex '%s' did not change (%d).",
			host.GetNodeName(), host.GetID(), index.identifier, oldIdx)
	}
	host.SetMeta(HostMetaLeastLoadedIndexPos, int32(newIdx))
	host.SetMeta(scheduling.HostIndexIdentifier, index.identifier)
}

func (index *LeastLoadedIndex) UpdateMultiple(hosts []scheduling.Host) {
	heap.Init(&index.hosts)

	for _, host := range hosts {
		host.SetMeta(HostMetaLeastLoadedIndexPos, int32(host.GetIdx()))
		host.SetMeta(scheduling.HostIndexIdentifier, index.identifier)
	}
}

func (index *LeastLoadedIndex) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaLeastLoadedIndexPos).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within LeastLoadedIndex", host.GetID())
		return
	}

	if !host.IsContainedWithinIndex() {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.GetID(), HostMetaLeastLoadedIndexPos, i)
	}

	index.log.Debug("Removing host %s from LeastLoadedIndex, position=%d", host.GetID(), i)

	heap.Remove(&index.hosts, int(i))

	host.SetMeta(HostMetaLeastLoadedIndexPos, nil)
	host.SetMeta(scheduling.HostIndexIdentifier, nil)
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
func (index *LeastLoadedIndex) unsafeSeek(blacklistArg []interface{}) scheduling.Host {
	if len(index.hosts) == 0 {
		return nil
	}

	// Convert the blacklistArg parameter into a slice of a concrete type; in this case, []int32.
	blacklist := index.getBlacklist(blacklistArg)

	// We only add back hosts that were in the blacklist.
	// It is the caller's responsibility to add back the host that we return.
	hostsToBeAddedBack := make([]scheduling.Host, 0)

	// Keep iterating as long as:
	// (a) we have not found a Host, and
	// (b) we've not yet looked at every slot in the index and found that it is blacklisted.
	index.log.Debug("Searching for host. Len of blacklist: %d. Number of hosts in index: %d.",
		len(blacklist), index.Len())

	var host scheduling.Host
	for host == nil && index.Len() > 0 {
		nextHost := index.hosts.Peek()
		host = nextHost.(scheduling.Host)

		if nextHost != nil {
			// If the given host is blacklisted, then look for a different host.
			if slices.Contains(blacklist, host.GetMeta(HostMetaLeastLoadedIndexPos).(int32)) {
				// Remove the host from the index temporarily so that we don't get it again.
				// We can't return it because it's blacklisted, but we need to keep looking.
				heap.Pop(&index.hosts)

				// Take note that we need to add the host back before we return from unsafeSeek.
				hostsToBeAddedBack = append(hostsToBeAddedBack, host)

				// Set to nil so that we have to continue searching.
				host = nil
			}
		}
	}

	// Did we fail to find the host? If so, we'll print a message.
	if host == nil {
		index.log.Debug("Exhausted remaining hosts in index; failed to find non-blacklisted host.")
	}

	// Add back any hosts that we skipped over due to them being blacklisted.
	for _, hostToBeAddedBack := range hostsToBeAddedBack {
		index.log.Debug("Adding blacklisted host %s (ID=%s) to index.",
			hostToBeAddedBack.GetNodeName(), hostToBeAddedBack.GetID())
		index.unsafeAddBack(hostToBeAddedBack)
	}

	return host
}

func (index *LeastLoadedIndex) Seek(blacklist []interface{}, metrics ...[]float64) (scheduling.Host, interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	host := index.unsafeSeek(blacklist)
	return host, -1
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *LeastLoadedIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction,
	blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {
	index.mu.Lock()

	hostsToBeAddedBack := make(map[string]scheduling.Host)
	defer func() {
		for _, hostToBeAddedBack := range hostsToBeAddedBack {
			index.unsafeAddBack(hostToBeAddedBack)
		}

		index.mu.Unlock()
	}()

	st := time.Now()

	// We use a map in case we generate a new permutation and begin examining hosts that we've already seen before.
	hostsMap := make(map[string]scheduling.Host)
	hosts := make([]scheduling.Host, 0, n)

	// We'll explicitly stop the loop.
	for {
		index.log.Debug("Searching for a total of %d hosts. Found so far: %d.", n, len(hosts))

		candidateHost := index.unsafeSeek(blacklist)

		// If the returned candidate host is nil, then we exhausted the index.
		// We'll break out of the loop and return any hosts that we found.
		// (Apparently we did not find all the hosts that we needed.)
		if candidateHost == nil {
			break
		}

		_, loaded := hostsMap[candidateHost.GetID()]
		if loaded {
			panic(fmt.Sprintf("Found duplicate: host %s (ID=%s)", candidateHost.GetNodeName(), candidateHost.GetID()))
		}

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
		}

		// We're done when the length of hostMap is equal to n.
		// This indicates that we've found all the hosts that we've been requested to find.
		if len(hostsMap) >= n {
			break
		}

		// Remove the host so that we don't get it again if we need to keep looking.
		// We'll add it back once we're done finding all the hosts.
		//
		// We use heap.Remove instead of heap.Pop because the criteraFunc called up above may reserve
		// resources on the host, which may cause its position in the heap to be updated. In this case,
		// it may no longer be the next element in the heap, so we remove it explicitly using whatever
		// its current index is.
		//
		// We only have to do this if we're going to keep searching.
		heap.Remove(&index.hosts, candidateHost.GetIdx())

		// Take note that we need to add the host back.
		hostsToBeAddedBack[candidateHost.GetID()] = candidateHost
	}

	// Put all the hosts from the map into the slice and return it.
	for _, host := range hostsMap {
		hosts = append(hosts, host)
	}

	if len(hosts) < n {
		index.log.Warn("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	} else {
		index.log.Debug("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	}

	// Note: we deferred two things up above:
	// - adding back any hosts we removed from the index
	// - unlocking the index
	return hosts, -1
}
