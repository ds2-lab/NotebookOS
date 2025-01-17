package index

import (
	"container/heap"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
)

const (
	LeastLoadedIndexMetadataKey types.HeapElementMetadataKey = "least_loaded_index_position"

	expectedLeastLoadedIndex = "*"
)

//type leastLoadedHost struct {
//	scheduling.Host
//
//	idx int
//}
//
//func (h *leastLoadedHost) SetIdx(idx int) {
//	h.idx = idx
//}
//
//func (h *leastLoadedHost) GetIdx() int {
//	return h.idx
//}
//
//func (h *leastLoadedHost) Compare(h2 interface{}) float64 {
//	if _, ok := h2.(*leastLoadedHost); ok {
//		return h.Host.Compare(h2.(*leastLoadedHost).Host)
//	}
//
//	return h.Host.Compare(h2)
//}

// LeastLoadedIndex is a simple Cluster that seeks the least-loaded hosts.
// LeastLoadedIndex uses CategoryClusterIndex and all hosts are qualified.
type LeastLoadedIndex struct {
	hosts       *types.Heap // The Host instances contained within the LeastLoadedIndex.
	mu          sync.Mutex
	log         logger.Logger
	metadataKey types.HeapElementMetadataKey
}

func NewLeastLoadedIndex(metadataKey types.HeapElementMetadataKey) *LeastLoadedIndex {
	index := &LeastLoadedIndex{
		// hosts: make(types.Heap, 0, size),
		hosts:       types.NewHeap(metadataKey),
		metadataKey: metadataKey,
	}

	config.InitLogger(&index.log, index)

	return index
}

func (index *LeastLoadedIndex) Category() (string, interface{}) {
	return scheduling.CategoryClusterIndex, "*"
}

func (index *LeastLoadedIndex) GetMetadataKey() types.HeapElementMetadataKey {
	return index.metadataKey
}

func (index *LeastLoadedIndex) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	val := host.GetMeta(index.metadataKey)
	if val == nil {
		return "*", scheduling.IndexNewQualified
	}

	if _, ok := val.(int32); ok {
		return "*", scheduling.IndexQualified
	} else {
		return "*", scheduling.IndexNewQualified
	}
}

func (index *LeastLoadedIndex) Len() int {
	return index.hosts.Len()
}

func (index *LeastLoadedIndex) Add(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.unsafeAdd(host)
}

func (index *LeastLoadedIndex) unsafeAdd(host scheduling.Host) {
	heap.Push(index.hosts, host)
	idx := host.GetIdx()

	host.SetMeta(index.metadataKey, int32(idx))
	host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
	host.SetMeta(scheduling.HostIndexKeyMetadata, expectedLeastLoadedIndex)
	host.SetContainedWithinIndex(true)
	index.log.Debug("Added Host %s (ID=%s) to LeastLoadedIndex at position %d.",
		host.GetNodeName(), host.GetID(), idx)
}

func (index *LeastLoadedIndex) unsafeAddBack(host scheduling.Host) {
	heap.Push(index.hosts, host)
	idx := host.GetIdx()
	host.SetMeta(index.metadataKey, int32(idx))
	host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
	host.SetMeta(scheduling.HostIndexKeyMetadata, expectedLeastLoadedIndex)
	host.SetContainedWithinIndex(true)
}

func (index *LeastLoadedIndex) Update(host scheduling.Host) {
	oldIdx := host.GetIdx()
	heap.Fix(index.hosts, oldIdx)
	newIdx := host.GetIdx()

	host.SetMeta(index.metadataKey, int32(newIdx))
	host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
	host.SetMeta(scheduling.HostIndexKeyMetadata, expectedLeastLoadedIndex)
}

func (index *LeastLoadedIndex) UpdateMultiple(hosts []scheduling.Host) {
	heap.Init(index.hosts)

	for _, host := range hosts {
		host.SetMeta(index.metadataKey, int32(host.GetIdx()))
		host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
		host.SetMeta(scheduling.HostIndexKeyMetadata, expectedLeastLoadedIndex)
	}
}

func (index *LeastLoadedIndex) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(index.metadataKey).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within LeastLoadedIndex", host.GetID())
		return
	}

	if !host.IsContainedWithinIndex() {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.GetID(), index.metadataKey, i)
	}

	index.log.Debug("Removing host %s from LeastLoadedIndex, position=%d", host.GetID(), i)

	heap.Remove(index.hosts, int(i))

	host.SetMeta(index.metadataKey, nil)
	host.SetMeta(scheduling.HostIndexCategoryMetadata, nil)
	host.SetMeta(scheduling.HostIndexKeyMetadata, nil)
	host.SetContainedWithinIndex(false)
}

func (index *LeastLoadedIndex) GetMetrics(_ scheduling.Host) []float64 {
	return nil
}

// getBlacklist converts the list of interface{} to a list of []int32 containing
// the indices of blacklisted Host instances within a LeastLoadedIndex.
func (index *LeastLoadedIndex) getBlacklist(blacklist []interface{}) []scheduling.Host {
	blacklistParsed := make([]scheduling.Host, 0)

	if len(blacklist) == 0 {
		return blacklistParsed
	}

	for i, host := range blacklist {
		if host == nil {
			index.log.Error("Blacklist contains nil entry at index %d: %v", i, blacklist)
			continue
		}

		blacklistParsed = append(blacklistParsed, host.(scheduling.Host))
	}

	return blacklistParsed
}

// unsafeSeek does the actual work of the Seek method.
// unsafeSeek does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *LeastLoadedIndex) unsafeSeek(blacklistArg []interface{}) scheduling.Host {
	if index.hosts.Len() == 0 {
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
			if ContainsHost(blacklist, host) {
				// Remove the host from the index temporarily so that we don't get it again.
				// We can't return it because it's blacklisted, but we need to keep looking.
				heap.Pop(index.hosts)

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

	//if host == nil {
	//	return nil, -1
	//}

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
		heap.Remove(index.hosts, candidateHost.GetIdx())

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
