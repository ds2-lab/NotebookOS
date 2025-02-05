package index

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
)

const (
	expectedRandomIndex                                 = "*"
	HostMetaRandomIndex    types.HeapElementMetadataKey = "random_index"
	randomIndexGCThreshold                              = 10
)

// RandomClusterIndex is a simple Cluster that seeks hosts randomly.
// RandomClusterIndex uses CategoryClusterIndex and all hosts are qualified.
type RandomClusterIndex struct {
	log logger.Logger
	*CallbackManager
	perm        []int             // The permutation of the hosts. Collection of indices that gets shuffled. We use these to index the hosts field.
	hosts       []scheduling.Host // The Host instances contained within the RandomClusterIndex.
	mu          sync.Mutex
	freeStart   int32        // The first freed index.
	seekStart   int32        // The start index of the seek.
	numShuffles atomic.Int32 // The number of times the index has been shuffled to a new random permutation.
	len         int32
}

func NewRandomClusterIndex(size int) *RandomClusterIndex {
	index := &RandomClusterIndex{
		CallbackManager: NewCallbackManager(),
		hosts:           make([]scheduling.Host, 0, size),
	}
	index.numShuffles.Store(0)

	config.InitLogger(&index.log, index)

	return index
}

func (index *RandomClusterIndex) Identifier() string {
	return fmt.Sprintf("RandomClusterIndex[%d]", index.len)
}

func (index *RandomClusterIndex) Category() (string, interface{}) {
	return scheduling.CategoryClusterIndex, expectedRandomIndex
}

func (index *RandomClusterIndex) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	if !host.Enabled() {
		// If the host is not enabled, then it is ineligible to be added to the index.
		// In general, disabled hosts will not be attempted to be added to the index,
		// but if that happens, then we return scheduling.IndexUnqualified.
		return expectedRandomIndex, scheduling.IndexUnqualified
	}

	// Since all hosts are qualified, we check if the host is in the index only.
	val := host.GetMeta(HostMetaRandomIndex)
	if val == nil {
		return expectedRandomIndex, scheduling.IndexNewQualified
	}

	if _, ok := val.(int32); ok {
		return expectedRandomIndex, scheduling.IndexQualified
	} else {
		return expectedRandomIndex, scheduling.IndexNewQualified
	}
}

// NumReshuffles returns the number of times that this index has reshuffled its internal permutation.
func (index *RandomClusterIndex) NumReshuffles() int32 {
	return index.numShuffles.Load()
}

func (index *RandomClusterIndex) Len() int {
	return int(index.len)
}

func (index *RandomClusterIndex) Add(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.log.Debug("Adding host %s (ID=%s) to RandomClusterIndex.", host.GetNodeName(), host.GetID())

	i := index.freeStart
	if i < int32(len(index.hosts)) {
		index.hosts[i] = host
		for j := i + 1; j < int32(len(index.hosts)); j++ {
			if index.hosts[j] == nil {
				index.freeStart = j
				break
			}
		}
	} else {
		index.hosts = append(index.hosts, host)
		i = index.freeStart // old len(index.hosts) or current len(index.hosts) - 1
		index.freeStart += 1
	}
	host.SetMeta(HostMetaRandomIndex, i)
	host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
	host.SetMeta(scheduling.HostIndexKeyMetadata, expectedRandomIndex)
	host.SetContainedWithinIndex(true)
	index.log.Debug("Added Host %s to RandomClusterIndex at position %d. Index length: %d.",
		host.GetID(), i, index.Len())
	index.len += 1

	// Invoke callback.
	index.InvokeHostAddedCallbacks(host)
}

func (index *RandomClusterIndex) Update(host scheduling.Host) {
	// No-op.

	// Invoke callbacks.
	index.InvokeHostUpdatedCallbacks(host)
}

func (index *RandomClusterIndex) UpdateMultiple(hosts []scheduling.Host) {
	// No-op.

	// Invoke callbacks.
	for _, host := range hosts {
		index.InvokeHostUpdatedCallbacks(host)
	}
}

func (index *RandomClusterIndex) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaRandomIndex).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within RandomClusterIndex", host.GetID())
		return
	}

	if !host.IsContainedWithinIndex() {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.GetID(), HostMetaRandomIndex, i)
	}

	index.log.Debug("Removing host %s from RandomClusterIndex, position=%d", host.GetID(), i)

	if i > int32(len(index.hosts)) {
		log.Fatalf("Index %d is out of range for RandomClusterIndex of length %d...\n", i, len(index.hosts))
	}

	if index.hosts[i] == nil {
		index.log.Error("There is no host at index %d of RandomClusterIndex (i.e., hosts[%d] is nil).", i, i)
		for idx := 0; idx < cap(index.hosts); idx++ {
			if index.hosts[idx] != nil {
				index.log.Error("index.hosts[%d] = %v", idx, index.hosts[idx])
			} else {
				index.log.Error("index.hosts[%d] = %v", idx, index.hosts[idx])
			}
		}

		log.Fatalf("There is no host at index %d of RandomClusterIndex (i.e., hosts[%d] is nil.\n", i, i)
	}

	if index.hosts[i].GetID() != host.GetID() {
		log.Fatalf("Host at index %d of RandomClusterIndex is Host %s; however, we're supposed to remove Host %s...\n",
			i, index.hosts[i].GetID(), host.GetID())
	}

	index.hosts[i] = nil
	index.len -= 1
	host.SetMeta(HostMetaRandomIndex, nil)
	host.SetMeta(scheduling.HostIndexCategoryMetadata, nil)
	host.SetMeta(scheduling.HostIndexKeyMetadata, nil)
	host.SetContainedWithinIndex(false)

	// Update freeStart.
	if i < index.freeStart {
		index.freeStart = i
	}

	// Compact the index.
	if len(index.hosts)-int(index.len) >= randomIndexGCThreshold {
		index.compactLocked(index.freeStart)
	}

	// Invoke callback.
	index.InvokeHostRemovedCallbacks(host)
}

func (index *RandomClusterIndex) compactLocked(from int32) {
	frontier := from
	for i := frontier + 1; i < int32(len(index.hosts)); i++ {
		if index.hosts[i] != nil {
			index.hosts[frontier], index.hosts[i] = index.hosts[i], nil
			index.hosts[frontier].SetMeta(HostMetaRandomIndex, frontier)
			index.hosts[frontier].SetContainedWithinIndex(true)
			frontier += 1
		}
	}
	index.freeStart = int32(frontier)
	index.hosts = index.hosts[:frontier]
}

func (index *RandomClusterIndex) GetMetrics(_ scheduling.Host) []float64 {
	return nil
}

// reshuffle shuffles the Host permutation of the target RandomClusterIndex.
func (index *RandomClusterIndex) reshuffle() {
	index.perm = rand.Perm(len(index.hosts))
	index.seekStart = 0
	index.numShuffles.Add(1)
}

// reshuffleIfNecessary will reshuffle the permutation of Host instances of the target RandomClusterIndex
// if the RandomClusterIndex is in a state in which a reshuffle is required.
func (index *RandomClusterIndex) reshuffleIfNecessary() {
	if index.reshuffleRequired() {
		index.reshuffle()
	}
}

// reshuffleRequired returns true if the RandomClusterIndex should reshuffle its permutation of Host instances.
func (index *RandomClusterIndex) reshuffleRequired() bool {
	return index.seekStart == 0 || index.seekStart >= int32(len(index.perm))
}

// unsafeSeek does the actual work of the Seek method.
// unsafeSeek does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *RandomClusterIndex) unsafeSeek(blacklistArg []interface{}, metrics ...[]float64) (scheduling.Host, interface{}) {
	if index.len == 0 {
		return nil, nil
	}

	// Convert the blacklistArg parameter into a slice of a concrete type; in this case, []int32.
	blacklist := getBlacklist(blacklistArg)
	hostsSeen := 0
	var host scheduling.Host

	// Keep iterating as long as:
	// (a) we have not found a Host, and
	// (b) we've not yet looked at every slot in the index and found that it is blacklisted.
	index.log.Debug("Searching for host. Len of blacklist: %d. Number of hosts in index: %d.", len(blacklist), index.Len())
	for host == nil && hostsSeen < index.Len() {
		// Generate a new permutation if seekStart is invalid.
		index.reshuffleIfNecessary()

		host = index.hosts[index.perm[index.seekStart]]
		index.seekStart++
		if host != nil {
			hostsSeen += 1

			// If the given host is blacklisted, then look for a different host.
			// if slices.Contains(blacklist, host.GetMeta(HostMetaRandomIndex).(int32)) {
			if ContainsHost(blacklist, host) {
				// Set to nil so that we have to continue searching.
				host = nil
			}
		}
	}

	if host == nil {
		index.log.Warn("Exhausted remaining hosts in index; failed to find non-blacklisted host.")
	}

	return host, index.seekStart
}

func (index *RandomClusterIndex) Seek(blacklist []interface{}, metrics ...[]float64) (ret scheduling.Host, pos interface{}, err error) {
	index.mu.Lock()
	defer index.mu.Unlock()

	ret, pos = index.unsafeSeek(blacklist, metrics...)

	return ret, pos, err
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *RandomClusterIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction,
	blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}, error) {

	index.mu.Lock()
	defer index.mu.Unlock()

	st := time.Now()

	var (
		candidateHost scheduling.Host
		nextPos       interface{}
	)

	initialNumShuffles := index.numShuffles.Load()

	// Even if we don't reset the permutation immediately, we want to loop until the number of shuffles is 2
	// greater than its current value.
	//
	// If we do reset the permutation immediately (by either passing 0 for pos or passing nil for pos, which
	// explicitly sets seekStart to 0), then we will immediately generate a new permutation upon calling Seek,
	// so numShuffles will already be equal to initialNumShuffles + 1. We iterate over the entire permutation,
	// then shuffle again, at which point numShuffles will equal initialNumShuffles + 2, and we'll have looked
	// at every possible candidateHost, so we should give up.
	//
	// If we specify some other starting index to begin our search from, then we will initially search until the
	// end of the current permutation, at which point we'll reshuffle. We want to search through again, in case
	// there were some hosts we didn't examine during our first partial pass (partial because we didn't start at
	// the beginning of the permutation).
	loopUntilNumShuffles := initialNumShuffles + 2

	// We use a map in case we generate a new permutation and begin examining hosts that we've already seen before.
	hostsMap := make(map[string]scheduling.Host)
	hosts := make([]scheduling.Host, 0, n)

	// Pick up from a particular position in the index.
	if start, ok := pos.(int32); ok {
		index.seekStart = start
	} else {
		// Reset the index. This will prompt Seek to generate a new random permutation.
		index.seekStart = 0
	}

	// If the number of shuffles becomes equal to initialNumShuffles+2, then we've iterated through the entire
	// permutation of hosts at least once, and we need to give up.
	//
	// This is because the first call to seekInternal will cause a new permutation to be generated, as we've reset
	// index.seekStart to 0. So, that will increment numShuffles by 1. Then, we'll iterate through that entire
	// permutation (if necessary) until we've found the n requested hosts. If we fail to find n hosts by that
	// point, then we'll reshuffle again, at which point we'll know we have looked at all possible hosts.
	//
	// (SeekMultipleFrom locks the index entirely such that no Hosts can be added or removed concurrently.)
	for len(hostsMap) < n && index.numShuffles.Load() < loopUntilNumShuffles {
		candidateHost, nextPos = index.unsafeSeek(blacklist, metrics...)

		if candidateHost == nil {
			index.log.Warn("Index returned nil host.")
			return hosts, nextPos, nil
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
			}
		} else {
			index.log.Warn("Found duplicate: host %s (ID=%s) (we must've generated a new permutation)", candidateHost.GetNodeName(), candidateHost.GetID())
		}
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

	return hosts, nextPos, nil
}
