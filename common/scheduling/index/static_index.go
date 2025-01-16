package index

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"log"
	"slices"
	"sync"
	"time"
)

const (
	expectedStaticIndex                        = "*"
	HostMetaStaticIndex scheduling.HostMetaKey = "static_index"
)

// StaticClusterIndex is a simple Cluster that seeks hosts randomly.
// StaticClusterIndex uses CategoryClusterIndex and all hosts are qualified.
type StaticClusterIndex struct {
	hosts     []scheduling.Host // The Host instances in the index.
	length    int               // The number of Host instances in the index.
	freeStart int32             // The first freed index.
	seekStart int32             // The index at which we begin searching for a Host. For this index, its reset after every seek.

	mu  sync.Mutex
	log logger.Logger
}

func NewStaticClusterIndex() *StaticClusterIndex {
	index := &StaticClusterIndex{
		hosts:     make([]scheduling.Host, 0),
		length:    0,
		freeStart: 0,
	}

	config.InitLogger(&index.log, index)

	return index
}

// // // // // // // // // // // // // //
// ClusterIndexProvider implementation //
// // // // // // // // // // // // // //

// Category returns the category of the index and the expected value.
func (index *StaticClusterIndex) Category() (category string, expected interface{}) {
	return scheduling.CategoryClusterIndex, expectedStaticIndex
}

func (index *StaticClusterIndex) GetMetadataKey() scheduling.HostMetaKey {
	return HostMetaStaticIndex
}

// IsQualified returns the actual value according to the index category and whether the host is qualified.
// An index provider must be able to track indexed hosts and indicate disqualification.
func (index *StaticClusterIndex) IsQualified(host scheduling.Host) (actual interface{}, qualified scheduling.IndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	if _, ok := host.GetMeta(HostMetaStaticIndex).(int32); ok {
		return expectedStaticIndex, scheduling.IndexQualified
	} else {
		return expectedStaticIndex, scheduling.IndexNewQualified
	}
}

// Len returns the number of hosts in the index.
func (index *StaticClusterIndex) Len() int {
	return index.length
}

// Add adds a host to the index.
func (index *StaticClusterIndex) Add(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

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

	host.SetMeta(HostMetaStaticIndex, i)
	host.SetMeta(scheduling.HostIndexIdentifier, "*")
	host.SetContainedWithinIndex(true)
	index.length += 1
	index.sortIndex()
}

// sortIndex sorts the Host instances in the index by their number of idle GPUs.
// Host instances with more idle GPUs available appear first in the index.
func (index *StaticClusterIndex) sortIndex() {
	slices.SortFunc(index.hosts, func(a, b scheduling.Host) int {
		// Note: we flipped the order of the greater/less-than signs here so that it sorts in descending order,
		// with the Hosts with the most idle GPUs appearing first.
		if a.IdleGPUs() > b.IdleGPUs() {
			return -1
		} else if a.IdleGPUs() < b.IdleGPUs() {
			return 1
		} else {
			return 0
		}
	})

	// Need to update the meta fields of all the hosts now.
	var idx int32 = 0
	for _, host := range index.hosts {
		host.SetMeta(HostMetaStaticIndex, idx)
		host.SetContainedWithinIndex(true)

		idx += 1
	}
}

func (index *StaticClusterIndex) Update(_ scheduling.Host) {
	index.sortIndex()
}

func (index *StaticClusterIndex) UpdateMultiple(_ []scheduling.Host) {
	index.sortIndex()
}

func (index *StaticClusterIndex) Remove(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaStaticIndex).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within StaticClusterIndex", host.GetID())
		return
	}

	if !host.IsContainedWithinIndex() {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.GetID(), HostMetaStaticIndex, i)
	}

	index.log.Debug("Removing host %s from StaticClusterIndex, position=%d", host.GetID(), i)

	if i > int32(len(index.hosts)) {
		log.Fatalf("Index %d is out of range for StaticClusterIndex of length %d...\n", i, len(index.hosts))
	}

	if index.hosts[i] == nil {
		index.log.Error("There is no host at index %d of StaticClusterIndex (i.e., hosts[%d] is nil).", i, i)
		for idx := 0; idx < cap(index.hosts); idx++ {
			if index.hosts[idx] != nil {
				index.log.Error("index.hosts[%d] = %v", idx, index.hosts[idx])
			} else {
				index.log.Error("index.hosts[%d] = %v", idx, index.hosts[idx])
			}
		}

		log.Fatalf("There is no host at index %d of StaticClusterIndex (i.e., hosts[%d] is nil.\n", i, i)
	}

	if index.hosts[i].GetID() != host.GetID() {
		log.Fatalf("Host at index %d of StaticClusterIndex is Host %s; however, we're supposed to remove Host %s...\n",
			i, index.hosts[i].GetID(), host.GetID())
	}

	index.hosts[i] = nil
	host.SetMeta(HostMetaStaticIndex, nil)
	host.SetMeta(scheduling.HostIndexIdentifier, nil)
	host.SetContainedWithinIndex(false)

	// Update freeStart.
	if i < index.freeStart {
		index.freeStart = i
	}

	index.compactLocked(index.freeStart)
}

// compact compacts the index by calling compactLocked.
//
// This will acquire the index's lock before calling compactLocked.
func (index *StaticClusterIndex) compact(from int32) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.compactLocked(from)
}

// compactLocked compacts the index.
//
// Important: this function is expected to be called with the index's lock.
func (index *StaticClusterIndex) compactLocked(from int32) {
	frontier := int(from)
	for i := frontier + 1; i < len(index.hosts); i++ {
		if index.hosts[i] != nil {
			index.hosts[frontier], index.hosts[i] = index.hosts[i], nil
			index.hosts[frontier].SetMeta(HostMetaStaticIndex, int32(frontier))
			index.hosts[frontier].SetContainedWithinIndex(true)
			frontier += 1
		}
	}
	index.freeStart = int32(frontier)
	index.hosts = index.hosts[:frontier]
}

// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
func (index *StaticClusterIndex) GetMetrics(scheduling.Host) (metrics []float64) {
	return nil
}

// getBlacklist converts the list of interface{} to a list of []int32 containing
// the indices of blacklisted Host instances within a RandomClusterIndex.
func (index *StaticClusterIndex) getBlacklist(blacklist []interface{}) []int32 {
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

// // // // // // // // // // // // // //
// ClusterIndexQuerier implementation  //
// // // // // // // // // // // // // //

// Seek returns the host specified by the metrics.
func (index *StaticClusterIndex) Seek(blacklist []interface{}, metrics ...[]float64) (ret scheduling.Host, pos interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	// Always search from the beginning for the static index.
	index.seekStart = 0

	if index.length == 0 {
		return nil, nil
	}

	// Convert the blacklist into a slice of a concrete type; in this case, []int32.
	__blacklist := make([]int32, 0)
	for i, meta := range blacklist {
		if meta == nil {
			index.log.Error("Blacklist contains nil entry at index %d.", i)
			continue
		}

		__blacklist = append(__blacklist, meta.(int32))
	}

	index.mu.Lock()
	defer index.mu.Unlock()

	return index.seekInternal(blacklist, metrics...)
}

// seekInternal does the actual work of the Seek method.
// seekInternal does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *StaticClusterIndex) seekInternal(blacklistArg []interface{}, _ ...[]float64) (scheduling.Host, int32) {
	if len(index.hosts) == 0 {
		return nil, 0
	}

	// Convert the blacklistArg parameter into a slice of a concrete type; in this case, []int32.
	blacklist := index.getBlacklist(blacklistArg)
	var host scheduling.Host

	// Keep iterating as long as:
	// (a) we have not found a Host, and
	// (b) we've not yet looked at every slot in the index and found that it is blacklisted.
	index.log.Debug("Searching for host. Len of blacklist: %d. Number of hosts in index: %d.", len(blacklist), index.Len())
	for i := index.seekStart; i < int32(len(index.hosts)) && host == nil; i++ {
		host = index.hosts[index.seekStart]
		index.seekStart++
		if host != nil {
			// If the given host is blacklisted, then look for a different host.
			if slices.Contains(blacklist, host.GetMeta(HostMetaStaticIndex).(int32)) {
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

func (index *StaticClusterIndex) SeekMultipleFrom(_ interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	st := time.Now()

	var (
		candidateHost scheduling.Host
		nextPos       interface{}
	)

	hostsMap := make(map[string]scheduling.Host)
	hosts := make([]scheduling.Host, 0, n)

	// Always search from the beginning for the static index.
	index.seekStart = 0

	for len(hostsMap) < n {
		candidateHost, nextPos = index.seekInternal(blacklist, metrics...)

		if candidateHost == nil {
			index.log.Warn("Index returned nil host.")
			break
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

		if nextPos == 0 {
			break
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

	return hosts, nextPos
}
