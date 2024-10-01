package scheduling

import (
	"log"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	expectedRandomIndex                = "*"
	HostMetaRandomIndex    HostMetaKey = "random_index"
	randomIndexGCThreshold             = 10
)

// RandomClusterIndex is a simple Cluster that seeks hosts randomly.
// RandomClusterIndex uses CategoryClusterIndex and all hosts are qualified.
type RandomClusterIndex struct {
	hosts       []*Host
	len         int32
	freeStart   int32        // The first freed index.
	perm        []int        // The permutation of the hosts.
	seekStart   int32        // The start index of the seek.
	numShuffles atomic.Int32 // The number of times the index has been shuffled to a new random permutation.
	mu          sync.Mutex

	log logger.Logger
}

func NewRandomClusterIndex(size int) *RandomClusterIndex {
	index := &RandomClusterIndex{
		hosts: make([]*Host, 0, size),
	}
	index.numShuffles.Store(0)

	config.InitLogger(&index.log, index)

	return index
}

func (index *RandomClusterIndex) Category() (string, interface{}) {
	return CategoryClusterIndex, expectedRandomIndex
}

func (index *RandomClusterIndex) IsQualified(host *Host) (interface{}, ClusterIndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	val := host.GetMeta(HostMetaRandomIndex)
	if val == nil {
		return expectedRandomIndex, ClusterIndexNewQualified
	}

	if _, ok := val.(int32); ok {
		return expectedRandomIndex, ClusterIndexQualified
	} else {
		return expectedRandomIndex, ClusterIndexNewQualified
	}
}

// NumReshuffles returns the number of times that this index has reshuffled its internal permutation.
func (index *RandomClusterIndex) NumReshuffles() int32 {
	return index.numShuffles.Load()
}

func (index *RandomClusterIndex) Len() int {
	return int(index.len)
}

func (index *RandomClusterIndex) Add(host *Host) {
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
	host.SetMeta(HostMetaRandomIndex, i)
	host.IsContainedWithinIndex = true
	index.log.Debug("Added Host %s to RandomClusterIndex at position %d.", host.ID, i)
	index.len += 1
}

func (index *RandomClusterIndex) Update(host *Host) {
	// No-op.
}

func (index *RandomClusterIndex) Remove(host *Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaRandomIndex).(int32)
	if !ok {
		index.log.Warn("Cannot remove host %s; it is not present within RandomClusterIndex", host.ID)
		return
	}

	if !host.IsContainedWithinIndex {
		index.log.Warn("Host %s thinks it is not contained within any Cluster indices; "+
			"however, its \"%s\" metadata has a non-nil value (%d).\n", host.ID, HostMetaRandomIndex, i)
	}

	index.log.Debug("Removing host %s from RandomClusterIndex, position=%d", host.ID, i)

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

	if index.hosts[i].ID != host.ID {
		log.Fatalf("Host at index %d of RandomClusterIndex is Host %s; however, we're supposed to remove Host %s...\n",
			i, index.hosts[i].ID, host.ID)
	}

	index.hosts[i] = nil
	index.len -= 1
	host.SetMeta(HostMetaRandomIndex, nil)
	host.IsContainedWithinIndex = false

	// Update freeStart.
	if i < index.freeStart {
		index.freeStart = i
	}

	// Compact the index.
	if len(index.hosts)-int(index.len) >= randomIndexGCThreshold {
		index.compactLocked(index.freeStart)
	}
}

func (index *RandomClusterIndex) compactLocked(from int32) {
	frontier := int(from)
	for i := frontier + 1; i < len(index.hosts); i++ {
		if index.hosts[i] != nil {
			index.hosts[frontier], index.hosts[i] = index.hosts[i], nil
			index.hosts[frontier].SetMeta(HostMetaRandomIndex, frontier)
			frontier += 1
		}
	}
	index.freeStart = int32(frontier)
	index.hosts = index.hosts[:frontier]
}

func (index *RandomClusterIndex) GetMetrics(_ *Host) []float64 {
	return nil
}

// unsafeSeek does the actual work of the Seek method.
// unsafeSeek does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *RandomClusterIndex) unsafeSeek(blacklist []interface{}, metrics ...[]float64) (ret *Host, pos interface{}) {
	if index.len == 0 {
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

	hostsSeen := 0

	index.log.Debug("Searching for host. Len of blacklist: %d. Number of hosts in index: %d.", len(__blacklist), index.Len())

	// Keep iterating as long as:
	// (a) we have not found a Host, and
	// (b) we've not yet looked at every slot in the index and found that it is blacklisted.
	for ret == nil && hostsSeen < index.Len() {
		// Generate a new permutation if seekStart is invalid.
		if index.seekStart == 0 || index.seekStart >= int32(len(index.perm)) {
			index.perm = rand.Perm(len(index.hosts))
			index.seekStart = 0
			index.numShuffles.Add(1)
		}
		ret = index.hosts[index.perm[index.seekStart]]
		index.seekStart++
		pos = index.seekStart

		// If the given host is blacklisted, then look for a different host.
		if slices.Contains(__blacklist, ret.GetMeta(HostMetaRandomIndex).(int32)) {
			ret = nil
			hostsSeen += 1
		}

		if hostsSeen >= index.Len() {
			index.log.Error("All hosts within index have been inspected. One should have been found by now.")
			return
		}
	}
	return
}

func (index *RandomClusterIndex) Seek(blacklist []interface{}, metrics ...[]float64) (ret *Host, pos interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	return index.unsafeSeek(blacklist, metrics...)
}

// SeekFrom seeks from the given position. Pass nil as pos to reset the seek.
func (index *RandomClusterIndex) SeekFrom(pos interface{}, metrics ...[]float64) (ret *Host, newPos interface{}) {
	if start, ok := pos.(int32); ok {
		index.seekStart = start
	} else {
		index.seekStart = 0
	}
	return index.Seek(make([]interface{}, 0), metrics...)
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *RandomClusterIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]*Host, interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	st := time.Now()

	if int32(n) > index.len {
		index.log.Error("Index contains just %d hosts. Cannot seek %d hosts.", index.len, n)
		return []*Host{}, nil
	}

	var (
		candidateHost *Host
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
	hostsMap := make(map[string]*Host)
	hosts := make([]*Host, 0, n)

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
	// This is because the first call to unsafeSeek will cause a new permutation to be generated, as we've reset
	// index.seekStart to 0. So, that will increment numShuffles by 1. Then, we'll iterate through that entire
	// permutation (if necessary) until we've found the n requested hosts. If we fail to find n hosts by that
	// point, then we'll reshuffle again, at which point we'll know we have looked at all possible hosts.
	//
	// (SeekMultipleFrom locks the index entirely such that no Hosts can be added or removed concurrently.)
	for len(hostsMap) < n && index.numShuffles.Load() < loopUntilNumShuffles {
		candidateHost, nextPos = index.unsafeSeek(blacklist, metrics...)

		// In case we reshuffled, make sure we haven't already received this host.
		// If indeed it is new, then we'll add it to the host map.
		if _, loaded := hostsMap[candidateHost.ID]; !loaded {
			if criteriaFunc == nil || criteriaFunc(candidateHost) {
				index.log.Debug("Found candidate: host %s", candidateHost.ID)
				hostsMap[candidateHost.ID] = candidateHost
			} else {
				index.log.Debug("Host %s failed supplied criteria function. Rejecting.", candidateHost.ID)
			}
		} else {
			index.log.Warn("Found duplicate: host %s (we must've generated a new permutation)", candidateHost.ID)
		}
	}

	// Put all the hosts from the map into the slice and return it.
	for _, host := range hostsMap {
		hosts = append(hosts, host)
	}

	if len(hosts) < n {
		index.log.Error("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	} else {
		index.log.Debug("Returning %d/%d candidateHost(s) from SeekMultipleFrom in %v.", len(hosts), n, time.Since(st))
	}

	return hosts, nextPos
}
