package scheduling

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"slices"
	"sync"
)

type StaticClusterIndex struct {
	hosts     []*Host // The Host instances in the index.
	length    int     // The number of Host instances in the index.
	freeStart int32   // The first freed index.
	seekStart int32   // The index at which we begin searching for a Host. For this index, its reset after every seek.

	mu  sync.Mutex
	log logger.Logger
}

func NewStaticClusterIndex() *StaticClusterIndex {
	index := &StaticClusterIndex{
		hosts:     make([]*Host, 0),
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
	return CategoryClusterIndex, expectedRandomIndex
}

// IsQualified returns the actual value according to the index category and whether the host is qualified.
// An index provider must be able to track indexed hosts and indicate disqualification.
func (index *StaticClusterIndex) IsQualified(host *Host) (actual interface{}, qualified ClusterIndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	if _, ok := host.GetMeta(HostMetaRandomIndex).(int32); ok {
		return expectedRandomIndex, ClusterIndexQualified
	} else {
		return expectedRandomIndex, ClusterIndexNewQualified
	}
}

// Len returns the number of hosts in the index.
func (index *StaticClusterIndex) Len() int {
	return index.length
}

// Add adds a host to the index.
func (index *StaticClusterIndex) Add(host *Host) {
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
	index.length += 1
	index.sortIndex()
}

// Update updates a host in the index.
// TODO: Call this when GPU amounts change.
func (index *StaticClusterIndex) Update(host *Host) {
	found := false
	for i, h := range index.hosts {
		if h.ID() == host.ID() {
			index.hosts[i] = host
			found = true
			break
		}
	}

	if !found {
		return
	}

	index.sortIndex()
}

// Remove removes a host from the index.
func (index *StaticClusterIndex) Remove(host *Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(HostMetaRandomIndex).(int32)
	if !ok {
		return
	}
	index.hosts[i] = nil
	index.length -= 1

	// Update freeStart.
	if i < index.freeStart {
		index.freeStart = i
	}

	// Compact the index.
	if len(index.hosts)-index.length >= randomIndexGCThreshold {
		index.compactLocked(index.freeStart)
	}

	slices.SortFunc(index.hosts, func(a, b *Host) int {
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
}

// sortIndex sorts the Host instances in the index by their number of idle GPUs.
// Host instances with more idle GPUs available appear first in the index.
func (index *StaticClusterIndex) sortIndex() {
	slices.SortFunc(index.hosts, func(a, b *Host) int {
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
			index.hosts[frontier].SetMeta(HostMetaRandomIndex, frontier)
			frontier += 1
		}
	}
	index.freeStart = int32(frontier)
	index.hosts = index.hosts[:frontier]
}

// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
func (index *StaticClusterIndex) GetMetrics(*Host) (metrics []float64) {
	return nil
}

// // // // // // // // // // // // // //
// ClusterIndexQuerier implementation  //
// // // // // // // // // // // // // //

// Seek returns the host specified by the metrics.
func (index *StaticClusterIndex) Seek(blacklist []interface{}, metrics ...[]float64) (ret *Host, pos interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	if index.length == 0 {
		index.seekStart = 0 // Reset
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

	index.log.Debug("Searching for host. Size of blacklist: %d. Number of hosts in index: %d.", len(__blacklist), index.Len())

	// Begin searching from `seekStart`, which is reset after every Seek operation.
	for _, host := range index.hosts[index.seekStart:] {
		// If the given host is blacklisted, then look for a different host.
		if slices.Contains(__blacklist, host.GetMeta(HostMetaRandomIndex).(int32)) {
			continue
		}

		ret = host
	}

	index.seekStart = 0 // Reset
	return
}

// SeekFrom continues the seek from the position.
func (index *StaticClusterIndex) SeekFrom(startIdx interface{}, metrics ...[]float64) (host *Host, pos interface{}) {
	if start, ok := startIdx.(int32); ok {
		index.seekStart = start
	} else {
		index.seekStart = 0
	}
	return index.Seek(make([]interface{}, 0), metrics...)
}
