package core

import (
	"math/rand"
	"sync"
)

const (
	expectedRandomIndex                = "*"
	hostMetaRandomIndex    HostMetaKey = "random_index"
	randomIndexGCThreshold             = 10
)

// RandomClusterIndex is a simple cluster that seeks hosts randomly.
// RandomClusterIndex uses CategoryClusterIndex and all hosts are qualified.
type RandomClusterIndex struct {
	hosts     []Host
	len       int32
	freeStart int32 // The first freed index.
	perm      []int // The permutation of the hosts.
	seekStart int32 // The start index of the seek.
	mu        sync.Mutex
}

func NewRandomClusterIndex(size int) *RandomClusterIndex {
	return &RandomClusterIndex{
		hosts: make([]Host, 0, size),
	}
}

func (index *RandomClusterIndex) Category() (string, interface{}) {
	return CategoryClusterIndex, expectedRandomIndex
}

func (index *RandomClusterIndex) IsQualified(host Host) (interface{}, ClusterIndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	if _, ok := host.GetMeta(hostMetaRandomIndex).(int32); ok {
		return expectedRandomIndex, ClusterIndexQualified
	} else {
		return expectedRandomIndex, ClusterIndexNewQualified
	}
}

func (index *RandomClusterIndex) Len() int {
	return int(index.len)
}

func (index *RandomClusterIndex) Add(host Host) {
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
	host.SetMeta(hostMetaRandomIndex, i)
	index.len += 1
}

func (index *RandomClusterIndex) Update(host Host) {
	// No-op.
}

func (index *RandomClusterIndex) Remove(host Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	i, ok := host.GetMeta(hostMetaRandomIndex).(int32)
	if !ok {
		return
	}
	index.hosts[i] = nil
	index.len -= 1

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
			index.hosts[frontier].SetMeta(hostMetaRandomIndex, frontier)
			frontier += 1
		}
	}
	index.freeStart = int32(frontier)
	index.hosts = index.hosts[:frontier]
}

func (index *RandomClusterIndex) GetMetrics(_ Host) []float64 {
	return nil
}

func (index *RandomClusterIndex) Seek(metrics ...[]float64) (ret Host, pos interface{}) {
	index.mu.Lock()
	defer index.mu.Unlock()

	if index.len == 0 {
		return nil, nil
	}

	for ret == nil {
		// Generate a new permutation if seekStart is invalid.
		if index.seekStart == 0 || index.seekStart >= int32(len(index.perm)) {
			index.perm = rand.Perm(len(index.hosts))
			index.seekStart = 0
		}
		pos = index.seekStart
		ret = index.hosts[index.perm[pos.(int32)]]
		index.seekStart++
	}
	return
}

// SeekFrom seeks from the given position. Pass nil as pos to reset the seek.
func (index *RandomClusterIndex) SeekFrom(pos interface{}, metrics ...[]float64) (ret Host, newPos interface{}) {
	if pos == nil {
		index.seekStart = 0
	}
	return index.Seek(metrics...)
}
