package index

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"math"
)

type StaticIndex struct {
	MultiIndex *MultiIndex[*LeastLoadedIndex]

	GpusPerHost int32
	NumPools    int32
}

// isPowerOf2 returns true if the given value is a power of 2.
func isPowerOf2(n int32) bool {
	return (n & (n - 1)) == 0
}

// NewStaticIndex creates and returns a pointer to a new StaticIndex struct.
//
// Important: the 'gpusPerHost' parameter is expected to be a power of 2. If it isn't, then NewStaticIndex will panic.
func NewStaticIndex(gpusPerHost int32) (*StaticIndex, error) {
	if !isPowerOf2(gpusPerHost) {
		panic(fmt.Sprintf("GPUs per host is NOT a power of 2: %d", gpusPerHost))
	}

	// Compute the number of pools.
	numPools := int32(math.Log2(float64(gpusPerHost)) + 1)

	multi, err := NewMultiIndex[*LeastLoadedIndex](numPools, NewLeastLoadedIndexWrapper)
	if err != nil {
		return nil, err
	}

	// Reinitialize the logger with a different prefix.
	config.InitLogger(&multi.log, fmt.Sprintf("StaticIndex[%d Pools] ", numPools))

	static := &StaticIndex{
		MultiIndex:  multi,
		NumPools:    numPools,
		GpusPerHost: gpusPerHost,
	}

	return static, nil
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiIndex.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (index *StaticIndex) NumFreeHosts() int {
	index.MultiIndex.mu.Lock()
	defer index.MultiIndex.mu.Unlock()

	return index.MultiIndex.FreeHosts.Len()
}

// HasHostPool returns true if the MultiIndex has a host pool for the specified pool index.
func (index *StaticIndex) HasHostPool(gpus int32) bool {
	bucket := index.GetBucket(gpus)
	_, loaded := index.MultiIndex.HostPools[bucket]
	return loaded
}

// NumHostsInPool returns the number of hosts in the specified host pool.
func (index *StaticIndex) NumHostsInPool(gpus int32) int {
	bucket := index.GetBucket(gpus)
	pool, loaded := index.MultiIndex.HostPools[bucket]
	if !loaded {
		index.MultiIndex.log.Warn("Size of pool %d (gpus=%d) requested; however, no such pool exists...",
			bucket, gpus)

		return -1
	}

	return pool.Len()
}

// GetHostPool returns the HostPool for the specified pool index.
func (index *StaticIndex) GetHostPool(gpus int32) (*HostPool[*LeastLoadedIndex], bool) {
	bucket := index.GetBucket(gpus)
	pool, loaded := index.MultiIndex.HostPools[bucket]
	if loaded {
		return pool, true
	}

	return nil, false
}

// GetBucket returns the slot/pool that the kernel (replica) with the given spec should be placed into.
func (index *StaticIndex) GetBucket(gpus int32) int32 {
	bucket := index.GpusPerHost / gpus
	if bucket > index.NumPools {
		bucket = index.NumPools
	}

	return bucket - 1
}

// Seek returns the host specified by the metrics.
func (index *StaticIndex) Seek(blacklist []interface{}, metrics ...[]float64) (host scheduling.Host, pos interface{}) {
	numGpus := int32(metrics[0][0])
	bucket := float64(index.GetBucket(numGpus))

	return index.MultiIndex.Seek(blacklist, []float64{bucket})
}

// SeekFrom continues the seek from the position.
// SeekFrom(start interface{}, metrics ...[]float64) (host scheduling.Host, pos interface{})

// SeekMultipleFrom seeks n scheduling.Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *StaticIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {
	numGpus := int32(metrics[0][0])
	bucket := float64(index.GetBucket(numGpus))

	return index.MultiIndex.SeekMultipleFrom(pos, n, criteriaFunc, blacklist, []float64{bucket})
}

// Category returns the category of the index and the expected value.
func (index *StaticIndex) Category() (category string, expected interface{}) {
	return scheduling.CategoryClusterIndex, "*"
}

// Identifier returns the index's identifier.
func (index *StaticIndex) Identifier() string {
	return fmt.Sprintf("StaticIndex[%d,%d]", len(index.MultiIndex.HostPools), index.MultiIndex.Len())
}

// IsQualified returns the actual value according to the index category and whether the scheduling.Host is qualified.
// An index provider must be able to track indexed scheduling.Host instances and indicate disqualification.
func (index *StaticIndex) IsQualified(host scheduling.Host) (actual interface{}, qualified scheduling.IndexQualification) {
	// Since all hosts are qualified, we check if the host is in the index only.
	_, hostIsFree := index.MultiIndex.FreeHostsMap[host.GetID()]
	if hostIsFree {
		// The host is already present in the index.
		// It has not yet been assigned to a particular host pool.
		return expectedMultiIndexValue, scheduling.IndexQualified
	}

	_, hostIsInPool := index.MultiIndex.HostIdToHostPool[host.GetID()]
	if hostIsInPool {
		// The host is already present in the index and has already been assigned to a particular host pool.
		return expectedMultiIndexValue, scheduling.IndexQualified
	}

	return expectedMultiIndexValue, scheduling.IndexNewQualified
}

// Len returns the number of scheduling.Host instances in the index.
func (index *StaticIndex) Len() int {
	return index.MultiIndex.Len()
}

// Add adds a scheduling.Host to the index.
func (index *StaticIndex) Add(host scheduling.Host) {
	index.MultiIndex.mu.Lock()
	defer index.MultiIndex.mu.Unlock()

	index.MultiIndex.FreeHosts.Enqueue(host)
	index.MultiIndex.FreeHostsMap[host.GetID()] = host

	index.MultiIndex.Size += 1
}

// Update updates a scheduling.Host in the index.
func (index *StaticIndex) Update(host scheduling.Host) {
	index.MultiIndex.Update(host)
}

// UpdateMultiple updates multiple scheduling.Host instances in the index.
func (index *StaticIndex) UpdateMultiple(hosts []scheduling.Host) {
	index.MultiIndex.UpdateMultiple(hosts)
}

// Remove removes a scheduling.Host from the index.
func (index *StaticIndex) Remove(host scheduling.Host) {
	index.MultiIndex.Remove(host)
}

// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
func (index *StaticIndex) GetMetrics(host scheduling.Host) (metrics []float64) {
	return index.MultiIndex.GetMetrics(host)
}
