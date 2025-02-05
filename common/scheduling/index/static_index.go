package index

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"math"
)

var (
	ErrMissingMetrics = errors.New("cannot seek host(s) because one or more required metrics were not provided")
)

// roundToLowestPowerOf2 rounds the given value (down) to the closest power of 2 (that is <= val).
func roundToLowestPowerOf2(val float64) float64 {
	// Special case for non-positive values.
	if val <= 0 {
		return 0
	}

	// Find the exponent of the power of 2.
	exponent := math.Floor(math.Log2(val))

	// Calculate the power of 2.
	result := math.Pow(2, exponent)

	return result
}

// GetStaticIndexBucket returns the bucket number of the specified number of GPUs, given the number
// of GPUs per host and the number of pools available in the StaticIndex.
//
// GetStaticIndexBucket will return -1 for an unknown or unexpected or unsupported set of inputs, such
// as when gpus > gpusPerHost.
func GetStaticIndexBucket(gpus int32, gpusPerHost int32) int32 {
	if gpus == 0 {
		gpus = 1
	}

	if gpus > gpusPerHost {
		// panic(fmt.Sprintf("Requested %d GPUs, but there are only %d GPUs per host", gpus, gpusPerHost))
		return -1
	}

	// First, divide the number of GPUs per host by the number of requested GPUs.
	// Typically, there are 8 GPUs per host.
	// So, we'll have:
	// - 8 / 8 = 1
	// - 8 / 4 = 2
	// - 8 / 2 = 4
	// - 8 / 1 = 8
	//
	// For non-standard/uncommon values (3, 5, 6, and 7), we'll round them to the nearest power of 2:
	// - 8 / 3 = 2.6666... --> 2
	// - 8 / 5 = 1.6 --> 1
	// - 8 / 6 = 1.3333... -> 1
	// - 8 / 7 = 1.1428... -> 1
	_bucket := float64(gpusPerHost) / float64(gpus)
	poolIndex := int32(roundToLowestPowerOf2(_bucket))

	//fmt.Printf("NumGpus=%d, GpusPerHost=%d, NumPools=%d, _bucket=%f, poolIndex=%d\n",
	//	gpus, gpusPerHost, numPools, _bucket, poolIndex)

	return poolIndex
}

// StaticIndex is a struct that implements the scheduling.ClusterIndex interface
// and provides support for the Static scheduling algorithm/policy.
type StaticIndex struct {
	log         logger.Logger
	MultiIndex  *MultiIndex[*LeastLoadedIndex]
	GpusPerHost int32
	NumPools    int32
}

// isPowerOf2 returns true if the given value is a power of 2.
func isPowerOf2(n int32) bool {
	return (n & (n - 1)) == 0
}

// staticIndexPoolInitializer satisfies the HostPoolInitializer type and is used to initialize the HostPool instances
// of the MultiIndex that underlies a StaticIndex.
func staticIndexPoolInitializer(numPools int32, indexProvider Provider[*LeastLoadedIndex]) map[int32]*HostPool[*LeastLoadedIndex] {
	pools := make(map[int32]*HostPool[*LeastLoadedIndex], numPools)

	poolNumber := int32(1)
	for int32(len(pools)) < numPools {
		pools[poolNumber] = NewHostPool(indexProvider(poolNumber), poolNumber)
		fmt.Printf("Initialized HostPool %d/%d with ID=%d\n", len(pools), numPools, poolNumber)

		poolNumber = poolNumber * 2
	}

	return pools
}

// NewStaticIndex creates and returns a pointer to a new StaticIndex struct.
//
// Important: the 'gpusPerHost' parameter is expected to be a power of 2. If it isn't, then NewStaticIndex will panic.
func NewStaticIndex(gpusPerHost int32) *StaticIndex {
	if !isPowerOf2(gpusPerHost) {
		panic(fmt.Sprintf("GPUs per host is NOT a power of 2: %d", gpusPerHost))
	}

	// Compute the number of pools.
	numPools := int32(math.Log2(float64(gpusPerHost)) + 1)

	multi := NewMultiIndexWithHostPoolInitializer[*LeastLoadedIndex](numPools, NewLeastLoadedIndexWrapper, staticIndexPoolInitializer)

	// Reinitialize the logger with a different prefix.
	config.InitLogger(&multi.log, fmt.Sprintf("StaticIndex[%d Pools] ", numPools))

	staticIndex := &StaticIndex{
		MultiIndex:  multi,
		NumPools:    numPools,
		GpusPerHost: gpusPerHost,
	}

	config.InitLogger(&staticIndex.log, fmt.Sprintf("StaticIndex[%d Pools] ", numPools))

	return staticIndex
}

// GetNumPools is a convenience method to return index.NumPools as an int (rather than an int32).
func (index *StaticIndex) GetNumPools() int {
	return int(index.NumPools)
}

// HostPoolIDs returns the valid IDs of each HostPool managed by the target StaticIndex.
//
// The returned slice of HostPool IDs will be sorted in ascending order (i.e., smallest to largest).
func (index *StaticIndex) HostPoolIDs() []int32 {
	return index.MultiIndex.HostPoolIDs()
}

// NumFreeHosts returns the number of "free" scheduling.Host instances within the target MultiIndex.
//
// "Free" hosts are those that have not been placed into a particular HostPool (yet).
func (index *StaticIndex) NumFreeHosts() int {
	index.MultiIndex.mu.Lock()
	defer index.MultiIndex.mu.Unlock()

	return index.MultiIndex.FreeHosts.Len()
}

// HasHostPool returns true if the MultiIndex has a host pool for the specified number of GPUs.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (index *StaticIndex) HasHostPool(gpus int32) bool {
	bucket := index.GetBucket(gpus)
	_, loaded := index.MultiIndex.HostPools[bucket]
	return loaded
}

// HasHostPoolByIndex returns true if the MultiIndex has a host pool for the specified pool index.
func (index *StaticIndex) HasHostPoolByIndex(poolNumber int32) bool {
	_, loaded := index.MultiIndex.HostPools[poolNumber]
	return loaded
}

// NumHostsInPoolByIndex returns the number of hosts in the specified host pool.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (index *StaticIndex) NumHostsInPoolByIndex(poolIndex int32) int {
	pool, loaded := index.MultiIndex.HostPools[poolIndex]
	if !loaded {
		index.log.Warn("Size of pool with unknown index=%d requested.", poolIndex)

		return -1
	}

	return pool.Len()
}

// NumHostsInPool returns the number of hosts in the specified host pool.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (index *StaticIndex) NumHostsInPool(gpus int32) int {
	bucket := index.GetBucket(gpus)
	pool, loaded := index.MultiIndex.HostPools[bucket]
	if !loaded {
		index.log.Warn("Size of pool %d (gpus=%d) requested; however, no such pool exists...",
			bucket, gpus)

		return -1
	}

	return pool.Len()
}

// GetHostPoolByIndex returns the HostPool for the specified pool index.
// GetHostPoolByIndex does not convert the given index to a bucket.
func (index *StaticIndex) GetHostPoolByIndex(poolNumber int32) (*HostPool[*LeastLoadedIndex], bool) {
	pool, loaded := index.MultiIndex.HostPools[poolNumber]
	if loaded {
		return pool, true
	}

	return nil, false
}

// GetHostPool returns the HostPool for the specified number of GPUs.
// The gpus parameter is not treated directly as an index. Instead, it is first converted to a bucket.
func (index *StaticIndex) GetHostPool(gpus int32) (*HostPool[*LeastLoadedIndex], bool) {
	bucket := index.GetBucket(gpus)
	pool, loaded := index.MultiIndex.HostPools[bucket]
	if loaded {
		return pool, true
	}

	return nil, false
}

// GetBucket returns the slot/pool that the kernel (replica) with the given spec should be placed into.
//
// If gpus is 0, then it is set to 1 for the purposes of bucket calculation.
func (index *StaticIndex) GetBucket(gpus int32) int32 {
	return GetStaticIndexBucket(gpus, index.GpusPerHost)
}

// Seek returns the host specified by the metrics.
func (index *StaticIndex) Seek(blacklist []interface{}, metrics ...[]float64) (scheduling.Host, interface{}, error) {
	if len(metrics) == 0 || len(metrics[0]) == 0 {
		index.log.Error("StaticIndex::Seek is missing valid metrics.")

		return nil, nil, fmt.Errorf("%w: gpus", ErrMissingMetrics)
	}

	numGpus := int32(metrics[0][0])
	bucket := float64(index.GetBucket(numGpus))

	index.log.Debug("Seeking host with numGPUs=%d, bucket=%d", numGpus, int32(bucket))

	return index.MultiIndex.Seek(blacklist, []float64{bucket})
}

// SeekFrom continues the seek from the position.
// SeekFrom(start interface{}, metrics ...[]float64) (host scheduling.Host, pos interface{})

// SeekMultipleFrom seeks n scheduling.Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *StaticIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}, error) {
	if len(metrics) == 0 || len(metrics[0]) == 0 {
		index.log.Error("StaticIndex::SeekMultipleFrom is missing valid metrics.")
		// panic("StaticIndex::SeekMultipleFrom requires a single valid metric to be passed in the form of the number of GPUs required for the host")

		return make([]scheduling.Host, 0), nil, fmt.Errorf("%w: gpus", ErrMissingMetrics)
	}

	numGpus := int32(metrics[0][0])
	bucket := float64(index.GetBucket(numGpus))

	index.log.Debug("Seeking %d host(s) with numGPUs=%d, bucket=%d", n, numGpus, int32(bucket))

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
	if !host.Enabled() {
		// If the host is not enabled, then it is ineligible to be added to the index.
		// In general, disabled hosts will not be attempted to be added to the index,
		// but if that happens, then we return scheduling.IndexUnqualified.
		return expectedRandomIndex, scheduling.IndexUnqualified
	}

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
	index.log.Debug("Adding host %s (ID=%s) to StaticIndex.", host.GetNodeName(), host.GetID())
	//index.MultiIndex.mu.Lock()
	//defer index.MultiIndex.mu.Unlock()
	//
	//host.SetMeta(scheduling.HostIndexCategoryMetadata, scheduling.CategoryClusterIndex)
	//
	//index.MultiIndex.FreeHosts.Enqueue(host)
	//index.MultiIndex.FreeHostsMap[host.GetID()] = host
	//
	//index.MultiIndex.Size += 1

	index.MultiIndex.Add(host)
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
