package index

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// MultiIndex manages a collection of sub-indices.
type MultiIndex struct {
	*LeastLoadedIndex
	numGpus    int32
	identifier string
}

func NewMultiIndex(numGpus int32) *MultiIndex {
	index := &MultiIndex{
		LeastLoadedIndex: NewLeastLoadedIndex(),
		numGpus:          numGpus,
		identifier:       fmt.Sprintf("%d-GPU Pool", numGpus),
	}

	config.InitLogger(&index.log, index)

	return index
}

func (index *MultiIndex) Len() int {
	return index.hosts.Len()
}

func (index *MultiIndex) Add(host scheduling.Host) {
	index.LeastLoadedIndex.Add(host)
	host.SetMeta(scheduling.HostIndexKeyMetadata, index.identifier)
}

func (index *MultiIndex) unsafeAddBack(host scheduling.Host) {
	index.LeastLoadedIndex.unsafeAddBack(host)
}

func (index *MultiIndex) Update(host scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.LeastLoadedIndex.unsafeUpdate(host, index.identifier)
}

func (index *MultiIndex) UpdateMultiple(hosts []scheduling.Host) {
	index.mu.Lock()
	defer index.mu.Unlock()

	index.LeastLoadedIndex.unsafeUpdateMultiple(hosts, index.identifier)
}

func (index *MultiIndex) Remove(host scheduling.Host) {
	index.LeastLoadedIndex.Remove(host)
}

func (index *MultiIndex) GetMetrics(host scheduling.Host) []float64 {
	return index.LeastLoadedIndex.GetMetrics(host)
}

func (index *MultiIndex) Category() (string, interface{}) {
	return scheduling.CategoryGandivaPoolIndex, index.identifier
}

func (index *MultiIndex) IsQualified(host scheduling.Host) (interface{}, scheduling.IndexQualification) {
	val := host.GetMeta(LeastLoadedIndexMetadataKey)
	if val == nil {
		// The only time a host is qualified is if we're adding it explicitly within the Gandiva scheduler,
		// or if the host is already present in the index.
		return index.identifier, scheduling.IndexUnqualified
	}

	if _, ok := val.(int32); ok {
		// The host is already present in the index.
		return index.identifier, scheduling.IndexQualified
	} else {
		// The only time a host is qualified is if we're adding it explicitly within the Gandiva scheduler,
		// or if the host is already present in the index.
		return index.identifier, scheduling.IndexUnqualified
	}
}

// unsafeSeek does the actual work of the Seek method.
// unsafeSeek does not acquire the mutex. It should be called from a function that has already acquired the mutex.
func (index *MultiIndex) unsafeSeek(blacklistArg []interface{}) scheduling.Host {
	return index.LeastLoadedIndex.unsafeSeek(blacklistArg)
}

func (index *MultiIndex) Seek(blacklist []interface{}, metrics ...[]float64) (scheduling.Host, interface{}) {
	return index.LeastLoadedIndex.Seek(blacklist, metrics...)
}

// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
// Pass nil as pos to reset the seek.
//
// This entire method is thread-safe. The index is locked until this method returns.
func (index *MultiIndex) SeekMultipleFrom(pos interface{}, n int, criteriaFunc scheduling.HostCriteriaFunction,
	blacklist []interface{}, metrics ...[]float64) ([]scheduling.Host, interface{}) {

	return index.LeastLoadedIndex.SeekMultipleFrom(pos, n, criteriaFunc, blacklist, metrics...)
}
