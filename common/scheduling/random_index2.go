package scheduling

//
//import (
//	"github.com/Scusemua/go-utils/config"
//	"github.com/Scusemua/go-utils/logger"
//	"math/rand"
//	"sync"
//	"sync/atomic"
//)
//
//type RandomClusterIndex struct {
//	hostIndexPermutation []int
//	numShuffles          atomic.Int32
//	hosts                map[int]*Host
//
//	mu  sync.Mutex
//	log logger.Logger
//}
//
//func NewRandomClusterIndex() *RandomClusterIndex {
//	index := &RandomClusterIndex{
//		hosts: make(map[int]*Host),
//	}
//	index.numShuffles.Store(0)
//
//	config.InitLogger(&index.log, index)
//
//	return index
//}
//
//// NumReshuffles returns the number of times that this index has reshuffled its internal permutation.
//func (index *RandomClusterIndex) NumReshuffles() int32 {
//	return index.numShuffles.Load()
//}
//
//func (index *RandomClusterIndex) Len() int {
//	return len(index.hosts)
//}
//
//func (index *RandomClusterIndex) Category() (string, interface{}) {
//	return CategoryClusterIndex, expectedRandomIndex
//}
//
//// Contains returns true if the given index is already contained within the target RandomClusterIndex.
//func (index *RandomClusterIndex) Contains(host *Host) bool {
//	index.mu.Lock()
//	defer index.mu.Unlock()
//
//	return index.unsafeContains(host)
//}
//
//func (index *RandomClusterIndex) unsafeContains(host *Host) bool {
//	randomIndexMetaVal := host.GetMeta(HostMetaRandomIndex)
//	if randomIndexMetaVal == nil {
//		return false
//	}
//
//	randomIndexMeta := randomIndexMetaVal.(int)
//	_, ok := index.hosts[randomIndexMeta]
//	return ok
//}
//
//// IsQualified returns true if the given Host is qualified to be added to the target RandomClusterIndex.
////
//// All Host instances are qualified for a RandomClusterIndex, so IsQualified simply checks if the Host
//// is already contained within the target RandomClusterIndex. If so, then ClusterIndexQualified is returned.
//// If the Host is not contained within the target RandomClusterIndex, then ClusterIndexNewQualified is returned.
//func (index *RandomClusterIndex) IsQualified(host *Host) (interface{}, ClusterIndexQualification) {
//	index.mu.Lock()
//	defer index.mu.Unlock()
//
//	if index.unsafeContains(host) {
//		return expectedRandomIndex, ClusterIndexQualified
//	}
//
//	return expectedRandomIndex, ClusterIndexNewQualified
//}
//
//// unsafeReshuffleHostPermutation reshuffles the target RandomClusterIndex's host permutation.
//func (index *RandomClusterIndex) unsafeReshuffleHostPermutation() {
//	// We'll reuse the existing slice if we can.
//	if len(index.hosts) == len(index.hostIndexPermutation) {
//
//	}
//
//	index.perm = rand.Perm(len(index.hosts))
//	index.seekStart = 0
//	index.numShuffles.Add(1)
//}
//
//func GetHostMetaRandomIndex(host *Host) {
//	randomIndexMetaVal := host.GetMeta(HostMetaRandomIndex)
//	if randomIndexMetaVal == nil {
//		return -1
//	} else {
//		return
//	}
//}
//
//func (index *RandomClusterIndex) AddHost(host *Host) {
//	index.mu.Lock()
//	defer index.mu.Unlock()
//
//	randomIndexMetaVal := host.GetMeta(HostMetaRandomIndex)
//	if randomIndexMetaVal == nil {
//		return false
//	}
//
//	index.hosts[host.ID] = host
//	index.unsafeReshuffleHostPermutation()
//}
