package scheduling

import (
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"sync"
)

type StaticClusterIndex struct {
	hosts  []Host
	length int

	mu sync.Mutex

	log logger.Logger
}

func NewStaticClusterIndex() *StaticClusterIndex {
	index := &StaticClusterIndex{
		hosts:  make([]Host, 0),
		length: 0,
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
func (index *StaticClusterIndex) IsQualified(host Host) (actual interface{}, qualified ClusterIndexQualification) {
	// First, verify that the host is in the index.
	if _, ok := host.GetMeta(HostMetaRandomIndex).(int32); !ok {

	}
}

// Len returns the number of hosts in the index.
func (index *StaticClusterIndex) Len() int {
	return index.length
}

// Add adds a host to the index.
func (index *StaticClusterIndex) Add(Host) {

}

// Update updates a host in the index.
func (index *StaticClusterIndex) Update(Host) {

}

// Remove removes a host from the index.
func (index *StaticClusterIndex) Remove(Host) {

}

// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
func (index *StaticClusterIndex) GetMetrics(Host) (metrics []float64) {

}

// // // // // // // // // // // // // //
// ClusterIndexQuerier implementation  //
// // // // // // // // // // // // // //

// Seek returns the host specified by the metrics.
func (index *StaticClusterIndex) Seek(metrics ...[]float64) (host Host, pos interface{}) {

}

// SeekFrom continues the seek from the position.
func (index *StaticClusterIndex) SeekFrom(start interface{}, metrics ...[]float64) (host Host, pos interface{}) {

}
