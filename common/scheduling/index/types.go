package index

import "github.com/zhangjyr/distributed-notebook/common/scheduling/entity"

type HostMetaKey string

// HostCriteriaFunction is used by a ClusterIndexQuerier, specifically in its implementation of
// SeekMultipleFrom, to determine if a Host will be considered viable by the Caller before returning it.
//
// A HostCriteriaFunction accepts a Host as an argument and returns a boolean indicating whether the Host
// is viable (true) or not (false) based on whatever criteria are defined and implemented within the
// HostCriteriaFunction function body.
type HostCriteriaFunction func(*entity.Host) bool

type ClusterIndexQuerier interface {
	// Seek returns the host specified by the metrics.
	Seek(blacklist []interface{}, metrics ...[]float64) (host *entity.Host, pos interface{})

	// SeekFrom continues the seek from the position.
	SeekFrom(start interface{}, metrics ...[]float64) (host *entity.Host, pos interface{})

	// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
	// Pass nil as pos to reset the seek.
	//
	// This entire method is thread-safe. The index is locked until this method returns.
	SeekMultipleFrom(pos interface{}, n int, criteriaFunc HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]*entity.Host, interface{})
}

type ClusterIndex interface {
	ClusterIndexProvider
	ClusterIndexQuerier
}
