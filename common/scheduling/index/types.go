package index

import "github.com/zhangjyr/distributed-notebook/common/scheduling/entity"

type ClusterIndexQualification int

type HostMetaKey string

const (
	CategoryClusterIndex = "BaseCluster"

	// ClusterIndexDisqualified indicates that the host has been indexed and unqualified now.
	ClusterIndexDisqualified ClusterIndexQualification = -1
	// ClusterIndexUnqualified indicates that the host is not qualified.
	ClusterIndexUnqualified ClusterIndexQualification = 0
	// ClusterIndexQualified indicates that the host has been indexed and is still qualified.
	ClusterIndexQualified ClusterIndexQualification = 1
	// ClusterIndexNewQualified indicates that the host is newly qualified and should be indexed.
	ClusterIndexNewQualified ClusterIndexQualification = 2
)

type ClusterIndexProvider interface {
	// Category returns the category of the index and the expected value.
	Category() (category string, expected interface{})

	// IsQualified returns the actual value according to the index category and whether the host is qualified.
	// An index provider must be able to track indexed hosts and indicate disqualification.
	IsQualified(*entity.Host) (actual interface{}, qualified ClusterIndexQualification)

	// Len returns the number of hosts in the index.
	Len() int

	// Add adds a host to the index.
	Add(*entity.Host)

	// Update updates a host in the index.
	Update(*entity.Host)

	// Remove removes a host from the index.
	Remove(*entity.Host)

	// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
	GetMetrics(*entity.Host) (metrics []float64)
}

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
