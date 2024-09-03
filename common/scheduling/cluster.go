package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
)

var (
	ErrDuplicatedIndexDefined = fmt.Errorf("duplicated index defined")
)

type ClusterIndexQualification int

const (
	CategoryClusterIndex = "cluster"

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
	IsQualified(*Host) (actual interface{}, qualified ClusterIndexQualification)

	// Len returns the number of hosts in the index.
	Len() int

	// Add adds a host to the index.
	Add(*Host)

	// Update updates a host in the index.
	Update(*Host)

	// Remove removes a host from the index.
	Remove(*Host)

	// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
	GetMetrics(*Host) (metrics []float64)
}

type ClusterIndexQuerier interface {
	// Seek returns the host specified by the metrics.
	Seek(metrics ...[]float64) (host *Host, pos interface{})

	// SeekFrom continues the seek from the position.
	SeekFrom(start interface{}, metrics ...[]float64) (host *Host, pos interface{})
}

type ClusterIndex interface {
	ClusterIndexProvider
	ClusterIndexQuerier
}

// Cluster defines the interface for a cluster that is responsible for:
// 1. Launching and terminating hosts.
// 2. Providing a global view of all hosts with multiple indexes.
// 3. Providing a statistics of the hosts.
type Cluster interface {
	// RequestHost requests a host to be launched.
	RequestHost(types.Spec) promise.Promise

	// ReleaseHost terminate a host
	ReleaseHost(id string) promise.Promise

	// GetHostManager returns the host manager of the cluster.
	GetHostManager() hashmap.HashMap[string, *Host]

	// AddIndex adds an index to the cluster. For each category and expected value, there can be only one index.
	AddIndex(index ClusterIndexProvider) error
}

type cluster struct {
	hosts   hashmap.HashMap[string, *Host]
	indexes hashmap.BaseHashMap[string, ClusterIndexProvider]
}

func NewCluster() Cluster {
	return &cluster{
		hosts:   hashmap.NewConcurrentMap[*Host](256),
		indexes: hashmap.NewSyncMap[string, ClusterIndexProvider](),
	}
}

func (c *cluster) RequestHost(spec types.Spec) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *cluster) ReleaseHost(id string) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *cluster) GetHostManager() hashmap.HashMap[string, *Host] {
	return c
}

func (c *cluster) AddIndex(index ClusterIndexProvider) error {
	category, expected := index.Category()
	key := fmt.Sprintf("%s:%v", category, expected)
	if _, ok := c.indexes.Load(key); ok {
		return ErrDuplicatedIndexDefined
	}

	c.indexes.Store(key, index)
	return nil
}

// onUpdate is called when a host is added to the cluster.
func (c *cluster) onUpdate(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, status := index.IsQualified(host); status == ClusterIndexNewQualified {
			index.Add(host)
		} else if status == ClusterIndexQualified {
			index.Update(host)
		} else if status == ClusterIndexDisqualified {
			index.Remove(host)
		} // else unqualified
		return true
	})
}

// onDelete is called when a host is deleted from the cluster.
func (c *cluster) onDelete(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, status := index.IsQualified(host); status != ClusterIndexUnqualified {
			index.Remove(host)
		}
		return true
	})
}

// Hashmap implementation

// Len returns the number of *Host instances in the Cluster.
func (c *cluster) Len() int {
	return c.hosts.Len()
}

func (c *cluster) Load(key string) (*Host, bool) {
	return c.hosts.Load(key)
}

func (c *cluster) Store(key string, value *Host) {
	c.hosts.Store(key, value)
	c.onUpdate(value)
}

func (c *cluster) LoadOrStore(key string, value *Host) (*Host, bool) {
	host, ok := c.hosts.LoadOrStore(key, value)
	if !ok {
		c.onUpdate(value)
	}
	return host, ok
}

// CompareAndSwap is not supported in host provisioning and will always return false.
func (c *cluster) CompareAndSwap(key string, oldValue, newValue *Host) (*Host, bool) {
	return oldValue, false
}

func (c *cluster) LoadAndDelete(key string) (*Host, bool) {
	host, ok := c.hosts.LoadAndDelete(key)
	if ok {
		c.onDelete(host)
	}
	return host, ok
}

func (c *cluster) Delete(key string) {
	c.hosts.LoadAndDelete(key)
}

func (c *cluster) Range(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}
