package scheduling

import (
	"fmt"
	"sync"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrDuplicatedIndexDefined = fmt.Errorf("duplicated index defined")
	ErrScalingActive          = status.Error(codes.FailedPrecondition, "there is already an active scaling operation taking place")
	// ErrDuplicateScaleOperation = errors.New("scale operation with same ID already exists")
)

type ClusterIndexQualification int

const (
	CategoryClusterIndex = "BasicCluster"

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

// Cluster defines the interface for a BasicCluster that is responsible for:
// 1. Launching and terminating hosts.
// 2. Providing a global view of all hosts with multiple indexes.
// 3. Providing a statistics of the hosts.
type Cluster interface {
	// RequestHost requests a host to be launched.
	RequestHost(types.Spec) promise.Promise

	// ReleaseHost terminate a host
	ReleaseHost(id string) promise.Promise

	// GetHostManager returns the host manager of the BasicCluster.
	GetHostManager() hashmap.HashMap[string, *Host]

	// RegisterScaleOutOperation registers a scale-out operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	RegisterScaleOutOperation(string, int32) (*ScaleOperation, error)

	// RegisterScaleInOperation registers a scale-in operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	RegisterScaleInOperation(string, int32) (*ScaleOperation, error)

	// AddIndex adds an index to the BasicCluster. For each category and expected value, there can be only one index.
	AddIndex(index ClusterIndexProvider) error

	// ActiveScaleOperation returns the active scaling operation, if one exists.
	// If there is no active scaling operation, then ActiveScaleOperation returns nil.
	ActiveScaleOperation() *ScaleOperation

	// IsThereAnActiveScaleOperation returns true if there is an active scaling operation taking place right now.
	IsThereAnActiveScaleOperation() bool

	// ClusterScheduler returns the ClusterScheduler used by the Cluster.
	ClusterScheduler() ClusterScheduler

	// Placer returns the Placer used by the Cluster.
	Placer() Placer

	// LockHosts locks the underlying host manager such that no Host instances can be added or removed.
	LockHosts()

	// UnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
	UnlockHosts()
}

type BasicCluster struct {
	// hosts is a map from host ID to *Host containing all the Host instances provisioned within the Cluster.
	hosts hashmap.HashMap[string, *Host]

	// indexes is a map from index key to ClusterIndexProvider containing all the indexes in the cluster.
	indexes hashmap.BaseHashMap[string, ClusterIndexProvider]

	// activeScaleOperation is a reference to the currently-active scale-out/scale-in operation.
	// There may only be one scaling operation active at any given time.
	// If activeScaleOperation is nil, then there is no active scale-out or scale-in operation.
	activeScaleOperation *ScaleOperation

	// gpusPerHost is the number of GPUs available on each host.
	gpusPerHost int

	scheduler ClusterScheduler

	placer Placer

	log logger.Logger

	scalingOpMutex sync.Mutex
	hostMutex      sync.Mutex
}

// newBaseCluster creates a new BasicCluster struct and returns a pointer to it.
// This function is for package-internal or file-internal use only.
func newBaseCluster(gpusPerHost int) *BasicCluster {
	cluster := &BasicCluster{
		gpusPerHost: gpusPerHost,
		hosts:       hashmap.NewConcurrentMap[*Host](256),
		indexes:     hashmap.NewSyncMap[string, ClusterIndexProvider](),
	}
	config.InitLogger(&cluster.log, cluster)
	return cluster
}

// NewDockerCluster creates a new BasicCluster struct and returns a pointer to it.
//
// NewDockerCluster should be used when the system is deployed in Docker mode (either compose or swarm, for now).
// This function accepts parameters that are used to construct a DockerScheduler to be used internally
// by the Cluster for scheduling decisions.
func NewDockerCluster(gatewayDaemon ClusterGateway, opts *ClusterSchedulerOptions) *BasicCluster {
	cluster := newBaseCluster(opts.GpusPerHost)

	placer, err := NewRandomPlacer(cluster, opts)
	if err != nil {
		cluster.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}
	cluster.placer = placer

	scheduler, err := NewDockerScheduler(gatewayDaemon, cluster, placer, opts)
	if err != nil {
		cluster.log.Error("Failed to create Kubernetes Cluster Scheduler: %v", err)
		panic(err)
	}

	cluster.scheduler = scheduler

	return cluster
}

// NewKubernetesCluster creates a new BasicCluster struct and returns a pointer to it.
//
// NewKubernetesCluster should be used when the system is deployed in Kubernetes mode.
// This function accepts parameters that are used to construct a KubernetesScheduler to be used internally
// by the Cluster for scheduling decisions and to respond to scheduling requests by the Kubernetes Scheduler.
func NewKubernetesCluster(gatewayDaemon ClusterGateway, kubeClient KubeClient, opts *ClusterSchedulerOptions) *BasicCluster {
	cluster := newBaseCluster(opts.GpusPerHost)

	placer, err := NewRandomPlacer(cluster, opts)
	if err != nil {
		cluster.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}
	cluster.placer = placer

	scheduler, err := NewKubernetesScheduler(gatewayDaemon, cluster, placer, kubeClient, opts)
	if err != nil {
		cluster.log.Error("Failed to create Kubernetes Cluster Scheduler: %v", err)
		panic(err)
	}

	cluster.scheduler = scheduler

	return cluster
}

// Placer returns the Placer used by the Cluster.
func (c *BasicCluster) Placer() Placer {
	return c.placer
}

// LockHosts locks the underlying host manager such that no Host instances can be added or removed.
func (c *BasicCluster) LockHosts() {
	c.hostMutex.Lock()
}

// UnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
//
// The caller must have already acquired the hostMutex or this function will fail panic.
func (c *BasicCluster) UnlockHosts() {
	c.hostMutex.Unlock()
}

// ClusterScheduler returns the ClusterScheduler used by the Cluster.
func (c *BasicCluster) ClusterScheduler() ClusterScheduler {
	return c.scheduler
}

// ActiveScaleOperation returns the active scaling operation, if one exists.
// If there is no active scaling operation, then ActiveScaleOperation returns nil.
func (c *BasicCluster) ActiveScaleOperation() *ScaleOperation {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation
}

// IsThereAnActiveScaleOperation returns true if there is an active scaling operation taking place right now.
func (c *BasicCluster) IsThereAnActiveScaleOperation() bool {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation != nil
}

// RegisterScaleOutOperation registers a scale-out operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there is already an active scaling operation taking place, then an error is returned.
func (c *BasicCluster) RegisterScaleOutOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	currentClusterSize := c.Len()
	scaleOperation, err := NewScaleOperation(operationId, currentClusterSize, targetClusterSize)
	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != ScaleOutOperation {
		return nil, fmt.Errorf("%w: cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	// if existingScaleOperation, loaded := c.scaleOperations.LoadOrStore(operationId, scaleOperation); loaded {
	// 	return existingScaleOperation, ErrDuplicateScaleOperation
	// }

	return scaleOperation, nil
}

// RegisterScaleInOperation registers a scale-in operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there already exists a scale operation with the same ID, then the existing scale operation is returned along with an error.
func (c *BasicCluster) RegisterScaleInOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleInOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	currentClusterSize := c.Len()
	scaleOperation, err := NewScaleOperation(operationId, currentClusterSize, targetClusterSize)
	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != ScaleInOperation {
		return nil, fmt.Errorf("%w: cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	// if existingScaleOperation, loaded := c.scaleOperations.LoadOrStore(operationId, scaleOperation); loaded {
	// 	return existingScaleOperation, ErrDuplicateScaleOperation
	// }

	return scaleOperation, nil
}

func (c *BasicCluster) RequestHost(spec types.Spec) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *BasicCluster) ReleaseHost(id string) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *BasicCluster) GetHostManager() hashmap.HashMap[string, *Host] {
	return c
}

func (c *BasicCluster) AddIndex(index ClusterIndexProvider) error {
	category, expected := index.Category()
	key := fmt.Sprintf("%s:%v", category, expected)
	if _, ok := c.indexes.Load(key); ok {
		return ErrDuplicatedIndexDefined
	}

	c.indexes.Store(key, index)
	return nil
}

// checkIfScalingComplete is used to check if there is an active scaling operation and, if there is, then to check
// if that operation is complete.
func (c *BasicCluster) checkIfScalingComplete() {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation == nil {
		return
	}

	if int32(c.hosts.Len()) == c.activeScaleOperation.TargetScale {
		c.log.Debug("%s %s has completed (target scale = %d).", c.activeScaleOperation.OperationType, c.activeScaleOperation.OperationId, c.activeScaleOperation.TargetScale)
		err := c.activeScaleOperation.SetOperationFinished()
		if err != nil {
			c.log.Error("Failed to mark active %s %s as finished because: %v", c.activeScaleOperation.OperationType, c.activeScaleOperation.OperationId, err)
		}
		c.activeScaleOperation.NotificationChan <- struct{}{}
		c.activeScaleOperation = nil
	} else {
		c.log.Debug("%s %s has completed (current scale = %d, target scale = %d).", c.activeScaleOperation.OperationType, c.activeScaleOperation.OperationId, c.hosts.Len(), c.activeScaleOperation.TargetScale)
	}
}

// onHostAdded is called when a host is added to the BasicCluster.
func (c *BasicCluster) onHostAdded(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, qualificationStatus := index.IsQualified(host); qualificationStatus == ClusterIndexNewQualified {
			c.log.Debug("Adding new host to index: %v", host)
			index.Add(host)
		} else if qualificationStatus == ClusterIndexQualified {
			c.log.Debug("Updating existing host within index: %v", host)
			index.Update(host)
		} else if qualificationStatus == ClusterIndexDisqualified {
			c.log.Debug("Removing existing host from index in onHostAdded: %v", host)
			index.Remove(host)
		} // else unqualified
		return true
	})

	c.checkIfScalingComplete()
}

// onHostRemoved is called when a host is deleted from the BasicCluster.
func (c *BasicCluster) onHostRemoved(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, status := index.IsQualified(host); status != ClusterIndexUnqualified {
			index.Remove(host)
		}
		return true
	})

	c.checkIfScalingComplete()
}

// ValidateCapacity ensures that the Cluster has the "right" amount of Host instances provisioned.
//
// If ValidateCapacity detects that there are too few Host instances provisioned to satisfy demand,
// then additional Host instances will be created.
//
// Alternatively, if ValidateCapacity determines that there are more Host instances provisioned than
// are truly needed, then some Host instances will be terminated to reduce unnecessary resource usage.
func (c *BasicCluster) ValidateCapacity() {
	c.scheduler.ValidateCapacity()
}

// BusyGPUs returns the number of GPUs that are actively committed to kernel replicas right now.
//
// If 'forceUpdate' is true, then the cached/local information is invalidated and updated by querying the
// Local Daemons.
func (c *BasicCluster) BusyGPUs(forceUpdate bool) float64 {
	if forceUpdate {
		panic("Not supported.")
	}

	busyGPUs := 0.0
	c.hosts.Range(func(_ string, host *Host) (contd bool) {
		busyGPUs += host.CommittedGPUs()
		return true
	})

	return busyGPUs
}

////////////////////////////
// Hashmap implementation //
////////////////////////////

// Len returns the number of *Host instances in the Cluster.
func (c *BasicCluster) Len() int {
	return c.hosts.Len()
}

func (c *BasicCluster) Load(key string) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	return c.hosts.Load(key)
}

func (c *BasicCluster) Store(key string, value *Host) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.Store(key, value)
	c.onHostAdded(value)
}

func (c *BasicCluster) LoadOrStore(key string, value *Host) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, ok := c.hosts.LoadOrStore(key, value)
	if !ok {
		c.onHostAdded(value)
	}
	return host, ok
}

// CompareAndSwap is not supported in host provisioning and will always return false.
func (c *BasicCluster) CompareAndSwap(_ string, oldValue, _ *Host) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	return oldValue, false
}

func (c *BasicCluster) LoadAndDelete(key string) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, ok := c.hosts.LoadAndDelete(key)
	if ok {
		c.onHostRemoved(host)
	}
	return host, ok
}

func (c *BasicCluster) Delete(key string) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.LoadAndDelete(key)
}

// Range executes the provided function on each Host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BasicCluster) Range(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}

// RangeUnsafe executes the provided function on each Host in the Cluster.
// This is an alias for the Range function.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BasicCluster) RangeUnsafe(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}

// RangeLocked executes the provided function on each Host in the Cluster.
//
// Importantly, this function DOES lock the hostsMutex.
func (c *BasicCluster) RangeLocked(f func(key string, value *Host) bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.Range(f)
}
