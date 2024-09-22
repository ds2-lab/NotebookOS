package scheduling

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

// BaseCluster encapsulates the core state and logic required to operate a Cluster.
type BaseCluster struct {
	instance clusterInternal

	// hosts is a map from host ID to *Host containing all the Host instances provisioned within the Cluster.
	hosts hashmap.HashMap[string, *Host]

	// indexes is a map from index key to ClusterIndexProvider containing all the indexes in the Cluster.
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

	numReplicas int

	subscriptionRatio float64

	clusterMetricsProvider metrics.ClusterMetricsProvider

	scalingOpMutex sync.Mutex
	hostMutex      sync.Mutex
}

// newBaseCluster creates a new BaseCluster struct and returns a pointer to it.
// This function is for package-internal or file-internal use only.
func newBaseCluster(gpusPerHost int, numReplicas int, clusterMetricsProvider metrics.ClusterMetricsProvider) *BaseCluster {
	cluster := &BaseCluster{
		gpusPerHost:            gpusPerHost,
		numReplicas:            numReplicas,
		clusterMetricsProvider: clusterMetricsProvider,
		subscriptionRatio:      7.0,
		hosts:                  hashmap.NewConcurrentMap[*Host](256),
		indexes:                hashmap.NewSyncMap[string, ClusterIndexProvider](),
	}
	config.InitLogger(&cluster.log, cluster)
	return cluster
}

func (c *BaseCluster) SubscriptionRatio() float64 {
	return c.subscriptionRatio
}

func (c *BaseCluster) SetSubscriptionRatio(ratio float64) {
	c.subscriptionRatio = ratio
}

// Placer returns the Placer used by the Cluster.
func (c *BaseCluster) Placer() Placer {
	return c.placer
}

// LockHosts locks the underlying host manager such that no Host instances can be added or removed.
func (c *BaseCluster) LockHosts() {
	c.hostMutex.Lock()
}

// UnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
//
// The caller must have already acquired the hostMutex or this function will fail panic.
func (c *BaseCluster) UnlockHosts() {
	c.hostMutex.Unlock()
}

// ClusterScheduler returns the ClusterScheduler used by the Cluster.
func (c *BaseCluster) ClusterScheduler() ClusterScheduler {
	return c.scheduler
}

//func (c *BaseCluster) HandleScaleInOperation(op *ScaleOperation) promise.Promise {
//	return c.instance.HandleScaleInOperation(op)
//}
//
//func (c *BaseCluster) HandleScaleOutOperation(op *ScaleOperation) promise.Promise {
//	return c.instance.HandleScaleOutOperation(op)
//}

func (c *BaseCluster) GetHostManager() hashmap.HashMap[string, *Host] {
	return c
}

func (c *BaseCluster) AddIndex(index ClusterIndexProvider) error {
	category, expected := index.Category()
	key := fmt.Sprintf("%s:%v", category, expected)
	if _, ok := c.indexes.Load(key); ok {
		return ErrDuplicatedIndexDefined
	}

	c.indexes.Store(key, index)
	return nil
}

// checkIfScalingComplete is used to check if there is an active scaling operation and,
// if there is, then to check if that operation is complete.
func (c *BaseCluster) checkIfScalingComplete(host *Host) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation == nil {
		return
	}

	if err := c.activeScaleOperation.RegisterAffectedHost(host); err != nil {
		panic(err)
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

// onHostAdded is called when a host is added to the BaseCluster.
func (c *BaseCluster) onHostAdded(host *Host) {
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

	c.checkIfScalingComplete(host)
}

// onHostRemoved is called when a host is deleted from the BaseCluster.
func (c *BaseCluster) onHostRemoved(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, hostQualificationStatus := index.IsQualified(host); hostQualificationStatus != ClusterIndexUnqualified {
			index.Remove(host)
		}
		return true
	})

	c.checkIfScalingComplete(host)
}

// ValidateCapacity ensures that the Cluster has the "right" amount of Host instances provisioned.
//
// If ValidateCapacity detects that there are too few Host instances provisioned to satisfy demand,
// then additional Host instances will be created.
//
// Alternatively, if ValidateCapacity determines that there are more Host instances provisioned than
// are truly needed, then some Host instances will be terminated to reduce unnecessary resource usage.
func (c *BaseCluster) ValidateCapacity() {
	c.scheduler.ValidateCapacity()
}

// BusyGPUs returns the number of GPUs that are actively committed to kernel replicas right now.
func (c *BaseCluster) BusyGPUs() float64 {
	busyGPUs := 0.0
	c.hosts.Range(func(_ string, host *Host) (contd bool) {
		busyGPUs += host.CommittedGPUs()
		return true
	})

	return busyGPUs
}

// DemandGPUs returns the number of GPUs that are required by all actively-running Sessions.
func (c *BaseCluster) DemandGPUs() float64 {
	panic("Not implemented")
}

////////////////////////////
// Hashmap implementation //
////////////////////////////

// Len returns the number of *Host instances in the Cluster.
func (c *BaseCluster) Len() int {
	return c.hosts.Len()
}

func (c *BaseCluster) Load(key string) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	return c.hosts.Load(key)
}

func (c *BaseCluster) Store(key string, value *Host) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.Store(key, value)
	c.onHostAdded(value)
}

func (c *BaseCluster) LoadOrStore(key string, value *Host) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, ok := c.hosts.LoadOrStore(key, value)
	if !ok {
		c.onHostAdded(value)
	}
	return host, ok
}

// CompareAndSwap is not supported in host provisioning and will always return false.
func (c *BaseCluster) CompareAndSwap(_ string, oldValue, _ *Host) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	return oldValue, false
}

func (c *BaseCluster) LoadAndDelete(key string) (*Host, bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, ok := c.hosts.LoadAndDelete(key)
	if ok {
		c.onHostRemoved(host)
	}
	return host, ok
}

func (c *BaseCluster) Delete(key string) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.LoadAndDelete(key)
}

// Range executes the provided function on each Host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BaseCluster) Range(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}

// RangeUnsafe executes the provided function on each Host in the Cluster.
// This is an alias for the Range function.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BaseCluster) RangeUnsafe(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *BaseCluster) NodeType() string {
	return c.instance.NodeType()
}

// RangeLocked executes the provided function on each Host in the Cluster.
//
// Importantly, this function DOES lock the hostsMutex.
func (c *BaseCluster) RangeLocked(f func(key string, value *Host) bool) {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	c.hosts.Range(f)
}

// ActiveScaleOperation returns the active scaling operation, if one exists.
// If there is no active scaling operation, then ActiveScaleOperation returns nil.
func (c *BaseCluster) ActiveScaleOperation() *ScaleOperation {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation
}

// IsThereAnActiveScaleOperation returns true if there is an active scaling operation taking place right now.
func (c *BaseCluster) IsThereAnActiveScaleOperation() bool {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation != nil
}

// RegisterScaleOperation registers a non-specific type of ScaleOperation.
// Specifically, whether the resulting scheduling.ScaleOperation is a ScaleOutOperation or a ScaleInOperation
// depends on how the target node count compares to the current node count.
//
// If the target node count is greater than the current node count, then a ScaleOutOperation is created,
// registered, and returned.
//
// Alternatively, if the target node count is less than the current node count, then a ScaleInOperation is created,
// registered, and returned.
func (c *BaseCluster) RegisterScaleOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	var (
		currentClusterSize = int32(c.Len())
		scaleOperation     *ScaleOperation
		err                error
	)
	if targetClusterSize > currentClusterSize {
		scaleOperation, err = NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	} else {
		scaleOperation, err = NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	}

	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != ScaleOutOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	return scaleOperation, nil
}

// RegisterScaleOutOperation registers a scale-out operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there is already an active scaling operation taking place, then an error is returned.
func (c *BaseCluster) RegisterScaleOutOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	currentClusterSize := int32(c.Len())
	scaleOperation, err := NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != ScaleOutOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	return scaleOperation, nil
}

// RegisterScaleInOperation registers a scale-in operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there already exists a scale operation with the same ID, then the existing scale operation is returned along with an error.
func (c *BaseCluster) RegisterScaleInOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleInOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	currentClusterSize := int32(c.Len())
	scaleOperation, err := NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != ScaleInOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	return scaleOperation, nil
}

// RequestHosts requests n Host instances to be launched and added to the Cluster, where n >= 1.
//
// If n is 0, then this returns immediately.
//
// If n is negative, then this returns with an error.
func (c *BaseCluster) RequestHosts(ctx context.Context, n int32) promise.Promise {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	currentNumNodes := int32(c.GetHostManager().Len())
	targetNumNodes := n + currentNumNodes

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.RegisterScaleOutOperation(opId, targetNumNodes)
	if err != nil {
		c.log.Error("Could not register new scale-out operation to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err)
	}

	// Start the operation.
	if err := scaleOp.Start(); err != nil {
		promise.Resolved(nil, status.Error(codes.Internal, err.Error())) // This really shouldn't happen.
	}

	// Wait for the shell command above to finish.
	result, err := c.handleScaleOperation(ctx, scaleOp)
	if err != nil {
		return promise.Resolved(nil, err)
	}

	return promise.Resolved(result)
}

// ReleaseSpecificHosts terminates one or more specific Host instances.
func (c *BaseCluster) ReleaseSpecificHosts(ctx context.Context, ids []string) promise.Promise {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	n := int32(len(ids))
	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < int32(c.numReplicas) {
		c.log.Error("Cannot remove %d specific Local Daemon Docker node(s) from the cluster", n)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, ErrInvalidTargetNumHosts)
	}

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.RegisterScaleInOperation(opId, targetNumNodes)
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	if err := scaleOp.Start(); err != nil {
		return promise.Resolved(nil, status.Error(codes.Internal, err.Error())) // This really shouldn't happen.
	}

	// Wait for the shell command above to finish.
	result, err := c.handleScaleOperation(ctx, scaleOp)
	if err != nil {
		return promise.Resolved(nil, err)
	}

	return promise.Resolved(result)
}

// ReleaseHosts terminates n arbitrary Host instances, where n >= 1.
//
// If n is 0, then ReleaseHosts returns immediately.
//
// If n is negative, then ReleaseHosts returns with an error.
func (c *BaseCluster) ReleaseHosts(ctx context.Context, n int32) promise.Promise {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < int32(c.numReplicas) {
		c.log.Error("Cannot remove %d Local Daemon Docker node(s) from the cluster", n)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, ErrInvalidTargetNumHosts)
	}

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.RegisterScaleInOperation(opId, targetNumNodes)
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	if err := scaleOp.Start(); err != nil {
		return promise.Resolved(nil, status.Error(codes.Internal, err.Error())) // This really shouldn't happen.
	}

	// Wait for the shell command above to finish.
	result, err := c.handleScaleOperation(ctx, scaleOp)
	if err != nil {
		return promise.Resolved(nil, err)
	}

	return promise.Resolved(result)
}

// ScaleToSize scales the Cluster to the specified number of Host instances.
//
// If n <= NUM_REPLICAS, then ScaleToSize returns with an error.
func (c *BaseCluster) ScaleToSize(ctx context.Context, n int32) promise.Promise {
	return c.instance.ScaleToSize(ctx, n)
}

// handleScaleOperation orchestrates the given scheduling.ScaleOperation.
//
// This function creates a child context from the given parent context. The child context has a timeout of 15 seconds.
//
// The resultChan is the channel used in the execScaleOp function to notify that the operation has either finished
// or resulted in an error.
//
// handleScaleOperation returns nil on success.
func (c *BaseCluster) handleScaleOperation(parentContext context.Context, scaleOp *ScaleOperation) (ScaleOperationResult, error) {
	result, err := scaleOp.Execute(parentContext)
	return result, err
}

// ClusterMetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
func (c *BaseCluster) ClusterMetricsProvider() metrics.ClusterMetricsProvider {
	return c.clusterMetricsProvider
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-out.
//
// targetNumNodes specifies the desired size of the cluster.
//
// resultChan is used to notify a waiting goroutine that the scale-out operation has finished.
func (c *BaseCluster) GetScaleOutCommand(targetNumNodes int32, resultChan chan interface{}) func() {
	return c.instance.GetScaleOutCommand(targetNumNodes, resultChan)
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-in.
//
// targetNumNodes specifies the desired size of the cluster.
//
// targetHosts specifies any specific hosts that are to be removed.
//
// resultChan is used to notify a waiting goroutine that the scale-in operation has finished.
func (c *BaseCluster) GetScaleInCommand(targetNumNodes int32, targetHosts []string, resultChan chan interface{}) func() {
	return c.instance.GetScaleInCommand(targetNumNodes, targetHosts, resultChan)
}
