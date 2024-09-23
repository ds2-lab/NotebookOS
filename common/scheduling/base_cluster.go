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
	scaleOperationCond   *sync.Cond

	// gpusPerHost is the number of GPUs available on each host.
	gpusPerHost int

	scheduler ClusterScheduler

	placer Placer

	log logger.Logger

	numReplicas int

	subscriptionRatio float64

	clusterMetricsProvider metrics.ClusterMetricsProvider

	// This is also used as the sync.Locker for the scaleOperationCond.
	scalingOpMutex sync.Mutex
	hostMutex      sync.RWMutex
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
	cluster.scaleOperationCond = sync.NewCond(&cluster.scalingOpMutex)
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

// ReadLockHosts locks the underlying host manager such that no Host instances can be added or removed.
func (c *BaseCluster) ReadLockHosts() {
	c.hostMutex.RLock()
}

// ReadUnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
//
// The caller must have already acquired the hostMutex or this function will fail panic.
func (c *BaseCluster) ReadUnlockHosts() {
	c.hostMutex.RUnlock()
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

//func (c *BaseCluster) GetHostManager() hashmap.HashMap[string, *Host] {
//	return c
//}

func (c *BaseCluster) AddIndex(index ClusterIndexProvider) error {
	category, expected := index.Category()
	key := fmt.Sprintf("%s:%v", category, expected)
	if _, ok := c.indexes.Load(key); ok {
		return ErrDuplicatedIndexDefined
	}

	c.indexes.Store(key, index)
	return nil
}

// unsafeCheckIfScaleOperationIsComplete is used to check if there is an active scaling operation and,
// if there is, then to check if that operation is complete.
func (c *BaseCluster) unsafeCheckIfScaleOperationIsComplete(host *Host) {
	if c.activeScaleOperation == nil {
		return
	}

	activeScaleOp := c.activeScaleOperation
	c.log.Debug("Checking if %s %s is finished...", activeScaleOp.OperationType, activeScaleOp.OperationId)

	if err := c.activeScaleOperation.RegisterAffectedHost(host); err != nil {
		panic(err)
	}

	if int32(c.hosts.Len()) == activeScaleOp.TargetScale {
		c.log.Debug("%s %s has finished (target scale = %d).",
			activeScaleOp.OperationType, activeScaleOp.OperationId, activeScaleOp.TargetScale)
		err := activeScaleOp.SetOperationFinished()
		if err != nil {
			c.log.Error("Failed to mark active %s %s as finished because: %v",
				activeScaleOp.OperationType, activeScaleOp.OperationId, err)
		}
		activeScaleOp.NotificationChan <- struct{}{}
		activeScaleOp = nil
		c.scaleOperationCond.Broadcast()
	} else {
		c.log.Debug("%s %s is not yet finished (current scale = %d, target scale = %d). Only %d/%d affected hosts have been registered.",
			activeScaleOp.OperationType, activeScaleOp.OperationId, c.Len(), activeScaleOp.TargetScale,
			len(activeScaleOp.NodesAffected), activeScaleOp.ExpectedNumAffectedNodes)
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

	c.unsafeCheckIfScaleOperationIsComplete(host)
}

// onHostRemoved is called when a host is deleted from the BaseCluster.
func (c *BaseCluster) onHostRemoved(host *Host) {
	c.indexes.Range(func(key string, index ClusterIndexProvider) bool {
		if _, hostQualificationStatus := index.IsQualified(host); hostQualificationStatus != ClusterIndexUnqualified {
			index.Remove(host)
		}
		return true
	})

	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()
	c.unsafeCheckIfScaleOperationIsComplete(host)
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

// NumReplicas returns the numer of replicas that each Jupyter kernel has associated with it.
// This is typically equal to 3, but may be altered in the system configuration.
func (c *BaseCluster) NumReplicas() int {
	return c.numReplicas
}

// RangeOverHosts executes the provided function on each Host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BaseCluster) RangeOverHosts(f func(key string, value *Host) bool) {
	c.hosts.Range(f)
}

// RemoveHost removes the Host with the specified ID.
func (c *BaseCluster) RemoveHost(hostId string) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	c.hostMutex.Lock()
	removedHost, loaded := c.hosts.LoadAndDelete(hostId)
	c.hostMutex.Unlock()

	if loaded {
		c.onHostRemoved(removedHost)
	}
}

////////////////////////////
// Hashmap implementation //
////////////////////////////

// Len returns the number of *Host instances in the Cluster.
func (c *BaseCluster) Len() int {
	return c.hosts.Len()
}

//func (c *BaseCluster) Load(key string) (*Host, bool) {
//	c.hostMutex.RLock()
//	defer c.hostMutex.RUnlock()
//
//	return c.hosts.Load(key)
//}

//func (c *BaseCluster) Store(key string, value *Host) {
//	log.Fatalf("The Store method should not be called directly. Arguments[key=%s, host=%v]", key, value)
//}

//func (c *BaseCluster) LoadOrStore(key string, value *Host) (*Host, bool) {
//	c.hostMutex.Lock()
//	defer c.hostMutex.Unlock()
//
//	host, ok := c.hosts.LoadOrStore(key, value)
//	if !ok {
//		c.onHostAdded(value)
//	}
//	return host, ok
//}

// CompareAndSwap is not supported in host provisioning and will always return false.
//func (c *BaseCluster) CompareAndSwap(_ string, oldValue, _ *Host) (*Host, bool) {
//	c.hostMutex.Lock()
//	defer c.hostMutex.Unlock()
//
//	return oldValue, false
//}

//func (c *BaseCluster) LoadAndDelete(key string) (*Host, bool) {
//	c.hostMutex.Lock()
//	defer c.hostMutex.Unlock()
//
//	host, ok := c.hosts.LoadAndDelete(key)
//	if ok {
//		c.onHostRemoved(host)
//	}
//	return host, ok
//}

//func (c *BaseCluster) Delete(key string) {
//	c.hostMutex.Lock()
//	defer c.hostMutex.Unlock()
//
//	c.hosts.LoadAndDelete(key)
//}

// Range executes the provided function on each Host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
//func (c *BaseCluster) Range(f func(key string, value *Host) bool) {
//	c.hosts.Range(f)
//}

// RangeUnsafe executes the provided function on each Host in the Cluster.
// This is an alias for the Range function.
//
// Importantly, this function does NOT lock the hostsMutex.
//func (c *BaseCluster) RangeUnsafe(f func(key string, value *Host) bool) {
//	c.hosts.Range(f)
//}

// RangeLocked executes the provided function on each Host in the Cluster.
//
// Importantly, this function DOES lock the hostsMutex.
//func (c *BaseCluster) RangeLocked(f func(key string, value *Host) bool) {
//	c.hostMutex.RLock()
//	defer c.hostMutex.RUnlock()
//
//	c.hosts.Range(f)
//}

// RegisterScaleOperation registers a non-specific type of ScaleOperation.
// Specifically, whether the resulting scheduling.ScaleOperation is a ScaleOutOperation or a ScaleInOperation
// depends on how the target node count compares to the current node count.
//
// If the target node count is greater than the current node count, then a ScaleOutOperation is created,
// registered, and returned.
//
// Alternatively, if the target node count is less than the current node count, then a ScaleInOperation is created,
// registered, and returned.
func (c *BaseCluster) registerScaleOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
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

	c.activeScaleOperation = scaleOperation
	return scaleOperation, nil
}

// RegisterScaleOutOperation registers a scale-out operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there is already an active scaling operation taking place, then an error is returned.
func (c *BaseCluster) registerScaleOutOperation(operationId string, targetClusterSize int32) (*ScaleOperation, error) {
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

	c.activeScaleOperation = scaleOperation
	return scaleOperation, nil
}

// RegisterScaleInOperation registers a scale-in operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there already exists a scale operation with the same ID, then the existing scale operation is returned along with an error.
func (c *BaseCluster) registerScaleInOperation(operationId string, targetClusterSize int32, targetHosts []string) (*ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleInOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	var (
		currentClusterSize = int32(c.Len())
		scaleOperation     *ScaleOperation
		err                error
	)
	if len(targetHosts) > 0 {
		scaleOperation, err = NewScaleInOperationWithTargetHosts(operationId, currentClusterSize, targetHosts, c.instance)
		if err != nil {
			return nil, err
		}
	} else {
		scaleOperation, err = NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
		if err != nil {
			return nil, err
		}
	}

	if scaleOperation.OperationType != ScaleInOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	c.activeScaleOperation = scaleOperation
	return scaleOperation, nil
}

// RequestHosts requests n Host instances to be launched and added to the Cluster, where n >= 1.
//
// If n is 0, then this returns immediately.
//
// If n is negative, then this returns with an error.
func (c *BaseCluster) RequestHosts(ctx context.Context, n int32) promise.Promise {
	currentNumNodes := int32(c.Len())
	targetNumNodes := n + currentNumNodes

	c.log.Debug("Received request for %d additional host(s). Current scale: %d. Target scale: %d.",
		n, currentNumNodes, targetNumNodes)

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.registerScaleOutOperation(opId, targetNumNodes)
	if err != nil {
		c.log.Error("Could not register new scale-out operation to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err)
	}

	c.log.Debug("Beginning scale-out from %d nodes to %d nodes.", scaleOp.InitialScale, scaleOp.TargetScale)

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Debug("Scale-out from %d nodes to %d nodes failed because: %v",
			scaleOp.InitialScale, scaleOp.TargetScale, err)
		promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug("Scale-out from %d nodes to %d nodes succeeded.")
	return promise.Resolved(result)
}

// ReleaseSpecificHosts terminates one or more specific Host instances.
func (c *BaseCluster) ReleaseSpecificHosts(ctx context.Context, ids []string) promise.Promise {
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
	scaleOp, err := c.registerScaleInOperation(opId, targetNumNodes, ids)
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	return promise.Resolved(result)
}

// ReleaseHosts terminates n arbitrary Host instances, where n >= 1.
//
// If n is 0, then ReleaseHosts returns immediately.
//
// If n is negative, then ReleaseHosts returns with an error.
func (c *BaseCluster) ReleaseHosts(ctx context.Context, n int32) promise.Promise {
	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < int32(c.numReplicas) {
		c.log.Error("Cannot remove %d Local Daemon Docker node(s) from the cluster", n)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, ErrInvalidTargetNumHosts)
	}

	c.log.Debug("Will attempt to release %d nodes. Current scale: %d. Target scale: %d.",
		n, currentNumNodes, targetNumNodes)

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.registerScaleInOperation(opId, targetNumNodes, []string{})
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	return promise.Resolved(result)
}

// ScaleToSize scales the Cluster to the specified number of Host instances.
//
// If n <= NUM_REPLICAS, then ScaleToSize returns with an error.
func (c *BaseCluster) ScaleToSize(ctx context.Context, n int32) promise.Promise {
	return c.instance.ScaleToSize(ctx, n)
}

// ClusterMetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
func (c *BaseCluster) ClusterMetricsProvider() metrics.ClusterMetricsProvider {
	return c.clusterMetricsProvider
}

// NewHostConnected should be called by an external entity when a new Host connects to the Cluster Gateway.
// NewHostConnected handles the logic of adding the Host to the Cluster, and in particular will handle the
// task of locking the required structures during scaling operations.
func (c *BaseCluster) NewHostConnected(host *Host) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	c.hostMutex.Lock()
	// The host mutex is already locked if we're performing a scaling operation.
	c.hosts.Store(host.ID, host)
	c.hostMutex.Unlock()

	c.onHostAdded(host)

	c.log.Debug("Finished handling scheduling.Cluster-level registration of newly-connected host %s", host.ID)
}

// GetHost returns the Host with the given ID, if one exists.
func (c *BaseCluster) GetHost(hostId string) (*Host, bool) {
	return c.hosts.Load(hostId)
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-out.
//
// targetNumNodes specifies the desired size of the cluster.
//
// resultChan is used to notify a waiting goroutine that the scale-out operation has finished.
func (c *BaseCluster) getScaleOutCommand(targetNumNodes int32, resultChan chan interface{}) func() {
	return c.instance.getScaleOutCommand(targetNumNodes, resultChan)
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
func (c *BaseCluster) getScaleInCommand(targetNumNodes int32, targetHosts []string, resultChan chan interface{}) (func(), error) {
	return c.instance.getScaleInCommand(targetNumNodes, targetHosts, resultChan)
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *BaseCluster) NodeType() string {
	return c.instance.NodeType()
}

// ActiveScaleOperation returns the active scaling operation, if one exists.
// If there is no active scaling operation, then ActiveScaleOperation returns nil.
func (c *BaseCluster) ActiveScaleOperation() *ScaleOperation {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation
}
