package cluster

import (
	"context"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/entity"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/scheduler"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"sync"
	"time"
)

// BaseCluster encapsulates the core state and logic required to operate a Cluster.
type BaseCluster struct {
	instance internalCluster

	// DisabledHosts is a map from host ID to *entity.Host containing all the Host instances that are currently set to "off".
	DisabledHosts hashmap.HashMap[string, *entity.Host]

	// hosts is a map from host ID to *entity.Host containing all the Host instances provisioned within the Cluster.
	hosts hashmap.HashMap[string, *entity.Host]

	// sessions is a map of Sessions.
	sessions hashmap.HashMap[string, scheduling.UserSession]

	// indexes is a map from index key to IndexProvider containing all the indexes in the Cluster.
	indexes hashmap.BaseHashMap[string, IndexProvider]

	// activeScaleOperation is a reference to the currently-active scale-out/scale-in operation.
	// There may only be one scaling operation active at any given time.
	// If activeScaleOperation is nil, then there is no active scale-out or scale-in operation.
	activeScaleOperation *scheduler.ScaleOperation
	scaleOperationCond   *sync.Cond

	numFailedScaleInOps      int
	numFailedScaleOutOps     int
	numSuccessfulScaleInOps  int
	numSuccessfulScaleOutOps int

	// gpusPerHost is the number of GPUs available on each host.
	gpusPerHost int

	// scheduler is the ClusterScheduler for the Cluster.
	scheduler ClusterScheduler

	// placer is the Placer for the Cluster.
	placer scheduling.Placer

	log logger.Logger

	// numReplicas is the number of replicas for each kernel.
	numReplicas        int
	numReplicasDecimal decimal.Decimal

	// minimumCapacity is the minimum number of nodes we must have available at any time.
	// It must be at least equal to the number of replicas per kernel.
	minimumCapacity int32

	// maximumCapacity is the maximum number of nodes we may have available at any time.
	// If this value is < 0, then it is unbounded.
	// It must be at least equal to the number of replicas per kernel, and it cannot be smaller than the minimum capacity.
	maximumCapacity int32

	// clusterMetricsProvider provides access to Prometheus metrics (for publishing purposes).
	clusterMetricsProvider metrics.ClusterMetricsProvider

	// scalingOpMutex controls access to the currently active scaling operation.
	// scalingOpMutex is also used as the sync.Locker for the scaleOperationCond.
	scalingOpMutex sync.Mutex

	// hostMutex controls external access to the internal Host mapping.
	hostMutex sync.RWMutex

	// validateCapacityInterval is how frequently the Cluster should validate its capacity,
	// scaling-in or scaling-out depending on the current load and whatnot.
	validateCapacityInterval time.Duration
}

// newBaseCluster creates a new BaseCluster struct and returns a pointer to it.
// This function is for package-internal or file-internal use only.
func newBaseCluster(opts *ClusterSchedulerOptions, clusterMetricsProvider metrics.ClusterMetricsProvider, loggerPrefix string) *BaseCluster {
	cluster := &BaseCluster{
		gpusPerHost:              opts.GpusPerHost,
		numReplicas:              opts.NumReplicas,
		numReplicasDecimal:       decimal.NewFromInt(int64(opts.NumReplicas)),
		clusterMetricsProvider:   clusterMetricsProvider,
		hosts:                    hashmap.NewConcurrentMap[*entity.Host](256),
		sessions:                 hashmap.NewCornelkMap[string, scheduling.UserSession](128),
		indexes:                  hashmap.NewSyncMap[string, IndexProvider](),
		validateCapacityInterval: time.Second * time.Duration(opts.ScalingInterval),
		DisabledHosts:            hashmap.NewConcurrentMap[*entity.Host](256),
		numFailedScaleInOps:      0,
		numFailedScaleOutOps:     0,
		numSuccessfulScaleInOps:  0,
		numSuccessfulScaleOutOps: 0,
	}
	cluster.scaleOperationCond = sync.NewCond(&cluster.scalingOpMutex)

	if loggerPrefix == "" {
		config.InitLogger(&cluster.log, cluster)
	} else {
		// Make sure that the prefix ends with a space so that there's a space between the prefix
		// and the beginning of log messages. Without the space, the log messages would look like:
		//
		// "DockerComposeClusterHello, world"
		//
		// instead of
		//
		// "DockerComposeCluster Hello, world"
		if !strings.HasSuffix(loggerPrefix, " ") {
			loggerPrefix = loggerPrefix + " "
		}

		config.InitLogger(&cluster.log, loggerPrefix)
	}

	go func() {
		cluster.log.Debug("Sleeping for %v before periodically validating Cluster capacity.", cluster.validateCapacityInterval)

		// We wait for `validateCapacityInterval` before the first call to UpdateRatio (which calls ValidateCapacity).
		time.Sleep(cluster.validateCapacityInterval)

		for {
			cluster.scheduler.UpdateRatio(false)
			time.Sleep(cluster.validateCapacityInterval)
		}
	}()

	return cluster
}

// NumScalingOperationsSucceeded returns the number of scale-in and scale-out operations that have been
// completed successfully.
func (c *BaseCluster) NumScalingOperationsSucceeded() int {
	return c.numSuccessfulScaleInOps + c.numSuccessfulScaleOutOps
}

// NumScaleOutOperationsSucceeded returns the number of scale-out operations that have been
// completed successfully.
func (c *BaseCluster) NumScaleOutOperationsSucceeded() int {
	return c.numSuccessfulScaleOutOps
}

// NumScaleInOperationsSucceeded returns the number of scale-in operations that have been
// completed successfully.
func (c *BaseCluster) NumScaleInOperationsSucceeded() int {
	return c.numSuccessfulScaleInOps
}

// NumScalingOperationsAttempted returns the number of scale-in and scale-out operations that have been
// attempted (i.e., count of both successful and failed operations).
func (c *BaseCluster) NumScalingOperationsAttempted() int {
	return c.numSuccessfulScaleInOps + c.numSuccessfulScaleOutOps + c.numFailedScaleOutOps + c.numFailedScaleInOps
}

// NumScaleOutOperationsAttempted returns the number of scale-out operations that have been
// attempted (i.e., count of both successful and failed operations).
func (c *BaseCluster) NumScaleOutOperationsAttempted() int {
	return c.numSuccessfulScaleOutOps + c.numFailedScaleOutOps
}

// NumScaleInOperationsAttempted returns the number of scale-in operations that have been
// attempted (i.e., count of both successful and failed operations).
func (c *BaseCluster) NumScaleInOperationsAttempted() int {
	return c.numSuccessfulScaleInOps + c.numFailedScaleInOps
}

// NumScalingOperationsFailed returns the number of scale-in and scale-out operations that have failed.
func (c *BaseCluster) NumScalingOperationsFailed() int {
	return c.numFailedScaleOutOps + c.numFailedScaleInOps
}

// NumScaleOutOperationsFailed returns the number of scale-out operations that have failed.
func (c *BaseCluster) NumScaleOutOperationsFailed() int {
	return c.numFailedScaleOutOps
}

// NumScaleInOperationsFailed returns the number of scale-in operations that have failed.
func (c *BaseCluster) NumScaleInOperationsFailed() int {
	return c.numFailedScaleInOps
}

// GetSession returns the Session with the specified ID.
//
// We return the scheduling.UserSession so that we can use this in unit tests with a mocked Session.
func (c *BaseCluster) GetSession(sessionID string) (scheduling.UserSession, bool) {
	return c.sessions.Load(sessionID)
}

// AddSession adds a Session to the Cluster.
func (c *BaseCluster) AddSession(sessionId string, session scheduling.UserSession) {
	c.sessions.Store(sessionId, session)
}

// Sessions returns a mapping from session ID to Session.
func (c *BaseCluster) Sessions() hashmap.HashMap[string, scheduling.UserSession] {
	return c.sessions
}

// canPossiblyScaleOut returns true if the Cluster could possibly scale-out.
// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
// it is currently not supported unless there is at least one disabled host already within the cluster.
func (c *BaseCluster) canPossiblyScaleOut() bool {
	return c.instance.canPossiblyScaleOut()
}

// NumReplicasAsDecimal returns the numer of replicas that each Jupyter kernel has associated with it as
// a decimal.Decimal struct.
//
// This value is typically equal to 3, but may be altered in the system configuration.
//
// This API exists as basically an optimization so we can return a cached decimal.Decimal struct,
// rather than recreate it each time we need it.
func (c *BaseCluster) NumReplicasAsDecimal() decimal.Decimal {
	return c.numReplicasDecimal
}

// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
// the given ratio and the Cluster's current subscription ratio.
//
// Cluster's GetOversubscriptionFactor simply calls the GetOversubscriptionFactor method of the
// Cluster's ClusterScheduler.
func (c *BaseCluster) GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal {
	return c.scheduler.GetOversubscriptionFactor(ratio)
}

func (c *BaseCluster) SubscriptionRatio() float64 {
	return c.ClusterScheduler().SubscriptionRatio()
}

// Placer returns the Placer used by the Cluster.
func (c *BaseCluster) Placer() scheduling.Placer {
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

func (c *BaseCluster) GetIndex(category string, expected interface{}) (IndexProvider, bool) {
	key := fmt.Sprintf("%s:%v", category, expected)
	return c.indexes.Load(key)
}

func (c *BaseCluster) AddIndex(index IndexProvider) error {
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
func (c *BaseCluster) unsafeCheckIfScaleOperationIsComplete(host *entity.Host) {
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

		if c.activeScaleOperation.IsScaleInOperation() {
			c.numSuccessfulScaleInOps += 1
		} else {
			c.numSuccessfulScaleOutOps += 1
		}

		activeScaleOp = nil
		c.scaleOperationCond.Broadcast()
	} else {
		c.log.Debug("%s %s is not yet finished (current scale = %d, target scale = %d). Only %d/%d affected hosts have been registered.",
			activeScaleOp.OperationType, activeScaleOp.OperationId, c.Len(), activeScaleOp.TargetScale,
			len(activeScaleOp.NodesAffected), activeScaleOp.ExpectedNumAffectedNodes)
	}
}

// onDisabledHostAdded when a new Host is added to the Cluster in a disabled state,
// meaning that it is intended to be unavailable unless we explicitly scale-out.
func (c *BaseCluster) onDisabledHostAdded(host *entity.Host) error {
	if host.Enabled() {
		return fmt.Errorf("host %s (ID=%s) is not disabled", host.NodeName, host.ID)
	}

	c.DisabledHosts.Store(host.ID, host)

	if c.clusterMetricsProvider != nil && c.clusterMetricsProvider.GetNumDisabledHostsGauge() != nil {
		c.clusterMetricsProvider.GetNumDisabledHostsGauge().Add(1)
	}

	return nil
}

// onHostAdded is called when a host is added to the BaseCluster.
func (c *BaseCluster) onHostAdded(host *entity.Host) {
	c.indexes.Range(func(key string, index IndexProvider) bool {
		if _, qualificationStatus := index.IsQualified(host); qualificationStatus == IndexNewQualified {
			c.log.Debug("Adding new host to index: %v", host)
			index.Add(host)
		} else if qualificationStatus == IndexQualified {
			c.log.Debug("Updating existing host within index: %v", host)
			index.Update(host)
		} else if qualificationStatus == IndexDisqualified {
			c.log.Debug("Removing existing host from index in onHostAdded: %v", host)
			index.Remove(host)
		} // else unqualified
		return true
	})

	c.unsafeCheckIfScaleOperationIsComplete(host)

	if c.clusterMetricsProvider != nil && c.clusterMetricsProvider.GetNumHostsGauge() != nil {
		c.clusterMetricsProvider.GetNumHostsGauge().Set(float64(c.hosts.Len()))
	}
}

// onHostRemoved is called when a host is deleted from the BaseCluster.
func (c *BaseCluster) onHostRemoved(host *entity.Host) {
	c.indexes.Range(func(key string, index IndexProvider) bool {
		if _, hostQualificationStatus := index.IsQualified(host); hostQualificationStatus != IndexUnqualified {
			index.Remove(host)
		}
		return true
	})

	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()
	c.unsafeCheckIfScaleOperationIsComplete(host)

	if c.clusterMetricsProvider != nil && c.clusterMetricsProvider.GetNumHostsGauge() != nil {
		c.clusterMetricsProvider.GetNumHostsGauge().Set(float64(c.hosts.Len()))
	}
}

// BusyGPUs returns the number of GPUs that are actively committed to kernel replicas right now.
func (c *BaseCluster) BusyGPUs() float64 {
	busyGPUs := 0.0
	c.hosts.Range(func(_ string, host *entity.Host) (contd bool) {
		busyGPUs += host.CommittedGPUs()
		return true
	})

	return busyGPUs
}

// DemandGPUs returns the number of GPUs that are required by all actively-running Sessions.
func (c *BaseCluster) DemandGPUs() float64 {
	demandGPUs := 0.0
	c.sessions.Range(func(_ string, session scheduling.UserSession) (contd bool) {
		if session.IsIdle() || session.IsTraining() {
			demandGPUs += session.ResourceSpec().GPU()
		}

		return true
	})

	return demandGPUs
}

// NumReplicas returns the numer of replicas that each Jupyter kernel has associated with it.
// This is typically equal to 3, but may be altered in the system configuration.
func (c *BaseCluster) NumReplicas() int {
	return c.numReplicas
}

// RangeOverHosts executes the provided function on each Host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BaseCluster) RangeOverHosts(f func(key string, value *entity.Host) bool) {
	c.hosts.Range(f)
}

// RemoveHost removes the Host with the specified ID.
func (c *BaseCluster) RemoveHost(hostId string) {
	c.scalingOpMutex.Lock()

	c.hostMutex.Lock()
	removedHost, loaded := c.hosts.LoadAndDelete(hostId)
	c.hostMutex.Unlock()

	c.scalingOpMutex.Unlock()

	if loaded {
		c.onHostRemoved(removedHost)
	}
}

// NumDisabledHosts returns the number of Host instances in the Cluster that are in the "disabled" state.
func (c *BaseCluster) NumDisabledHosts() int {
	return c.DisabledHosts.Len()
}

// Len returns the number of *entity.Host instances in the Cluster.
func (c *BaseCluster) Len() int {
	if c.hosts == nil {
		return 0
	}

	return c.hosts.Len()
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
func (c *BaseCluster) registerScaleOperation(operationId string, targetClusterSize int32) (*scheduler.ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	var (
		currentClusterSize = int32(c.Len())
		scaleOperation     *scheduler.ScaleOperation
		err                error
	)
	if targetClusterSize > currentClusterSize {
		scaleOperation, err = scheduler.NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	} else {
		scaleOperation, err = scheduler.NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	}

	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != scheduler.ScaleOutOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", scheduler.ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	c.activeScaleOperation = scaleOperation
	return scaleOperation, nil
}

// unregisterActiveScaleOp unregisters the current active ScaleOperation and returns a boolean indicating whether
// a ScaleOperation was in fact unregistered.
//
// If there is no active ScaleOperation, then unregisterActiveScaleOp returns false.
//
// If there is an active ScaleOperation, but that ScaleOperation is incomplete, then unregisterActiveScaleOp returns false.
//
// If there is an active ScaleOperation, and that ScaleOperation has finished (either successfully or in an error state),
// then unregisterActiveScaleOp returns true.
func (c *BaseCluster) unregisterActiveScaleOp(force bool) bool {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation == nil {
		c.log.Error("Cannot unregister active scale operation, as there is no active scale operation right now...")
		return false
	}

	// If force is true, then we'll unregister it anyway.
	if !c.activeScaleOperation.IsComplete() && !force {
		c.log.Error("Cannot unregister active %s %s, as the operation is incomplete: %v",
			c.activeScaleOperation.OperationType, c.activeScaleOperation.OperationId, c.activeScaleOperation)
		return false
	} else if c.activeScaleOperation.IsComplete() && force {
		c.log.Warn("Forcibly unregistering in-progress %s %s (state=%v)",
			c.activeScaleOperation.OperationType, c.activeScaleOperation.OperationId, c.activeScaleOperation.Status)
	}

	previouslyActiveScaleOperation := c.activeScaleOperation
	c.activeScaleOperation = nil
	c.log.Debug("Unregistered %v %s %s now.",
		previouslyActiveScaleOperation.Status,
		previouslyActiveScaleOperation.OperationType,
		previouslyActiveScaleOperation.OperationId)

	return true
}

// RegisterScaleOutOperation registers a scale-out operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there is already an active scaling operation taking place, then an error is returned.
func (c *BaseCluster) registerScaleOutOperation(operationId string, targetClusterSize int32) (*scheduler.ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	currentClusterSize := int32(c.Len())
	scaleOperation, err := scheduler.NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
	if err != nil {
		return nil, err
	}

	if scaleOperation.OperationType != scheduler.ScaleOutOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", scheduler.ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
	}

	c.activeScaleOperation = scaleOperation
	return scaleOperation, nil
}

// RegisterScaleInOperation registers a scale-in operation.
// When the operation completes, a notification is sent on the channel passed to this function.
//
// If there already exists a scale operation with the same ID, then the existing scale operation is returned along with an error.
func (c *BaseCluster) registerScaleInOperation(operationId string, targetClusterSize int32, targetHosts []string) (*scheduler.ScaleOperation, error) {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if c.activeScaleOperation != nil {
		c.log.Error("Cannot register new ScaleInOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, ErrScalingActive
	}

	var (
		currentClusterSize = int32(c.Len())
		scaleOperation     *scheduler.ScaleOperation
		err                error
	)
	if len(targetHosts) > 0 {
		scaleOperation, err = scheduler.NewScaleInOperationWithTargetHosts(operationId, currentClusterSize, targetHosts, c.instance)
		if err != nil {
			return nil, err
		}
	} else {
		scaleOperation, err = scheduler.NewScaleOperation(operationId, currentClusterSize, targetClusterSize, c.instance)
		if err != nil {
			return nil, err
		}
	}

	if scaleOperation.OperationType != scheduler.ScaleInOperation {
		return nil, fmt.Errorf("%w: Cluster is currently of size %d, and scale-out operation is requesting target scale of %d", scheduler.ErrInvalidTargetScale, currentClusterSize, targetClusterSize)
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

	if targetNumNodes < c.maximumCapacity {
		c.log.Error("Cannot add %d Local Daemon Docker node(s) from the Cluster, "+
			"as doing so would violate the Cluster's maximum capacity constraint of %d.", n, c.maximumCapacity)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: "+
			"adding %d nodes would violate maximum capacity constraint of %d",
			scheduling.ErrInvalidTargetNumHosts, n, c.maximumCapacity))
	}

	c.log.Debug("Received request for %d additional host(s). Current scale: %d. Target scale: %d.",
		n, currentNumNodes, targetNumNodes)

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target Cluster size).
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

		// Unregister the failed scale-out operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug("Scale-out from %d nodes to %d nodes succeeded.", scaleOp.InitialScale, scaleOp.TargetScale)
	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
	}

	return promise.Resolved(result)
}

// ReleaseSpecificHosts terminates one or more specific Host instances.
func (c *BaseCluster) ReleaseSpecificHosts(ctx context.Context, ids []string) promise.Promise {
	n := int32(len(ids))
	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < c.minimumCapacity {
		c.log.Error("Cannot remove %d Local Daemon Docker node(s) from the Cluster, "+
			"as doing so would violate the Cluster's minimum capacity constraint of %d.", n, c.minimumCapacity)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: "+
			"removing %d nodes would violate minimum capacity constraint of %d",
			scheduling.ErrInvalidTargetNumHosts, n, c.minimumCapacity))
	}

	if targetNumNodes < int32(c.numReplicas) {
		c.log.Error("Cannot remove %d specific Local Daemon Docker node(s) from the Cluster", n)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, scheduling.ErrInvalidTargetNumHosts)
	}

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target Cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.registerScaleInOperation(opId, targetNumNodes, ids)
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Debug("Scale-in from %d nodes down to %d nodes failed because: %v",
			scaleOp.InitialScale, scaleOp.TargetScale, err)

		// Unregister the failed scale-in operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug("Scale-in from %d nodes down to %d nodes succeeded.", scaleOp.InitialScale, scaleOp.TargetScale)
	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
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
		c.log.Error("Cannot remove %d Local Daemon Docker node(s) from the Cluster", n)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, scheduling.ErrInvalidTargetNumHosts)
	}

	if targetNumNodes < c.minimumCapacity {
		c.log.Error("Cannot remove %d Local Daemon Docker node(s) from the Cluster, "+
			"as doing so would violate the Cluster's minimum capacity constraint of %d.", n, c.minimumCapacity)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: "+
			"removing %d nodes would violate minimum capacity constraint of %d",
			scheduling.ErrInvalidTargetNumHosts, n, c.minimumCapacity))
	}

	c.log.Debug("Will attempt to release %d nodes. Current scale: %d. Target scale: %d.",
		n, currentNumNodes, targetNumNodes)

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target Cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.registerScaleInOperation(opId, targetNumNodes, []string{})
	if err != nil {
		c.log.Error("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Debug("Scale-in from %d nodes down to %d nodes failed because: %v",
			scaleOp.InitialScale, scaleOp.TargetScale, err)

		// Unregister the failed scale-in operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug("Scale-in from %d nodes to %d nodes has succeeded.", scaleOp.InitialScale, scaleOp.TargetScale)
	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		log.Fatalf("Failed to unregister active scale operation %v.", c.activeScaleOperation)
	}

	return promise.Resolved(result)
}

// ScaleToSize scales the Cluster to the specified number of Host instances.
//
// If n <= NUM_REPLICAS, then ScaleToSize returns with an error.
func (c *BaseCluster) ScaleToSize(ctx context.Context, targetNumNodes int32) promise.Promise {
	currentNumNodes := int32(c.Len())

	// Are we trying to scale-down below the minimum cluster size? If so, return an error.
	if targetNumNodes < int32(c.numReplicas) {
		c.log.Error("Cannot scale to size of %d Local Daemon Docker node(s) from the Cluster", targetNumNodes)
		c.log.Error("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: targetNumNodes=%d", scheduling.ErrInvalidTargetNumHosts, targetNumNodes))
	}

	// Do we already have the requested number of hosts? If so, return an error.
	if targetNumNodes == currentNumNodes {
		c.log.Warn("Cluster is already of size %d. Rejecting request to scale to size %d.", currentNumNodes, targetNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: Cluster is already of size %d", scheduling.ErrInvalidTargetNumHosts, targetNumNodes))
	}

	// Scale out (i.e., add hosts)?
	if targetNumNodes > currentNumNodes {
		c.log.Debug("Requesting %d additional host(s) in order to scale-out to target size of %d.", targetNumNodes-currentNumNodes, targetNumNodes)
		return c.RequestHosts(ctx, targetNumNodes-currentNumNodes)
	}

	// Scale in (i.e., remove hosts).
	c.log.Debug("Releasing %d host(s) in order to scale-in to target size of %d.", targetNumNodes-currentNumNodes, targetNumNodes)
	return c.ReleaseHosts(ctx, currentNumNodes-targetNumNodes)
}

// ClusterMetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
func (c *BaseCluster) ClusterMetricsProvider() metrics.ClusterMetricsProvider {
	return c.clusterMetricsProvider
}

// NewHostAddedOrConnected should be called by an external entity when a new Host connects to the Cluster Gateway.
// NewHostAddedOrConnected handles the logic of adding the Host to the Cluster, and in particular will handle the
// task of locking the required structures during scaling operations.
func (c *BaseCluster) NewHostAddedOrConnected(host *entity.Host) error {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	if !host.Enabled() {
		c.log.Debug("Attempting to add disabled host %s (ID=%s) to the cluster now.", host.NodeName, host.ID)
		err := c.onDisabledHostAdded(host)
		if err != nil {
			c.log.Error("Failed to add disabled Host %s (ID=%s) to cluster because: %v", host.NodeName, host.ID, err)
			return err
		}
		c.log.Debug("Successfully added disabled host %s (ID=%s) to the cluster.", host.NodeName, host.ID)
		return nil
	}

	c.log.Debug("Host %s (ID=%s) has just connected to the Cluster or is being re-enabled", host.NodeName, host.ID)

	c.hostMutex.Lock()
	// The host mutex is already locked if we're performing a scaling operation.
	c.hosts.Store(host.ID, host)
	c.hostMutex.Unlock()

	c.onHostAdded(host)

	c.log.Debug("Finished handling scheduling.Cluster-level registration of newly-added host %s (ID=%s)", host.NodeName, host.ID)

	return nil
}

// GetHost returns the Host with the given ID, if one exists.
func (c *BaseCluster) GetHost(hostId string) (*entity.Host, bool) {
	return c.hosts.Load(hostId)
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-out.
//
// targetNumNodes specifies the desired size of the Cluster.
//
// resultChan is used to notify a waiting goroutine that the scale-out operation has finished.
func (c *BaseCluster) getScaleOutCommand(targetNumNodes int32, resultChan chan interface{}) func() {
	return c.instance.getScaleOutCommand(targetNumNodes, resultChan)
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-in.
//
// targetNumNodes specifies the desired size of the Cluster.
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
func (c *BaseCluster) ActiveScaleOperation() *scheduler.ScaleOperation {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	return c.activeScaleOperation
}
