package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BaseCluster encapsulates the core state and logic required to operate a Cluster.
type BaseCluster struct {
	instance internalCluster

	// DisabledHosts is a map from host ID to *entity.Host containing all the host instances that are currently set to "off".
	DisabledHosts hashmap.HashMap[string, scheduling.Host]

	// hosts is a map from host ID to *entity.Host containing all the host instances provisioned within the Cluster.
	hosts hashmap.HashMap[string, scheduling.Host]

	// sessions is a map of Sessions.
	sessions hashmap.HashMap[string, scheduling.UserSession]

	// indexes is a map from index key to IndexProvider containing all the indexes in the Cluster.
	indexes hashmap.BaseHashMap[string, scheduling.IndexProvider]

	// scheduler is the scheduling.Scheduler for the Cluster.
	scheduler scheduling.Scheduler

	log logger.Logger

	// metricsProvider provides access to Prometheus metrics (for publishing purposes).
	metricsProvider scheduling.MetricsProvider

	// activeScaleOperation is a reference to the currently-active scale-out/scale-in operation.
	// There may only be one scaling operation active at any given time.
	// If activeScaleOperation is nil, then there is no active scale-out or scale-in operation.
	activeScaleOperation *scheduler.ScaleOperation
	scaleOperationCond   *sync.Cond

	// statisticsUpdaterProvider is used to update metrics/statistics.
	statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics))

	opts *scheduling.SchedulerOptions

	numFailedScaleInOps      int
	numFailedScaleOutOps     int
	numSuccessfulScaleInOps  int
	numSuccessfulScaleOutOps int

	// The initial size of the cluster.
	// If more than this many Local Daemons connect during the 'initial connection period',
	// then the extra nodes will be disabled until a scale-out event occurs.
	//
	// Specifying this as a negative number will disable this feature (so any number of hosts can connect).
	//
	// TODO: If a Local Daemon connects "unexpectedly", then perhaps it should be disabled by default?
	initialClusterSize int

	// numHostsDisabledDuringInitialConnectionPeriod keeps track of the number of host instances we disabled
	// during the initial connection period.
	numHostsDisabledDuringInitialConnectionPeriod int

	// gpusPerHost is the number of GPUs available on each host.
	gpusPerHost int

	// validateCapacityInterval is how frequently the Cluster should validate its capacity,
	// scaling-in or scaling-out depending on the current load and whatnot.
	validateCapacityInterval time.Duration

	MeanScaleOutPerHost   time.Duration
	StdDevScaleOutPerHost time.Duration

	MeanScaleInPerHost   time.Duration
	StdDevScaleInPerHost time.Duration

	// hostMutex controls external access to the internal host mapping.
	hostMutex sync.RWMutex

	sessionsMutex sync.RWMutex

	// scalingOpMutex controls access to the currently active scaling operation.
	// scalingOpMutex is also used as the sync.Locker for the scaleOperationCond.
	scalingOpMutex sync.Mutex

	// minimumCapacity is the minimum number of nodes we must have available at any time.
	// It must be at least equal to the number of replicas per kernel.
	minimumCapacity int32

	// maximumCapacity is the maximum number of nodes we may have available at any time.
	// If this value is < 0, then it is unbounded.
	// It must be at least equal to the number of replicas per kernel, and it cannot be smaller than the minimum capacity.
	maximumCapacity int32

	closed atomic.Bool

	// The initial connection period is the time immediately after the Cluster Gateway begins running during
	// which it expects all Local Daemons to connect. If greater than N local daemons connect during this period,
	// where N is the initial cluster size, then those extra daemons will be disabled.
	//
	// TODO: If a Local Daemon connects "unexpectedly", then perhaps it should be disabled by default?
	initialConnectionPeriod time.Duration

	// inInitialConnectionPeriod indicates whether we're still in the "initial connection period" or not.
	inInitialConnectionPeriod atomic.Bool
}

// newBaseCluster creates a new BaseCluster struct and returns a pointer to it.
// This function is for package-internal or file-internal use only.
func newBaseCluster(opts *scheduling.SchedulerOptions, placer scheduling.Placer,
	clusterMetricsProvider scheduling.MetricsProvider, loggerPrefix string,
	statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics))) *BaseCluster {

	cluster := &BaseCluster{
		opts:                      opts,
		gpusPerHost:               opts.GetGpusPerHost(),
		metricsProvider:           clusterMetricsProvider,
		hosts:                     hashmap.NewConcurrentMap[scheduling.Host](256),
		sessions:                  hashmap.NewCornelkMap[string, scheduling.UserSession](128),
		indexes:                   hashmap.NewSyncMap[string, scheduling.IndexProvider](),
		validateCapacityInterval:  time.Millisecond * time.Duration(opts.GetScalingIntervalSec()*1000),
		DisabledHosts:             hashmap.NewConcurrentMap[scheduling.Host](256),
		statisticsUpdaterProvider: statisticsUpdaterProvider,
		numFailedScaleInOps:       0,
		numFailedScaleOutOps:      0,
		numSuccessfulScaleInOps:   0,
		numSuccessfulScaleOutOps:  0,
		MeanScaleOutPerHost:       time.Millisecond * time.Duration(opts.MeanScaleOutPerHostSec*1000),
		StdDevScaleOutPerHost:     time.Millisecond * time.Duration(opts.StdDevScaleOutPerHostSec*1000),
		MeanScaleInPerHost:        time.Millisecond * time.Duration(opts.MeanScaleInPerHostSec*1000),
		StdDevScaleInPerHost:      time.Millisecond * time.Duration(opts.StdDevScaleInPerHostSec*1000),
		initialClusterSize:        opts.InitialClusterSize,
		initialConnectionPeriod:   time.Second * time.Duration(opts.InitialClusterConnectionPeriodSec),
	}
	cluster.closed.Store(false)
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
		// "DockerCluster Hello, world"
		if !strings.HasSuffix(loggerPrefix, " ") {
			loggerPrefix = loggerPrefix + " "
		}

		config.InitLogger(&cluster.log, loggerPrefix)
	}

	if err := cluster.AddIndex(placer.GetIndex()); err != nil {
		panic(err)
	}

	go cluster.handleInitialConnectionPeriod()

	return cluster
}

// handleInitialConnectionPeriod first waits for the "initial connection period" to elapse, after which it sets the
// ClusterGatewayImpl's inInitialConnectionPeriod field to false.
//
// After that, handleInitialConnectionPeriod invokes the ProvisionInitialPrewarmContainers method of the
// scheduling.ContainerPrewarmer (if one exists).
func (c *BaseCluster) handleInitialConnectionPeriod() {
	initialConnPeriodEnabled := c.initialClusterSize >= 0

	// If initialClusterSize is specified as a negative number, then the feature is disabled, so we only bother
	// setting inInitialConnectionPeriod to true and creating a goroutine to eventually set inInitialConnectionPeriod
	// to false if the feature is enabled in the first place.
	if initialConnPeriodEnabled {
		c.inInitialConnectionPeriod.Store(true)

		// Spawn a separate goroutine to formally exit "initial connection" mode after the configured
		// interval of time has elapsed.
		start := time.Now()
		go func() {
			c.log.Debug("Initial Connection Period will end in at most %v.", c.initialConnectionPeriod)

			// Sleep for the configured interval of time.
			time.Sleep(c.initialConnectionPeriod)

			// Exit "initial connection" mode.
			c.inInitialConnectionPeriod.Store(false)

			c.log.Debug("Initial Connection Period has ended after %v. Cluster size: %d.",
				c.initialConnectionPeriod, c.Len())
		}()

		// Once a second for the duration of the "initial connection" period, we'll check the size of the cluster.
		// If we reach the configured initial size, then we'll exit this loop and begin pre-warming containers
		// on all the connected hosts. We do not need to wait for the full "initial connection" period to elapse
		// before we start to pre-warm containers if all "<initial cluster size>" hosts have connected.
		for c.Len() < c.initialClusterSize && c.inInitialConnectionPeriod.Load() {
			time.Sleep(time.Second * 1)

			// If we've reached the configured initial cluster size, then we can break out of the loop.
			if c.Len() >= c.initialClusterSize {
				c.log.Debug("Cluster has reached configured initial size of %d after %v.",
					c.initialClusterSize, time.Since(start))

				break
			}
		}
	} else {
		c.log.Debug("Initial Cluster Size specified as negative number (%d). "+
			"Disabling 'initial connection period' feature.", c.initialClusterSize)
	}

	if c.Scheduler() == nil {
		c.log.Warn("No ContainerPrewarmer available.")
		return
	}

	// Trigger the initial pre-warming phase now that the initial connection period has elapsed.
	prewarmer := c.Scheduler().ContainerPrewarmer()
	if prewarmer == nil {
		c.log.Debug("No ContainerPrewarmer available.")
		return
	}

	if c.opts.InitialNumContainersPerHost == 0 {
		c.log.Debug("Not supposed to pre-warm any containers per host.")
		return
	}

	c.log.Debug("Triggering initial pre-warming of %d container(s) on each connected host.",
		c.opts.InitialNumContainersPerHost)

	created, target := prewarmer.ProvisionInitialPrewarmContainers()

	// If we were supposed to create some number of containers, and we created none, then that's problematic.
	if created == 0 && target > 0 {
		c.log.Error("Created 0/%d pre-warm containers...", created, target)
		panic(fmt.Sprintf("Something is wrong. Failed to create any pre-warmed containers (target=%d).", target))
	}

	// Start the prewarmer when we return.
	defer func(prewarmer scheduling.ContainerPrewarmer) {
		err := prewarmer.Run()
		if err != nil {
			c.log.Error("ContainerPrewarmer encountered an error while running: %v", err)
		}
	}(prewarmer)

	// If the target was 0, then return immediately to avoid printing the unnecessary log message below.
	// We deferred `prewarmer.Run()`, so that'll run when we return.
	if target == 0 {
		return
	}

	c.log.Debug("Created %d/%d pre-warm containers.", created, target)
	// We deferred `prewarmer.Run()`, so that'll run when we return.
}

func (c *BaseCluster) initRatioUpdater() {
	go func() {
		c.log.Debug("Sleeping for %v before periodically validating Cluster capacity.", c.validateCapacityInterval)

		// We wait for `validateCapacityInterval` before the first call to UpdateRatio (which calls ValidateCapacity).
		time.Sleep(c.validateCapacityInterval)

		for !c.closed.Load() {
			c.scheduler.UpdateRatio(false)
			time.Sleep(c.validateCapacityInterval)
		}
	}()
}

// Close closes down the BaseCluster.
func (c *BaseCluster) Close() {
	c.closed.Store(true)
}

// NumScalingOperationsSucceeded returns the number of scale-in and scale-out operations that have been
// completed successfully.
func (c *BaseCluster) NumScalingOperationsSucceeded() int {
	return c.numSuccessfulScaleInOps + c.numSuccessfulScaleOutOps
}

// HasActiveScalingOperation returns true if there is an active scaling operation (of either kind).
func (c *BaseCluster) HasActiveScalingOperation() bool {
	return c.activeScaleOperation != nil
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
	c.sessionsMutex.Lock()
	defer c.sessionsMutex.Unlock()

	c.sessions.Store(sessionId, session)
}

// RemoveSession removes and returns a UserSession.
func (c *BaseCluster) RemoveSession(sessionId string) scheduling.UserSession {
	c.sessionsMutex.Lock()
	defer c.sessionsMutex.Unlock()

	session, _ := c.sessions.LoadAndDelete(sessionId)
	return session
}

// Sessions returns a mapping from session ID to Session.
func (c *BaseCluster) Sessions() hashmap.HashMap[string, scheduling.UserSession] {
	return c.sessions
}

// CanPossiblyScaleOut returns true if the Cluster could possibly scale-out.
// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
// it is currently not supported unless there is at least one disabled host already within the cluster.
func (c *BaseCluster) CanPossiblyScaleOut() bool {
	return c.instance.CanPossiblyScaleOut()
}

// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
// the given ratio and the Cluster's current subscription ratio.
//
// Cluster's GetOversubscriptionFactor simply calls the GetOversubscriptionFactor method of the
// Cluster's Scheduler.
func (c *BaseCluster) GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal {
	return c.scheduler.GetOversubscriptionFactor(ratio)
}

func (c *BaseCluster) SubscriptionRatio() float64 {
	return c.Scheduler().SubscriptionRatio()
}

// Placer returns the Placer used by the Cluster.
func (c *BaseCluster) Placer() scheduling.Placer {
	return c.scheduler.Placer()
}

// ReadLockHosts locks the underlying host manager such that no host instances can be added or removed.
func (c *BaseCluster) ReadLockHosts() {
	c.hostMutex.RLock()
}

// ReadUnlockHosts unlocks the underlying host manager, enabling the addition or removal of host instances.
//
// The caller must have already acquired the hostMutex or this function will fail panic.
func (c *BaseCluster) ReadUnlockHosts() {
	c.hostMutex.RUnlock()
}

// Scheduler returns the Scheduler used by the Cluster.
func (c *BaseCluster) Scheduler() scheduling.Scheduler {
	return c.scheduler
}

func (c *BaseCluster) GetIndex(category string, expected interface{}) (scheduling.IndexProvider, bool) {
	key := fmt.Sprintf("%s:%v", category, expected)
	return c.indexes.Load(key)
}

func (c *BaseCluster) AddIndex(index scheduling.IndexProvider) error {
	category, expected := index.Category()
	key := fmt.Sprintf("%s:%v", category, expected)
	if _, ok := c.indexes.Load(key); ok {
		return ErrDuplicatedIndexDefined
	}

	c.indexes.Store(key, index)

	c.log.Debug("Added index under key '%s'", key)

	return nil
}

// UpdateIndex updates the ClusterIndex that contains the specified host.
func (c *BaseCluster) UpdateIndex(host scheduling.Host) error {
	categoryMetadata := host.GetMeta(scheduling.HostIndexCategoryMetadata)
	if categoryMetadata == nil {
		return fmt.Errorf("host %s (ID=%s) does not have a HostIndexCategoryMetadata ('%s') metadata entry",
			host.GetNodeName(), host.GetID(), scheduling.HostIndexCategoryMetadata)
	}

	keyMetadata := host.GetMeta(scheduling.HostIndexKeyMetadata)
	if keyMetadata == nil {
		return fmt.Errorf("host %s (ID=%s) does not have a HostIndexKeyMetadata ('%s') metadata entry",
			host.GetNodeName(), host.GetID(), scheduling.HostIndexKeyMetadata)
	}

	key := fmt.Sprintf("%s:%v", categoryMetadata, keyMetadata.(string))
	clusterIndex, loaded := c.indexes.Load(key)

	if !loaded || clusterIndex == nil {
		return fmt.Errorf("could not find cluster index with category '%s' and key '%s'",
			categoryMetadata, keyMetadata.(string))
	}

	c.log.Debug("Updating index %s for host %s (id=%s)", key, host.GetNodeName(), host.GetID())
	clusterIndex.Update(host)
	return nil
}

// unsafeCheckIfScaleOperationIsComplete is used to check if there is an active scaling operation and,
// if there is, then to check if that operation is complete.
func (c *BaseCluster) unsafeCheckIfScaleOperationIsComplete(host scheduling.Host) {
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

// onDisabledHostAdded when a new host is added to the Cluster in a disabled state,
// meaning that it is intended to be unavailable unless we explicitly scale-out.
func (c *BaseCluster) onDisabledHostAdded(host scheduling.Host) error {
	if host.Enabled() {
		return fmt.Errorf("host %s (ID=%s) is not disabled", host.GetNodeName(), host.GetID())
	}

	c.DisabledHosts.Store(host.GetID(), host)

	if c.metricsProvider != nil && c.metricsProvider.GetNumDisabledHostsGauge() != nil {
		c.metricsProvider.GetNumDisabledHostsGauge().Add(1)
	}

	return nil
}

// onHostAdded is called when a host is added to the BaseCluster.
//
// onHostAdded must be called with the scalingOpMutex already held.
func (c *BaseCluster) onHostAdded(host scheduling.Host) {
	c.scheduler.HostAdded(host)

	c.indexes.Range(func(indexKey string, index scheduling.IndexProvider) bool {
		if _, qualificationStatus := index.IsQualified(host); qualificationStatus == scheduling.IndexNewQualified {
			c.log.Debug("Adding new host to index %s: %v", indexKey, host)
			index.Add(host)
		} else if qualificationStatus == scheduling.IndexQualified {
			c.log.Debug("Updating existing host within index %s: %v", indexKey, host)
			index.Update(host)
		} else if qualificationStatus == scheduling.IndexDisqualified {
			c.log.Debug("Removing existing host from index %s in onHostAdded: %v", indexKey, host)
			index.Remove(host)
		} else {
			// Log level is set to warn because, as of right now, there are never any actual qualifications.
			c.log.Warn("host %s (ID=%s) is not qualified to be added to index '%s'.",
				host.GetNodeName(), host.GetID(), index.Identifier())
		}

		return true
	})

	c.unsafeCheckIfScaleOperationIsComplete(host)

	if c.metricsProvider != nil {
		c.metricsProvider.IncrementResourceCountsForNewHost(host)

		if c.metricsProvider.PrometheusMetricsEnabled() && c.metricsProvider.GetNumHostsGauge() != nil {
			c.metricsProvider.GetNumHostsGauge().Set(float64(c.hosts.Len()))
		}
	}
}

// onHostRemoved is called when a host is deleted from the BaseCluster.
//
// onHostRemoved must NOT be called with the scalingOpMutex already held.
// The scalingOpMutex will be acquired by onHostRemoved.
// This differs from onHostAdded, which must be called with the scalingOpMutex already held.
func (c *BaseCluster) onHostRemoved(host scheduling.Host) {
	c.scheduler.HostRemoved(host)

	c.indexes.Range(func(key string, index scheduling.IndexProvider) bool {
		if _, hostQualificationStatus := index.IsQualified(host); hostQualificationStatus != scheduling.IndexUnqualified {
			index.Remove(host)
		}
		return true
	})

	c.scalingOpMutex.Lock()
	c.unsafeCheckIfScaleOperationIsComplete(host)
	c.scalingOpMutex.Unlock()

	if c.metricsProvider != nil {
		c.metricsProvider.DecrementResourceCountsForRemovedHost(host)

		if c.metricsProvider.PrometheusMetricsEnabled() && c.metricsProvider.GetNumHostsGauge() != nil {
			c.metricsProvider.GetNumHostsGauge().Set(float64(c.hosts.Len()))
		}
	}
}

// BusyGPUs returns the number of GPUs that are actively committed to kernel replicas right now.
func (c *BaseCluster) BusyGPUs() float64 {
	c.hostMutex.RLock()
	defer c.hostMutex.RUnlock()

	busyGPUs := 0.0
	c.hosts.Range(func(_ string, host scheduling.Host) (contd bool) {
		busyGPUs += host.CommittedGPUs()
		return true
	})

	return busyGPUs
}

// DemandAndBusyGPUs returns Demand GPUs, Busy GPUs, num idle sessions, num training sessions.
func (c *BaseCluster) DemandAndBusyGPUs() (float64, float64, int, int, int) {
	c.sessionsMutex.RLock()
	defer c.sessionsMutex.RUnlock()

	seenSessions := make(map[string]struct{})

	var demandGPUs, busyGPUs float64
	var numRunning, numIdle, numTraining int
	c.sessions.Range(func(_ string, session scheduling.UserSession) (contd bool) {
		if _, loaded := seenSessions[session.ID()]; loaded {
			// Shouldn't be possible, right...?
			c.log.Warn("Found duplicate session: \"%s\"", session.ID())
			return true // Skip
		}

		if session.IsMigrating() {
			numRunning += 1
		} else if session.IsIdle() {
			demandGPUs += session.ResourceSpec().GPU()
			numIdle += 1
			numRunning += 1
		} else if session.IsTraining() {
			demandGPUs += session.ResourceSpec().GPU()
			busyGPUs += session.ResourceSpec().GPU()
			numTraining += 1
			numRunning += 1
		}

		seenSessions[session.ID()] = struct{}{}

		return true
	})

	return demandGPUs, busyGPUs, numRunning, numIdle, numTraining
}

// DemandGPUs returns the number of GPUs that are required by all actively-running Sessions.
func (c *BaseCluster) DemandGPUs() float64 {
	c.sessionsMutex.RLock()
	defer c.sessionsMutex.RUnlock()

	seenSessions := make(map[string]struct{})

	demandGPUs := 0.0
	c.sessions.Range(func(_ string, session scheduling.UserSession) (contd bool) {
		if _, loaded := seenSessions[session.ID()]; loaded {
			// Shouldn't be possible, right...?
			c.log.Warn("Found duplicate session: \"%s\"", session.ID())
			return true // Skip
		}

		if session.IsIdle() || session.IsTraining() {
			demandGPUs += session.ResourceSpec().GPU()
		}

		seenSessions[session.ID()] = struct{}{}

		return true
	})

	return demandGPUs
}

// NumReplicas returns the numer of replicas that each Jupyter kernel has associated with it.
// This is typically equal to 3, but may be altered in the system configuration.
func (c *BaseCluster) NumReplicas() int {
	return c.scheduler.Policy().NumReplicas()
}

// RangeOverHosts executes the provided function on each host in the Cluster.
func (c *BaseCluster) RangeOverHosts(f func(key string, value scheduling.Host) bool) {
	c.hostMutex.RLock()
	defer c.hostMutex.RUnlock()

	c.hosts.Range(f)
}

// RangeOverSessions executes the provided function on each UserSession in the Cluster.
func (c *BaseCluster) RangeOverSessions(f func(key string, value scheduling.UserSession) bool) {
	c.sessionsMutex.RLock()
	defer c.sessionsMutex.RUnlock()

	c.sessions.Range(f)
}

// RangeOverDisabledHosts executes the provided function on each disabled host in the Cluster.
//
// Importantly, this function does NOT lock the hostsMutex.
func (c *BaseCluster) RangeOverDisabledHosts(f func(key string, value scheduling.Host) bool) {
	c.DisabledHosts.Range(f)
}

// RemoveHost removes the host with the specified ID.
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

// NumDisabledHosts returns the number of host instances in the Cluster that are in the "disabled" state.
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
		c.log.Debug("Cannot register new ScaleOutOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, scheduling.ErrScalingActive
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
		c.log.Debug("Cannot register new ScaleInOperation, as there is already an active %s", c.activeScaleOperation.OperationType)
		return nil, scheduling.ErrScalingActive
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

// DefaultOnScaleOperationFailed is automatically registered when scheduler.ScaleOperation instances are created.
// DefaultOnScaleOperationFailed just updates some relevant statistics.
func (c *BaseCluster) DefaultOnScaleOperationFailed(op scheduling.ScaleOperation) {
	if op == nil {
		c.log.Error("Received nil ScaleOperation in call to BaseCluster::DefaultOnScaleOperationFailed...")
		return
	}

	if op.IsScaleInOperation() {
		c.log.Debug("Scale-In TransactionOperation \"%s\" has failed.", op.GetOperationId())
		c.numFailedScaleInOps += 1
	} else if op.IsScaleOutOperation() {
		c.log.Debug("Scale-Out TransactionOperation \"%s\" has failed.", op.GetOperationId())
		c.numFailedScaleOutOps += 1
	} else {
		c.log.Error("Received ScaleOperation that is neither a scale-out operation nor a scale-in operation...")
	}
}

// MeanScaleOutTime returns the average time to scale-out and add a host to the Cluster.
func (c *BaseCluster) MeanScaleOutTime() time.Duration {
	return c.MeanScaleOutPerHost
}

// StdDevScaleOutTime returns the standard deviation of the time it takes to scale-out
// and add a host to the Cluster.
func (c *BaseCluster) StdDevScaleOutTime() time.Duration {
	return c.StdDevScaleOutPerHost
}

// MeanScaleInTime returns the average time to scale-in and remove a host from the Cluster.
func (c *BaseCluster) MeanScaleInTime() time.Duration {
	return c.MeanScaleInPerHost
}

// StdDevScaleInTime returns the standard deviation of the time it takes to scale-in
// and remove a host from the Cluster.
func (c *BaseCluster) StdDevScaleInTime() time.Duration {
	return c.StdDevScaleInPerHost
}

// RequestHosts requests n host instances to be launched and added to the Cluster, where n >= 1.
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
		c.log.Warn("Could not register new scale-out operation to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err)
	}

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleOutStarted,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Metadata: map[string]interface{}{
					"initial_scale": currentNumNodes,
					"target_scale":  targetNumNodes,
					"operation_id":  scaleOp.OperationId,
				},
			})

			stats.NumActiveScaleOutEvents += 1
		})
	}

	c.log.Debug("Beginning scale-out operation %s from %d nodes to %d nodes.",
		scaleOp.OperationId, scaleOp.InitialScale, scaleOp.TargetScale)

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Warn("Scale-out operation %s from %d nodes to %d nodes failed because: %v",
			scaleOp.OperationId, scaleOp.InitialScale, scaleOp.TargetScale, err)

		// Unregister the failed scale-out operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, err) // status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug(utils.LightGreenStyle.Render("Scale-out operation %s from %d nodes to %d nodes succeeded."),
		scaleOp.OperationId, scaleOp.InitialScale, scaleOp.TargetScale)

	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
	}

	numProvisioned := c.Len() - int(scaleOp.InitialScale)
	if numProvisioned > 0 && c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(statistics *metrics.ClusterStatistics) {

		})
	}

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			var operationStatus string
			if numProvisioned > 0 {
				stats.NumSuccessfulScaleOutEvents.Add(1)
				stats.CumulativeNumHostsProvisioned.Add(int32(numProvisioned))

				if int32(c.Len()) == targetNumNodes {
					operationStatus = "complete_success"
				} else {
					operationStatus = "partial_success"
				}
			} else {
				stats.NumFailedScaleOutEvents.Add(1)
				operationStatus = "total_failure"
			}

			duration, _ := scaleOp.GetDuration()
			stats.CumulativeTimeProvisioningHosts.Add(duration.Seconds())

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleOutEnded,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            duration,
				DurationMillis:      duration.Milliseconds(),
				Metadata: map[string]interface{}{
					"initial_scale":   currentNumNodes,
					"target_scale":    targetNumNodes,
					"resulting_scale": c.Len(),
					"operationStatus": operationStatus,
					"operation_id":    scaleOp.OperationId,
				},
			})

			stats.NumActiveScaleOutEvents -= 1
		})
	}

	return promise.Resolved(result)
}

// ReleaseSpecificHosts terminates one or more specific host instances.
func (c *BaseCluster) ReleaseSpecificHosts(ctx context.Context, ids []string) promise.Promise {
	n := int32(len(ids))
	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < c.minimumCapacity {
		c.log.Warn("Cannot remove %d Local Daemon Docker node(s) from the Cluster, "+
			"as doing so would violate the Cluster's minimum capacity constraint of %d.", n, c.minimumCapacity)
		c.log.Warn("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, fmt.Errorf("%w: "+
			"removing %d nodes would violate minimum capacity constraint of %d",
			scheduling.ErrInvalidTargetNumHosts, n, c.minimumCapacity))
	}

	if targetNumNodes < int32(c.NumReplicas()) {
		c.log.Warn("Cannot remove %d specific Local Daemon Docker node(s) from the Cluster", n)
		c.log.Warn("Current number of Local Daemon Docker nodes: %d", currentNumNodes)
		return promise.Resolved(nil, scheduling.ErrInvalidTargetNumHosts)
	}

	// Register a new scaling operation.
	// The registration process validates that the requested operation makes sense (i.e., we would indeed scale-out based
	// on the current and target Cluster size).
	opId := uuid.NewString()
	scaleOp, err := c.registerScaleInOperation(opId, targetNumNodes, ids)
	if err != nil {
		c.log.Warn("Could not register new scale-in operation down to %d nodes because: %v", targetNumNodes, err)
		return promise.Resolved(nil, err) // This error should already be gRPC compatible...
	}

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			stats.NumActiveScaleInEvents.Add(1)

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleInStarted,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Metadata: map[string]interface{}{
					"initial_scale": currentNumNodes,
					"target_scale":  targetNumNodes,
					"operation_id":  scaleOp.OperationId,
					"target_nodes":  ids,
				},
			})
		})
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Debug("Scale-in from %d nodes down to %d nodes failed because: %v",
			scaleOp.InitialScale, scaleOp.TargetScale, err)

		// Unregister the failed scale-in operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, err) // status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug(utils.LightGreenStyle.Render("Scale-in from %d nodes down to %d nodes succeeded."),
		scaleOp.InitialScale, scaleOp.TargetScale)

	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
	}

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			numReleased := currentNumNodes - int32(c.Len())
			var operationStatus string
			if numReleased > 0 {
				stats.NumSuccessfulScaleInEvents.Add(1)
				stats.CumulativeNumHostsReleased.Add(numReleased)

				if int32(c.Len()) == targetNumNodes {
					operationStatus = "complete_success"
				} else {
					operationStatus = "partial_success"
				}
			} else {
				stats.NumFailedScaleInEvents.Add(1)
				operationStatus = "total_failure"
			}

			duration, _ := scaleOp.GetDuration()
			stats.CumulativeTimeProvisioningHosts.Add(duration.Seconds())

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleInEnded,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            duration,
				DurationMillis:      duration.Milliseconds(),
				Metadata: map[string]interface{}{
					"initial_scale":   currentNumNodes,
					"target_scale":    targetNumNodes,
					"resulting_scale": c.Len(),
					"operationStatus": operationStatus,
					"operation_id":    scaleOp.OperationId,
				},
			})

			stats.NumActiveScaleInEvents -= 1
		})
	}

	return promise.Resolved(result)
}

// ReleaseHosts terminates n arbitrary host instances, where n >= 1.
//
// If n is 0, then ReleaseHosts returns immediately.
//
// If n is negative, then ReleaseHosts returns with an error.
func (c *BaseCluster) ReleaseHosts(ctx context.Context, n int32) promise.Promise {
	currentNumNodes := int32(c.Len())
	targetNumNodes := currentNumNodes - n

	if targetNumNodes < int32(c.NumReplicas()) {
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

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			stats.NumActiveScaleInEvents.Add(1)

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleInStarted,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Metadata: map[string]interface{}{
					"initial_scale": currentNumNodes,
					"target_scale":  targetNumNodes,
					"operation_id":  scaleOp.OperationId,
				},
			})
		})
	}

	// Start the operation.
	result, err := scaleOp.Start(ctx)
	if err != nil {
		c.log.Debug("Scale-in from %d nodes down to %d nodes failed because: %v",
			scaleOp.InitialScale, scaleOp.TargetScale, err)

		// Unregister the failed scale-in operation.
		if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
			c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
		}

		return promise.Resolved(nil, err) // status.Error(codes.Internal, err.Error()))
	}

	c.log.Debug("Scale-in from %d nodes to %d nodes has succeeded.", scaleOp.InitialScale, scaleOp.TargetScale)
	if unregistered := c.unregisterActiveScaleOp(false); !unregistered {
		c.log.Error("Failed to unregister active scale operation %v.", c.activeScaleOperation)
	}

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			numReleased := currentNumNodes - int32(c.Len())
			var operationStatus string
			if numReleased > 0 {
				stats.NumSuccessfulScaleInEvents.Add(1)
				stats.CumulativeNumHostsReleased.Add(numReleased)

				if int32(c.Len()) == targetNumNodes {
					operationStatus = "complete_success"
				} else {
					operationStatus = "partial_success"
				}
			} else {
				stats.NumFailedScaleInEvents.Add(1)
				operationStatus = "total_failure"
			}

			duration, _ := scaleOp.GetDuration()
			stats.CumulativeTimeProvisioningHosts.Add(duration.Seconds())

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.ScaleInEnded,
				KernelId:            "-",
				ReplicaId:           -1,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            duration,
				DurationMillis:      duration.Milliseconds(),
				Metadata: map[string]interface{}{
					"initial_scale":   currentNumNodes,
					"target_scale":    targetNumNodes,
					"resulting_scale": c.Len(),
					"operationStatus": operationStatus,
					"operation_id":    scaleOp.OperationId,
				},
			})

			stats.NumActiveScaleInEvents -= 1
		})
	}

	return promise.Resolved(result)
}

// DisableScalingOut modifies the scaling policy to disallow scaling-out, even if the policy isn't
// supposed to support scaling out. This is only intended to be used for unit tests.
func (c *BaseCluster) DisableScalingOut() {
	c.scheduler.Policy().ResourceScalingPolicy().DisableScalingOut()
}

// EnableScalingOut modifies the scaling policy to enable scaling-out, even if the policy isn't
// supposed to support scaling out. This is only intended to be used for unit tests.
func (c *BaseCluster) EnableScalingOut() {
	c.scheduler.Policy().ResourceScalingPolicy().EnableScalingOut()
}

// ScaleToSize scales the Cluster to the specified number of host instances.
//
// If n <= NUM_REPLICAS, then ScaleToSize returns with an error.
func (c *BaseCluster) ScaleToSize(ctx context.Context, targetNumNodes int32) promise.Promise {
	currentNumNodes := int32(c.Len())

	// Are we trying to scale-down below the minimum cluster size? If so, return an error.
	if targetNumNodes < int32(c.NumReplicas()) {
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
		if !c.scheduler.Policy().ResourceScalingPolicy().ScalingOutEnabled() {
			return promise.Resolved(nil, scheduling.ErrScalingProhibitedBySchedulingPolicy)
		}

		c.log.Debug("Requesting %d additional host(s) in order to scale-out to target size of %d.", targetNumNodes-currentNumNodes, targetNumNodes)
		return c.RequestHosts(ctx, targetNumNodes-currentNumNodes)
	}

	if !c.scheduler.Policy().ResourceScalingPolicy().ScalingInEnabled() {
		return promise.Resolved(nil, scheduling.ErrScalingProhibitedBySchedulingPolicy)
	}

	// Scale in (i.e., remove hosts).
	c.log.Debug("Releasing %d host(s) in order to scale-in to target size of %d.", currentNumNodes-targetNumNodes, targetNumNodes)
	return c.ReleaseHosts(ctx, currentNumNodes-targetNumNodes)
}

// MetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
func (c *BaseCluster) MetricsProvider() scheduling.MetricsProvider {
	return c.metricsProvider
}

// IsInInitialConnectionPeriod returns true if the scheduling.Cluster is still in "initial connection" mode/phase.
func (c *BaseCluster) IsInInitialConnectionPeriod() bool {
	return c.inInitialConnectionPeriod.Load()
}

// NewHostAddedOrConnected should be called by an external entity when a new host connects to the Cluster Gateway.
// NewHostAddedOrConnected handles the logic of adding the host to the Cluster, and in particular will handle the
// task of locking the required structures during scaling operations.
func (c *BaseCluster) NewHostAddedOrConnected(host scheduling.Host) error {
	c.scalingOpMutex.Lock()
	defer c.scalingOpMutex.Unlock()

	initialConnPeriodEnabled := c.initialClusterSize >= 0

	// If we're still in the "initial connection period" and we already have all the hosts we're supposed to have,
	// then we'll disable this host before registering it.
	if initialConnPeriodEnabled && c.inInitialConnectionPeriod.Load() && c.Len() >= c.initialClusterSize {
		c.numHostsDisabledDuringInitialConnectionPeriod += 1
		c.log.Debug("We are still in the Initial Connection Period, and cluster has size %d. Disabling "+
			"newly-connected host %s (ID=%s). Disabled %d host(s) during initial connection period so far.",
			c.Len(), host.GetNodeName(), host.GetID(), c.numHostsDisabledDuringInitialConnectionPeriod)

		err := host.Disable()

		// As of right now, the only reason Disable will fail/return an error is if the host is already disabled.
		if err != nil && !errors.Is(err, scheduling.ErrHostAlreadyDisabled) {
			c.log.Error("Failed to disable newly-connected host %s (ID=%s) because: %v",
				host.GetNodeName(), host.GetID(), err)
			return err
		}
	}

	if !host.Enabled() {
		c.log.Debug("Attempting to add disabled host %s (ID=%s) to the cluster now.",
			host.GetNodeName(), host.GetID())
		err := c.onDisabledHostAdded(host)
		if err != nil {
			c.log.Error("Failed to add disabled host %s (ID=%s) to cluster because: %v",
				host.GetNodeName(), host.GetID(), err)
			return err
		}
		c.log.Debug("Successfully added disabled host %s (ID=%s) to the cluster. Number of disabled hosts: %d.",
			host.GetNodeName(), host.GetID(), c.DisabledHosts.Len())
		return nil
	}

	c.log.Debug("host %s (ID=%s) has just connected to the Cluster or is being re-enabled",
		host.GetNodeName(), host.GetID())

	c.hostMutex.Lock()
	// The host mutex is already locked if we're performing a scaling operation.
	c.hosts.Store(host.GetID(), host)
	c.hostMutex.Unlock()

	c.onHostAdded(host)

	c.log.Debug("Finished handling Cluster-level registration of newly-added host %s (ID=%s)",
		host.GetNodeName(), host.GetID())

	return nil
}

// GetHostEvenIfDisabled returns the Host with the given ID, if one exists, regardless of
// whether the Host is enabled or disabled.
func (c *BaseCluster) GetHostEvenIfDisabled(hostId string) (host scheduling.Host, enabled bool, err error) {
	var loaded bool
	host, loaded = c.hosts.Load(hostId)
	if loaded {
		return host, true, nil
	}

	host, loaded = c.DisabledHosts.Load(hostId)
	if loaded {
		return host, false, nil
	}

	return nil, false, fmt.Errorf("%w: host \"%s\"", scheduling.ErrHostNotFound, hostId)
}

// GetHost returns the host with the given ID, if one exists.
func (c *BaseCluster) GetHost(hostId string) (scheduling.Host, bool) {
	return c.hosts.Load(hostId)
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
// logic for scaling-out.
//
// targetNumNodes specifies the desired size of the Cluster.
//
// resultChan is used to notify a waiting goroutine that the scale-out operation has finished.
func (c *BaseCluster) GetScaleOutCommand(targetNumNodes int32, resultChan chan interface{}, scaleOpId string) func() {
	return c.instance.GetScaleOutCommand(targetNumNodes, resultChan, scaleOpId)
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
func (c *BaseCluster) GetScaleInCommand(targetNumNodes int32, targetHosts []string, resultChan chan interface{}) (func(), error) {
	return c.instance.GetScaleInCommand(targetNumNodes, targetHosts, resultChan)
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
