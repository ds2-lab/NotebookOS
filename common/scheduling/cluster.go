package scheduling

import (
	"github.com/Scusemua/go-utils/promise"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"golang.org/x/net/context"
)

type ClusterSessionManager interface {
	// Sessions returns a mapping from session ID to Session.
	Sessions() hashmap.HashMap[string, UserSession]

	// AddSession adds a Session to the Cluster.
	AddSession(sessionId string, session UserSession)

	// GetSession returns the Session with the specified ID.
	//
	// We return the UserSession so that we can use this in unit tests with a mocked Session.
	GetSession(sessionID string) (UserSession, bool)
}

type ClusterHostManager interface {
	// RequestHosts requests n Host instances to be launched and added to the Cluster, where n >= 1.
	//
	// If n is 0, then this returns immediately.
	//
	// If n is negative, then this returns with an error.
	RequestHosts(ctx context.Context, n int32) promise.Promise

	// ReleaseSpecificHosts terminates one or more specific Host instances.
	ReleaseSpecificHosts(ctx context.Context, ids []string) promise.Promise

	// ReleaseHosts terminates n arbitrary Host instances, where n >= 1.
	//
	// If n is 0, then ReleaseHosts returns immediately.
	//
	// If n is negative, then ReleaseHosts returns with an error.
	ReleaseHosts(ctx context.Context, n int32) promise.Promise

	// ScaleToSize scales the Cluster to the specified number of Host instances.
	//
	// If n <= NUM_REPLICAS, then ScaleToSize returns with an error.
	ScaleToSize(ctx context.Context, targetNumNodes int32) promise.Promise

	// RemoveHost removes the Host with the specified ID.
	// This is called when a DefaultSchedulingPolicy Daemon loses connection.
	RemoveHost(hostId string)

	// NewHostAddedOrConnected should be called by an external entity when a new Host connects to the Cluster Gateway.
	// NewHostAddedOrConnected handles the logic of adding the Host to the Cluster, and in particular will handle the
	// task of locking the required structures during scaling operations.
	NewHostAddedOrConnected(host Host) error

	// GetHost returns the Host with the given ID, if one exists.
	GetHost(hostId string) (Host, bool)

	// RangeOverHosts executes the provided function on each enabled Host in the Cluster.
	//
	// Importantly, this function does NOT lock the hostsMutex.
	RangeOverHosts(f func(key string, value Host) bool)

	// RangeOverDisabledHosts executes the provided function on each disabled Host in the Cluster.
	//
	// Importantly, this function does NOT lock the hostsMutex.
	RangeOverDisabledHosts(f func(key string, value Host) bool)

	// ReadLockHosts locks the underlying host manager such that no Host instances can be added or removed.
	ReadLockHosts()

	// ReadUnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
	ReadUnlockHosts()

	// NumDisabledHosts returns the number of Host instances in the Cluster that are in the "disabled" state.
	NumDisabledHosts() int

	// Len returns the current size of the Cluster (i.e., the number of Host instances within the Cluster).
	Len() int
}

type ScalingManager interface {
	ScalingMetricsManager

	// GetScaleInCommand returns the function to be executed to perform a scale-in.
	// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
	// logic for scaling-in.
	//
	// targetScale specifies the desired size of the Cluster.
	//
	// targetHosts specifies any specific hosts that are to be removed.
	//
	// resultChan is used to notify a waiting goroutine that the scale-in operation has finished.
	//
	// If there's an error, then you send the error over the result chan.
	// If it succeeds, then you send a struct{}{} indicating that the core logic has finished.
	//
	// IMPORTANT: this method should be called while the hostMutex is already held.
	GetScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error)

	// GetScaleOutCommand returns the function to be executed to perform a scale-out.
	// This API exists so each platform-specific Cluster implementation can provide its own platform-specific
	// logic for scaling-out.
	//
	// targetScale specifies the desired size of the Cluster.
	//
	// resultChan is used to notify a waiting goroutine that the scale-out operation has finished.
	//
	// If there's an error, then you send the error over the result chan.
	// If it succeeds, then you send a struct{}{} indicating that the core logic has finished.
	//
	// IMPORTANT: this method should be called while the hostMutex is already held.
	GetScaleOutCommand(targetNumNodes int32, coreLogicDoneChan chan interface{}) func()

	// CanPossiblyScaleOut returns true if the Cluster could possibly scale-out.
	// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
	// it is currently not supported unless there is at least one disabled host already within the cluster.
	CanPossiblyScaleOut() bool
}

type ScalingMetricsManager interface {
	// NumScalingOperationsSucceeded returns the number of scale-in and scale-out operations that have been
	// completed successfully.
	NumScalingOperationsSucceeded() int

	// NumScaleOutOperationsSucceeded returns the number of scale-out operations that have been
	// completed successfully.
	NumScaleOutOperationsSucceeded() int

	// NumScaleInOperationsSucceeded returns the number of scale-in operations that have been
	// completed successfully.
	NumScaleInOperationsSucceeded() int

	// NumScalingOperationsAttempted returns the number of scale-in and scale-out operations that have been
	// attempted (i.e., count of both successful and failed operations).
	NumScalingOperationsAttempted() int

	// NumScaleOutOperationsAttempted returns the number of scale-out operations that have been
	// attempted (i.e., count of both successful and failed operations).
	NumScaleOutOperationsAttempted() int

	// NumScaleInOperationsAttempted returns the number of scale-in operations that have been
	// attempted (i.e., count of both successful and failed operations).
	NumScaleInOperationsAttempted() int

	// NumScalingOperationsFailed returns the number of scale-in and scale-out operations that have failed.
	NumScalingOperationsFailed() int

	// NumScaleOutOperationsFailed returns the number of scale-out operations that have failed.
	NumScaleOutOperationsFailed() int

	// NumScaleInOperationsFailed returns the number of scale-in operations that have failed.
	NumScaleInOperationsFailed() int
}

type IndexManager interface {
	// GetIndex returns the IndexProvider whose key is created with the given category and expected values.
	// The category and expected values are returned by the IndexProvider.Category method.
	GetIndex(category string, expected interface{}) (IndexProvider, bool)

	// AddIndex adds an index to the BaseCluster. For each category and expected value, there can be only one index.
	AddIndex(index IndexProvider) error
}

type ClusterMetricsManager interface {
	MetricsProvider() MetricsProvider

	// BusyGPUs returns the number of GPUs that are actively committed to kernel replicas right now.
	BusyGPUs() float64

	// DemandGPUs returns the number of GPUs that are required by all actively-running Sessions.
	DemandGPUs() float64

	// SubscriptionRatio returns the SubscriptionRatio of the Cluster.
	SubscriptionRatio() float64

	// NodeType returns the type of node provisioned within the Cluster.
	NodeType() string

	// NumReplicas returns the numer of replicas that each Jupyter kernel has associated with it.
	// This is typically equal to 3, but may be altered in the system configuration.
	NumReplicas() int

	// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
	// the given ratio and the Cluster's current subscription ratio.
	//
	// Cluster's GetOversubscriptionFactor simply calls the GetOversubscriptionFactor method of the
	// Cluster's ClusterScheduler.
	GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal
}

type ComponentManager interface {
	// Scheduler returns the Scheduler used by the Cluster.
	Scheduler() Scheduler

	// Placer returns the Placer used by the Cluster.
	Placer() Placer
}

// Cluster defines the interface for a BaseCluster that is responsible for:
// 1. Launching and terminating hosts.
// 2. Providing a global view of all hosts with multiple indexes.
// 3. Providing a statistics of the hosts.
type Cluster interface {
	ComponentManager
	ScalingManager
	IndexManager
	ClusterMetricsManager
	ClusterHostManager
	ClusterSessionManager
}
