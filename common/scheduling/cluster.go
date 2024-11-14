package scheduling

import (
	"github.com/Scusemua/go-utils/promise"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/cluster"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/entity"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/scheduler"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"golang.org/x/net/context"
)

// Cluster defines the interface for a BaseCluster that is responsible for:
// 1. Launching and terminating hosts.
// 2. Providing a global view of all hosts with multiple indexes.
// 3. Providing a statistics of the hosts.
type Cluster interface {
	GetScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error)
	GetScaleOutCommand(targetNumNodes int32, coreLogicDoneChan chan interface{}) func()
	MetricsProvider() MetricsProvider

	// Sessions returns a mapping from session ID to Session.
	Sessions() hashmap.HashMap[string, UserSession]

	// AddSession adds a Session to the Cluster.
	AddSession(sessionId string, session UserSession)

	// GetIndex returns the IndexProvider whose key is created with the given category and expected values.
	// The category and expected values are returned by the IndexProvider.Category method.
	GetIndex(category string, expected interface{}) (cluster.IndexProvider, bool)

	// GetSession returns the Session with the specified ID.
	//
	// We return the UserSession so that we can use this in unit tests with a mocked Session.
	GetSession(sessionID string) (UserSession, bool)

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

	// AddIndex adds an index to the BaseCluster. For each category and expected value, there can be only one index.
	AddIndex(index cluster.IndexProvider) error

	// ActiveScaleOperation returns the active scaling operation, if one exists.
	// If there is no active scaling operation, then ActiveScaleOperation returns nil.
	ActiveScaleOperation() *scheduler.ScaleOperation

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

	// Scheduler returns the Scheduler used by the Cluster.
	Scheduler() Scheduler

	// Placer returns the Placer used by the Cluster.
	Placer() Placer

	// ReadLockHosts locks the underlying host manager such that no Host instances can be added or removed.
	ReadLockHosts()

	// ReadUnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
	ReadUnlockHosts()

	// NewHostAddedOrConnected should be called by an external entity when a new Host connects to the Cluster Gateway.
	// NewHostAddedOrConnected handles the logic of adding the Host to the Cluster, and in particular will handle the
	// task of locking the required structures during scaling operations.
	NewHostAddedOrConnected(host *entity.Host) error

	// RemoveHost removes the Host with the specified ID.
	// This is called when a Local Daemon loses connection.
	RemoveHost(hostId string)

	// Len returns the current size of the Cluster (i.e., the number of Host instances within the Cluster).
	Len() int

	// NumDisabledHosts returns the number of Host instances in the Cluster that are in the "disabled" state.
	NumDisabledHosts() int

	// GetHost returns the Host with the given ID, if one exists.
	GetHost(hostId string) (*entity.Host, bool)

	// RangeOverHosts executes the provided function on each Host in the Cluster.
	//
	// Importantly, this function does NOT lock the hostsMutex.
	RangeOverHosts(f func(key string, value *entity.Host) bool)

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

	// NumReplicasAsDecimal returns the numer of replicas that each Jupyter kernel has associated with it as
	// a decimal.Decimal struct.
	//
	// This value is typically equal to 3, but may be altered in the system configuration.
	//
	// This API exists as basically an optimization so we can return a cached decimal.Decimal struct,
	// rather than recreate it each time we need it.
	NumReplicasAsDecimal() decimal.Decimal

	// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
	// the given ratio and the Cluster's current subscription ratio.
	//
	// Cluster's GetOversubscriptionFactor simply calls the GetOversubscriptionFactor method of the
	// Cluster's ClusterScheduler.
	GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal
}
