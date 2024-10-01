package scheduling

import (
	"context"
	"fmt"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
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

// HostCriteriaFunction is used by a ClusterIndexQuerier, specifically in its implementation of
// SeekMultipleFrom, to determine if a Host will be considered viable by the Caller before returning it.
//
// A HostCriteriaFunction accepts a Host as an argument and returns a boolean indicating whether the Host
// is viable (true) or not (false) based on whatever criteria are defined and implemented within the
// HostCriteriaFunction function body.
type HostCriteriaFunction func(*Host) bool

type ClusterIndexQuerier interface {
	// Seek returns the host specified by the metrics.
	Seek(blacklist []interface{}, metrics ...[]float64) (host *Host, pos interface{})

	// SeekFrom continues the seek from the position.
	SeekFrom(start interface{}, metrics ...[]float64) (host *Host, pos interface{})

	// SeekMultipleFrom seeks n Host instances from a random permutation of the index.
	// Pass nil as pos to reset the seek.
	//
	// This entire method is thread-safe. The index is locked until this method returns.
	SeekMultipleFrom(pos interface{}, n int, criteriaFunc HostCriteriaFunction, blacklist []interface{}, metrics ...[]float64) ([]*Host, interface{})
}

type ClusterIndex interface {
	ClusterIndexProvider
	ClusterIndexQuerier
}

// clusterInternal defines a non-exported API that must be provided by all Cluster implementations.
type clusterInternal interface {
	Cluster

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
	getScaleOutCommand(targetScale int32, coreLogicDoneChan chan interface{}) func()

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
	getScaleInCommand(targetScale int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error)

	// RegisterScaleOperation registers a non-specific type of ScaleOperation.
	// Specifically, whether the resulting scheduling.ScaleOperation is a ScaleOutOperation or a ScaleInOperation
	// depends on how the target node count compares to the current node count.
	//
	// If the target node count is greater than the current node count, then a ScaleOutOperation is created,
	// registered, and returned.
	//
	// Alternatively, if the target node count is less than the current node count, then a ScaleInOperation is created,
	// registered, and returned.
	registerScaleOperation(string, int32) (*ScaleOperation, error)

	// RegisterScaleOutOperation registers a scale-out operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleOutOperation(string, int32) (*ScaleOperation, error)

	// RegisterScaleInOperation registers a scale-in operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleInOperation(string, int32, []string) (*ScaleOperation, error)

	// ClusterMetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
	ClusterMetricsProvider() metrics.ClusterMetricsProvider
}

// Cluster defines the interface for a BaseCluster that is responsible for:
// 1. Launching and terminating hosts.
// 2. Providing a global view of all hosts with multiple indexes.
// 3. Providing a statistics of the hosts.
type Cluster interface {
	// Sessions returns a mapping from session ID to Session.
	Sessions() hashmap.HashMap[string, *Session]

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
	AddIndex(index ClusterIndexProvider) error

	// ActiveScaleOperation returns the active scaling operation, if one exists.
	// If there is no active scaling operation, then ActiveScaleOperation returns nil.
	ActiveScaleOperation() *ScaleOperation

	// ClusterScheduler returns the ClusterScheduler used by the Cluster.
	ClusterScheduler() ClusterScheduler

	// Placer returns the Placer used by the Cluster.
	Placer() Placer

	// ReadLockHosts locks the underlying host manager such that no Host instances can be added or removed.
	ReadLockHosts()

	// ReadUnlockHosts unlocks the underlying host manager, enabling the addition or removal of Host instances.
	ReadUnlockHosts()

	// NewHostAddedOrConnected should be called by an external entity when a new Host connects to the Cluster Gateway.
	// NewHostAddedOrConnected handles the logic of adding the Host to the Cluster, and in particular will handle the
	// task of locking the required structures during scaling operations.
	NewHostAddedOrConnected(host *Host)

	// RemoveHost removes the Host with the specified ID.
	// This is called when a Local Daemon loses connection.
	RemoveHost(hostId string)

	// Len returns the current size of the Cluster (i.e., the number of Host instances within the Cluster).
	Len() int

	// GetHost returns the Host with the given ID, if one exists.
	GetHost(hostId string) (*Host, bool)

	// RangeOverHosts executes the provided function on each Host in the Cluster.
	//
	// Importantly, this function does NOT lock the hostsMutex.
	RangeOverHosts(f func(key string, value *Host) bool)

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
