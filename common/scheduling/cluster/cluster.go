package cluster

import (
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrDuplicatedIndexDefined = fmt.Errorf("duplicated index defined")
	ErrScalingActive          = status.Error(codes.FailedPrecondition, "there is already an active scaling operation taking place")
	// ErrDuplicateScaleOperation = errors.New("scale operation with same ID already exists")
)

// internalCluster defines a non-exported API that must be provided by all Cluster implementations.
type internalCluster interface {
	scheduling.Cluster

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
	registerScaleOperation(string, int32) (*scheduler.ScaleOperation, error)

	// RegisterScaleOutOperation registers a scale-out operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleOutOperation(string, int32) (*scheduler.ScaleOperation, error)

	// RegisterScaleInOperation registers a scale-in operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleInOperation(string, int32, []string) (*scheduler.ScaleOperation, error)

	// canPossiblyScaleOut returns true if the Cluster could possibly scale-out.
	// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
	// it is currently not supported unless there is at least one disabled host already within the cluster.
	canPossiblyScaleOut() bool

	// ClusterMetricsProvider returns the metrics.ClusterMetricsProvider of the Cluster.
	ClusterMetricsProvider() metrics.ClusterMetricsProvider
}
