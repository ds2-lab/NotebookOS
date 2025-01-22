package cluster

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
)

var (
	ErrDuplicatedIndexDefined = fmt.Errorf("duplicated index defined")
	// ErrDuplicateScaleOperation = errors.New("scale operation with same ID already exists")
)

// internalCluster defines a non-exported API that must be provided by all Cluster implementations.
type internalCluster interface {
	scheduling.Cluster

	// RegisterScaleOutOperation registers a scale-out operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleOutOperation(string, int32) (*scheduler.ScaleOperation, error)

	// RegisterScaleInOperation registers a scale-in operation.
	// When the operation completes, a notification is sent on the channel associated with the ScaleOperation.
	registerScaleInOperation(string, int32, []string) (*scheduler.ScaleOperation, error)
}
