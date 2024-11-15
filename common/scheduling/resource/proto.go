package resource

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

// ProtoNodeResourcesSnapshotWrapper is a wrapper around the proto.NodeResourcesSnapshot struct that
// provides a way for a proto.NodeResourcesSnapshot struct to satisfy the types.HostResourceSnapshot interface.
type ProtoNodeResourcesSnapshotWrapper struct {
	*proto.NodeResourcesSnapshotWithContainers
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetIdleResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.IdleResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetPendingResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.PendingResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetCommittedResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.CommittedResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetSpecResources() types.ArbitraryResourceSnapshot {
	return w.ResourceSnapshot.SpecResources
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetContainers() []types.ContainerInfo {
	containerInfo := make([]types.ContainerInfo, 0, len(w.Containers))
	for _, container := range w.Containers {
		containerInfo = append(containerInfo, container)
	}
	return containerInfo
}

func (w *ProtoNodeResourcesSnapshotWrapper) String() string {
	return w.NodeResourcesSnapshotWithContainers.String()
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetSnapshotId() int32 {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.SnapshotId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetNodeId() string {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.NodeId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetManagerId() string {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.ManagerId
}

func (w *ProtoNodeResourcesSnapshotWrapper) GetGoTimestamp() time.Time {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.GetGoTimestamp()
}

func (w *ProtoNodeResourcesSnapshotWrapper) Compare(obj interface{}) float64 {
	return w.NodeResourcesSnapshotWithContainers.ResourceSnapshot.Compare(obj)
}
