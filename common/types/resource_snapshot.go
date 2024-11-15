package types

import (
	"github.com/zhangjyr/distributed-notebook/common/container"
	"time"
)

// HostResourceSnapshot is an interface exposed by structs that encode snapshots of resource quantities
// on a particular Host at a particular time.
//
// This interface exists so that we can use the proto.NodeResourcesSnapshot and ManagerSnapshot structs
// interchangeably/in a type-agnostic way.
type HostResourceSnapshot[T ArbitraryResourceSnapshot] interface {
	container.Comparable

	String() string
	GetSnapshotId() int32
	GetNodeId() string
	GetManagerId() string
	GetGoTimestamp() time.Time
	GetIdleResources() T
	GetPendingResources() T
	GetCommittedResources() T
	GetSpecResources() T
	GetContainers() []ContainerInfo
}

// ContainerInfo encodes information about a single kernel replica container, namely the kernel ID and replica ID.
type ContainerInfo interface {
	GetKernelId() string
	GetReplicaId() int32
}

// ArbitraryResourceSnapshot is an interface exposed by structs that encode snapshots of resource quantities
// on a particular Host at a particular time.
//
// ArbitraryResourceSnapshot is "arbitrary" in that it could be a gRPC/protobuf-based struct or a ResourceSnapshot
// struct from our scheduling module/package. Indeed, this interface exists so that we can use the
// proto.ResourcesSnapshot and ResourceSnapshot structs interchangeably/in a type-agnostic way.
type ArbitraryResourceSnapshot interface {
	String() string
	GetResourceStatus() string
	GetMillicpus() int32
	GetMemoryMb() float32
	GetGpus() int32
	GetVramGb() float32
	GetSnapshotId() int32
}
