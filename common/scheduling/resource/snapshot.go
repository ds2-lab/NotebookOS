package resource

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"log"
	"reflect"
	"time"
)

// ManagerSnapshot encapsulates a JSON-compatible snapshot of the resource quantities of the AllocationManager.
type ManagerSnapshot struct {
	// SnapshotId uniquely identifies the ManagerSnapshot and defines a total order amongst all ManagerSnapshot
	// structs originating from the same node. Each newly-created ManagerSnapshot is assigned an ID from a
	// monotonically-increasing counter by the AllocationManager.
	SnapshotId int32 `json:"snapshot_id" mapstructure:"snapshot_id"`

	// NodeId is the ID of the node from which the snapshot originates.
	NodeId string `json:"host_id" mapstructure:"host_id"`

	// ManagerId is the unique ID of the AllocationManager struct from which the ManagerSnapshot was constructed.
	ManagerId string `json:"manager_id" mapstructure:"manager_id"`

	// Timestamp is the time at which the ManagerSnapshot was taken/created.
	Timestamp time.Time `json:"timestamp" mapstructure:"timestamp"`

	IdleResources      *ComputeResourceSnapshot `json:"idle_resources" mapstructure:"idle_resources"`
	PendingResources   *ComputeResourceSnapshot `json:"pending_resources" mapstructure:"pending_resources"`
	CommittedResources *ComputeResourceSnapshot `json:"committed_resources" mapstructure:"committed_resources"`
	SpecResources      *ComputeResourceSnapshot `json:"spec_resources" mapstructure:"spec_resources"`

	// Containers are the Containers presently running on the Host.
	Containers []*proto.ReplicaInfo `json:"containers,omitempty" mapstructure:"containers,omitempty"`
}

// MetadataResourceWrapperSnapshot is a simpel wrapper around a ManagerSnapshot struct so that
// we can deserialize a ManagerSnapshot struct from the metadata frame of a JupyterMessage, which
// is typically a map[string]interface{}.
type MetadataResourceWrapperSnapshot struct {
	ManagerSnapshot *ManagerSnapshot `json:"resource_snapshot"`
}

func (s *ManagerSnapshot) GetContainers() []types.ContainerInfo {
	containers := make([]types.ContainerInfo, 0, len(s.Containers))

	for _, container := range s.Containers {
		containers = append(containers, container)
	}

	return containers
}

////////////////////////////////////////////////////
// HostResourceSnapshot interface implementation. //
////////////////////////////////////////////////////

// Compare compares the object with specified object.
// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
func (s *ManagerSnapshot) Compare(obj interface{}) float64 {
	if obj == nil {
		log.Fatalf("Cannot compare target ManagerSnapshot with nil.")
	}

	other, ok := obj.(types.ArbitraryResourceSnapshot)
	if !ok {
		log.Fatalf("Cannot compare target ManagerSnapshot with specified object of type '%s'.",
			reflect.ValueOf(obj).Type().String())
	}

	if s.GetSnapshotId() < other.GetSnapshotId() {
		return -1
	} else if s.GetSnapshotId() == other.GetSnapshotId() {
		return 0
	} else {
		return 1
	}
}

// String returns a string representation of the ManagerSnapshot suitable for logging/printing.
func (s *ManagerSnapshot) String() string {
	idle := "nil"
	if s.IdleResources != nil {
		idle = s.IdleResources.String()
	}

	pending := "nil"
	if s.PendingResources != nil {
		pending = s.PendingResources.String()
	}

	committed := "nil"
	if s.CommittedResources != nil {
		committed = s.CommittedResources.String()
	}

	spec := "nil"
	if s.SpecResources != nil {
		spec = s.SpecResources.String()
	}

	return fmt.Sprintf("ManagerSnapshot[SnapshotID=%d, HostId=%s, ManagerID=%s, Timestamp=%v, Idle=%s, Pending=%s, Committed=%s, Spec=%s]",
		s.SnapshotId, s.NodeId, s.ManagerId, s.Timestamp, idle, pending, committed, spec)
}

// GetSnapshotId is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetSnapshotId() int32 {
	return s.SnapshotId
}

// GetNodeId is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetNodeId() string {
	return s.NodeId
}

// GetManagerId is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetManagerId() string {
	return s.ManagerId
}

// GetGoTimestamp is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetGoTimestamp() time.Time {
	return s.Timestamp
}

// GetIdleResources is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetIdleResources() types.ArbitraryResourceSnapshot {
	return s.IdleResources
}

// GetPendingResources is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetPendingResources() types.ArbitraryResourceSnapshot {
	return s.PendingResources
}

// GetCommittedResources is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetCommittedResources() types.ArbitraryResourceSnapshot {
	return s.CommittedResources
}

// GetSpecResources is part of the HostResourceSnapshot interface implementation.
func (s *ManagerSnapshot) GetSpecResources() types.ArbitraryResourceSnapshot {
	return s.SpecResources
}

// ComputeResourceSnapshot is a snapshot of a HostResources struct with exported
// fields so that it can be marshalled and unmarshalled to JSON.
type ComputeResourceSnapshot struct {
	ResourceStatus Status          `json:"resource_status"` // resourceStatus is the ResourceStatus represented/encoded by this struct.
	Millicpus      decimal.Decimal `json:"cpus"`            // millicpus is CPU in 1/1000th of CPU core.
	Gpus           decimal.Decimal `json:"gpus"`            // gpus is the number of GPUs.
	VRamGB         decimal.Decimal `json:"vram"`            // VRamGB is the amount of VRAM (GPU memory) in GBs.
	MemoryMB       decimal.Decimal `json:"memoryMB"`        // memoryMB is the amount of memory in MB.

	// SnapshotId uniquely identifies the HostResourceSnapshot in which this ComputeResourceSnapshot struct will be included.
	// Specifically, the SnapshotId and defines a total order amongst all HostResourceSnapshot instances that originate
	// from the same node. Each newly-created HostResourceSnapshot is assigned an ID from a monotonically-increasing
	// counter by the AllocationManager from the associated Host.
	SnapshotId int32 `json:"snapshot_id"`
}

func (s *ComputeResourceSnapshot) GetSnapshotId() int32 {
	return s.SnapshotId
}

func (s *ComputeResourceSnapshot) GetResourceStatus() string {
	return s.ResourceStatus.String()
}

func (s *ComputeResourceSnapshot) GetMillicpus() int32 {
	return int32(s.Millicpus.InexactFloat64())
}

func (s *ComputeResourceSnapshot) GetMemoryMb() float32 {
	return float32(s.MemoryMB.InexactFloat64())
}

func (s *ComputeResourceSnapshot) GetGpus() int32 {
	return int32(s.Gpus.InexactFloat64())
}

func (s *ComputeResourceSnapshot) GetVramGb() float32 {
	return float32(s.VRamGB.InexactFloat64())
}

// String returns a string representation of the target ComputeResourceSnapshot struct that is suitable for logging.
func (s *ComputeResourceSnapshot) String() string {
	return fmt.Sprintf("ComputeResourceSnapshot[Status=%s,Millicpus=%s,MemoryMB=%s,GPUs=%s,VRAM=%s",
		s.ResourceStatus.String(), s.Millicpus.StringFixed(4), s.MemoryMB.StringFixed(4),
		s.Gpus.StringFixed(1), s.VRamGB.StringFixed(4))
}

func (s *ComputeResourceSnapshot) ToProtoResourcesSnapshot() *proto.ResourcesSnapshot {
	return &proto.ResourcesSnapshot{
		ResourceStatus: s.ResourceStatus.String(),
		Millicpus:      int32(s.Millicpus.InexactFloat64()),
		Gpus:           int32(s.Gpus.InexactFloat64()),
		VramGb:         float32(s.VRamGB.InexactFloat64()),
		MemoryMb:       float32(s.MemoryMB.InexactFloat64()),
		SnapshotId:     s.SnapshotId,
	}
}
