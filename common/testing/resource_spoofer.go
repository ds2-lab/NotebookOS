package testing

import (
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync/atomic"
	"time"
)

// ResourceSpoofer is used to provide spoofed resources to be used by mock_proto.MockLocalGatewayClient
// instances when spoofing calls to proto.LocalGatewayClient.ResourcesSnapshot.
type ResourceSpoofer struct {
	SnapshotId atomic.Int32
	HostId     string
	NodeName   string
	ManagerId  string
	HostSpec   types.Spec
	Manager    *resource.Manager
}

func NewResourceSpoofer(nodeName string, hostId string, hostSpec types.Spec) *ResourceSpoofer {
	spoofer := &ResourceSpoofer{
		HostId:    hostId,
		NodeName:  nodeName,
		ManagerId: uuid.NewString(),
		HostSpec:  hostSpec,
		Manager:   resource.NewManager(hostSpec),
	}

	ginkgo.GinkgoWriter.Printf("Created new ResourceSpoofer for host %s (ID=%s) with spec=%s\n", nodeName, hostId, hostSpec.String())

	return spoofer
}

func (s *ResourceSpoofer) ResourcesSnapshot(_ context.Context, _ *proto.Void, _ ...grpc.CallOption) (*proto.NodeResourcesSnapshotWithContainers, error) {
	snapshotId := s.SnapshotId.Add(1)
	resourceSnapshot := &proto.NodeResourcesSnapshot{
		// SnapshotId uniquely identifies the NodeResourcesSnapshot and defines a total order amongst all NodeResourcesSnapshot
		// structs originating from the same node. Each newly-created NodeResourcesSnapshot is assigned an ID from a
		// monotonically-increasing counter by the ResourceManager.
		SnapshotId: snapshotId,
		// NodeId is the ID of the node from which the resourceSnapshot originates.
		NodeId: s.HostId,
		// ManagerId is the unique ID of the ResourceManager struct from which the NodeResourcesSnapshot was constructed.
		ManagerId: s.ManagerId,
		// Timestamp is the time at which the NodeResourcesSnapshot was taken/created.
		Timestamp:          timestamppb.New(time.Now()),
		IdleResources:      s.Manager.IdleProtoResourcesSnapshot(snapshotId),
		PendingResources:   s.Manager.PendingProtoResourcesSnapshot(snapshotId),
		CommittedResources: s.Manager.CommittedProtoResourcesSnapshot(snapshotId),
		SpecResources:      s.Manager.SpecProtoResourcesSnapshot(snapshotId),
	}

	snapshotWithContainers := &proto.NodeResourcesSnapshotWithContainers{
		Id:               uuid.NewString(),
		ResourceSnapshot: resourceSnapshot,
		Containers:       make([]*proto.ReplicaInfo, 0), // TODO: Incorporate this field into ResourceSpoofer.
	}

	// fmt.Printf("Spoofing resources for Host \"%s\" (ID=\"%s\") with SnapshotID=%d now: %v\n", s.NodeName, s.HostId, SnapshotId, resourceSnapshot.String())
	return snapshotWithContainers, nil
}
