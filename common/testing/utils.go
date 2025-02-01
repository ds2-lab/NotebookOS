package testing

import (
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"go.uber.org/mock/gomock"
)

func ContainsOffendingResourceKind(lst []scheduling.ResourceKind, target scheduling.ResourceKind) bool {
	for _, elem := range lst {
		if elem == target {
			return true
		}
	}

	return false
}

// NewHostWithSpoofedGRPC creates a new scheduling.Host struct with a spoofed proto.LocalGatewayClient.
func NewHostWithSpoofedGRPC(ctrl *gomock.Controller, cluster scheduling.Cluster, hostId string, nodeName string,
	resourceSpoofer *ResourceSpoofer) (scheduling.UnitTestingHost, *mock_proto.MockLocalGatewayClient, error) {

	//gpuSchedulerId := uuid.NewString()

	localGatewayClient := mock_proto.NewMockLocalGatewayClient(ctrl)

	localGatewayClient.EXPECT().SetID(
		gomock.Any(),
		&proto.HostId{
			Id: hostId,
		},
	).Return(&proto.HostId{
		Id:       hostId,
		NodeName: nodeName,
		SpecResources: &proto.ResourceSpec{
			Cpu:    int32(resourceSpoofer.Manager.SpecResources().Millicpus()),
			Memory: float32(resourceSpoofer.Manager.SpecResources().MemoryMB()),
			Gpu:    int32(resourceSpoofer.Manager.SpecResources().GPUs()),
			Vram:   float32(resourceSpoofer.Manager.SpecResources().VRAM()),
		},
	}, nil)

	localGatewayClient.EXPECT().ResourcesSnapshot(
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(resourceSpoofer.ResourcesSnapshot).AnyTimes()

	host, err := entity.NewHost(hostId, "0.0.0.0", 3, cluster, cluster, nil,
		localGatewayClient, cluster.Scheduler().Policy(),
		func(_ string, _ string, _ string, _ string) error { return nil })

	return entity.NewUnitTestingHost(host), localGatewayClient, err
}
