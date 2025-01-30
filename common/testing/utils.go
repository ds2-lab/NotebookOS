package testing

import (
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/mock_proto"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"go.uber.org/mock/gomock"
)

func ContainsOffendingResourceKind(lst []resource.Kind, target resource.Kind) bool {
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

	gpuSchedulerId := uuid.NewString()

	localGatewayClient := mock_proto.NewMockLocalGatewayClient(ctrl)

	localGatewayClient.EXPECT().SetID(
		gomock.Any(),
		&proto.HostId{
			Id: hostId,
		},
	).Return(&proto.HostId{
		Id:       hostId,
		NodeName: nodeName,
	}, nil)

	localGatewayClient.EXPECT().GetActualGpuInfo(
		gomock.Any(),
		&proto.Void{},
	).Return(&proto.GpuInfo{
		SpecGPUs:              int32(resourceSpoofer.Manager.SpecResources().GPUs()),
		IdleGPUs:              int32(resourceSpoofer.Manager.SpecResources().GPUs()),
		CommittedGPUs:         0,
		PendingGPUs:           0,
		NumPendingAllocations: 0,
		NumAllocations:        0,
		GpuSchedulerID:        gpuSchedulerId,
		LocalDaemonID:         hostId,
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
