package placer_test

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	jupyter "github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	distNbTesting "github.com/scusemua/distributed-notebook/common/testing"
	"github.com/scusemua/distributed-notebook/common/types"
	"go.uber.org/mock/gomock"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	debugLoggingEnabled = false
)

func init() {
	if os.Getenv("DEBUG") != "" || os.Getenv("VERBOSE") != "" {
		debugLoggingEnabled = true
	}
}

func TestPlacer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Placer Suite")
}

var _ = BeforeSuite(func() {
	if debugLoggingEnabled {
		config.LogLevel = logger.LOG_LEVEL_ALL
	}
})

func releaseResources(host scheduling.UnitTestingHost, resources *types.DecimalSpec, dockerCluster scheduling.Cluster,
	gpuDeviceIdsToAdd []int) {

	err := host.SubtractFromCommittedResources(resources)
	Expect(err).To(BeNil())

	err = host.AddToIdleResources(resources)
	Expect(err).To(BeNil())

	fmt.Printf("\nReleased the following resources from Host %s (ID=%s): %v\n",
		host.GetNodeName(), host.GetID(), resources.String())
	fmt.Printf("Host %s now has the following idle resources: %v\n",
		host.GetNodeName(), host.IdleResources().String())
	fmt.Printf("Host %s now has the following committed resources: %v\n\n",
		host.GetNodeName(), host.CommittedResources().String())

	err = dockerCluster.UpdateIndex(host)
	Expect(err).To(BeNil())

	if gpuDeviceIdsToAdd != nil && len(gpuDeviceIdsToAdd) > 0 {
		host.AddGpuDeviceIds(gpuDeviceIdsToAdd)
	}
}

func createHost(idx int, dockerCluster scheduling.Cluster, mockCtrl *gomock.Controller, hostSpec *types.DecimalSpec) (scheduling.UnitTestingHost, *distNbTesting.ResourceSpoofer) {
	Expect(dockerCluster).ToNot(BeNil())

	hostId := uuid.NewString()
	hostName := fmt.Sprintf("TestHost-%d", idx)
	resourceSpoofer := distNbTesting.NewResourceSpoofer(hostName, hostId, hostSpec)
	host, _, err := distNbTesting.NewHostWithSpoofedGRPC(mockCtrl, dockerCluster, hostId, hostName, resourceSpoofer)
	Expect(err).To(BeNil())
	Expect(host).ToNot(BeNil())

	err = dockerCluster.NewHostAddedOrConnected(host)
	Expect(err).To(BeNil())

	return host, resourceSpoofer
}

func createKernelSpec(spec types.Spec) *proto.KernelSpec {
	kernelId := uuid.NewString()
	kernelKey := uuid.NewString()
	resourceSpec := proto.NewResourceSpec(int32(spec.CPU()), float32(spec.MemoryMB()),
		int32(spec.GPU()), float32(spec.VRAM()))
	return &proto.KernelSpec{
		Id:              kernelId,
		Session:         kernelId,
		Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
		SignatureScheme: jupyter.JupyterSignatureScheme,
		Key:             kernelKey,
		ResourceSpec:    resourceSpec,
	}
}
