package invoker_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/distributed-notebook/local_daemon/invoker"
)

var _ = Describe("Docker Invoker Tests", func() {
	Context("GPU command snippets", func() {
		It("Should generate an empty GPU command snippet when simulating training using sleep", func() {
			dockerInvoker := &invoker.DockerInvoker{
				SimulateTrainingUsingSleep: true,
				BindGPUs:                   false,
			}

			snippet := dockerInvoker.InitGpuCommand()
			Expect(snippet).To(Equal(""))
		})

		It("Should correctly generate GPU command snippets when binding all GPUs", func() {
			dockerInvoker := &invoker.DockerInvoker{
				SimulateTrainingUsingSleep: false,
				BindAllGpus:                true,
				BindGPUs:                   true,
			}

			snippet := dockerInvoker.InitGpuCommand()
			Expect(snippet).To(Equal(" --gpus all"))
		})

		It("Should correctly generate GPU command snippets when binding a single GPU", func() {
			for deviceId := int32(0); deviceId < int32(8); deviceId++ {
				dockerInvoker := &invoker.DockerInvoker{
					SimulateTrainingUsingSleep: false,
					BindAllGpus:                false,
					BindGPUs:                   true,
					AssignedGpuDeviceIds:       []int32{deviceId},
				}

				target := fmt.Sprintf(" --gpus 'device=%d'", deviceId)
				snippet := dockerInvoker.InitGpuCommand()

				Expect(snippet).To(Equal(target))
			}
		})

		It("Should correctly generate GPU command snippets when binding multiple GPUs", func() {
			deviceIds := []int32{1, 3, 5}

			dockerInvoker := &invoker.DockerInvoker{
				SimulateTrainingUsingSleep: false,
				BindAllGpus:                false,
				BindGPUs:                   true,
				AssignedGpuDeviceIds:       deviceIds,
			}

			target := fmt.Sprintf(" --gpus 'device=%d,%d,%d'", deviceIds[0], deviceIds[1], deviceIds[2])
			snippet := dockerInvoker.InitGpuCommand()

			Expect(snippet).To(Equal(target))
		})

		It("Should correctly generate GPU command snippets when binding all GPUs via ids", func() {
			deviceIds := []int32{0, 1, 2, 3, 4, 5, 6, 7}

			dockerInvoker := &invoker.DockerInvoker{
				SimulateTrainingUsingSleep: false,
				BindAllGpus:                false,
				BindGPUs:                   true,
				AssignedGpuDeviceIds:       deviceIds,
			}

			target := " --gpus 'device=0,1,2,3,4,5,6,7'"
			snippet := dockerInvoker.InitGpuCommand()

			Expect(snippet).To(Equal(target))
		})
	})
})
