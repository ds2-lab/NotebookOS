package kernel

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
)

type Provider struct {
	// Kernels is a mapping from kernel ID to scheduling.Kernel.
	// There may be duplicate values (i.e., multiple sessions mapping to the same kernel).
	Kernels hashmap.HashMap[string, scheduling.Kernel]

	// KernelIdToKernel is a map from kernel ID to client.DistributedKernelClient.
	KernelIdToKernel hashmap.HashMap[string, scheduling.Kernel]
}

func (p Provider) NumKernels() int {
	return p.KernelIdToKernel.Len()
}

func (p Provider) StoreKernelBySessionId(sessionId string, kernel scheduling.Kernel) {
	p.Kernels.Store(sessionId, kernel)
}

func (p Provider) StoreKernelByKernelId(kernelId string, kernel scheduling.Kernel) {
	p.Kernels.Store(kernelId, kernel)
	p.KernelIdToKernel.Store(kernelId, kernel)
}

func (p Provider) GetKernel(kernelId string) (scheduling.Kernel, bool) {
	return p.Kernels.Load(kernelId)
}

func (p Provider) GetKernels() hashmap.HashMap[string, scheduling.Kernel] {
	return p.Kernels
}

func (p Provider) GetKernelsByKernelId() hashmap.HashMap[string, scheduling.Kernel] {
	return p.KernelIdToKernel
}

func NewProvider() *Provider {
	return &Provider{
		Kernels:          hashmap.NewConcurrentMap[scheduling.Kernel](32),
		KernelIdToKernel: hashmap.NewConcurrentMap[scheduling.Kernel](32),
	}
}
