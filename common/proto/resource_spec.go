package proto

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
)

func NewResourceSpec(cpu int32, memory float32, gpu int32, vram float32) *ResourceSpec {
	return &ResourceSpec{
		Cpu:    cpu,
		Memory: memory,
		Gpu:    gpu,
		Vram:   vram,
	}
}

// ResourceSpecFromSpec creates a new ResourceSpec struct from the given types.Spec and returns a
// pointer to the newly-created ResourceSpec struct.
func ResourceSpecFromSpec(spec types.Spec) *ResourceSpec {
	return &ResourceSpec{
		Cpu:    int32(spec.CPU()),
		Memory: float32(spec.MemoryMB()),
		Gpu:    int32(spec.GPU()),
		Vram:   float32(spec.VRAM()),
	}
}

// GetResourceQuantity returns the quantity of the given ResourceType encoded by the target ResourceSpec.
func (s *ResourceSpec) GetResourceQuantity(typ types.ResourceType) float64 {
	switch typ {
	case types.Millicpus:
		return float64(s.Cpu)
	case types.Memory:
		return float64(s.Memory)
	case types.GPUs:
		return float64(s.Gpu)
	case types.VRAM:
		return float64(s.Vram)
	case types.NoResource:
		return -1
	}

	panic(fmt.Sprintf("Unknown resource quantity: %s", typ.String()))
}

// GPU returns the number of GPUs required.
//
// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
// other integers. That is, it's merely for convenience to avoid having to cast it every time.
//
// It should be an integral value.
func (s *ResourceSpec) GPU() float64 {
	return float64(s.GetGpu())
}

// CPU returns the number of vCPUs, which may be fractional.
func (s *ResourceSpec) CPU() float64 {
	return float64(s.GetCpu())
}

// MemoryMB returns the amount of memory in megabytes.
func (s *ResourceSpec) MemoryMB() float64 {
	return float64(s.GetMemory())
}

// UpdateSpecGPUs can be used to update the number of GPUs.
func (s *ResourceSpec) UpdateSpecGPUs(gpus float64) {
	s.Gpu = int32(gpus)
}

// UpdateSpecCPUs can be used to update the number of Millicpus.
func (s *ResourceSpec) UpdateSpecCPUs(cpus float64) {
	s.Cpu = int32(cpus)
}

// UpdateSpecMemoryMB can be used to update the amount of memory (in MB).
func (s *ResourceSpec) UpdateSpecMemoryMB(memory float64) {
	s.Memory = float32(memory)
}

// Mem returns the amount of memory in megabytes.
// Mem is simply an alias for MemoryMB.
func (s *ResourceSpec) Mem() float64 {
	return float64(s.GetMemory())
}

// VRAM returns the amount of GPU memory required in GB.
func (s *ResourceSpec) VRAM() float64 {
	return float64(s.GetVram())
}

// ToDecimalSpec converts the ResourceSpec to a types.DecimalSpec.
//
// Important: millicpu and GPU values are rounded to 0 decimal places. memory (mb) values are rounded to 3
// decimal places. vram values are rounded to 6 decimal places. This is to be consistent with the granularities
// supported by Kubernetes for resource requests/limits (millicpus and kilobytes/kibibytes).
func (s *ResourceSpec) ToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		Millicpus: decimal.NewFromFloat(float64(s.Cpu)).Round(0),
		MemoryMb:  decimal.NewFromFloat(float64(s.Memory)).Round(3),
		GPUs:      decimal.NewFromFloat(float64(s.Gpu)).Round(0),
		VRam:      decimal.NewFromFloat(float64(s.Vram)).Round(6),
	}
}

func (s *ResourceSpec) Add(other types.Spec) types.Spec {
	return &ResourceSpec{
		Cpu:    s.Cpu + int32(other.CPU()),
		Memory: s.Memory + float32(other.MemoryMB()),
		Gpu:    s.Gpu + int32(other.GPU()),
		Vram:   s.Vram + float32(other.VRAM()),
	}
}

// IsZero returns true of the resource quantities are all zero.
func (s *ResourceSpec) IsZero() bool {
	return s.Cpu == 0 && s.Memory == 0 && s.Gpu == 0 && s.Vram == 0
}

func (s *ResourceSpec) Equals(other types.Spec) bool {
	d1 := types.ToDecimalSpec(s)
	d2 := types.ToDecimalSpec(other)

	return d1.GPUs.Equal(d2.GPUs) && d1.Millicpus.Equals(d2.Millicpus) && d1.VRam.Equal(d2.VRam) && d1.MemoryMb.Equal(d2.MemoryMb)
}

// Clone creates a copy of the target ResourceSpec and returns a pointer to it.
func (s *ResourceSpec) Clone() *ResourceSpec {
	return &ResourceSpec{
		Cpu:    s.Cpu,
		Memory: s.Memory,
		Gpu:    s.Gpu,
		Vram:   s.Vram,
	}
}

// EqualsWithField returns a flag indicating whether the two Spec instances are equal.
//
// If they are not, then a string is returned indicating the resource type for which they are unequal.
//
// Resources are checked in this order: CPU, Memory, GPU, VRAM.
//
// If the Spec instances are unequal, then the first resource type for which they are unequal (in the order in which
// resource types are checked/compared) will be the one that is returned.
func (s *ResourceSpec) EqualsWithField(other types.Spec) (bool, types.ResourceType) {
	d1 := types.ToDecimalSpec(s)
	d2 := types.ToDecimalSpec(other)

	if !d1.Millicpus.Equals(d2.Millicpus) {
		return false, types.Millicpus
	}

	if !d1.MemoryMb.Equal(d2.MemoryMb) {
		return false, types.Memory
	}

	if !d1.GPUs.Equal(d2.GPUs) {
		return false, types.GPUs
	}

	if !d1.VRam.Equal(d2.VRam) {
		return false, types.VRAM
	}

	return true, types.NoResource
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
//func (s *ResourceSpec) Validate(requirement types.Spec) bool {
//	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemoryMB() > requirement.MemoryMB()
//}

// FullSpecFromKernelReplicaSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelReplicaSpec to a *Float64Spec and returns the resulting *Float64Spec.
func (x *KernelReplicaSpec) FullSpecFromKernelReplicaSpec() *types.Float64Spec {
	return &types.Float64Spec{
		Millicpus: float64(x.Kernel.ResourceSpec.Cpu),
		Memory:    float64(x.Kernel.ResourceSpec.Memory),
		GPUs:      float64(x.Kernel.ResourceSpec.Gpu),
		VRam:      float64(x.Kernel.ResourceSpec.Vram),
	}
}

// DecimalSpecFromKernelSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelSpec to a *DecimalSpec and returns the resulting *DecimalSpec.
//
// If the proto.KernelSpec argument is nil, then FullSpecFromKernelSpec will return nil.
//
// Important: millicpu and GPU values are rounded to 0 decimal places. memory (mb) values are rounded to 3
// decimal places. vram values are rounded to 6 decimal places. This is to be consistent with the granularities
// supported by Kubernetes for resource requests/limits (millicpus and kilobytes/kibibytes).
func (x *KernelSpec) DecimalSpecFromKernelSpec() *types.DecimalSpec {
	if x == nil {
		return nil
	}

	return &types.DecimalSpec{
		Millicpus: decimal.NewFromFloat(float64(x.ResourceSpec.Cpu)).Round(0),
		MemoryMb:  decimal.NewFromFloat(float64(x.ResourceSpec.Memory)).Round(3),
		GPUs:      decimal.NewFromFloat(float64(x.ResourceSpec.Gpu)).Round(0),
		VRam:      decimal.NewFromFloat(float64(x.ResourceSpec.Vram)).Round(6),
	}
}

func (x *KernelReplicaSpec) Clone() *KernelReplicaSpec {
	kernelSpec := &KernelSpec{
		Id:              x.Kernel.Id,
		Session:         x.Kernel.Session,
		Argv:            x.Kernel.Argv,
		SignatureScheme: x.Kernel.SignatureScheme,
		Key:             x.Kernel.Key,
		ResourceSpec:    x.Kernel.ResourceSpec,
		WorkloadId:      x.Kernel.WorkloadId,
	}

	return &KernelReplicaSpec{
		Kernel:                    kernelSpec,
		ReplicaId:                 x.ReplicaId,
		Join:                      x.Join,
		NumReplicas:               x.NumReplicas,
		DockerModeKernelDebugPort: x.DockerModeKernelDebugPort,
		PersistentId:              x.PersistentId,
		WorkloadId:                x.WorkloadId,
		Replicas:                  x.Replicas,
		PrewarmContainer:          x.PrewarmContainer,
	}
}
