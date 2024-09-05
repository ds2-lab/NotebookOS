package types

import (
	"fmt"

	"github.com/zhangjyr/distributed-notebook/common/proto"
)

type Spec interface {
	// GPU returns the number of GPUs required.
	//
	// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
	// other integers. That is, it's merely for convenience to avoid having to cast it every time.
	//
	// It should be an integral value.
	GPU() float64

	// UpdateSpecGPUs can be used to update the number of GPUs.
	UpdateSpecGPUs(float64)

	// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
	CPU() float64

	// MemoryMB returns the amount of memory in MB.
	MemoryMB() float64

	// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
	//
	// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
	// parameterized Spec (the Spec being satisfied).
	Validate(Spec) bool

	// String returns a string representation of the Spec.
	String() string

	// Clone returns a copy of the Spec.
	Clone() Spec
}

type GPUSpec float64

// GPU returns the number of GPUs required.
//
// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
// other integers. That is, it's merely for convenience to avoid having to cast it every time.
//
// It should be an integral value.
func (s GPUSpec) GPU() float64 {
	return float64(s)
}

// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
func (s GPUSpec) CPU() float64 {
	return 0.0
}

func (s GPUSpec) Clone() GPUSpec {
	var gpus = float64(s)
	return GPUSpec(gpus)
}

// MemoryMB returns the amount of memory in MB.
func (s GPUSpec) MemoryMB() float64 {
	return 0.0
}

func (s GPUSpec) String() string {
	return fmt.Sprintf("GPUSpec[GPUs: %.2f]", s)
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
func (s GPUSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU()
}

type FullSpec struct {
	GPUs     GPUSpec `json:"gpus"`
	CPUs     float64 `json:"cpus"` // Number of CPUs in millicpus, where 1000 mCPU = 1 vCPU
	MemoryMb float64 `json:"memory_mb"`
}

// GPU returns the number of GPUs required.
//
// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
// other integers. That is, it's merely for convenience to avoid having to cast it every time.
//
// It should be an integral value.
func (s *FullSpec) GPU() float64 {
	return float64(s.GPUs)
}

// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
func (s *FullSpec) CPU() float64 {
	return s.CPUs
}

// MemoryMB returns the amount of memory in MB.
func (s *FullSpec) MemoryMB() float64 {
	return s.MemoryMb
}

// UpdateSpecGPUs can be used to update the number of GPUs.
func (s *FullSpec) UpdateSpecGPUs(gpus float64) {
	s.GPUs = GPUSpec(gpus)
}

func (s *FullSpec) String() string {
	return fmt.Sprintf("FullSpec[CPUs: %.2f, Memory: %.2f MB, GPUs: %.2f]", s.CPUs, s.MemoryMb, s.GPUs)
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
func (s *FullSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemoryMB() > requirement.MemoryMB()
}

func (s *FullSpec) Clone() Spec {
	return &FullSpec{
		GPUs:     s.GPUs,
		CPUs:     s.CPUs,
		MemoryMb: s.MemoryMb,
	}
}

// FullSpecFromKernelReplicaSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelReplicaSpec to a *FullSpec and returns the resulting *FullSpec.
func FullSpecFromKernelReplicaSpec(in *proto.KernelReplicaSpec) *FullSpec {
	return &FullSpec{
		GPUs:     GPUSpec(in.Kernel.ResourceSpec.Gpu),
		CPUs:     float64(in.Kernel.ResourceSpec.Cpu),
		MemoryMb: float64(in.Kernel.ResourceSpec.Memory),
	}
}

// FullSpecFromKernelSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelSpec to a *FullSpec and returns the resulting *FullSpec.
func FullSpecFromKernelSpec(in *proto.KernelSpec) *FullSpec {
	return &FullSpec{
		GPUs:     GPUSpec(in.ResourceSpec.Gpu),
		CPUs:     float64(in.ResourceSpec.Cpu),
		MemoryMb: float64(in.ResourceSpec.Memory),
	}
}
