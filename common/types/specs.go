package types

import (
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/proto"
)

// CloneableSpec is a superset of the Spec interface with an additional Clone method.
// The Clone method returns a new CloneableSpec instance with the same resource (cpu, gpu, memory) values.
type CloneableSpec interface {
	Spec

	// Clone returns an exact copy of the CloneableSpec.
	Clone() CloneableSpec
}

// ValidatableResourceSpec is a superset of the Spec interface with an additional Validate method.
// The Validate method is used to verify that the target Spec (or rather, the target ValidatableResourceSpec)
// can "satisfy" the given/parameterized Spec. To "satisfy" a Spec means that all the resources of the target
// Spec/ValidatableResourceSpec are greater than or equal to the given/parameterized Spec.
type ValidatableResourceSpec interface {
	Spec

	// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
	//
	// To "satisfy" a Spec means that all the resource values of "this" Spec are greater than or equal to those of
	// the parameterized Spec (the Spec being satisfied).
	Validate(Spec) bool
}

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

	// UpdateSpecCPUs can be used to update the number of CPUs.
	UpdateSpecCPUs(float64)

	// UpdateSpecMemoryMB can be used to update the amount of memory (in MB).
	UpdateSpecMemoryMB(float64)

	// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
	CPU() float64

	// MemoryMB returns the amount of memory in MB.
	MemoryMB() float64

	// String returns a string representation of the Spec.
	String() string
}

type GPUSpec float64

// GPU is an implementation of Spec that just maintains the GPU requirement.
// CPUs and MemoryMB are both 0.
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

// ToDecimalSpec creates a new DecimalSpec struct using the same resource values as the provided Spec and returns
// a pointer to the new DecimalSpec struct.
//
// If the provided Spec is actually a DecimalSpec (or *DecimalSpec), then the returned *DecimalSpec is created
// by calling the given Spec's CloneDecimalSpec method.
func ToDecimalSpec(spec Spec) *DecimalSpec {
	if decimalSpecPtr, ok := spec.(*DecimalSpec); ok {
		return decimalSpecPtr.CloneDecimalSpec()
	} else if decimalSpec, ok := spec.(DecimalSpec); ok {
		return decimalSpec.CloneDecimalSpec()
	}

	return &DecimalSpec{
		Millicpus: decimal.NewFromFloat(spec.CPU()),
		MemoryMb:  decimal.NewFromFloat(spec.MemoryMB()),
		GPUs:      decimal.NewFromFloat(spec.GPU()),
	}
}

// DecimalSpec is a concrete implementation of the Spec interface that is backed by decimal.Decimal structs
// for each resource value (CPUs, GPUs, and memory).
type DecimalSpec struct {
	GPUs      decimal.Decimal `json:"gpus"`      // Number of vGPUs.
	Millicpus decimal.Decimal `json:"cpus"`      // Number of CPUs in millicpus, where 1000 mCPU = 1 vCPU.
	MemoryMb  decimal.Decimal `json:"memory_mb"` // Amount of memory in megabytes (MB).
}

func (d DecimalSpec) GPU() float64 {
	return d.GPUs.InexactFloat64()
}

func (d DecimalSpec) UpdateSpecGPUs(gpus float64) {
	d.GPUs = decimal.NewFromFloat(gpus)
}

func (d DecimalSpec) UpdateSpecCPUs(millicpus float64) {
	d.Millicpus = decimal.NewFromFloat(millicpus)
}

func (d DecimalSpec) UpdateSpecMemoryMB(memoryMb float64) {
	d.MemoryMb = decimal.NewFromFloat(memoryMb)
}

func (d DecimalSpec) CPU() float64 {
	return d.Millicpus.InexactFloat64()
}

func (d DecimalSpec) MemoryMB() float64 {
	return d.MemoryMb.InexactFloat64()
}

func (d DecimalSpec) Validate(requirement Spec) bool {
	return d.GPUs.GreaterThanOrEqual(decimal.NewFromFloat(requirement.GPU())) &&
		d.Millicpus.GreaterThanOrEqual(decimal.NewFromFloat(requirement.CPU())) &&
		d.MemoryMb.GreaterThanOrEqual(decimal.NewFromFloat(requirement.MemoryMB()))
}

func (d DecimalSpec) String() string {
	return fmt.Sprintf("ResourceSpec[CPUs: %s, Memory: %s MB, GPUs: %s]",
		d.Millicpus.StringFixed(0), d.MemoryMb.StringFixed(4), d.GPUs.StringFixed(0))
}

// CloneDecimalSpec returns a copy/clone of the target DecimalSpec as a *DecimalSpec.
func (d DecimalSpec) CloneDecimalSpec() *DecimalSpec {
	return &DecimalSpec{
		GPUs:      d.GPUs.Copy(),
		Millicpus: d.Millicpus.Copy(),
		MemoryMb:  d.MemoryMb.Copy(),
	}
}

// Clone returns a copy/clone of the target DecimalSpec as a Spec.
func (d DecimalSpec) Clone() CloneableSpec {
	return d.CloneDecimalSpec()
}

// Float64Spec is a concrete implementation of the Spec interface that is backed by float64 variables for each
// resource value (CPUs, GPUs, and memory).
type Float64Spec struct {
	GPUs     GPUSpec `json:"gpus"`      // Number of vGPUs.
	CPUs     float64 `json:"cpus"`      // Number of CPUs in millicpus, where 1000 mCPU = 1 vCPU
	MemoryMb float64 `json:"memory_mb"` // Amount of memory in megabytes (MB).
}

// GPU returns the number of GPUs required.
//
// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
// other integers. That is, it's merely for convenience to avoid having to cast it every time.
//
// It should be an integral value.
func (s *Float64Spec) GPU() float64 {
	return float64(s.GPUs)
}

// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
func (s *Float64Spec) CPU() float64 {
	return s.CPUs
}

// MemoryMB returns the amount of memory in MB.
func (s *Float64Spec) MemoryMB() float64 {
	return s.MemoryMb
}

// UpdateSpecGPUs can be used to update the number of GPUs.
func (s *Float64Spec) UpdateSpecGPUs(gpus float64) {
	s.GPUs = GPUSpec(gpus)
}

// UpdateSpecCPUs can be used to update the number of CPUs.
func (s *Float64Spec) UpdateSpecCPUs(cpus float64) {
	s.CPUs = cpus
}

// UpdateSpecMemoryMB can be used to update the amount of memory (in MB).
func (s *Float64Spec) UpdateSpecMemoryMB(memory float64) {
	s.MemoryMb = memory
}

func (s *Float64Spec) String() string {
	return fmt.Sprintf("ResourceSpec[CPUs: %.2f, Memory: %.2f MB, GPUs: %.2f]", s.CPUs, s.MemoryMb, s.GPUs)
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
func (s *Float64Spec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemoryMB() > requirement.MemoryMB()
}

func (s *Float64Spec) Clone() CloneableSpec {
	return &Float64Spec{
		GPUs:     s.GPUs,
		CPUs:     s.CPUs,
		MemoryMb: s.MemoryMb,
	}
}

// FullSpecFromKernelReplicaSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelReplicaSpec to a *Float64Spec and returns the resulting *Float64Spec.
func FullSpecFromKernelReplicaSpec(in *proto.KernelReplicaSpec) *Float64Spec {
	return &Float64Spec{
		CPUs:     float64(in.Kernel.ResourceSpec.Cpu),
		MemoryMb: float64(in.Kernel.ResourceSpec.Memory),
		GPUs:     GPUSpec(in.Kernel.ResourceSpec.Gpu),
	}
}

// FullSpecFromKernelSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelSpec to a *Float64Spec and returns the resulting *Float64Spec.
//
// If the proto.KernelSpec argument is nil, then FullSpecFromKernelSpec will return nil.
func FullSpecFromKernelSpec(in *proto.KernelSpec) *Float64Spec {
	if in == nil {
		return nil
	}

	return &Float64Spec{
		CPUs:     float64(in.ResourceSpec.Cpu),
		MemoryMb: float64(in.ResourceSpec.Memory),
		GPUs:     GPUSpec(in.ResourceSpec.Gpu),
	}
}

// DecimalSpecFromKernelSpec converts the *proto.ResourceSpec contained within the given
// *proto.KernelSpec to a *DecimalSpec and returns the resulting *DecimalSpec.
//
// If the proto.KernelSpec argument is nil, then FullSpecFromKernelSpec will return nil.
func DecimalSpecFromKernelSpec(in *proto.KernelSpec) *DecimalSpec {
	if in == nil {
		return nil
	}

	return &DecimalSpec{
		Millicpus: decimal.NewFromFloat(float64(in.ResourceSpec.Cpu)),
		MemoryMb:  decimal.NewFromFloat(float64(in.ResourceSpec.Memory)),
		GPUs:      decimal.NewFromFloat(float64(in.ResourceSpec.Gpu)),
	}
}
