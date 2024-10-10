package types

import (
	"fmt"
	"github.com/shopspring/decimal"
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

	// VRAM is the amount of GPU memory required in GB.
	VRAM() float64

	// UpdateSpecGPUs can be used to update the number of GPUs.
	UpdateSpecGPUs(float64)

	// UpdateSpecCPUs can be used to update the number of Millicpus.
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

// ToDecimalSpec creates a new DecimalSpec struct using the same resource values as the provided Spec and returns
// a pointer to the new DecimalSpec struct.
//
// If the provided Spec is actually a DecimalSpec (or *DecimalSpec), then the returned *DecimalSpec is created
// by calling the given Spec's CloneDecimalSpec method.
func ToDecimalSpec(spec Spec) *DecimalSpec {
	if decimalSpecPtr, ok := spec.(*DecimalSpec); ok {
		return decimalSpecPtr.CloneDecimalSpec()
	} else if decimalSpec, ok := spec.(*DecimalSpec); ok {
		return decimalSpec.CloneDecimalSpec()
	}

	return &DecimalSpec{
		Millicpus: decimal.NewFromFloat(spec.CPU()),
		MemoryMb:  decimal.NewFromFloat(spec.MemoryMB()),
		GPUs:      decimal.NewFromFloat(spec.GPU()),
	}
}

// DecimalSpec is a concrete implementation of the Spec interface that is backed by decimal.Decimal structs
// for each resource value (Millicpus, GPUs, and memory).
type DecimalSpec struct {
	GPUs      decimal.Decimal `json:"gpus"`      // Number of vGPUs.
	VRam      decimal.Decimal `json:"vram"`      // Amount of VRAM required in GB.
	Millicpus decimal.Decimal `json:"cpus"`      // Number of Millicpus in millicpus, where 1000 mCPU = 1 vCPU.
	MemoryMb  decimal.Decimal `json:"memory_mb"` // Amount of memory in megabytes (MB).
}

// VRAM is the amount of GPU memory required in GB.
func (d *DecimalSpec) VRAM() float64 {
	return d.VRam.InexactFloat64()
}

func (d *DecimalSpec) GPU() float64 {
	return d.GPUs.InexactFloat64()
}

func (d *DecimalSpec) UpdateSpecGPUs(gpus float64) {
	d.GPUs = decimal.NewFromFloat(gpus)
}

func (d *DecimalSpec) UpdateSpecCPUs(millicpus float64) {
	d.Millicpus = decimal.NewFromFloat(millicpus)
}

func (d *DecimalSpec) UpdateSpecMemoryMB(memoryMb float64) {
	d.MemoryMb = decimal.NewFromFloat(memoryMb)
}

func (d *DecimalSpec) CPU() float64 {
	return d.Millicpus.InexactFloat64()
}

func (d *DecimalSpec) MemoryMB() float64 {
	return d.MemoryMb.InexactFloat64()
}

func (d *DecimalSpec) Validate(requirement Spec) bool {
	if requirement == nil {
		panic("Received null requirement spec in DecimalSpec::Validate.")
	}

	// We can bypass having to create a bunch of new decimal.Decimal structs
	// if the other spec is also a DecimalSpec.
	if requirementDecimalSpec, ok := requirement.(*DecimalSpec); ok {
		return d.GPUs.GreaterThanOrEqual(requirementDecimalSpec.GPUs) &&
			d.Millicpus.GreaterThanOrEqual(requirementDecimalSpec.Millicpus) &&
			d.MemoryMb.GreaterThanOrEqual(requirementDecimalSpec.MemoryMb) &&
			d.VRam.GreaterThanOrEqual(requirementDecimalSpec.VRam)
	}

	return d.GPUs.GreaterThanOrEqual(decimal.NewFromFloat(requirement.GPU())) &&
		d.Millicpus.GreaterThanOrEqual(decimal.NewFromFloat(requirement.CPU())) &&
		d.MemoryMb.GreaterThanOrEqual(decimal.NewFromFloat(requirement.MemoryMB())) &&
		d.VRam.GreaterThanOrEqual(decimal.NewFromFloat(requirement.VRAM()))
}

func (d *DecimalSpec) String() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %s, Memory: %s MB, GPUs: %s, VRAM: %s GB]",
		d.Millicpus.StringFixed(0), d.MemoryMb.StringFixed(4), d.GPUs.StringFixed(0), d.VRam.StringFixed(4))
}

// CloneDecimalSpec returns a copy/clone of the target DecimalSpec as a *DecimalSpec.
func (d *DecimalSpec) CloneDecimalSpec() *DecimalSpec {
	return &DecimalSpec{
		GPUs:      d.GPUs.Copy(),
		Millicpus: d.Millicpus.Copy(),
		MemoryMb:  d.MemoryMb.Copy(),
	}
}

// Clone returns a copy/clone of the target DecimalSpec as a Spec.
func (d *DecimalSpec) Clone() CloneableSpec {
	return d.CloneDecimalSpec()
}

// Float64Spec is a concrete implementation of the Spec interface that is backed by float64 variables for each
// resource value (Millicpus, GPUs, and memory).
type Float64Spec struct {
	GPUs      float64 `json:"gpus"`      // Number of vGPUs.
	VRam      float64 `json:"vram"`      // Amount of VRAM in GB.
	Millicpus float64 `json:"cpus"`      // Number of Millicpus in millicpus, where 1000 mCPU = 1 vCPU
	MemoryMb  float64 `json:"memory_mb"` // Amount of memory in megabytes (MB).
}

// GPU returns the number of GPUs required.
//
// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
// other integers. That is, it's merely for convenience to avoid having to cast it every time.
//
// It should be an integral value.
func (s *Float64Spec) GPU() float64 {
	return s.GPUs
}

// VRAM is the amount of GPU memory required in GB.
func (s *Float64Spec) VRAM() float64 {
	return s.VRam
}

// CPU returns the number of vCPUs in milliCPUs, where 1000 mCPU = 1 vCPU, which may be fractional.
func (s *Float64Spec) CPU() float64 {
	return s.Millicpus
}

// MemoryMB returns the amount of memory in MB.
func (s *Float64Spec) MemoryMB() float64 {
	return s.MemoryMb
}

// UpdateSpecGPUs can be used to update the number of GPUs.
func (s *Float64Spec) UpdateSpecGPUs(gpus float64) {
	s.GPUs = gpus
}

// UpdateSpecCPUs can be used to update the number of Millicpus.
func (s *Float64Spec) UpdateSpecCPUs(cpus float64) {
	s.Millicpus = cpus
}

// UpdateSpecMemoryMB can be used to update the amount of memory (in MB).
func (s *Float64Spec) UpdateSpecMemoryMB(memory float64) {
	s.MemoryMb = memory
}

func (s *Float64Spec) String() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %.0f, Memory: %.2f MB, GPUs: %.0f, VRAM: %.2f GB]", s.Millicpus, s.MemoryMb, s.GPUs, s.VRAM())
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
func (s *Float64Spec) Validate(requirement Spec) bool {
	if requirement == nil {
		panic("Received null requirement spec in Float64Spec::Validate.")
	}

	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemoryMB() > requirement.MemoryMB() && s.VRam >= requirement.VRAM()
}

func (s *Float64Spec) Clone() CloneableSpec {
	return &Float64Spec{
		GPUs:      s.GPUs,
		Millicpus: s.Millicpus,
		MemoryMb:  s.MemoryMb,
	}
}

// ArbitraryKernelSpec is an extraction of the proto.KernelSpec API to an interface.
type ArbitraryKernelSpec interface {
	GetId() string
	GetSession() string
	GetArgv() []string
	GetSignatureScheme() string
	GetKey() string
	GetResourceSpec() Spec
}

// ArbitraryResourceSpec is an extraction of the proto.ResourceSpec API to an interface.
//type ArbitraryResourceSpec interface {
//	GPU() float64
//	CPU() float64
//	MemoryMB() float64
//	UpdateSpecGPUs(gpus float64)
//	UpdateSpecCPUs(cpus float64)
//	UpdateSpecMemoryMB(memory float64)
//	Mem() float64
//	GetCpu() int32
//	GetMemory() float32
//	GetGpu() int32
//}
