package types

import (
	"fmt"
	"github.com/shopspring/decimal"
)

var (
	ZeroDecimalSpec = NewDecimalSpec(0, 0, 0, 0)
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

	// Equals returns true if the target Spec is equal to the given Spec.
	Equals(Spec) bool

	// IsZero returns true if the resource counts for all resource types are zero.
	IsZero() bool

	// Add adds the corresponding resource counts of the given Spec to the target Spec
	// and returns a new Spec struct. Neither the target spec nor the parameterized spec
	// are modified by Add.
	Add(other Spec) Spec
}

// ToDecimalSpec creates a new DecimalSpec struct using the same resource values as the provided Spec and returns
// a pointer to the new DecimalSpec struct.
//
// If the provided Spec is actually a DecimalSpec (or *DecimalSpec), then the returned *DecimalSpec is created
// by calling the given Spec's CloneDecimalSpec method.
func ToDecimalSpec(spec Spec) *DecimalSpec {
	if decimalSpec, ok := spec.(*DecimalSpec); ok {
		return decimalSpec
	}

	return &DecimalSpec{
		Millicpus: decimal.NewFromFloat(spec.CPU()),
		MemoryMb:  decimal.NewFromFloat(spec.MemoryMB()),
		GPUs:      decimal.NewFromFloat(spec.GPU()),
		VRam:      decimal.NewFromFloat(spec.VRAM()),
	}
}

// DecimalSpec is a concrete implementation of the Spec interface that is backed by decimal.Decimal structs
// for each resource value (Millicpus, GPUs, and memory).
//
// DecimalSpec is immutable (unless you explicitly modify the fields yourself).
type DecimalSpec struct {
	GPUs      decimal.Decimal `json:"gpus"`   // Number of vGPUs.
	VRam      decimal.Decimal `json:"vram"`   // Amount of VRAM required in GB.
	Millicpus decimal.Decimal `json:"cpus"`   // Number of Millicpus in millicpus, where 1000 mCPU = 1 vCPU.
	MemoryMb  decimal.Decimal `json:"memory"` // Amount of memory in megabytes (MB).
}

// NewDecimalSpec creates a new DecimalSpec struct and returns a pointer to it.
func NewDecimalSpec(millicpus float64, memoryMb float64, gpus float64, vramGb float64) *DecimalSpec {
	return &DecimalSpec{
		Millicpus: decimal.NewFromFloat(millicpus),
		MemoryMb:  decimal.NewFromFloat(memoryMb),
		GPUs:      decimal.NewFromFloat(gpus),
		VRam:      decimal.NewFromFloat(vramGb),
	}
}

// ToMap converts the target DecimalSpec to a map[string]interface{} with keys matching the JSON tags of
// the DecimalSpec struct.
func (d *DecimalSpec) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"cpus":   d.Millicpus.InexactFloat64(),
		"memory": d.MemoryMb.InexactFloat64(),
		"gpus":   d.GPUs.InexactFloat64(),
		"vram":   d.VRam.InexactFloat64(),
	}
}

func (d *DecimalSpec) IsZero() bool {
	return d.GPUs.IsZero() && d.Millicpus.IsZero() && d.MemoryMb.IsZero() && d.VRam.IsZero()
}

func (d *DecimalSpec) Subtract(spec Spec) *DecimalSpec {
	d2 := ToDecimalSpec(spec)

	return &DecimalSpec{
		GPUs:      d.GPUs.Sub(d2.GPUs),
		VRam:      d.VRam.Sub(d2.VRam),
		MemoryMb:  d.MemoryMb.Sub(d2.MemoryMb),
		Millicpus: d.Millicpus.Sub(d2.Millicpus),
	}
}

func (d *DecimalSpec) Add(other Spec) Spec {
	d2 := ToDecimalSpec(other)

	return &DecimalSpec{
		GPUs:      d.GPUs.Add(d2.GPUs),
		VRam:      d.VRam.Add(d2.VRam),
		MemoryMb:  d.MemoryMb.Add(d2.MemoryMb),
		Millicpus: d.Millicpus.Add(d2.Millicpus),
	}
}

func (d *DecimalSpec) AddDecimal(other Spec) *DecimalSpec {
	d2 := ToDecimalSpec(other)

	return &DecimalSpec{
		GPUs:      d.GPUs.Add(d2.GPUs),
		VRam:      d.VRam.Add(d2.VRam),
		MemoryMb:  d.MemoryMb.Add(d2.MemoryMb),
		Millicpus: d.Millicpus.Add(d2.Millicpus),
	}
}

func (d *DecimalSpec) Equals(other Spec) bool {
	d2 := ToDecimalSpec(other)

	return d.GPUs.Equal(d2.GPUs) && d.Millicpus.Equals(d2.Millicpus) && d.VRam.Equal(d2.VRam) && d.MemoryMb.Equal(d2.MemoryMb)
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
		d.Millicpus.StringFixed(4), d.MemoryMb.StringFixed(4), d.GPUs.StringFixed(1), d.VRam.StringFixed(4))
}

// CloneDecimalSpec returns a copy/clone of the target DecimalSpec as a *DecimalSpec.
func (d *DecimalSpec) CloneDecimalSpec() *DecimalSpec {
	return &DecimalSpec{
		GPUs:      d.GPUs.Copy(),
		Millicpus: d.Millicpus.Copy(),
		MemoryMb:  d.MemoryMb.Copy(),
		VRam:      d.VRam.Copy(),
	}
}

// Clone returns a copy/clone of the target DecimalSpec as a Spec.
func (d *DecimalSpec) Clone() CloneableSpec {
	return d.CloneDecimalSpec()
}

// Float64Spec is a concrete implementation of the Spec interface that is backed by float64 variables for each
// resource value (Millicpus, GPUs, and memory).
type Float64Spec struct {
	Millicpus float64 `json:"cpus" mapstructure:"cpus"`     // Number of Millicpus in millicpus, where 1000 mCPU = 1 vCPU
	Memory    float64 `json:"memory" mapstructure:"memory"` // Amount of memory in megabytes (MB).
	GPUs      float64 `json:"gpus" mapstructure:"gpus"`     // Number of vGPUs.
	VRam      float64 `json:"vram" mapstructure:"vram"`     // Amount of VRAM in GB.
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
	return s.Memory
}

// UpdateSpecGPUs can be used to update the number of GPUs.
func (s *Float64Spec) UpdateSpecGPUs(gpus float64) {
	s.GPUs = gpus
}

func (s *Float64Spec) Add(other Spec) Spec {
	return &Float64Spec{
		Millicpus: s.Millicpus + other.CPU(),
		Memory:    s.Memory + other.MemoryMB(),
		GPUs:      s.GPUs + other.GPU(),
		VRam:      s.VRam + other.VRAM(),
	}
}

// IsZero returns true of the resource quantities are all zero.
func (s *Float64Spec) IsZero() bool {
	return s.Millicpus == 0 && s.Memory == 0 && s.GPUs == 0 && s.VRam == 0
}

// UpdateSpecCPUs can be used to update the number of Millicpus.
func (s *Float64Spec) UpdateSpecCPUs(cpus float64) {
	s.Millicpus = cpus
}

// UpdateSpecMemoryMB can be used to update the amount of memory (in MB).
func (s *Float64Spec) UpdateSpecMemoryMB(memory float64) {
	s.Memory = memory
}

func (s *Float64Spec) String() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %.0f, Memory: %.2f MB, GPUs: %.0f, VRAM: %.2f GB]", s.Millicpus, s.Memory, s.GPUs, s.VRAM())
}

func (s *Float64Spec) Equals(other Spec) bool {
	d1 := ToDecimalSpec(s)
	d2 := ToDecimalSpec(other)

	return d1.GPUs.Equal(d2.GPUs) && d1.Millicpus.Equals(d2.Millicpus) && d1.VRam.Equal(d2.VRam) && d1.MemoryMb.Equal(d2.MemoryMb)
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
		Memory:    s.Memory,
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
