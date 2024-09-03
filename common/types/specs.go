package types

import "fmt"

type Spec interface {
	// GPU returns the number of GPUs required.
	//
	// Although the return type is float64, this is merely because it is often compared to other float64s and rarely
	// other integers. That is, it's merely for convenience to avoid having to cast it every time.
	//
	// It should be an integral value.
	GPU() float64

	// CPU returns the number of vCPUs, which may be fractional.
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

// CPU returns the number of vCPUs, which may be fractional.
func (s GPUSpec) CPU() float64 {
	return 0.0
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
	CPUs     float64 `json:"cpus"`
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

// CPU returns the number of vCPUs, which may be fractional.
func (s *FullSpec) CPU() float64 {
	return s.CPUs
}

// MemoryMB returns the amount of memory in MB.
func (s *FullSpec) MemoryMB() float64 {
	return s.MemoryMb
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
