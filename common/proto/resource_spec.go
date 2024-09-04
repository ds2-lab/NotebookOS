package proto

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

// Mem returns the amount of memory in megabytes.
// Mem is simply an alias for MemoryMB.
func (s *ResourceSpec) Mem() float64 {
	return float64(s.GetMemory())
}

// Validate checks that "this" Spec could "satisfy" the parameterized Spec.
//
// To "satisfy" a Spec means that all the resource values of "this" Spec are larger than that of the
// parameterized Spec (the Spec being satisfied).
//func (s *ResourceSpec) Validate(requirement types.Spec) bool {
//	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemoryMB() > requirement.MemoryMB()
//}
