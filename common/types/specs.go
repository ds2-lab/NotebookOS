package types

type Spec interface {
	GPU() float64
	CPU() float64
	MemGB() float64
	Validate(Spec) bool
}

type GPUSpec float64

func (s GPUSpec) GPU() float64 {
	return float64(s)
}

func (s GPUSpec) CPU() float64 {
	return 0.0
}

func (s GPUSpec) MemGB() float64 {
	return 0.0
}

func (s GPUSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU()
}

type FullSpec struct {
	GPUs     GPUSpec `json:"gpus"`
	CPUs     float64 `json:"cpus"`
	MemoryGB float64 `json:"memory_gb"`
}

func (s *FullSpec) GPU() float64 {
	return float64(s.GPUs)
}

func (s *FullSpec) CPU() float64 {
	return s.CPUs
}

func (s *FullSpec) MemGB() float64 {
	return s.MemoryGB
}

func (s *FullSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU() && s.MemGB() > requirement.MemGB()
}
