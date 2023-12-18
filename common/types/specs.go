package types

type Spec interface {
	GPU() float64
	CPU() float64
	Validate(Spec) bool
}

type GPUSpec float64

func (s GPUSpec) GPU() float64 {
	return float64(s)
}

func (s GPUSpec) CPU() float64 {
	return 0.0
}

func (s GPUSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU()
}

type FullSpec struct {
	GPUSpec
	CPUSpec float64
}

func (s *FullSpec) CPU() float64 {
	return s.CPUSpec
}

func (s *FullSpec) Validate(requirement Spec) bool {
	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU()
}
