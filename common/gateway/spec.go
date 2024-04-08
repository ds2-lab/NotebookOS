package gateway

import "github.com/zhangjyr/distributed-notebook/common/types"

func (s *ResourceSpec) GPU() float64 {
	return float64(s.GetGpu())
}

func (s *ResourceSpec) CPU() float64 {
	return float64(s.GetCpu())
}

func (s *ResourceSpec) Mem() float64 {
	return float64(s.GetMemory())
}

func (s *ResourceSpec) Validate(requirement types.Spec) bool {
	return s.GPU() >= requirement.GPU() && s.CPU() >= requirement.CPU()
}
