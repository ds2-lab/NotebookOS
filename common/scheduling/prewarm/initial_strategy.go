package prewarm

import "github.com/scusemua/distributed-notebook/common/scheduling"

const (
	// NPerHost is an InitialPrewarmStrategyName in which pre-warmed scheduling.KernelContainer instances
	// are created per active scheduling.Host in the scheduling.Cluster.
	NPerHost InitialPrewarmStrategyName = "Prewarm N Containers per Host"
)

type InitialPrewarmStrategyName string

type NPerHostStrategy struct {
	// Name is the InitialPrewarmStrategyName of the InitialPrewarmStrategy.
	Name InitialPrewarmStrategyName `json:"name" yaml:"name" name:"name"`

	// N is the number of pre-warmed scheduling.KernelContainer instances to create on each scheduling.Host.
	N int `json:"n" yaml:"n" name:"n"`
}

// Run executes the logic of the target NPerHostStrategy.
func (s *NPerHostStrategy) Run(hosts []scheduling.Host) error {
	return nil
}
