package prewarm

import "github.com/scusemua/distributed-notebook/common/scheduling"

// MinCapacityPrewarmer attempts to maintain the minimum number of prewarmed containers on each scheduling.Host
// in the scheduling.Cluster.
type MinCapacityPrewarmer struct {
	*BaseContainerPrewarmer
}

// NewMinCapacityPrewarmer creates a new MinCapacityPrewarmer struct and returns a pointer to it.
func NewMinCapacityPrewarmer(cluster scheduling.Cluster, configuration *LittlesLawPrewarmerConfig) *MinCapacityPrewarmer {
	base := NewContainerPrewarmer(cluster, configuration.PrewarmerConfig)

	warmer := &MinCapacityPrewarmer{
		BaseContainerPrewarmer: base,
	}

	base.instance = warmer
	warmer.instance = warmer

	return warmer
}

// Run creates a separate goroutine in which the MinCapacityPrewarmer maintains the overall capacity/availability of
// pre-warmed containers in accordance with MinCapacityPrewarmer's policy for doing so.
func (p *MinCapacityPrewarmer) Run() {
	// TODO: Implement me.
}
