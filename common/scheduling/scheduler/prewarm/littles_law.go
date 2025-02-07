package prewarm

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"time"
)

type LittlesLawPrewarmerConfig struct {
	*PrewarmerConfig `json:",inline"`

	// W is the average time that a scheduling.KernelContainer remains in the system.
	//
	// The value of W will vary depending upon the scheduling.Policy that is used.
	//
	// For policies in which the scheduling.ContainerLifetime is scheduling.LongRunning, W will be approximately equal
	// to the average lifetime of a scheduling.UserSession.
	//
	// For policies in which the scheduling.ContainerLifetime is scheduling.SingleTrainingEvent, W will be roughly equal
	// to the average duration of a single training event.
	W time.Duration

	// Lambda is the long-term, average effective arrival rate of scheduling.KernelContainer instances in the system.
	//
	// Just like the W parameter, the value of Lambda will vary depending upon the scheduling.Policy that is used.
	//
	// For policies in which the scheduling.ContainerLifetime is scheduling.LongRunning, Lambda will be roughly equal
	// to the average amount of time between 'session creation' events.
	//
	// For policies in which the scheduling.ContainerLifetime is scheduling.SingleTrainingEvent, W will be roughly
	// equal to the average inter-arrival time of training events (with respect to the entire scheduling.Cluster,
	// rather than with respect to a single scheduling.UserSession or scheduling.Kernel).
	Lambda time.Duration
}

// LittlesLawPrewarmer creates prewarmed containers in accordance with [Little's Law].
//
// [Little's Law]: https://en.wikipedia.org/wiki/Little%27s_law
type LittlesLawPrewarmer struct {
	*BaseContainerPrewarmer

	Config *LittlesLawPrewarmerConfig
}

// NewLittlesLawPrewarmer creates a new LittlesLawPrewarmer struct and returns a pointer to it.
func NewLittlesLawPrewarmer(cluster scheduling.Cluster, configuration *LittlesLawPrewarmerConfig) *LittlesLawPrewarmer {
	base := NewContainerPrewarmer(cluster, configuration.PrewarmerConfig)

	warmer := &LittlesLawPrewarmer{
		BaseContainerPrewarmer: base,
		Config:                 configuration,
	}

	base.instance = warmer
	warmer.instance = warmer

	return warmer
}

// Run creates a separate goroutine in which the LittlesLawPrewarmer maintains the overall capacity/availability of
// pre-warmed containers in accordance with LittlesLawPrewarmer's policy for doing so.
func (p *LittlesLawPrewarmer) Run() {
	// TODO: Implement me.
}

// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
// ContainerPrewarmer's policy.
func (p *LittlesLawPrewarmer) ValidateHostCapacity(host scheduling.Host) {
	// TODO: Implement me.
}
