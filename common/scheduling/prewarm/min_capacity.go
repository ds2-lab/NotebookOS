package prewarm

import (
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type MinCapacityPrewarmerConfig struct {
	*PrewarmerConfig

	// MinPrewarmedContainersPerHost is the minimum number of pre-warmed containers that should be available on any
	// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
	// below this quantity, then a new pre-warmed container will be provisioned.
	MinPrewarmedContainersPerHost int

	// ProactiveReplacementEnabled controls whether new pre-warm containers are immediately provisioned
	// when an existing prewarm container is used, or if the pool relies on containers being returned
	// after they are used.
	//
	// Warning: enabling this option may cause the pool's size to grow unbounded if container re-use is
	// also enabled.
	ProactiveReplacementEnabled bool `yaml:"replacementEnabled" json:"replacementEnabled"`
}

// MinCapacityPrewarmer attempts to maintain the minimum number of prewarmed containers on each scheduling.Host
// in the scheduling.Cluster.
type MinCapacityPrewarmer struct {
	*BaseContainerPrewarmer

	Config *MinCapacityPrewarmerConfig
}

// NewMinCapacityPrewarmer creates a new MinCapacityPrewarmer struct and returns a pointer to it.
func NewMinCapacityPrewarmer(cluster scheduling.Cluster, configuration *MinCapacityPrewarmerConfig,
	metricsProvider scheduling.MetricsProvider) *MinCapacityPrewarmer {

	base := NewBaseContainerPrewarmer(cluster, configuration.PrewarmerConfig, metricsProvider)

	warmer := &MinCapacityPrewarmer{
		BaseContainerPrewarmer: base,
		Config:                 configuration,
	}

	base.instance = warmer
	warmer.instance = warmer

	return warmer
}

// ValidatePoolCapacity ensures that there are enough pre-warmed containers available throughout the entire cluster.
func (p *MinCapacityPrewarmer) ValidatePoolCapacity() {
	hosts := make([]scheduling.Host, 0, p.Cluster.Len())
	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		hosts = append(hosts, host)
		return true
	})

	for _, host := range hosts {
		// Skip disabled hosts.
		if !host.Enabled() {
			continue
		}

		p.ValidateHostCapacity(host)
	}
}

// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
// ContainerPrewarmer's policy.
func (p *MinCapacityPrewarmer) ValidateHostCapacity(host scheduling.Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !host.Enabled() {
		p.log.Debug("Host %s is disabled. Won't bother provisioning pre-warm containers.", host.GetNodeName())
		return
	}

	if host.IsExcludedFromScheduling() {
		p.log.Debug("Host %s is excluded from scheduling. (Idle reclamation in progress?) "+
			"Won't bother provisioning pre-warm containers.", host.GetNodeName())
		return
	}

	count, provisioning := p.unsafeHostLen(host)
	combined := count + provisioning

	// Check if we're satisfying the minimum capacity constraint. If we are, then we can return.
	if combined >= p.Config.MinPrewarmedContainersPerHost {
		return
	}

	// Calculate how many containers we need to provision on this host.
	numToProvision := p.Config.MinPrewarmedContainersPerHost - combined
	p.log.Debug("Host %s (ID=%s) is under capacity (current=%d, provisioning=%d, min=%d). Provisioning %d pre-warm container(s) on host.",
		host.GetNodeName(), host.GetID(), count, provisioning, p.Config.MinPrewarmedContainersPerHost, numToProvision)

	// Provision the containers in a separate goroutine.
	go p.ProvisionContainers(host, numToProvision)
}

// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
// below this quantity, then a new pre-warmed container will be provisioned.
func (p *MinCapacityPrewarmer) MinPrewarmedContainersPerHost() int {
	return p.Config.MinPrewarmedContainersPerHost
}

// prewarmContainerUsed is called when a pre-warm container is used, to give the container prewarmer a chance
// to react (i.e., provision another prewarm container, if it is supposed to do so).
func (p *MinCapacityPrewarmer) prewarmContainerUsed(host scheduling.Host, prewarmedContainer scheduling.PrewarmedContainer) {
	if !p.Config.ProactiveReplacementEnabled {
		return
	}

	err := p.ProvisionContainer(host)
	if err != nil {
		p.log.Error("Failed to provision new pre-warmed container on host %s (ID=%s) after prewarm container \"%s\" was used: %v",
			host.GetNodeName(), host.GetID(), prewarmedContainer.ID(), err)
	}
}
