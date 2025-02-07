package prewarm

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"time"
)

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
	for {
		select {
		case <-p.stopChan:
			{
				p.log.Debug("Stopping.")
				return
			}
		default:
		}

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

		time.Sleep(time.Second * 5)
	}
}

// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
// ContainerPrewarmer's policy.
func (p *MinCapacityPrewarmer) ValidateHostCapacity(host scheduling.Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	containers, ok := p.PrewarmContainersPerHost[host.GetID()]
	if !ok {
		panic(fmt.Sprintf("No queue found for host %s (ID=%s)",
			host.GetNodeName(), host.GetID()))
	}

	if containers.Len() > p.Config.MinPrewarmedContainersPerHost {
		return
	}

	numToProvision := p.Config.MinPrewarmedContainersPerHost - containers.Len()
	p.log.Debug("Host %s (ID=%s) is under capacity (current=%d, min=%d). Provisioning %d pre-warm container(s) on host.",
		host.GetNodeName(), host.GetID(), containers.Len(), p.Config.MinPrewarmedContainersPerHost, numToProvision)

	go p.ProvisionContainers(host, numToProvision)
}
