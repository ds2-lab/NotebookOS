package prewarm

import (
	"container/heap"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"go.uber.org/atomic"
)

type FixedCapacityPrewarmerConfig struct {
	*PrewarmerConfig

	// Capacity is the size of the FixedCapacityPrewarmer's container pool.
	Capacity int32 `yaml:"capacity" json:"capacity"`

	// ProactiveReplacementEnabled controls whether new pre-warm containers are immediately provisioned
	// when an existing prewarm container is used, or if the pool relies on containers being returned
	// after they are used.
	//
	// Warning: enabling this option may cause the pool's size to grow unbounded if container re-use is
	// also enabled.
	ProactiveReplacementEnabled bool `yaml:"replacementEnabled" json:"replacementEnabled"`
}

// FixedCapacityPrewarmer attempts to maintain the minimum number of prewarmed containers on each scheduling.Host
// in the scheduling.Cluster.
type FixedCapacityPrewarmer struct {
	*BaseContainerPrewarmer

	Config *FixedCapacityPrewarmerConfig

	// capacityMetOnce is set to true when the target capacity is reached for the first time.
	// Once this occurs, a FixedCapacityPrewarmer will not provision any additional prewarm containers unless
	// the ProactiveReplacementEnabled field of its FixedCapacityPrewarmerConfig is set to true, in which case it
	// will provision additional pre-warm containers in response to containers being used.
	capacityMetOnce atomic.Bool
}

// NewFixedCapacityPrewarmer creates a new FixedCapacityPrewarmer struct and returns a pointer to it.
func NewFixedCapacityPrewarmer(cluster scheduling.Cluster, configuration *FixedCapacityPrewarmerConfig,
	metricsProvider scheduling.MetricsProvider) *FixedCapacityPrewarmer {

	base := NewBaseContainerPrewarmer(cluster, configuration.PrewarmerConfig, metricsProvider)

	warmer := &FixedCapacityPrewarmer{
		BaseContainerPrewarmer: base,
		Config:                 configuration,
	}

	base.instance = warmer
	warmer.instance = warmer

	return warmer
}

// ValidatePoolCapacity ensures that there are enough pre-warmed containers available throughout the entire cluster.
func (p *FixedCapacityPrewarmer) ValidatePoolCapacity() {
	p.mu.Lock()
	defer p.mu.Unlock()

	prewarmHostHeap := types.NewHeap(prewarmHostMetadataKey)

	numProvisioning := int32(0)
	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		if !host.Enabled() {
			p.log.Debug("Host %s is disabled. Won't bother provisioning pre-warm containers.", host.GetNodeName())
			return true
		}

		if host.IsExcludedFromScheduling() {
			p.log.Debug("Host %s is excluded from scheduling. (Idle reclamation in progress?) "+
				"Won't bother provisioning pre-warm containers.", host.GetNodeName())
			return true
		}

		val, loaded := p.NumPrewarmContainersProvisioningPerHost[hostId]

		var nProvisioning int32
		if !loaded {
			nProvisioning = 0
		} else {
			nProvisioning = val.Load()
		}

		var nCurrent int32
		containers, ok := p.PrewarmContainersPerHost[hostId]
		if !ok {
			nCurrent = 0
		} else {
			nCurrent = int32(containers.Len())
		}

		numProvisioning += nProvisioning

		hostWithPrewarm := newPrewarmHost(host, nCurrent, nProvisioning)

		heap.Push(prewarmHostHeap, hostWithPrewarm)

		return true
	})

	poolSize := int32(p.PoolSize())
	totalSize := poolSize + numProvisioning

	if totalSize >= p.Config.Capacity {
		p.log.Debug("PoolSize: %d, Prov: %d, Target: %d. %s",
			poolSize, numProvisioning, p.Config.Capacity, utils.GreenStyle.Render("[✓]"))

		p.capacityMetOnce.Store(true)

		return
	}

	capacityMetOnce := p.capacityMetOnce.Load()

	// If we've met the target capacity once, then we won't provision any additional containers as part of routine
	// "maintenance". We only provision new containers if our ProactiveReplacementEnabled flag is set to true.
	if capacityMetOnce {
		p.log.Debug("PoolSize: %d, Prov: %d, Target: %d, TargetCapacityMetOnce: %v. %s",
			poolSize, numProvisioning, p.Config.Capacity, capacityMetOnce, utils.OrangeStyle.Render("[✗]"))

		return
	}

	p.log.Debug("PoolSize: %d, Prov: %d, Target: %d, TargetCapacityMetOnce: %v. %s",
		poolSize, numProvisioning, p.Config.Capacity, capacityMetOnce, utils.RedStyle.Render("[✗]"))

	numToCreate := p.Config.Capacity - totalSize
	p.log.Debug("Need to create %d new prewarmed containers (curr=%d, target=%d).",
		numToCreate, totalSize, p.Config.Capacity)

	newContainersProvisioned := int32(0)

	// Create new prewarm containers evenly across the hosts, such that there is a similar number of prewarm
	// containers on each host in the cluster (including currently-provisioning pre-warm containers).
	for i := 0; i < int(numToCreate); i++ {
		v := heap.Pop(prewarmHostHeap)

		if v == nil {
			break
		}

		host := v.(*prewarmHost)
		host.NumProvisioning += 1

		go func() {
			err := p.ProvisionContainer(host.Host)
			if err != nil {
				p.log.Error("Failed to provision new pre-warmed container on host %s (ID=%s): %v",
					host.Host.GetNodeName(), host.Host.GetID(), err)
			}
		}()

		heap.Push(prewarmHostHeap, host)
		newContainersProvisioned += 1
	}

	if newContainersProvisioned < numToCreate {
		p.log.Warn("Only began provisioning %d/%d new prewarm containers", newContainersProvisioned, numToCreate)
	} else {
		p.log.Debug("Began provisioning %d/%d new prewarm containers", newContainersProvisioned, numToCreate)
	}
}

// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
// ContainerPrewarmer's policy.
func (p *FixedCapacityPrewarmer) ValidateHostCapacity(_ scheduling.Host) {
	p.log.Warn("ValidateHostCapacity called for FixedCapacityPrewarmer.")
	p.log.Warn("FixedCapacityPrewarmer only uses ValidatePoolCapacity.")
	// No-op.
}

// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
// below this quantity, then a new pre-warmed container will be provisioned.
func (p *FixedCapacityPrewarmer) MinPrewarmedContainersPerHost() int {
	return 0
}

// prewarmContainerUsed is called when a pre-warm container is used, to give the container prewarmer a chance
// to react (i.e., provision another prewarm container, if it is supposed to do so).
func (p *FixedCapacityPrewarmer) prewarmContainerUsed(host scheduling.Host, prewarmedContainer scheduling.PrewarmedContainer) {
	if !p.Config.ProactiveReplacementEnabled {
		return
	}

	err := p.ProvisionContainer(host)
	if err != nil {
		p.log.Error("Failed to provision new pre-warmed container on host %s (ID=%s) after prewarm container \"%s\" was used: %v",
			host.GetNodeName(), host.GetID(), prewarmedContainer.ID(), err)
	}
}
