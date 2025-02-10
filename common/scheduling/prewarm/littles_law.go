package prewarm

import (
	"container/heap"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"math"
	"sync"
	"time"
)

const (
	prewarmHostMetadataKey types.HeapElementMetadataKey = "PrewarmHost"
)

type prewarmHost struct {
	Host            scheduling.Host
	NumProvisioning int32
	CurrentNum      int32

	indexes map[types.HeapElementMetadataKey]int
	meta    map[types.HeapElementMetadataKey]interface{}

	metaMu  sync.Mutex
	indexMu sync.Mutex
}

func newPrewarmHost(host scheduling.Host, current int32, provisioning int32) *prewarmHost {
	return &prewarmHost{
		Host:            host,
		NumProvisioning: provisioning,
		CurrentNum:      current,
		indexes:         make(map[types.HeapElementMetadataKey]int),
		meta:            make(map[types.HeapElementMetadataKey]interface{}),
	}
}

// TotalNumPrewarm returns the sum of NumProvisioning and CurrentNum.
func (p *prewarmHost) TotalNumPrewarm() int32 {
	return p.NumProvisioning + p.CurrentNum
}

func (p *prewarmHost) SetIdx(key types.HeapElementMetadataKey, idx int) {
	p.indexMu.Lock()
	defer p.indexMu.Unlock()

	p.indexes[key] = idx
}

func (p *prewarmHost) GetIdx(key types.HeapElementMetadataKey) int {
	p.indexMu.Lock()
	defer p.indexMu.Unlock()

	return p.indexes[key]
}

func (p *prewarmHost) String() string {
	return fmt.Sprintf("PrewarmHost[HostName=%s,HostId=%s,NumProvisioned=%d,CurrentNum=%d]",
		p.Host.GetNodeName(), p.Host.GetID(), p.NumProvisioning, p.CurrentNum)
}

func (p *prewarmHost) SetMeta(key types.HeapElementMetadataKey, value interface{}) {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	p.meta[key] = value
}

func (p *prewarmHost) GetMeta(key types.HeapElementMetadataKey) interface{} {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()

	return p.meta[key]
}

// Compare compares the object with specified object.
// Returns negative, 0, positive if the object is smaller than, equal to, or larger than specified object respectively.
func (p *prewarmHost) Compare(o interface{}) float64 {
	p2 := o.(*prewarmHost)

	return float64(p.TotalNumPrewarm() - p2.TotalNumPrewarm())
}

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

	TargetPoolSize int32
}

// NewLittlesLawPrewarmer creates a new LittlesLawPrewarmer struct and returns a pointer to it.
func NewLittlesLawPrewarmer(cluster scheduling.Cluster, configuration *LittlesLawPrewarmerConfig,
	metricsProvider scheduling.MetricsProvider) *LittlesLawPrewarmer {

	base := NewBaseContainerPrewarmer(cluster, configuration.PrewarmerConfig, metricsProvider)

	warmer := &LittlesLawPrewarmer{
		BaseContainerPrewarmer: base,
		Config:                 configuration,
	}

	base.instance = warmer
	warmer.instance = warmer

	warmer.TargetPoolSize = int32(math.Ceil(float64(configuration.Lambda * configuration.W)))

	return warmer
}

// Run creates a separate goroutine in which the LittlesLawPrewarmer maintains the overall capacity/availability of
// pre-warmed containers in accordance with LittlesLawPrewarmer's policy for doing so.
func (p *LittlesLawPrewarmer) Run() {
	for {
		select {
		case <-p.stopChan:
			{
				p.log.Debug("Stopping.")
				return
			}
		default:
		}

		p.ValidatePoolCapacity()

		time.Sleep(scheduling.PreWarmerInterval)
	}
}

// ValidatePoolCapacity ensures that there are enough pre-warmed containers available throughout the system.
func (p *LittlesLawPrewarmer) ValidatePoolCapacity() {
	p.mu.Lock()
	defer p.mu.Unlock()

	numActiveExecutions := p.metricsProvider.NumActiveExecutions()

	prewarmHostHeap := types.NewHeap(prewarmHostMetadataKey)

	numProvisioning := int32(0)
	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
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

	totalSize := numActiveExecutions + int32(p.PoolSize()) + numProvisioning

	if totalSize >= p.TargetPoolSize {
		return
	}

	numToCreate := p.TargetPoolSize - totalSize
	p.log.Debug("Need to create %d new prewarmed containers (curr=%d, target=%d).",
		numToCreate, totalSize, p.TargetPoolSize)

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
func (p *LittlesLawPrewarmer) ValidateHostCapacity(_ scheduling.Host) {
	p.log.Warn("ValidateHostCapacity called for LittlesLawPrewarmer.")
	p.log.Warn("LittlesLawPrewarmer uses ValidatePoolCapacity.")
	// No-op.
}

// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
// below this quantity, then a new pre-warmed container will be provisioned.
func (p *LittlesLawPrewarmer) MinPrewarmedContainersPerHost() int {
	return 0
}
