package prewarm

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

type ContainerPrewarmer struct {
	// PrewarmContainers is a map from prewarm/temporary ID to scheduling.KernelContainer consisting
	// of pre-warmed containers.
	PrewarmContainers map[string]scheduling.KernelContainer

	// NumPrewarmContainersPerHost is a map from scheduling.Host ID to the number of pre-warmed containers on
	// that scheduling.Host.
	NumPrewarmContainersPerHost map[string]int

	// Scheduler is a reference to the scheduling.Scheduler.
	Scheduler scheduling.Scheduler

	// Cluster is a reference to the scheduling.Cluster.
	Cluster scheduling.Cluster

	// initialNumPerHost is the number of pre-warmed containers to create per host at the very beginning.
	initialNumPerHost int

	log logger.Logger
}

// NewContainerPrewarmer creates a new ContainerPrewarmer struct and returns a pointer to it.
func NewContainerPrewarmer(cluster scheduling.Cluster, initialNumContainersPerHost int) *ContainerPrewarmer {
	warmer := &ContainerPrewarmer{
		PrewarmContainers:           make(map[string]scheduling.KernelContainer),
		NumPrewarmContainersPerHost: make(map[string]int),
		Cluster:                     cluster,
		Scheduler:                   cluster.Scheduler(),
		initialNumPerHost:           initialNumContainersPerHost,
	}

	config.InitLogger(&warmer.log, warmer)

	return warmer
}

// OnPrewarmedContainerUsed is a callback to execute when a pre-warmed container is used.
func (p *ContainerPrewarmer) OnPrewarmedContainerUsed() {
	// No-op.
}

// OnKernelStopped is a callback to execute when a scheduling.Kernel is stopped.
func (p *ContainerPrewarmer) OnKernelStopped() {
	// No-op.
}

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *ContainerPrewarmer) ProvisionContainer(host scheduling.Host) error {
	panic("Not implemented.")
}

// ProvisionContainers is used to provision n pre-warmed scheduling.KernelContainer instances on the specified scheduling.Host.
func (p *ContainerPrewarmer) ProvisionContainers(host scheduling.Host, n int) error {
	// If we're not supposed to provision any containers, then return immediately.
	if n == 0 {
		p.log.Warn("Instructed to prewarm 0 containers on host %s...", host.GetNodeName())
		return nil
	}

	// If the target host is nil, then return an error.
	if host == nil {
		return scheduling.ErrNilHost
	}

	// If we're just supposed to provision a single container, then do so.
	if n == 1 {
		p.log.Debug("Instructed to prewarm a single container on host %s.", host.GetNodeName())
		return p.ProvisionContainer(host)
	}

	p.log.Debug("Instructed to prewarm a %d containers on host %s.", n, host.GetNodeName())

	// Determine how many worker goroutines to use.
	var nWorkers int
	if n > 8 {
		nWorkers = 4
	} else if n > 2 {
		nWorkers = 2
	} else {
		nWorkers = 1
	}

	containersToCreate := make([]int, 0, nWorkers)
	containersPerWorker := n / nWorkers

	host.StartKernelReplica()
}

func (p *ContainerPrewarmer) Start() {
	// If we're not supposed to create any pre-warmed containers upon starting, then just return immediately.
	if p.initialNumPerHost == 0 {
		return
	}

	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		return true
	})
}
