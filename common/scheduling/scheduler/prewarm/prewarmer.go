package prewarm

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"golang.org/x/net/context"
	"time"
)

// DivideWork divides the work of creating n containers between m workers.
//
// DivideWork returns an array of length m, where arr[i] is the number of containers that should be created
// by worker i.
func DivideWork(n, m int) []int {
	result := make([]int, m)

	// Calculate the base work for each worker.
	base := n / m
	remainder := n % m

	// Distribute the work of creating n containers amongst the m workers.
	for i := 0; i < m; i++ {
		result[i] = base

		// Distribute the remainder evenly amongst the workers.
		if i < remainder {
			result[i]++
		}
	}

	return result
}

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
func (p *ContainerPrewarmer) provisionContainer(host scheduling.Host) (*proto.KernelConnectionInfo, error) {
	p.log.Debug("Provisioning pre-warmed container on host %s.", host.GetNodeName())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	return host.StartKernelReplica(ctx, nil)
}

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *ContainerPrewarmer) ProvisionContainer(host scheduling.Host) error {
	p.log.Debug("Provisioning pre-warmed container on host %s.", host.GetNodeName())

	resp, err := p.provisionContainer(host)

	if err != nil {
		p.log.Error("Failed to provision pre-warmed container on host %s because: %v", host.GetNodeName(), err)
		return err
	}

	p.registerPrewarmedContainer(resp, host)
	return nil
}

// registerPrewarmedContainer registers a pre-warmed container that was successfully created on the specified Host.
func (p *ContainerPrewarmer) registerPrewarmedContainer(connInfo *proto.KernelConnectionInfo, host scheduling.Host) {
	p.log.Debug("Registering pre-warmed container created on host %s.", host.GetNodeName())

	panic("Not implemented.")
}

// provisionContainers provisions n pre-warmed scheduling.KernelContainer instances on the specified scheduling.Host.
func (p *ContainerPrewarmer) provisionContainers(host scheduling.Host, n int) error {
	for i := 0; i < n; i++ {
		err := p.ProvisionContainer(host)

		if err != nil {
			return err
		}
	}

	p.log.Debug("Successfully provisioned %d pre-warmed container(s) on host %s.", n, host.GetNodeName())
	return nil
}

// ProvisionContainers is used to launch a job of provisioning n pre-warmed scheduling.KernelContainer instances on
// the specified scheduling.Host. The work of provisioning the n containers is distributed amongst several goroutines,
// the number of which depends upon the size of n.
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

	work := DivideWork(n, nWorkers)

	for i := 0; i < nWorkers; i++ {
		go func() {
			err := p.provisionContainers(host, work[i])
			if err != nil {
				// TODO: Do something...
			}
		}()
	}

	// TODO: Return something meaningful.
	return nil
}

func (p *ContainerPrewarmer) Start() error {
	// If we're not supposed to create any pre-warmed containers upon starting, then just return immediately.
	if p.initialNumPerHost == 0 {
		return nil
	}

	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		go func() {
			err := p.ProvisionContainers(host, p.initialNumPerHost)
			if err != nil {
				// TODO: Do something meaningful.
			}
		}()

		return true
	})

	// TODO: Return something meaningful.
	return nil
}
