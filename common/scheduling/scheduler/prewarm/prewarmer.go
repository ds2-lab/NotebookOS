package prewarm

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPrewarmedContainerRegistrationFailure = errors.New("could not register the specified prewarmed container")
	ErrPrewarmedContainerAlreadyRegistered   = errors.New("a prewarmed container with the same ID is already registered")
	ErrNoPrewarmedContainersAvailable        = errors.New("there are no prewarmed containers available on the specified host")
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

type PrewarmedContainer struct {
	Host              scheduling.Host
	ConnectionInfo    *proto.KernelConnectionInfo
	KernelReplicaSpec *proto.KernelReplicaSpec
}

type ContainerPrewarmer struct {
	// AllPrewarmContainers is a map from prewarm/temporary ID to scheduling.KernelContainer consisting
	// of pre-warmed containers.
	AllPrewarmContainers map[string]*PrewarmedContainer

	// PrewarmContainersPerHost is a map from host ID to a queue of PrewarmedContainer created and available on the
	// associated host.
	PrewarmContainersPerHost map[string]*queue.ThreadsafeFifo[*PrewarmedContainer]

	// Scheduler is a reference to the scheduling.Scheduler.
	Scheduler scheduling.Scheduler

	// Cluster is a reference to the scheduling.Cluster.
	Cluster scheduling.Cluster

	// initialNumPerHost is the number of pre-warmed containers to create per host at the very beginning.
	initialNumPerHost int

	mu  sync.Mutex
	log logger.Logger
}

// NewContainerPrewarmer creates a new ContainerPrewarmer struct and returns a pointer to it.
func NewContainerPrewarmer(cluster scheduling.Cluster, initialNumContainersPerHost int) *ContainerPrewarmer {
	warmer := &ContainerPrewarmer{
		AllPrewarmContainers:     make(map[string]*PrewarmedContainer),
		PrewarmContainersPerHost: make(map[string]*queue.ThreadsafeFifo[*PrewarmedContainer]),
		Cluster:                  cluster,
		Scheduler:                cluster.Scheduler(),
		initialNumPerHost:        initialNumContainersPerHost,
	}

	config.InitLogger(&warmer.log, warmer)

	return warmer
}

// RequestPrewarmContainer is used to request a pre-warm container on a particular host.
//
// RequestPrewarmContainer is explicitly thread safe (i.e., it uses a mutex).
func (p *ContainerPrewarmer) RequestPrewarmContainer(host scheduling.Host) (*PrewarmedContainer, error) {
	p.log.Debug("Received request[Host %s (ID=%s)].", host.GetNodeName(), host.GetID())

	p.mu.Lock()
	defer p.mu.Unlock()

	containers, loaded := p.PrewarmContainersPerHost[host.GetID()]

	// If there is no queue associated with the specified host, then we'll create the queue,
	// but we'll still return an error, as we'll have no containers available.
	if !loaded {
		p.log.Debug("Request rejected[Host %s (ID=%s), NoneAvailable, UnknownHost].",
			host.GetNodeName(), host.GetID(), containers.Len())

		fifo := queue.NewThreadsafeFifo[*PrewarmedContainer](p.initialNumPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo

		return nil, fmt.Errorf("%w: host \"%s\" (ID=\"%s\")",
			ErrNoPrewarmedContainersAvailable, host.GetNodeName(), host.GetID())
	}

	if containers.Len() == 0 {
		p.log.Debug("Request rejected[Host %s (ID=%s), NoneAvailable].",
			host.GetNodeName(), host.GetID(), containers.Len())

		return nil, fmt.Errorf("%w: host \"%s\" (ID=\"%s\")",
			ErrNoPrewarmedContainersAvailable, host.GetNodeName(), host.GetID())
	}

	prewarmedContainer, ok := containers.Dequeue()

	// Sanity check. Since this is all occurring with the mutex held,
	// `ok` should always be true and `prewarmedContainer` should never be nil.
	if prewarmedContainer == nil || !ok {
		panic("Expected to receive valid, non-nil pre-warmed container.")
	}

	p.log.Debug("Request fulfilled[Host %s (ID=%s), Remaining=%d].",
		host.GetNodeName(), host.GetID(), containers.Len())

	return prewarmedContainer, nil
}

// ReturnUnusedPrewarmContainer is used to return a pre-warmed container that was originally returned to the caller
// via the RequestPrewarmContainer method, but ended up being unused, and so it can simply be put back into the pool.
func (p *ContainerPrewarmer) ReturnUnusedPrewarmContainer(container *PrewarmedContainer) error {
	return p.registerPrewarmedContainer(container)
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
func (p *ContainerPrewarmer) provisionContainer(host scheduling.Host) (*proto.KernelConnectionInfo, *proto.KernelReplicaSpec, error) {
	p.log.Debug("Provisioning pre-warmed container on host %s.", host.GetNodeName())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	kernelSpec := &proto.KernelSpec{
		Id:              uuid.NewString(),
		Session:         "",
		Argv:            []string{"~/home/Python3.12.6/debug/python3", "-m", "distributed_notebook.kernel", "-f", "{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"},
		SignatureScheme: "hmac-sha256",
		Key:             "",
		ResourceSpec:    proto.NewResourceSpec(0, 0, 0, 0),
		WorkloadId:      "",
	}

	spec := &proto.KernelReplicaSpec{
		Kernel:           kernelSpec,
		ReplicaId:        0,
		NumReplicas:      0,
		Join:             false,
		WorkloadId:       "",
		PrewarmContainer: true,
	}

	resp, err := host.StartKernelReplica(ctx, spec)

	return resp, spec, err
}

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *ContainerPrewarmer) ProvisionContainer(host scheduling.Host) error {
	p.log.Debug("Provisioning pre-warmed container on host %s.", host.GetNodeName())

	resp, spec, err := p.provisionContainer(host)

	if err != nil {
		p.log.Error("Failed to provision pre-warmed container on host %s because: %v", host.GetNodeName(), err)
		return err
	}

	return p.registerPrewarmedContainerInfo(resp, spec, host)
}

func (p *ContainerPrewarmer) registerPrewarmedContainer(container *PrewarmedContainer) error {
	return p.registerPrewarmedContainerInfo(container.ConnectionInfo, container.KernelReplicaSpec, container.Host)
}

// registerPrewarmedContainerInfo registers a pre-warmed container that was successfully created on the specified Host.
//
// registerPrewarmedContainerInfo is explicitly thread safe.
func (p *ContainerPrewarmer) registerPrewarmedContainerInfo(connInfo *proto.KernelConnectionInfo, spec *proto.KernelReplicaSpec, host scheduling.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Debug("Registering pre-warmed container created on host %s.", host.GetNodeName())

	prewarmedContainer := &PrewarmedContainer{
		Host:              host,
		ConnectionInfo:    connInfo,
		KernelReplicaSpec: spec,
	}

	// Verify that the specified pre-warmed container isn't already registered.
	if _, loaded := p.AllPrewarmContainers[spec.Kernel.Id]; loaded {
		return fmt.Errorf("%w: %w: \"%s\"", ErrPrewarmedContainerRegistrationFailure,
			ErrPrewarmedContainerAlreadyRegistered, spec.Kernel.Id)
	}

	p.AllPrewarmContainers[spec.Kernel.Id] = prewarmedContainer

	fifo, _ := p.PrewarmContainersPerHost[host.GetID()]
	fifo.Enqueue(prewarmedContainer)

	p.log.Debug("Number of pre-warmed containers on host %s: %d", host.GetNodeName(), fifo.Len())
	return nil
}

// provisionContainers provisions n pre-warmed scheduling.KernelContainer instances on the specified scheduling.Host.
//
// provisionContainers returns the number of pre-warmed scheduling.KernelContainer instances created.
func (p *ContainerPrewarmer) provisionContainers(host scheduling.Host, n int) (int, error) {
	for i := 0; i < n; i++ {
		err := p.ProvisionContainer(host)

		if err != nil {
			return i, err
		}
	}

	p.log.Debug("Successfully provisioned %d pre-warmed container(s) on host %s.", n, host.GetNodeName())
	return n, nil
}

// ProvisionContainers is used to launch a job of provisioning n pre-warmed scheduling.KernelContainer instances on
// the specified scheduling.Host. The work of provisioning the n containers is distributed amongst several goroutines,
// the number of which depends upon the size of n.
//
// ProvisionContainers returns the number of scheduling.KernelContainer instances that were successfully pre-warmed.
//
// ProvisionContainers will panic if the given scheduling.Host is nil.
func (p *ContainerPrewarmer) ProvisionContainers(host scheduling.Host, n int) int32 {
	// If we're not supposed to provision any containers, then return immediately.
	if n == 0 {
		p.log.Warn("Instructed to prewarm 0 containers on host %s...", host.GetNodeName())
		return 0
	}

	// If the target host is nil, then return an error.
	if host == nil {
		panic(scheduling.ErrNilHost)
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

	var wg sync.WaitGroup
	numCreated := atomic.Int32{}
	work := DivideWork(n, nWorkers)

	for i := 0; i < nWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			created, _ := p.provisionContainers(host, work[i])

			numCreated.Add(int32(created))
		}()
	}

	wg.Wait()

	return numCreated.Load()
}

// ProvisionInitialPrewarmContainers provisions the configured number of initial pre-warmed containers on each host.
//
// ProvisionInitialPrewarmContainers returns the number of pre-warmed containers that were created as well as the
// maximum number that were supposed to be created (if no errors were to occur).
func (p *ContainerPrewarmer) ProvisionInitialPrewarmContainers() (created int32, target int32) {
	// If we're not supposed to create any pre-warmed containers upon starting, then just return immediately.
	if p.initialNumPerHost == 0 {
		return 0, 0
	}

	var wg sync.WaitGroup
	target = 0
	created = 0

	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		wg.Add(1)
		atomic.AddInt32(&target, int32(p.initialNumPerHost))

		fifo := queue.NewThreadsafeFifo[*PrewarmedContainer](p.initialNumPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo

		go func() {
			defer wg.Done()
			numCreated := p.ProvisionContainers(host, p.initialNumPerHost)
			atomic.AddInt32(&created, numCreated)
		}()

		return true
	})

	wg.Wait()

	return atomic.LoadInt32(&created), atomic.LoadInt32(&target)
}
