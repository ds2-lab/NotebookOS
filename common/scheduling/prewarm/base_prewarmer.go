package prewarm

import (
	"container/heap"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/queue"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"golang.org/x/net/context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPrewarmedContainerRegistrationFailure = errors.New("could not register the specified prewarmed container")
	ErrPrewarmedContainerAlreadyRegistered   = errors.New("a prewarmed container with the same ID is already registered")
	ErrPrewarmedContainerAlreadyUsed         = errors.New("the prewarmed container has already been used")
	ErrPrewarmedContainerUnused              = errors.New("the prewarmed container has not yet been used")
	ErrNoPrewarmedContainersAvailable        = errors.New("there are no prewarmed containers available on the specified host")
	ErrAlreadyRunning                        = errors.New("the prewarmer is already running")
	ErrFailedToStop                          = errors.New("the prewarmer failed to stop cleanly")
	ErrNotRunning                            = errors.New("the prewarmer is not running")
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

// PrewarmedContainerBuilder is a utility struct used to simplify the process of creating PrewarmedContainer structs.
type PrewarmedContainerBuilder struct {
	host                             scheduling.Host
	connectionInfo                   *proto.KernelConnectionInfo
	kernelReplicaSpec                *proto.KernelReplicaSpec
	onPrewarmedContainerUsedCallback scheduling.PrewarmedContainerUsedCallback
}

func NewPrewarmedContainerBuilder() *PrewarmedContainerBuilder {
	return &PrewarmedContainerBuilder{}
}

func (b *PrewarmedContainerBuilder) WithHost(host scheduling.Host) *PrewarmedContainerBuilder {
	b.host = host
	return b
}

func (b *PrewarmedContainerBuilder) WithKernelConnectionInfo(connectionInfo *proto.KernelConnectionInfo) *PrewarmedContainerBuilder {
	b.connectionInfo = connectionInfo
	return b
}

func (b *PrewarmedContainerBuilder) WithKernelReplicaSpec(kernelReplicaSpec *proto.KernelReplicaSpec) *PrewarmedContainerBuilder {
	b.kernelReplicaSpec = kernelReplicaSpec
	return b
}

func (b *PrewarmedContainerBuilder) WithPrewarmedContainerUsedCallback(callback scheduling.PrewarmedContainerUsedCallback) *PrewarmedContainerBuilder {
	b.onPrewarmedContainerUsedCallback = callback
	return b
}

func (b *PrewarmedContainerBuilder) Build() *PrewarmedContainer {
	container := &PrewarmedContainer{
		host:                     b.host,
		connectionInfo:           b.connectionInfo,
		kernelReplicaSpec:        b.kernelReplicaSpec,
		createdAt:                time.Now(),
		onPrewarmedContainerUsed: b.onPrewarmedContainerUsedCallback,
	}

	container.available.Store(true)

	return container
}

// PrewarmedContainer encapsulates information about a pre-warmed container that exists on a particular Host.
type PrewarmedContainer struct {
	host              scheduling.Host
	connectionInfo    *proto.KernelConnectionInfo
	kernelReplicaSpec *proto.KernelReplicaSpec
	createdAt         time.Time
	available         atomic.Bool

	// onPrewarmedContainerUsed is a callback function to be called by the scheduling.Scheduler if it commits
	// to using a prewarmed container.
	onPrewarmedContainerUsed scheduling.PrewarmedContainerUsedCallback
}

func (p *PrewarmedContainer) Host() scheduling.Host {
	return p.host
}

func (p *PrewarmedContainer) KernelConnectionInfo() *proto.KernelConnectionInfo {
	return p.connectionInfo
}

func (p *PrewarmedContainer) KernelReplicaSpec() *proto.KernelReplicaSpec {
	return p.kernelReplicaSpec
}

func (p *PrewarmedContainer) CreatedAt() time.Time {
	return p.createdAt
}

func (p *PrewarmedContainer) IsAvailable() bool {
	return p.available.Load()
}

func (p *PrewarmedContainer) SetUnavailable() {
	p.available.Store(false)
}

func (p *PrewarmedContainer) String() string {
	return fmt.Sprintf("PrewarmContainer[ID=%s,Host=%s,HostId=%s]",
		p.kernelReplicaSpec.Kernel.Id, p.host.GetNodeName(), p.host.GetID())
}

func (p *PrewarmedContainer) Age() time.Duration {
	return time.Since(p.createdAt)
}

func (p *PrewarmedContainer) ID() string {
	return p.kernelReplicaSpec.Kernel.Id
}

func (p *PrewarmedContainer) HostId() string {
	return p.host.GetID()
}

func (p *PrewarmedContainer) HostName() string {
	return p.host.GetNodeName()
}

// OnPrewarmedContainerUsed is a callback to execute when a pre-warmed container is used.
//
// If this PrewarmedContainer is officially used, then this function should be called.
func (p *PrewarmedContainer) OnPrewarmedContainerUsed() {
	if p.onPrewarmedContainerUsed != nil {
		p.onPrewarmedContainerUsed(p)
	}
}

// GetOnPrewarmedContainerUsed returns the callback to execute when a pre-warmed container is used.
func (p *PrewarmedContainer) GetOnPrewarmedContainerUsed() scheduling.PrewarmedContainerUsedCallback {
	return p.onPrewarmedContainerUsed
}

// PrewarmerConfig encapsulates configuration information/parameters of a scheduling.ContainerPrewarmer implementation.
type PrewarmerConfig struct {
	// InitialPrewarmedContainersPerHost returns the number of pre-warmed containers to create per host after the
	// conclusion of the 'initial connection period'.
	InitialPrewarmedContainersPerHost int

	// MaxPrewarmedContainersPerHost is the maximum number of pre-warmed containers that should be provisioned on any
	// given scheduling.Host at any given time. If there are MaxPrewarmedContainersPerHost pre-warmed containers
	// available on a given scheduling.Host, then more will not be provisioned.
	//
	// If MaxPrewarmedContainersPerHost is negative, then there will be no limit/cap on the number of pre-warmed
	// containers that can be created on any given scheduling.Host.
	MaxPrewarmedContainersPerHost int

	// Interval is the frequency at which the scheduling.ContainerPrewarmer should execute each iteration of its
	// Run method.
	Interval time.Duration
}

// NewPrewarmerConfig creates a new PrewarmerConfig struct and returns a pointer to it.
func NewPrewarmerConfig(initialCapacity, maxCapacity int, intervalSec float64) *PrewarmerConfig {
	interval := time.Millisecond * time.Duration(intervalSec*1000.0)

	return &PrewarmerConfig{
		InitialPrewarmedContainersPerHost: initialCapacity,
		MaxPrewarmedContainersPerHost:     maxCapacity,
		Interval:                          interval,
	}
}

type containerPrewarmer interface {
	scheduling.ContainerPrewarmer

	// prewarmContainerUsed is called when a pre-warm container is used, to give the container prewarmer a chance
	// to react (i.e., provision another prewarm container, if it is supposed to do so).
	prewarmContainerUsed(scheduling.Host, scheduling.PrewarmedContainer)
}

// BaseContainerPrewarmer is responsible for provisioning pre-warmed containers and maintaining information about
// these pre-warmed containers, such as how many are available on each scheduling.Host.
type BaseContainerPrewarmer struct {
	// instance is the scheduling.ContainerPrewarmer struct that may be promoting this BaseContainerPrewarmer struct.
	// Methods with logic specific to each unique implementation of scheduling.ContainerPrewarmer are called on the
	// instance field rather than executed directly by the BaseContainerPrewarmer struct.
	instance containerPrewarmer

	// AllPrewarmContainers is a map from prewarm/temporary ID to scheduling.KernelContainer consisting
	// of pre-warmed containers.
	AllPrewarmContainers map[string]scheduling.PrewarmedContainer

	// PrewarmContainersPerHost is a map from host ID to a queue of PrewarmedContainer created and available on the
	// associated host.
	PrewarmContainersPerHost map[string]*queue.ThreadsafeFifo[scheduling.PrewarmedContainer]

	// NumPrewarmContainersProvisioningPerHost is a map from scheduling.Host ID to the number of PrewarmedContainer instances currently
	// being provisioned on that scheduling.Host.
	NumPrewarmContainersProvisioningPerHost map[string]*atomic.Int32

	// Cluster is a reference to the scheduling.Cluster.
	Cluster scheduling.Cluster

	// Config encapsulates the configuration of the BaseContainerPrewarmer.
	Config *PrewarmerConfig

	// GuardChannel is a channel used during unit tests that can be assigned to a scheduling.ContainerPrewarmer.
	// If the GuardChannel is non-nil, then the scheduling.ContainerPrewarmer will poll the channel, waiting to
	// receive a value at the end of each iteration of its Run method.
	GuardChannel chan struct{}

	// totalNumProvisioning is the total number of prewarm containers being provisioned across all scheduling.Host
	// instances in the scheduling.Cluster.
	totalNumProvisioning atomic.Int32

	// metricsProvider provides access to some metrics necessary for certain implementations of X,
	// such as the number of active executions.
	metricsProvider scheduling.MetricsProvider

	// stopChan is used to signal to the goroutine executing the Run method that it should stop and exit.
	stopChan chan struct{}

	// running ensures that only one goroutine can execute the Run method at a time.
	running atomic.Int32

	mu  sync.Mutex
	log logger.Logger
}

// NewBaseContainerPrewarmer creates a new BaseContainerPrewarmer struct and returns a pointer to it.
func NewBaseContainerPrewarmer(cluster scheduling.Cluster, configuration *PrewarmerConfig, metricsProvider scheduling.MetricsProvider) *BaseContainerPrewarmer {
	warmer := &BaseContainerPrewarmer{
		AllPrewarmContainers:                    make(map[string]scheduling.PrewarmedContainer),
		PrewarmContainersPerHost:                make(map[string]*queue.ThreadsafeFifo[scheduling.PrewarmedContainer]),
		NumPrewarmContainersProvisioningPerHost: make(map[string]*atomic.Int32),
		stopChan:                                make(chan struct{}),
		metricsProvider:                         metricsProvider,
		Cluster:                                 cluster,
		Config:                                  configuration,
	}

	if configuration.Interval <= 0 {
		configuration.Interval = scheduling.DefaultPreWarmerInterval
	}

	config.InitLogger(&warmer.log, warmer)

	return warmer
}

// Len returns the total number of prewarmed containers available.
//
// Len is ultimately just an alias for PoolSize.
func (p *BaseContainerPrewarmer) Len() int {
	return p.PoolSize()
}

// GetNumPrewarmContainersOnHost returns the number of pre-warmed containers currently available on the targeted scheduling.Host as well
// as the number of pre-warmed containers that are currently being provisioned on the targeted scheduling.Host.
func (p *BaseContainerPrewarmer) GetNumPrewarmContainersOnHost(host scheduling.Host) (curr int, provisioning int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	curr, provisioning = p.unsafeHostLen(host)
	return
}

// Size returns the total number of prewarmed containers available.
//
// Size is ultimately just an alias for PoolSize.
func (p *BaseContainerPrewarmer) Size() int {
	return p.PoolSize()
}

// PoolSize returns the total number of prewarmed containers available.
//
// Size and Len are ultimately just aliases for PoolSize.
func (p *BaseContainerPrewarmer) PoolSize() int {
	return len(p.AllPrewarmContainers)
}

// Run maintains the overall capacity/availability of pre-warmed containers in accordance with BaseContainerPrewarmer's
// policy for doing so.
//
// Run should be executed within its own goroutine.
//
// If another thread is executing the Run method, then Run will return an error. Only one goroutine may execute
// the Run method at a time.
func (p *BaseContainerPrewarmer) Run() error {
	if !p.running.CompareAndSwap(0, 1) {
		p.log.Warn("Cannot run prewarmer as already running")
		return ErrAlreadyRunning
	}

	p.log.Debug("Started running.")

	doStop := func() error {
		if !p.running.CompareAndSwap(1, 0) {
			p.log.Warn("Prewarmer failed to stop cleanly")
			return ErrFailedToStop
		}

		p.log.Debug("Prewarmer stopped.")
		return nil
	}

	for {
		select {
		case <-p.stopChan:
			{
				return doStop()
			}
		default:
		}

		p.ValidatePoolCapacity()

		if p.GuardChannel != nil {
			p.log.Debug("Polling on GuardChannel")

			select {
			case <-p.GuardChannel:
				{
					// No-op.
				}
			case <-p.stopChan:
				{
					return doStop()
				}
			}

			p.log.Debug("Received value from GuardChannel")
		} else {
			time.Sleep(p.Config.Interval)
		}
	}
}

// IsRunning returns true if the target ContainerPrewarmer is actively running.
func (p *BaseContainerPrewarmer) IsRunning() bool {
	return p.running.Load() > 0
}

// Stop instructs the ContainerPrewarmer to stop.
//
// Stop will block until the notification to stop has been receiving by the goroutine running the Run method.
func (p *BaseContainerPrewarmer) Stop() error {
	if !p.IsRunning() {
		p.log.Warn("Prewarmer is not running.")
		return errors.Join(ErrFailedToStop, ErrNotRunning)
	}

	p.log.Debug("Stopping...")
	p.stopChan <- struct{}{}

	return nil
}

// RequestProvisionContainers attempts to provision n new pre-warm containers on scheduling.Host instances that
// are validated by the provided HostCriteriaFunction. If the HostCriteriaFunction is nil, then the scheduling.Host
// instances are selected at the discretion of the implementing ContainerPrewarmer.
//
// RequestProvisionContainers returns a map from host ID to the number of prewarm containers provisioned on that
// scheduling.Host, as well as an error, if one occurred.
func (p *BaseContainerPrewarmer) RequestProvisionContainers(n int, criteria scheduling.HostCriteriaFunction, separateHostsOnly bool) (map[string]int, error) {
	if n == 0 {
		return make(map[string]int), nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		// Keep track of the hosts on which we've provisioned a pre-warm container in case 'separateHostsOnly' is true.
		provisionMap           = make(map[string]int)
		totalNumCreated        = 0
		startTime              = time.Now()
		createdContainerMap    = make(map[string]int)
		consecutiveFailures    = 0 // Failure means we didn't provision any prewarm containers that iteration
		maxConsecutiveFailures = 2
	)

	for totalNumCreated < n && consecutiveFailures < maxConsecutiveFailures {
		// We recreate the heap every iteration.
		prewarmHostHeap := types.NewHeap(prewarmHostMetadataKey)

		p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
			// Skip disabled hosts.
			if !host.Enabled() {
				return true
			}

			// Skip hosts that are excluded from scheduling (i.e., probably hosts that are being idle-reclaimed).
			if host.IsExcludedFromScheduling() {
				return true
			}

			// If we're only supposed to create pre-warm containers on a strictly separate hosts, and we've already
			// created a prewarm container on this host, then we'll skip it.
			if nProvisioned, ok := provisionMap[host.GetID()]; separateHostsOnly && ok && nProvisioned > 0 {
				return true
			}

			// If a criteria function was provided, then skip the host if it does not satisfy the criteria.
			if criteria != nil && criteria(host) != nil {
				return true
			}

			count, provisioning := p.unsafeHostLen(host)
			combined := count + provisioning

			// Check if the host is at-capacity.
			if p.Config.MaxPrewarmedContainersPerHost >= 0 && combined >= p.Config.MaxPrewarmedContainersPerHost {
				return true
			}

			hostWithPrewarm := newPrewarmHost(host, int32(count), int32(provisioning))
			heap.Push(prewarmHostHeap, hostWithPrewarm)

			return true
		})

		numToCreate := n - totalNumCreated
		numCreatedThisIteration := 0

		// Create new prewarm containers evenly across the hosts, such that there is a similar number of prewarm
		// containers on each host in the cluster (including currently-provisioning pre-warm containers).
		//
		// We do this by creating pre-warm containers on hosts with the least number of pre-warm containers.
		for i := 0; i < numToCreate && prewarmHostHeap.Len() > 0; i++ {
			v := heap.Pop(prewarmHostHeap)
			if v == nil {
				break
			}

			host := v.(*prewarmHost)
			if host == nil {
				break
			}

			host.NumProvisioning += 1

			// Record how many containers we're provisioning on this host.
			if val, loaded := provisionMap[host.Host.GetID()]; loaded {
				provisionMap[host.Host.GetID()] = val + 1
			} else {
				provisionMap[host.Host.GetID()] = 1
			}

			go func() {
				err := p.ProvisionContainer(host.Host)
				if err != nil {
					p.log.Error("Failed to provision new pre-warmed container on host %s (ID=%s): %v",
						host.Host.GetNodeName(), host.Host.GetID(), err)
				}
			}()

			heap.Push(prewarmHostHeap, host)
			numCreatedThisIteration += 1
			totalNumCreated += 1
		}

		// If we didn't create any this iteration, then we'll give up.
		// Otherwise, we may be stuck here forever (i.e., if none of the hosts are passing the criteria).
		if numCreatedThisIteration == 0 {
			// If we have two consecutive iterations with no new prewarm containers, then we'll give up.
			consecutiveFailures += 1

			sleepInterval := (time.Millisecond * 250) + (time.Millisecond * time.Duration(rand.Intn(1750)))

			p.log.Warn(
				utils.YellowStyle.Render(
					"Prewarmed %d/%d container(s) this iter. Total so far: %d/%d. Consecutive failures: %d. Time elapsed: %v. Sleeping for %v."),
				numCreatedThisIteration, numToCreate, totalNumCreated, n, consecutiveFailures, time.Since(startTime), sleepInterval)

			// Sleep for a small amount of time, as sometimes things can change if you wait a little.
			time.Sleep(sleepInterval)

			continue
		}

		p.log.Debug(
			utils.LightGreenStyle.Render(
				"Prewarmed 0/%d container(s) this iter. Total so far: %d/%d. Time elapsed: %v."),
			numCreatedThisIteration, numToCreate, totalNumCreated, n, time.Since(startTime))
		consecutiveFailures = 0 // Reset this.
	}

	return createdContainerMap, nil
}

// RequestPrewarmedContainer is used to request a pre-warm container on a particular host.
//
// RequestPrewarmedContainer is explicitly thread safe (i.e., it uses a mutex).
func (p *BaseContainerPrewarmer) RequestPrewarmedContainer(host scheduling.Host) (scheduling.PrewarmedContainer, error) {
	if !host.Enabled() {
		p.log.Warn("Prewarm container requested for DISABLED host %s (ID=%s)...",
			host.GetNodeName(), host.GetID())

		return nil, fmt.Errorf("%w: cannot fulfill prewarm container request for disabled host %s (ID=%s)",
			scheduling.ErrHostDisabled, host.GetNodeName(), host.GetID())
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	containers, loaded := p.PrewarmContainersPerHost[host.GetID()]

	// If there is no queue associated with the specified host, then we'll create the queue,
	// but we'll still return an error, as we'll have no containers available.
	if !loaded {
		p.log.Debug(utils.LightOrangeStyle.Render("✗ Pre-warm container request rejected [Host %s (ID=%s), NoneAvailable, UnknownHost]."),
			host.GetNodeName(), host.GetID())

		fifo := queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo

		return nil, fmt.Errorf("%w: host \"%s\" (ID=\"%s\")",
			ErrNoPrewarmedContainersAvailable, host.GetNodeName(), host.GetID())
	}

	// Check if there are simply no pre-warmed containers available.
	if containers.Len() == 0 {
		p.log.Debug(utils.LightOrangeStyle.Render("✗ Pre-warm container request rejected [Host %s (ID=%s), NoneAvailable]."),
			host.GetNodeName(), host.GetID())

		return nil, fmt.Errorf("%w: host \"%s\" (ID=\"%s\")",
			ErrNoPrewarmedContainersAvailable, host.GetNodeName(), host.GetID())
	}

	prewarmedContainer, ok := containers.Dequeue()

	// Sanity check. Since this is all occurring with the mutex held,
	// `ok` should always be true and `prewarmedContainer` should never be nil.
	if prewarmedContainer == nil || !ok {
		panic("Expected to receive valid, non-nil pre-warmed container.")
	}

	p.log.Debug(utils.LightGreenStyle.Render("✓ Pre-warm container request fulfilled [Host %s (ID=%s), Remaining=%d]."),
		host.GetNodeName(), host.GetID(), containers.Len())

	prewarmedContainer.SetUnavailable()
	delete(p.AllPrewarmContainers, prewarmedContainer.ID())

	if p.instance != nil {
		p.instance.prewarmContainerUsed(host, prewarmedContainer)
	}

	return prewarmedContainer, nil
}

// ReturnPrewarmContainer is used to return a used pre-warmed container so that it may be reused in the future.
func (p *BaseContainerPrewarmer) ReturnPrewarmContainer(container scheduling.PrewarmedContainer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !container.IsAvailable() {
		// Re-create the container so that the "IsAvailable" field is set to true.
		container = NewPrewarmedContainerBuilder().
			WithHost(container.Host()).
			WithPrewarmedContainerUsedCallback(container.GetOnPrewarmedContainerUsed()).
			WithKernelReplicaSpec(container.KernelReplicaSpec()).
			WithKernelConnectionInfo(container.KernelConnectionInfo()).
			Build()
	}

	p.log.Debug("Container returned after being used [%v]", container.String())

	return p.unsafeRegisterPrewarmedContainer(container)
}

// ProvisionInitialPrewarmContainers provisions the configured number of initial pre-warmed containers on each host.
//
// ProvisionInitialPrewarmContainers returns the number of pre-warmed containers that were created as well as the
// maximum number that were supposed to be created (if no errors were to occur).
func (p *BaseContainerPrewarmer) ProvisionInitialPrewarmContainers() (created int32, target int32) {
	// If we're not supposed to create any pre-warmed containers upon starting, then just return immediately.
	if p.Config.InitialPrewarmedContainersPerHost == 0 {
		return 0, 0
	}

	expectedNumResponses := 0
	numCreatedChan := make(chan int32)

	target = 0
	created = 0

	// Clamp initial number to the maximum.
	if p.Config.MaxPrewarmedContainersPerHost > 0 && p.Config.InitialPrewarmedContainersPerHost > p.Config.MaxPrewarmedContainersPerHost {
		p.log.Warn("Configured 'initial prewarmed containers per host' (%d) is greater than configured maximum (%d). Clamping.",
			p.Config.InitialPrewarmedContainersPerHost, p.Config.MaxPrewarmedContainersPerHost)
		p.Config.InitialPrewarmedContainersPerHost = p.Config.MaxPrewarmedContainersPerHost
	}

	p.log.Debug("Will create %d prewarmed container(s) on each host (initially).",
		p.Config.InitialPrewarmedContainersPerHost)

	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		expectedNumResponses += 1

		atomic.AddInt32(&target, int32(p.Config.InitialPrewarmedContainersPerHost))
		p.ProvisionInitialPrewarmContainersOnHost(host, numCreatedChan)
		return true
	})

	numResponses := 0

	for numResponses < expectedNumResponses {
		select {
		case numCreated := <-numCreatedChan:
			{
				atomic.AddInt32(&created, numCreated)
				numResponses += 1
			}
		}
	}

	return atomic.LoadInt32(&created), atomic.LoadInt32(&target)
}

// ProvisionInitialPrewarmContainersOnHost provisions the configured number of 'initial' pre-warm containers on the
// specified scheduling.Host.
func (p *BaseContainerPrewarmer) ProvisionInitialPrewarmContainersOnHost(host scheduling.Host, numCreatedChan chan<- int32) {
	p.mu.Lock()

	fifo, loaded := p.PrewarmContainersPerHost[host.GetID()]
	if !loaded {
		fifo = queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo
	}

	p.mu.Unlock()

	numContainers := fifo.Len()
	if numContainers >= p.Config.InitialPrewarmedContainersPerHost {
		p.log.Debug("Host %s already has %d/%d prewarmed container(s) provisioned. Skipping initial provisioning step.",
			host.GetNodeName(), numContainers, p.Config.InitialPrewarmedContainersPerHost)
		numCreatedChan <- 0
		return
	}

	go func() {
		// defer wg.Done()
		numCreated := p.ProvisionContainers(host, p.Config.InitialPrewarmedContainersPerHost)
		numCreatedChan <- numCreated
		// atomic.AddInt32(&created, numCreated)
	}()
}

// ProvisionContainers is used to launch a job of provisioning n pre-warmed scheduling.KernelContainer instances on
// the specified scheduling.Host. The work of provisioning the n containers is distributed amongst several goroutines,
// the number of which depends upon the size of n.
//
// ProvisionContainers returns the number of scheduling.KernelContainer instances that were successfully pre-warmed.
//
// ProvisionContainers will panic if the given scheduling.Host is nil.
func (p *BaseContainerPrewarmer) ProvisionContainers(host scheduling.Host, n int) int32 {
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
		err := p.ProvisionContainer(host)

		if err != nil {
			p.log.Error("Failed to provision single container on host %s: %v", host.GetNodeName(), err)
			return 0
		}

		p.log.Debug("Provisioned single container on host %s.", host.GetNodeName())
		return 1
	}

	p.log.Debug("Instructed to prewarm %d containers on host %s.", n, host.GetNodeName())

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

	p.log.Debug("Dividing work of provisioning %d container(s) with %d worker(s) as follows: %v",
		n, nWorkers, work)

	p.recordProvisioning(int32(n), host)

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

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *BaseContainerPrewarmer) ProvisionContainer(host scheduling.Host) error {
	p.recordProvisioning(1, host)

	return p.provisionContainer(host)
}

// MaxPrewarmedContainersPerHost returns the maximum number of pre-warmed containers that should be provisioned on any
// given scheduling.Host at any given time. If there are MaxPrewarmedContainersPerHost pre-warmed containers
// available on a given scheduling.Host, then more will not be provisioned.
func (p *BaseContainerPrewarmer) MaxPrewarmedContainersPerHost() int {
	return p.Config.MaxPrewarmedContainersPerHost
}

// InitialPrewarmedContainersPerHost returns the number of pre-warmed containers to create per host after the
// conclusion of the 'initial connection period'.
func (p *BaseContainerPrewarmer) InitialPrewarmedContainersPerHost() int {
	return p.Config.InitialPrewarmedContainersPerHost
}

// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
// ContainerPrewarmer's policy.
func (p *BaseContainerPrewarmer) ValidateHostCapacity(host scheduling.Host) {
	if p.instance != nil {
		p.instance.ValidateHostCapacity(host)
	}
}

// ValidatePoolCapacity ensures that there are enough pre-warmed containers available throughout the entire cluster.
func (p *BaseContainerPrewarmer) ValidatePoolCapacity() {
	if p.instance != nil {
		p.instance.ValidatePoolCapacity()
	}
}

// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
// below this quantity, then a new pre-warmed container will be provisioned.
func (p *BaseContainerPrewarmer) MinPrewarmedContainersPerHost() int {
	if p.instance != nil {
		return p.instance.MinPrewarmedContainersPerHost()
	}

	return 0
}

// TotalNumProvisioning returns the total number of prewarm containers being provisioned across all scheduling.Host
// instances in the scheduling.Cluster.
func (p *BaseContainerPrewarmer) TotalNumProvisioning() int32 {
	return p.totalNumProvisioning.Load()
}

// unsafeHostLen returns the number of pre-warmed containers currently available on the targeted scheduling.Host as
// well as the number of pre-warmed containers that are currently being provisioned on the targeted scheduling.Host.
func (p *BaseContainerPrewarmer) unsafeHostLen(host scheduling.Host) (int, int) {
	containers, loaded := p.PrewarmContainersPerHost[host.GetID()]
	if !loaded {
		containers = queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = containers
	}

	currentNum := containers.Len()

	return currentNum, int(p.unsafeNumContainersProvisioningOnHost(host))
}

func (p *BaseContainerPrewarmer) unsafeNumContainersProvisioningOnHost(host scheduling.Host) int32 {
	numProvisioning, ok := p.NumPrewarmContainersProvisioningPerHost[host.GetID()]
	if !ok {
		tmp := atomic.Int32{}
		numProvisioning = &tmp
		p.NumPrewarmContainersProvisioningPerHost[host.GetID()] = numProvisioning
	}

	return numProvisioning.Load()
}

// decrementProvisioning decrements the counter of the number of pre-warmed containers currently being provisioned
// on the specified scheduling.Host by the specified quantity.
//
// decrementProvisioning expects the specified quantity to be passed in as a positive number.
func (p *BaseContainerPrewarmer) decrementProvisioning(n int32, host scheduling.Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.unsafeDecrementProvisioning(n, host)
}

// unsafeDecrementProvisioning decrements the counter of the number of pre-warmed containers currently being
// provisioned on the specified scheduling.Host by the specified quantity.
//
// unsafeDecrementProvisioning expects the specified quantity to be passed in as a positive number.
func (p *BaseContainerPrewarmer) unsafeDecrementProvisioning(n int32, host scheduling.Host) {
	// Attempt to load the counter.
	numProvisioning, ok := p.NumPrewarmContainersProvisioningPerHost[host.GetID()]
	if !ok {
		// Create new atomic.Int32.
		tmp := atomic.Int32{}
		numProvisioning = &tmp
		p.NumPrewarmContainersProvisioningPerHost[host.GetID()] = numProvisioning
	}

	val := n * -1

	// Decrement the counter.
	numProvisioning.Add(val)
	p.totalNumProvisioning.Add(val)
}

// recordProvisioning atomically records that n prewarmed containers are being provisioned on the specified
// scheduling.Host.
func (p *BaseContainerPrewarmer) recordProvisioning(n int32, host scheduling.Host) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Attempt to load the counter.
	numProvisioning, ok := p.NumPrewarmContainersProvisioningPerHost[host.GetID()]
	if !ok {
		// Create new atomic.Int32.
		tmp := atomic.Int32{}
		numProvisioning = &tmp
		p.NumPrewarmContainersProvisioningPerHost[host.GetID()] = numProvisioning
	}

	// Increment the counter.
	numProvisioning.Add(n)
	p.totalNumProvisioning.Add(n)
}

// onPrewarmedContainerUsed is a callback to execute when a pre-warmed container is used.
func (p *BaseContainerPrewarmer) onPrewarmedContainerUsed(container scheduling.PrewarmedContainer) {
	p.log.Debug(utils.LightPurpleStyle.Render("Pre-warmed container \"%s\" from host \"%s\" (ID=\"%s\") is being used."),
		container.ID(), container.HostName(), container.HostId())

	//p.mu.Lock()
	//defer p.mu.Unlock()
	//
	//container.SetUnavailable()
	//delete(p.AllPrewarmContainers, container.ID())

	return
}

// provisionContainers provisions n pre-warmed scheduling.KernelContainer instances on the specified scheduling.Host.
//
// provisionContainers returns the number of pre-warmed scheduling.KernelContainer instances created.
func (p *BaseContainerPrewarmer) provisionContainers(host scheduling.Host, n int) (int, error) {
	startTime := time.Now()
	for i := 0; i < n; i++ {
		p.log.Debug("Provisioning new pre-warmed container %d/%d on host %s.", i+1, n, host.GetNodeName())

		startCurrHost := time.Now()
		err := p.provisionContainer(host)

		if err != nil {
			return i, err
		}

		p.log.Debug("Successfully provisioned new pre-warmed container %d/%d on host %s in %v.",
			i+1, n, host.GetNodeName(), time.Since(startCurrHost))
	}

	p.log.Debug("Successfully provisioned %d pre-warmed container(s) on host %s in %v.",
		n, host.GetNodeName(), time.Since(startTime))

	return n, nil
}

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *BaseContainerPrewarmer) provisionContainer(host scheduling.Host) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// TODO: Make the executable path configurable or passed via an environment variable.
	argv := []string{"/home/jovyan/Python-3.12.6/debug/python", "-m", "distributed_notebook.kernel", "-f",
		"{connection_file}", "--debug", "--IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"}

	kernelSpec := &proto.KernelSpec{
		Id:              uuid.NewString(),
		Session:         "",
		Argv:            argv,
		SignatureScheme: "hmac-sha256",
		Key:             "",
		ResourceSpec:    proto.NewResourceSpec(0, 0, 0, 0),
		WorkloadId:      "",
	}

	spec := &proto.KernelReplicaSpec{
		Kernel:                    kernelSpec,
		ReplicaId:                 1,
		NumReplicas:               int32(p.Cluster.Scheduler().Policy().NumReplicas()),
		Replicas:                  []string{},
		Join:                      false,
		WorkloadId:                "",
		DockerModeKernelDebugPort: -1,
		PrewarmContainer:          true,
	}

	p.log.Debug("provisionContainer: calling StartKernelReplica on host %s", host.GetNodeName())
	startTime := time.Now()

	resp, err := host.StartKernelReplica(ctx, spec)
	if err != nil {
		p.log.Error("Failed to provision pre-warmed container on host %s after %v because: %v",
			host.GetNodeName(), time.Since(startTime), err)

		p.decrementProvisioning(1, host)

		return err
	}

	p.log.Debug("provisionContainer: successfully called StartKernelReplica on host %s in %v",
		host.GetNodeName(), time.Since(startTime))

	prewarmedContainer := NewPrewarmedContainerBuilder().
		WithHost(host).
		WithKernelConnectionInfo(resp).
		WithKernelReplicaSpec(spec).
		WithPrewarmedContainerUsedCallback(p.onPrewarmedContainerUsed).
		Build()

	return p.onPrewarmContainerProvisioned(prewarmedContainer)
}

func (p *BaseContainerPrewarmer) unsafeRegisterPrewarmedContainer(container scheduling.PrewarmedContainer) error {
	// Verify that the specified pre-warmed container isn't already registered.
	if _, loaded := p.AllPrewarmContainers[container.ID()]; loaded {
		return fmt.Errorf("%w: %w: \"%s\"", ErrPrewarmedContainerRegistrationFailure,
			ErrPrewarmedContainerAlreadyRegistered, container.ID())
	}

	// Ensure the container hasn't already been used.
	if !container.IsAvailable() {
		p.log.Error("Returned prewarmed container %s is marked as having been used.", container.ID())
		return fmt.Errorf("%w: container \"%s\"", ErrPrewarmedContainerAlreadyUsed, container.ID())
	}

	p.AllPrewarmContainers[container.ID()] = container

	fifo, loaded := p.PrewarmContainersPerHost[container.HostId()]
	if !loaded {
		fifo = queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[container.HostId()] = fifo
	}

	fifo.Enqueue(container)

	count := fifo.Len()
	provisioning := p.unsafeNumContainersProvisioningOnHost(container.Host())

	combined := count + int(provisioning)

	if p.Config.MaxPrewarmedContainersPerHost >= 0 && combined > p.Config.MaxPrewarmedContainersPerHost {
		p.log.Debug("Host \"%s\" is now over-capacity with pre-warm containers: %d (limit=%d)",
			container.HostName(), combined)
	} else {
		p.log.Debug("Pre-warm Containers on Host \"%s\": %d available, %d provisioning",
			container.HostName(), count, provisioning)
	}

	return nil
}

// onPrewarmContainerProvisioned is called whenever a PrewarmedContainer is successfully provisioned.
func (p *BaseContainerPrewarmer) onPrewarmContainerProvisioned(container *PrewarmedContainer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.unsafeDecrementProvisioning(1, container.host)

	return p.unsafeRegisterPrewarmedContainer(container)
}

// prewarmContainerUsed is called when a pre-warm container is used, to give the container prewarmer a chance
// to react (i.e., provision another prewarm container, if it is supposed to do so).
func (p *BaseContainerPrewarmer) prewarmContainerUsed(_ scheduling.Host, _ scheduling.PrewarmedContainer) {
	// No-op.
}
