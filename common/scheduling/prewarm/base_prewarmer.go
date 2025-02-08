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
	ErrPrewarmedContainerAlreadyUsed         = errors.New("the prewarmed container has already been used")
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
func (p *PrewarmedContainer) OnPrewarmedContainerUsed(container scheduling.PrewarmedContainer) {
	if p.onPrewarmedContainerUsed != nil {
		p.onPrewarmedContainerUsed(container)
	}
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
}

// BaseContainerPrewarmer is responsible for provisioning pre-warmed containers and maintaining information about
// these pre-warmed containers, such as how many are available on each scheduling.Host.
type BaseContainerPrewarmer struct {
	instance scheduling.ContainerPrewarmer

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

	stopChan chan struct{}

	mu  sync.Mutex
	log logger.Logger
}

// NewContainerPrewarmer creates a new BaseContainerPrewarmer struct and returns a pointer to it.
func NewContainerPrewarmer(cluster scheduling.Cluster, configuration *PrewarmerConfig) *BaseContainerPrewarmer {
	warmer := &BaseContainerPrewarmer{
		AllPrewarmContainers:                    make(map[string]scheduling.PrewarmedContainer),
		PrewarmContainersPerHost:                make(map[string]*queue.ThreadsafeFifo[scheduling.PrewarmedContainer]),
		NumPrewarmContainersProvisioningPerHost: make(map[string]*atomic.Int32),
		stopChan:                                make(chan struct{}, 1),
		Cluster:                                 cluster,
		Config:                                  configuration,
	}

	config.InitLogger(&warmer.log, warmer)

	return warmer
}

// Run creates a separate goroutine in which the BaseContainerPrewarmer maintains the overall capacity/availability of
// pre-warmed containers in accordance with BaseContainerPrewarmer's policy for doing so.
func (p *BaseContainerPrewarmer) Run() {
	if p.instance == nil {
		p.log.Warn("No specific 'Warm Container Pool' maintenance strategy configured.")
		return
	}

	p.instance.Run()
}

// Stop instructs the ContainerPrewarmer to stop.
func (p *BaseContainerPrewarmer) Stop() {
	p.stopChan <- struct{}{}
}

// RequestPrewarmedContainer is used to request a pre-warm container on a particular host.
//
// RequestPrewarmedContainer is explicitly thread safe (i.e., it uses a mutex).
func (p *BaseContainerPrewarmer) RequestPrewarmedContainer(host scheduling.Host) (scheduling.PrewarmedContainer, error) {
	p.log.Debug("Received request[Host %s (ID=%s)].", host.GetNodeName(), host.GetID())

	p.mu.Lock()
	defer p.mu.Unlock()

	containers, loaded := p.PrewarmContainersPerHost[host.GetID()]

	// If there is no queue associated with the specified host, then we'll create the queue,
	// but we'll still return an error, as we'll have no containers available.
	if !loaded {
		p.log.Debug("Request rejected[Host %s (ID=%s), NoneAvailable, UnknownHost].",
			host.GetNodeName(), host.GetID(), containers.Len())

		fifo := queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo

		return nil, fmt.Errorf("%w: host \"%s\" (ID=\"%s\")",
			ErrNoPrewarmedContainersAvailable, host.GetNodeName(), host.GetID())
	}

	// Check if there are simply no pre-warmed containers available.
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
// via the RequestPrewarmedContainer method, but ended up being unused, and so it can simply be put back into the pool.
func (p *BaseContainerPrewarmer) ReturnUnusedPrewarmContainer(container scheduling.PrewarmedContainer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Debug("Container returned unused [%v]", container.String())

	// Ensure the container hasn't already been used.
	if !container.IsAvailable() {
		p.log.Error("Returned prewarmed container %s is marked as having been used.", container.ID())
		return fmt.Errorf("%w: container \"%s\"", ErrPrewarmedContainerAlreadyUsed, container.ID())
	}

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

	var wg sync.WaitGroup
	target = 0
	created = 0

	// Clamp initial number to the maximum.
	if p.Config.InitialPrewarmedContainersPerHost > p.Config.MaxPrewarmedContainersPerHost {
		p.log.Warn("Configured 'initial prewarmed containers per host' (%d) is greater than configured maximum (%d). Clamping.",
			p.Config.InitialPrewarmedContainersPerHost, p.Config.MaxPrewarmedContainersPerHost)
		p.Config.InitialPrewarmedContainersPerHost = p.Config.MaxPrewarmedContainersPerHost
	}

	p.log.Debug("Will create %d prewarmed container(s) on each host (initially).",
		p.Config.InitialPrewarmedContainersPerHost)

	p.Cluster.RangeOverHosts(func(hostId string, host scheduling.Host) bool {
		wg.Add(1)
		atomic.AddInt32(&target, int32(p.Config.InitialPrewarmedContainersPerHost))

		fifo := queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = fifo

		go func() {
			defer wg.Done()
			numCreated := p.ProvisionContainers(host, p.Config.InitialPrewarmedContainersPerHost)
			atomic.AddInt32(&created, numCreated)
		}()

		return true
	})

	wg.Wait()

	return atomic.LoadInt32(&created), atomic.LoadInt32(&target)
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
			return 0
		}

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

// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
// below this quantity, then a new pre-warmed container will be provisioned.
func (p *BaseContainerPrewarmer) MinPrewarmedContainersPerHost() int {
	if p.instance != nil {
		return p.instance.MinPrewarmedContainersPerHost()
	}

	return 0
}

// numContainersOnHost returns the number of pre-warmed containers on the specified scheduling.Host, with a second
// parameter enabling the specification of whether to include containers that are being provisioned (but are not yet
// created) in that count.
//
// The first quantity returned by numContainersOnHost is the number without provisioning.
//
// If `includeProvisioning` is specified as true, then the second quantity will be the number with provisioning.
//
// If `includeProvisioning` is specified as false, then the second quantity will be -1.
func (p *BaseContainerPrewarmer) numContainersOnHost(host scheduling.Host, includeProvisioning bool) (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.unsafeNumContainersOnHost(host, includeProvisioning)
}

// unsafeNumContainersOnHost returns the number of pre-warmed containers on the specified scheduling.Host, with a
// second parameter enabling the specification of whether to include containers that are being provisioned (but are not
// yet created) in that count.
//
// The first quantity returned by unsafeNumContainersOnHost is the number without provisioning.
//
// If `includeProvisioning` is specified as true, then the second quantity will be the number with provisioning.
//
// If `includeProvisioning` is specified as false, then the second quantity will be -1.
func (p *BaseContainerPrewarmer) unsafeNumContainersOnHost(host scheduling.Host, includeProvisioning bool) (int, int) {
	containers, loaded := p.PrewarmContainersPerHost[host.GetID()]
	if !loaded {
		containers = queue.NewThreadsafeFifo[scheduling.PrewarmedContainer](p.Config.InitialPrewarmedContainersPerHost)
		p.PrewarmContainersPerHost[host.GetID()] = containers
	}

	currentNum := containers.Len()

	// If we aren't supposed to include the containers being provisioned too, then just return `currentNum`.
	if !includeProvisioning {
		return currentNum, -1
	}

	numProvisioning, ok := p.NumPrewarmContainersProvisioningPerHost[host.GetID()]
	if !ok {
		tmp := atomic.Int32{}
		numProvisioning = &tmp
		p.NumPrewarmContainersProvisioningPerHost[host.GetID()] = numProvisioning
	}

	return currentNum, int(numProvisioning.Load()) + currentNum
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

	// Decrement the counter.
	numProvisioning.Add(n * -1)
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
}

// onPrewarmedContainerUsed is a callback to execute when a pre-warmed container is used.
func (p *BaseContainerPrewarmer) onPrewarmedContainerUsed(container scheduling.PrewarmedContainer) {
	p.log.Debug("Pre-warmed container \"%s\" from host \"%s\" (ID=\"%s\") is being.",
		container.ID(), container.HostName(), container.HostId())

	p.mu.Lock()
	defer p.mu.Unlock()

	container.SetUnavailable()
	delete(p.AllPrewarmContainers, container.ID())

	return
}

// provisionContainers provisions n pre-warmed scheduling.KernelContainer instances on the specified scheduling.Host.
//
// provisionContainers returns the number of pre-warmed scheduling.KernelContainer instances created.
func (p *BaseContainerPrewarmer) provisionContainers(host scheduling.Host, n int) (int, error) {
	for i := 0; i < n; i++ {
		err := p.provisionContainer(host)

		if err != nil {
			return i, err
		}
	}

	p.log.Debug("Successfully provisioned %d pre-warmed container(s) on host %s.", n, host.GetNodeName())
	return n, nil
}

// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
func (p *BaseContainerPrewarmer) provisionContainer(host scheduling.Host) error {
	p.log.Debug("Provisioning pre-warmed container on host %s.", host.GetNodeName())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
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

	resp, err := host.StartKernelReplica(ctx, spec)
	if err != nil {
		p.log.Error("Failed to provision pre-warmed container on host %s because: %v", host.GetNodeName(), err)

		p.decrementProvisioning(1, host)

		return err
	}

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

	fifo, _ := p.PrewarmContainersPerHost[container.HostId()]
	fifo.Enqueue(container)

	p.log.Debug("Number of pre-warmed containers on host %s: %d", container.HostName(), fifo.Len())
	return nil
}

// registerPrewarmedContainerInfo registers a pre-warmed container that was successfully created on the specified Host.
//
// registerPrewarmedContainerInfo is explicitly thread safe.
func (p *BaseContainerPrewarmer) registerPrewarmedContainerInfo(connInfo *proto.KernelConnectionInfo, spec *proto.KernelReplicaSpec, host scheduling.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Debug("Registering pre-warmed container created on host %s.", host.GetNodeName())

	prewarmedContainer := NewPrewarmedContainerBuilder().
		WithHost(host).
		WithKernelConnectionInfo(connInfo).
		WithKernelReplicaSpec(spec).
		WithPrewarmedContainerUsedCallback(p.onPrewarmedContainerUsed).
		Build()

	return p.unsafeRegisterPrewarmedContainer(prewarmedContainer)
}

// onPrewarmContainerProvisioned is called whenever a PrewarmedContainer is successfully provisioned.
func (p *BaseContainerPrewarmer) onPrewarmContainerProvisioned(container *PrewarmedContainer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.unsafeDecrementProvisioning(1, container.host)

	return p.unsafeRegisterPrewarmedContainer(container)
}
