package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"time"
)

const (
	DefaultPreWarmerInterval = time.Second * 5

	// MaintainMinCapacity attempts to maintain the minimum number of prewarmed containers on each Host in the Cluster.
	MaintainMinCapacity PrewarmingPolicy = "maintain_minimum_capacity"

	// LittleLawCapacity creates prewarmed containers in accordance with [Little's Law].
	//
	// [Little's Law]: https://en.wikipedia.org/wiki/Little%27s_law
	LittleLawCapacity PrewarmingPolicy = "little_law_capacity"

	// FixedCapacity creates a pool with a fixed size and optionally allows for proactive replacement of used
	// prewarm containers (which may cause the pool's size to grow unbounded if container re-use is also enabled).
	FixedCapacity PrewarmingPolicy = "fixed_capacity"

	// NoMaintenance specifies that there should be no maintenance of the warm container pool, and that the only
	// warm containers should be those that are created initially.
	//
	// NoMaintenance is primarily used for testing or debugging and is not necessarily intended for use in production.
	NoMaintenance PrewarmingPolicy = "no_maintenance"
)

type PrewarmingPolicy string

func (p PrewarmingPolicy) String() string {
	return string(p)
}

// PrewarmedContainerUsedCallback is a callback function to be called by the scheduling.Scheduler if it commits
// to using a prewarmed container.
type PrewarmedContainerUsedCallback func(container PrewarmedContainer)

// ContainerPool maintains a collection of (pre)warmed containers.
type ContainerPool interface {
	// Len returns the total number of prewarmed containers available within the target ContainerPool.
	//
	// Len is ultimately just an alias for PoolSize.
	Len() int

	// GetNumPrewarmContainersOnHost returns the number of pre-warmed containers currently available on the targeted
	// scheduling.Host as well as the number of pre-warmed containers that are currently being provisioned on the
	// targeted scheduling.Host.
	GetNumPrewarmContainersOnHost(host Host) (curr int, provisioning int)

	// InitialPrewarmedContainersPerHost returns the number of pre-warmed containers to create per host after the
	// conclusion of the 'initial connection period'.
	InitialPrewarmedContainersPerHost() int

	// MinPrewarmedContainersPerHost returns the minimum number of pre-warmed containers that should be available on any
	// given scheduling.Host. If the number of pre-warmed containers available on a particular scheduling.Host falls
	// below this quantity, then a new pre-warmed container will be provisioned.
	MinPrewarmedContainersPerHost() int

	// MaxPrewarmedContainersPerHost returns the maximum number of pre-warmed containers that should be provisioned on any
	// given scheduling.Host at any given time. If there are MaxPrewarmedContainersPerHost pre-warmed containers
	// available on a given scheduling.Host, then more will not be provisioned.
	MaxPrewarmedContainersPerHost() int
}

// ContainerPrewarmer is responsible for provisioning pre-warmed containers and maintaining information about
// these pre-warmed containers, such as how many are available on each scheduling.Host.
type ContainerPrewarmer interface {
	ContainerPool

	// Run maintains the overall capacity/availability of pre-warmed containers in accordance with BaseContainerPrewarmer's
	// policy for doing so.
	//
	// Run should be executed within its own goroutine.
	//
	// If another thread is executing the Run method, then Run will return an error. Only one goroutine may execute
	// the Run method at a time.
	Run() error

	// TotalNumProvisioning returns the total number of prewarm containers being provisioned across all scheduling.Host
	// instances in the scheduling.Cluster.
	TotalNumProvisioning() int32

	// ProvisionContainers is used to launch a job of provisioning n pre-warmed scheduling.KernelContainer instances on
	// the specified scheduling.Host. The work of provisioning the n containers is distributed amongst several goroutines,
	// the number of which depends upon the size of n.
	//
	// ProvisionContainers returns the number of scheduling.KernelContainer instances that were successfully pre-warmed.
	//
	// ProvisionContainers will panic if the given scheduling.Host is nil.
	ProvisionContainers(host Host, n int) int32

	// RequestProvisionContainers attempts to provision n new pre-warm containers on scheduling.Host instances that
	// are validated by the provided HostCriteriaFunction. If the HostCriteriaFunction is nil, then the scheduling.Host
	// instances are selected at the discretion of the implementing ContainerPrewarmer.
	//
	// RequestProvisionContainers returns a map from host ID to the number of prewarm containers provisioned on that
	// scheduling.Host, as well as an error, if one occurred.
	RequestProvisionContainers(n int, criteria HostCriteriaFunction, separateHostsOnly bool) (map[string]int, error)

	// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
	ProvisionContainer(host Host) error

	// ReturnPrewarmContainer returns a used pre-warmed container so that it may be reused in the future.
	ReturnPrewarmContainer(container PrewarmedContainer) error

	// RequestPrewarmedContainer is used to request a pre-warm container on a particular host.
	//
	// RequestPrewarmedContainer is explicitly thread safe (i.e., it uses a mutex).
	RequestPrewarmedContainer(host Host) (PrewarmedContainer, error)

	// ProvisionInitialPrewarmContainers provisions the configured number of initial pre-warmed containers on each host.
	//
	// ProvisionInitialPrewarmContainers returns the number of pre-warmed containers that were created as well as the
	// maximum number that were supposed to be created (if no errors were to occur).
	ProvisionInitialPrewarmContainers() (created int32, target int32)

	// ProvisionInitialPrewarmContainersOnHost provisions the configured number of 'initial' pre-warm containers on the
	// specified scheduling.Host.
	ProvisionInitialPrewarmContainersOnHost(host Host, numCreatedChan chan<- int32)

	// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
	// ContainerPrewarmer's policy.
	ValidateHostCapacity(host Host)

	// ValidatePoolCapacity ensures that there are enough pre-warmed containers available throughout the entire cluster.
	ValidatePoolCapacity()

	// Stop instructs the ContainerPrewarmer to stop.
	Stop() error

	// IsRunning returns true if the target ContainerPrewarmer is actively running.
	IsRunning() bool
}

// PrewarmedContainer encapsulates information about a pre-warmed container that exists on a particular Host.
type PrewarmedContainer interface {
	String() string
	Age() time.Duration
	ID() string
	HostId() string
	HostName() string
	OnPrewarmedContainerUsed()
	GetOnPrewarmedContainerUsed() PrewarmedContainerUsedCallback
	IsAvailable() bool
	SetUnavailable()
	Host() Host
	KernelConnectionInfo() *proto.KernelConnectionInfo
	KernelReplicaSpec() *proto.KernelReplicaSpec
	CreatedAt() time.Time
}
