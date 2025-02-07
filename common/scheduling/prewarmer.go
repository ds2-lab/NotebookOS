package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"time"
)

const (
	PreWarmerInterval                    = time.Second * 5
	MaintainMinCapacity PrewarmingPolicy = "maintain_minimum_capacity"
	LittleLawCapacity   PrewarmingPolicy = "little_law_capacity"
)

type PrewarmingPolicy string

func (p PrewarmingPolicy) String() string {
	return string(p)
}

// PrewarmedContainerUsedCallback is a callback function to be called by the scheduling.Scheduler if it commits
// to using a prewarmed container.
type PrewarmedContainerUsedCallback func(container PrewarmedContainer)

// ContainerPrewarmer is responsible for provisioning pre-warmed containers and maintaining information about
// these pre-warmed containers, such as how many are available on each scheduling.Host.
type ContainerPrewarmer interface {
	// Run creates a separate goroutine in which the ContainerPrewarmer maintains the overall capacity/availability of
	// pre-warmed containers in accordance with ContainerPrewarmer's policy for doing so.
	Run()

	// ProvisionContainers is used to launch a job of provisioning n pre-warmed scheduling.KernelContainer instances on
	// the specified scheduling.Host. The work of provisioning the n containers is distributed amongst several goroutines,
	// the number of which depends upon the size of n.
	//
	// ProvisionContainers returns the number of scheduling.KernelContainer instances that were successfully pre-warmed.
	//
	// ProvisionContainers will panic if the given scheduling.Host is nil.
	ProvisionContainers(host Host, n int) int32

	// ProvisionContainer is used to provision 1 pre-warmed scheduling.KernelContainer on the specified scheduling.Host.
	ProvisionContainer(host Host) error

	// ReturnUnusedPrewarmContainer is used to return a pre-warmed container that was originally returned to the caller
	// via the RequestPrewarmedContainer method, but ended up being unused, and so it can simply be put back into the pool.
	ReturnUnusedPrewarmContainer(container PrewarmedContainer) error

	// RequestPrewarmedContainer is used to request a pre-warm container on a particular host.
	//
	// RequestPrewarmedContainer is explicitly thread safe (i.e., it uses a mutex).
	RequestPrewarmedContainer(host Host) (PrewarmedContainer, error)

	// ProvisionInitialPrewarmContainers provisions the configured number of initial pre-warmed containers on each host.
	//
	// ProvisionInitialPrewarmContainers returns the number of pre-warmed containers that were created as well as the
	// maximum number that were supposed to be created (if no errors were to occur).
	ProvisionInitialPrewarmContainers() (created int32, target int32)

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

	// ValidateHostCapacity ensures that the number of prewarmed containers on the specified host does not violate the
	// ContainerPrewarmer's policy.
	ValidateHostCapacity(host Host)

	// Stop instructs the ContainerPrewarmer to stop.
	Stop()
}

// PrewarmedContainer encapsulates information about a pre-warmed container that exists on a particular Host.
type PrewarmedContainer interface {
	String() string
	Age() time.Duration
	ID() string
	HostId() string
	HostName() string
	OnPrewarmedContainerUsed(container PrewarmedContainer)
	IsAvailable() bool
	SetUnavailable()
	Host() Host
	KernelConnectionInfo() *proto.KernelConnectionInfo
	KernelReplicaSpec() *proto.KernelReplicaSpec
	CreatedAt() time.Time
}
