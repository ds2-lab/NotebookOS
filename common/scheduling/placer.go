package scheduling

import (
	"errors"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

var (
	ErrExceedsMaxGPU = errors.New("exceeds max gpus settings per host")
	ErrNotSupported  = errors.New("not supported")
)

// internalPlacer is the internal API implemented by all Placer instances.
type internalPlacer interface {
	Placer

	// index returns the ClusterIndex of the specific Placer implementation.
	getIndex() ClusterIndex

	// findHost returns a host that can satisfy the resourceSpec.
	// This is the Placer-implementation-specific logic of the Placer.FindHost method.
	findHost(blacklist []interface{}, metrics types.Spec) AbstractHost

	// FindHosts returns a slice of Host instances that can satisfy the resourceSpec.
	// This is the Placer-implementation-specific logic of the Placer.FindHosts method.
	findHosts(spec types.Spec) []AbstractHost

	// hostIsViable returns a tuple (bool, bool).
	// First bool represents whether the host is viable.
	// Second bool indicates whether the host was successfully locked. This does not mean that it is still locked.
	// Merely that we were able to lock it when we tried. If we locked it and found that the host wasn't viable,
	// then we'll have unlocked it before hostIsViable returns.
	hostIsViable(candidateHost AbstractHost, spec types.Spec) (bool, bool)
}

// Placer defines the interface for a placer that is responsible for:
// 1. Finding hosts that can satisfy the resourceSpec.
//   - A host satisfies the resourceSpec as long as the over-subscription rate is below a threshold given the assumption that an interactive session:
//     i. requires minimum resources to restore the runtime state.
//     ii. has multiple replicas as candidates to meet the resource requirement.
//
// 2. Placing a replica on a host.
//
// Note that the placer is not responsible for finding a host that ensure the resource resourceSpec on placement.
// It's the responsibility of the scheduler to meet the (potential) resource requirement on task executing.
type Placer interface {
	// FindHosts returns a list of hosts that can satisfy the resourceSpec.
	// The number of hosts returned is determined by the placer.
	FindHosts(types.Spec) []AbstractHost

	// FindHost returns a host that can satisfy the resourceSpec.
	// This method is provided for development. Implementation are not required to implement this method.
	FindHost(blacklist []interface{}, metrics types.Spec) AbstractHost

	// Place atomically places a replica on a host.
	// The subscription rate of the host will be checked before placing the replica. If the rate is above the threshold, a new host will be launched to place the replica.
	// The reasons to launch a new host are:
	// 1. If the host is selected by the placer, the subscription rate is updated before placement to ensure the rate is below the threshold.
	// 2. We assume the host selected by the scheduler is best fit. If such a choice would fail the subscription rate check, a reselection could not help.
	Place(host AbstractHost, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error)

	// Reclaim atomically reclaims a replica from a host.
	// If noop is specified, it is the caller's responsibility to stop the replica.
	Reclaim(host AbstractHost, sess *Session, noop bool) error
}

type PlacerStats interface {
	IdleGPUs() types.StatFloat64Field
	IdleHosts() types.StatInt32Field
}
