package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
)

// Placer defines the interface for a placer that is responsible for:
// 1. Finding hosts that can satisfy the resourceSpec.
//   - A host satisfies the resourceSpec as long as the over-subscription rate is below a threshold given the assumption that an interactive session:
//     i. requires minimum Resources to restore the runtime state.
//     ii. has multiple replicas as candidates to meet the resource requirement.
//
// 2. Placing a replica on a host.
//
// Note that the placer is not responsible for finding a host that ensure the resource resourceSpec on placement.
// It's the responsibility of the scheduler to meet the (potential) resource requirement on task executing.
type Placer interface {
	// FindHosts returns a list of hosts that can satisfy the resourceSpec.
	// The number of hosts returned is determined by the placer.
	FindHosts(blacklist []interface{}, kernelSpec *proto.KernelSpec, numHosts int, forTraining bool) []Host

	// FindHost returns a host that can satisfy the resourceSpec.
	// This method is provided for development. Implementation are not required to implement this method.
	FindHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) Host

	// Place atomically places a replica on a host.
	// The subscription rate of the host will be checked before placing the replica. If the rate is above the threshold, a new host will be launched to place the replica.
	// The reasons to launch a new host are:
	// 1. If the host is selected by the placer, the subscription rate is updated before placement to ensure the rate is below the threshold.
	// 2. We assume the host selected by the scheduler is best fit. If such a choice would fail the subscription rate check, a reselection could not help.
	Place(host Host, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error)

	// Reclaim atomically reclaims a replica from a host.
	// If noop is specified, it is the caller's responsibility to stop the replica.
	Reclaim(host Host, sess UserSession, noop bool) error

	// NumHostsInIndex returns the length of the Placer's index.
	NumHostsInIndex() int

	// UpdateIndex updates a Host in the index.
	//
	// Important: UpdateIndex is NOT thread-safe.
	UpdateIndex(host Host)

	// UpdateIndexMultiple updates multiple Host instances within the index.
	//
	// Important: UpdateIndexMultiple is NOT thread-safe.
	UpdateIndexMultiple(hosts []Host)

	// GetIndex returns the placer's index.ClusterIndex.
	GetIndex() ClusterIndex
}

type PlacerStats interface {
	IdleGPUs() types.StatFloat64Field
	IdleHosts() types.StatInt32Field
}
