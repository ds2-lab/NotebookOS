package placer

import (
	"errors"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

var (
	ErrExceedsMaxGPU = errors.New("exceeds max gpus settings per host")
	ErrNotSupported  = errors.New("not supported")
)

// internalPlacer is the internal API implemented by all Placer instances.
type internalPlacer interface {
	scheduling.Placer

	// findHost returns a host that can satisfy the resourceSpec.
	// This is the Placer-implementation-specific logic of the Placer.FindHost method.
	findHost(blacklist []interface{}, replicaSpec *proto.KernelReplicaSpec, forTraining bool,
		ignoreOversubscriptionRisk bool, metrics ...[]float64) (scheduling.Host, error)

	// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
	// on each Host until either the requested number of Host instances has been found, or until all Host
	// instances have been checked.
	//
	// If findHosts cannot find the requested number of viable Host instances, then it returns as many as it could
	// find. These Host instances will have the resources reserved on them.
	//
	// This is the Placer-implementation-specific logic of the Placer.FindHosts method.
	findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool, metrics ...[]float64) ([]scheduling.Host, error)
}

// kernelResourceReserver is used by placers to reserve resources on candidate hosts for arbitrary/unspecified
// replicas of a particular kernel.
//
// kernelResourceReserver returns true (and nil) if resources were reserved.
//
// If resources could not be reserved, then false is returned, along with an error explaining why
// the resources could not be reserved.
//
// The 'forTraining' argument indicates whether the reservation is for a "ready-to-train" replica, in which case it
// will be created as a scheduling.CommittedAllocation, or if it for a "regular" (i.e., not "ready-to-train") replica,
// in which case it will be created as either a scheduling.CommittedAllocation or scheduling.PendingAllocation
// depending upon the scheduling.Policy configured for the AllocationManager.
type kernelResourceReserver func(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) (bool, error)

// replicaResourceReserver is used by placers to reserve resources on candidate hosts for specified replicas of a
// particular kernel.
//
// replicaResourceReserver returns true (and nil) if resources were reserved.
//
// If resources could not be reserved, then false is returned, along with an error explaining why
// the resources could not be reserved.
//
// The 'forTraining' argument indicates whether the reservation is for a "ready-to-train" replica, in which case it
// will be created as a scheduling.CommittedAllocation, or if it for a "regular" (i.e., not "ready-to-train") replica,
// in which case it will be created as either a scheduling.CommittedAllocation or scheduling.PendingAllocation
// depending upon the scheduling.Policy configured for the AllocationManager.
type replicaResourceReserver func(candidateHost scheduling.Host, kernelReplicaSpec *proto.KernelReplicaSpec, forTraining bool) (bool, error)
