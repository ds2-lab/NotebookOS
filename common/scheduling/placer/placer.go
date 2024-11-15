package placer

import (
	"errors"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/entity"
	"github.com/scusemua/distributed-notebook/common/scheduling/index"
	"github.com/scusemua/distributed-notebook/common/types"
)

var (
	ErrExceedsMaxGPU = errors.New("exceeds max gpus settings per host")
	ErrNotSupported  = errors.New("not supported")
)

// internalPlacer is the internal API implemented by all Placer instances.
type internalPlacer interface {
	scheduling.Placer

	// index returns the ClusterIndex of the specific Placer implementation.
	getIndex() index.ClusterIndex

	// findHost returns a host that can satisfy the resourceSpec.
	// This is the Placer-implementation-specific logic of the Placer.FindHost method.
	findHost(blacklist []interface{}, metrics types.Spec) *entity.Host

	// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
	// on each Host until either the requested number of Host instances has been found, or until all Host
	// instances have been checked.
	//
	// If findHosts cannot find the requested number of viable Host instances, then it returns as many as it could
	// find. These Host instances will have the resources reserved on them.
	//
	// This is the Placer-implementation-specific logic of the Placer.FindHosts method.
	findHosts(kernelSpec *proto.KernelSpec, numHosts int) []*entity.Host

	// hostIsViable returns a tuple (bool, bool).
	// First bool represents whether the host is viable.
	// Second bool indicates whether the host was successfully locked. This does not mean that it is still locked.
	// Merely that we were able to lock it when we tried. If we locked it and found that the host wasn't viable,
	// then we'll have unlocked it before hostIsViable returns.
	hostIsViable(candidateHost *entity.Host, spec types.Spec) (bool, bool)
}
