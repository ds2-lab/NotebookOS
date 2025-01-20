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
	findHost(blacklist []interface{}, spec *proto.KernelSpec, forTraining bool, metrics ...[]float64) scheduling.Host

	// findHosts iterates over the Host instances in the index, attempting to reserve the requested resources
	// on each Host until either the requested number of Host instances has been found, or until all Host
	// instances have been checked.
	//
	// If findHosts cannot find the requested number of viable Host instances, then it returns as many as it could
	// find. These Host instances will have the resources reserved on them.
	//
	// This is the Placer-implementation-specific logic of the Placer.FindHosts method.
	findHosts(blacklist []interface{}, spec *proto.KernelSpec, numHosts int, forTraining bool, metrics ...[]float64) []scheduling.Host
}

// resourceReserver is used by placers to reserve resources on candidate hosts.
type resourceReserver func(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool

// getResourceReserver returns a resourceReserver based on the closure created by passing a value for
// reservationShouldUsePendingResources, which may differ depending on the placer implementation and
// configured scheduling policy.
func getResourceReserver(reservationShouldUsePendingResources bool) resourceReserver {
	return func(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) bool {
		var usePendingReservation bool

		if forTraining {
			// If we are migrating a replica that needs to begin training right away, then we should not use a pending reservation.
			// The container will need resources committed to it immediately.
			usePendingReservation = false
		} else {
			usePendingReservation = reservationShouldUsePendingResources
		}

		reserved, err := candidateHost.ReserveResources(kernelSpec, usePendingReservation)
		if err != nil {
			// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
			if reserved {
				panic("We successfully reserved resources on a Host despite ReserveResources also returning an error...")
			}
		}

		return reserved
	}
}
