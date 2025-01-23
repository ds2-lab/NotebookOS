package policy

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

// checkSingleReplica provides a common implementation of FindReadyReplica for scheduling.Policy instances
// that use just a single kernel replica.
func checkSingleReplica(kernel scheduling.Kernel, migrationAllowed bool) (scheduling.KernelReplica, error) {
	// Sanity check: make sure there's only one replica.
	if len(kernel.Replicas()) > 1 {
		panic(fmt.Sprintf("checkSingleReplica called for kernel with more than one replica: %d replicas, kernel %s",
			len(kernel.Replicas()), kernel.ID()))
	}

	// Get a reference to that single replica.
	replica := kernel.Replicas()[0]

	// Attempt to pre-allocate resources to the kernel.
	allocationError := replica.Host().PreCommitResources(replica.Container())
	if allocationError != nil {
		// If migration is allowed by the scheduling policy that invoked this method,
		// then we will NOT return an error.
		//
		// This will enable the single replica to be migrated.
		if migrationAllowed {
			return nil, nil
		}

		// Migration is not supported by the scheduling policy that invoked us.
		// Therefore, we'll return the error, which will cause an error message
		// to be sent back to the client.
		return nil, errors.Join(scheduling.ErrInsufficientHostsAvailable, allocationError)
	}

	// We were successful in pre-allocating resources to the kernel replica.
	return replica, nil
}
