package policy

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"math"
	"time"
)

// checkSingleReplica provides a common implementation of FindReadyReplica for scheduling.Policy instances
// that use just a single kernel replica.
func checkSingleReplica(kernel scheduling.Kernel, migrationAllowed bool, executionId string) (scheduling.KernelReplica, error) {
	// Sanity check: make sure there's only one replica.
	if len(kernel.Replicas()) > 1 {
		panic(fmt.Sprintf("checkSingleReplica called for kernel with more than one replica: %d replicas, kernel %s",
			len(kernel.Replicas()), kernel.ID()))
	}

	// Get a reference to that single replica.
	replica := kernel.Replicas()[0]

	// Attempt to pre-allocate resources to the kernel.
	allocationError := replica.Host().PreCommitResources(replica.Container(), executionId)
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

// multiReplicaValidateCapacity validates the Cluster's capacity for static and dynamic policies.
func multiReplicaValidateCapacity(policy scheduling.Policy, cluster scheduling.Cluster, log logger.Logger) {
	if !policy.SupportsPredictiveAutoscaling() {
		return
	}

	var load int32
	cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		load += int32(host.CommittedGPUs())
		return true
	})

	scalingFactor := policy.ScalingConfiguration().ScalingFactor
	gpusPerHost := policy.ScalingConfiguration().GpusPerHost
	scalingLimit := policy.ScalingConfiguration().ScalingLimit
	scalingBufferSize := policy.ScalingConfiguration().ScalingBufferSize
	maximumHostsToReleaseAtOnce := policy.ScalingConfiguration().MaximumHostsToReleaseAtOnce

	// minNumHosts := int32(math.Ceil(float64(load) / s.gpusPerHost))                      // The minimum number of hosts required to satisfy the Cluster's current committed GPUs.
	minNumHosts := int32(policy.NumReplicas())
	scaledOutNumHosts := int32(math.Ceil(float64(load) * scalingFactor / float64(gpusPerHost))) // The number of hosts we would scale-out to based on the configured scaling factor.
	limit := int32(math.Ceil(float64(load) * scalingLimit / float64(gpusPerHost)))              // The maximum number of hosts we're permitted to scale-out to.

	log.Debug("Validating Cluster Capacity. MinNumHosts: %d, ScaledOutNumHosts: %d, Limit: %d",
		minNumHosts, scaledOutNumHosts, limit)

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool more so from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + scalingBufferSize) {
		scaledOutNumHosts = minNumHosts + scalingBufferSize
		log.Debug("Adjusted scaledOutNumHosts: %d.", scaledOutNumHosts)
	}
	if limit < minNumHosts+4 {
		limit = minNumHosts + 4
		log.Debug("Adjusted limit: %d.", limit)
	}

	if log.GetLevel() == logger.LOG_LEVEL_ALL {
		log.Debug("Load (CommittedGPUs): %d. Current #Hosts: %d. Minimum #Hosts to Satisfy Load: %d. Target #Hosts: %d. Max Scaled-Out #Hosts: %d.",
			load, cluster.Len(), minNumHosts, scaledOutNumHosts, limit)
	}
	oldNumHosts := int32(cluster.Len())
	// Only scale-out if that feature is enabled.
	if cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts {
		// Scaling out
		numProvisioned := 0
		targetNumProvisioned := scaledOutNumHosts - oldNumHosts

		if log.GetLevel() == logger.LOG_LEVEL_ALL {
			log.Debug("Scaling out by %d hosts (from %d to %d).", targetNumProvisioned, oldNumHosts, scaledOutNumHosts)
		}

		// This is such a minor optimization, but we cache the size of the active host pool locally so that we don't have to grab it everytime.
		// The size of the pending host pool will grow each time we provision a new host.
		numFailures := 0
		for int32(cluster.Len()) < scaledOutNumHosts {
			err := cluster.Scheduler().RequestNewHost()
			if err != nil {
				log.Error("Failed to add new host because: %v", err)
				numFailures += 1

				if numFailures > 3 {
					log.Error("We've failed three times to provision a new host. Aborting automated operation.")
					return
				} else if errors.Is(err, scheduling.ErrUnsupportedOperation) {
					log.Warn("Aborting scale-out operation as we lack sufficient disabled hosts to scale-out, and adding additional hosts directly is not supported by the current cluster type.")
					return
				} else {
					continue
				}
			}

			numProvisioned++
		}

		// If we provisioned any hosts -- or if we were supposed to provision at least one host -- then we'll
		// print a message about how many we provisioned, and how many failures we encountered.
		if (numProvisioned > 0 || targetNumProvisioned > 0) && log.GetLevel() == logger.LOG_LEVEL_ALL {
			log.Debug("Provisioned %d new hosts based on #CommittedGPUs(%d). Previous #hosts: %d. Current #hosts: %d. #FailedProvisions: %d.", numProvisioned, load, oldNumHosts, cluster.Len(), numFailures)
		}
	} else if !cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts { // If this was the reason the first if-statement evaluated to false, then we'll log a warning message.
		log.Warn("Would like to scale out by %d hosts (from %d to %d); however, cluster cannot possibly scale-out right now.", scaledOutNumHosts-oldNumHosts, oldNumHosts, scaledOutNumHosts)
	}

	// Should we scale in?
	if !cluster.Scheduler().CanScaleIn() || load <= limit {
		return
	}

	// Scaling in.
	// NOTE: Jingyuan's algorithm uses initial capacity here, rather than minimum capacity.
	if limit < cluster.Scheduler().MinimumCapacity() {
		limit = cluster.Scheduler().MinimumCapacity()
	}

	numToRelease := int32(cluster.Len()) - limit
	if numToRelease > 0 {
		if numToRelease > maximumHostsToReleaseAtOnce {
			if log.GetLevel() == logger.LOG_LEVEL_ALL {
				log.Debug("Decreased the number of idle hosts to release from %d to the maximum allowed value of %s.", numToRelease, maximumHostsToReleaseAtOnce)
			}
			numToRelease = maximumHostsToReleaseAtOnce
		}

		if log.GetLevel() == logger.LOG_LEVEL_ALL {
			log.Debug("Scaling in %d hosts", numToRelease)
		}

		numReleased, err := cluster.Scheduler().ReleaseIdleHosts(numToRelease)
		if err != nil {
			log.Error("Error while releasing idle hosts: %v", err)
		}

		if numReleased > 0 && log.GetLevel() == logger.LOG_LEVEL_ALL {
			log.Debug("Released %d idle hosts based on #CommittedGPUs (%d). Prev #hosts: %s. New #hosts: %s.", numReleased, load, oldNumHosts, cluster.Len())
		}
	}

	cluster.Scheduler().SetLastCapacityValidation(time.Now())
}
