package policy

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"golang.org/x/net/context"
	"math"
	"time"
)

type schedulingPolicy interface {
	scheduling.Policy

	getLogger() logger.Logger
}

// defaultFindReadyReplicaSingleReplicaPolicy provides a common implementation of FindReadyReplica for scheduling.Policy
// instances that use just a single kernel replica.
func defaultFindReadyReplicaSingleReplicaPolicy(policy schedulingPolicy, kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {

	// Sanity check: make sure there's only one replica.
	if len(kernel.Replicas()) > 1 {
		panic(fmt.Sprintf("defaultFindReadyReplicaSingleReplicaPolicy called for kernel with more than one replica: %d replicas, kernel %s",
			len(kernel.Replicas()), kernel.ID()))
	}

	// Get a reference to that single replica.
	replica := kernel.Replicas()[0]

	// If the scheduling policy is such that we bind resources when the container is scheduled, then it should
	// already have resources bound to it.
	if policy.ResourceBindingMode() == scheduling.BindResourcesWhenContainerScheduled {
		if replica.Host().HasResourcesCommittedToKernel(kernel.ID()) {
			return replica, nil
		}

		log := policy.getLogger()

		log.Error("Scheduling policy '%s' is supposed to bind resources at scheduling-time.", policy.Name())
		log.Error("However, replica %d of kernel %s does not have resources committed to it on host %s (ID=%s).",
			replica.ReplicaID(), replica.ID(), replica.Host().GetNodeName(), replica.Host().GetID())

		panic("Expected kernel replica to already have resources committed to it.")
	}

	// Attempt to pre-allocate resources to the kernel.
	_, allocationError := replica.Host().PreCommitResources(replica.Container(), executionId, nil)
	if allocationError != nil {
		// If migration is allowed by the scheduling policy that invoked this method,
		// then we will NOT return an error.
		//
		// This will enable the single replica to be migrated.
		if policy.SupportsMigration() {
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

// defaultTryScaleIn is a basic auto-scaling mechanism for scaling in.
func defaultTryScaleIn(policy scheduling.Policy, cluster scheduling.Cluster, log logger.Logger, limit int32, load int32) {
	// Should we scale in?
	if !cluster.Scheduler().CanScaleIn() {
		return
	}

	oldClusterSize := int32(cluster.Len())

	maximumHostsToReleaseAtOnce := policy.ScalingConfiguration().MaximumHostsToReleaseAtOnce

	//if load <= limit {
	//	log.Debug("Load (%d) is <= limit (%d); cannot scale-in.", load, limit)
	//	return
	//}

	// Scaling in.
	// NOTE: Jingyuan's algorithm uses initial capacity here, rather than minimum capacity.
	if limit < cluster.Scheduler().MinimumCapacity() {
		// [02/03/2025] Added '+ policy.ScalingConfiguration().ScalingBufferSize' because otherwise,
		// we'll just end up thrashing back and forth. That is, if we scale to just MinimumCapacity, then
		// we'll scale-out to MinimumCapacity + ScalingBufferSize.
		limit = cluster.Scheduler().MinimumCapacity() + policy.ScalingConfiguration().ScalingBufferSize
	}

	numToRelease := int32(cluster.Len()) - limit

	// If we're not supposed to release any hosts, then just return.
	if numToRelease <= 0 {
		return
	}

	// Clamp the value.
	if numToRelease > maximumHostsToReleaseAtOnce {
		numToRelease = maximumHostsToReleaseAtOnce
	}

	log.Debug("Preparing to scale-in %d (idle) hosts. Current cluster size: %d.", numToRelease, oldClusterSize)
	numReleased, err := cluster.Scheduler().ReleaseIdleHosts(numToRelease)
	if err != nil {
		if errors.Is(err, scheduling.ErrScalingActive) || errors.Is(err, scheduling.ErrAssociatedKernelActiveTraining) {
			// If it's just because there's already a scaling operation, then that's not really a problem.
			log.Debug("Could not release %d idle host(s) because: %v", err)
		} else {
			log.Error("Error while releasing idle hosts: %v", err)
		}
	}

	if numReleased > 0 {
		log.Debug(utils.LightBlueStyle.Render("Released %d idle hosts based on #CommittedGPUs (%d). Cluster size: %d → %d."),
			numReleased, load, oldClusterSize, cluster.Len())
	}
}

// defaultComputeLimitAndLoad computes the current cluster GPU load and current scale-out limit.
func defaultComputeLimitAndLoad(policy scheduling.Policy, cluster scheduling.Cluster) (int32, int32) {
	var load int32
	cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		load += int32(host.CommittedGPUs())
		return true
	})

	gpusPerHost := policy.ScalingConfiguration().GpusPerHost
	scalingLimit := policy.ScalingConfiguration().ScalingLimit

	limit := int32(math.Ceil(float64(load) * scalingLimit / float64(gpusPerHost))) // The maximum number of hosts we're permitted to scale-out to.

	return limit, load
}

// multiReplicaTryScaleOut tries to scale-out and returns the limit and load values.
func multiReplicaTryScaleOut(policy scheduling.Policy, cluster scheduling.Cluster, log logger.Logger) (int32, int32) {
	scalingFactor := policy.ScalingConfiguration().ScalingFactor
	gpusPerHost := policy.ScalingConfiguration().GpusPerHost
	scalingBufferSize := policy.ScalingConfiguration().ScalingBufferSize
	numReplicas := int32(policy.NumReplicas())

	// The minimum number of hosts required to satisfy the Cluster's current committed GPUs.
	minNumHosts := policy.ScalingConfiguration().MinimumCapacity

	limit, load := defaultComputeLimitAndLoad(policy, cluster)

	// The number of hosts we would scale-out to based on the configured scaling factor.
	scaledOutNumHosts := int32(math.Ceil(float64(load) * scalingFactor / float64(gpusPerHost)))

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool more so from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + scalingBufferSize) {
		scaledOutNumHosts = minNumHosts + scalingBufferSize
	}

	// [02/03/2025] I added the clause 'minNumHosts < numReplicas'.
	// The idea is that, if the minimum number of hosts is > the number of replicas,
	// then we do not need to artificially increase the limit.
	if minNumHosts < numReplicas && limit < minNumHosts+numReplicas { // Used to be minNumHosts + 4
		limit = minNumHosts + numReplicas // Used to be minNumHosts + 4
	}

	log.Debug("Load (CommittedGPUs): %d. Current #Hosts: %d. Minimum #Hosts to Satisfy Load: %d. Target #Hosts: %d. Max Scaled-Out #Hosts: %d.",
		load, cluster.Len(), minNumHosts, scaledOutNumHosts, limit)

	oldNumHosts := int32(cluster.Len())
	// Only scale-out if that feature is enabled.
	if cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts {
		// Scaling out
		numProvisioned := int32(0)
		targetNumProvisioned := scaledOutNumHosts - oldNumHosts

		log.Debug("Scaling out by %d hosts (from %d to %d).", targetNumProvisioned, oldNumHosts, scaledOutNumHosts)

		// This is such a minor optimization, but we cache the size of the active host pool locally so that we don't have to grab it everytime.
		// The size of the pending host pool will grow each time we provision a new host.
		numFailures := 0
		for int32(cluster.Len()) < scaledOutNumHosts {
			p := cluster.RequestHosts(context.Background(), targetNumProvisioned)
			if err := p.Error(); err != nil {
				log.Warn("Failed to add new host because: %v", err)
				numFailures += 1

				if numFailures > 3 {
					log.Warn("We've failed three times to provision a new host. Aborting automated operation.")
					return limit, load
				} else if errors.Is(err, scheduling.ErrUnsupportedOperation) {
					log.Warn("Aborting scale-out operation as we lack sufficient disabled hosts to scale-out, and adding additional hosts directly is not supported by the current cluster type.")
					return limit, load
				} else {
					continue
				}
			}

			numProvisioned += targetNumProvisioned
		}

		// If we provisioned any hosts -- or if we were supposed to provision at least one host -- then we'll
		// print a message about how many we provisioned, and how many failures we encountered.
		if (numProvisioned > 0 || targetNumProvisioned > 0) && log.GetLevel() == logger.LOG_LEVEL_ALL {
			log.Debug("Provisioned %d new hosts based on #CommittedGPUs(%d). Previous #hosts: %d. Current #hosts: %d. #FailedProvisions: %d.",
				numProvisioned, load, oldNumHosts, cluster.Len(), numFailures)
		}
	} else if !cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts { // If this was the reason the first if-statement evaluated to false, then we'll log a warning message.
		log.Warn("Would like to scale out by %d hosts (from %d to %d); however, cluster cannot possibly scale-out right now.",
			scaledOutNumHosts-oldNumHosts, oldNumHosts, scaledOutNumHosts)
	}

	return limit, load
}

// multiReplicaValidateCapacity validates the Cluster's capacity for static and dynamic policies.
//
// This function exists so that it can be reused by Static, DynamicV3, and DynamicV4, as they all
// operate identically with respect to capacity validation.
//
// multiReplicaValidateCapacity will return immediately if the scheduling.Policy does not support
// predictive auto-scaling (i.e., if policy.SupportsPredictiveAutoscaling() were to return false).
func multiReplicaValidateCapacity(policy scheduling.Policy, cluster scheduling.Cluster, log logger.Logger) {
	defer cluster.Scheduler().SetLastCapacityValidation(time.Now())

	// Sanity check. The multiReplicaValidateCapacity function should only be called by
	// policies that support predictive auto-scaling, but just in case...
	if !policy.SupportsPredictiveAutoscaling() {
		return
	}

	limit, load := multiReplicaTryScaleOut(policy, cluster, log)

	defaultTryScaleIn(policy, cluster, log, limit, load)
}

// singleReplicaValidateCapacity is used by single-replica policies like Reservation and FCFS to scale up/down.
func singleReplicaValidateCapacity(policy scheduling.Policy, cluster scheduling.Cluster, log logger.Logger) {
	defer cluster.Scheduler().SetLastCapacityValidation(time.Now())

	// Sanity check. The multiReplicaValidateCapacity function should only be called by
	// policies that support predictive auto-scaling, but just in case...
	if !policy.SupportsPredictiveAutoscaling() {
		return
	}

	limit, load := defaultComputeLimitAndLoad(policy, cluster)

	defaultTryScaleIn(policy, cluster, log, limit, load)
}
