package placer

import (
	"context"
	"github.com/Scusemua/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/logger"
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
	metricsProvider  scheduling.MetricsProvider
	log              logger.Logger
	instance         internalPlacer
	schedulingPolicy scheduling.Policy

	numReplicas int
	mu          sync.Mutex
}

// NewAbstractPlacer creates a new AbstractPlacer struct and returns a pointer to it.
func NewAbstractPlacer(metricsProvider scheduling.MetricsProvider, numReplicas int, schedulingPolicy scheduling.Policy) *AbstractPlacer {
	placer := &AbstractPlacer{
		metricsProvider:  metricsProvider,
		numReplicas:      numReplicas,
		schedulingPolicy: schedulingPolicy,
	}

	config.InitLogger(&placer.log, placer)
	return placer
}

// reserveResourcesForKernel is used by placers to reserve resources on candidate hosts for arbitrary/unspecified
// replicas of a particular kernel.
//
// reserveResourcesForKernel returns true (and nil) if resources were reserved.
//
// If resources could not be reserved, then false is returned, along with an error explaining why
// the resources could not be reserved.
//
// The 'forTraining' argument indicates whether the reservation is for a "ready-to-train" replica, in which case it
// will be created as a scheduling.CommittedAllocation, or if it for a "regular" (i.e., not "ready-to-train") replica,
// in which case it will be created as either a scheduling.CommittedAllocation or scheduling.PendingAllocation
// depending upon the scheduling.Policy configured for the AllocationManager.
func (placer *AbstractPlacer) reserveResourcesForKernel(candidateHost scheduling.Host, kernelSpec *proto.KernelSpec, forTraining bool) (bool, error) {
	var usePendingReservation bool

	// If we are migrating a replica that needs to begin training right away,
	// then we should not use a pending reservation.
	//
	// The container will need resources committed to it immediately.
	//
	// Alternatively, if we aren't going to be creating reservations for a kernel container that intends to
	// begin training immediately upon being created, then we defer to the configured scheduling policy.
	// To do this, we simply query the resource binding mode of the configured scheduling policy by calling
	// the placer's 'reservationShouldUsePendingResources' method.
	if forTraining {
		usePendingReservation = false
	} else {
		usePendingReservation = placer.reservationShouldUsePendingResources()
	}

	if candidateHost.IsExcludedFromScheduling() {
		placer.log.Warn("Candidate host %s (ID=%s) is excluded from scheduling...",
			candidateHost.GetNodeName(), candidateHost.GetID())

		return false, scheduling.ErrHostExcludedFromScheduling
	}

	if !candidateHost.Enabled() {
		placer.log.Error("Candidate host %s (ID=%s) is disabled...",
			candidateHost.GetNodeName(), candidateHost.GetID())

		return false, scheduling.ErrHostDisabled
	}

	reserved, err := candidateHost.ReserveResources(kernelSpec, usePendingReservation)
	if err != nil {
		// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
		if reserved {
			panic("We successfully reserved resources on a host despite ReserveResources also returning an error...")
		}
	}

	return reserved, err
}

// reserveResourcesForReplica is used by placers to reserve resources on candidate hosts for specified replicas of a
// particular kernel.
//
// reserveResourcesForReplica returns true (and nil) if resources were reserved.
//
// If resources could not be reserved, then false is returned, along with an error explaining why
// the resources could not be reserved.
//
// The 'forTraining' argument indicates whether the reservation is for a "ready-to-train" replica, in which case it
// will be created as a scheduling.CommittedAllocation, or if it for a "regular" (i.e., not "ready-to-train") replica,
// in which case it will be created as either a scheduling.CommittedAllocation or scheduling.PendingAllocation
// depending upon the scheduling.Policy configured for the AllocationManager.
func (placer *AbstractPlacer) reserveResourcesForReplica(candidateHost scheduling.Host, replicaSpec *proto.KernelReplicaSpec, forTraining bool) (bool, error) {
	var usePendingReservation bool

	// If we are migrating a replica that needs to begin training right away,
	// then we should not use a pending reservation.
	//
	// The container will need resources committed to it immediately.
	//
	// Alternatively, if we aren't going to be creating reservations for a kernel container that intends to
	// begin training immediately upon being created, then we defer to the configured scheduling policy.
	// To do this, we simply query the resource binding mode of the configured scheduling policy by calling
	// the placer's 'reservationShouldUsePendingResources' method.
	if forTraining {
		usePendingReservation = false
	} else {
		usePendingReservation = placer.reservationShouldUsePendingResources()
	}

	if candidateHost.IsExcludedFromScheduling() {
		placer.log.Warn("Candidate host %s (ID=%s) is excluded from scheduling...",
			candidateHost.GetNodeName(), candidateHost.GetID())

		return false, scheduling.ErrHostExcludedFromScheduling
	}

	if !candidateHost.Enabled() {
		placer.log.Error("Candidate host %s (ID=%s) is disabled...",
			candidateHost.GetNodeName(), candidateHost.GetID())

		return false, scheduling.ErrHostDisabled
	}

	reserved, err := candidateHost.ReserveResourcesForSpecificReplica(replicaSpec, usePendingReservation)
	if err != nil {
		// Sanity check. If there was an error, then reserved should be false, so we'll panic if it is true.
		if reserved {
			panic("We successfully reserved resources on a host despite ReserveResources also returning an error...")
		}
	}

	return reserved, err
}

// reservationShouldUsePendingResources returns true if resource reservations on candidate hosts should be made
// using pending resources, and false if they should be made using committed resources.
//
// Reservations should use pending resources if the resources are only bound when training starts.
//
// If resources are bound when the container is created, then pending resources should NOT be used.
func (placer *AbstractPlacer) reservationShouldUsePendingResources() bool {
	return placer.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart
}

// FindHosts returns a list of hosts that can satisfy the resourceSpec.
// The number of hosts returned is determined by the placer.
//
// The core logic of FindHosts is implemented by the AbstractPlacer's internalPlacer instance/field.
//
// If FindHosts returns nil (rather than an empty slice), then an error occurred.
func (placer *AbstractPlacer) FindHosts(blacklist []interface{}, kernelSpec *proto.KernelSpec, numHosts int, forTraining bool) ([]scheduling.Host, error) {
	placer.mu.Lock()
	defer placer.mu.Unlock()
	st := time.Now()

	// The following checks make sense/apply for all concrete implementations of Placer.
	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", numHosts, kernelSpec.ResourceSpec.String(), placer.instance.GetIndex().Len())
	if placer.instance.GetIndex().Len() < numHosts {
		placer.log.Warn("Index has insufficient number of hosts: %d. Required: %d. "+
			"We won't find enough hosts on this pass, but we can try to scale-out afterwards.",
			placer.instance.GetIndex().Len(), numHosts)
	}

	// Invoke internalPlacer's implementation of the findHosts method for the core logic of FindHosts.
	metrics := []float64{kernelSpec.ResourceSpec.GPU()}
	hosts, err := placer.instance.findHosts(blacklist, kernelSpec, numHosts, forTraining, metrics)
	if err != nil {
		placer.log.Error("Error encountered while trying to find viable hosts for replica of kernel %s: %v",
			kernelSpec.Id, err)
	}

	latency := time.Since(st)

	var successLabel string
	if hosts == nil || len(hosts) < numHosts {
		placer.log.Warn(utils.OrangeStyle.Render("Found only %d/%d hosts for kernel %s. Time elapsed: %v."),
			len(hosts), numHosts, kernelSpec.Id, latency)
		successLabel = "false"
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Found %d/%d viable hosts for kernel %s after %v."),
			len(hosts), numHosts, kernelSpec.Id, latency)
		successLabel = "true"
	}

	if placer.metricsProvider != nil && placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
		placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": successLabel}).Observe(float64(latency.Microseconds()))
	}

	if hosts != nil {
		placer.instance.UpdateIndexMultiple(hosts)
	}

	return hosts, err
}

// ReserveResourcesForReplica is used to instruct the scheduling.Placer to explicitly reserve resources for a
// particular KernelReplica of a particular kernel.
//
// The primary use case for ReserveResourcesForReplica is when a specific scheduling.KernelReplica is specified to
// serve as the primary replica within the metadata of an "execute_request" message. This may occur because the user
// explicitly placed that metadata there, or following a migration when the ClusterGateway has a specific
// replica that should be able to serve the execution request.
//
// PRECONDITION: The specified scheduling.KernelReplica should already be scheduled on the scheduling.Host
// on which the resources are to be reserved.
func (placer *AbstractPlacer) ReserveResourcesForReplica(kernel scheduling.Kernel, replica scheduling.KernelReplica, commitResources bool) error {
	host := replica.Host()

	decimalSpec := kernel.ResourceSpec()
	placer.log.Debug("Explicitly reserving resources [%v] for replica %d of kernel %s [commitResources=%v].",
		decimalSpec, replica.ReplicaID(), kernel.ID(), commitResources)
	reserved, err := host.ReserveResourcesForSpecificReplica(replica.KernelReplicaSpec(), !commitResources)

	if reserved {
		placer.log.Debug("Explicitly reserved [%v] resources for replica %d of kernel %s [commitResources=%v].",
			decimalSpec, replica.ReplicaID(), kernel.ID(), commitResources)
		return nil
	}

	placer.log.Warn("Failed to explicitly reserve resources [%v] for replica %d of kernel %s [commitResources=%v]: %v",
		decimalSpec, replica.ReplicaID(), kernel.ID(), commitResources, err)
	return err
}

// FindHost returns a single host instance that can satisfy the resourceSpec.
func (placer *AbstractPlacer) FindHost(blacklist []interface{}, replicaSpec *proto.KernelReplicaSpec, forTraining bool) (scheduling.Host, error) {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	st := time.Now()
	metrics := []float64{replicaSpec.ResourceSpec().GPU()}

	// Invoke internalPlacer's implementation of the findHost method for the core logic of FindHost.
	host, err := placer.instance.findHost(blacklist, replicaSpec, forTraining, metrics)
	if err != nil {
		placer.log.Error("Error while finding host for replica of kernel %s: %v", replicaSpec.Kernel.Id, err)
		return nil, err
	}

	latency := time.Since(st)

	if host == nil {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to identify single viable hosts. Time elapsed: %v."), latency)

		if placer.metricsProvider != nil && placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
			placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram().
				With(prometheus.Labels{"successful": "false"}).Observe(float64(latency.Microseconds()))
		}
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified single viable host after %v."), latency)

		if placer.metricsProvider != nil && placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
			placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram().
				With(prometheus.Labels{"successful": "true"}).Observe(float64(latency.Microseconds()))
		}
	}

	if host != nil {
		placer.instance.UpdateIndex(host)
	}
	return host, nil
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host scheduling.Host, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if host == nil {
		placer.log.Debug("host cannot be nil when placing a kernel replica...")
		return nil, scheduling.ErrNilHost
	}

	placer.log.Debug("Starting replica %d of kernel %s on host %s (ID=%s) now...",
		in.ReplicaId, in.Kernel.Id, host.GetNodeName(), host.GetID())

	connInfo, err := host.StartKernelReplica(context.Background(), in)

	if err != nil {
		placer.log.Error("host %s (ID=%s) returned an error after trying to start replica %d of kernel %s: %v",
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id, err)

		return nil, err
	}

	if connInfo != nil {
		placer.log.Debug("host %s (ID=%s) returned the following connection info for replica %d of kernel %s: %v",
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id, connInfo)
	} else {
		placer.log.Error(
			utils.RedStyle.Render(
				"host %s (ID=%s) returned no error and no connection info after trying to start replica %d of kernel %s..."),
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id)

		return nil, scheduling.ErrNilConnectionInfo
	}

	placer.instance.UpdateIndex(host)

	placer.log.Debug("Returning connection info for replica %d of kernel %s after placing it on host %s (ID=%s): %v",
		in.ReplicaId, in.Kernel.Id, host.GetNodeName(), host.GetID(), connInfo)

	return connInfo, err
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host scheduling.Host, sess scheduling.UserSession, noop bool) error {
	if noop {
		return nil
	}

	// We'll wait up to 5 minutes for the operation to complete, in case there is some significant I/O required.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %s (ID=%v).",
		sess.ID(), host.GetNodeName(), host.GetID())
	_, err := host.StopKernel(ctx, &proto.KernelId{Id: sess.ID()})

	return err
}
