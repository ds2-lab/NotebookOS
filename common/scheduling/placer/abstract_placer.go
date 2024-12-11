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
	mu               sync.Mutex
	metricsProvider  scheduling.MetricsProvider
	log              logger.Logger
	numReplicas      int
	instance         internalPlacer
	schedulingPolicy scheduling.Policy
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
func (placer *AbstractPlacer) FindHosts(kernelSpec *proto.KernelSpec, numHosts int) []scheduling.Host {
	placer.mu.Lock()
	st := time.Now()

	// The following checks make sense/apply for all concrete implementations of Placer.
	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", numHosts, kernelSpec.ResourceSpec.String(), placer.instance.GetIndex().Len())
	if placer.instance.GetIndex().Len() < numHosts {
		placer.log.Warn("Index has insufficient number of hosts: %d. Required: %d. "+
			"We won't find enough hosts on this pass, but we can try to scale-out afterwards.",
			placer.instance.GetIndex().Len(), numHosts)
	}

	// Invoke internalPlacer's implementation of the findHosts method for the core logic of FindHosts.
	hosts := placer.instance.findHosts(kernelSpec, numHosts)

	latency := time.Since(st)

	var successLabel string
	if hosts == nil || len(hosts) < numHosts {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to identify the %d required hosts for kernel %s. Found only %d/%d. Time elapsed: %v."),
			placer.numReplicas, kernelSpec.Id, len(hosts), placer.numReplicas, latency)
		successLabel = "false"
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified %d/%d viable hosts for kernel %s after %v."),
			len(hosts), numHosts, kernelSpec.Id, latency)
		successLabel = "true"
	}

	if placer.metricsProvider != nil && placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
		placer.metricsProvider.GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": successLabel}).Observe(float64(latency.Microseconds()))
	}

	placer.instance.UpdateIndexMultiple(hosts)
	return hosts
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *AbstractPlacer) FindHost(blacklist []interface{}, kernelSpec *proto.KernelSpec, forTraining bool) scheduling.Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	st := time.Now()
	// Invoke internalPlacer's implementation of the findHost method for the core logic of FindHost.
	host := placer.instance.findHost(blacklist, kernelSpec, forTraining)
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

	placer.instance.UpdateIndex(host)
	return host
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host scheduling.Host, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if host == nil {
		placer.log.Debug("Host cannot be nil when placing a kernel replica...")
		return nil, scheduling.ErrNilHost
	}

	placer.log.Debug("Starting replica %d of kernel %s on host %s (ID=%s) now...",
		in.ReplicaId, in.Kernel.Id, host.GetNodeName(), host.GetID())

	connInfo, err := host.StartKernelReplica(context.Background(), in)

	if err != nil {
		placer.log.Error("Host %s (ID=%s) returned an error after trying to start replica %d of kernel %s: %v",
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id, err)

		return nil, err
	}

	if connInfo != nil {
		placer.log.Debug("Host %s (ID=%s) returned the following connection info for replica %d of kernel %s: %v",
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id, connInfo)
	} else {
		placer.log.Error(
			utils.RedStyle.Render(
				"Host %s (ID=%s) returned no error and no connection info after trying to start replica %d of kernel %s..."),
			host.GetNodeName(), host.GetID(), in.ReplicaId, in.Kernel.Id)

		return nil, scheduling.ErrNilConnectionInfo
	}

	placer.instance.UpdateIndex(host)

	placer.log.Debug("Returning connection info for replica %d of kernel %s after placing it on Host %s: %v",
		in.ReplicaId, in.Kernel.Id, host.GetID(), connInfo)

	return connInfo, err
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host scheduling.Host, sess scheduling.UserSession, noop bool) error {
	if noop {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %v.", sess.ID(), host)
	_, err := host.StopKernel(ctx, &proto.KernelId{Id: sess.ID()})

	return err
}
