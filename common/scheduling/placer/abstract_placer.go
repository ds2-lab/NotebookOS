package placer

import (
	"context"
	"github.com/Scusemua/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/entity"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/logger"
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
	mu          sync.Mutex
	cluster     scheduling.Cluster
	log         logger.Logger
	numReplicas int
	instance    internalPlacer
}

// NewAbstractPlacer creates a new AbstractPlacer struct and returns a pointer to it.
func NewAbstractPlacer(cluster scheduling.Cluster, numReplicas int) *AbstractPlacer {
	placer := &AbstractPlacer{
		cluster:     cluster,
		numReplicas: numReplicas,
	}
	config.InitLogger(&placer.log, placer)
	return placer
}

// FindHosts returns a list of hosts that can satisfy the resourceSpec.
// The number of hosts returned is determined by the placer.
//
// The core logic of FindHosts is implemented by the AbstractPlacer's internalPlacer instance/field.
func (placer *AbstractPlacer) FindHosts(kernelSpec *proto.KernelSpec, numHosts int) []*entity.Host {
	placer.mu.Lock()
	st := time.Now()

	// The following checks make sense/apply for all concrete implementations of Placer.
	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", numHosts, kernelSpec.ResourceSpec.String(), placer.instance.getIndex().Len())
	if placer.instance.getIndex().Len() < numHosts {
		placer.log.Warn("Index has insufficient number of hosts: %d. Required: %d. "+
			"We won't find enough hosts on this pass, but we can try to scale-out afterwards.",
			placer.instance.getIndex().Len(), numHosts)
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

	if placer.cluster.MetricsProvider() != nil && placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
		placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": successLabel}).Observe(float64(latency.Microseconds()))
	}

	return hosts
}

// hostIsViable returns a tuple (bool, bool).
// First bool represents whether the host is viable.
// Second bool indicates whether the host was successfully locked. This does not mean that it is still locked.
// Merely that we were able to lock it when we tried. If we locked it and found that the host wasn't viable,
// then we'll have unlocked it before hostIsViable returns.
func (placer *AbstractPlacer) hostIsViable(candidateHost *entity.Host, spec types.Spec) (bool, bool) {
	return placer.instance.hostIsViable(candidateHost, spec)
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *AbstractPlacer) FindHost(blacklist []interface{}, spec types.Spec) *entity.Host {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	st := time.Now()
	// Invoke internalPlacer's implementation of the findHost method for the core logic of FindHost.
	host := placer.instance.findHost(blacklist, spec)
	latency := time.Since(st)

	if host == nil {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to identify single viable hosts. Time elapsed: %v."), latency)

		if placer.cluster.MetricsProvider() != nil && placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
			placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
				With(prometheus.Labels{"successful": "false"}).Observe(float64(latency.Microseconds()))
		}
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified single viable host after %v."), latency)

		if placer.cluster.MetricsProvider() != nil && placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram() != nil {
			placer.cluster.MetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
				With(prometheus.Labels{"successful": "true"}).Observe(float64(latency.Microseconds()))
		}
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return host
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host *entity.Host, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if host == nil {
		placer.log.Debug("Host cannot be nil when placing a kernel replica...")
		return nil, scheduling.ErrNilHost
	}

	placer.log.Debug("Starting replica %d of kernel %s on host %s (ID=%s) now...",
		in.ReplicaId, in.Kernel.Id, host.NodeName, host.ID)

	connInfo, err := host.StartKernelReplica(context.Background(), in)

	if err != nil {
		placer.log.Error("Host %s (ID=%s) returned an error after trying to start replica %d of kernel %s: %v",
			host.NodeName, host.ID, in.ReplicaId, in.Kernel.Id, err)

		return nil, err
	}

	if connInfo != nil {
		placer.log.Debug("Host %s (ID=%s) returned the following connection info for replica %d of kernel %s: %v",
			host.NodeName, host.ID, in.ReplicaId, in.Kernel.Id, connInfo)
	} else {
		placer.log.Error(
			utils.RedStyle.Render(
				"Host %s (ID=%s) returned no error and no connection info after trying to start replica %d of kernel %s..."),
			host.NodeName, host.ID, in.ReplicaId, in.Kernel.Id)

		return nil, scheduling.ErrNilConnectionInfo
	}

	return connInfo, err
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host *entity.Host, sess scheduling.UserSession, noop bool) error {
	if noop {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %v.", sess.ID(), host)
	_, err := host.StopKernel(ctx, &proto.KernelId{Id: sess.ID()})

	return err
}
