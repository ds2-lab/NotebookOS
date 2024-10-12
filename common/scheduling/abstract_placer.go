package scheduling

import (
	"context"
	"errors"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/logger"
)

var (
	ErrNilHost = errors.New("host is nil when attempting to place kernel")
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
	mu       sync.Mutex
	cluster  clusterInternal
	opts     *ClusterSchedulerOptions
	log      logger.Logger
	instance internalPlacer
}

// newAbstractPlacer creates a new AbstractPlacer struct and returns a pointer to it.
func newAbstractPlacer(cluster clusterInternal, opts *ClusterSchedulerOptions) *AbstractPlacer {
	placer := &AbstractPlacer{
		cluster: cluster,
		opts:    opts,
	}
	config.InitLogger(&placer.log, placer)
	return placer
}

// FindHosts returns a list of hosts that can satisfy the resourceSpec.
// The number of hosts returned is determined by the placer.
//
// The core logic of FindHosts is implemented by the AbstractPlacer's internalPlacer instance/field.
func (placer *AbstractPlacer) FindHosts(spec types.Spec) []AbstractHost {
	placer.mu.Lock()
	st := time.Now()
	numReplicas := placer.opts.NumReplicas

	// The following checks make sense/apply for all concrete implementations of Placer.
	// If the Placer's index is empty, or if the index has too few hosts in it, then we simply return an empty slice.
	placer.log.Debug("Searching index for %d hosts to satisfy request %s. Number of hosts in index: %d.", numReplicas, spec.String(), placer.instance.getIndex().Len())
	if placer.instance.getIndex().Len() == 0 {
		placer.log.Warn(utils.OrangeStyle.Render("Index is empty... returning empty slice of Hosts."))
		return make([]AbstractHost, 0)
	} else if placer.instance.getIndex().Len() < numReplicas {
		placer.log.Warn("Index has just %d hosts (%d are required).", placer.instance.getIndex().Len(), numReplicas)
		return make([]AbstractHost, 0)
	}

	// Invoke internalPlacer's implementation of the findHosts method for the core logic of FindHosts.
	hosts := placer.instance.findHosts(spec)
	latency := time.Since(st)
	if hosts == nil || len(hosts) < numReplicas {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to identify the %d required hosts for kernel %s. Found only %d/%d. Time elapsed: %v."),
			placer.opts.NumReplicas, len(hosts), placer.opts.NumReplicas, latency)

		placer.cluster.ClusterMetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": "false"}).Observe(float64(latency.Microseconds()))
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified %d/%d viable hosts after %v."),
			len(hosts), numReplicas, latency)
		placer.cluster.ClusterMetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": "true"}).Observe(float64(latency.Microseconds()))
	}

	return hosts
}

// hostIsViable returns a tuple (bool, bool).
// First bool represents whether the host is viable.
// Second bool indicates whether the host was successfully locked. This does not mean that it is still locked.
// Merely that we were able to lock it when we tried. If we locked it and found that the host wasn't viable,
// then we'll have unlocked it before hostIsViable returns.
func (placer *AbstractPlacer) hostIsViable(candidateHost AbstractHost, spec types.Spec) (bool, bool) {
	return placer.instance.hostIsViable(candidateHost, spec)
}

// FindHost returns a single Host instance that can satisfy the resourceSpec.
func (placer *AbstractPlacer) FindHost(blacklist []interface{}, spec types.Spec) AbstractHost {
	placer.mu.Lock()
	defer placer.mu.Unlock()

	st := time.Now()
	// Invoke internalPlacer's implementation of the findHost method for the core logic of FindHost.
	host := placer.instance.findHost(blacklist, spec)
	latency := time.Since(st)

	if host == nil {
		placer.log.Warn(utils.OrangeStyle.Render("Failed to identify single viable hosts. Time elapsed: %v."), latency)
		placer.cluster.ClusterMetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": "false"}).Observe(float64(latency.Microseconds()))
	} else {
		placer.log.Debug(utils.GreenStyle.Render("Successfully identified single viable host after %v."), latency)
		placer.cluster.ClusterMetricsProvider().GetPlacerFindHostLatencyMicrosecondsHistogram().
			With(prometheus.Labels{"successful": "true"}).Observe(float64(latency.Microseconds()))
	}

	// The Host could not satisfy the resourceSpec, so return nil.
	return host
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host AbstractHost, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if host == nil {
		placer.log.Debug("Host cannot be nil when placing a kernel replica...")
		return nil, ErrNilHost
	}

	return host.StartKernelReplica(context.Background(), in)
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host AbstractHost, sess *Session, noop bool) error {
	if noop {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %v.", sess.ID(), host)
	_, err := host.StopKernel(ctx, &proto.KernelId{Id: sess.ID()})

	return err
}
