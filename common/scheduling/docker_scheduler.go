package scheduling

import (
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"google.golang.org/grpc/connectivity"
	"time"
)

type DockerScheduler struct {
	*BaseScheduler
}

func NewDockerScheduler(gateway ClusterGateway, cluster Cluster, opts *ClusterSchedulerOptions) (*DockerScheduler, error) {
	if !gateway.KubernetesMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	baseScheduler := NewClusterScheduler(gateway, cluster, opts)

	dockerScheduler := &DockerScheduler{
		BaseScheduler: baseScheduler,
	}

	baseScheduler.instance = dockerScheduler

	err := dockerScheduler.RefreshClusterNodes()
	if err != nil {
		dockerScheduler.log.Error("Initial retrieval of Docker nodes failed: %v", err)
	}

	go dockerScheduler.pollForResourceData()

	return dockerScheduler, nil
}

// pollForResourceData queries each Host in the Cluster for updated resource usage information.
// These queries are issued at a configurable frequency specified in the Cluster Gateway's configuration file.
func (s *DockerScheduler) pollForResourceData() {
	// Keep track of failed gRPC requests.
	// If too many requests fail in a row, then we'll assume that the Host is dead.
	numConsecutiveFailuresPerHost := make(map[string]int)

	for {
		s.cluster.LockHosts()
		hostManager := s.cluster.GetHostManager()

		hosts := make([]*Host, 0, hostManager.Len())
		hostManager.Range(func(_ string, host *Host) (contd bool) {
			hosts = append(hosts, host)
			return true
		})
		s.cluster.UnlockHosts()

		for _, host := range hosts {
			hostId := host.ID()
			err := host.RefreshResourceInformation()
			if err != nil {
				var (
					numConsecutiveFailures int
					ok                     bool
				)
				if numConsecutiveFailures, ok = numConsecutiveFailuresPerHost[hostId]; !ok {
					numConsecutiveFailures = 0
				}

				numConsecutiveFailures += 1
				numConsecutiveFailuresPerHost[hostId] = numConsecutiveFailures

				s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s (consecutive: %d): %v", hostId, host.NodeName(), numConsecutiveFailures, err)

				// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
				if numConsecutiveFailures >= ConsecutiveFailuresWarning {
					// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
					if host.Conn().GetState() == connectivity.TransientFailure || host.Conn().GetState() == connectivity.Shutdown {
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.", numConsecutiveFailures, host.ID(), host.NodeName(), host.Conn().GetState().String(), host.ID())
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.ID(), host.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					} else if numConsecutiveFailures >= ConsecutiveFailuresBad { // If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.", numConsecutiveFailures, host.ID(), host.NodeName(), host.Conn().GetState().String(), host.ID())
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.ID(), host.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					} else { // Otherwise, we won't assume it is dead yet...
						s.log.Warn("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...", numConsecutiveFailures, host.ID(), host.NodeName(), host.Conn().GetState().String())
					}
				}
			} else {
				// We succeeded, so reset the consecutive failure counter, in case it is non-zero.
				numConsecutiveFailuresPerHost[hostId] = 0
			}
		}

		time.Sleep(s.gpuInfoRefreshInterval)
	}
}

// RefreshClusterNodes updates the cached list of Host nodes.
// Returns nil on success; returns an error on one or more failures.
// If there are multiple failures, then their associated errors will be joined together via errors.Join(...).
func (s *DockerScheduler) RefreshClusterNodes() error {
	s.cluster.LockHosts()
	hostManager := s.cluster.GetHostManager()

	hosts := make([]*Host, 0, hostManager.Len())
	hostManager.Range(func(_ string, host *Host) (contd bool) {
		hosts = append(hosts, host)
		return true
	})
	s.cluster.UnlockHosts()

	errs := make([]error, 0)
	for _, host := range hosts {
		hostId := host.ID()
		err := host.RefreshResourceInformation()
		if err != nil {
			s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s: %v",
				hostId, host.NodeName(), err)
			errs = append(errs, err)
		}
	}

	if len(errs) > 1 {
		return errors.Join(errs...)
	}

	return nil
}
