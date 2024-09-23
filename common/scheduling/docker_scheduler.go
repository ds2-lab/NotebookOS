package scheduling

import (
	"context"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"google.golang.org/grpc/connectivity"
	"sync/atomic"
	"time"
)

const (
	DockerKernelDebugPortDefault int32 = 32000
)

type DockerScheduler struct {
	*BaseScheduler

	// Used in Docker mode. Assigned to individual kernel replicas, incremented after each assignment.
	dockerModeKernelDebugPort atomic.Int32
}

func NewDockerScheduler(gateway ClusterGateway, cluster Cluster, placer Placer, hostSpec types.Spec, opts *ClusterSchedulerOptions) (*DockerScheduler, error) {
	if !gateway.DockerMode() {
		return nil, types.ErrIncompatibleDeploymentMode
	}

	baseScheduler := NewBaseScheduler(gateway, cluster, placer, hostSpec, opts)

	dockerScheduler := &DockerScheduler{
		BaseScheduler: baseScheduler,
	}

	dockerScheduler.dockerModeKernelDebugPort.Store(DockerKernelDebugPortDefault)

	baseScheduler.instance = dockerScheduler

	err := dockerScheduler.RefreshClusterNodes()
	if err != nil {
		dockerScheduler.log.Error("Initial retrieval of Docker nodes failed: %v", err)
	}

	go dockerScheduler.pollForResourceData()

	return dockerScheduler, nil
}

func (s *DockerScheduler) selectViableHostForReplica(replicaId int32, replicaSpec *proto.KernelReplicaSpec) (*Host, error) {
	blacklist := make([]interface{}, 0)
	kernelId := replicaSpec.ID()

	replicaHosts, err := s.gateway.GetHostsOfKernel(kernelId)
	if err != nil {
		return nil, err
	}

	// We "blacklist" all the hosts for which other replicas of this kernel are scheduled.
	// That way, we'll necessarily select a host on which no other replicas of this kernel are running.
	for _, host := range replicaHosts {
		s.log.Debug("Adding host %s (on node %s) of kernel %s-%d to blacklist.", host.ID, host.NodeName, kernelId, replicaId)
		blacklist = append(blacklist, host.GetMeta(HostMetaRandomIndex))
	}

	//for _, replica := range kernel.Replicas() {
	//	host := replica.GetHost()
	//	if host == nil {
	//		// This shouldn't happen as far as I know, but if it does, then we can't really identify the proper host to migrate to.
	//		panic(fmt.Sprintf("Replica %d of kernel %s does NOT have a host...", replicaId, in.ID()))
	//	}
	//
	//	s.log.Debug("Adding host %s (on node %s) of kernel %s-%d to blacklist.", host.ID, host.NodeName, in.ID(), replicaId)
	//	blacklist = append(blacklist, host.GetMeta(HostMetaRandomIndex))
	//}

	host := s.placer.FindHost(blacklist, types.FullSpecFromKernelReplicaSpec(replicaSpec))
	if host == nil {
		return nil, ErrInsufficientHostsAvailable
	}

	s.log.Debug("Selected host %s as target for migration. Will migrate kernel %s-%d to host %s.", host.ID, kernelId, replicaId, host.ID)
	return host, nil
}

func (s *DockerScheduler) MigrateContainer(container *Container, host *Host, b bool) (bool, error) {
	//TODO implement me
	panic("implement me")
}

// ScheduleKernelReplica schedules a particular replica onto the given Host.
//
// Exactly one of replicaSpec and kernelSpec should be non-nil.
// That is, both cannot be nil, and both cannot be non-nil.
//
// If targetHost is nil, then a candidate host is identified automatically by the ClusterScheduler.
func (s *DockerScheduler) ScheduleKernelReplica(replicaId int32, kernelId string, replicaSpec *proto.KernelReplicaSpec, kernelSpec *proto.KernelSpec, host *Host) error {
	if kernelSpec == nil && replicaSpec == nil {
		panic("Both `kernelSpec` and `replicaSpec` cannot be nil; exactly one of these two arguments must be non-nil.")
	}

	if kernelSpec != nil && replicaSpec != nil {
		panic("Both `kernelSpec` and `replicaSpec` cannot be non-nil; exactly one of these two arguments must be non-nil.")
	}

	if host == nil {
		var err error
		host, err = s.selectViableHostForReplica(replicaId, replicaSpec)
		if err != nil {
			s.log.Error("Could not find viable host for replica %d of kernel %s: %v", replicaId, kernelId, err)
			return err
		}
	}

	if kernelSpec != nil {
		s.log.Debug("Launching replica %d of kernel %s on host %v now.", replicaId, kernelId, host)
		replicaSpec = &proto.KernelReplicaSpec{
			Kernel:                    kernelSpec,
			ReplicaId:                 replicaId,
			NumReplicas:               int32(s.opts.NumReplicas),
			DockerModeKernelDebugPort: s.dockerModeKernelDebugPort.Add(1),
		}
	} else {
		// Make sure to assign a value to DockerModeKernelDebugPort if one is not already set.
		if replicaSpec.DockerModeKernelDebugPort <= 1023 {
			replicaSpec.DockerModeKernelDebugPort = s.dockerModeKernelDebugPort.Add(1)
		}

		s.log.Debug("Launching replica %d of kernel %s on host %v now.", replicaSpec.ReplicaId, kernelId, host)
	}

	replicaConnInfo, err := s.placer.Place(host, replicaSpec)
	if err != nil {
		if kernelSpec != nil {
			s.log.Warn("Failed to start kernel replica(%s:%d): %v", kernelId, replicaId, err)
		} else {
			s.log.Warn("Failed to start kernel replica(%s:%d): %v", kernelId, replicaId, err)
		}
		return err
	}

	s.log.Debug("Received replica connection info after calling placer.Place: %v", replicaConnInfo)
	return nil
}

// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *DockerScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec) error {
	// Channel to send either notifications that we successfully launched a replica (in the form of a struct{}{})
	// or errors that occurred when launching a replica.
	resultChan := make(chan interface{}, 3)

	s.log.Debug("Preparing to search for %d hosts to serve replicas of kernel %s. Resources required: %s.", s.opts.NumReplicas, in.Id, in.ResourceSpec.String())

	// Identify the hosts onto which we will place replicas of the kernel.
	hosts := s.placer.FindHosts(types.FullSpecFromKernelSpec(in))

	if len(hosts) < s.opts.NumReplicas {
		s.log.Error("Found %d/%d hosts to serve replicas of kernel %s.", len(hosts), s.opts.NumReplicas, in.Id)
		return fmt.Errorf("%w: found %d/%d required hosts to serve replicas of kernel %s", ErrInsufficientHostsAvailable, len(hosts), s.opts.NumReplicas, in.Id)
	}

	s.log.Debug("Found %d hosts to serve replicas of kernel %s: %v", s.opts.NumReplicas, in.Id, hosts)

	// For each host, launch a Docker replica on that host.
	for i, host := range hosts {
		// Launch replicas in parallel.
		go func(replicaId int, targetHost *Host) {
			// Only 1 of arguments 2 and 3 can be non-nil.
			if err := s.ScheduleKernelReplica(int32(replicaId), in.Id, nil, in, targetHost); err != nil {
				// An error occurred. Send it over the channel.
				resultChan <- err
			} else {
				// Send a notification that a replica was launched successfully.
				resultChan <- struct{}{}
			}
		}(i+1, host)
	}

	// Keep looping until we've received all responses or the context times-out.
	responsesReceived := 0
	responsesRequired := len(hosts)
	for responsesReceived < responsesRequired {
		select {
		// Context time-out, meaning the operation itself has timed-out or been cancelled.
		case <-ctx.Done():
			{
				err := ctx.Err()
				if err != nil {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. Error extracted from now-cancelled context: %v", in.Id, err)
					return err
				} else {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. No error extracted from now-cancelled context.", in.Id, err)
					return types.ErrRequestTimedOut // Return generic error if we can't get one from the Context for some reason.
				}
			}
		// Received response.
		case val := <-resultChan:
			{
				if err, ok := val.(error); ok {
					s.log.Error("Error while launching at least one of the replicas of kernel %s: %v", in.Id, err)
					return err
				}

				responsesReceived += 1

				s.log.Debug("Launched %d/%d replica(s) of kernel %s.", responsesReceived, responsesRequired, in.Id)
			}
		}
	}

	return nil
}

// pollForResourceData queries each Host in the Cluster for updated resource usage information.
// These queries are issued at a configurable frequency specified in the Cluster Gateway's configuration file.
func (s *DockerScheduler) pollForResourceData() {
	// Keep track of failed gRPC requests.
	// If too many requests fail in a row, then we'll assume that the Host is dead.
	numConsecutiveFailuresPerHost := make(map[string]int)
	lastSync := time.Now()

	for {
		s.cluster.ReadLockHosts()

		// If we've forcibly synchronized this Host recently (i.e., within half the synchronization interval ago),
		// then we'll just skip it to save network bandwidth.

		// This should be approximately equal to s.remoteSynchronizationInterval
		timeSinceLastSync := time.Now().Sub(lastSync)

		// ts is the time exactly "a quarter of the interval since the last synchronization" ago.
		// So, if we last synchronized 16 min ago, then ts is equal to whatever time it was 4 min ago.
		ts := time.Now().Add(-1 * time.Duration(float64(timeSinceLastSync)*0.25))

		hosts := make([]*Host, 0, s.cluster.Len())
		s.cluster.RangeOverHosts(func(_ string, host *Host) (contd bool) {
			// If we've not synchronized this host within the last <interval of time since last sync> / 4,
			// then we'll synchronize it again now.
			//
			// So, for example, if the last round of synchronizations was 16 minutes ago, then we'll synchronize
			// this Host now as long as we've not done so within the last 4 minutes.
			if host.LastRemoteSync.Before(ts) {
				hosts = append(hosts, host)
			}
			return true
		})
		s.cluster.ReadUnlockHosts()

		for _, host := range hosts {
			hostId := host.ID
			err := host.SynchronizeResourceInformation()
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

				s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s (consecutive: %d): %v", hostId, host.NodeName, numConsecutiveFailures, err)

				// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
				if numConsecutiveFailures >= ConsecutiveFailuresWarning {
					// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
					if host.Conn().GetState() == connectivity.TransientFailure || host.Conn().GetState() == connectivity.Shutdown {
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.", numConsecutiveFailures, host.ID, host.NodeName, host.Conn().GetState().String(), host.ID)
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.ID, host.NodeName, "Local Daemon Connectivity Error", errorMessage)
					} else if numConsecutiveFailures >= ConsecutiveFailuresBad { // If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.", numConsecutiveFailures, host.ID, host.NodeName, host.Conn().GetState().String(), host.ID)
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.ID, host.NodeName, "Local Daemon Connectivity Error", errorMessage)
					} else { // Otherwise, we won't assume it is dead yet...
						s.log.Warn("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...", numConsecutiveFailures, host.ID, host.NodeName, host.Conn().GetState().String())
					}
				}
			} else {
				// We succeeded, so reset the consecutive failure counter, in case it is non-zero.
				numConsecutiveFailuresPerHost[hostId] = 0
			}
		}

		lastSync = time.Now()
		time.Sleep(s.remoteSynchronizationInterval)
	}
}

// RefreshClusterNodes updates the cached list of Host nodes.
// Returns nil on success; returns an error on one or more failures.
// If there are multiple failures, then their associated errors will be joined together via errors.Join(...).
func (s *DockerScheduler) RefreshClusterNodes() error {
	s.cluster.ReadLockHosts()
	hosts := make([]*Host, 0, s.cluster.Len())
	s.cluster.RangeOverHosts(func(_ string, host *Host) (contd bool) {
		hosts = append(hosts, host)
		return true
	})
	s.cluster.ReadUnlockHosts()

	errs := make([]error, 0)
	for _, host := range hosts {
		hostId := host.ID
		err := host.SynchronizeResourceInformation()
		if err != nil {
			s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s: %v",
				hostId, host.NodeName, err)
			errs = append(errs, err)
		}
	}

	if len(errs) > 1 {
		return errors.Join(errs...)
	}

	return nil
}
