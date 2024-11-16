package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"google.golang.org/grpc/connectivity"
	"strings"
	"sync/atomic"
	"time"
)

const (
	DockerKernelDebugPortDefault int32 = 32000

	ErrorHostname = "ERROR" // We return this from certain gRPC calls when there's an error.

	ConsecutiveFailuresWarning int = 2
	ConsecutiveFailuresBad     int = 3
)

var (
	ErrNilKernelReplica = errors.New("specified KernelReplica is nil")
	ErrNilContainer     = errors.New("specified kernel has a nil container")
	ErrNilOriginalHost  = errors.New("current host of container is nil")
)

type DockerScheduler struct {
	*BaseScheduler

	// Used in Docker mode. Assigned to individual kernel replicas, incremented after each assignment.
	dockerModeKernelDebugPort atomic.Int32
}

func NewDockerScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper, hostSpec types.Spec, kernelProvider KernelProvider, opts *scheduling.SchedulerOptions) (*DockerScheduler, error) {
	baseScheduler := NewBaseScheduler(cluster, placer, hostMapper, hostSpec, kernelProvider, opts)

	dockerScheduler := &DockerScheduler{
		BaseScheduler: baseScheduler,
	}

	dockerScheduler.dockerModeKernelDebugPort.Store(DockerKernelDebugPortDefault)

	baseScheduler.instance = dockerScheduler

	err := dockerScheduler.refreshClusterNodes()
	if err != nil {
		dockerScheduler.log.Error("Initial retrieval of Docker nodes failed: %v", err)
	}

	// go dockerScheduler.pollForResourceData()

	return dockerScheduler, nil
}

// selectViableHostForReplica is called at scheduling-time (rather than before we get to the point of scheduling, such
// as searching for viable hosts before trying to schedule the container).
//
// selectViableHostForReplica searches for a viable training host and, if one is found, then that host is returned.
// Otherwise, an error is returned.
func (s *DockerScheduler) selectViableHostForReplica(replicaSpec *proto.KernelReplicaSpec, blacklistedHosts []scheduling.Host) (scheduling.Host, error) {
	kernelId := replicaSpec.ID()

	blacklist := make([]interface{}, 0)
	for _, blacklistedHost := range blacklistedHosts {
		s.log.Debug("Host %s (ID=%s) of kernel %s-%d was specifically specified as being blacklisted.",
			blacklistedHost.GetNodeName(), blacklistedHost.GetID(), kernelId, replicaSpec.ReplicaId)
		blacklist = append(blacklist, blacklistedHost.GetMeta(s.placer.GetIndex().GetMetadataKey()))
	}

	replicaHosts, err := s.hostMapper.GetHostsOfKernel(kernelId)
	if err != nil {
		return nil, err
	}

	// We "blacklist" all the hosts for which other replicas of this kernel are scheduled.
	// That way, we'll necessarily select a host on which no other replicas of this kernel are running.
	for _, host := range replicaHosts {
		s.log.Debug("Adding host %s (on node %s) of kernel %s-%d to blacklist.",
			host.GetID(), host.GetNodeName(), kernelId, replicaSpec.ReplicaId)
		blacklist = append(blacklist, host.GetMeta(s.placer.GetIndex().GetMetadataKey()))
	}

	host := s.placer.FindHost(blacklist, replicaSpec.Kernel)
	if host == nil {
		return nil, scheduling.ErrInsufficientHostsAvailable
	}

	s.log.Debug("Selected host %s as target for migration. Will migrate kernel %s-%d to host %s.",
		host.GetID, kernelId, replicaSpec.ReplicaId, host.GetID())
	return host, nil
}

// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
func (s *DockerScheduler) addReplicaSetup(_ string, _ *scheduling.AddReplicaOperation) {
	// no-op
}

// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
func (s *DockerScheduler) postScheduleKernelReplica(_ string, _ *scheduling.AddReplicaOperation) {
	// no-op
}

// RemoveReplicaFromHost removes the specified replica from its Host.
func (s *DockerScheduler) RemoveReplicaFromHost(kernelReplica scheduling.KernelReplica) error {
	if kernelReplica == nil {
		return ErrNilKernelReplica
	}

	kernel, loaded := s.kernelProvider.GetKernel(kernelReplica.ID())
	if !loaded {
		return types.ErrKernelNotFound
	}

	// First, stop the kernel on the replica we'd like to remove.
	_, err := kernel.RemoveReplicaByID(kernelReplica.ReplicaID(), s.cluster.Placer().Reclaim, false)
	if err != nil {
		s.log.Error("Error while stopping replica %d of kernel %s: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)
		return err
	}

	if err = kernelReplica.Container().Session().RemoveReplicaById(kernelReplica.ReplicaID()); err != nil {
		s.log.Error("Failed to remove replica %d from session \"%s\" because: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)
		return err
	}

	s.log.Debug("Successfully removed replica %d of kernel %s.", kernelReplica.ReplicaID(), kernelReplica.ID())

	return nil
}

// ScheduleKernelReplica schedules a particular replica onto the given Host.
//
// If targetHost is nil, then a candidate Host is identified automatically by the Scheduler.
func (s *DockerScheduler) ScheduleKernelReplica(replicaSpec *proto.KernelReplicaSpec, targetHost scheduling.Host, blacklistedHosts []scheduling.Host) (err error) {
	kernelId := replicaSpec.Kernel.Id // We'll use this a lot.

	if targetHost == nil {
		s.log.Debug("No target host specified when scheduling replica %d of kernel %s. Searching for one now...",
			replicaSpec.ReplicaId, kernelId)

		targetHost, err = s.selectViableHostForReplica(replicaSpec, blacklistedHosts)
		if err != nil {
			s.log.Error("Could not find viable targetHost for replica %d of kernel %s: %v",
				replicaSpec.ReplicaId, kernelId, err)
			return err
		}

		s.log.Debug("Found viable target host for replica %d of kernel %s at scheduling time: host %s",
			replicaSpec.ReplicaId, kernelId, targetHost.GetID())
	}

	// Make sure to assign a value to DockerModeKernelDebugPort if one is not already set.
	if replicaSpec.DockerModeKernelDebugPort <= 1023 {
		replicaSpec.DockerModeKernelDebugPort = s.dockerModeKernelDebugPort.Add(1)

		s.log.Debug("Assigned docker mode kernel replica debug port to %d for replica %d of kernel %s.",
			replicaSpec.DockerModeKernelDebugPort, replicaSpec.ReplicaId, kernelId)
	}

	s.log.Debug("Launching replica %d of kernel %s on targetHost %v now.", replicaSpec.ReplicaId, kernelId, targetHost)

	replicaConnInfo, err := s.placer.Place(targetHost, replicaSpec)
	if err != nil {
		s.log.Warn("Failed to start kernel replica(%s:%d): %v", kernelId, replicaSpec.ReplicaId, err)
		return err
	}

	s.log.Debug("Received replica connection info after calling placer.Place: %v", replicaConnInfo)
	return nil
}

// scheduleKernelReplicas schedules a replica of the specified kernel on each Host within the given slice of scheduling.Host.
// Specifically, scheduleKernelReplicas calls ScheduleKernelReplica for each of the Host instances within the given
// slice of Hosts in a separate goroutine, thereby scheduling a replica of the given kernel on the Host. That is, the
// scheduling of a replica of the kernel occurs in a unique goroutine for each of the specified Host instances.
//
// scheduleKernelReplicas returns a <-chan interface{} used to notify the caller when the scheduling operations
// have completed.
func (s *DockerScheduler) scheduleKernelReplicas(in *proto.KernelSpec, hosts []scheduling.Host, blacklistedHosts []scheduling.Host) <-chan *schedulingNotification {
	// Channel to send either notifications that we successfully launched a replica (in the form of a struct{}{})
	// or errors that occurred when launching a replica.
	resultChan := make(chan *schedulingNotification, 3)

	// For each host, launch a Docker replica on that host.
	for i, host := range hosts {
		// Launch replicas in parallel.
		go func(replicaId int32, targetHost scheduling.Host) {
			replicaSpec := &proto.KernelReplicaSpec{
				Kernel:                    in,
				ReplicaId:                 replicaId,
				NumReplicas:               int32(s.opts.NumReplicas),
				DockerModeKernelDebugPort: s.dockerModeKernelDebugPort.Add(1),
			}
			s.log.Debug("Assigned docker mode kernel replica debug port to %d for replica %d of kernel %s.",
				replicaSpec.DockerModeKernelDebugPort, replicaSpec.ReplicaId, in.Id)

			// Only 1 of arguments 2 and 3 can be non-nil.
			var schedulingError error
			if schedulingError = s.ScheduleKernelReplica(replicaSpec, targetHost, blacklistedHosts); schedulingError != nil {
				// An error occurred. Send it over the channel.
				resultChan <- &schedulingNotification{
					SchedulingCompletedAt: time.Now(),
					KernelId:              in.Id,
					ReplicaId:             replicaId,
					Host:                  targetHost,
					Error:                 schedulingError,
					Successful:            false,
				}
			} else {
				// Send a notification that a replica was launched successfully.
				resultChan <- &schedulingNotification{
					SchedulingCompletedAt: time.Now(),
					KernelId:              in.Id,
					ReplicaId:             int32(replicaId),
					Host:                  targetHost,
					Error:                 nil,
					Successful:            true,
				}
			}
		}(int32(i+1), host)
	}

	return resultChan
}

// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *DockerScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec, blacklistedHosts []scheduling.Host) error {
	st := time.Now()

	s.log.Debug("Preparing to search for %d hosts to serve replicas of kernel %s. Resources required: %s.",
		s.opts.NumReplicas, in.Id, in.ResourceSpec.String())

	deadline, ok := ctx.Deadline()
	if ok {
		s.log.Debug("Context (for deploying replicas of kernel %s) has deadline of %v, which is in %v.",
			in.Id, deadline, time.Until(deadline))
	}

	// Retrieve a slice of viable Hosts onto which we can schedule replicas of the specified kernel.
	hosts, candidateError := s.GetCandidateHosts(ctx, in)
	if candidateError != nil {
		return candidateError
	}

	// Schedule a replica of the kernel on each of the candidate hosts.
	resultChan := s.scheduleKernelReplicas(in, hosts, blacklistedHosts)

	// Keep looping until we've received all responses or the context times-out.
	responsesReceived := make([]*schedulingNotification, 0, len(hosts))
	responsesRequired := len(hosts)
	for len(responsesReceived) < responsesRequired {
		select {
		// Context time-out, meaning the operation itself has timed-out or been cancelled.
		// TODO: We ultimately need to handle this somehow, as we'll have allocated Resources to the kernel replicas
		// 		 on the hosts that we selected. If this operation fails or times-out, then we need to potentially
		//		 terminate the replicas that we know were scheduled successfully and release the Resources on those hosts.
		case <-ctx.Done():
			{
				err := ctx.Err()
				if err != nil {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. "+
						"Error extracted from now-cancelled context: %v", in.Id, err)
				} else {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. "+
						"No error extracted from now-cancelled context.", in.Id, err)
					err = types.ErrRequestTimedOut // Return generic error if we can't get one from the Context for some reason.
				}

				// Write out the host/replica IDs that we scheduled like:
				// "0"
				// "0 and 1"
				// "0, 1, and 2"
				// "0, 1, 2, and 3"
				// etc.
				var replicasScheduledBuilder strings.Builder
				var hostsWithOrphanedReplicaBuilder strings.Builder

				replicasScheduled := make([]int32, 0, len(responsesReceived))
				hostsWithOrphanedReplica := make([]string, 0, len(responsesReceived))

				for idx, notification := range responsesReceived {
					// If we're writing the ID of the last replica, and we scheduled at least 2, then we'll
					// prepend the replica ID with "and " so that the final string is like "0 and 1" or "0, 1, and 2".
					if len(responsesReceived) > 1 && (idx+1) == len(responsesReceived) {
						replicasScheduledBuilder.WriteString("and ")
						hostsWithOrphanedReplicaBuilder.WriteString("and ")
					}

					replicasScheduled = append(replicasScheduled, notification.ReplicaId)
					hostsWithOrphanedReplica = append(hostsWithOrphanedReplica, notification.Host.GetID())

					// Write the replica ID.
					replicasScheduledBuilder.WriteString(fmt.Sprintf("%d", notification.ReplicaId))
					// Write the host ID.
					hostsWithOrphanedReplicaBuilder.WriteString(fmt.Sprintf("Host %s", notification.Host.GetID()))

					// If this is not the last replica ID that we'll be writing out...
					if (idx + 1) < len(responsesReceived) {
						// If we just scheduled two replicas, then just add a space. We'll add the "and " on the
						// final iteration of the loop, so the resulting string will look like "0 and 1".
						if len(responsesReceived) == 2 {
							replicasScheduledBuilder.WriteString(" ")
							hostsWithOrphanedReplicaBuilder.WriteString(" ")
						} else {
							// We scheduled more than 2 replicas (which means the total number of replicas is > 3,
							// since we otherwise would have succeeded, so this is unlikely as we usually use 3
							// replicas, but nevertheless)...
							//
							// ... so add a comma so that the string is of the form "0, 1, and 2".
							replicasScheduledBuilder.WriteString(", ")
							hostsWithOrphanedReplicaBuilder.WriteString(", ")
						}
					}
				}

				// TODO: kill orphaned replicas so that they don't just sit there, taking up Resources unnecessarily.
				if len(responsesReceived) > 0 {
					s.log.Error("Scheduling of kernel %s has failed after %v. Only managed to schedule replicas %s (%d/%d).",
						in.Id, time.Since(st), replicasScheduledBuilder.String(), len(responsesReceived), responsesRequired)

					s.log.Error("As such, the following Hosts have orphaned replicas of kernel %s scheduled onto them: %s",
						in.Id, hostsWithOrphanedReplicaBuilder.String())

					// TODO: Kill orphaned replicas.
				} else {
					s.log.Error("Scheduling of kernel %s has failed after %v. Failed to schedule any of the %d replicas.",
						in.Id, time.Since(st), responsesRequired)
				}

				return &scheduling.ErrorDuringScheduling{
					UnderlyingError:           err,
					HostsWithOrphanedReplicas: hostsWithOrphanedReplica,
					ScheduledReplicaIDs:       replicasScheduled,
				}
			}
		// Received response.
		case notification := <-resultChan:
			{
				if !notification.Successful {
					s.log.Error("Error while launching at least one of the replicas of kernel %s: %v", in.Id, notification.Error)
					return notification.Error
				}

				responsesReceived = append(responsesReceived, notification)
				s.log.Debug("Successfully scheduled replica %d of kernel %s on host %s. %d/%d replicas scheduled. Time elapsed: %v.",
					notification.ReplicaId, in.Id, notification.Host.GetID(), len(responsesReceived), responsesRequired, time.Since(st))
			}
		}
	}

	s.log.Debug("Successfully scheduled all %d replica(s) of kernel %s in %v.",
		s.opts.NumReplicas, in.Id, time.Since(st))

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
		timeSinceLastSync := time.Since(lastSync)

		// ts is the time exactly "a quarter of the interval since the last synchronization" ago.
		// So, if we last synchronized 16 min ago, then ts is equal to whatever time it was 4 min ago.
		ts := time.Now().Add(-1 * time.Duration(float64(timeSinceLastSync)*0.25))

		hosts := make([]scheduling.Host, 0, s.cluster.Len())
		s.cluster.RangeOverHosts(func(_ string, host scheduling.Host) (contd bool) {
			// If we've not synchronized this host within the last <interval of time since last sync> / 4,
			// then we'll synchronize it again now.
			//
			// So, for example, if the last round of synchronizations was 16 minutes ago, then we'll synchronize
			// this Host now as long as we've not done so within the last 4 minutes.
			if host.GetLastRemoteSync().Before(ts) {
				hosts = append(hosts, host)
			}
			return true
		})
		s.cluster.ReadUnlockHosts()

		for _, host := range hosts {
			hostId := host.GetID()
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

				s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s (consecutive: %d): %v",
					hostId, host.GetNodeName(), numConsecutiveFailures, err)

				// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
				if numConsecutiveFailures >= ConsecutiveFailuresWarning {
					// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
					if host.GetConnectionState() == connectivity.TransientFailure || host.GetConnectionState() == connectivity.Shutdown {
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.",
							numConsecutiveFailures, host.GetID(), host.GetNodeName(), host.GetConnectionState().String(), host.GetID())
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.GetID(), host.GetNodeName(), "Local Daemon Connectivity Error", errorMessage)
					} else if numConsecutiveFailures >= ConsecutiveFailuresBad {
						// If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
						errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.",
							numConsecutiveFailures, host.GetID(), host.GetNodeName(), host.GetConnectionState().String(), host.GetID())
						s.log.Error(errorMessage)
						_ = host.ErrorCallback()(host.GetID(), host.GetNodeName(), "Local Daemon Connectivity Error", errorMessage)
					} else {
						// Otherwise, we won't assume it is dead yet...
						s.log.Warn("Failed %d consecutive times to retrieve GPU info from Local Daemon %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...",
							numConsecutiveFailures, host.GetID(), host.GetNodeName(), host.GetConnectionState().String())
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

// refreshClusterNodes updates the cached list of Host nodes.
// Returns nil on success; returns an error on one or more failures.
// If there are multiple failures, then their associated errors will be joined together via errors.Join(...).
func (s *DockerScheduler) refreshClusterNodes() error {
	s.cluster.ReadLockHosts()
	hosts := make([]scheduling.Host, 0, s.cluster.Len())
	s.cluster.RangeOverHosts(func(_ string, host scheduling.Host) (contd bool) {
		hosts = append(hosts, host)
		return true
	})
	s.cluster.ReadUnlockHosts()

	errs := make([]error, 0)
	for _, host := range hosts {
		hostId := host.GetID()
		err := host.SynchronizeResourceInformation()
		if err != nil {
			s.log.Error("Failed to refresh resource usage information from Local Daemon %s on Node %s: %v",
				hostId, host.GetNodeName(), err)
			errs = append(errs, err)
		}
	}

	if len(errs) > 1 {
		return errors.Join(errs...)
	}

	return nil
}
