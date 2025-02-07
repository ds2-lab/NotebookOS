package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// AssignKernelDebugPorts is a flag that, when true, directs the DockerScheduler to assign "debug ports" to
	// kernel containers that will be passed to their Golang backend to start a net/pprof debug server.
	AssignKernelDebugPorts bool
}

// newDockerScheduler is called internally by "constructors" of other schedulers that "extend" DockerScheduler.
func newDockerScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	hostSpec types.Spec, kernelProvider KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy SchedulingPolicy, opts *scheduling.SchedulerOptions) (*DockerScheduler, error) {
	if cluster == nil {
		panic("Cluster cannot be nil")
	}

	baseScheduler := newBaseSchedulerBuilder().
		WithCluster(cluster).
		WithHostMapper(hostMapper).
		WithPlacer(placer).
		WithHostSpec(hostSpec).
		WithSchedulingPolicy(schedulingPolicy).
		WithKernelProvider(kernelProvider).
		WithNotificationBroker(notificationBroker).
		WithInitialNumContainersPerHost(opts.InitialNumContainersPerHost).
		WithOptions(opts).Build()

	dockerScheduler := &DockerScheduler{
		BaseScheduler:          baseScheduler,
		AssignKernelDebugPorts: opts.AssignKernelDebugPorts,
	}

	dockerScheduler.dockerModeKernelDebugPort.Store(DockerKernelDebugPortDefault)

	return dockerScheduler, nil
}

func NewDockerScheduler(cluster scheduling.Cluster, placer scheduling.Placer, hostMapper HostMapper,
	hostSpec types.Spec, kernelProvider KernelProvider, notificationBroker NotificationBroker,
	schedulingPolicy SchedulingPolicy, opts *scheduling.SchedulerOptions) (*DockerScheduler, error) {

	dockerScheduler, err := newDockerScheduler(cluster, placer, hostMapper, hostSpec, kernelProvider,
		notificationBroker, schedulingPolicy, opts)
	if err != nil {
		return nil, err
	}

	dockerScheduler.BaseScheduler.instance = dockerScheduler

	return dockerScheduler, nil
}

func (s *DockerScheduler) Instance() scheduling.Scheduler {
	return s.BaseScheduler.instance
}

func (s *DockerScheduler) setInstance(instance clusterSchedulerInternal) {
	s.instance = instance
	s.BaseScheduler.instance = instance
	s.BaseScheduler.setInstance(instance)
}

// selectViableHostForReplica is called at scheduling-time (rather than before we get to the point of scheduling, such
// as searching for viable hosts before trying to schedule the container).
//
// selectViableHostForReplica is most often called for kernels that need to begin training immediately.
//
// selectViableHostForReplica searches for a viable host and, if one is found, then that host is returned.
// Otherwise, an error is returned.
func (s *DockerScheduler) selectViableHostForReplica(replicaSpec *proto.KernelReplicaSpec, blacklistedHosts []scheduling.Host, forTraining bool) (scheduling.Host, error) {
	kernelId := replicaSpec.ID()

	blacklist := make([]interface{}, 0)
	for _, blacklistedHost := range blacklistedHosts {
		s.log.Debug("Host %s (ID=%s) of kernel %s-%d was specifically specified as being blacklisted.",
			blacklistedHost.GetNodeName(), blacklistedHost.GetID(), kernelId, replicaSpec.ReplicaId)
		// blacklist = append(blacklist, blacklistedHost.GetMeta(s.placer.GetIndex().GetMetadataKey()))
		blacklist = append(blacklist, blacklistedHost)
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
		blacklist = append(blacklist, host)
	}

	host, err := s.placer.FindHost(blacklist, replicaSpec, forTraining)
	if err != nil {
		s.log.Error("Error while finding host for replica %d of kernel %s: %v",
			replicaSpec.ReplicaId, replicaSpec.Kernel.Id, err)

		return nil, err
	}

	if host == nil {
		return nil, scheduling.ErrInsufficientHostsAvailable
	}

	s.log.Debug("Selected host %s (ID=%s) as target for migration. Will migrate kernel %s-%d to host %s.",
		host.GetNodeName(), host.GetID(), kernelId, replicaSpec.ReplicaId, host.GetNodeName())
	return host, nil
}

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *DockerScheduler) HostAdded(host scheduling.Host) {
	s.log.Debug("Host %s (ID=%s) has been added.", host.GetNodeName(), host.GetID())
	heap.Push(s.idleHosts, &idleSortedHost{
		Host: host,
	})
	s.log.Debug("Length of idle hosts: %d", s.idleHosts.Len())
}

// HostRemoved is called by the Cluster when a Host is removed from the Cluster.
func (s *DockerScheduler) HostRemoved(host scheduling.Host) {
	idleHostIndex := host.GetIdx(IdleHostMetadataKey)
	if idleHostIndex >= 0 {
		s.log.Debug("Host %s (ID=%s) has a valid idle host index (\"%s\") of %d. Removing.",
			host.GetNodeName(), host.GetID(), IdleHostMetadataKey, idleHostIndex)
		heap.Remove(s.idleHosts, host.GetIdx(IdleHostMetadataKey))
	}

	s.log.Debug("Host %s (ID=%s) has been removed.", host.GetNodeName(), host.GetID())
}

// findCandidateHosts is a scheduler-specific implementation for finding candidate hosts for the given kernel.
// DockerScheduler does not do anything special or fancy.
//
// If findCandidateHosts returns nil, rather than an empty slice, then that indicates that an error occurred.
func (s *DockerScheduler) findCandidateHosts(numToFind int, kernelSpec *proto.KernelSpec) ([]scheduling.Host, error) {
	// Identify the hosts onto which we will place replicas of the kernel.
	return s.placer.FindHosts([]interface{}{}, kernelSpec, numToFind, false)
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

	// TODO: We should just be able to remove this.
	// 		 The container should be removed from the Session when we call KernelReplica::RemoveReplicaByID up above.
	// 		 So, this should never be necessary. I'm leaving the code here because I don't want to check it right now.
	session := kernelReplica.Container().Session()
	if _, loaded = session.GetReplicaContainer(kernelReplica.ReplicaID()); loaded {
		s.log.Warn("Session \"%s\" still has replica %d registered. Explicitly removing the replica from the session now.",
			kernelReplica.ID(), kernelReplica.ReplicaID())

		err = kernelReplica.Container().Session().RemoveReplicaById(kernelReplica.ReplicaID())
		if err != nil {
			s.log.Warn("Got an error when trying to explicitly remove replica %d from session \"%s\" because: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)

			// If the error is something other than scheduling.ErrReplicaNotFound, then we'll return the error so that
			// whatever is going on fails, as something went wrong here.
			//
			// Even if we did get back a scheduling.ErrReplicaNotFound here -- that's bad. Because we shouldn't have
			// tried to remove the container replica again here in the first place. The container is supposed to be
			// removed from the Session when we call KernelReplica::RemoveReplicaByID up above.
			if !errors.Is(err, scheduling.ErrReplicaNotFound) {
				return err
			}
		}
	}

	s.log.Debug("Successfully removed replica %d of kernel %s.", kernelReplica.ReplicaID(), kernelReplica.ID())

	return nil
}

// ScheduleKernelReplica schedules a particular replica onto the given Host.
//
// If targetHost is nil, then a candidate Host is identified automatically by the Scheduler.
func (s *DockerScheduler) ScheduleKernelReplica(replicaSpec *proto.KernelReplicaSpec, targetHost scheduling.Host, blacklistedHosts []scheduling.Host, forTraining bool) (err error) {
	kernelId := replicaSpec.Kernel.Id // We'll use this a lot.

	if targetHost == nil {
		s.log.Debug("No target host specified when scheduling replica %d of kernel %s. Searching for one now...",
			replicaSpec.ReplicaId, kernelId)

		targetHost, err = s.selectViableHostForReplica(replicaSpec, blacklistedHosts, forTraining)
		if err != nil {
			s.log.Warn("Could not find viable targetHost for replica %d of kernel %s: %v",
				replicaSpec.ReplicaId, kernelId, err)
			return err
		}

		s.log.Debug("Found viable target host for replica %d of kernel %s at scheduling time: host %s",
			replicaSpec.ReplicaId, kernelId, targetHost.GetID())
	}

	// Check if we're supposed to assign "debug ports" to kernels.
	// If so, then we'll check if one has already been assigned (somehow), and if not, then we'll assign one.
	//
	// If we're NOT supposed to assign "debug ports", then we'll explicitly assign -1, which will
	// ensure that the Local Daemon that creates the kernel container does not bind a port for this purpose.
	if !s.AssignKernelDebugPorts {
		replicaSpec.DockerModeKernelDebugPort = -1
	} else if replicaSpec.DockerModeKernelDebugPort <= 1023 {
		// Make sure to assign a value to DockerModeKernelDebugPort if one is not already set.
		replicaSpec.DockerModeKernelDebugPort = s.dockerModeKernelDebugPort.Add(1)

		s.log.Debug("Assigned docker mode kernel replica debug port to %d for replica %d of kernel %s.",
			replicaSpec.DockerModeKernelDebugPort, replicaSpec.ReplicaId, kernelId)
	}

	container, unavailErr := s.prewarmer.RequestPrewarmedContainer(targetHost)
	if container != nil {
		s.log.Debug("Found pre-warmed container on host %s. Using for replica %d of kernel %s.",
			targetHost.GetID(), replicaSpec.ReplicaId, kernelId)

		err = s.scheduleKernelReplicaPrewarm(replicaSpec, container, targetHost)

		if err == nil {
			return nil
		}

		if errors.Is(err, prewarm.ErrPrewarmedContainerAlreadyUsed) {
			s.log.Error("Will use on-demand container for replica %d of kernel \"%s\" since pre-warmed container was already used...",
				replicaSpec.ReplicaId, kernelId)
			return s.scheduleKernelReplicaOnDemand(replicaSpec, targetHost)
		}

		return err
	}

	s.log.Debug("No pre-warmed containers available on host %s: %v.", targetHost.GetNodeName(), unavailErr)
	return s.scheduleKernelReplicaOnDemand(replicaSpec, targetHost)
}

// scheduleKernelReplicaPrewarm creates a new scheduling.KernelReplica using an existing, pre-warmed scheduling.PrewarmedContainer
// that is available on the specified scheduling.Host.
func (s *DockerScheduler) scheduleKernelReplicaPrewarm(replicaSpec *proto.KernelReplicaSpec,
	container scheduling.PrewarmedContainer, targetHost scheduling.Host) error {

	// Validate argument.
	if replicaSpec == nil {
		panic("Invalid arguments to scheduling kernel replica prewarm (replicaSpec is nil).")
	}

	// Validate argument.
	if container == nil {
		panic("Invalid arguments to scheduling kernel replica prewarm (container is nil).")
	}

	// Validate argument.
	if targetHost == nil {
		panic("Invalid arguments to scheduling kernel replica prewarm (targetHost is nil).")
	}

	// Validate that the target host matches the pre-warmed container's host.
	if container.Host() != targetHost {
		panic("Invalid arguments to scheduling kernel replica prewarm (container.Host() != targetHost).")
	}

	if !container.IsAvailable() {
		s.log.Error("Pre-warmed container \"%s\" has already been used...", container.ID())
		return prewarm.ErrPrewarmedContainerAlreadyUsed
	}

	s.log.Debug("Launching replica %d of kernel %s in pre-warmed container \"%s\" on targetHost %s (ID=%s) now.",
		replicaSpec.ReplicaId, replicaSpec.Kernel.Id, container.ID(), targetHost.GetNodeName(), targetHost.GetID())

	// We'll wait up to 5 minutes for the operation to complete.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	spec := &proto.PrewarmedKernelReplicaSpec{
		KernelReplicaSpec:    replicaSpec,
		PrewarmedContainerId: container.ID(),
	}

	replicaConnInfo, err := targetHost.PromotePrewarmedContainer(ctx, spec)
	if err != nil {
		s.log.Warn("Failed to start replica %d of kernel %s using pre-warmed container %s on host %s (ID=%s): %v",
			replicaSpec.ReplicaId, replicaSpec.Kernel.Id, container.ID(), targetHost.GetNodeName(), targetHost.GetID(), err)
		return err
	}

	s.log.Debug("Successfully created replica %d of kernel %s in pre-warmed container on host %s (ID=%s): %v",
		replicaSpec.ReplicaId, replicaSpec.Kernel.Id, targetHost.GetNodeName(), targetHost.GetID(), replicaConnInfo)
	return nil
}

// scheduleKernelReplicaOnDemand uses the scheduling.Placer to create a new scheduling.KernelContainer on the specified
// scheduling.Host for the specified scheduling.KernelReplica.
func (s *DockerScheduler) scheduleKernelReplicaOnDemand(replicaSpec *proto.KernelReplicaSpec, targetHost scheduling.Host) error {
	s.log.Debug("Launching replica %d of kernel %s in on-demand container on targetHost %s (ID=%s) now.",
		replicaSpec.ReplicaId, replicaSpec.Kernel.Id, targetHost.GetNodeName(), targetHost.GetID())

	replicaConnInfo, err := s.placer.Place(targetHost, replicaSpec)
	if err != nil {
		s.log.Warn("Failed to start replica %d of kernel %s using on-demand container %s on host %s (ID=%s): %v",
			replicaSpec.ReplicaId, replicaSpec.Kernel.Id, targetHost.GetNodeName(), targetHost.GetID(), err)
		return err
	}

	s.log.Debug("Successfully placed on-demand container for replica %d of kernel %s on host %s (ID=%s): %v",
		replicaSpec.ReplicaId, replicaSpec.Kernel.Id, targetHost.GetNodeName(), targetHost.GetID(), replicaConnInfo)
	return nil
}

// scheduleKernelReplicas schedules a replica of the specified kernel on each Host within the given slice of scheduling.Host.
// Specifically, scheduleKernelReplicas calls ScheduleKernelReplica for each of the Host instances within the given
// slice of Hosts in a separate goroutine, thereby scheduling a replica of the given kernel on the Host. That is, the
// scheduling of a replica of the kernel occurs in a unique goroutine for each of the specified Host instances.
//
// scheduleKernelReplicas returns a <-chan interface{} used to notify the caller when the scheduling operations
// have completed.
func (s *DockerScheduler) scheduleKernelReplicas(in *proto.KernelSpec, hosts []scheduling.Host, blacklistedHosts []scheduling.Host, forTraining bool) <-chan *schedulingNotification {
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
				NumReplicas:               int32(s.schedulingPolicy.NumReplicas()),
				DockerModeKernelDebugPort: s.dockerModeKernelDebugPort.Add(1),
				WorkloadId:                in.WorkloadId,
			}
			s.log.Debug("Assigned docker mode kernel replica debug port to %d for replica %d of kernel %s.",
				replicaSpec.DockerModeKernelDebugPort, replicaSpec.ReplicaId, in.Id)

			var schedulingError error
			if schedulingError = s.ScheduleKernelReplica(replicaSpec, targetHost, blacklistedHosts, forTraining); schedulingError != nil {
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
					ReplicaId:             replicaId,
					Host:                  targetHost,
					Error:                 nil,
					Successful:            true,
				}
			}
		}(int32(i+1), host)
	}

	return resultChan
}

// removeOrphanedReplicas is called if there's an error deploying one of the replicas of the specified scheduling.Kernel.
// If there are other replicas that were created successfully, then they're orphaned in the sense that this scheduling
// operation failed, and so they need to be removed.
//
// PRECONDITION: removeOrphanedReplicas expects the given context.Context to have a value with key "start_time".
func (s *DockerScheduler) removeOrphanedReplicas(ctx context.Context, kernel scheduling.Kernel, responsesReceived []*schedulingNotification) error {
	startTime := ctx.Value("start_time").(time.Time)

	// Write out the host/replica IDs that we scheduled like:
	// "0"
	// "0 and 1"
	// "0, 1, and 2"
	// "0, 1, 2, and 3"
	// etc.
	var replicasScheduledBuilder strings.Builder
	var hostsWithOrphanedReplicaBuilder strings.Builder

	replicasScheduled := make([]int32, 0, len(responsesReceived))
	idsOfHostsWithOrphanedReplica := make([]string, 0, len(responsesReceived))
	hostsWithOrphanedReplica := make([]scheduling.Host, 0, len(responsesReceived))

	// Build out an error message indicating the hosts with orphaned replicas and whatnot.
	for idx, notification := range responsesReceived {
		// If we're writing the ID of the last replica, and we scheduled at least 2, then we'll
		// prepend the replica ID with "and " so that the final string is like "0 and 1" or "0, 1, and 2".
		if len(responsesReceived) > 1 && (idx+1) == len(responsesReceived) {
			replicasScheduledBuilder.WriteString("and ")
			hostsWithOrphanedReplicaBuilder.WriteString("and ")
		}

		hostsWithOrphanedReplica = append(hostsWithOrphanedReplica, notification.Host)

		replicasScheduled = append(replicasScheduled, notification.ReplicaId)
		idsOfHostsWithOrphanedReplica = append(idsOfHostsWithOrphanedReplica, notification.Host.GetID())

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

	s.log.Error("Scheduling of kernel %s has failed after %v. Only managed to schedule replicas %s (%d/%d).",
		kernel.ID(), time.Since(startTime), replicasScheduledBuilder.String(), len(responsesReceived), s.Policy().NumReplicas())

	s.log.Error("As such, the following Hosts have orphaned replicas of kernel %s scheduled onto them: %s",
		kernel.ID(), hostsWithOrphanedReplicaBuilder.String())

	notifyChan := make(chan interface{}, len(hostsWithOrphanedReplica))
	numResponsesReceived := atomic.Int32{}

	type wrappedResult struct {
		Error error
		Host  scheduling.Host
	}

	for _, host := range hostsWithOrphanedReplica {
		go func(targetHost scheduling.Host) {
			err := s.placer.Reclaim(targetHost, kernel.GetSession(), false)

			res := &wrappedResult{
				Error: err,
				Host:  host,
			}

			notifyChan <- res

			numResponsesReceived.Add(1)
		}(host)
	}

	for numResponsesReceived.Load() < int32(len(hostsWithOrphanedReplica)) {
		select {
		case <-ctx.Done():
			{
				s.log.Error("Context cancelled while removing %d orphaned replica(s) of kernel \"%s\".",
					len(hostsWithOrphanedReplica), kernel.ID())

				if err := ctx.Err(); err != nil {
					return fmt.Errorf("error occurred while removing %d orphaned replica(s) of kernel \"%s\": %w",
						len(hostsWithOrphanedReplica), kernel.ID(), err)
				}

				return fmt.Errorf("context cancelled occurred while removing %d orphaned replica(s) of kernel \"%s\"",
					len(hostsWithOrphanedReplica), kernel.ID())
			}
		case v := <-notifyChan:
			{
				res := v.(*wrappedResult)

				if res.Error != nil {
					s.log.Error("Failed to remove orphaned replica of kernel \"%s\" from host \"%s\": %v",
						kernel.ID(), res.Host.GetNodeName(), res.Error)

					return fmt.Errorf("failed to remove orphaned replica of kernel \"%s\" from host \"%s\": %w",
						kernel.ID(), res.Host.GetNodeName(), res.Error)
				}

				s.log.Debug("Successfully removed orphaned replica of kernel \"%s\" from host \"%s\".",
					kernel.ID(), res.Host.GetNodeName())
			}
		}
	}

	return nil
}

// DeployKernelReplicas is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *DockerScheduler) DeployKernelReplicas(ctx context.Context, kernel scheduling.Kernel, blacklistedHosts []scheduling.Host) error {
	st := time.Now()

	kernelSpec := kernel.KernelSpec()

	s.log.Debug("Preparing to search for %d hosts to serve replicas of kernel %s. TransactionResources required: %s.",
		s.schedulingPolicy.NumReplicas(), kernel.ID(), kernelSpec.String())

	deadline, ok := ctx.Deadline()
	if ok {
		s.log.Debug("Context (for deploying replicas of kernel %s) has deadline of %v, which is in %v.",
			kernel.ID(), deadline, time.Until(deadline))
	}

	// Retrieve a slice of viable Hosts onto which we can schedule replicas of the specified kernel.
	hosts, candidateError := s.GetCandidateHosts(ctx, kernelSpec)
	if candidateError != nil {
		return candidateError
	}

	// Take note that we're starting to place the kernel replicas now.
	kernel.RecordContainerPlacementStarted()

	// Schedule a replica of the kernel on each of the candidate hosts.
	resultChan := s.scheduleKernelReplicas(kernelSpec, hosts, blacklistedHosts, false)

	// Keep looping until we've received all responses or the context times-out.
	responsesReceived := make([]*schedulingNotification, 0, len(hosts))
	responsesRequired := len(hosts)
	for len(responsesReceived) < responsesRequired {
		select {
		// Context time-out, meaning the operation itself has timed-out or been cancelled.
		case <-ctx.Done():
			{
				err := ctx.Err()
				if err != nil {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. "+
						"Error extracted from now-cancelled context: %v", kernel.ID(), err)
				} else {
					s.log.Error("Context cancelled while waiting for new Docker replicas to register for kernel %s. "+
						"No error extracted from now-cancelled context.", kernel.ID(), err)
					err = types.ErrRequestTimedOut // Return generic error if we can't get one from the Context for some reason.
				}

				// If we received no responses, then no replicas were scheduled (apparently), so we can just return.
				if len(responsesReceived) == 0 {
					s.log.Error("Scheduling of kernel %s has failed after %v. Failed to schedule any of the %d replicas.",
						kernel.ID(), time.Since(st), responsesRequired)

					return status.Error(codes.Internal, (&scheduling.ErrorDuringScheduling{
						UnderlyingError:           err,
						HostsWithOrphanedReplicas: []string{},
						ScheduledReplicaIDs:       []int32{},
					}).Error())
				}

				// We'll need to remove any replicas that were successfully scheduled.
				return s.removeOrphanedReplicas(context.WithValue(ctx, "start_time", st), kernel, responsesReceived)
			}
		// Received response.
		case notification := <-resultChan:
			{
				if !notification.Successful {
					s.log.Error("Error while launching at least one of the replicas of kernel %s: %v",
						kernel.ID(), notification.Error)
					return notification.Error
				}

				responsesReceived = append(responsesReceived, notification)
				s.log.Debug("Successfully scheduled replica %d of kernel %s on host %s (ID=%s). %d/%d replicas scheduled. Time elapsed: %v.",
					notification.ReplicaId, kernel.ID(), notification.Host.GetNodeName(), notification.Host.GetID(), len(responsesReceived),
					responsesRequired, time.Since(st))
			}
		}
	}

	s.log.Debug("Successfully scheduled all %d replica(s) of kernel %s in %v.",
		s.schedulingPolicy.NumReplicas(), kernel.ID(), time.Since(st))

	return nil
}
