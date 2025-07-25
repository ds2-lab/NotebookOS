package cluster

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"math/rand"
	"strings"
	"time"
)

type DockerClusterBuilder struct {
	hostSpec                  types.Spec
	placer                    scheduling.Placer
	hostMapper                scheduling.HostMapper
	kernelProvider            scheduling.KernelProvider
	clusterMetricsProvider    scheduling.MetricsProvider
	notificationBroker        scheduler.NotificationBroker
	schedulingPolicy          scheduling.Policy
	statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics))
	opts                      *scheduling.SchedulerOptions
}

func (b *DockerClusterBuilder) WithHostSpec(hostSpec types.Spec) *DockerClusterBuilder {
	b.hostSpec = hostSpec
	return b
}

func (b *DockerClusterBuilder) WithPlacer(placer scheduling.Placer) *DockerClusterBuilder {
	b.placer = placer
	return b
}

func (b *DockerClusterBuilder) WithHostMapper(hostMapper scheduling.HostMapper) *DockerClusterBuilder {
	b.hostMapper = hostMapper
	return b
}

func (b *DockerClusterBuilder) WithKernelProvider(kernelProvider scheduling.KernelProvider) *DockerClusterBuilder {
	b.kernelProvider = kernelProvider
	return b
}

func (b *DockerClusterBuilder) WithClusterMetricsProvider(clusterMetricsProvider scheduling.MetricsProvider) *DockerClusterBuilder {
	b.clusterMetricsProvider = clusterMetricsProvider
	return b
}

func (b *DockerClusterBuilder) WithNotificationBroker(notificationBroker scheduler.NotificationBroker) *DockerClusterBuilder {
	b.notificationBroker = notificationBroker
	return b
}

func (b *DockerClusterBuilder) WithSchedulingPolicy(schedulingPolicy scheduling.Policy) *DockerClusterBuilder {
	b.schedulingPolicy = schedulingPolicy
	return b
}

func (b *DockerClusterBuilder) WithStatisticsUpdaterProvider(statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics))) *DockerClusterBuilder {
	b.statisticsUpdaterProvider = statisticsUpdaterProvider
	return b
}

func (b *DockerClusterBuilder) WithOpts(opts *scheduling.SchedulerOptions) *DockerClusterBuilder {
	b.opts = opts
	return b
}

func (b *DockerClusterBuilder) Build() *DockerCluster {
	baseCluster := newBaseCluster(b.opts, b.placer, b.clusterMetricsProvider, "DockerCluster", b.statisticsUpdaterProvider)

	dockerCluster := &DockerCluster{
		BaseCluster: baseCluster,
	}

	dockerCluster.scheduler = scheduler.GetDockerScheduler(dockerCluster, b.placer, b.hostMapper, b.hostSpec,
		b.kernelProvider, b.notificationBroker, b.schedulingPolicy, b.opts)
	baseCluster.instance = dockerCluster
	baseCluster.initRatioUpdater()

	return dockerCluster
}

// DockerCluster encapsulates the logic for a Docker compose Cluster, in which the nodes are simulated
// locally, and scaling-up and down sometimes involves simulation steps in which nodes are not actually deleted,
// but simply toggled "off" and "on".
type DockerCluster struct {
	*BaseCluster
}

// NewDockerCluster creates a new DockerCluster struct and returns a pointer to it.
//
// NewDockerCluster should be used when the system is deployed in Docker mode (either compose or swarm, for now).
// This function accepts parameters that are used to construct a DockerScheduler to be used internally
// by the Cluster for scheduling decisions.
func NewDockerCluster(hostSpec types.Spec, placer scheduling.Placer, hostMapper scheduling.HostMapper, kernelProvider scheduling.KernelProvider,
	clusterMetricsProvider scheduling.MetricsProvider, notificationBroker scheduler.NotificationBroker,
	schedulingPolicy scheduling.Policy, statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics)),
	opts *scheduling.SchedulerOptions) *DockerCluster {

	baseCluster := newBaseCluster(opts, placer, clusterMetricsProvider, "DockerCluster", statisticsUpdaterProvider)

	dockerCluster := &DockerCluster{
		BaseCluster: baseCluster,
	}

	dockerCluster.scheduler = scheduler.GetDockerScheduler(dockerCluster, placer, hostMapper, hostSpec,
		kernelProvider, notificationBroker, schedulingPolicy, opts)
	baseCluster.instance = dockerCluster
	baseCluster.initRatioUpdater()

	return dockerCluster
}

func (c *DockerCluster) String() string {
	return fmt.Sprintf("DockerCluster[Size=%d,NumSessions=%d]", c.Len(), c.sessions.Len())
}

// CanPossiblyScaleOut returns true if the Cluster could possibly scale-out.
// For now, we scale-out using disabled nodes, so whether we can scale-out depends
// upon whether there is at least one disabled node.
func (c *DockerCluster) CanPossiblyScaleOut() bool {
	return c.DisabledHosts.Len() > 0
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *DockerCluster) NodeType() string {
	return types.DockerNode
}

// unsafeDisableHost disables an active host.
//
// If the host does not exist or is not already disabled, then an error is returned.
//
// Important: this should be called with the DockerCluster's hostMutex already acquired.
func (c *DockerCluster) unsafeDisableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, loaded := c.hosts.Load(id)
	if !loaded {
		// Let's check if the host even exists.
		_, exists := c.DisabledHosts.Load(id)
		if exists {
			return fmt.Errorf("%w: host \"%s\" is already disabled", scheduling.ErrInvalidHost, id)
		} else {
			return fmt.Errorf("%w: host \"%s\" does not exist (in any capacity)", scheduling.ErrInvalidHost, id)
		}
	}

	if host.NumContainers() > 0 {
		return fmt.Errorf("%w: host \"%s\" is hosting at least one kernel replica, and automated migrations are not yet implemented",
			scheduling.ErrInvalidHost, id)
	}

	c.log.Debug("Disabling host with ID=\"%s\" now...", id)
	if err := host.Disable(); err != nil {
		// This really shouldn't happen.
		// This would mean that the host was in an inconsistent state relative to the Cluster,
		// as the host was stored in the wrong map.
		panic(err)
	}
	c.DisabledHosts.Store(id, host)
	c.hosts.Delete(id)

	c.onHostRemoved(host)

	if c.metricsProvider != nil && c.metricsProvider.GetNumDisabledHostsGauge() != nil {
		c.metricsProvider.GetNumDisabledHostsGauge().Add(1)
	}

	return nil
}

// unsafeEnableHost enables a disabled host.
//
// If the host does not exist or is not disabled, then an error is returned.
//
// Important: this should be called with the DockerCluster's hostMutex already acquired.
func (c *DockerCluster) unsafeEnableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	disabledHost, loaded := c.DisabledHosts.LoadAndDelete(id)
	if !loaded {
		// Let's check if the host even exists.
		_, exists := c.hosts.Load(id)
		if exists {
			return fmt.Errorf("%w: host \"%s\" is not disabled", scheduling.ErrInvalidHost, id)
		} else {
			return fmt.Errorf("%w: host \"%s\" does not exist (in any capacity)", scheduling.ErrInvalidHost, id)
		}
	}

	c.log.Debug("Enabling host %s now...", id)
	if err := disabledHost.Enable(true); err != nil {
		// This really shouldn't happen.
		// This would mean that the host was in an inconsistent state relative to the Cluster,
		// as the host was stored in the wrong map.
		panic(err)
	}
	c.hosts.Store(id, disabledHost)

	if c.metricsProvider != nil && c.metricsProvider.GetNumDisabledHostsGauge() != nil {
		c.metricsProvider.GetNumDisabledHostsGauge().Sub(1)
	}

	return nil
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
//
// Important: this should be called with the Cluster's scalingMutex already acquired.
func (c *DockerCluster) GetScaleOutCommand(targetScale int32, doneChan chan interface{}, scaleOpId string) func() {
	return func() {
		// Record the current scale, before the scale-out operation is executed.
		initialScale := c.Len()

		// The number of disabled hosts we'll need in order to fully satisfy the scale-out request/operation.
		numHostsRequired := targetScale - int32(initialScale)

		c.log.Debug("Scaling out to %d nodes. CurrentSize: %d. #NewNodesRequired: %d. #DisabledNodes: %d. ScaleOpId: %s.",
			targetScale, c.Len(), numHostsRequired, c.DisabledHosts.Len(), scaleOpId)

		// If we have no disabled hosts, then we can just return.
		if c.DisabledHosts.Len() == 0 {
			c.log.Warn("❗ Cannot scale-out from %d → %d nodes as there are no disabled hosts.",
				initialScale, targetScale)

			doneChan <- fmt.Errorf("%w: adding additional nodes is not supported by Docker clusters",
				scheduling.ErrUnsupportedOperation)
			return
		}

		// Identify the disabled hosts that we can enable as part of the scale-out operation as well as the
		// duration that we should sleep to simulate the scale-out operation.
		hostsToEnable, scaleOutDuration := c.unsafeIdentifyDisabledHostsForScaleOut(scaleOpId, numHostsRequired)

		// If we found one or more hosts to enable, then let's enable them.
		if len(hostsToEnable) > 0 {
			c.log.Debug("Found %d disabled host(s) to use in scale-out operation from %d → %d (ID=%s).",
				len(hostsToEnable), targetScale, numHostsRequired, scaleOpId)

			// We found one or more hosts to enable.
			// Let's enable them now.
			c.unsafeEnableDisabledHostsForScaleOut(hostsToEnable, scaleOpId, scaleOutDuration)
			numHostsRequired -= int32(len(hostsToEnable))
		}

		// Check if we satisfied the scale-out request using disabled nodes, in which case we do not
		// need to execute a Docker CLI command and can just return immediately.
		if numHostsRequired == 0 {
			// Note that initialScale should be outdated at this point, but its old/outdated
			// value can be used to calculate how many disabled hosts we must have used
			// in order to satisfy the scale-out request.
			c.log.Debug(
				utils.LightGreenStyle.Render(
					"✓ Satisfied scale-out request from %d → %d nodes using %d disabled nodes."),
				initialScale, targetScale, len(hostsToEnable))
			doneChan <- struct{}{}
			return
		}

		c.log.Warn(
			utils.YellowStyle.Render(
				"❗ Could not satisfy scale-out request to %d nodes using disabled nodes."), targetScale)
		c.log.Warn("Used %d disabled host(s). Still need %d additional host(s) to satisfy request.",
			len(hostsToEnable), targetScale-int32(initialScale))
		doneChan <- fmt.Errorf("%w: adding additional nodes is not supported by Docker clusters",
			scheduling.ErrUnsupportedOperation)
	}
}

// unsafeIdentifyDisabledHostsForScaleOut is used to identify the specified number of disabled hosts to be enabled
// as part of a scale-out operation.
//
// unsafeIdentifyDisabledHostsForScaleOut is intended to be only called by GetScaleOutCommand.
func (c *DockerCluster) unsafeIdentifyDisabledHostsForScaleOut(scaleOpId string, numHostsRequired int32) ([]scheduling.Host, time.Duration) {
	// Keep track of how many more nodes we need (to scale out to) in order to reach the target scale of the
	// scale-out operation.
	remainingNumNodesRequired := numHostsRequired

	// This is the largest scale-out interval generated for any host involved in the scale-out operation.
	//
	// We're simulating the simultaneous provisioning of these hosts, so we'll just sleep for the longest
	// scale-out interval generated for any of the hosts.
	scaleOutDuration := time.Duration(0)

	// These are the currently-disabled hosts that we'll enable as part of the simulated scale-out operation.
	hostsToEnable := make([]scheduling.Host, 0)

	// First, check if we have any disabled nodes. If we do, then we'll just re-enable them.
	c.DisabledHosts.Range(func(hostId string, host scheduling.Host) (contd bool) {
		c.log.Debug("Using disabled host %s (ID=%s) in scale-out operation %s.",
			host.GetNodeName(), hostId, scaleOpId)

		// Generate a scale-out interval for this host.
		// If it is larger than the largest we've found, then we'll use that interval.
		duration := time.Duration(rand.NormFloat64()*float64(c.StdDevScaleOutPerHost)) + c.MeanScaleOutPerHost
		if duration > scaleOutDuration {
			scaleOutDuration = duration
		}

		// AddHost the host to the slice of hosts to be enabled.
		hostsToEnable = append(hostsToEnable, host)

		// Update these counters to keep track of how many hosts we need.
		remainingNumNodesRequired -= 1

		// If we have already satisfied the scale-out requirement, then we'll stop iterating.
		// So, return true (i.e., continue iterating) if remainingNumNodesRequired > 0.
		// Otherwise, return false (i.e., stop iterating).
		return remainingNumNodesRequired > 0
	})

	return hostsToEnable, scaleOutDuration
}

// unsafeEnableDisabledHostsForScaleOut enables any currently-disabled scheduling.Host instances as part of a
// scale-out operation.
//
// unsafeEnableDisabledHostsForScaleOut is intended to be only called by GetScaleOutCommand.
func (c *DockerCluster) unsafeEnableDisabledHostsForScaleOut(hostsToEnable []scheduling.Host, scaleOpId string, scaleOutDuration time.Duration) {
	c.log.Debug("Will remove %d host(s) from DisabledHosts after simulating scale-out operation %s",
		len(hostsToEnable), scaleOpId)

	c.log.Debug("Simulating scale-out for %d hosts during operation %s: %v",
		len(hostsToEnable), scaleOpId, scaleOutDuration)

	// Simulate the scale-out.
	time.Sleep(scaleOutDuration)

	// RemoveHost all the previously-disabled hosts (that we used in the scale-out operation) from the
	// "disabled hosts" mapping.
	for _, host := range hostsToEnable {
		// First, enable the host.
		err := host.Enable(true)
		if err != nil {
			c.log.Error("Failed to re-enable host %s (ID=%s) during scale-out %s because: %v",
				host.GetNodeName(), host.GetID(), scaleOpId, err)

			// For now, we panic, as we don't expect there to be a "valid" reason to fail to enable a host.
			// Later on, we may find that there are valid reasons, in which case we'd just handle the
			// error in whatever way is appropriate, such as by skipping this host and trying the next one.
			panic(err)
		}

		// Next, remove the host from the DisabledHosts mapping.
		_, loaded := c.DisabledHosts.LoadAndDelete(host.GetID())
		if !loaded {
			c.log.Error("Failed to find host %s in DisabledHosts map after using it in scale-out operation.",
				host.GetID())

			// Let's check if the host is in the regular hosts mapping... if so, then that's really bizarre.
			if _, loaded = c.hosts.Load(host.GetID()); loaded {
				errorMessage := fmt.Sprintf(
					"Found host %s (ID=%s) in standard 'hosts' mapping when it should have been in 'disabled hosts' mapping...",
					host.GetNodeName(), host.GetID())
				c.log.Error(errorMessage)
				panic(errorMessage)
			}
		} else {
			c.log.Debug("Removed host %s (ID=%s) from DisabledHosts", host.GetNodeName(), host.GetID())
		}

		// This will add the host back to the Cluster.
		err = c.NewHostAddedOrConnected(host)
		if err != nil {
			c.log.Error("Error adding previously-disabled host %s (ID=%s) to cluster during scale-out %s: %v",
				host.GetNodeName(), host.GetID(), scaleOpId, err)
		}
	}
}

// unsafeGetTargetedScaleInCommand returns a function that, when executed, will terminate the hosts specified in the targetHosts parameter.
//
// Important: this should be called with the Cluster's hostMutex already acquired.
func (c *DockerCluster) unsafeGetTargetedScaleInCommand(targetScale int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
	numAffectedNodes := int32(c.hosts.Len()) - targetScale
	if numAffectedNodes != int32(len(targetHosts)) {
		return nil, fmt.Errorf("inconsistent targetScale (%d) and length of hosts to remove (%d)", targetScale, len(targetHosts))
	}

	return func() {
		c.log.Debug("Attempting to remove the following %d host(s): %s", len(targetHosts), strings.Join(targetHosts, ", "))

		disabledHosts := make([]string, 0, len(targetHosts))
		errs := make([]error, 0)

		var scaleInDuration time.Duration
		for i, id := range targetHosts {
			err := c.unsafeDisableHost(id)
			if err != nil {
				c.log.Error("Could not remove host \"%s\" from Docker Compose Cluster because: %v", id, err)
				errs = append(errs, err)
				break
			}

			disabledHosts = append(disabledHosts, id)
			scaleInForHost := time.Duration(rand.NormFloat64()*float64(c.StdDevScaleInPerHost)) + c.MeanScaleInPerHost
			c.log.Debug("Simulated scale-in duration for target host #%d (%s): %v",
				i+1, targetHosts[i], scaleInForHost)

			// We're simulating the concurrent termination of several hosts.
			// We'll sleep for whatever host takes the longest to terminate.
			if scaleInForHost > scaleInDuration {
				scaleInDuration = scaleInForHost
			}
		}

		// If we failed to disable one or more hosts, then we'll abort the entire operation.
		if len(errs) > 0 {
			c.log.Warn("Could not disable all %d hosts during scale-in. Re-enabling %d hosts that were already disabled.",
				len(targetHosts), len(disabledHosts))
			for _, disabledHostId := range disabledHosts {
				enableErr := c.unsafeEnableHost(disabledHostId)
				if enableErr != nil {
					c.log.Error("Failed to enable freshly-disabled host %s during failed scale-in operation because: %v",
						disabledHostId, enableErr)
				}
			}

			err := errors.Join(errs...)
			c.log.Warn("Scale-in operation of %d nodes has failed because: %v", len(targetHosts), err)
			coreLogicDoneChan <- err
			return
		}

		c.log.Debug("Simulating scale-in of %d host(s) for %v.", len(targetHosts), scaleInDuration)
		time.Sleep(scaleInDuration)

		coreLogicDoneChan <- struct{}{}
	}, nil
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
//
// DockerCluster scales-in by disabling Local Daemon nodes while leaving their containers active and running.
//
// This is because Docker Compose does not allow you to specify the container to be terminated when scaling-down
// a docker compose service.
//
// Important: this should be called with the Cluster's hostMutex already acquired.
func (c *DockerCluster) GetScaleInCommand(targetScale int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
	if len(targetHosts) > 0 {
		return c.unsafeGetTargetedScaleInCommand(targetScale, targetHosts, coreLogicDoneChan)
	}

	// If no target host instances were specified, then we need to identify some host instances ourselves.
	numAffectedNodes := int32(c.hosts.Len()) - targetScale

	c.log.Debug("Searching for %d hosts to terminate for requested scale-in.", numAffectedNodes)

	// First, just look for Hosts that are entirely idle.
	// NOTE: targetHosts is empty at this point. If it wasn't, we would have called unsafeGetTargetedScaleInCommand(...).
	c.hosts.Range(func(hostId string, host scheduling.Host) (contd bool) {
		if host.NumContainers() == 0 {
			targetHosts = append(targetHosts, hostId)
			c.log.Debug("Identified host %s as viable target for termination during scale-in. Identified %d/%d hosts to terminate.",
				host.GetID(), len(targetHosts), numAffectedNodes)
		}

		// If we've identified enough hosts, then we can stop iterating.
		if int32(len(targetHosts)) == numAffectedNodes {
			c.log.Debug("Successfully identified %d/%d hosts to terminate for scale-in.", len(targetHosts), numAffectedNodes)
			return false
		}

		return true
	})

	// If we've found enough Hosts to terminate, then we can get the scale-in command for the specified hosts.
	// If not, then we'll have to keep trying. Or, for now, we just return an error indicating that we cannot
	// scale-down by that many Hosts as there are insufficient idle hosts available.
	if int32(len(targetHosts)) == numAffectedNodes {
		return c.unsafeGetTargetedScaleInCommand(targetScale, targetHosts, coreLogicDoneChan)
	}

	c.log.Warn("Failed to identify %d hosts for scale-in. Only identified %d/%d.",
		numAffectedNodes, len(targetHosts), numAffectedNodes)
	return nil, fmt.Errorf("%w: insufficient idle hosts available to scale-in by %d host(s); largest scale-in possible: %d host(s)",
		scheduler.ErrInvalidTargetScale, numAffectedNodes, len(targetHosts))
}
