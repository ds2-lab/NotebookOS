package cluster

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"math/rand"
	"strings"
	"time"
)

type DockerClusterBuilder struct {
	hostSpec                  types.Spec
	placer                    scheduling.Placer
	hostMapper                scheduler.HostMapper
	kernelProvider            scheduler.KernelProvider
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

func (b *DockerClusterBuilder) WithHostMapper(hostMapper scheduler.HostMapper) *DockerClusterBuilder {
	b.hostMapper = hostMapper
	return b
}

func (b *DockerClusterBuilder) WithKernelProvider(kernelProvider scheduler.KernelProvider) *DockerClusterBuilder {
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
func NewDockerCluster(hostSpec types.Spec, placer scheduling.Placer, hostMapper scheduler.HostMapper, kernelProvider scheduler.KernelProvider,
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

	c.log.Debug("Disabling host %s now...", id)
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
func (c *DockerCluster) GetScaleOutCommand(targetScale int32, coreLogicDoneChan chan interface{}, scaleOpId string) func() {
	return func() {
		currentScale := c.Len()
		numNewNodesRequired := targetScale - int32(currentScale)
		c.log.Debug("Scaling out to %d nodes. CurrentSize: %d. #NewNodesRequired: %d. #DisabledNodes: %d. ScaleOpId: %s.",
			targetScale, currentScale, numNewNodesRequired, c.DisabledHosts.Len(), scaleOpId)

		numDisabledHostsUsed := 0
		if c.DisabledHosts.Len() > 0 {
			enabledHosts := make([]scheduling.Host, 0)
			// First, check if we have any disabled nodes. If we do, then we'll just re-enable them.
			c.DisabledHosts.Range(func(hostId string, host scheduling.Host) (contd bool) {
				err := host.Enable(true)
				if err != nil {
					c.log.Error("Failed to re-enable host %s (ID=%s) during scale-out %s because: %v",
						host.GetNodeName(), hostId, scaleOpId, err)

					// For now, we panic, as we don't expect there to be a "valid" reason to fail to enable a host.
					// Later on, we may find that there are valid reasons, in which case we'd just handle the
					// error in whatever way is appropriate, such as by skipping this host and trying the next one.
					panic(err)
				}

				c.log.Debug("Using disabled host %s (ID=%s) in scale-out operation %s.",
					host.GetNodeName(), hostId, scaleOpId)

				scaleOutDuration := time.Duration(rand.NormFloat64()*float64(c.StdDevScaleOutPerHost)) + c.MeanScaleOutPerHost
				c.log.Debug("Simulating scale-out for host %s (ID=%s) during operation %s. Duration: %v",
					host.GetNodeName(), hostId, scaleOpId, scaleOutDuration)
				time.Sleep(scaleOutDuration)

				// This will add the host back to the Cluster.
				err = c.NewHostAddedOrConnected(host)
				if err != nil {
					c.log.Error("Error adding newly-connected host %s (ID=%s) to cluster during scale-out %s: %v",
						host.GetNodeName(), hostId, scaleOpId, err)
				}

				enabledHosts = append(enabledHosts, host)
				numNewNodesRequired -= 1
				numDisabledHostsUsed += 1

				// If we have already satisfied the scale-out requirement, then we'll stop iterating.
				// So, return true (i.e., continue iterating) if numNewNodesRequired > 0.
				// Otherwise, return false (i.e., stop iterating).
				return numNewNodesRequired > 0
			})

			c.log.Debug("Removing %d host(s) from DisabledHosts after simulating scale-out operation %s",
				len(enabledHosts), scaleOpId)

			// Remove all the previously-disabled hosts (that we used in the scale-out operation) from the
			// "disabled hosts" mapping.
			for _, host := range enabledHosts {
				_, loaded := c.DisabledHosts.LoadAndDelete(host.GetID())
				c.log.Debug("Removed host %s (ID=%s) from DisabledHosts", host.GetNodeName(), host.GetID())
				if !loaded {
					c.log.Error("Failed to find host %s in DisabledHosts map after using it in scale-out operation.",
						host.GetID())
				}
			}
		}

		// Check if we satisfied the scale-out request using disabled nodes, in which case we do not
		// need to execute a Docker CLI command and can just return immediately.
		if numNewNodesRequired == 0 {
			// Note that currentScale should be outdated at this point, but its old/outdated
			// value can be used to calculate how many disabled hosts we must have used
			// in order to satisfy the scale-out request.
			c.log.Debug("Satisfied scale-out request to %d nodes exclusively using %d disabled nodes.",
				targetScale, numDisabledHostsUsed)
			coreLogicDoneChan <- struct{}{}
			return
		}

		c.log.Warn("Could not satisfy scale-out request to %d nodes exclusively using disabled nodes.", targetScale)
		c.log.Warn("Used %d disabled host(s). Still need %d additional host(s) to satisfy request.",
			numDisabledHostsUsed, targetScale-int32(currentScale))

		coreLogicDoneChan <- fmt.Errorf("%w: adding additional nodes is not supported by docker compose clusters",
			scheduling.ErrUnsupportedOperation)
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
		for _, id := range targetHosts {
			err := c.unsafeDisableHost(id)
			if err != nil {
				c.log.Error("Could not remove host \"%s\" from Docker Compose Cluster because: %v", id, err)
				errs = append(errs, err)
				break
			} else {
				disabledHosts = append(disabledHosts, id)
			}
		}

		var totalScaleInDuration time.Duration
		for i := 0; i < int(numAffectedNodes); i++ {
			scaleInDuration := time.Duration(rand.NormFloat64()*float64(c.StdDevScaleInPerHost)) + c.MeanScaleInPerHost
			c.log.Debug("Simulated scale-in duration for target host #%d (%s): %v",
				i+1, targetHosts[i], scaleInDuration)

			totalScaleInDuration += scaleInDuration
		}

		c.log.Debug("Simulating scale-in of %d host(s) for %v.", len(targetHosts), totalScaleInDuration)
		time.Sleep(totalScaleInDuration)

		// If we failed to disable one or more hosts, then we'll abort the entire operation.
		if len(errs) > 0 {
			c.log.Warn("Could not identify all %d hosts during scale-in. Re-enabling %d hosts that were already disabled.",
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
