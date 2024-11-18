package cluster

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/scheduler"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"strings"
)

// DockerSwarmCluster encapsulates the logic for a Docker compose Cluster, in which the nodes are simulated
// locally, and scaling-up and down sometimes involves simulation steps in which nodes are not actually deleted,
// but simply toggled "off" and "on".
type DockerSwarmCluster struct {
	*BaseCluster
}

// NewDockerSwarmCluster creates a new DockerSwarmCluster struct and returns a pointer to it.
//
// NewDockerSwarmCluster should be used when the system is deployed in Docker Swarm mode.
//
// This function accepts parameters that are used to construct a DockerScheduler to be used internally by the
// DockerSwarmCluster for scheduling decisions.
func NewDockerSwarmCluster(hostSpec types.Spec, placer scheduling.Placer, hostMapper scheduler.HostMapper, kernelProvider scheduler.KernelProvider,
	clusterMetricsProvider scheduling.MetricsProvider, notificationBroker scheduler.NotificationBroker, opts *scheduling.SchedulerOptions) *DockerSwarmCluster {

	baseCluster := newBaseCluster(opts, placer, clusterMetricsProvider, "DockerSwarmCluster")

	dockerCluster := &DockerSwarmCluster{
		BaseCluster: baseCluster,
	}

	dockerScheduler, err := scheduler.NewDockerScheduler(dockerCluster, placer, hostMapper, hostSpec, kernelProvider, notificationBroker, opts)
	if err != nil {
		dockerCluster.log.Error("Failed to create Docker Swarm Cluster Scheduler: %v", err)
		panic(err)
	}

	dockerCluster.scheduler = dockerScheduler
	baseCluster.instance = dockerCluster

	return dockerCluster
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *DockerSwarmCluster) NodeType() string {
	return types.DockerNode
}

// unsafeDisableHost disables an active Host.
//
// If the Host does not exist or is not already disabled, then an error is returned.
//
// Important: this should be called with the DockerSwarmCluster's hostMutex already acquired.
func (c *DockerSwarmCluster) unsafeDisableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, loaded := c.hosts.Load(id)
	if !loaded {
		// Let's check if the Host even exists.
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
		// This would mean that the Host was in an inconsistent state relative to the Cluster,
		// as the Host was stored in the wrong map.
		panic(err)
	}
	c.DisabledHosts.Store(id, host)
	c.hosts.Delete(id)

	c.onHostRemoved(host)

	if c.metricsProvider != nil {
		c.metricsProvider.GetNumDisabledHostsGauge().Add(1)
	}

	return nil
}

// unsafeEnableHost enables a disabled Host.
//
// If the Host does not exist or is not disabled, then an error is returned.
//
// Important: this should be called with the DockerSwarmCluster's hostMutex already acquired.
func (c *DockerSwarmCluster) unsafeEnableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	disabledHost, loaded := c.DisabledHosts.LoadAndDelete(id)
	if !loaded {
		// Let's check if the Host even exists.
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
		// This would mean that the Host was in an inconsistent state relative to the Cluster,
		// as the Host was stored in the wrong map.
		panic(err)
	}
	c.hosts.Store(id, disabledHost)

	if c.metricsProvider != nil {
		c.metricsProvider.GetNumDisabledHostsGauge().Sub(1)
	}

	return nil
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
//
// Important: this should be called with the Cluster's hostMutex already acquired.
func (c *DockerSwarmCluster) GetScaleOutCommand(targetScale int32, coreLogicDoneChan chan interface{}) func() {
	return func() {
		currentScale := c.Len()
		numNewNodesRequired := targetScale - int32(currentScale)
		c.log.Debug("Scaling-out to %d nodes. CurrentSize: %d. #NewNodesRequired: %d. #DisabledNodes: %d.",
			targetScale, currentScale, numNewNodesRequired, c.DisabledHosts.Len())

		numDisabledHostsUsed := 0
		if c.DisabledHosts.Len() > 0 {
			enabledHosts := make([]scheduling.Host, 0)
			// First, check if we have any disabled nodes. If we do, then we'll just re-enable them.
			c.DisabledHosts.Range(func(hostId string, host scheduling.Host) (contd bool) {
				err := host.Enable(true)
				if err != nil {
					c.log.Error("Failed to re-enable host %s because: %v", hostId, err)
					// For now, we panic, as we don't expect there to be a "valid" reason to fail to enable a host.
					// Later on, we may find that there are valid reasons, in which case we'd just handle the
					// error in whatever way is appropriate, such as by skipping this host and trying the next one.
					panic(err)
				}

				c.log.Debug("Using disabled host %s in scale-out operation.", hostId)

				// This will add the host back to the Cluster.
				err = c.NewHostAddedOrConnected(host)
				if err != nil {
					c.log.Error("Error adding newly-connected host %s (ID=%s) to cluster: %v",
						host.GetNodeName(), host.GetID(), err)
					// TODO: Need to handle this error...
				}

				enabledHosts = append(enabledHosts, host)
				numNewNodesRequired -= 1
				numDisabledHostsUsed += 1

				// If we have already satisfied the scale-out requirement, then we'll stop iterating.
				return numNewNodesRequired != 0
			})

			// Remove all the previously-disabled hosts (that we used in the scale-out operation) from the "disabled hosts" mapping.
			for _, host := range enabledHosts {
				_, loaded := c.DisabledHosts.LoadAndDelete(host.GetID())
				if !loaded {
					log.Fatalf("Failed to find host %s in DisabledHosts map after using it in scale-out operation.", host.GetID())
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

		c.log.Error("Could not satisfy scale-out request to %d nodes exclusively using disabled nodes.", targetScale)
		c.log.Error("Used %d disabled host(s). Still need %d additional host(s) to satisfy request.", numDisabledHostsUsed, targetScale-int32(currentScale))

		coreLogicDoneChan <- fmt.Errorf("%w: adding additional nodes is not supported by docker swarm clusters", scheduling.ErrUnsupportedOperation)
	}
}

// CanPossiblyScaleOut returns true if the Cluster could possibly scale-out.
// This is always true for docker compose clusters, but for kubernetes and docker swarm clusters,
// it is currently not supported unless there is at least one disabled host already within the cluster.
func (c *DockerSwarmCluster) CanPossiblyScaleOut() bool {
	return c.DisabledHosts.Len() > 0
}

// unsafeGetTargetedScaleInCommand returns a function that, when executed, will terminate the hosts specified in the targetHosts parameter.
//
// Important: this should be called with the Cluster's hostMutex already acquired.
func (c *DockerSwarmCluster) unsafeGetTargetedScaleInCommand(targetScale int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
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
// DockerSwarmCluster scales-in by disabling Local Daemon nodes while leaving their containers active and running.
//
// This is because Docker Compose does not allow you to specify the container to be terminated when scaling-down
// a docker compose service.
//
// Important: this should be called with the Cluster's hostMutex already acquired.
func (c *DockerSwarmCluster) GetScaleInCommand(targetScale int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
	if len(targetHosts) > 0 {
		return c.unsafeGetTargetedScaleInCommand(targetScale, targetHosts, coreLogicDoneChan)
	}

	// If no target Host instances were specified, then we need to identify some Host instances ourselves.
	numAffectedNodes := int32(c.hosts.Len()) - targetScale

	c.log.Debug("Searching for %d hosts to terminate for requested scale-in.", numAffectedNodes)

	// First, just look for Hosts that are entirely idle.
	// NOTE: targetHosts is empty at this point. If it wasn't, we would have called unsafeGetTargetedScaleInCommand(...).
	c.hosts.Range(func(hostId string, host scheduling.Host) (contd bool) {
		if host.NumContainers() == 0 {
			targetHosts = append(targetHosts, hostId)
			c.log.Debug("Identified Host %s as viable target for termination during scale-in. Identified %d/%d hosts to terminate.",
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
