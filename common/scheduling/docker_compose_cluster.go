package scheduling

import (
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"os/exec"
	"strings"
)

// DockerComposeCluster encapsulates the logic for a Docker compose cluster, in which the nodes are simulated
// locally, and scaling-up and down sometimes involves simulation steps in which nodes are not actually deleted,
// but simply toggled "off" and "on".
type DockerComposeCluster struct {
	*BaseCluster

	// DisabledHosts is a map from host ID to *Host containing all the Host instances that are currently set to "off".
	DisabledHosts hashmap.HashMap[string, *Host]
}

// NewDockerComposeCluster creates a new DockerComposeCluster struct and returns a pointer to it.
//
// NewDockerComposeCluster should be used when the system is deployed in Docker mode (either compose or swarm, for now).
// This function accepts parameters that are used to construct a DockerScheduler to be used internally
// by the Cluster for scheduling decisions.
func NewDockerComposeCluster(gatewayDaemon ClusterGateway, hostSpec types.Spec,
	clusterMetricsProvider metrics.ClusterMetricsProvider, opts *ClusterSchedulerOptions) *DockerComposeCluster {

	baseCluster := newBaseCluster(opts.GpusPerHost, opts.NumReplicas, clusterMetricsProvider)

	dockerCluster := &DockerComposeCluster{
		BaseCluster:   baseCluster,
		DisabledHosts: hashmap.NewConcurrentMap[*Host](64),
	}

	placer, err := NewRandomPlacer(dockerCluster, opts)
	if err != nil {
		dockerCluster.log.Error("Failed to create Random Placer: %v", err)
		panic(err)
	}
	dockerCluster.placer = placer

	scheduler, err := NewDockerScheduler(gatewayDaemon, dockerCluster, placer, hostSpec, opts)
	if err != nil {
		dockerCluster.log.Error("Failed to create Kubernetes Cluster Scheduler: %v", err)
		panic(err)
	}

	dockerCluster.scheduler = scheduler
	baseCluster.instance = dockerCluster

	return dockerCluster
}

// NodeType returns the type of node provisioned within the Cluster.
func (c *DockerComposeCluster) NodeType() string {
	return types.DockerNode
}

// disableHost disables an active Host.
//
// If the Host does not exist or is not already disabled, then an error is returned.
func (c *DockerComposeCluster) disableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	host, loaded := c.LoadAndDelete(id)
	if !loaded {
		// Let's check if the Host even exists.
		_, exists := c.DisabledHosts.Load(id)
		if exists {
			return fmt.Errorf("%w: host \"%s\" is already disabled", ErrInvalidHost, id)
		} else {
			return fmt.Errorf("%w: host \"%s\" does not exist (in any capacity)", ErrInvalidHost, id)
		}
	}

	if host.containers.Len() > 0 {
		return fmt.Errorf("%w: host \"%s\" is hosting at least one kernel replica, and automated migrations are not yet implemented", ErrInvalidHost, id)
	}

	c.DisabledHosts.Store(id, host)
	if err := host.Enable(); err != nil {
		// This really shouldn't happen.
		// This would mean that the Host was in an inconsistent state relative to the Cluster,
		// as the Host was stored in the wrong map.
		panic(err)
	}

	return nil
}

// enableHost enables a disabled Host.
//
// If the Host does not exist or is not disabled, then an error is returned.
func (c *DockerComposeCluster) enableHost(id string) error {
	c.hostMutex.Lock()
	defer c.hostMutex.Unlock()

	disabledHost, loaded := c.DisabledHosts.LoadAndDelete(id)
	if !loaded {
		// Let's check if the Host even exists.
		_, exists := c.hosts.Load(id)
		if exists {
			return fmt.Errorf("%w: host \"%s\" is not disabled", ErrInvalidHost, id)
		} else {
			return fmt.Errorf("%w: host \"%s\" does not exist (in any capacity)", ErrInvalidHost, id)
		}
	}

	c.Store(id, disabledHost)
	if err := disabledHost.Enable(); err != nil {
		// This really shouldn't happen.
		// This would mean that the Host was in an inconsistent state relative to the Cluster,
		// as the Host was stored in the wrong map.
		panic(err)
	}

	return nil
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
func (c *DockerComposeCluster) GetScaleOutCommand(targetNumNodes int32, coreLogicDoneChan chan interface{}) func() {
	return func() {
		app := "docker"
		argString := fmt.Sprintf("compose up -d --scale daemon=%d --no-deps --no-recreate", targetNumNodes)
		args := strings.Split(argString, " ")

		cmd := exec.Command(app, args...)
		stdout, err := cmd.Output()

		if err != nil {
			c.log.Error("Failed to scale-out to %d node because: %v", targetNumNodes, err)
			coreLogicDoneChan <- err
		} else {
			c.log.Debug("Output from scaling-out to %d node:\n%s", targetNumNodes, string(stdout))

			coreLogicDoneChan <- struct{}{}
		}
	}
}

// getTargetedScaleInCommand returns a function that, when executed, will terminate the hosts specified in the targetHosts parameter.
func (c *DockerComposeCluster) getTargetedScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
	if targetNumNodes != int32(len(targetHosts)) {
		return nil, fmt.Errorf("inconsistent targetNumHosts (%d) and length of target hosts (%d)", targetNumNodes, len(targetHosts))
	}

	return func() {
		disabledHosts := make([]string, 0, len(targetHosts))
		errs := make([]error, 0)
		for _, id := range targetHosts {
			err := c.disableHost(id)
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
			for _, disabledHostId := range disabledHosts {
				enableErr := c.enableHost(disabledHostId)
				if enableErr != nil {
					c.log.Error("Failed to enable freshly-disabled host %s during failed scale-in operation because: %v",
						disabledHostId, enableErr)
				}
			}

			err := errors.Join(errs...)
			coreLogicDoneChan <- err
			return
		}

		coreLogicDoneChan <- struct{}{}
	}, nil
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
//
// DockerComposeCluster scales-in by disabling Local Daemon nodes while leaving their containers active and running.
//
// This is because Docker Compose does not allow you to specify the container to be terminated when scaling-down
// a docker compose service.
func (c *DockerComposeCluster) GetScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) (func(), error) {
	if len(targetHosts) > 0 {
		return c.getTargetedScaleInCommand(targetNumNodes, targetHosts, coreLogicDoneChan)
	}

	// If no target Host instances were specified, then we need to identify some Host instances ourselves.
	c.hostMutex.Lock()

	// First, just look for Hosts that are entirely idle.
	c.hosts.Range(func(hostId string, host *Host) (contd bool) {
		if host.containers.Len() == 0 {
			targetHosts = append(targetHosts, hostId)
		}

		// If we've identified enough hosts, then we can stop iterating.
		if int32(len(targetHosts)) == targetNumNodes {
			return false
		}

		return true
	})

	c.hostMutex.Unlock()

	// If we've found enough Hosts to terminate, then we can get the scale-in command for the specified hosts.
	// If not, then we'll have to keep trying. Or, for now, we just return an error indicating that we cannot
	// scale-down by that many Hosts as there are insufficient idle hosts available.
	if int32(len(targetHosts)) == targetNumNodes {
		return c.getTargetedScaleInCommand(targetNumNodes, targetHosts, coreLogicDoneChan)
	}

	return nil, fmt.Errorf("%w: insufficient idle hosts available to scale-in by %dhost(s); largest scale-in possible: %d host(s)",
		ErrInvalidTargetScale, targetNumNodes, len(targetHosts))
}
