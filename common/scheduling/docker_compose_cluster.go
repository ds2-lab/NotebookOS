package scheduling

import (
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

	// OfflineHosts is a map from host ID to *Host containing all the Host instances that are currently set to "off".
	OfflineHosts hashmap.HashMap[string, *Host]
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
		BaseCluster:  baseCluster,
		OfflineHosts: hashmap.NewConcurrentMap[*Host](64),
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
	return nil
}

// enableHost enables a disabled Host.
//
// If the Host does not exist or is not disabled, then an error is returned.
func (c *DockerComposeCluster) enableHost(id string) error {
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

// GetScaleInCommand returns the function to be executed to perform a scale-in.
//
// DockerComposeCluster scales-in by disabling Local Daemon nodes while leaving their containers active and running.
//
// This is because Docker Compose does not allow you to specify the container to be terminated when scaling-down
// a docker compose service.
func (c *DockerComposeCluster) GetScaleInCommand(targetNumNodes int32, targetHosts []string, coreLogicDoneChan chan interface{}) func() {
	return func() {
		errs := make([]error, 0)
		for _, id := range targetHosts {
			err := c.disableHost(id)
			if err != nil {
				c.log.Error("Could not remove host \"%s\" from Docker Compose Cluster because: %v", id, err)
				errs = append(errs, err)
			}
		}

		panic("Not implemented")

		//if len(errs) > 0 {
		//	err := errors.Join(errs...)
		//	coreLogicDoneChan <- err
		//} else {
		//	coreLogicDoneChan <- struct{}{}
		//}
	}
}
