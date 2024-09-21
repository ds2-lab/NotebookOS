package scheduling

import (
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"os/exec"
	"strings"
)

type DockerCluster struct {
	*BaseCluster
}

// NewDockerCluster creates a new DockerCluster struct and returns a pointer to it.
//
// NewDockerCluster should be used when the system is deployed in Docker mode (either compose or swarm, for now).
// This function accepts parameters that are used to construct a DockerScheduler to be used internally
// by the Cluster for scheduling decisions.
func NewDockerCluster(gatewayDaemon ClusterGateway, hostSpec types.Spec,
	clusterMetricsProvider metrics.ClusterMetricsProvider, opts *ClusterSchedulerOptions) *DockerCluster {

	baseCluster := newBaseCluster(opts.GpusPerHost, opts.NumReplicas, clusterMetricsProvider)

	dockerCluster := &DockerCluster{
		BaseCluster: baseCluster,
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
func (c *DockerCluster) NodeType() string {
	return types.DockerNode
}

// GetScaleOutCommand returns the function to be executed to perform a scale-out.
func (c *DockerCluster) GetScaleOutCommand(targetNumNodes int32, resultChan chan interface{}) func() {
	return func() {
		app := "docker"
		argString := fmt.Sprintf("compose up -d --scale daemon=%d --no-deps --no-recreate", targetNumNodes)
		args := strings.Split(argString, " ")

		cmd := exec.Command(app, args...)
		stdout, err := cmd.Output()

		if err != nil {
			c.log.Error("Failed to scale-out to %d node because: %v", targetNumNodes, err)
			// return nil, status.Errorf(codes.Internal, err.Error())
			resultChan <- err
		} else {
			c.log.Debug("Output from scaling-out to %d node:\n%s", targetNumNodes, string(stdout))
			resultChan <- struct{}{}
		}
	}
}

// GetScaleInCommand returns the function to be executed to perform a scale-in.
func (c *DockerCluster) GetScaleInCommand(targetNumNodes int32, resultChan chan interface{}) func() {
	return func() {
		app := "docker"
		argString := fmt.Sprintf("compose up -d --scale daemon=%d --no-deps --no-recreate", targetNumNodes)
		args := strings.Split(argString, " ")

		cmd := exec.Command(app, args...)
		stdout, err := cmd.Output()

		if err != nil {
			c.log.Error("Failed to scale-in to %d node because: %v", targetNumNodes, err)
			resultChan <- err
			// return nil, status.Errorf(codes.Internal, err.Error())
		} else {
			c.log.Debug("Output from scaling-in to %d node:\n%s", targetNumNodes, string(stdout))
			resultChan <- struct{}{}
		}
	}
}
