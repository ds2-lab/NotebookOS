package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

type DockerCluster struct {
	*BaseCluster
}

// NewDockerCluster creates a new DockerCluster struct and returns a pointer to it.
//
// NewDockerCluster should be used when the system is deployed in Docker mode (either compose or swarm, for now).
// This function accepts parameters that are used to construct a DockerScheduler to be used internally
// by the Cluster for scheduling decisions.
func NewDockerCluster(gatewayDaemon ClusterGateway, hostSpec types.Spec, opts *ClusterSchedulerOptions) *DockerCluster {
	baseCluster := newBaseCluster(opts.GpusPerHost)

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

// HandleScaleOutOperation handles a scale-out operation, adding the necessary Host instances to the Cluster.
func (c *DockerCluster) HandleScaleOutOperation(op *ScaleOperation) promise.Promise {
	if !op.IsScaleOutOperation() {
		return fmt.Errorf("%w: expected \"%s\", got \"%s\"", ErrIncorrectScaleOperation, ScaleOutOperation, op.OperationType)
	}

	return nil
}

// HandleScaleInOperation handles a scale-in operation, removing the necessary Host instances from the Cluster.
func (c *DockerCluster) HandleScaleInOperation(op *ScaleOperation) promise.Promise {
	if !op.IsScaleInOperation() {
		return fmt.Errorf("%w: expected \"%s\", got \"%s\"", ErrIncorrectScaleOperation, ScaleInOperation, op.OperationType)
	}

	return nil
}

func (c *DockerCluster) RequestHost(spec types.Spec) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}

func (c *DockerCluster) ReleaseHost(id string) promise.Promise {
	return promise.Resolved(nil, promise.ErrNotImplemented)
}
