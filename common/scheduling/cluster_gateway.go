package scheduling

import (
	"context"
	jupyter "github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/proto"
)

type ClusterGateway interface {
	proto.ClusterGatewayServer

	SetClusterOptions(*ClusterSchedulerOptions)
	ConnectionOptions() *jupyter.ConnectionInfo

	// ClusterScheduler returns the associated ClusterScheduler.
	ClusterScheduler() ClusterScheduler

	// GetClusterActualGpuInfo returns the current GPU resource metrics on the node.
	GetClusterActualGpuInfo(ctx context.Context, in *proto.Void) (*proto.ClusterActualGpuInfo, error)

	// KubernetesMode returns true if we're running in a Kubernetes cluster (rather than as a docker-compose application).
	KubernetesMode() bool

	// DockerMode returns true if we're running in "docker swarm" mode or "docker compose" mode.
	DockerMode() bool

	// DockerSwarmMode returns true if we're running in "docker swarm" mode.
	DockerSwarmMode() bool

	// DockerComposeMode returns true if we're running in "docker compose" mode.
	DockerComposeMode() bool

	// GetHostsOfKernel returns the Host instances on which replicas of the specified kernel are scheduled.
	GetHostsOfKernel(kernelId string) ([]*Host, error)
}
