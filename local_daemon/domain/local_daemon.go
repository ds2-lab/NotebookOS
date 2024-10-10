package domain

import (
	"context"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"google.golang.org/grpc"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
)

type SchedulerDaemon interface {
	proto.LocalGatewayServer
	router.RouterProvider

	// SetID sets the SchedulerDaemonImpl id by the gateway.
	SetID(ctx context.Context, in *proto.HostId) (*proto.HostId, error)

	// StartKernel starts a single kernel.
	StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error)

	// DockerMode returns true if we're running in Docker (i.e., the Docker-based deployment).
	// We could technically be running within a Docker container that is managed/orchestrated
	// by Kubernetes. In this case, this function would return false.
	DockerMode() bool

	// KubernetesMode returns true if we're running in Kubernetes.
	KubernetesMode() bool

	// LocalMode returns true if we're running in Local mode.
	LocalMode() bool

	Start() error

	Close() error

	Provisioner() proto.ClusterGatewayClient

	SetProvisioner(proto.ClusterGatewayClient, *grpc.ClientConn)
}
