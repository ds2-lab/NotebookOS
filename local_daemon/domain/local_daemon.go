package domain

import (
	"context"

	"github.com/go-zeromq/zmq4"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/router"
)

type SchedulerDaemon interface {
	gateway.LocalGatewayServer
	router.RouterProvider

	// SetID sets the SchedulerDaemonImpl id by the gateway.
	SetID(ctx context.Context, in *gateway.HostId) (*gateway.HostId, error)

	// StartKernel starts a single kernel.
	StartKernel(ctx context.Context, in *gateway.KernelSpec) (*gateway.KernelConnectionInfo, error)

	AckHandler(info router.RouterInfo, msg *zmq4.Msg) error

	// Return true if we're running in Docker (i.e., the Docker-based deployment).
	// We could technically be running within a Docker container that is managed/orchestrated
	// by Kubernetes. In this case, this function would return false.
	DockerMode() bool

	// Return true if we're running in Kubernetes.
	KubernetesMode() bool

	// Return true if we're running in Local mode.
	LocalMode() bool

	Start() error

	Close() error

	Provisioner() gateway.ClusterGatewayClient

	SetProvisioner(gateway.ClusterGatewayClient)
}
