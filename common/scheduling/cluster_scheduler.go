package scheduling

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/zhangjyr/distributed-notebook/common/proto"
)

type ClusterScheduler interface {
	// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
	// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
	// need to start immediately after replica started, e.g., preempting a training task.
	//MigrateKernelReplica(ctx context.Context, in *proto.KernelId, opts ...grpc.CallOption) (*proto.ReplicaId, error)

	// ValidateCapacity validates the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
	// Adjust the Cluster's capacity as directed by scaling policy.
	//
	// If ValidateCapacity detects that there are too few Host instances provisioned to satisfy demand,
	// then additional Host instances will be created.
	//
	// Alternatively, if ValidateCapacity determines that there are more Host instances provisioned than
	// are truly needed, then some Host instances will be terminated to reduce unnecessary resource usage.
	ValidateCapacity()

	// AddNode adds a new node to the kubernetes cluster.
	// We simulate this using node taints.
	AddNode() error

	// RemoveNode removes a new from the kubernetes cluster.
	// We simulate this using node taints.
	RemoveNode() error

	// MinimumCapacity Returns the minimum number of nodes we must have available at any time.
	MinimumCapacity() int32

	// ReleaseIdleHosts Tries to release n idle hosts. Return the number of hosts that were actually released.
	// Error will be nil on success and non-nil if some sort of failure is encountered.
	ReleaseIdleHosts(n int32) (int, error)

	// RefreshActualGpuInfo Refreshes the actual GPU usage information.
	// Returns nil on success; returns an error on failure.
	RefreshActualGpuInfo() error

	// RefreshClusterNodes Updates the cached list of Kubernetes nodes.
	// Returns nil on success; returns an error on failure.
	RefreshClusterNodes() error

	// RefreshAll refreshes all metrics maintained/cached/required by the Cluster Scheduler,
	// including the list of current kubernetes nodes, actual and virtual GPU usage information, etc.
	//
	// Return a slice of any errors that occurred. If an error occurs while refreshing a particular piece of information,
	// then the error is recorded, and the refresh proceeds, attempting all refreshes (even if an error occurs during one refresh).
	RefreshAll() []error

	// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
	DeployNewKernel(context.Context, *proto.KernelSpec) error

	// ScheduleKernelReplica schedules a particular replica onto the given Host.
	//
	// Exactly one of replicaSpec and kernelSpec should be non-nil.
	// That is, both cannot be nil, and both cannot be non-nil.
	//
	// If targetHost is nil, then a candidate host is identified automatically by the ClusterScheduler.
	ScheduleKernelReplica(replicaId int32, kernelId string, replicaSpec *proto.KernelReplicaSpec, kernelSpec *proto.KernelSpec, host *Host) error
}

type KubernetesClusterScheduler interface {
	ClusterScheduler

	// StartHttpKubernetesSchedulerService starts the HTTP service used to make scheduling decisions.
	// This method should be called from its own goroutine.
	StartHttpKubernetesSchedulerService()

	// HandleKubeSchedulerFilterRequest handles a 'filter' request from the kubernetes scheduler.
	HandleKubeSchedulerFilterRequest(ctx *gin.Context)
}
