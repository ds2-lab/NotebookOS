package scheduling

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"strings"
	"time"
)

// ErrorDuringScheduling is a custom error for when the scheduling of a new kernel fails.
type ErrorDuringScheduling struct {
	// UnderlyingError is the underlying error.
	UnderlyingError error `json:"underlying_error"`

	// ScheduledReplicaIDs are the IDs of replicas whose scheduling was successful.
	ScheduledReplicaIDs []int32 `json:"scheduled_replica_ids"`

	// HostsWithOrphanedReplicas are the IDs of Host instances who have an orphaned replica Container
	// scheduled onto them (because some replicas may have been scheduled successfully while others weren't).
	HostsWithOrphanedReplicas []string `json:"hosts_with_orphaned_replicas"`
}

func (e *ErrorDuringScheduling) Error() string {
	return fmt.Sprintf("ErrorDuringScheduling[ScheduledReplicaIDs: %v, HostsWithOrphanedReplicas: %s, UnderlyingError: %v",
		e.ScheduledReplicaIDs, strings.Join(e.HostsWithOrphanedReplicas, ", "), e.UnderlyingError)
}

func (e *ErrorDuringScheduling) String() string {
	return e.Error()
}

// ClusterScheduler defines the interface of a scheduler for the Cluster.
//
// The scheduler is ultimately responsible for deciding where to schedule kernel replicas, when and where to migrate
// kernel replicas, etc.
//
// The ClusterScheduler works together with the Placer to fulfill its role and responsibilities.
type ClusterScheduler interface {
	// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
	// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
	// need to start immediately after replica started, e.g., preempting a training task.
	//MigrateKernelReplica(ctx context.Context, in *proto.KernelId, opts ...grpc.CallOption) (*proto.ReplicaId, error)

	// MigrateContainer tries to migrate the given Container from the given host.
	// Flag indicates whether we're allowed to create a new host for the container (if necessary).
	MigrateContainer(*Container, *Host, bool) (bool, error)

	// UpdateRatio updates the Cluster's subscription ratio.
	// UpdateRatio also validates the Cluster's overall capacity as well, scaling in or out as needed.
	UpdateRatio() bool

	// AddNode adds a new node to the kubernetes Cluster.
	// We simulate this using node taints.
	AddNode() error

	// RemoveNode removes a new from the kubernetes Cluster.
	// We simulate this using node taints.
	RemoveNode(hostId string) error

	// MinimumCapacity Returns the minimum number of nodes we must have available at any time.
	MinimumCapacity() int32

	// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
	// the given ratio and the Cluster's current subscription ratio.
	GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal

	// GetCandidateHosts returns a slice of *Host containing Host instances that could serve
	// a Container (i.e., a kernel replica) with the given resource requirements (encoded as a types.Spec).
	//
	// GetCandidateHosts will automatically request that new Host instances be provisioned and added to the Cluster
	// if it fails to find sufficiently many viable Host instances. This process will be attempted three times.
	// If GetCandidateHosts is unsuccessful (at finding sufficiently many viable hosts) after those three attempts,
	// then GetCandidateHosts will give up and return an error.
	//
	// The size of the returned slice will be equal to the configured number of replicas for each kernel (usually 3).
	GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec) ([]*Host, error)

	// ReleaseIdleHosts Tries to release n idle hosts. Return the number of hosts that were actually released.
	// Error will be nil on success and non-nil if some sort of failure is encountered.
	ReleaseIdleHosts(n int32) (int, error)

	// RefreshActualGpuInfo Refreshes the actual GPU usage information.
	// Returns nil on success; returns an error on failure.
	RefreshActualGpuInfo() error

	// RemoteSynchronizationInterval returns the interval at which the ClusterScheduler synchronizes
	// the Host instances within the Cluster with their remote nodes.
	RemoteSynchronizationInterval() time.Duration

	// RefreshClusterNodes Updates the cached list of Cluster nodes.
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
