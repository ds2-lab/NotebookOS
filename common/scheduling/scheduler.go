package scheduling

import (
	"context"
	"fmt"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/gin-gonic/gin"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

const (
	SchedulerPoolTypeUndersubscribed SchedulerPoolType = 1
	SchedulerPoolTypeOversubscribed  SchedulerPoolType = 2
)

type SchedulerPoolType int

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

type KernelScheduler interface {
	// MigrateKernelReplica tries to migrate the given KernelReplica to another Host.
	// Flag indicates whether we're allowed to create a new host for the container (if necessary).
	MigrateKernelReplica(kernelReplica KernelReplica, targetHostId string, canCreateNewHost bool) (*proto.MigrateKernelResponse, error)

	// DeployKernelReplicas is responsible for scheduling the replicas of a new kernel onto Host instances.
	DeployKernelReplicas(ctx context.Context, kernelSpec *proto.KernelSpec, blacklistedHosts []Host) error

	// ScheduleKernelReplica schedules a particular replica onto the given Host.
	//
	// If targetHost is nil, then a candidate host is identified automatically by the Scheduler.
	ScheduleKernelReplica(replicaSpec *proto.KernelReplicaSpec, targetHost Host, blacklistedHosts []Host) error

	// RemoveReplicaFromHost removes the specified replica from its Host.
	RemoveReplicaFromHost(kernelReplica KernelReplica) error

	GetAddReplicaOperationManager() hashmap.HashMap[string, *AddReplicaOperation]

	GetActiveAddReplicaOperationsForKernel(kernelId string) (*orderedmap.OrderedMap[string, *AddReplicaOperation], bool)
}

type HostScheduler interface {
	// AddHost adds a new Host to the Cluster.
	// We simulate this using node taints.
	AddHost() error

	// RemoveHost removes a Host from the Cluster.
	// We simulate this using node taints.
	RemoveHost(hostId string) error

	// ReleaseIdleHosts Tries to release n idle hosts. Return the number of hosts that were actually released.
	// Error will be nil on success and non-nil if some sort of failure is encountered.
	ReleaseIdleHosts(n int32) (int, error)

	// GetCandidateHosts identifies candidate hosts for a particular kernel, reserving resources on hosts
	// before returning them.
	GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec) ([]Host, error)
}

type SchedulerMetricsManager interface {
	// UpdateRatio updates the Cluster's subscription ratio.
	// UpdateRatio also validates the Cluster's overall capacity as well, scaling in or out as needed.
	UpdateRatio(skipValidateCapacity bool) bool

	// MinimumCapacity Returns the minimum number of nodes we must have available at any time.
	MinimumCapacity() int32

	// SubscriptionRatio returns the subscription ratio of the Cluster.
	SubscriptionRatio() float64

	// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
	// the given ratio and the Cluster's current subscription ratio.
	GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal

	// RemoteSynchronizationInterval returns the interval at which the Scheduler synchronizes
	// the Host instances within the Cluster with their remote nodes.
	RemoteSynchronizationInterval() time.Duration
}

// PolicyManager is an interface that exposes methods for reporting what policies the Scheduler is configured to use.
type PolicyManager interface {
	PolicyKey() PolicyKey

	// Policy returns the Policy used by the Scheduler.
	Policy() Policy
}

// Scheduler defines the interface of a scheduler for the Cluster.
//
// The scheduler is ultimately responsible for deciding where to schedule kernel replicas, when and where to migrate
// kernel replicas, etc.
//
// The Scheduler works together with the Placer to fulfill its role and responsibilities.
type Scheduler interface {
	KernelScheduler
	HostScheduler
	SchedulerMetricsManager
	PolicyManager

	// Placer returns the Placer used by the scheduling.Scheduler.
	Placer() Placer
}

type KubernetesClusterScheduler interface {
	Scheduler

	// StartHttpKubernetesSchedulerService starts the HTTP service used to make scheduling decisions.
	// This method should be called from its own goroutine.
	StartHttpKubernetesSchedulerService()

	// HandleKubeSchedulerFilterRequest handles a 'filter' request from the kubernetes scheduler.
	HandleKubeSchedulerFilterRequest(ctx *gin.Context)
}
