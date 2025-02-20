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
	//
	// The first error that is returned (i.e., 'reason') does not indicate that an actual error occurred.
	// It simply provides an explanation for why the migration failed.
	//
	// The second error that is returned (i.e., 'err') indicates that an actual error occurs.
	MigrateKernelReplica(ctx context.Context, kernelReplica KernelReplica, targetHostId string, forTraining bool,
		createNewHostPermitted bool) (resp *proto.MigrateKernelResponse, reason error, err error)

	// DeployKernelReplicas is responsible for scheduling the replicas of a new kernel onto Host instances.
	DeployKernelReplicas(ctx context.Context, kernel Kernel, blacklistedHosts []Host) error

	// ScheduleKernelReplica schedules a particular replica onto the given Host.
	//
	// If targetHost is nil, then a candidate host is identified automatically by the Scheduler.
	ScheduleKernelReplica(ctx context.Context, replicaSpec *proto.KernelReplicaSpec, targetHost Host, blacklistedHosts []Host, forTraining bool) error

	// RemoveReplicaFromHost removes the specified replica from its Host.
	RemoveReplicaFromHost(kernelReplica KernelReplica) error

	GetAddReplicaOperationManager() hashmap.HashMap[string, *AddReplicaOperation]

	GetActiveAddReplicaOperationsForKernel(kernelId string) (*orderedmap.OrderedMap[string, *AddReplicaOperation], bool)

	// SelectReplicaForMigration selects a KernelReplica of the specified Kernel to be migrated.
	SelectReplicaForMigration(kernel Kernel) (KernelReplica, error)

	// ReserveResourcesForReplica is used to instruct the KernelScheduler to explicitly reserve resources for a
	// particular KernelReplica of a particular Kernel.
	//
	// The primary use case for ReserveResourcesForReplica is when a specific KernelReplica is specified to serve as
	// the primary replica within the metadata of an "execute_request" message. This may occur because the user
	// explicitly placed that metadata there, or following a migration when the ClusterGateway has a specific
	// replica that should be able to serve the execution request.
	//
	// PRECONDITION: The specified KernelReplica should already be scheduled on the Host on which the resources are to
	// be reserved.
	ReserveResourcesForReplica(kernel Kernel, replica KernelReplica, commitResources bool) error

	// FindReadyReplica (optionally) selects a KernelReplica of the specified Kernel to be
	// pre-designated as the leader of a code execution.
	//
	// If the returned KernelReplica is nil and the returned error is nil, then that indicates
	// that no KernelReplica is being pre-designated as the leader, and the KernelReplicas
	// will fight amongst themselves to determine the leader.
	//
	// If a non-nil KernelReplica is returned, then the "execute_request" messages that are
	// forwarded to that KernelReplica's peers should first be converted to "yield_request"
	// messages, thereby ensuring that the selected KernelReplica becomes the leader.
	//
	// FindReadyReplica also returns a map of ineligible replicas, or replicas that have already
	// been ruled out.
	//
	// PRECONDITION: The resource spec of the specified scheduling.Kernel should already be
	// updated (in cases where dynamic resource requests are supported) such that the current
	// resource spec reflects the requirements for this code execution. That is, the logic of
	// selecting a replica now depends upon the kernel's resource request correctly specifying
	// the requirements. If the requirements were to change after selection a replica, then
	// that could invalidate the selection.
	FindReadyReplica(kernel Kernel, executionId string) (KernelReplica, error)

	// ContainerPrewarmer returns the ContainerPrewarmer used by the BaseScheduler.
	ContainerPrewarmer() ContainerPrewarmer
}

type HostScheduler interface {
	// RequestNewHost adds a new Host to the Cluster.
	// We simulate this using node taints.
	RequestNewHost() error

	// RemoveHost removes a Host from the Cluster.
	// We simulate this using node taints.
	RemoveHost(hostId string) error

	// HostAdded is called by the Cluster when a new Host connects to the Cluster.
	HostAdded(host Host)

	// HostRemoved is called by the Cluster when a Host is removed from the Cluster.
	HostRemoved(host Host)

	// ReleaseIdleHosts Tries to release n idle hosts. Return the number of hosts that were actually released.
	// Error will be nil on success and non-nil if some sort of failure is encountered.
	ReleaseIdleHosts(n int32) (int, error)

	// GetCandidateHosts identifies candidate hosts for a particular kernel, reserving resources on hosts
	// before returning them.
	GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec) ([]Host, error)

	// GetCandidateHost identifies a single candidate host for a particular kernel replica, reserving resources on hosts
	// before returning them.
	//
	// If the specified replica's current scheduling.Host isn't already blacklisted, then GetCandidateHost will add it
	// to the blacklist.
	GetCandidateHost(replica KernelReplica, blacklistedHosts []Host, forTraining bool, createNewHostPermitted bool) (Host, error)

	// CanScaleIn returns true if scaling-in is possible now.
	CanScaleIn() bool
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

	// SetLastCapacityValidation is used to record that a capacity validation has occurred.
	SetLastCapacityValidation(time.Time)

	// NumCapacityValidation returns the number of times that the BaseScheduler has validated the scheduling.Cluster's
	// host capacity (and potentially invoked the auto-scaling policy).
	NumCapacityValidation() int64
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
