package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/elliotchance/orderedmap/v2"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/semaphore"
	"k8s.io/apimachinery/pkg/util/wait"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	InvalidationThreshold = 0.1

	OversubscribedIndexKey  types.HeapElementMetadataKey = "oversubscribed_index"
	UndersubscribedIndexKey types.HeapElementMetadataKey = "undersubscribed_index"
	IdleIndexKey            types.HeapElementMetadataKey = "idle_index"
)

var IdleHostMetadataKey types.HeapElementMetadataKey = "idle_host_metadata_key"

// schedulingNotification is a struct that is sent over a channel to notify the "main" goroutine handling the
// scheduling of a new kernel that the scheduling of one of that kernel's replicas has completed successfully.
type schedulingNotification struct {
	// SchedulingCompletedAt is the time at which the scheduling operation concluded
	// for the particular replica of the associated kernel.
	SchedulingCompletedAt time.Time

	// Host is the Host on which the kernel was scheduled (or attempted to be scheduled).
	Host scheduling.Host

	// Error is the error that occurred that caused the scheduling operation to fail.
	// This field will be nil if the scheduling operation was successful.
	Error error

	// KernelId is the ID of the kernel for which a replica was scheduled.
	KernelId string

	// ReplicaId is the SMR node ID of the replica that was scheduled (or whose scheduling was attempted but failed).
	ReplicaId int32

	// Successful indicates whether the operation succeeded or whether it failed.
	Successful bool
}

type KernelProvider interface {
	GetKernel(kernelId string) (scheduling.Kernel, bool)
}

type NotificationBroker interface {
	SendErrorNotification(errorName string, errorMessage string)
	SendInfoNotification(title string, message string)
}

type baseSchedulerBuilder struct {
	cluster                     scheduling.Cluster
	placer                      scheduling.Placer
	hostMapper                  HostMapper
	hostSpec                    types.Spec
	kernelProvider              KernelProvider
	clusterProvider             scheduling.ClusterProvider
	notificationBroker          NotificationBroker
	metricsProvider             scheduling.MetricsProvider
	activeExecutionProvider     scheduling.ActiveExecutionProvider
	schedulingPolicy            SchedulingPolicy // Optional, will be extracted from ClusterGatewayOptions if not specified.
	initialNumContainersPerHost int
	options                     *scheduling.SchedulerOptions
}

func newBaseSchedulerBuilder() *baseSchedulerBuilder {
	return &baseSchedulerBuilder{}
}

func (b *baseSchedulerBuilder) WithCluster(cluster scheduling.Cluster) *baseSchedulerBuilder {
	b.cluster = cluster
	return b
}

func (b *baseSchedulerBuilder) WithPlacer(placer scheduling.Placer) *baseSchedulerBuilder {
	b.placer = placer
	return b
}

func (b *baseSchedulerBuilder) WithHostMapper(hostMapper HostMapper) *baseSchedulerBuilder {
	b.hostMapper = hostMapper
	return b
}

func (b *baseSchedulerBuilder) WithHostSpec(hostSpec types.Spec) *baseSchedulerBuilder {
	b.hostSpec = hostSpec
	return b
}

func (b *baseSchedulerBuilder) WithSchedulingPolicy(schedulingPolicy SchedulingPolicy) *baseSchedulerBuilder {
	b.schedulingPolicy = schedulingPolicy
	return b
}

func (b *baseSchedulerBuilder) WithMetricsProvider(provider scheduling.MetricsProvider) *baseSchedulerBuilder {
	b.metricsProvider = provider
	return b
}

func (b *baseSchedulerBuilder) WithKernelProvider(kernelProvider KernelProvider) *baseSchedulerBuilder {
	b.kernelProvider = kernelProvider
	return b
}

func (b *baseSchedulerBuilder) WithNotificationBroker(notificationBroker NotificationBroker) *baseSchedulerBuilder {
	b.notificationBroker = notificationBroker
	return b
}

func (b *baseSchedulerBuilder) WithOptions(options *scheduling.SchedulerOptions) *baseSchedulerBuilder {
	b.options = options
	return b
}

func (b *baseSchedulerBuilder) WithClusterProvider(clusterProvider scheduling.ClusterProvider) *baseSchedulerBuilder {
	b.clusterProvider = clusterProvider
	return b
}

func (b *baseSchedulerBuilder) WithInitialNumContainersPerHost(initialNumContainersPerHost int) *baseSchedulerBuilder {
	b.initialNumContainersPerHost = initialNumContainersPerHost
	return b
}

// Build method
func (b *baseSchedulerBuilder) Build() *BaseScheduler {
	if b.options == nil {
		panic("Cannot construct BaseScheduler using baseSchedulerBuilder with nil options.")
	}

	// Automatically create a scheduling.ClusterProvider based on the cluster we were given, if we
	// do not already have a scheduling.ClusterProvider.
	if b.clusterProvider == nil {
		b.clusterProvider = func() scheduling.Cluster {
			return b.cluster
		}
	}

	if b.schedulingPolicy == nil {
		schedulingPolicy, err := GetSchedulingPolicy(b.options, b.clusterProvider)
		if err != nil {
			panic(err)
		}

		b.schedulingPolicy = schedulingPolicy.(SchedulingPolicy)
	}

	clusterScheduler := &BaseScheduler{
		cluster:                       b.cluster,
		hostMapper:                    b.hostMapper,
		stRatio:                       types.NewMovingStatFromWindow(5),
		opts:                          b.options,
		remoteSynchronizationInterval: time.Second * time.Duration(b.options.GpuPollIntervalSeconds),
		placer:                        b.placer,
		hostSpec:                      b.hostSpec,
		oversubscribed:                types.NewHeap(OversubscribedIndexKey),
		undersubscribed:               types.NewHeap(UndersubscribedIndexKey),
		idleHosts:                     types.NewHeap(IdleIndexKey),
		maxSubscribedRatio:            decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		subscriptionRatio:             decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		kernelProvider:                b.kernelProvider,
		notificationBroker:            b.notificationBroker,
		schedulingPolicy:              b.schedulingPolicy,
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	migrator := newKernelMigrator(b.kernelProvider, b.cluster, clusterScheduler, b.metricsProvider, int32(b.options.SMRPort))
	clusterScheduler.kernelMigrator = migrator

	b.buildPrewarmPolicy(clusterScheduler)

	if b.options.GpuPollIntervalSeconds <= 0 {
		clusterScheduler.remoteSynchronizationInterval = time.Second * 5
	}

	if clusterScheduler.log.GetLevel() == logger.LOG_LEVEL_ALL {
		clusterScheduler.log.Debug("Scheduling Configuration:")
		clusterScheduler.log.Debug("GpusPerHost: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().GpusPerHost)
		clusterScheduler.log.Debug("ScalingFactor: %.2f",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingFactor)
		clusterScheduler.log.Debug("ScalingLimit: %.2f",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingLimit)
		clusterScheduler.log.Debug("MaximumHostsToReleaseAtOnce: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().MaximumHostsToReleaseAtOnce)
		clusterScheduler.log.Debug("ScalingIntervalSec: %.3f",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec)
		clusterScheduler.log.Debug("PredictiveAutoscalingEnabled: %v",
			clusterScheduler.schedulingPolicy.SupportsPredictiveAutoscaling())
		clusterScheduler.log.Debug("ScalingBufferSize: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v",
			clusterScheduler.remoteSynchronizationInterval)
		clusterScheduler.log.Debug("Scheduling policy: %s",
			clusterScheduler.schedulingPolicy.Name())
	}

	return clusterScheduler
}

// buildPrewarmPolicy is called by Build and specifically configures/creates the prewarming policy.
func (b *baseSchedulerBuilder) buildPrewarmPolicy(clusterScheduler *BaseScheduler) {
	prewarmerConfig := prewarm.NewPrewarmerConfig(b.initialNumContainersPerHost, b.options.MaxPrewarmContainersPerHost,
		b.options.PrewarmRunIntervalSec)

	if b.options.PrewarmingEnabled {
		switch b.options.PrewarmingPolicy {
		case scheduling.MaintainMinCapacity.String():
			{
				clusterScheduler.log.Warn("Using \"%s\" pre-warming policy.", b.options.PrewarmingPolicy)

				minCapacityPrewarmerConfig := &prewarm.MinCapacityPrewarmerConfig{
					PrewarmerConfig:               prewarmerConfig,
					MinPrewarmedContainersPerHost: b.options.MinPrewarmContainersPerHost,
					DynamicallyMaintainCapacity:   b.options.DynamicallyMaintainCapacity,
					ReplenishOnUse:                b.options.ReplenishOnUse,
				}

				prewarmer := prewarm.NewMinCapacityPrewarmer(b.cluster, minCapacityPrewarmerConfig, b.metricsProvider)
				clusterScheduler.prewarmer = prewarmer
			}
		case scheduling.FixedCapacity.String():
			{
				clusterScheduler.log.Warn("Using \"%s\" pre-policy.", b.options.PrewarmingPolicy)

				fixedCapacityConfig := &prewarm.FixedCapacityPrewarmerConfig{
					PrewarmerConfig: prewarmerConfig,
					ReplenishOnUse:  b.options.ReplenishOnUse,
					Capacity:        int32(b.options.FixedCapacitySize),
				}

				prewarmer := prewarm.NewFixedCapacityPrewarmer(b.cluster, fixedCapacityConfig, b.metricsProvider)
				clusterScheduler.prewarmer = prewarmer
			}
		case scheduling.LittleLawCapacity.String():
			{
				clusterScheduler.log.Warn("Using \"%s\" pre-warming policy.", b.options.PrewarmingPolicy)

				littlesLawConfig := &prewarm.LittlesLawPrewarmerConfig{
					PrewarmerConfig: prewarmerConfig,
					W:               time.Duration(b.options.LittlesLawW * float64(time.Minute)),
					Lambda:          b.options.LittlesLawLambda,
				}

				prewarmer := prewarm.NewLittlesLawPrewarmer(b.cluster, littlesLawConfig, b.metricsProvider)
				clusterScheduler.prewarmer = prewarmer
			}
		case scheduling.NoMaintenance.String():
			{
				clusterScheduler.log.Warn("Using \"%s\" pre-warming policy.", b.options.PrewarmingPolicy)
				prewarmer := prewarm.NewBaseContainerPrewarmer(b.cluster, prewarmerConfig, b.metricsProvider)
				clusterScheduler.prewarmer = prewarmer
			}
		case "":
			{
				clusterScheduler.log.Warn("No pre-warming policy specified. Using default (i.e., none).")
				prewarmer := prewarm.NewBaseContainerPrewarmer(b.cluster, prewarmerConfig, b.metricsProvider)
				clusterScheduler.prewarmer = prewarmer
			}
		default:
			{
				panic(fmt.Sprintf("Unknown or unsupported prewarming policy: \"%s\"", b.options.PrewarmingPolicy))
			}
		}
	}
}

type BaseScheduler struct {
	lastNodeRefreshTime time.Time // The time at which the nodes were last refreshed.

	lastCapacityValidation time.Time // lastCapacityValidation is the time at which the last call to ValidateCapacity finished.
	instance               clusterSchedulerInternal
	cluster                scheduling.Cluster
	hostMapper             HostMapper
	placer                 scheduling.Placer
	kernelProvider         KernelProvider
	notificationBroker     NotificationBroker
	kernelMigrator         *kernelMigrator

	// schedulingPolicy specifies the scheduling behavior for the scheduling.Cluster and scheduling.Scheduler.
	schedulingPolicy SchedulingPolicy

	hostSpec types.Spec // The types.Spec used when creating new Host instances.

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerEventHandler scheduling.ContainerWatcher

	log logger.Logger

	opts *scheduling.SchedulerOptions // Configuration options.

	overUnderMutex  sync.Mutex
	oversubscribed  *types.Heap // The host index for oversubscribed hosts. Ordering is implemented by schedulerHost.
	undersubscribed *types.Heap // The host index for under-subscribed hosts. Ordering is implemented by schedulerHost.

	idleHostMutex sync.Mutex
	idleHosts     *types.Heap

	stRatio *types.MovingStat // session/training ratio

	// resourceBindingMode describes when resources are committed and uncommitted from Containers.
	resourceBindingMode scheduling.ResourceBindingMode

	subscriptionRatio             decimal.Decimal // Subscription ratio.
	maxSubscribedRatio            decimal.Decimal
	remoteSynchronizationInterval time.Duration // remoteSynchronizationInterval specifies how frequently to poll the remote scheduler nodes for updated GPU info.
	invalidated                   float64
	lastSubscribedRatio           float64
	pendingSubscribedRatio        float64

	// numCapacityValidations is a counter for the number of times we call scheduling.Policy::ValidateCapacity.
	numCapacityValidations atomic.Int64

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	prewarmer scheduling.ContainerPrewarmer

	canScaleIn bool // Can the Cluster/Placer scale-in?
}

func (s *BaseScheduler) GetResourceBindingMode() scheduling.ResourceBindingMode {
	return s.resourceBindingMode
}

func (s *BaseScheduler) sendErrorNotification(errorName string, errorMessage string) {
	if s.notificationBroker == nil {
		return
	}

	s.notificationBroker.SendErrorNotification(errorName, errorMessage)
}

func (s *BaseScheduler) sendInfoNotification(title string, message string) {
	if s.notificationBroker == nil {
		return
	}

	s.notificationBroker.SendInfoNotification(title, message)
}

func (s *BaseScheduler) WithNotificationBroker(notificationBroker NotificationBroker) {
	s.notificationBroker = notificationBroker
}

func (s *BaseScheduler) WithHostMapper(mapper HostMapper) {
	s.hostMapper = mapper
}

func (s *BaseScheduler) SetHostSpec(spec types.Spec) {
	s.hostSpec = spec
}

func (s *BaseScheduler) PolicyKey() scheduling.PolicyKey {
	return s.schedulingPolicy.PolicyKey()
}

func (s *BaseScheduler) Policy() scheduling.Policy {
	return s.schedulingPolicy
}

// GetOversubscriptionFactor returns the oversubscription factor calculated as the difference between
// the given ratio and the Cluster's current subscription ratio.
func (s *BaseScheduler) GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal {
	return ratio.Sub(s.subscriptionRatio)
}

// SubscriptionRatio returns the subscription ratio of the Cluster.
func (s *BaseScheduler) SubscriptionRatio() float64 {
	return s.subscriptionRatio.InexactFloat64()
}

// Placer returns the Placer used by the scheduling.Scheduler.
func (s *BaseScheduler) Placer() scheduling.Placer {
	return s.placer
}

func (s *BaseScheduler) setInstance(instance clusterSchedulerInternal) {
	s.instance = instance
}

// isScalingOutEnabled returns true if the scheduling.ResourceScalingPolicy of the configured scheduling.Policy permits
// scaling out.
func (s *BaseScheduler) isScalingOutEnabled() bool {
	return s.Policy().ResourceScalingPolicy().ScalingOutEnabled()
}

// isScalingInEnabled returns true if the scheduling.ResourceScalingPolicy of the configured scheduling.Policy permits
// scaling in.
func (s *BaseScheduler) isScalingInEnabled() bool {
	return s.Policy().ResourceScalingPolicy().ScalingInEnabled()
}

// FindReadyContainer selects one of the scheduling.KernelContainer instances of the specified scheduling.UserSession
// to handle a training event.
func (s *BaseScheduler) FindReadyContainer(_ scheduling.UserSession) scheduling.KernelContainer {
	panic("Not implemented.")
}

// UpdateIndex is used to update a Host's position in its index.
// It also updates the "idle hosts" heap.
func (s *BaseScheduler) UpdateIndex(host scheduling.Host) error {
	s.validate()

	old := s.loadPool(host)
	target, t := s.getPool(host)

	var meta types.HeapElementMetadataKey
	if old == s.oversubscribed {
		meta = OversubscribedIndexKey
	} else {
		meta = UndersubscribedIndexKey
	}

	if old == target {
		s.log.Debug("Updating host %v(%v of %d) in %v(len=%d)",
			host.GetNodeName(), host.GetIdx(meta), old.Len(), host.SchedulerPoolType().String(), old.Len())

		s.overUnderMutex.Lock()
		heap.Fix(old, host.GetIdx(meta))
		s.overUnderMutex.Unlock()

		s.log.Debug("Updated host %v (now %v of %d) in %v(len=%d)",
			host.GetNodeName(), host.GetIdx(meta), old.Len(), host.SchedulerPoolType().String(), old.Len())
	} else {
		s.log.Debug("Moving host %v(%d of %d) from %v(len=%d) to %v(len=%d)",
			host.GetNodeName(), host.GetIdx(meta), old.Len(), host.SchedulerPoolType().String(), old.Len(),
			t.String(), target.Len())

		s.overUnderMutex.Lock()
		heap.Remove(old, host.GetIdx(meta))
		s.overUnderMutex.Unlock()

		s.moveToPool(host, target, t)
	}

	s.idleHostMutex.Lock()
	heap.Fix(s.idleHosts, host.GetIdx(IdleHostMetadataKey))
	s.idleHostMutex.Unlock()

	s.placer.UpdateIndex(host)

	return nil
}

func (s *BaseScheduler) SearchForCandidateHosts(numToFind int, kernelSpec *proto.KernelSpec, forTraining bool) ([]scheduling.Host, error) {
	// Identify the hosts onto which we will place replicas of the kernel.
	return s.placer.FindHosts([]interface{}{}, kernelSpec, numToFind, forTraining)
}

// GetCandidateHosts returns a slice of scheduling.Host containing Host instances that could serve
// a Container (i.e., a kernel replica) with the given resource requirements (encoded as a types.Spec).
//
// GetCandidateHosts will automatically request that new Host instances be provisioned and added to the Cluster
// if it fails to find sufficiently many viable Host instances. This process will be attempted three times.
// If GetCandidateHosts is unsuccessful (at finding sufficiently many viable hosts) after those three attempts,
// then GetCandidateHosts will give up and return an error.
//
// The size of the returned slice will be equal to the configured number of replicas for each kernel (usually 3).
//
// This function is NOT idempotent. This reserves resources on the Hosts that are returned (before returning them).
func (s *BaseScheduler) GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec, numHosts int32,
	forTraining bool) ([]scheduling.Host, error) {

	var hosts []scheduling.Host

	// TODO: How many times should we really retry here? If we fail, try to scale out, and fail again,
	// 		 then shouldn't we just give up and leave it to the client to resubmit?
	maxAttempts := 5
	retryParameters := wait.Backoff{
		Duration: time.Duration(float64(s.cluster.MeanScaleOutTime()) * 0.75),
		Factor:   1.25,
		Jitter:   1.125,
		Steps:    maxAttempts,
		Cap:      time.Duration(float64(s.cluster.MeanScaleOutTime()) * 1.50),
	}

	for retryParameters.Steps > 0 && int32(len(hosts)) < numHosts {
		numHostsRequired := numHosts - int32(len(hosts))
		s.log.Debug("Searching for %d candidate host(s) for kernel %s. Have identified %d candidate host(s) so far.",
			numHostsRequired, kernelSpec.Id, len(hosts))

		// note: this function executes atomically.
		hostBatch, err := s.SearchForCandidateHosts(int(numHostsRequired), kernelSpec, forTraining)
		if err != nil {
			s.log.Error("Error while searching for %d candidate host(s) for kernel %s: %v",
				numHostsRequired, kernelSpec.Id, err)
			return nil, err
		}

		hosts = append(hosts, hostBatch...)

		if len(hosts) < s.schedulingPolicy.NumReplicas() {
			s.log.Warn("Found only %d/%d hosts to serve replicas of kernel %s so far (attempt=%d).",
				len(hosts), numHosts, kernelSpec.Id, maxAttempts-retryParameters.Steps+1)

			var shouldContinue bool
			shouldContinue, err = s.Policy().HandleFailedAttemptToGetViableHosts(ctx, kernelSpec, numHosts, hosts)
			if err != nil {
				s.log.Error("Error while handling failed attempt to search for %d candidate host(s) for kernel %s: %v",
					numHosts, kernelSpec.Id, err)
				return nil, err
			}

			if !shouldContinue {
				break
			}

			sleepInterval := retryParameters.Step()
			s.log.Debug("Sleeping for %v before retrying to find %d candidate host(s) for replica(s) of kernel %s.",
				sleepInterval, numHostsRequired, kernelSpec.Id)
			time.Sleep(sleepInterval)
		}

		s.log.Debug("Found %d/%d hosts to serve replicas of kernel %s: %v",
			len(hosts), numHosts, kernelSpec.Id, hosts)
	}

	// Check if we were able to reserve the requested number of hosts.
	// If not, then we need to release any hosts we did reserve.
	if int32(len(hosts)) < numHosts {
		s.log.Warn("Failed to find %d hosts to serve replicas of kernel %s after %d tries...",
			numHosts, kernelSpec.Id, maxAttempts-retryParameters.Steps+1)

		// Release any resource reservations that we created, since we're aborting the scheduling
		// of the replicas of the kernel.
		for _, host := range hosts {
			err := host.ReleaseReservation(kernelSpec)
			if err != nil {
				s.log.Error("Failed to release resource reservation for kernel %s on host %s (ID=%s): %v",
					kernelSpec.Id, host.GetNodeName(), host.GetID(), err)
			}

			err = s.UpdateIndex(host)
			if err != nil {
				s.log.Error("Error while attempting to update index of host %s (ID=%s): %v",
					host.GetNodeName(), host.GetID(), err)
			}
		}

		return nil, scheduling.ErrInsufficientHostsAvailable
	}

	return hosts, nil
}

// ReserveResourcesForReplica is used to instruct the KernelScheduler to explicitly reserve resources for a
// particular KernelReplica of a particular kernel.
//
// The primary use case for ReserveResourcesForReplica is when a specific scheduling.KernelReplica is specified to
// serve as the primary replica within the metadata of an "execute_request" message. This may occur because the user
// explicitly placed that metadata there, or following a migration when the ClusterGateway has a specific
// replica that should be able to serve the execution request.
//
// NOTE: TransactionResources are always COMMITTED to the target scheduling.KernelReplica when using ReserveResourcesForReplica.
//
// PRECONDITION: The specified scheduling.KernelReplica should already be scheduled on the scheduling.Host
// on which the resources are to be reserved.
func (s *BaseScheduler) ReserveResourcesForReplica(kernel scheduling.Kernel, replica scheduling.KernelReplica, commitResources bool,
	ignoreOversubscriptionRisk bool) error {

	err := s.placer.ReserveResourcesForReplica(kernel, replica, commitResources, ignoreOversubscriptionRisk)

	if err != nil {
		host := replica.Host()

		if host != nil {
			updateIndexErr := s.UpdateIndex(host)

			if updateIndexErr != nil {
				s.log.Error("Error updating index of host %s (ID=%s): %v")
			}
		}
	}

	return err
}

// DeployKernelReplicas is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *BaseScheduler) DeployKernelReplicas(ctx context.Context, kernel scheduling.Kernel, numReplicasToSchedule int32, blacklistedHosts []scheduling.Host) error {
	return s.instance.DeployKernelReplicas(ctx, kernel, numReplicasToSchedule, blacklistedHosts)
}

// RemoteSynchronizationInterval returns the interval at which the Scheduler synchronizes
// the Host instances within the Cluster with their remote nodes.
func (s *BaseScheduler) RemoteSynchronizationInterval() time.Duration {
	return s.remoteSynchronizationInterval
}

// MinimumCapacity returns the minimum number of nodes we must have available at any time.
func (s *BaseScheduler) MinimumCapacity() int32 {
	return s.schedulingPolicy.ScalingConfiguration().MinimumCapacity
}

// RequestNewHost adds a new node to the kubernetes Cluster.
// We simulate this using node taints.
func (s *BaseScheduler) RequestNewHost() error {
	p := s.cluster.RequestHosts(context.Background(), 1) /* s.hostSpec */

	result, err := p.Result()
	if err != nil {
		// If it is just a scheduling.ErrUnsupportedOperation, then we do not need to notify the frontend.
		// This is because Docker-based clusters cannot add new nodes themselves.
		if errors.Is(err, scheduling.ErrUnsupportedOperation) {
			return err
		}

		s.log.Error("Failed to add new host because: %v", err)
		s.sendErrorNotification("Failed to AddHost Host to Cluster", err.Error())

		return err
	}

	message := ""
	if result != nil {
		message = result.(ScaleOperationResult).String()
	}

	s.sendInfoNotification("Successfully Added Host to Cluster", message)
	return nil
}

// RemoveHost removes a new from the kubernetes Cluster.
// We simulate this using node taints.
func (s *BaseScheduler) RemoveHost(hostId string) error {
	s.log.Debug("Removing host with ID=\"%s\"", hostId)

	p := s.cluster.ReleaseSpecificHosts(context.Background(), []string{hostId})

	result, err := p.Result()
	if err != nil {
		// If the error isn't something trivial like there already being another concurrent scaling operation,
		// then just we'll just return the error.
		if !errors.Is(err, scheduling.ErrScalingActive) {
			s.sendErrorNotification(fmt.Sprintf("Failed to RemoveHost Host %s from the Cluster", hostId), err.Error())
			s.log.Error("Failed to remove host %s because: %v", hostId, err)
			return err
		}

		s.log.Warn("Failed to remove host %s because: %v", hostId, err)
		return err
	}

	message := ""
	if result != nil {
		message = result.(ScaleOperationResult).String()
	}

	s.sendInfoNotification(fmt.Sprintf("Successfully Removed Host %s from the Cluster", hostId), message)

	return nil
}

// RemoveReplicaFromHost removes the specified replica from its Host.
func (s *BaseScheduler) RemoveReplicaFromHost(kernelReplica scheduling.KernelReplica) error {
	s.log.Debug("Removing replica %d of kernel \"%s\" from its host: %v",
		kernelReplica.ReplicaID(), kernelReplica.ID(), kernelReplica.Host())

	return s.instance.RemoveReplicaFromHost(kernelReplica)
}

func (s *BaseScheduler) GetActiveAddReplicaOperationsForKernel(kernelId string) (*orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation], bool) {
	return s.kernelMigrator.GetActiveAddReplicaOperationsForKernel(kernelId)
}

func (s *BaseScheduler) GetAddReplicaOperation(id string) (*scheduling.AddReplicaOperation, bool) {
	return s.kernelMigrator.GetAddReplicaOperation(id)
}

func (s *BaseScheduler) GetAddReplicaOperationManager() hashmap.HashMap[string, *scheduling.AddReplicaOperation] {
	return s.kernelMigrator.GetAddReplicaOperationManager()
}

// UpdateRatio updates the Cluster's subscription ratio.
// UpdateRatio also validates the Cluster's overall capacity as well, scaling in or out as needed.
func (s *BaseScheduler) UpdateRatio(skipValidateCapacity bool) bool {
	var ratio float64
	if s.cluster.BusyGPUs() == 0 {
		//// Technically if the number of committed GPUs is zero, then the ratio is infinite (undefined).
		//// TODO: Previously, I'd just set the ratio to 0 if BusyGPUs was 0.
		//// But technically, it should be undefined/infinite, so I will try setting it to maxSubscribedRatio...
		ratio = s.maxSubscribedRatio.InexactFloat64()
		//ratio = 0

		//if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		//	s.log.Debug("DemandGPUs: %.0f. CommittedGPUs: %.0f. Ratio: %.4f.", s.cluster.DemandGPUs(),
		//		s.cluster.BusyGPUs(), ratio)
		//}
		//return false
	} else {
		// NOTE: DemandAndBusyGPUs computes the busy GPUs based on sessions that are actively training,
		// whereas BusyGPUs computes the quantity based on reservations on Hosts (i.e., pre-reserved
		// resources and whatnot).
		demandGpus, busyGpus1, numRunning, numIdle, numTraining := s.cluster.DemandAndBusyGPUs()
		busyGpus2 := s.cluster.BusyGPUs()

		var busyGPUs float64
		if busyGpus1 != busyGpus2 {
			s.log.Warn("Computed busy GPUs as %.0f from DemandAndBusyGPUs and %.0f from BusyGPUs...",
				busyGpus1, busyGpus2)

			// Pick the largest.
			busyGPUs = math.Max(busyGpus1, busyGpus2)
		} else {
			busyGPUs = busyGpus1 // Doesn't matter
		}

		if demandGpus < busyGPUs {
			// This can happen sometimes if we pre-reserve resources.
			s.log.Warn("Demand GPUs (%.0f) are somehow lower than busy GPUs (%.0f)...", demandGpus, busyGPUs)
			s.log.Warn("There are %d sessions, of which %d are idle and %d are training.",
				numRunning, numIdle, numTraining)

			ratio = 1.0 // Force to be 1.0
			s.log.Debug("DemandGPUs: %.0f (%.0f). CommittedGPUs: %.0f. Ratio: %.4f.",
				busyGPUs, demandGpus, busyGPUs, ratio)
		} else {
			ratio = demandGpus / busyGPUs
			s.log.Debug("DemandGPUs: %.0f. CommittedGPUs: %.0f. Ratio: %.4f.", demandGpus, busyGPUs, ratio)
		}
	}

	s.stRatio.Add(ratio)
	avg := s.stRatio.Avg()
	if s.stRatio.N() == s.stRatio.Window() {
		if !s.canScaleIn && s.schedulingPolicy.ResourceScalingPolicy().ScalingInEnabled() {
			s.log.Debug("We can now scale-in.")
			s.canScaleIn = true
		}
		previousSubscriptionRatio := s.subscriptionRatio
		s.subscriptionRatio = decimal.NewFromFloat(avg)

		if !previousSubscriptionRatio.Equal(s.subscriptionRatio) {
			s.log.Debug("Subscription Ratio updated: %s → %s",
				previousSubscriptionRatio.StringFixed(3), s.subscriptionRatio.StringFixed(3))
		} else {
			s.log.Debug("Subscription Ratio unchanged: %s", s.subscriptionRatio.StringFixed(3))
		}

		s.rebalance(avg)

		scalingInterval := s.schedulingPolicy.ScalingConfiguration().ScalingInterval
		scalingIntervalSec := s.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec
		itHasBeenAWhileSinceLastCapacityValidation := time.Since(s.lastCapacityValidation) >= scalingInterval
		if !skipValidateCapacity && scalingIntervalSec > 0 && itHasBeenAWhileSinceLastCapacityValidation {
			s.log.Debug("Validating capacity.")

			s.schedulingPolicy.ValidateCapacity(s.cluster)

			s.numCapacityValidations.Add(1)

			s.log.Debug("Time until next capacity validation: %v (interval=%v).",
				scalingInterval-time.Since(s.lastCapacityValidation), scalingInterval)
		}

		return true
	}
	return false
}

// NumCapacityValidation returns the number of times that the BaseScheduler has validated the scheduling.Cluster's
// host capacity (and potentially invoked the auto-scaling policy).
func (s *BaseScheduler) NumCapacityValidation() int64 {
	return s.numCapacityValidations.Load()
}

func (s *BaseScheduler) rebalance(newRatio float64) {
	s.pendingSubscribedRatio = newRatio
	s.invalidated = newRatio - s.lastSubscribedRatio
	s.log.Debug("Pending subscription ratio: %.4f. Invalidated: %.4f", s.pendingSubscribedRatio, s.invalidated)
}

// findViableHostForReplica is called at scheduling-time (rather than before we get to the point of scheduling, such
// as searching for viable hosts before trying to schedule the container).
//
// PRECONDITION: If we're finding a viable host for an existing replica, then the blacklisted hosts argument should
// be non-nil and should contain the replica's current/original host (in order to prevent the replica from being
// scheduled back onto the same host).
//
// This method searches for a viable training host and, if one is found, then that host is returned.
// Otherwise, an error is returned.
//
// If we fail to find a host, then we'll try to scale-out (if we're allowed).
func (s *BaseScheduler) findViableHostForReplica(replicaSpec scheduling.KernelReplica, blacklistedHosts []scheduling.Host,
	forTraining bool, createNewHostPermitted bool) (host scheduling.Host, failureReason error) {
	numTries := 0

	if blacklistedHosts == nil {
		blacklistedHosts = make([]scheduling.Host, 0)
	}

	ignoreOversubscriptionRisk := false

	// We'll try a few times if we keep scaling-out successfully but somehow manage to fail again and again.
	for numTries < 5 {
		host, failureReason = s.instance.selectViableHostForReplica(replicaSpec.KernelReplicaSpec(), blacklistedHosts,
			forTraining, ignoreOversubscriptionRisk)

		if host != nil {
			s.log.Debug("Found viable host for replica %d of kernel %s: host %s",
				replicaSpec.ReplicaID(), replicaSpec.ID(), host.GetNodeName())

			err := s.UpdateIndex(host)
			if err != nil {
				s.log.Error("Error while attempting to update index of host %s (ID=%s): %v",
					host.GetNodeName(), host.GetID(), err)
			}

			return host, nil
		}

		s.log.Warn("Failed to find viable host for replica %d of kernel %s (forTraining=%v): %v",
			replicaSpec.ReplicaID(), replicaSpec.ID(), forTraining, failureReason)

		if !s.isScalingOutEnabled() || !errors.Is(failureReason, scheduling.ErrInsufficientHostsAvailable) {
			return nil, failureReason
		}

		// Check if we're allowed to try to provision a new host.
		//
		// We usually are, but if we're idle-reclaiming a host and migrating replicas, then we don't want to scale-out,
		// as that would defeat the purposes of reclaiming the idle host.
		if errors.Is(failureReason, scheduling.ErrInsufficientHostsAvailable) && !createNewHostPermitted {
			s.log.Warn("Could not find viable host for replica %d of kernel %s due to a lack of available resources, "+
				"and createNewHostPermitted is false. Giving up.",
				replicaSpec.ReplicaID(), replicaSpec.ID())

			return nil, failureReason
		}

		s.log.Debug("Attempting to scale-out to provide host for replica %d of kernel %s (forTraining=%v).",
			replicaSpec.ReplicaID(), replicaSpec.ID(), forTraining)
		p := s.cluster.RequestHosts(context.Background(), 1)
		if err := p.Error(); err != nil {
			if errors.Is(err, scheduling.ErrScalingActive) || errors.Is(err, scheduling.ErrUnsupportedOperation) {
				s.log.Warn("Cluster failed to provision 1 additional host (for replica %d of kernel %s) because: %v",
					replicaSpec.ReplicaID(), replicaSpec.ID(), err)

				// We failed to scale out because we CAN'T scale-out.
				// Let's try one more time, this time ignoring the risk of over-subscribing the hosts, because
				// we literally have no other options, and there may be some hosts with idle GPUs available
				// that we skipped over because we didn't want to oversubscribe them too much.
				if errors.Is(err, scheduling.ErrUnsupportedOperation) && !ignoreOversubscriptionRisk {
					// Try again, this time ignoring the risk of oversubscribing the host(s).
					ignoreOversubscriptionRisk = true
					numTries += 1
					continue
				}
			} else {
				s.log.Error("Cluster failed to provision 1 additional host (for replica %d of kernel %s) because: %v",
					replicaSpec.ReplicaID(), replicaSpec.ID(), err)
			}

			return nil, err
		}

		numTries += 1
	}

	return nil, failureReason
}

// MigrateKernelReplica tries to migrate the given kernel to another Host.
//
// The first error that is returned (i.e., 'reason') does not indicate that an actual error occurred.
// It simply provides an explanation for why the migration failed.
//
// The second error that is returned (i.e., 'err') indicates that an actual error occurs.
func (s *BaseScheduler) MigrateKernelReplica(ctx context.Context, kernelReplica scheduling.KernelReplica,
	targetHostId string, forTraining bool, createNewHostPermitted bool) (resp *proto.MigrateKernelResponse, reason error, err error) {

	args := &MigrationArgs{
		kernelReplica:          kernelReplica,
		targetHostId:           targetHostId,
		forTraining:            forTraining,
		createNewHostPermitted: createNewHostPermitted,
	}
	return s.kernelMigrator.MigrateKernelReplica(ctx, args)
}

// HostRemoved is called by the Cluster when a Host is removed from the Cluster.
func (s *BaseScheduler) HostRemoved(host scheduling.Host) {
	s.instance.HostRemoved(host)

	s.validate()
	old := s.loadPool(host)

	var meta types.HeapElementMetadataKey
	if old == s.oversubscribed {
		meta = OversubscribedIndexKey
	} else {
		meta = UndersubscribedIndexKey
	}

	s.overUnderMutex.Lock()
	defer s.overUnderMutex.Unlock()

	s.log.Trace("Removing host %v(%v of %d) from %v",
		host.GetNodeName(), host.GetIdx(meta), old.Len(), host.SchedulerPoolType())

	heap.Remove(old, host.GetIdx(meta))
}

// baseHostAdded should be called by HostAdded in classes that promote a *BaseScheduler.
func (s *BaseScheduler) baseHostAdded(host scheduling.Host) {
	s.log.Debug("Host %s (ID=%s) has been added.", host.GetNodeName(), host.GetID())
	s.idleHostMutex.Lock()
	heap.Push(s.idleHosts, &idleSortedHost{Host: host})
	s.idleHostMutex.Unlock()
	s.log.Debug("Length of idle hosts: %d", s.idleHosts.Len())

	s.validate()
	target, t := s.getPool(host)
	s.moveToPool(host, target, t)

	s.log.Debug("Added host %s (%v of %d) to %v",
		host.GetNodeName(), host.GetIdx(OversubscribedIndexKey), target.Len(), t.String())
}

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *BaseScheduler) HostAdded(host scheduling.Host) {
	s.instance.HostAdded(host)
}

func (s *BaseScheduler) moveToPool(host scheduling.Host, pool heap.Interface, t scheduling.SchedulerPoolType) {
	s.overUnderMutex.Lock()
	defer s.overUnderMutex.Unlock()

	host.SetSchedulerPoolType(t)
	s.log.Debug("Moving host %s to pool with type %s. NumContainers: %d. Host's S-Ratio: %.4f. Host's OSFactor: %s.",
		host.GetNodeName(), t.String(), host.NumContainers(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))
	heap.Push(pool, host)
	s.log.Debug("Moved host %s to pool with type %s. NumContainers: %d. Host's S-Ratio: %.4f. Host's OSFactor: %s.",
		host.GetNodeName(), t.String(), host.NumContainers(), host.SubscribedRatio(), host.OversubscriptionFactor().StringFixed(4))
}

// SetLastCapacityValidation is used to record that a capacity validation has occurred.
func (s *BaseScheduler) SetLastCapacityValidation(ts time.Time) {
	s.lastCapacityValidation = ts
}

// CanScaleIn returns true if scaling-in is possible now.
func (s *BaseScheduler) CanScaleIn() bool {
	return s.canScaleIn
}

// designateSubscriptionPoolType places the specified Host into the specified scheduler pool (i.e., oversubscribed
// or undersubscribed).
func (s *BaseScheduler) designateSubscriptionPoolType(host scheduling.Host, pool heap.Interface, t scheduling.SchedulerPoolType) {
	host.SetSchedulerPoolType(t)

	s.overUnderMutex.Lock()
	heap.Push(pool, host)
	s.overUnderMutex.Unlock()
}

func (s *BaseScheduler) validate() {
	if s.invalidated > InvalidationThreshold {
		// StaticPlacerMaxSubscribedRatio increase, release oversubscribed hosts to under-subscribed hosts.
		s.log.Debug("Apply subscription ratio change %.4f -> %.4f, add under-subscription hosts to candidate pool",
			s.lastSubscribedRatio, s.pendingSubscribedRatio)

		for s.oversubscribed.Len() > 0 && s.oversubscribed.Peek().(scheduling.Host).OversubscriptionFactor().LessThan(decimal.Zero) {
			s.overUnderMutex.Lock()
			host := heap.Pop(s.oversubscribed)
			s.overUnderMutex.Unlock()

			s.log.Debug("Designating host %s as 'undersubscribed'", host.(scheduling.Host).GetNodeName())
			s.designateSubscriptionPoolType(host.(scheduling.Host), s.undersubscribed, scheduling.SchedulerPoolTypeUndersubscribed)
		}

		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
	} else if s.invalidated < (-1 * InvalidationThreshold) {
		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
	}
}

func (s *BaseScheduler) getPool(host scheduling.Host) (heap.Interface, scheduling.SchedulerPoolType) {
	if host.OversubscriptionFactor().GreaterThan(decimal.Zero) {
		return s.oversubscribed, scheduling.SchedulerPoolTypeOversubscribed
	} else {
		return s.undersubscribed, scheduling.SchedulerPoolTypeUndersubscribed
	}
}

func (s *BaseScheduler) loadPool(host scheduling.Host) heap.Interface {
	switch host.SchedulerPoolType() {
	case scheduling.SchedulerPoolTypeUndersubscribed:
		return s.undersubscribed
	default:
		return s.oversubscribed
	}
}

type idleSortedHost struct {
	scheduling.Host
}

func (h *idleSortedHost) Compare(other interface{}) float64 {
	// MaxHeap based on the number of idle GPUs.
	diff := other.(*idleSortedHost).IdleGPUs() - h.IdleGPUs()
	if diff != 0 {
		return diff
	}

	// MaxHeap based on the subscription ratio, in order to promote rebalancing.
	diff = other.(scheduling.Host).SubscribedRatio() - h.SubscribedRatio()
	if diff != 0 {
		return diff
	}

	// MinHeap based on the number of containers. Fewer containers means less overhead to migrate.
	diff = float64(h.NumContainers() - other.(scheduling.Host).NumContainers())
	if diff != 0 {
		return diff
	}

	// MinHeap based on the number of active scheduling operations.
	// Hosts being considered in fewer scheduling operations can be considered more idle.
	diff = float64(h.NumActiveSchedulingOperations() - other.(scheduling.Host).NumActiveSchedulingOperations())

	return diff
}

func (h *idleSortedHost) SetIdx(key types.HeapElementMetadataKey, idx int) {
	h.Host.SetIdx(key, idx)
}

func (h *idleSortedHost) GetIdx(key types.HeapElementMetadataKey) int {
	return h.Host.GetIdx(key)
}

// migrateContainersFromIdleHost attempts to migrate all the kernels scheduled on the specified Host to other Hosts.
func (s *BaseScheduler) migrateContainersFromIdleHost(host scheduling.Host, forTraining bool) error {
	numContainersToMigrate := host.Containers().Len()

	// If there are no containers to migrate, then we're done.
	if numContainersToMigrate == 0 {
		s.log.Debug("There are no containers on host %s to migrate.", host.GetNodeName())
		return nil
	}

	getMigrationTarget := func(containerId string, container scheduling.KernelContainer) (scheduling.Host, error) {
		// Get a candidate host, but do not allow the cluster to provision new hosts/scale out, as that would
		// defeat the purpose of idle-reclaiming this host.
		return s.findViableHostForReplica(container.GetClient(), []scheduling.Host{host}, false, false)
	}

	type MigrationTarget struct {
		TargetHost scheduling.Host
		Container  scheduling.KernelContainer
	}

	migrationTargets := make(map[string]*MigrationTarget, host.Containers().Len())

	// This actually migrates the container.
	migrateContainer := func(containerId string, container scheduling.KernelContainer) error {
		migrationTarget := migrationTargets[containerId]

		// Pass true for `noNewHost`, as we don't want to create a new host for this.
		// (We already have a migration target selected, so it shouldn't matter, but still.)
		_, failedMigrationReason, err := s.MigrateKernelReplica(context.Background(), container.GetClient(),
			migrationTarget.TargetHost.GetID(), forTraining, false)

		if err != nil {
			// We cannot migrate the Container due to an actual error.
			s.log.Error("Abandoning the release of idle host %s (ID=%s) because we encountered an error while migrating one of the containers: %v",
				host.GetNodeName(), host.GetID(), err)
			return err
		}

		if failedMigrationReason != nil {
			// We cannot migrate the Container.
			s.log.Warn("Abandoning the release of idle host %s (ID=%s) because: %v",
				host.GetNodeName(), host.GetID(), err)
			return failedMigrationReason
		}

		s.log.Debug("Successfully migrated replica %d of kernel %s off of host %s.",
			container.ReplicaId(), container.KernelID(), host.GetNodeName())

		return nil
	}

	// Do one pass where we just check for actively-training kernels, so we don't waste time trying to reserve
	// resources on hosts only to abort everything because another kernel is actively training.
	foundActiveTraining := false
	host.Containers().Range(func(containerId string, container scheduling.KernelContainer) bool {
		kernel, loaded := s.kernelProvider.GetKernel(container.KernelID())
		if !loaded {
			s.log.Error("Could not find kernel \"%s\" associated with container \"%s\" that we're trying to migrate...",
				container.KernelID(), containerId)

			foundActiveTraining = true
			return false
		}

		// Verify that none of the kernels are actively training.
		if kernel.HasActiveTraining() {
			s.log.Debug("Kernel \"%s\" is actively training. We should not migrate replica %d, even if that replica is not the primary replica.",
				kernel.ID(), container.ReplicaId())

			foundActiveTraining = true
			return false
		}

		return true
	})

	if foundActiveTraining {
		s.log.Warn("Found at least one container whose kernel is actively training. Aborting migration of idle host \"%s\".",
			host.GetNodeName())

		return fmt.Errorf("%w: %w", scheduling.ErrMigrationFailed, scheduling.ErrAssociatedKernelActiveTraining)
	}

	aborted := false
	// Called if we have to abort the migration because we couldn't find a viable target for some replica.
	abortMigrationOperation := func() {
		s.log.Warn("Aborting migration of containers from idle host %s. Releasing %d reservation(s).",
			host.GetNodeName(), len(migrationTargets))

		aborted = true

		counter := 0
		for containerId, migrationTarget := range migrationTargets {
			container := migrationTarget.Container
			targetHost := migrationTarget.TargetHost

			replica := container.GetClient()

			s.log.Debug("Releasing reservation made for container %s on host %s (ID=%s) (migration cancelled).",
				containerId, host.GetNodeName(), host.GetID())

			err := targetHost.ReleaseReservation(replica.KernelSpec())
			if err != nil {
				s.log.Error("Failed to release reservation %d/%d: container %s on host %s: %v",
					counter+1, len(migrationTargets), containerId, host.GetNodeName(), err)
			} else {
				s.log.Debug("Successfully released reservation %d/%d: container %s on host %s: %v",
					counter+1, len(migrationTargets), containerId, host.GetNodeName(), err)
			}

			counter += 1
		}
	}

	// First, identify target hosts for all the replicas that need to be migrated.
	// If any of them fail to find a target, then we'll abort.
	// AddHost all the containers (that we need to migrate) to the work queue BEFORE creating the workers.
	host.Containers().Range(func(containerId string, container scheduling.KernelContainer) bool {
		s.log.Debug("Searching for migration target for container %s from idle host %s.",
			containerId, host.GetNodeName())

		kernel, loaded := s.kernelProvider.GetKernel(container.KernelID())
		if !loaded {
			s.log.Error("Could not find kernel \"%s\" associated with container \"%s\" that we're trying to migrate...",
				container.KernelID(), containerId)

			// Failed to find a viable migration target, so we must abort.
			abortMigrationOperation()
			return false
		}

		// Verify that none of the kernels are actively training.
		if kernel.HasActiveTraining() {
			s.log.Debug("Kernel \"%s\" is actively training. We should not migrate replica %d, even if that replica is not the primary replica.",
				kernel.ID(), container.ReplicaId())

			// Failed to find a viable migration target, so we must abort.
			abortMigrationOperation()
			foundActiveTraining = true
			return false
		}

		targetHost, err := getMigrationTarget(containerId, container)
		if err != nil {
			s.log.Warn("Could not find viable migration target for container %s; aborting migration of all containers from idle host %s: %v",
				containerId, host.GetNodeName(), err)

			// Failed to find a viable migration target, so we must abort.
			abortMigrationOperation()
			return false
		}

		s.log.Debug("Identified migration target for container %s: host %s",
			container, targetHost.GetNodeName())

		migrationTargets[containerId] = &MigrationTarget{
			TargetHost: targetHost,
			Container:  container,
		}

		return true
	})

	if aborted || foundActiveTraining {
		s.log.Warn("Operation to migrate all %d container(s) from idle host %s has been aborted.",
			host.Containers().Len(), host.GetNodeName())

		if foundActiveTraining {
			s.log.Warn("Found at least one container whose kernel is actively training. Aborting migration of idle host \"%s\".",
				host.GetNodeName())

			return fmt.Errorf("%w: %w", scheduling.ErrMigrationFailed, scheduling.ErrAssociatedKernelActiveTraining)
		}

		return fmt.Errorf("%w: failed to find viable migration targets for one or more containers",
			scheduling.ErrMigrationFailed)
	}

	// If there's just one, then just migrate the one container.
	if numContainersToMigrate == 1 {
		s.log.Debug("There's just one container on idle host %s to migrate.", host.GetNodeName())

		var err error
		host.Containers().Range(func(containerId string, container scheduling.KernelContainer) (contd bool) {
			err = migrateContainer(containerId, container)

			// There should just be one container to migrate, so we should ultimately stop looping immediately
			// either way.
			return err == nil
		})

		return err
	}

	// One more pass now that we may have spent a bunch more time pre-reserving resources.
	host.Containers().Range(func(containerId string, container scheduling.KernelContainer) bool {
		kernel, loaded := s.kernelProvider.GetKernel(container.KernelID())
		if !loaded {
			s.log.Error("Could not find kernel \"%s\" associated with container \"%s\" that we're trying to migrate...",
				container.KernelID(), containerId)

			foundActiveTraining = true
			//aborted = true
			abortMigrationOperation()
			return false
		}

		// Verify that none of the kernels are actively training.
		if kernel.HasActiveTraining() {
			s.log.Debug("Kernel \"%s\" is actively training. We should not migrate replica %d, even if that replica is not the primary replica.",
				kernel.ID(), container.ReplicaId())

			foundActiveTraining = true
			//aborted = true
			abortMigrationOperation()
			return false
		}

		return true
	})

	if aborted || foundActiveTraining {
		s.log.Warn("Operation to migrate all %d container(s) from idle host %s has been aborted.",
			host.Containers().Len(), host.GetNodeName())

		if foundActiveTraining {
			s.log.Warn("Found at least one container whose kernel is actively training. Aborting migration of idle host \"%s\".",
				host.GetNodeName())

			return fmt.Errorf("%w: %w", scheduling.ErrMigrationFailed, scheduling.ErrAssociatedKernelActiveTraining)
		}

		return fmt.Errorf("%w: failed to find viable migration targets for one or more containers",
			scheduling.ErrMigrationFailed)
	}

	var nWorkers int
	if numContainersToMigrate == 2 {
		nWorkers = 2
	} else {
		nWorkers = numContainersToMigrate

		if nWorkers > 8 {
			nWorkers = 8
		}
	}

	timeoutInterval := time.Minute * time.Duration(float64(numContainersToMigrate)*2.5)
	ctx, cancel := context.WithTimeout(context.Background(), timeoutInterval)
	defer cancel()

	workerDoneSemaphore := semaphore.NewWeighted(int64(nWorkers))
	numContainersMigratedSuccessfully := atomic.Int32{}
	errorOccurred := atomic.Bool{}
	workQueue := make(chan scheduling.KernelContainer, numContainersToMigrate)
	errorChan := make(chan error, nWorkers)

	var startTime time.Time
	migrationWorker := func(workerId int) {
		var numContainersMigrated int

		for !errorOccurred.Load() {
			select {
			case container := <-workQueue:
				{
					// If another worker failed to migrate a kernel replica, then we might as well give up.
					if errorOccurred.Load() {
						s.log.Warn("Migration Worker #%d of idle Host %s is exiting because another worker failed to migrate a kernel replica.",
							workerId, host.GetNodeName())

						// Increment the semaphore to signal to the main goroutine that we're done.
						workerDoneSemaphore.Release(1)
						return
					}

					startMigrateTime := time.Now()

					s.log.Debug("Migration Worker #%d of idle Host %s is migrating container %s now.",
						workerId, host.GetNodeName(), container.ContainerID())

					// Migrate the container.
					err := migrateContainer(container.ContainerID(), container)

					// Check if we failed to migrate.
					if err != nil {
						errorChan <- err
						errorOccurred.Store(true)

						s.log.Warn("Migration Worker #%d for idle Host %s failed to migrate replica %d of kernel %s after %v: %v.",
							workerId, host.GetNodeName(), container.ReplicaId(), container.KernelID(), time.Since(startMigrateTime), err)
						s.log.Warn("Migration Worker #%d for idle Host %s is aborting. Number of containers migrated (by worker #%d): %d. Total time elapsed: %v.",
							workerId, host.GetNodeName(), numContainersMigrated, workerId, time.Since(startTime))

						// Increment the semaphore to signal to the main goroutine that we're done.
						workerDoneSemaphore.Release(1)
						return
					}

					s.log.Debug("Migration Worker #%d for idle Host %s successfully migrated replica %d of kernel %s in %v. Total time elapsed: %v.",
						workerId, host.GetNodeName(), container.ReplicaId(), container.KernelID(), time.Since(startMigrateTime), time.Since(startTime))

					numContainersMigrated += 1
					numContainersMigratedSuccessfully.Add(1)
				}
			default:
				{
					s.log.Debug("Migration Worker #%d for idle Host %s is done. Number of containers migrated: %d. Time elapsed: %v.",
						workerId, host.GetNodeName(), numContainersMigrated, time.Since(startTime))

					// Increment the semaphore to signal to the main goroutine that we're done.
					workerDoneSemaphore.Release(1)
					return
				}
			}
		}
	}

	s.log.Debug("Parallelizing the migration of %d containers from idle host %s using %d workers.",
		numContainersToMigrate, host.GetNodeName(), nWorkers)

	// AddHost all the containers (that we need to migrate) to the work queue BEFORE creating the workers.
	host.Containers().Range(func(containerId string, container scheduling.KernelContainer) bool {
		workQueue <- container
		return true
	})

	startTime = time.Now()
	for i := 0; i < nWorkers; i++ {
		// Call acquire to decrement the semaphore.
		// Each worker will increment the semaphore when it exits.
		err := workerDoneSemaphore.Acquire(ctx, 1)
		if err != nil {
			// There shouldn't be any errors here.
			panic(err)
		}

		// Start the worker.
		go migrationWorker(i + 1)
	}

	s.log.Debug("Started %d workers to migrate %d kernel replicas from idle host %s. Waiting for up to %v.",
		nWorkers, numContainersToMigrate, host.GetNodeName(), timeoutInterval)

	// Block until all workers are done, or until the operation times out.
	err := workerDoneSemaphore.Acquire(ctx, int64(nWorkers))

	// If there was an error (i.e., time-out) or we haven't migrated all the containers yet,
	// then we'll return a timed-out error.
	if err != nil || numContainersMigratedSuccessfully.Load() < int32(numContainersToMigrate) {
		s.log.Debug("Timed out waiting for %d worker(s) to migrate %d containers from idle host %s. Time elapsed: %v. Number of successful migrations: %d. %v",
			nWorkers, numContainersToMigrate, host.GetNodeName(), time.Since(startTime), numContainersMigratedSuccessfully.Load(), err)

		return fmt.Errorf("%w: migration of %d containers from idle host %s timed out after %v: %v",
			types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime), err)
	}

	err = nil
	nErrors := 0
	for {
		select {
		// If there are one or more errors, then we'll join all the errors together.
		case migrationError := <-errorChan:
			{
				// If we haven't initialized the error variable yet, then do so now.
				if err == nil {
					err = migrationError
					nErrors = 1
					continue
				}

				// Join the errors together.
				err = errors.Join(err, migrationError)
				nErrors += 1
			}
		default:
			{
				// Default case: no errors in the channel.
				// If the error variable is still nil, then the operation must have been successful.
				if err == nil {
					// Sanity check.
					//
					// The number of successful migrations should not be less than the target by this point.
					// We already checked for this up above.
					if numContainersMigratedSuccessfully.Load() < int32(numContainersToMigrate) {
						s.log.Error("Expected for the number of migrated containers (%d) to equal the target (%d)",
							numContainersMigratedSuccessfully.Load(), numContainersToMigrate)

						return fmt.Errorf("%w: migration of %d containers from idle host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					// Sanity check.
					//
					// This should NEVER happen.
					if numContainersMigratedSuccessfully.Load() > int32(numContainersToMigrate) {
						s.log.Error("Number of successful migrations (%d) of containers from idle host %s is somehow > than target (%d)",
							numContainersMigratedSuccessfully.Load(), host.GetNodeName(), numContainersToMigrate)

						return fmt.Errorf("%w: migration of %d containers from idle host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					// Sanity check.
					//
					// There should be no more containers on the host now.
					if host.NumContainers() > 0 {
						s.log.Error("Host %s still has %d container(s), but we should've migrated all of them...",
							host.GetNodeName(), host.NumContainers())

						return fmt.Errorf("%w: migration of %d containers from idle host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					s.log.Debug("Successfully migrated all %d kernel replica(s) from idle host %s in %v.",
						numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					return nil
				}

				// The error variable was not nil, so 1+ migrations failed.
				s.log.Warn("At least %d error(s) occurred while trying to migrate all %d kernel replica(s) from idle host %s: %v",
					nErrors, numContainersToMigrate, host.GetNodeName(), err)
				return err
			}
		}
	}
}

// includeHostsInScheduling iterates over the given slice of scheduling.Host instances and sets their
// ExcludedFromScheduling field to false.
//
// includeHostsInScheduling then pushes the host back into the idleHosts heap.
func (s *BaseScheduler) includeHostsInScheduling(hosts []scheduling.Host, addBackToIdleHostsHeap bool) {
	for _, host := range hosts {
		err := host.IncludeForScheduling()
		if err != nil {
			s.log.Error("Host %s (ID=%s) is already allowed to be considered for scheduling (%v)",
				host.GetNodeName(), host.GetID(), err)
			continue
		}

		if addBackToIdleHostsHeap {
			s.idleHostMutex.Lock()
			heap.Push(s.idleHosts, &idleSortedHost{Host: host})
			s.idleHostMutex.Unlock()

			s.log.Debug("Added host %s back to 'idle hosts' heap.", host.GetNodeName())

			updateIndexErr := s.UpdateIndex(host)

			if updateIndexErr != nil {
				s.log.Error("Error while attempting to update index of host %s: %v", host.GetNodeName(), err)
			}
		}
	}
}

// ContainerPrewarmer returns the ContainerPrewarmer used by the BaseScheduler.
func (s *BaseScheduler) ContainerPrewarmer() scheduling.ContainerPrewarmer {
	return s.prewarmer
}

func (s *BaseScheduler) releaseIdleHost(host scheduling.Host) error {
	s.log.Debug("Releasing idle host %s (ID=%s)", host.GetNodeName(), host.GetID())

	// If the host has no containers running on it at all, then we can simply release the host.
	var err error
	if host.NumContainers() > 0 {
		s.log.Debug("Must migrate %d container(s) from idle host %s before we remove it.",
			host.NumContainers(), host.GetNodeName())

		err = s.migrateContainersFromIdleHost(host, false) // Host is completely idle, so no training.
		if err != nil {
			s.log.Warn("Failed to migrate all kernels from host %s because: %v", host.GetNodeName(), err)
			return err
		}
	}

	var (
		numTries    = 0
		maxNumTries = 16
	)

	for numTries < maxNumTries {
		err = s.RemoveHost(host.GetID())
		if err == nil {
			return nil
		}

		if errors.Is(err, scheduling.ErrScalingActive) {
			// If we've hit our maximum number of tries, then we'll just give up on this host.
			// Don't need to sleep again for no reason.
			if (numTries + 1) >= maxNumTries {
				s.log.Debug("Cannot release idle host %s because there's already an active scaling operation.",
					host.GetNodeName())
				return err
			}

			// We'll wait for a bit and try again.
			// Operation isn't necessarily a scale-out operation, but that's fine.
			sleepInterval := (time.Duration(s.cluster.MeanScaleOutTime().Seconds()/4) * time.Second) + (time.Millisecond * time.Duration(rand.Int31n(1250)))

			s.log.Debug("Cannot release idle host %s because there's already an active scaling operation. "+
				"Will sleep for %v before trying again.", host.GetNodeName(), sleepInterval)
			time.Sleep(sleepInterval)

			numTries += 1
			continue
		}

		// Some other error (i.e., not just an ErrScalingActive).
		s.log.Error("Failed to remove host %s because: %v", host.GetNodeName(), err)
		return err
	}

	s.log.Warn("Could not manage to release idle host %s within %d tries due to scaling contention...",
		host.GetNodeName(), maxNumTries)
	return err
}

// ReleaseIdleHosts tries to release n idle hosts. Return the number of hosts that were actually released.
// Error will be nil on success and non-nil if some sort of failure is encountered.
func (s *BaseScheduler) ReleaseIdleHosts(n int32) (int, error) {
	s.validate()

	s.idleHostMutex.Lock()
	// For now, just ensure the heap is in a valid order.
	heap.Init(s.idleHosts)
	s.idleHostMutex.Unlock()

	s.log.Debug("Attempting to release %d idle host(s). Currently %d host(s) in the Cluster. Length of idle hosts: %d.",
		n, s.cluster.Len(), s.idleHosts.Len())

	var (
		err         error
		numReleased int32
	)

	for numReleased < n {
		idleHost := s.idleHosts.Peek().(*idleSortedHost)

		excluded := idleHost.Host.ExcludeFromScheduling()
		if !excluded {
			s.log.Debug("Host \"%s\" (ID=%s) is ineligible for release: it's being considered in >= 1 scheduling operation(s).",
				idleHost.Host.GetNodeName(), idleHost.Host.GetID())
			break
		}

		// If the host is not completely idle, then we'll break and stop looking.
		if idleHost.IdleGPUs() < idleHost.ResourceSpec().GPU() || idleHost.CommittedGPUs() > 0 {
			s.includeHostsInScheduling([]scheduling.Host{idleHost.Host}, false) // Re-include it in scheduling operations.
			break
		}

		// If containers are only created for a single training event, then the fact that there are containers here
		// indicates that they're going to be training. So, the host is not truly idle.
		if s.schedulingPolicy.ContainerLifetime() == scheduling.SingleTrainingEvent && idleHost.NumContainers() > 0 {
			s.includeHostsInScheduling([]scheduling.Host{idleHost.Host}, false) // Re-include it in scheduling operations.
			break
		}

		// If there are too many containers on it, then it isn't necessarily worth the overhead of migrating the host.
		if idleHost.NumContainers() > 4 {
			s.includeHostsInScheduling([]scheduling.Host{idleHost.Host}, false) // Re-include it in scheduling operations.
			break
		}

		s.log.Debug("Selected host \"%s\" (ID=%s) as candidate for release.",
			idleHost.GetNodeName(), idleHost.GetID())

		// RemoveHost the host so that we can get to the next host.
		s.idleHostMutex.Lock()
		tmpHost := heap.Pop(s.idleHosts)
		s.idleHostMutex.Unlock()

		// Sanity check.
		if tmpHost.(scheduling.Host).GetID() != idleHost.GetID() {
			panic("Host popped off of idleHosts heap does not equal host peeked from idleHosts.")
		}

		s.log.Debug("Releasing idle host %d/%d: host %s. NumContainers: %d.",
			numReleased+1, n, idleHost.Host.GetNodeName(), idleHost.Host.NumContainers())

		err = s.releaseIdleHost(idleHost.Host)
		if err != nil {
			s.log.Warn("Could not release idle host \"%s\" because: %v", idleHost.Host.GetNodeName(), err)
			s.includeHostsInScheduling([]scheduling.Host{idleHost.Host}, true)
			break
		}

		s.log.Debug("Successfully released idle host %d/%d: host %s.",
			numReleased+1, n, idleHost.Host.GetNodeName())
		numReleased += 1
	}

	var logMethod func(format string, args ...interface{})
	if numReleased > 0 {
		logMethod = s.log.Debug
	} else {
		logMethod = s.log.Warn
	}
	logMethod("Released %d/%d idle host(s).", numReleased, n)

	return int(numReleased), err
}

// SelectReplicaForMigration selects a KernelReplica of the specified kernel to be migrated.
func (s *BaseScheduler) SelectReplicaForMigration(kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	return s.schedulingPolicy.SelectReplicaForMigration(kernel)
}

// FindReadyReplica (optionally) selects a KernelReplica of the specified kernel to be
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
// PRECONDITION: The resource spec of the specified scheduling.Kernel should already be
// updated (in cases where dynamic resource requests are supported) such that the current
// resource spec reflects the requirements for this code execution. That is, the logic of
// selecting a replica now depends upon the kernel's resource request correctly specifying
// the requirements. If the requirements were to change after selection a replica, then
// that could invalidate the selection.
func (s *BaseScheduler) FindReadyReplica(kernel scheduling.Kernel, executionId string) (scheduling.KernelReplica, error) {
	replica, err := s.schedulingPolicy.FindReadyReplica(kernel, executionId)

	if replica != nil && err == nil {
		host := replica.Host()

		if host != nil {
			updateIndexErr := s.UpdateIndex(host)

			if updateIndexErr != nil {
				s.log.Error("Error while attempting to update index of host %s of replica %d of kernel \"%s\": %v",
					host.GetNodeName(), replica.ReplicaID(), replica.ID(), err)
			}
		}
	}

	return replica, err
}

// addReplicaSetup performs any platform-specific setup required when adding a new replica to a kernel.
func (s *BaseScheduler) addReplicaSetup(kernelId string, addReplicaOp *scheduling.AddReplicaOperation) {
	s.instance.addReplicaSetup(kernelId, addReplicaOp)
}

func (s *BaseScheduler) getHost(hostId string) (scheduling.Host, bool) {
	return s.cluster.GetHost(hostId)
}

// postScheduleKernelReplica is called immediately after ScheduleKernelReplica is called.
func (s *BaseScheduler) postScheduleKernelReplica(kernelId string, addReplicaOp *scheduling.AddReplicaOperation) {
	s.instance.postScheduleKernelReplica(kernelId, addReplicaOp)
}

// selectViableHostForReplica identifies a viable scheduling.Host to serve the given scheduling.KernelContainer.
//
// selectViableHostForReplica is most often called for kernels that need to begin training immediately.
//
// Important: selectViableHostForReplica will reserve resources on the Host.
func (s *BaseScheduler) selectViableHostForReplica(replicaSpec *proto.KernelReplicaSpec, blacklistedHosts []scheduling.Host,
	forTraining bool, ignoreOversubscriptionRisk bool) (scheduling.Host, error) {

	host, err := s.instance.selectViableHostForReplica(replicaSpec, blacklistedHosts, forTraining, ignoreOversubscriptionRisk)

	if host != nil && err == nil {
		updateIndexErr := s.UpdateIndex(host)

		if updateIndexErr != nil {
			s.log.Error("Error while attempting to update index of host %s: %v",
				host.GetNodeName(), err)
		}
	}

	return host, err
}

func (s *BaseScheduler) ScheduleKernelReplica(ctx context.Context, args *scheduling.ScheduleReplicaArgs) error {
	err := s.instance.ScheduleKernelReplica(ctx, args)

	if err == nil && args.TargetHost != nil {
		updateIndexErr := s.UpdateIndex(args.TargetHost)

		if updateIndexErr != nil {
			s.log.Error("Error while attempting to update index of host %s: %v",
				args.TargetHost.GetNodeName(), err)
		}
	}

	return err
}
