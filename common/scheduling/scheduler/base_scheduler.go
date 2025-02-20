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
	"github.com/scusemua/distributed-notebook/common/utils"
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

func (b *baseSchedulerBuilder) WithInitialNumContainersPerHost(initialNumContainersPerHost int) *baseSchedulerBuilder {
	b.initialNumContainersPerHost = initialNumContainersPerHost
	return b
}

// Build method
func (b *baseSchedulerBuilder) Build() *BaseScheduler {
	if b.options == nil {
		panic("Cannot construct BaseScheduler using baseSchedulerBuilder with nil options.")
	}

	if b.schedulingPolicy == nil {
		schedulingPolicy, err := GetSchedulingPolicy(b.options)
		if err != nil {
			panic(err)
		}

		b.schedulingPolicy = schedulingPolicy.(SchedulingPolicy)
	}

	clusterScheduler := &BaseScheduler{
		cluster:                                  b.cluster,
		hostMapper:                               b.hostMapper,
		stRatio:                                  types.NewMovingStatFromWindow(5),
		opts:                                     b.options,
		remoteSynchronizationInterval:            time.Second * time.Duration(b.options.GpuPollIntervalSeconds),
		placer:                                   b.placer,
		hostSpec:                                 b.hostSpec,
		oversubscribed:                           types.NewHeap(OversubscribedIndexKey),
		undersubscribed:                          types.NewHeap(UndersubscribedIndexKey),
		idleHosts:                                types.NewHeap(IdleIndexKey),
		maxSubscribedRatio:                       decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		subscriptionRatio:                        decimal.NewFromFloat(b.options.MaxSubscribedRatio),
		activeAddReplicaOpsPerKernel:             hashmap.NewCornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]](64),
		addReplicaOperationsByKernelReplicaId:    hashmap.NewCornelkMap[string, *scheduling.AddReplicaOperation](64),
		addReplicaNewPodOrContainerNotifications: hashmap.NewCornelkMap[string, chan *scheduling.AddReplicaOperation](64),
		kernelProvider:                           b.kernelProvider,
		notificationBroker:                       b.notificationBroker,
		schedulingPolicy:                         b.schedulingPolicy,
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

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
				}

				prewarmer := prewarm.NewMinCapacityPrewarmer(b.cluster, minCapacityPrewarmerConfig, b.metricsProvider)
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

	// schedulingPolicy specifies the scheduling behavior for the scheduling.Cluster and scheduling.Scheduler.
	schedulingPolicy SchedulingPolicy

	hostSpec types.Spec // The types.Spec used when creating new Host instances.

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerEventHandler scheduling.ContainerWatcher

	log logger.Logger

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from TransactionOperation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]]

	// Mapping from new kernel-replica key (i.e., <kernel-id>-<replica-id>) to AddReplicaOperation.
	addReplicaOperationsByKernelReplicaId *hashmap.CornelkMap[string, *scheduling.AddReplicaOperation]

	// Mapping from NewPodName to chan string.
	// In theory, it's possible to receive a PodCreated notification from Kubernetes AFTER the replica within the new Pod
	// has started running and has registered with the Gateway. In this case, we won't be able to retrieve the AddReplicaOperation
	// associated with that replica via the new Pod's name, as that mapping is created when the PodCreated notification is received.
	// In this case, the goroutine handling the replica registration waits on a channel for the associated AddReplicaOperation.
	addReplicaNewPodOrContainerNotifications *hashmap.CornelkMap[string, chan *scheduling.AddReplicaOperation]

	opts *scheduling.SchedulerOptions // Configuration options.

	oversubscribed  *types.Heap // The host index for oversubscribed hosts. Ordering is implemented by schedulerHost.
	undersubscribed *types.Heap // The host index for under-subscribed hosts. Ordering is implemented by schedulerHost.
	idleHosts       *types.Heap

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

// FindCandidateHosts performs a single attempt/pass of searching for candidate Host instances.
//
// FindCandidateHosts is exported so that it can be unit tested.
//
// If FindCandidateHosts returns nil, rather than an empty slice, then that indicates that an error occurred.
func (s *BaseScheduler) FindCandidateHosts(numHosts int, kernelSpec *proto.KernelSpec) ([]scheduling.Host, error) {
	return s.instance.findCandidateHosts(numHosts, kernelSpec)
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

// GetCandidateHost identifies a single candidate host for a particular kernel replica, reserving resources on hosts
// before returning them.
//
// If the specified replica's current scheduling.Host isn't already blacklisted, then GetCandidateHost will add it to
// the blacklist.
func (s *BaseScheduler) GetCandidateHost(replica scheduling.KernelReplica, blacklistedHosts []scheduling.Host, forTraining bool) (scheduling.Host, error) {
	if blacklistedHosts == nil {
		blacklistedHosts = make([]scheduling.Host, 0)
	}

	if replica.Host() != nil {
		currentHost := replica.Host()
		found := false

		// If the replica's current host isn't already blacklisted, then add it to the blacklist.
		for _, blacklistedHost := range blacklistedHosts {
			if blacklistedHost.GetID() == currentHost.GetID() {
				found = true
				break
			}
		}

		if !found {
			blacklistedHosts = append(blacklistedHosts, currentHost)
		}
	}

	candidate, err := s.findViableHostForReplica(replica, blacklistedHosts, forTraining)
	if candidate != nil {
		s.log.Debug("Found viable candidate host for replica %d of kernel %s: host %s",
			replica.ReplicaID(), replica.ID(), candidate.GetNodeName())
	}

	return candidate, err
}

// FindReadyContainer selects one of the scheduling.KernelContainer instances of the specified scheduling.UserSession
// to handle a training event.
func (s *BaseScheduler) FindReadyContainer(session scheduling.UserSession) scheduling.KernelContainer {
	panic("Not implemented.")
}

// UpdateIndex is used to update a Host's position in its index.
// It also updates the "idle hosts" heap.
func (s *BaseScheduler) UpdateIndex(host scheduling.Host) error {
	heap.Fix(s.idleHosts, host.GetIdx(IdleHostMetadataKey))

	s.placer.UpdateIndex(host)

	return nil
}

// GetCandidateHosts returns a slice of scheduling.Host containing Host instances that could serve
// a Container (i.e., a kernel replica) with the given resource requirements (encoded as a types.Spec).
//
// GetCandidateHosts will automatically request that new Host instances be provisioned and added to the Cluster) S
// if it fails to find sufficiently many viable Host instances. This process will be attempted three times.
// If GetCandidateHosts is unsuccessful (at finding sufficiently many viable hosts) after those three attempts,
// then GetCandidateHosts will give up and return an error.
//
// The size of the returned slice will be equal to the configured number of replicas for each kernel (usually 3).
//
// This function is NOT idempotent. This locks the Hosts that are returned.
func (s *BaseScheduler) GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec) ([]scheduling.Host, error) {
	var (
		bestAttempt = 0
		hosts       []scheduling.Host
	)

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

	for retryParameters.Steps > 0 && len(hosts) < s.schedulingPolicy.NumReplicas() {
		numHostsRequired := s.schedulingPolicy.NumReplicas() - len(hosts)
		s.log.Debug("Searching for %d candidate host(s) for kernel %s. Have identified %d candidate host(s) so far.",
			numHostsRequired, kernelSpec.Id, len(hosts))
		hostBatch, err := s.FindCandidateHosts(numHostsRequired, kernelSpec) // note: this function executes atomically.
		if err != nil {
			s.log.Error("Error while searching for %d candidate host(s) for kernel %s: %v",
				numHostsRequired, kernelSpec.Id, err)
			return nil, err
		}

		hosts = append(hosts, hostBatch...)

		if len(hosts) < s.schedulingPolicy.NumReplicas() {
			s.log.Warn("Found only %d/%d hosts to serve replicas of kernel %s so far (attempt=%d).",
				len(hosts), s.schedulingPolicy.NumReplicas(), kernelSpec.Id, maxAttempts-retryParameters.Steps+1)

			if !s.isScalingOutEnabled() {
				s.log.Warn("Scaling-out is disabled. Giving up on finding hosts for kernel %s.", kernelSpec.Id)
				break // Give up.
			}

			numHostsRequired = s.schedulingPolicy.NumReplicas() - len(hosts)
			s.log.Debug("Will attempt to provision %d new host(s) so that we can serve kernel %s.",
				numHostsRequired, kernelSpec.Id)

			p := s.cluster.RequestHosts(ctx, int32(numHostsRequired))
			if err = p.Error(); err != nil {
				if errors.Is(err, scheduling.ErrScalingActive) {
					s.log.Debug("Cannot register scale-out operation for kernel %s: there is already an active scale-out operation.",
						kernelSpec.Id)
				} else if errors.Is(err, scheduling.ErrUnsupportedOperation) {
					s.log.Warn("Cluster failed to provision %d additional host(s) for us (for kernel %s) because: %v",
						numHostsRequired, kernelSpec.Id, err)
					break // We're out of hosts. Give up. It can be resubmitted by the client later.
				} else {
					s.log.Warn("Cluster failed to provision %d additional host(s) for us (for kernel %s) because: %v",
						numHostsRequired, kernelSpec.Id, err)
				}
			}

			if len(hosts) > bestAttempt {
				bestAttempt = len(hosts)
			}

			sleepInterval := retryParameters.Step()
			s.log.Debug("Sleeping for %v before retrying to find %d candidate host(s) for replica(s) of kernel %s.",
				sleepInterval, numHostsRequired, kernelSpec.Id)
			time.Sleep(sleepInterval)

			continue
		}

		s.log.Debug("Found %d hosts to serve replicas of kernel %s: %v",
			s.schedulingPolicy.NumReplicas(), kernelSpec.Id, hosts)
	}

	// Check if we were able to reserve the requested number of hosts.
	// If not, then we need to release any hosts we did reserve.
	if len(hosts) < s.schedulingPolicy.NumReplicas() {
		s.log.Warn("Failed to find %d hosts to serve replicas of kernel %s after %d tries...",
			s.schedulingPolicy.NumReplicas(), kernelSpec.Id, maxAttempts-retryParameters.Steps+1)

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
func (s *BaseScheduler) ReserveResourcesForReplica(kernel scheduling.Kernel, replica scheduling.KernelReplica, commitResources bool) error {
	return s.placer.ReserveResourcesForReplica(kernel, replica, commitResources)
}

// DeployKernelReplicas is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *BaseScheduler) DeployKernelReplicas(ctx context.Context, kernel scheduling.Kernel, blacklistedHosts []scheduling.Host) error {
	return s.instance.DeployKernelReplicas(ctx, kernel, blacklistedHosts)
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

// AddReplica adds a new replica to a particular distributed kernel.
// This is only used for adding new replicas beyond the base set of replicas created
// when the CloneSet is first created. The first 3 (or however many there are configured
// to be) replicas are created automatically by the CloneSet.
//
// Parameters:
// - kernelId (string): The ID of the kernel to which we're adding a new replica.
// - opts (AddReplicaWaitOptions): Specifies whether we'll wait for registration and/or SMR-joining.
// - dataDirectory (string): Path to etcd-raft data directory in RemoteStorage.
func (s *BaseScheduler) addReplica(ctx context.Context, in *proto.ReplicaInfo, targetHost scheduling.Host,
	opts scheduling.AddReplicaWaitOptions, dataDirectory string, blacklistedHosts []scheduling.Host,
	forTraining bool) (*scheduling.AddReplicaOperation, error) {

	var kernelId = in.KernelId
	var persistentId = in.PersistentId

	kernel, ok := s.kernelProvider.GetKernel(kernelId)
	if !ok {
		s.log.Error("Cannot add replica %d to kernel %s: cannot find kernel %s", in.ReplicaId, kernelId, kernelId)
		return nil, types.ErrKernelNotFound
	}

	kernel.AddOperationStarted()

	var smrNodeId int32 = -1

	// Reuse the same SMR node ID if we've been told to do so.
	if opts.ReuseSameNodeId() {
		smrNodeId = in.ReplicaId
	}

	// The spec to be used for the new replica that is created during the migration.
	var newReplicaSpec = kernel.PrepareNewReplica(persistentId, smrNodeId)

	addReplicaOp := scheduling.NewAddReplicaOperation(kernel, newReplicaSpec, dataDirectory)
	key := fmt.Sprintf("%s-%d", addReplicaOp.KernelId(), addReplicaOp.ReplicaId())
	s.addReplicaOperationsByKernelReplicaId.Store(key, addReplicaOp)

	s.log.Debug("Created new AddReplicaOperation \"%s\": %s", addReplicaOp.OperationID(), addReplicaOp.String())
	s.log.Debug("Adding replica %d to kernel \"%s\" as part of AddReplicaOperation \"%s\" now.",
		newReplicaSpec.ReplicaId, kernelId, addReplicaOp.OperationID())

	// AddHost the AddReplicaOperation to the associated maps belonging to the Gateway Daemon.
	s.addReplicaMutex.Lock()
	ops, ok := s.activeAddReplicaOpsPerKernel.Load(kernelId)
	if !ok {
		ops = orderedmap.NewOrderedMap[string, *scheduling.AddReplicaOperation]()
	}
	ops.Set(addReplicaOp.OperationID(), addReplicaOp)
	s.activeAddReplicaOpsPerKernel.Store(kernelId, ops)
	s.addReplicaMutex.Unlock()

	s.instance.addReplicaSetup(kernelId, addReplicaOp)

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Anything that needs to happen before it's possible for the kernel to have registered already must
	// occur before this line (i.e., before we call ScheduleKernelReplica). Once we call ScheduleKernelReplica,
	// we cannot assume that we've not yet received the registration notification from the kernel, so all of
	// our state needs to be set up BEFORE that call occurs.
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	sem := semaphore.NewWeighted(1)
	if !sem.TryAcquire(1) {
		panic("Failed to acquire on Semaphore")
	}

	notifyChan := make(chan interface{}, 1)
	go func() {
		defer sem.Release(1)

		err := s.cluster.Scheduler().ScheduleKernelReplica(ctx, newReplicaSpec, targetHost,
			blacklistedHosts, forTraining)

		if err != nil {
			notifyChan <- err
		} else {
			notifyChan <- struct{}{}
		}
	}()

	// In Kubernetes deployments, the key is the Pod name, which is also the kernel ID + replica suffix.
	// In Docker deployments, the container name isn't really the container's name, but its ID, which is a hash
	// or something like that.
	s.instance.postScheduleKernelReplica(kernelId, addReplicaOp)

	if opts.WaitRegistered() {
		s.log.Debug("Waiting for new replica %d of kernel \"%s\" to register during AddReplicaOperation \"%s\"",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		replicaRegisteredChannel := addReplicaOp.ReplicaRegisteredChannel()

		// We'll keep looping until the call to ScheduleKernelReplica either succeeds, explicitly fails, or times out.
		// We'll also keep looping until the replica has registered.
		var sentBeforeClosed, replicaScheduled, replicaRegistered bool
		for !replicaScheduled || !replicaRegistered {
			select {
			case _, sentBeforeClosed = <-replicaRegisteredChannel:
				{
					if !sentBeforeClosed {
						errorMessage := fmt.Sprintf("Received default value from \"Replica Registered\" channel for AddReplicaOperation \"%s\": %v",
							addReplicaOp.OperationID(), addReplicaOp.String())
						s.log.Error(errorMessage)
						go s.sendErrorNotification("Channel Receive on Closed \"ReplicaRegisteredChannel\" Channel", errorMessage)
					} else {
						addReplicaOp.CloseReplicaRegisteredChannel()
						replicaRegisteredChannel = nil // Prevent infinite loop
					}

					replicaRegistered = true
				}
			case v := <-notifyChan:
				{
					if err, ok := v.(error); ok {
						return addReplicaOp, err
					}

					replicaScheduled = true
				}
			}
		}

		s.log.Debug("New replica %d of kernel \"%s\" has registered with the Gateway during AddReplicaOperation \"%s\".",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
	}

	// If we waited for the replica to register, then this will return immediately.
	// Otherwise, we'll be blocked until the call to ScheduleKernelReplica returns (or fails/times out).
	err := sem.Acquire(ctx, int64(1))
	if err != nil {
		return addReplicaOp, err
	}

	var smrWg sync.WaitGroup
	smrWg.Add(1)

	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted
	// when the new replica joins its SMR cluster.
	go func() {
		s.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster during AddReplicaOperation \"%s\" now...",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())

		replicaJoinedSmrChannel := addReplicaOp.ReplicaJoinedSmrChannel()
		_, sentBeforeClosed := <-replicaJoinedSmrChannel

		if !sentBeforeClosed {
			errorMessage := fmt.Sprintf("Received default value from \"Replica Joined SMR\" channel for AddReplicaOperation \"%s\": %v",
				addReplicaOp.OperationID(), addReplicaOp.String())
			s.log.Error(errorMessage)

			go s.sendErrorNotification("Channel Receive on Closed \"ReplicaJoinedSmrChannel\" Channel",
				errorMessage)
		}

		close(replicaJoinedSmrChannel)
		s.log.Debug("New replica %d of kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
		kernel.AddOperationCompleted()
		smrWg.Done()

		if !addReplicaOp.Completed() {
			s.log.Error("AddReplicaOperation \"%s\" does not think it's done, even though it should...",
				addReplicaOp.OperationID())

			go s.sendErrorNotification(fmt.Sprintf("AddReplicaOperation \"%s\" is Confused",
				addReplicaOp.OperationID()),
				fmt.Sprintf("AddReplicaOperation \"%s\" does not think it's done, even though it should: %s",
					addReplicaOp.OperationID(), addReplicaOp.String()))
		}
	}()

	if opts.WaitSmrJoined() {
		s.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster...",
			addReplicaOp.ReplicaId(), kernelId)
		smrWg.Wait()
	}

	// Return nil on success.
	return addReplicaOp, nil
}

func (s *BaseScheduler) GetActiveAddReplicaOperationsForKernel(kernelId string) (*orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation], bool) {
	return s.activeAddReplicaOpsPerKernel.Load(kernelId)
}

func (s *BaseScheduler) GetAddReplicaOperation(id string) (*scheduling.AddReplicaOperation, bool) {
	return s.addReplicaOperationsByKernelReplicaId.Load(id)
}

func (s *BaseScheduler) GetAddReplicaOperationManager() hashmap.HashMap[string, *scheduling.AddReplicaOperation] {
	return s.addReplicaOperationsByKernelReplicaId
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
		if !s.canScaleIn {
			s.log.Debug("We can now scale-in.")
			s.canScaleIn = true
		}
		s.subscriptionRatio = decimal.NewFromFloat(avg)
		s.log.Debug("Recomputed subscription ratio as %s.", s.subscriptionRatio.StringFixed(4))
		s.rebalance(avg)

		scalingInterval := s.schedulingPolicy.ScalingConfiguration().ScalingInterval
		scalingIntervalSec := s.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec
		itHasBeenAWhileSinceLastCapacityValidation := time.Since(s.lastCapacityValidation) >= scalingInterval
		if !skipValidateCapacity && scalingIntervalSec > 0 && itHasBeenAWhileSinceLastCapacityValidation {
			s.log.Debug("Validating capacity.")

			s.schedulingPolicy.ValidateCapacity(s.cluster)

			s.numCapacityValidations.Add(1)
		}

		s.log.Debug("Time until next capacity validation: %v (interval=%v).",
			scalingInterval-time.Since(s.lastCapacityValidation), scalingInterval)

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
// findViableHostForReplica searches for a viable training host and, if one is found, then that host is returned.
// Otherwise, an error is returned.
//
// If we fail to find a host, then we'll try to scale-out (if we're allowed).
func (s *BaseScheduler) findViableHostForReplica(replicaSpec scheduling.KernelReplica, blacklistedHosts []scheduling.Host, forTraining bool) (host scheduling.Host, failureReason error) {
	numTries := 0

	// We'll try a few times if we keep scaling-out successfully but somehow manage to fail again and again.
	for numTries < 5 {
		host, failureReason = s.instance.selectViableHostForReplica(replicaSpec.KernelReplicaSpec(), blacklistedHosts, forTraining)
		if host != nil {
			s.log.Debug("Found viable host for replica %d of kernel %s: host %s",
				replicaSpec.ReplicaID(), replicaSpec.ID(), host.GetNodeName())
			return host, nil
		}

		s.log.Warn("Failed to find viable host for replica %d of kernel %s (forTraining=%v): %v",
			replicaSpec.ReplicaID(), replicaSpec.ID(), forTraining, failureReason)

		if !s.isScalingOutEnabled() || !errors.Is(failureReason, scheduling.ErrInsufficientHostsAvailable) {
			return nil, failureReason
		}

		s.log.Debug("Attempting to scale-out to provide host for replica %d of kernel %s (forTraining=%v).",
			replicaSpec.ReplicaID(), replicaSpec.ID(), forTraining)
		p := s.cluster.RequestHosts(context.Background(), 1)
		if err := p.Error(); err != nil {
			if errors.Is(err, scheduling.ErrScalingActive) || errors.Is(err, scheduling.ErrUnsupportedOperation) {
				s.log.Warn("Cluster failed to provision 1 additional host (for replica %d of kernel %s) because: %v",
					replicaSpec.ReplicaID(), replicaSpec.ID(), err)
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
	targetHostId string, forTraining bool) (resp *proto.MigrateKernelResponse, reason error, err error) {

	if kernelReplica == nil {
		s.log.Error("MigrateContainer received nil KernelReplica")
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilKernelReplica
	}

	s.log.Debug("Migrating replica %d of kernel %s. Target host ID: %s.",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHostId)

	kernelContainer := kernelReplica.Container()
	if kernelContainer == nil {
		s.log.Error("Cannot migrate replica %d of kernel %s; kernel's kernelContainer is nil",
			kernelReplica.ReplicaID(), kernelReplica.ID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilContainer
	}

	originalHost := kernelContainer.Host()
	if originalHost == nil {
		s.log.Error("Cannot migrate kernelContainer %s. Container's host is nil.", kernelContainer.ContainerID())
		return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId}, nil, ErrNilOriginalHost
	}

	var (
		targetHost scheduling.Host
		loaded     bool
	)

	// If the caller specified a particular host, then we'll verify that the specified host exists.
	// If it doesn't, then we'll return an error.
	if targetHostId != "" {
		targetHost, loaded = s.cluster.GetHost(targetHostId)

		if !loaded {
			s.log.Error("Host %s specified as migration target for replica %d of kernel %s; however, host %s does not exist.",
				targetHostId, kernelReplica.ReplicaID(), kernelReplica.ID(), targetHostId)
			return &proto.MigrateKernelResponse{Id: -1, Hostname: ErrorHostname, NewNodeId: targetHostId},
				nil, fmt.Errorf("%w: cannot find specified target host %s for migration of replica %d of kernel %s",
					scheduling.ErrHostNotFound, targetHostId, kernelReplica.ReplicaID(), kernelReplica.ID())
		}

		// Make sure that the Host doesn't already have the kernel replica to be migrated or another
		// replica of the same kernel running on it. If so, then the target host is not viable.
		//
		// Likewise, if the host does not have enough resources to serve the kernel replica,
		// then it is not viable.
		if reason = s.isHostViableForMigration(targetHost, kernelReplica, forTraining); reason != nil {
			return &proto.MigrateKernelResponse{
				Id:          -1,
				Hostname:    ErrorHostname,
				NewNodeId:   targetHostId,
				NewNodeName: targetHost.GetNodeName(),
			}, reason, nil
		}
	}

	// If we weren't already given a target host to migrate the kernel replica to, then let's try to find one now.
	if targetHost == nil {
		targetHost, reason = s.findViableHostForReplica(kernelReplica, []scheduling.Host{originalHost}, forTraining)
		if reason != nil || targetHost == nil {
			s.log.Warn("Failed to find a viable host for replica %d of kernel %s: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), reason)
			return &proto.MigrateKernelResponse{
				Id:       -1,
				Hostname: ErrorHostname,
			}, reason, nil
		}
	}

	s.log.Debug("Found viable migration target for replica %d of kernel %s: host %s",
		kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetNodeName())

	var dataDirectory string
	dataDirectory, err = s.issuePrepareToMigrateRequest(kernelReplica, originalHost)
	if err != nil {
		s.log.Error("Failed to issue 'prepare-to-migrate' request to replica %d of kernel %s: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			s.log.Error("Failed to release reservation for replica %d of kernel %s after failing to issue 'prepare-to-migrate' request during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := s.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			s.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	err = s.RemoveReplicaFromHost(kernelReplica)
	if err != nil {
		s.log.Error("Failed to remove replica %d of kernel %s from its current host: %v",
			kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			s.log.Error("Failed to release reservation for replica %d of kernel %s after failing to remove replica from its current host during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := s.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			s.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	replicaSpec := &proto.ReplicaInfo{
		KernelId:     kernelReplica.ID(),
		ReplicaId:    kernelReplica.ReplicaID(),
		PersistentId: kernelReplica.PersistentID(),
	}

	// AddHost a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
	opts := scheduling.NewAddReplicaWaitOptions(true, true, true)

	var addReplicaOp *scheduling.AddReplicaOperation
	addReplicaOp, err = s.addReplica(ctx, replicaSpec, targetHost, opts, dataDirectory, []scheduling.Host{originalHost}, forTraining)

	// If there's an error here, it's presumably a "real" error, as we already picked out a viable host up above.
	if err != nil {
		s.log.Error("Failed to add new replica %d to kernel %s: %v", kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			s.log.Error("Failed to release reservation for replica %d of kernel %s after failing to recreate replica during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := s.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			s.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	}

	s.log.Debug("Successfully added new replica %d of kernel \"%s\" during migration (not quite done yet)",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	var newlyAddedReplica scheduling.KernelReplica
	newlyAddedReplica, err = addReplicaOp.Kernel().GetReplicaByID(addReplicaOp.ReplicaId())
	if err != nil {
		s.log.Error("Could not find replica %d for kernel %s after migration is supposed to have completed: %v", addReplicaOp.ReplicaId(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			s.log.Error("Failed to release reservation for replica %d of kernel %s after not being able to find the replica after supposedly successful migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		updateIndexErr := s.UpdateIndex(targetHost)
		if updateIndexErr != nil {
			s.log.Error("Failed to update index containing host %s: %v", targetHost.GetNodeName(), updateIndexErr)
			err = errors.Join(err, updateIndexErr)
		}

		return &proto.MigrateKernelResponse{
			Id:          -1,
			Hostname:    ErrorHostname,
			NewNodeId:   targetHost.GetID(),
			NewNodeName: targetHost.GetNodeName(),
			Success:     false,
		}, nil, err
	} else {
		s.log.Debug("Successfully added new replica %d to kernel %s during migration operation.",
			addReplicaOp.ReplicaId(), kernelReplica.ID())
	}

	s.log.Debug("Designating new replica %d of kernel \"%s\" as \"ready\"",
		addReplicaOp.ReplicaId(), kernelReplica.ID())

	// The replica is fully operational at this point, so record that it is ready.
	newlyAddedReplica.SetReady()

	resp = &proto.MigrateKernelResponse{
		Id:          addReplicaOp.ReplicaId(),
		Hostname:    addReplicaOp.ReplicaPodHostname(),
		NewNodeId:   targetHost.GetID(),
		NewNodeName: targetHost.GetNodeName(),
		Success:     true,
	}
	return resp, nil, err
}

// isHostViableForMigration returns nil if the specified Host is a viable migration target for the specified
// KernelReplica -- that is, if the specified Host does not already serve another replica of the same kernel, or
// the replica being migrated itself.
//
// Likewise, this also checks that the specified Host has enough resources to serve the specified KernelReplica.
//
// If the Host is not viable, then an ErrHostNotViable error is returned.
func (s *BaseScheduler) isHostViableForMigration(targetHost scheduling.Host, kernelReplica scheduling.KernelReplica, forTraining bool) error {
	if targetHost == nil {
		return scheduling.ErrNilHost
	}

	// If we were able to resolve the host, then let's also verify that the host doesn't already contain
	// another replica of the same kernel (or the replica we're migrating). If so, then the host is not viable.
	existingReplica := targetHost.GetAnyReplicaOfKernel(kernelReplica.ID())
	if existingReplica != nil && existingReplica.ReplicaId() == kernelReplica.ReplicaID() {
		s.log.Warn("Cannot migrate replica %d of kernel %s to host %s. "+
			"Host %s is already hosting replica %d of kernel %s.", kernelReplica.ReplicaID(), kernelReplica.ID(),
			targetHost.GetID(), targetHost.GetID(), existingReplica.ReplicaId(), kernelReplica.ID())

		return fmt.Errorf("%w: replica %d of kernel %s is already running on host %s",
			scheduling.ErrHostNotViable, existingReplica.ReplicaId(), kernelReplica.ID(), targetHost.GetID())
	}

	// Check that there are enough resources available.
	kernelResourceSpec := kernelReplica.ResourceSpec()
	if !targetHost.ResourceSpec().Validate(kernelResourceSpec) {
		s.log.Warn("Cannot migrate replica %d of kernel %s to host %s, as host does not have sufficiently-many allocatable resources to accommodate the replica.",
			kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetID())
		return fmt.Errorf("%w: host lacks sufficiently-many allocatable resourecs", scheduling.ErrHostNotViable)
	}

	// If we're migrating a kernel explicitly to begin training, then we need to see if the target host has sufficient
	// idle resources available.
	if forTraining && !targetHost.CanCommitResources(kernelResourceSpec) {
		s.log.Warn("Cannot migrate replica %d of kernel %s to host %s, as kernel needs to start training, and host lacks sufficient idle resources for this (current idle resources: %v).",
			kernelReplica.ReplicaID(), kernelReplica.ID(), targetHost.GetID(), targetHost.IdleResources().String())
		return fmt.Errorf("%w: insufficient idle resources available for training", scheduling.ErrHostNotViable)
	}

	return nil
}

// issuePrepareMigrateRequest issues a 'prepare-to-migrate' request to a specific replica of a specific kernel.
// This will prompt the kernel to shut down its etcd process (but not remove itself from the cluster)
// before writing the contents of its data directory to intermediate storage.
//
// Returns the path to the data directory in intermediate storage.
func (s *BaseScheduler) issuePrepareToMigrateRequest(kernelReplica scheduling.KernelReplica, originalHost scheduling.Host) (string, error) {
	// If the host is nil, then we'll attempt to retrieve it from the kernel itself.
	if originalHost == nil {
		kernelContainer := kernelReplica.Container()
		if kernelContainer == nil {
			return "", scheduling.ErrNilHost // It's ultimately the host that we need.
		}

		originalHost = kernelContainer.Host()
		if originalHost == nil {
			return "", scheduling.ErrNilHost
		}
	}

	s.log.Debug("Calling PrepareToMigrate RPC targeting host %s (ID=%s) of replica %d of kernel %s now.",
		originalHost.GetNodeName(), originalHost.GetID(), kernelReplica.ReplicaID(), kernelReplica.ID())

	replicaInfo := &proto.ReplicaInfo{
		ReplicaId: kernelReplica.ReplicaID(),
		KernelId:  kernelReplica.ID(),
	}

	resultChan := make(chan interface{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	go func() {
		gRpcClientConnection := originalHost.GetGrpcConnection()

		if gRpcClientConnection == nil {
			err := fmt.Errorf("gRPC Client Connection with host %s (ID=%s) is nil; I hope we're unit-testing",
				originalHost.GetNodeName(), originalHost.GetID())
			s.log.Warn(utils.OrangeStyle.Render(err.Error()))
			// resultChan <- err
		} else {
			s.log.Debug("TransactionState of gRPC ClientConn with host %s (ID=%s): %s (%v)", originalHost.GetNodeName(),
				originalHost.GetID(), gRpcClientConnection.GetState().String(), gRpcClientConnection.GetState())
		}

		// Issue the 'prepare-to-migrate' request. We panic if there was an error.
		resp, err := originalHost.PrepareToMigrate(ctx, replicaInfo)
		if err != nil {
			s.log.Error("Failed to add replica %d of kernel %s to SMR cluster because: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			resultChan <- err
		} else {
			resultChan <- resp
		}
	}()

	var resp *proto.PrepareToMigrateResponse
	select {
	case <-ctx.Done():
		{
			s.log.Error("Timed out waiting for response from host %s (ID=%s) for 'prepare-to-migrate' request for replica %d of kernel %s...",
				originalHost.GetNodeName(), originalHost.GetID(), kernelReplica.ReplicaID(), kernelReplica.ID())
			return "", fmt.Errorf("timed out")
		}
	case res := <-resultChan:
		{
			switch res.(type) {
			case *proto.PrepareToMigrateResponse:
				{
					resp = res.(*proto.PrepareToMigrateResponse)
				}
			case error:
				{
					return "", res.(error)
				}
			}
		}
	}

	dataDirectory := resp.DataDir
	s.log.Debug("Successfully issued 'prepare-to-migrate' request to replica %d of kernel %s on host %s. Data directory: \"%s\"",
		kernelReplica.ReplicaID(), kernelReplica.ID(), originalHost.GetID(), dataDirectory)

	return dataDirectory, nil
}

// HostRemoved is called by the Cluster when a Host is removed from the Cluster.
func (s *BaseScheduler) HostRemoved(host scheduling.Host) {
	s.instance.HostRemoved(host)
}

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *BaseScheduler) HostAdded(host scheduling.Host) {
	s.instance.HostAdded(host)
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
	heap.Push(pool, host)
}

func (s *BaseScheduler) validate() {
	if s.invalidated > InvalidationThreshold {
		// StaticPlacerMaxSubscribedRatio increase, release oversubscribed hosts to under-subscribed hosts.
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Apply subscription ratio change %.4f -> %.4f, add under-subscription hosts to candidate pool",
				s.lastSubscribedRatio, s.pendingSubscribedRatio)
		}

		for s.oversubscribed.Len() > 0 && s.oversubscribed.Peek().(scheduling.Host).OversubscriptionFactor().LessThan(decimal.Zero) {
			host := s.oversubscribed.Peek()
			heap.Pop(s.oversubscribed)
			s.designateSubscriptionPoolType(host.(scheduling.Host), s.undersubscribed, scheduling.SchedulerPoolTypeUndersubscribed)
		}

		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
	} else if s.invalidated < (-1 * InvalidationThreshold) {
		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
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

// migrateContainersFromHost attempts to migrate all the kernels scheduled on the specified Host to other Hosts.
func (s *BaseScheduler) migrateContainersFromHost(host scheduling.Host, forTraining bool) error {
	numContainersToMigrate := host.Containers().Len()

	// If there are no containers to migrate, then we're done.
	if numContainersToMigrate == 0 {
		s.log.Debug("There are no containers on host %s to migrate.", host.GetNodeName())
		return nil
	}

	getMigrationTarget := func(containerId string, container scheduling.KernelContainer) (scheduling.Host, error) {
		return s.GetCandidateHost(container.GetClient(), nil, false)
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
		_, failedMigrationReason, err := s.MigrateKernelReplica(context.Background(), container.GetClient(),
			migrationTarget.TargetHost.GetID(), forTraining)

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

	// If there's just one, then just migrate the one container.
	if numContainersToMigrate == 1 {
		s.log.Debug("There's just one container on host %s to migrate.", host.GetNodeName())

		var err error
		host.Containers().Range(func(containerId string, container scheduling.KernelContainer) (contd bool) {
			err = migrateContainer(containerId, container)

			// There should just be one container to migrate, so we should ultimately stop looping immediately
			// either way.
			return err == nil
		})

		return err
	}

	aborted := false

	// Called if we have to abort the migration because we couldn't find a viable target for some replica.
	abortMigrationOperation := func() {
		s.log.Warn("Aborting migration of containers from host %s. Releasing %d reservation(s).",
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
		targetHost, err := getMigrationTarget(containerId, container)
		if err != nil {
			s.log.Warn("Could not find viable migration target for container %s; aborting migration of all containers from host %s: %v",
				containerId, targetHost.GetNodeName(), err)

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

	if aborted {
		s.log.Warn("Operation to migrate all %d container(s) from host %s has been aborted.",
			host.Containers().Len(), host.GetNodeName())

		return fmt.Errorf("%w: failed to find viable migration targets for one or more containers",
			scheduling.ErrMigrationFailed)
	}

	var nWorkers int
	if numContainersToMigrate <= 8 {
		nWorkers = numContainersToMigrate / 2
	} else {
		nWorkers = numContainersToMigrate / 4
	}

	timeoutInterval := time.Minute * time.Duration(float64(numContainersToMigrate)*1.5)
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

		select {
		case container := <-workQueue:
			{
				// If another worker failed to migrate a kernel replica, then we might as well give up.
				if errorOccurred.Load() {
					s.log.Warn("Migration Worker #%d of Host %s is exiting because another worker failed to migrate a kernel replica.",
						workerId, host.GetNodeName())

					// Increment the semaphore to signal to the main goroutine that we're done.
					workerDoneSemaphore.Release(1)
					return
				}

				startMigrateTime := time.Now()

				// Migrate the container.
				err := migrateContainer(container.ContainerID(), container)

				// Check if we failed to migrate.
				if err != nil {
					errorChan <- err
					errorOccurred.Store(true)

					s.log.Warn("Migration Worker #%d for Host %s failed to migrate replica %d of kernel %s after %v: %v.",
						workerId, host.GetNodeName(), container.ReplicaId(), container.KernelID(), time.Since(startMigrateTime), err)
					s.log.Warn("Migration Worker #%d for Host %s is aborting. Number of containers migrated (by worker #%d): %d. Total time elapsed: %v.",
						workerId, host.GetNodeName(), numContainersMigrated, workerId, time.Since(startTime))

					// Increment the semaphore to signal to the main goroutine that we're done.
					workerDoneSemaphore.Release(1)
					return
				}

				s.log.Debug("Migration Worker #%d for Host %s successfully migrated replica %d of kernel %s in %v. Total time elapsed: %v.",
					workerId, host.GetNodeName(), container.ReplicaId(), container.KernelID(), time.Since(startMigrateTime), time.Since(startTime))

				numContainersMigrated += 1
				numContainersMigratedSuccessfully.Add(1)
			}
		default:
			{
				s.log.Debug("Migration Worker #%d for Host %s is done. Number of containers migrated: %d. Time elapsed: %v.",
					workerId, host.GetNodeName(), numContainersMigrated, time.Since(startTime))

				// Increment the semaphore to signal to the main goroutine that we're done.
				workerDoneSemaphore.Release(1)
				return
			}
		}
	}

	s.log.Debug("Parallelizing the migration of %d containers from host %s using %d workers.",
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

	s.log.Debug("Started %d workers to migrate %d kernel replicas from host %s. Waiting for up to %v.",
		nWorkers, numContainersToMigrate, host.GetNodeName(), timeoutInterval)

	// Block until all workers are done, or until the operation times out.
	err := workerDoneSemaphore.Acquire(ctx, int64(nWorkers))

	// If there was an error (i.e., time-out) or we haven't migrated all the containers yet,
	// then we'll return a timed-out error.
	if err != nil || numContainersMigratedSuccessfully.Load() < int32(numContainersToMigrate) {
		s.log.Debug("Timed out waiting for %d worker(s) to migrate %d containers from host %s. Time elapsed: %v. Number of successful migrations: %d.",
			nWorkers, numContainersToMigrate, host.GetNodeName(), time.Since(startTime), numContainersMigratedSuccessfully.Load())

		return fmt.Errorf("%w: migration of %d containers from host %s timed out after %v",
			types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
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

						return fmt.Errorf("%w: migration of %d containers from host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					// Sanity check.
					//
					// This should NEVER happen.
					if numContainersMigratedSuccessfully.Load() > int32(numContainersToMigrate) {
						s.log.Error("Number of successful migrations (%d) of containers from host %s is somehow > than target (%d)",
							numContainersMigratedSuccessfully.Load(), host.GetNodeName(), numContainersToMigrate)

						return fmt.Errorf("%w: migration of %d containers from host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					// Sanity check.
					//
					// There should be no more containers on the host now.
					if host.NumContainers() > 0 {
						s.log.Error("Host %s still has %d container(s), but we should've migrated all of them...",
							host.GetNodeName(), host.NumContainers())

						return fmt.Errorf("%w: migration of %d containers from host %s timed out after %v",
							types.ErrRequestTimedOut, numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					}

					s.log.Debug("Successfully migrated all %d kernel replica(s) from host %s in %v.",
						numContainersToMigrate, host.GetNodeName(), time.Since(startTime))
					return nil
				}

				// The error variable was not nil, so 1+ migrations failed.
				s.log.Warn("At least %d error(s) occurred while trying to migrate all %d kernel replica(s) from host %s: %v",
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
func (s *BaseScheduler) includeHostsInScheduling(hosts []scheduling.Host) {
	for _, host := range hosts {
		err := host.IncludeForScheduling()
		if err != nil {
			s.log.Error("Host %s (ID=%s) is already allowed to be considered for scheduling (%v)",
				host.GetNodeName(), host.GetID(), err)
			continue
		}

		heap.Push(s.idleHosts, &idleSortedHost{
			Host: host,
		})

		s.log.Debug("Added host %s back to 'idle hosts' heap.", host.GetNodeName())
	}
}

// ContainerPrewarmer returns the ContainerPrewarmer used by the BaseScheduler.
func (s *BaseScheduler) ContainerPrewarmer() scheduling.ContainerPrewarmer {
	return s.prewarmer
}

func (s *BaseScheduler) releaseIdleHost(host scheduling.Host) error {
	// If the host has no containers running on it at all, then we can simply release the host.
	var err error
	if host.NumContainers() > 0 {
		err = s.migrateContainersFromHost(host, false) // Host is completely idle, so no training.
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

	// For now, just ensure the heap is in a valid order.
	heap.Init(s.idleHosts)

	s.log.Debug("Attempting to release %d idle host(s). Currently %d host(s) in the Cluster. Length of idle hosts: %d.",
		n, s.cluster.Len(), s.idleHosts.Len())

	var (
		err         error
		numReleased int32
	)

	for numReleased < n {
		idleHost := s.idleHosts.Peek().(*idleSortedHost)

		// If the host is not completely idle, then we'll break and stop looking.
		if idleHost.IdleGPUs() < idleHost.ResourceSpec().GPU() {
			break
		}

		excluded := idleHost.Host.ExcludeFromScheduling()
		if excluded {
			s.log.Debug("Selected host \"%s\" (ID=%s) as candidate for release.", idleHost.GetNodeName(), idleHost.GetID())

			// RemoveHost the host so that we can get to the next host.
			tmpHost := heap.Pop(s.idleHosts)

			// Sanity check.
			if tmpHost.(scheduling.Host).GetID() != idleHost.GetID() {
				panic("Host popped off of idleHosts heap does not equal host peeked from idleHosts.")
			}
		} else {
			s.log.Debug("Host \"%s\" (ID=%s) is ineligible for release: it's being considered in >= 1 scheduling operation(s).",
				idleHost.Host.GetNodeName(), idleHost.Host.GetID())
			break
		}

		s.log.Debug("Releasing idle host %d/%d: host %s. NumContainers: %d.",
			numReleased+1, n, idleHost.Host.GetNodeName(), idleHost.Host.NumContainers())

		err = s.releaseIdleHost(idleHost.Host)
		if err != nil {
			s.log.Warn("Could not release idle host \"%s\" because: %v", idleHost.Host.GetNodeName(), err)
			s.includeHostsInScheduling([]scheduling.Host{idleHost.Host})
			break
		}

		s.log.Debug("Successfully released idle host %d/%d: host %s.",
			numReleased+1, n, idleHost.Host.GetNodeName())
		numReleased += 1
	}

	if numReleased > 0 {
		s.log.Debug("Released %d/%d idle host(s).", numReleased, n)
	} else {
		s.log.Warn("Released %d/%d idle host(s).", numReleased, n)
	}

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
	return s.schedulingPolicy.FindReadyReplica(kernel, executionId)
}
