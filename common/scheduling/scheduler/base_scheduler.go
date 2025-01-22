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
	"github.com/scusemua/distributed-notebook/common/scheduling/policy"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/shopspring/decimal"
	"k8s.io/apimachinery/pkg/util/wait"
	"math"
	"sync"
	"time"
)

const (
	InvalidationThreshold = 0.1

	OversubscribedIndexKey  types.HeapElementMetadataKey = "oversubscribed_index"
	UndersubscribedIndexKey types.HeapElementMetadataKey = "undersubscribed_index"
	IdleIndexKey            types.HeapElementMetadataKey = "idle_index"
)

// schedulingNotification is a struct that is sent over a channel to notify the "main" goroutine handling the
// scheduling of a new kernel that the scheduling of one of that kernel's replicas has completed successfully.
type schedulingNotification struct {
	// SchedulingCompletedAt is the time at which the scheduling operation concluded
	// for the particular replica of the associated kernel.
	SchedulingCompletedAt time.Time

	// Host is the Host on which the kernel was scheduled (or attempted to be scheduled).
	Host scheduling.Host

	// KernelId is the ID of the kernel for which a replica was scheduled.
	KernelId string

	// ReplicaId is the SMR node ID of the replica that was scheduled (or whose scheduling was attempted but failed).
	ReplicaId int32

	// Successful indicates whether the operation succeeded or whether it failed.
	Successful bool

	// Error is the error that occurred that caused the scheduling operation to fail.
	// This field will be nil if the scheduling operation was successful.
	Error error
}

type KernelProvider interface {
	GetKernel(kernelId string) (scheduling.Kernel, bool)
}

type NotificationBroker interface {
	SendErrorNotification(errorName string, errorMessage string)
	SendInfoNotification(title string, message string)
}

type baseSchedulerBuilder struct {
	cluster            scheduling.Cluster
	placer             scheduling.Placer
	hostMapper         HostMapper
	hostSpec           types.Spec
	kernelProvider     KernelProvider
	notificationBroker NotificationBroker
	schedulingPolicy   scheduling.Policy // Optional, will be extracted from Options if not specified.
	options            *scheduling.SchedulerOptions
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

func (b *baseSchedulerBuilder) WithSchedulingPolicy(schedulingPolicy scheduling.Policy) *baseSchedulerBuilder {
	b.schedulingPolicy = schedulingPolicy
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

// Build method
func (b *baseSchedulerBuilder) Build() *BaseScheduler {
	if b.options == nil {
		panic("Cannot construct BaseScheduler using baseSchedulerBuilder with nil options.")
	}

	if b.schedulingPolicy == nil {
		schedulingPolicy, err := policy.GetSchedulingPolicy(b.options)
		if err != nil {
			panic(err)
		}

		b.schedulingPolicy = schedulingPolicy
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
		//gpusPerHost:                              float64(b.options.GpusPerHost),
		//virtualGpusPerHost:                       int32(b.options.VirtualGpusPerHost),
		//scalingFactor:                            b.options.ScalingFactor,
		//scalingLimit:                             b.options.ScalingLimit,
		//scalingInterval:                          time.Second * time.Duration(b.options.ScalingInterval),
		//maximumHostsToReleaseAtOnce:              int32(b.options.MaximumHostsToReleaseAtOnce),
		//scalingIntervalSec:                       int32(b.options.ScalingInterval),
		//predictiveAutoscalingEnabled:             b.options.PredictiveAutoscalingEnabled,
		//scalingBufferSize:                        int32(b.options.ScalingBufferSize),
		//maximumCapacity:                          int32(b.options.MaximumNumNodes),
		//minimumCapacity:                          int32(b.options.MinimumNumNodes),
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

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
		clusterScheduler.log.Debug("ScalingInterval: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec)
		clusterScheduler.log.Debug("PredictiveAutoscalingEnabled: %v",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().PredictiveAutoscalingEnabled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d",
			clusterScheduler.schedulingPolicy.ScalingConfiguration().ScalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v",
			clusterScheduler.remoteSynchronizationInterval)
		clusterScheduler.log.Debug("Scheduling policy: %s",
			clusterScheduler.schedulingPolicy.Name())
	}

	return clusterScheduler
}

type BaseScheduler struct {
	instance           clusterSchedulerInternal
	cluster            scheduling.Cluster
	hostMapper         HostMapper
	placer             scheduling.Placer
	kernelProvider     KernelProvider
	notificationBroker NotificationBroker
	policyKey          scheduling.PolicyKey

	// addReplicaMutex makes certain operations atomic, specifically operations that target the same
	// kernels (or other resources) and could occur in-parallel (such as being triggered
	// by multiple concurrent RPC requests).
	addReplicaMutex sync.Mutex

	// Mapping of kernel ID to all active add-replica operations associated with that kernel. The inner maps are from Operation ID to AddReplicaOperation.
	activeAddReplicaOpsPerKernel *hashmap.CornelkMap[string, *orderedmap.OrderedMap[string, *scheduling.AddReplicaOperation]]

	// Mapping from new kernel-replica key (i.e., <kernel-id>-<replica-id>) to AddReplicaOperation.
	addReplicaOperationsByKernelReplicaId *hashmap.CornelkMap[string, *scheduling.AddReplicaOperation]

	// Mapping from NewPodName to chan string.
	// In theory, it's possible to receive a PodCreated notification from Kubernetes AFTER the replica within the new Pod
	// has started running and has registered with the Gateway. In this case, we won't be able to retrieve the AddReplicaOperation
	// associated with that replica via the new Pod's name, as that mapping is created when the PodCreated notification is received.
	// In this case, the goroutine handling the replica registration waits on a channel for the associated AddReplicaOperation.
	addReplicaNewPodOrContainerNotifications *hashmap.CornelkMap[string, chan *scheduling.AddReplicaOperation]

	candidateHostMutex sync.Mutex

	// resourceBindingMode describes when resources are committed and uncommitted from Containers.
	resourceBindingMode scheduling.ResourceBindingMode

	// schedulingPolicy specifies the scheduling behavior for the scheduling.Cluster and scheduling.Scheduler.
	schedulingPolicy scheduling.Policy

	//-//-//-//-//-//-//-//-//-//
	//  Scaling Configuration  //
	//-//-//-//-//-//-//-//-//-//
	//gpusPerHost                  float64       // The number of actual GPUs that are available for use on each node/host.
	//virtualGpusPerHost           int32         // The number of virtual GPUs per host.
	//scalingFactor                float64       // scalingFactor defines how many hosts the cluster will provision based on busy Resources.
	//maximumHostsToReleaseAtOnce  int32         // `maximumHostsToReleaseAtOnce` defines how many hosts the cluster can de-provision during a single scale-in event. This is equivalent to Jingyuan's "scaling-in limit" parameter.
	//scalingIntervalSec           int32         // How often to call UpdateRatio in seconds.
	//scalingInterval              time.Duration // How often to call UpdateRatio .
	//scalingLimit                 float64       // scalingLimit defines how many hosts the cluster will provision at maximum based on busy Resources.
	//predictiveAutoscalingEnabled bool          // If enabled, the scaling manager will attempt to over-provision hosts slightly to leave room for fluctuation, and will also scale-in if we are over-provisioned relative to the current request load. If this is disabled, the cluster can still provision new hosts if demand surges, but it will not scale-down, nor will it automatically scale to leave room for fluctuation.
	//scalingBufferSize            int32         // How many extra hosts we provision so that we can quickly scale if needed.
	//minimumCapacity              int32         // The minimum number of nodes we must have available at any time.
	//maximumCapacity              int32         // The maximum number of nodes we may have available at any time. If this value is < 0, then it is unbounded.

	canScaleIn                    bool                         // Can the Cluster/Placer scale-in?
	opts                          *scheduling.SchedulerOptions // Configuration options.
	hostSpec                      types.Spec                   // The types.Spec used when creating new Host instances.
	remoteSynchronizationInterval time.Duration                // remoteSynchronizationInterval specifies how frequently to poll the remote scheduler nodes for updated GPU info.
	lastNodeRefreshTime           time.Time                    // The time at which the nodes were last refreshed.

	// Watches for new Pods/Containers.
	//
	// The concrete/implementing type differs depending on whether we're deployed in Kubernetes Mode or Docker Mode.
	containerEventHandler scheduling.ContainerWatcher

	oversubscribed  *types.Heap // The host index for oversubscribed hosts. Ordering is implemented by schedulerHost.
	undersubscribed *types.Heap // The host index for under-subscribed hosts. Ordering is implemented by schedulerHost.
	idleHosts       *types.Heap

	lastCapacityValidation time.Time         // lastCapacityValidation is the time at which the last call to ValidateCapacity finished.
	stRatio                *types.MovingStat // session/training ratio
	subscriptionRatio      decimal.Decimal   // Subscription ratio.
	maxSubscribedRatio     decimal.Decimal
	invalidated            float64
	lastSubscribedRatio    float64
	pendingSubscribedRatio float64

	log logger.Logger
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
	return s.policyKey
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
	s.candidateHostMutex.Lock()
	defer s.candidateHostMutex.Unlock()
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

	return s.findViableHostForReplica(replica, blacklistedHosts, forTraining)
}

// FindReadyContainer selects one of the scheduling.KernelContainer instances of the specified scheduling.UserSession
// to handle a training event.
func (s *BaseScheduler) FindReadyContainer(session scheduling.UserSession) scheduling.KernelContainer {
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
			s.log.Warn("Found only %d/%d hosts to serve replicas of kernel %s so far.",
				len(hosts), s.schedulingPolicy.NumReplicas(), kernelSpec.Id)

			if !s.isScalingOutEnabled() {
				s.log.Warn("Scaling-out is disabled. Giving up on finding hosts for kernel %s.", kernelSpec.Id)
				break // Give up.
			}

			numHostsRequired := s.schedulingPolicy.NumReplicas() - len(hosts)
			s.log.Debug("Will attempt to provision %d new host(s) so that we can serve kernel %s.",
				numHostsRequired, kernelSpec.Id)

			p := s.cluster.RequestHosts(ctx, int32(numHostsRequired))
			if err := p.Error(); err != nil {
				if errors.Is(err, scheduling.ErrScalingActive) {
					s.log.Debug("Cannot register scale-out operation for kernel %s: there is already an active scale-out operation.",
						kernelSpec.Id)
				} else {
					s.log.Error("Cluster failed to provision %d additional host(s) for us (for kernel %s) because: %v",
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

			err = s.cluster.UpdateIndex(host)
			if err != nil {
				s.log.Error("Error while attempting to update index of host %s (ID=%s): %v",
					host.GetNodeName(), host.GetID(), err)
			}
		}

		return nil, scheduling.ErrInsufficientHostsAvailable
	}

	return hosts, nil
}

// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *BaseScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec, blacklistedHosts []scheduling.Host) error {
	return s.instance.DeployKernelReplicas(ctx, in, blacklistedHosts)
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
		s.log.Error("Failed to add new host because: %v", err)
		s.sendErrorNotification("Failed to Add Host to Cluster", err.Error())
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
		s.log.Error("Failed to remove host %s because: %v", hostId, err)
		s.sendErrorNotification(fmt.Sprintf("Failed to Remove Host %s from the Cluster", hostId), err.Error())
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
func (s *BaseScheduler) addReplica(in *proto.ReplicaInfo, targetHost scheduling.Host, opts scheduling.AddReplicaWaitOptions, dataDirectory string, blacklistedHosts []scheduling.Host, forTraining bool) (*scheduling.AddReplicaOperation, error) {
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

	// Add the AddReplicaOperation to the associated maps belonging to the Gateway Daemon.
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
	err := s.cluster.Scheduler().ScheduleKernelReplica(newReplicaSpec, targetHost, blacklistedHosts, forTraining)
	if err != nil {
		return addReplicaOp, err
	}

	// In Kubernetes deployments, the key is the Pod name, which is also the kernel ID + replica suffix.
	// In Docker deployments, the container name isn't really the container's name, but its ID, which is a hash
	// or something like that.
	s.instance.postScheduleKernelReplica(kernelId, addReplicaOp)

	if opts.WaitRegistered() {
		s.log.Debug("Waiting for new replica %d of kernel \"%s\" to register during AddReplicaOperation \"%s\"",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		replicaRegisteredChannel := addReplicaOp.ReplicaRegisteredChannel()
		_, sentBeforeClosed := <-replicaRegisteredChannel
		if !sentBeforeClosed {
			errorMessage := fmt.Sprintf("Received default value from \"Replica Registered\" channel for AddReplicaOperation \"%s\": %v",
				addReplicaOp.OperationID(), addReplicaOp.String())
			s.log.Error(errorMessage)
			go s.sendErrorNotification("Channel Receive on Closed \"ReplicaRegisteredChannel\" Channel", errorMessage)
		} else {
			addReplicaOp.CloseReplicaRegisteredChannel()
		}

		s.log.Debug("New replica %d of kernel \"%s\" has registered with the Gateway during AddReplicaOperation \"%s\".",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
	}

	var smrWg sync.WaitGroup
	smrWg.Add(1)
	// Separate goroutine because this has to run everytime, even if we don't wait, as we call AddOperationCompleted when the new replica joins its SMR cluster.
	go func() {
		s.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster during AddReplicaOperation \"%s\" now...",
			addReplicaOp.ReplicaId(), kernelId, addReplicaOp.OperationID())
		replicaJoinedSmrChannel := addReplicaOp.ReplicaJoinedSmrChannel()
		_, sentBeforeClosed := <-replicaJoinedSmrChannel
		if !sentBeforeClosed {
			errorMessage := fmt.Sprintf("Received default value from \"Replica Joined SMR\" channel for AddReplicaOperation \"%s\": %v",
				addReplicaOp.OperationID(), addReplicaOp.String())
			s.log.Error(errorMessage)
			go s.sendErrorNotification("Channel Receive on Closed \"ReplicaJoinedSmrChannel\" Channel", errorMessage)
		}

		close(replicaJoinedSmrChannel)
		s.log.Debug("New replica %d of kernel %s has joined its SMR cluster.", addReplicaOp.ReplicaId(), kernelId)
		kernel.AddOperationCompleted()
		smrWg.Done()

		if !addReplicaOp.Completed() {
			s.log.Error("AddReplicaOperation \"%s\" does not think it's done, even though it should...", addReplicaOp.OperationID())
			go s.sendErrorNotification(fmt.Sprintf("AddReplicaOperation \"%s\" is Confused", addReplicaOp.OperationID()),
				fmt.Sprintf("AddReplicaOperation \"%s\" does not think it's done, even though it should: %s",
					addReplicaOp.OperationID(), addReplicaOp.String()))
		}
	}()

	if opts.WaitSmrJoined() {
		s.log.Debug("Waiting for new replica %d of kernel %s to join its SMR cluster...", addReplicaOp.ReplicaId(), kernelId)
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
		// Technically if the number of committed GPUs is zero, then the ratio is infinite (undefined).
		// TODO: Previously, I'd just set the ratio to 0 if BusyGPUs was 0.
		// But technically, it should be undefined/infinite, so I will try setting it to maxSubscribedRatio...
		ratio = s.maxSubscribedRatio.InexactFloat64()

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("DemandGPUs: %.0f. CommittedGPUs: %.0f. Ratio: %.4f.", s.cluster.DemandGPUs(), s.cluster.BusyGPUs(), ratio)
		}
	} else {
		ratio = s.cluster.DemandGPUs() / s.cluster.BusyGPUs()

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("DemandGPUs: %.0f. CommittedGPUs: %.0f. Ratio: %.4f.", s.cluster.DemandGPUs(), s.cluster.BusyGPUs(), ratio)
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

		if !skipValidateCapacity && s.schedulingPolicy.ScalingConfiguration().ScalingIntervalSec > 0 && time.Since(s.lastCapacityValidation) >= s.schedulingPolicy.ScalingConfiguration().ScalingInterval {
			s.ValidateCapacity()
		}

		return true
	}
	return false
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
			s.log.Error("Cluster failed to provision 1 additional host (for replica %d of kernel %s) because: %v",
				replicaSpec.ReplicaID(), replicaSpec.ID(), err)

			return nil, err
		}

		numTries += 1
	}

	return nil, failureReason
}

// MigrateKernelReplica tries to migrate the given Kernel to another Host.
//
// The first error that is returned (i.e., 'reason') does not indicate that an actual error occurred.
// It simply provides an explanation for why the migration failed.
//
// The second error that is returned (i.e., 'err') indicates that an actual error occurs.
func (s *BaseScheduler) MigrateKernelReplica(kernelReplica scheduling.KernelReplica, targetHostId string, forTraining bool) (resp *proto.MigrateKernelResponse, reason error, err error) {
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

		s.cluster.UpdateIndex(targetHost)
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

		s.cluster.UpdateIndex(targetHost)
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

	// Add a new replica. We pass "true" for both options (registration and SMR-joining) so we wait for the replica to start fully.
	opts := scheduling.NewAddReplicaWaitOptions(true, true, true)

	var addReplicaOp *scheduling.AddReplicaOperation
	addReplicaOp, err = s.addReplica(replicaSpec, targetHost, opts, dataDirectory, []scheduling.Host{originalHost}, forTraining)

	// If there's an error here, it's presumably a "real" error, as we already picked out a viable host up above.
	if err != nil {
		s.log.Error("Failed to add new replica %d to kernel %s: %v", kernelReplica.ReplicaID(), kernelReplica.ID(), err)

		releaseReservationError := targetHost.ReleaseReservation(kernelReplica.KernelSpec())
		if releaseReservationError != nil {
			s.log.Error("Failed to release reservation for replica %d of kernel %s after failing to recreate replica during migration: %v",
				kernelReplica.ReplicaID(), kernelReplica.ID(), err)
			err = errors.Join(err, releaseReservationError)
		}

		s.cluster.UpdateIndex(targetHost)
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

		s.cluster.UpdateIndex(targetHost)
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
	if existingReplica != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	go func() {
		gRpcClientConnection := originalHost.GetGrpcConnection()

		if gRpcClientConnection == nil {
			err := fmt.Errorf("gRPC Client Connection with host %s (ID=%s) is nil. I hope we're unit-testing.",
				originalHost.GetNodeName(), originalHost.GetID())
			s.log.Warn(utils.OrangeStyle.Render(err.Error()))
			// resultChan <- err
		} else {
			s.log.Debug("State of gRPC ClientConn with host %s (ID=%s): %s (%v)", originalHost.GetNodeName(),
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
			s.log.Error("Timed-out waiting for response from host %s (ID=%s) for 'prepare-to-migrate' request for replica %d of kernel %s...",
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

// HostAdded is called by the Cluster when a new Host connects to the Cluster.
func (s *BaseScheduler) HostAdded(host scheduling.Host) {
	s.instance.HostAdded(host)
}

// ValidateCapacity validates the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
// Adjust the Cluster's capacity as directed by scaling policy.
func (s *BaseScheduler) ValidateCapacity() {
	var load int32
	s.cluster.RangeOverHosts(func(_ string, host scheduling.Host) bool {
		load += int32(host.CommittedGPUs())
		return true
	})

	scalingFactor := s.schedulingPolicy.ScalingConfiguration().ScalingFactor
	gpusPerHost := s.schedulingPolicy.ScalingConfiguration().GpusPerHost
	scalingLimit := s.schedulingPolicy.ScalingConfiguration().ScalingLimit
	scalingBufferSize := s.schedulingPolicy.ScalingConfiguration().ScalingBufferSize
	predictiveAutoscalingEnabled := s.schedulingPolicy.ScalingConfiguration().PredictiveAutoscalingEnabled
	maximumHostsToReleaseAtOnce := s.schedulingPolicy.ScalingConfiguration().MaximumHostsToReleaseAtOnce

	// minNumHosts := int32(math.Ceil(float64(load) / s.gpusPerHost))                      // The minimum number of hosts required to satisfy the Cluster's current committed GPUs.
	minNumHosts := int32(s.schedulingPolicy.NumReplicas())
	scaledOutNumHosts := int32(math.Ceil(float64(load) * scalingFactor / float64(gpusPerHost))) // The number of hosts we would scale-out to based on the configured scaling factor.
	limit := int32(math.Ceil(float64(load) * scalingLimit / float64(gpusPerHost)))              // The maximum number of hosts we're permitted to scale-out to.

	s.log.Debug("Validating Cluster Capacity. MinNumHosts: %d, ScaledOutNumHosts: %d, Limit: %d",
		minNumHosts, scaledOutNumHosts, limit)

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool more so from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + scalingBufferSize) {
		scaledOutNumHosts = minNumHosts + scalingBufferSize
		s.log.Debug("Adjusted scaledOutNumHosts: %d.", scaledOutNumHosts)
	}
	if limit < minNumHosts+4 {
		limit = minNumHosts + 4
		s.log.Debug("Adjusted limit: %d.", limit)
	}

	if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		s.log.Debug("Load (CommittedGPUs): %d. Current #Hosts: %d. Minimum #Hosts to Satisfy Load: %d. Target #Hosts: %d. Max Scaled-Out #Hosts: %d.",
			load, s.cluster.Len(), minNumHosts, scaledOutNumHosts, limit)
	}
	oldNumHosts := int32(s.cluster.Len())
	// Only scale-out if that feature is enabled.
	if predictiveAutoscalingEnabled && s.cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts {
		// Scaling out
		numProvisioned := 0
		targetNumProvisioned := scaledOutNumHosts - oldNumHosts

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Scaling out by %d hosts (from %d to %d).", targetNumProvisioned, oldNumHosts, scaledOutNumHosts)
		}

		// This is such a minor optimization, but we cache the size of the active host pool locally so that we don't have to grab it everytime.
		// The size of the pending host pool will grow each time we provision a new host.
		numFailures := 0
		for int32(s.cluster.Len()) < scaledOutNumHosts {
			err := s.RequestNewHost()
			if err != nil {
				s.log.Error("Failed to add new host because: %v", err)
				numFailures += 1

				if numFailures > 3 {
					s.log.Error("We've failed three times to provision a new host. Aborting automated operation.")
					return
				} else if errors.Is(err, scheduling.ErrUnsupportedOperation) {
					s.log.Warn("Aborting scale-out operation as we lack sufficient disabled hosts to scale-out, and adding additional hosts directly is not supported by the current cluster type.")
					return
				} else {
					continue
				}
			}

			numProvisioned++
		}

		// If we provisioned any hosts -- or if we were supposed to provision at least one host -- then we'll
		// print a message about how many we provisioned, and how many failures we encountered.
		if (numProvisioned > 0 || targetNumProvisioned > 0) && s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Provisioned %d new hosts based on #CommittedGPUs(%d). Previous #hosts: %d. Current #hosts: %d. #FailedProvisions: %d.", numProvisioned, load, oldNumHosts, s.cluster.Len(), numFailures)
		}
	} else if !s.cluster.CanPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts { // If this was the reason the first if-statement evaluated to false, then we'll log a warning message.
		s.log.Warn("Would like to scale out by %d hosts (from %d to %d); however, cluster cannot possibly scale-out right now.", scaledOutNumHosts-oldNumHosts, oldNumHosts, scaledOutNumHosts)
	}

	// Should we scale in?
	if !s.canScaleIn || !predictiveAutoscalingEnabled || load <= limit {
		return
	}

	// Scaling in.
	// NOTE: Jingyuan's algorithm uses initial capacity here, rather than minimum capacity.
	if limit < s.MinimumCapacity() {
		limit = s.MinimumCapacity()
	}

	numToRelease := int32(s.cluster.Len()) - limit
	if numToRelease > 0 {
		if numToRelease > maximumHostsToReleaseAtOnce {
			if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
				s.log.Debug("Decreased the number of idle hosts to release from %d to the maximum allowed value of %s.", numToRelease, maximumHostsToReleaseAtOnce)
			}
			numToRelease = maximumHostsToReleaseAtOnce
		}

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Scaling in %d hosts", numToRelease)
		}

		numReleased, err := s.ReleaseIdleHosts(numToRelease)
		if err != nil {
			s.log.Error("Error while releasing idle hosts: %v", err)
		}

		if numReleased > 0 && s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Released %d idle hosts based on #CommittedGPUs (%d). Prev #hosts: %s. New #hosts: %s.", numReleased, load, oldNumHosts, s.cluster.Len())
		}
	}

	s.lastCapacityValidation = time.Now()
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
	// max gpu heap
	diff := other.(*idleSortedHost).IdleGPUs() - h.IdleGPUs()
	if diff == 0.0 {
		// max subscription heap for promoting rebalancing.
		return other.(scheduling.Host).SubscribedRatio() - h.SubscribedRatio()
	} else {
		return diff
	}
}

func (h *idleSortedHost) SetIdx(idx int) {
	h.Host.SetIdx(idx)
}

// migrateContainersFromHost attempts to migrate all the kernels scheduled on the specified Host to other Hosts.
func (s *BaseScheduler) migrateContainersFromHost(host scheduling.Host, forTraining bool) (err error) {
	var failedMigrationReason error
	host.Containers().Range(func(containerId string, c scheduling.KernelContainer) (contd bool) {
		_, failedMigrationReason, err = s.MigrateKernelReplica(c.GetClient(), "", forTraining) // Pass true for `noNewHost`, as we don't want to create a new host for this.
		if err != nil {
			// We cannot migrate the Container due to an actual error.
			s.log.Error("Abandoning the release of idle host %s (ID=%s) because we encountered an error while migrating one of the containers: %v", host.GetNodeName(), host.GetID(), err)
			return false
		}

		if failedMigrationReason != nil {
			// We cannot migrate the Container.
			s.log.Warn("Abandoning the release of idle host %s (ID=%s) because: %v", host.GetNodeName(), host.GetID(), err)
			return false
		}

		s.log.Debug("Successfully migrated all kernels from host %s (ID=%s).", host.GetNodeName(), host.GetID())

		// Keep going.
		return true
	})

	return err
}

// includeHostsInScheduling iterates over the given slice of scheduling.Host instances and sets their ExcludedFromScheduling
// field to false.
func (s *BaseScheduler) includeHostsInScheduling(hosts []scheduling.Host) {
	for _, host := range hosts {
		err := host.IncludeForScheduling()
		if err != nil {
			s.log.Error("Host %s (ID=%s) is already allowed to be considered for scheduling (%v)",
				host.GetNodeName(), host.GetID(), err)
		}
	}
}

// ReleaseIdleHosts tries to release n idle hosts. Return the number of hosts that were actually released.
// Error will be nil on success and non-nil if some sort of failure is encountered.
func (s *BaseScheduler) ReleaseIdleHosts(n int32) (int, error) {
	s.validate()

	if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		s.log.Debug("Attempting to release %d idle host(s). There are currently %d host(s) in the Cluster.", n, s.cluster.Len())
	}

	toBeReleased := make([]scheduling.Host, 0, n)
	for int32(len(toBeReleased)) < n && s.idleHosts.Len() > 0 {
		idleHost := s.idleHosts.Peek().(*idleSortedHost)

		// If the host is not completely idle, then we'll break and stop looking.
		if idleHost.IdleGPUs() < idleHost.ResourceSpec().GPU() {
			break
		}

		excluded := idleHost.Host.ExcludeFromScheduling()
		if excluded {
			toBeReleased = append(toBeReleased, idleHost.Host)
		}
	}

	var released int
	for i, host := range toBeReleased {
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Releasing idle host %d/%d: Virtual Machine  %s. NumContainers: %d.", i+1, len(toBeReleased), host.GetID(), host.NumContainers())
		}

		// If the host has no containers running on it at all, then we can simply release the host.
		if host.NumContainers() > 0 {
			err := s.migrateContainersFromHost(host, false) // Host is completely idle, so no training.
			if err != nil {
				s.log.Warn("Failed to migrate all kernels from host %s (ID=%s) because: %v",
					host.GetNodeName(), host.GetID(), err)

				s.includeHostsInScheduling(toBeReleased[i:])

				return released, err
			}
		}

		err := s.RemoveHost(host.GetID())
		if err != nil {
			s.log.Error("Failed to remove host %s (ID=%s) because: %v", host.GetNodeName(), host.GetID(), err)

			s.includeHostsInScheduling(toBeReleased[i:])

			return released, err
		}

		released += 1
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Successfully released idle host %d/%d: Virtual Machine  %s.", i+1, len(toBeReleased), host.GetID())
		}
	}

	return released, nil
}
