package scheduling

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/shopspring/decimal"
	"github.com/zhangjyr/distributed-notebook/common/container"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"math"
	"sync"
	"time"
)

const (
	SchedulerInvalidationThreshold = 0.1
)

var (
	ErrNotImplementedYet = errors.New("this method has not yet been implemented")
)

type aggregateGpuInfo struct {
	*proto.ClusterActualGpuInfo

	TotalSpecGPUs              int32 `json:"specGPUs,omitempty"`              // The total number of GPUs configured/present on this node.
	TotalIdleGPUs              int32 `json:"idleGPUs,omitempty"`              // The number of GPUs that are uncommitted and therefore available on this node. This quantity is equal to specGPUs - committedGPUs.
	TotalCommittedGPUs         int32 `json:"committedGPUs,omitempty"`         // The number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
	TotalPendingGPUs           int32 `json:"pendingGPUs,omitempty"`           // The sum of the outstanding GPUs of all replicas scheduled onto this node. Pending GPUs are not allocated or committed to a particular replica yet. The time at which Resources are actually committed to a replica depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
	TotalNumPendingAllocations int32 `json:"numPendingAllocations,omitempty"` // Number of individual allocations consisting of GPUs that have NOT been fully committed to a kernel.
	TotalNumAllocations        int32 `json:"numAllocations,omitempty"`        // Number of individual allocations such that the GPUs have been committed to a container.
}

// schedulingNotification is a struct that is sent over a channel to notify the "main" goroutine handling the
// scheduling of a new kernel that the scheduling of one of that kernel's replicas has completed successfully.
type schedulingNotification struct {
	// SchedulingCompletedAt is the time at which the scheduling operation concluded
	// for the particular replica of the associated kernel.
	SchedulingCompletedAt time.Time

	// Host is the Host on which the kernel was scheduled (or attempted to be scheduled).
	Host *Host

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

type BaseScheduler struct {
	instance   ClusterScheduler
	cluster    ClusterInternal
	hostMapper HostMapper
	placer     Placer

	candidateHostMutex sync.Mutex

	//-//-//-//-//-//-//-//-//-//
	//  Scaling Configuration  //
	//-//-//-//-//-//-//-//-//-//
	gpusPerHost                   float64                  // The number of actual GPUs that are available for use on each node/host.
	virtualGpusPerHost            int32                    // The number of virtual GPUs per host.
	scalingFactor                 float64                  // scalingFactor defines how many hosts the cluster will provision based on busy Resources.
	maximumHostsToReleaseAtOnce   int32                    // `maximumHostsToReleaseAtOnce` defines how many hosts the cluster can de-provision during a single scale-in event. This is equivalent to Jingyuan's "scaling-in limit" parameter.
	scalingIntervalSec            int32                    // How often to call UpdateRatio in seconds.
	scalingInterval               time.Duration            // How often to call UpdateRatio .
	scalingLimit                  float64                  // scalingLimit defines how many hosts the cluster will provision at maximum based on busy Resources.
	canScaleIn                    bool                     // Can the Cluster/Placer scale-in?
	shouldUpdateRatio             bool                     // Should the Placer update its subscription ratio?
	predictiveAutoscalingEnabled  bool                     // If enabled, the scaling manager will attempt to over-provision hosts slightly to leave room for fluctuation, and will also scale-in if we are over-provisioned relative to the current request load. If this is disabled, the cluster can still provision new hosts if demand surges, but it will not scale-down, nor will it automatically scale to leave room for fluctuation.
	scalingBufferSize             int32                    // How many extra hosts we provision so that we can quickly scale if needed.
	minimumCapacity               int32                    // The minimum number of nodes we must have available at any time.
	maximumCapacity               int32                    // The maximum number of nodes we may have available at any time. If this value is < 0, then it is unbounded.
	opts                          *ClusterSchedulerOptions // Configuration options.
	hostSpec                      types.Spec               // The types.Spec used when creating new Host instances.
	remoteSynchronizationInterval time.Duration            // remoteSynchronizationInterval specifies how frequently to poll the remote scheduler nodes for updated GPU info.
	lastNodeRefreshTime           time.Time                // The time at which the nodes were last refreshed.

	oversubscribed  container.Heap // The host index for oversubscribed hosts. Ordering is implemented by schedulerHost.
	undersubscribed container.Heap // The host index for under-subscribed hosts. Ordering is implemented by schedulerHost.
	idleHosts       container.Heap

	lastCapacityValidation time.Time         // lastCapacityValidation is the time at which the last call to ValidateCapacity finished.
	stRatio                *types.MovingStat // session/training ratio
	subscriptionRatio      decimal.Decimal   // Subscription ratio.
	maxSubscribedRatio     decimal.Decimal
	invalidated            float64
	lastSubscribedRatio    float64
	pendingSubscribedRatio float64

	log logger.Logger
}

func NewBaseScheduler(cluster ClusterInternal, placer Placer, hostMapper HostMapper, hostSpec types.Spec, opts *ClusterSchedulerOptions) *BaseScheduler {
	clusterScheduler := &BaseScheduler{
		cluster:                       cluster,
		hostMapper:                    hostMapper,
		gpusPerHost:                   float64(opts.GpusPerHost),
		virtualGpusPerHost:            int32(opts.VirtualGpusPerHost),
		scalingFactor:                 opts.ScalingFactor,
		scalingLimit:                  opts.ScalingLimit,
		maximumHostsToReleaseAtOnce:   int32(opts.MaximumHostsToReleaseAtOnce),
		scalingIntervalSec:            int32(opts.ScalingInterval),
		predictiveAutoscalingEnabled:  opts.PredictiveAutoscalingEnabled,
		scalingBufferSize:             int32(opts.ScalingBufferSize),
		stRatio:                       types.NewMovingStatFromWindow(5),
		opts:                          opts,
		remoteSynchronizationInterval: time.Second * time.Duration(opts.GpuPollIntervalSeconds),
		placer:                        placer,
		hostSpec:                      hostSpec,
		oversubscribed:                make(container.Heap, 0, 10),
		undersubscribed:               make(container.Heap, 0, 10),
		idleHosts:                     make(container.Heap, 0, 10),
		maximumCapacity:               int32(opts.MaximumNumNodes),
		minimumCapacity:               int32(opts.MinimumNumNodes),
		maxSubscribedRatio:            decimal.NewFromFloat(opts.MaxSubscribedRatio),
		subscriptionRatio:             decimal.NewFromFloat(opts.MaxSubscribedRatio),
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	if opts.GpuPollIntervalSeconds <= 0 {
		clusterScheduler.remoteSynchronizationInterval = time.Second * 5
	}

	if clusterScheduler.scalingIntervalSec < 0 {
		clusterScheduler.scalingIntervalSec = 30
		clusterScheduler.scalingInterval = time.Second * time.Duration(30)
	}

	if clusterScheduler.log.GetLevel() == logger.LOG_LEVEL_ALL {
		clusterScheduler.log.Debug("Scheduling Configuration:")
		clusterScheduler.log.Debug("GpusPerHost: %.2f", clusterScheduler.gpusPerHost)
		clusterScheduler.log.Debug("VirtualGpusPerHost: %d", clusterScheduler.virtualGpusPerHost)
		clusterScheduler.log.Debug("ScalingFactor: %.2f", clusterScheduler.scalingFactor)
		clusterScheduler.log.Debug("ScalingLimit: %.2f", clusterScheduler.scalingLimit)
		clusterScheduler.log.Debug("MaximumHostsToReleaseAtOnce: %d", clusterScheduler.maximumHostsToReleaseAtOnce)
		clusterScheduler.log.Debug("ScalingInterval: %d", clusterScheduler.scalingIntervalSec)
		clusterScheduler.log.Debug("PredictiveAutoscalingEnabled: %v", clusterScheduler.predictiveAutoscalingEnabled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d", clusterScheduler.scalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v", clusterScheduler.remoteSynchronizationInterval)
	}

	return clusterScheduler
}

func (s *BaseScheduler) SetHostSpec(spec types.Spec) {
	s.hostSpec = spec
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

// Placer returns the Placer used by the BaseScheduler.
func (s *BaseScheduler) Placer() Placer {
	return s.placer
}

// TryGetCandidateHosts performs a single attempt/pass of searching for candidate Host instances.
//
// TryGetCandidateHosts is exported so that it can be unit tested.
func (s *BaseScheduler) TryGetCandidateHosts(hosts []*Host, kernelSpec *proto.KernelSpec) []*Host {
	s.candidateHostMutex.Lock()

	// Identify the hosts onto which we will place replicas of the kernel.
	numHostsRequired := s.opts.NumReplicas - len(hosts)
	s.log.Debug("Searching for %d candidate host(s) for kernel %s. Have identified %d candidate host(s) so far.",
		numHostsRequired, kernelSpec.Id, len(hosts))

	hostBatch := s.placer.FindHosts(kernelSpec, numHostsRequired)

	// Add all the hosts returned by FindHosts to our running slice of hosts.
	for _, host := range hostBatch {
		hosts = append(hosts, host)
	}

	s.candidateHostMutex.Unlock()

	return hosts
}

// GetCandidateHosts returns a slice of *Host containing Host instances that could serve
// a Container (i.e., a kernel replica) with the given resource requirements (encoded as a types.Spec).
//
// GetCandidateHosts will automatically request that new Host instances be provisioned and added to the Cluster
// if it fails to find sufficiently many viable Host instances. This process will be attempted three times.
// If GetCandidateHosts is unsuccessful (at finding sufficiently many viable hosts) after those three attempts,
// then GetCandidateHosts will give up and return an error.
//
// The size of the returned slice will be equal to the configured number of replicas for each kernel (usually 3).
//
// This function is NOT idempotent. This locks the Hosts that are returned.
func (s *BaseScheduler) GetCandidateHosts(ctx context.Context, kernelSpec *proto.KernelSpec) ([]*Host, error) {
	var (
		numTries    = 0
		maxAttempts = 3
		bestAttempt = -1
		hosts       []*Host
	)
	for numTries < maxAttempts && len(hosts) < s.opts.NumReplicas {
		hosts = s.TryGetCandidateHosts(hosts, kernelSpec)

		if len(hosts) < s.opts.NumReplicas {
			s.log.Warn("Found only %d/%d hosts to serve replicas of kernel %s so far.",
				len(hosts), s.opts.NumReplicas, kernelSpec.Id)

			numHostsRequired := s.opts.NumReplicas - len(hosts)
			s.log.Debug("Will attempt to provision %d new host(s).", numHostsRequired)

			p := s.cluster.RequestHosts(ctx, int32(numHostsRequired))
			if err := p.Error(); err != nil {
				s.log.Error("Cluster failed to provision %d additional host(s) for us because: %v", numHostsRequired, err)
			}

			numTries += 1

			if len(hosts) > bestAttempt {
				bestAttempt = len(hosts)
			}

			if (numTries + 1) < maxAttempts {
				// Don't want to print this if we've just used up our last try, so to speak.
				s.log.Debug("Trying again to find %d hosts to serve replicas of kernel %s.", s.opts.NumReplicas, kernelSpec.Id)
			}
		} else {
			s.log.Debug("Found %d hosts to serve replicas of kernel %s: %v", s.opts.NumReplicas, kernelSpec.Id, hosts)
			break
		}
	}

	// Check if we were able to reserve the requested number of hosts.
	// If not, then we need to release any hosts we did reserve.
	if len(hosts) < s.opts.NumReplicas {
		s.log.Warn("Failed to find %d hosts to serve replicas of kernel %s after %d tries...",
			s.opts.NumReplicas, kernelSpec.Id, numTries)

		// Release any resource reservations that we created, since we're aborting the scheduling
		// of the replicas of the kernel.
		for _, host := range hosts {
			err := host.ReleaseReservation(kernelSpec)
			if err != nil {
				s.log.Error("Failed to release resource reservation for kernel %s on host %s (ID=%s): %v",
					kernelSpec.Id, host.NodeName, host.ID, err)
			}
		}

		return nil, fmt.Errorf("%w: could only find at-most %d/%d required hosts to serve replicas of kernel %s",
			ErrInsufficientHostsAvailable, bestAttempt, s.opts.NumReplicas, kernelSpec.Id)
	}

	return hosts, nil
}

// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *BaseScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec, blacklistedHosts []*Host) error {
	return s.instance.DeployNewKernel(ctx, in, blacklistedHosts)
}

// RemoteSynchronizationInterval returns the interval at which the ClusterScheduler synchronizes
// the Host instances within the Cluster with their remote nodes.
func (s *BaseScheduler) RemoteSynchronizationInterval() time.Duration {
	return s.remoteSynchronizationInterval
}

// MinimumCapacity returns the minimum number of nodes we must have available at any time.
func (s *BaseScheduler) MinimumCapacity() int32 {
	return s.minimumCapacity
}

// AddHost adds a new node to the kubernetes Cluster.
// We simulate this using node taints.
func (s *BaseScheduler) AddHost() error {
	p := s.cluster.RequestHosts(context.Background(), 1) /* s.hostSpec */
	err := p.Error()
	if err != nil {
		s.log.Error("Failed to add new host because: %v", err)
		return err
	}

	return nil
}

// RemoveHost removes a new from the kubernetes Cluster.
// We simulate this using node taints.
func (s *BaseScheduler) RemoveHost(hostId string) error {
	p := s.cluster.ReleaseSpecificHosts(context.Background(), []string{hostId})
	err := p.Error()
	if err != nil {
		s.log.Error("Failed to release host %s because: %v", hostId, err)
		return err
	}

	return nil
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

		if !skipValidateCapacity && s.scalingIntervalSec > 0 && time.Since(s.lastCapacityValidation) >= s.scalingInterval {
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

func (s *BaseScheduler) MigrateContainer(container *Container, host *Host, b bool) error {
	return s.instance.MigrateContainer(container, host, b)
}

// ValidateCapacity validates the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
// Adjust the Cluster's capacity as directed by scaling policy.
func (s *BaseScheduler) ValidateCapacity() {
	var load int32
	s.cluster.RangeOverHosts(func(_ string, host *Host) bool {
		load += int32(host.CommittedGPUs())
		return true
	})

	// minNumHosts := int32(math.Ceil(float64(load) / s.gpusPerHost))                      // The minimum number of hosts required to satisfy the Cluster's current committed GPUs.
	minNumHosts := int32(s.opts.NumReplicas)
	scaledOutNumHosts := int32(math.Ceil(float64(load) * s.scalingFactor / s.gpusPerHost)) // The number of hosts we would scale-out to based on the configured scaling factor.
	limit := int32(math.Ceil(float64(load) * s.scalingLimit / s.gpusPerHost))              // The maximum number of hosts we're permitted to scale-out to.

	s.log.Debug("Validating Cluster Capacity. MinNumHosts: %d, ScaledOutNumHosts: %d, Limit: %d",
		minNumHosts, scaledOutNumHosts, limit)

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool more so from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + s.scalingBufferSize) {
		scaledOutNumHosts = minNumHosts + s.scalingBufferSize
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
	if s.predictiveAutoscalingEnabled && s.cluster.canPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts {
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
			err := s.AddHost()
			if err != nil {
				s.log.Error("Failed to add new host because: %v", err)
				numFailures += 1

				if numFailures > 3 {
					s.log.Error("We've failed three times to provision a new host. Aborting automated operation.")
					return
				} else if errors.Is(err, ErrUnsupportedOperation) {
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
	} else if !s.cluster.canPossiblyScaleOut() && oldNumHosts < scaledOutNumHosts { // If this was the reason the first if-statement evaluated to false, then we'll log a warning message.
		s.log.Warn("Would like to scale out by %d hosts (from %d to %d); however, cluster cannot possibly scale-out right now.", scaledOutNumHosts-oldNumHosts, oldNumHosts, scaledOutNumHosts)
	}

	// Should we scale in?
	if !s.canScaleIn || !s.predictiveAutoscalingEnabled || load <= limit {
		return
	}

	// Scaling in.
	// NOTE: Jingyuan's algorithm uses initial capacity here, rather than minimum capacity.
	if limit < s.MinimumCapacity() {
		limit = s.MinimumCapacity()
	}

	numToRelease := int32(s.cluster.Len()) - limit
	if numToRelease > 0 {
		if numToRelease > s.maximumHostsToReleaseAtOnce {
			if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
				s.log.Debug("Decreased the number of idle hosts to release from %d to the maximum allowed value of %s.", numToRelease, s.maximumHostsToReleaseAtOnce)
			}
			numToRelease = s.maximumHostsToReleaseAtOnce
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
func (s *BaseScheduler) designateSubscriptionPoolType(host *Host, pool heap.Interface, t SchedulerPoolType) {
	host.SetSchedulerPoolType(t)
	heap.Push(pool, host)
}

func (s *BaseScheduler) validate() {
	if s.invalidated > SchedulerInvalidationThreshold {
		// StaticPlacerMaxSubscribedRatio increase, release oversubscribed hosts to under-subscribed hosts.
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Apply subscription ratio change %.4f -> %.4f, add under-subscription hosts to candidate pool",
				s.lastSubscribedRatio, s.pendingSubscribedRatio)
		}

		for s.oversubscribed.Len() > 0 && s.oversubscribed.Peek().(*Host).OversubscriptionFactor().LessThan(decimal.Zero) {
			host := s.oversubscribed.Peek()
			heap.Pop(&s.oversubscribed)
			s.designateSubscriptionPoolType(host.(*Host), &s.undersubscribed, SchedulerPoolTypeUndersubscribed)
		}

		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
	} else if s.invalidated < (-1 * SchedulerInvalidationThreshold) {
		s.lastSubscribedRatio = s.pendingSubscribedRatio
		s.invalidated = 0.0
	}
}

type idleSortedHost struct {
	*Host
}

func (h *idleSortedHost) Compare(other interface{}) float64 {
	// max gpu heap
	diff := other.(*idleSortedHost).IdleGPUs() - h.IdleGPUs()
	if diff == 0.0 {
		// max subscription heap for promoting rebalancing.
		return other.(*Host).SubscribedRatio() - h.SubscribedRatio()
	} else {
		return diff
	}
}

func (h *idleSortedHost) SetIdx(idx int) {
	h.SetIdx(idx)
}

// migrateContainersFromHost attempts to migrate all the kernels scheduled on the specified Host to other Hosts.
func (s *BaseScheduler) migrateContainersFromHost(host *Host) (err error) {
	host.containers.Range(func(containerId string, c *Container) (contd bool) {
		err = s.MigrateContainer(c, host, true) // Pass true for `noNewHost`, as we don't want to create a new host for this.
		if err != nil {
			// We cannot migrate the Container.
			s.log.Warn("Abandoning the release of idle host %s (ID=%s) because: %v", host.NodeName, host.ID, err)
			return false
		}

		s.log.Debug("Successfully migrated all kernels from host %s (ID=%s).", host.NodeName, host.ID)

		// Keep going.
		return true
	})

	return err
}

// includeHostsInScheduling iterates over the given slice of *Host instances and sets their ExcludedFromScheduling
// field to false.
func (s *BaseScheduler) includeHostsInScheduling(hosts []*Host) {
	for _, host := range hosts {
		err := host.IncludeForScheduling()
		if err != nil {
			s.log.Error("Host %s (ID=%s) is already allowed to be considered for scheduling (%v)",
				host.NodeName, host.ID, err)
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

	toBeReleased := make([]*Host, 0, n)
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
			s.log.Debug("Releasing idle host %d/%d: Virtual Machine  %s. NumContainers: %d.", i+1, len(toBeReleased), host.ID, host.NumContainers())
		}

		// If the host has no containers running on it at all, then we can simply release the host.
		if host.NumContainers() > 0 {
			err := s.migrateContainersFromHost(host)
			if err != nil {
				s.log.Warn("Failed to migrate all kernels from host %s (ID=%s) because: %v",
					host.NodeName, host.ID, err)

				s.includeHostsInScheduling(toBeReleased[i:])

				return released, err
			}
		}

		err := s.RemoveHost(host.ID)
		if err != nil {
			s.log.Error("Failed to remove host %s (ID=%s) because: %v", host.NodeName, host.ID, err)

			s.includeHostsInScheduling(toBeReleased[i:])

			return released, err
		}

		released += 1
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Successfully released idle host %d/%d: Virtual Machine  %s.", i+1, len(toBeReleased), host.ID)
		}
	}

	return released, nil
}
