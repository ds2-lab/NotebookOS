package scheduling

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/container"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"math"
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
	TotalPendingGPUs           int32 `json:"pendingGPUs,omitempty"`           // The sum of the outstanding GPUs of all replicas scheduled onto this node. Pending GPUs are not allocated or committed to a particular replica yet. The time at which resources are actually committed to a replica depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
	TotalNumPendingAllocations int32 `json:"numPendingAllocations,omitempty"` // Number of individual allocations consisting of GPUs that have NOT been fully committed to a kernel.
	TotalNumAllocations        int32 `json:"numAllocations,omitempty"`        // Number of individual allocations such that the GPUs have been committed to a container.
}

type BaseScheduler struct {
	gateway ClusterGateway

	instance ClusterScheduler
	cluster  Cluster
	placer   Placer

	gpuInfo                *aggregateGpuInfo // The current "actual" GPU usage within the cluster.
	lastGpuInfoRefresh     time.Time         // The time at which the 'actual' GPU info was last refreshed.
	gpuInfoRefreshInterval time.Duration     // gpuInfoRefreshInterval specifies how frequently to poll the remote scheduler nodes for updated GPU info.

	lastNodeRefreshTime time.Time // The time at which the nodes were last refreshed.

	//-//-//-//-//-//-//-//-//-//
	//  Scaling Configuration  //
	//-//-//-//-//-//-//-//-//-//
	gpusPerHost                 float64                  // The number of actual GPUs that are available for use on each node/host.
	virtualGpusPerHost          int32                    // The number of virtual GPUs per host.
	scalingFactor               float64                  // scalingFactor defines how many hosts the cluster will provision based on busy resources.
	maximumHostsToReleaseAtOnce int32                    // `maximumHostsToReleaseAtOnce` defines how many hosts the cluster can de-provision during a single scale-in event. This is equivalent to Jingyuan's "scaling-in limit" parameter.
	scalingIntervalSec          int32                    // How often to call UpdateRatio in seconds.
	scalingInterval             time.Duration            // How often to call UpdateRatio .
	scalingLimit                float64                  // scalingLimit defines how many hosts the cluster will provision at maximum based on busy resources.
	canScalingIn                bool                     // Can the Cluster/Placer scale-in?
	shouldUpdateRatio           bool                     // Should the Placer update its subscription ratio?
	scalingOutEnabled           bool                     // If enabled, the scaling manager will attempt to over-provision hosts slightly to leave room for fluctuation. If disabled, then the Cluster will exclusively scale-out in response to real-time demand, rather than attempt to have some hosts available in the case that demand surges.
	scalingBufferSize           int32                    // How many extra hosts we provision so that we can quickly scale if needed.
	minimumCapacity             int32                    // The minimum number of nodes we must have available at any time.
	opts                        *ClusterSchedulerOptions // Configuration options.
	hostSpec                    types.Spec               // The types.Spec used when creating new Host instances.

	lastCapacityValidation time.Time         // lastCapacityValidation is the time at which the last call to ValidateCapacity finished.
	stRatio                *types.MovingStat // session/training ratio
	subscriptionRatio      float64           // Subscription ratio.
	invalidated            float64
	lastSubscribedRatio    float64
	pendingSubscribedRatio float64
	idleHosts              container.Heap

	log logger.Logger
}

func NewBaseScheduler(gateway ClusterGateway, cluster Cluster, placer Placer, hostSpec types.Spec, opts *ClusterSchedulerOptions) *BaseScheduler {
	clusterScheduler := &BaseScheduler{
		gateway:                     gateway,
		cluster:                     cluster,
		gpusPerHost:                 float64(opts.GpusPerHost),
		virtualGpusPerHost:          int32(opts.VirtualGpusPerHost),
		scalingFactor:               opts.ScalingFactor,
		scalingLimit:                opts.ScalingLimit,
		maximumHostsToReleaseAtOnce: int32(opts.MaximumHostsToReleaseAtOnce),
		scalingIntervalSec:          int32(opts.ScalingInterval),
		scalingOutEnabled:           opts.ScalingOutEnabled,
		scalingBufferSize:           int32(opts.ScalingBufferSize),
		minimumCapacity:             int32(opts.MinimumNumNodes),
		stRatio:                     types.NewMovingStatFromWindow(5),
		opts:                        opts,
		gpuInfoRefreshInterval:      time.Second * time.Duration(opts.GpuPollIntervalSeconds),
		placer:                      placer,
		hostSpec:                    hostSpec,
		idleHosts:                   make(container.Heap, 0, 10),
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	if opts.GpuPollIntervalSeconds <= 0 {
		clusterScheduler.gpuInfoRefreshInterval = time.Second * 5
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
		clusterScheduler.log.Debug("ScalingOutEnabled: %v", clusterScheduler.scalingOutEnabled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d", clusterScheduler.scalingBufferSize)
		clusterScheduler.log.Debug("GPU Refresh Interval: %v", clusterScheduler.gpuInfoRefreshInterval)
	}

	return clusterScheduler
}

func (s *BaseScheduler) SetHostSpec(spec types.Spec) {
	s.hostSpec = spec
}

// DeployNewKernel is responsible for scheduling the replicas of a new kernel onto Host instances.
func (s *BaseScheduler) DeployNewKernel(ctx context.Context, in *proto.KernelSpec) error {
	return s.instance.DeployNewKernel(ctx, in)
}

// ClusterGateway returns the associated ClusterGateway.
func (s *BaseScheduler) ClusterGateway() ClusterGateway {
	return s.gateway
}

// MinimumCapacity returns the minimum number of nodes we must have available at any time.
func (s *BaseScheduler) MinimumCapacity() int32 {
	return s.minimumCapacity
}

// AddNode adds a new node to the kubernetes cluster.
// We simulate this using node taints.
func (s *BaseScheduler) AddNode() error {
	p := s.cluster.RequestHost(s.hostSpec)
	err := p.Error()
	if err != nil {
		s.log.Error("Failed to add new host because: %v", err)
		return err
	}

	return nil
}

// RemoveNode removes a new from the kubernetes cluster.
// We simulate this using node taints.
func (s *BaseScheduler) RemoveNode(hostId string) error {
	p := s.cluster.ReleaseHost(hostId)
	err := p.Error()
	if err != nil {
		s.log.Error("Failed to release host %s because: %v", hostId, err)
		return err
	}

	return nil
}

// currentSize returns the current number of nodes in the kubernetes cluster.
// This includes both nodes that are already running and nodes that are being provisioned.
// TODO(Ben): Implement "node provisioning" (i.e., simulating the time it takes to spin-up a new node).
func (s *BaseScheduler) currentSize() int32 {
	return int32(s.cluster.GetHostManager().Len())
}

// activeSize returns the number of active/already-running/already-provisioned hosts within the Cluster.
func (s *BaseScheduler) activeSize() int32 {
	return int32(s.cluster.GetHostManager().Len())
}

// pendingSize returns the number of hosts currently being provisioned within the Cluster.
func (s *BaseScheduler) pendingSize() int32 {
	panic("Not implemented yet.")
}

// RefreshAll refreshes all metrics maintained/cached/required by the Cluster Scheduler,
// including the list of current kubernetes nodes, actual and virtual GPU usage information, etc.
//
// Return a slice of any errors that occurred. If an error occurs while refreshing a particular piece of information,
// then the error is recorded, and the refresh proceeds, attempting all refreshes (even if an error occurs during one refresh).
func (s *BaseScheduler) RefreshAll() []error {
	errs := make([]error, 0)
	var err error

	if s.ClusterGateway().KubernetesMode() {
		err = s.RefreshClusterNodes()
		if err != nil {
			errs = append(errs, err)
		}
	}

	err = s.RefreshActualGpuInfo()
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		s.log.Error("%d error(s) occurred while refreshing all metrics.", len(errs))
	}

	return errs
}

// RefreshActualGpuInfo refreshes the actual GPU usage information.
// Returns nil on success; returns an error on failure.
func (s *BaseScheduler) RefreshActualGpuInfo() error {
	clusterGpuInfo, err := s.gateway.GetClusterActualGpuInfo(context.TODO(), &proto.Void{})
	if err != nil {
		s.log.Error("Error while to retrieving 'actual' GPU info: %v", err)
	} else {
		aggregateGpuInfo := &aggregateGpuInfo{
			ClusterActualGpuInfo: clusterGpuInfo,
		}

		for _, gpuInfo := range clusterGpuInfo.GpuInfo {
			aggregateGpuInfo.TotalSpecGPUs += gpuInfo.SpecGPUs
			aggregateGpuInfo.TotalIdleGPUs += gpuInfo.IdleGPUs
			aggregateGpuInfo.TotalCommittedGPUs += gpuInfo.CommittedGPUs
			aggregateGpuInfo.TotalPendingGPUs += gpuInfo.PendingGPUs
			aggregateGpuInfo.TotalNumPendingAllocations += gpuInfo.NumPendingAllocations
			aggregateGpuInfo.TotalNumAllocations += gpuInfo.NumAllocations
		}

		s.gpuInfo = aggregateGpuInfo
		s.lastGpuInfoRefresh = time.Now()
		s.log.Debug("Successfully refreshed 'actual' GPU info.")
	}

	return err // Will be nil if there was no error.
}

// RefreshClusterNodes updates the cached list of Kubernetes nodes.
// Returns nil on success; returns an error on failure.
func (s *BaseScheduler) RefreshClusterNodes() error {
	return s.instance.RefreshClusterNodes()
}

func (s *BaseScheduler) UpdateRatio() bool {
	var ratio float64
	if s.cluster.BusyGPUs() == 0 || s.cluster.DemandGPUs() == 0 {
		// Technically if the number of committed GPUs is zero, then the ratio is infinite (undefined).
		ratio = 0.0

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("DemandGPUs: %.1f. CommittedGPUs: %.1f. Ratio: %.4f.", s.cluster.DemandGPUs(), s.cluster.BusyGPUs(), ratio)
		}
	} else {
		ratio = s.cluster.DemandGPUs() / s.cluster.BusyGPUs()

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("DemandGPUs: %.1f. CommittedGPUs: %.1f. Ratio: %.4f.", s.cluster.DemandGPUs(), s.cluster.BusyGPUs(), ratio)
		}
	}

	s.stRatio.Add(ratio)
	avg := s.stRatio.Avg()
	s.cluster.SetSubscriptionRatio(avg)
	if s.stRatio.N() == s.stRatio.Window() {
		if !s.canScalingIn {
			s.log.Debug("We can now scale-in.")
			s.canScalingIn = true
		}
		s.subscriptionRatio = avg
		s.rebalance(avg)

		if s.scalingIntervalSec > 0 && time.Since(s.lastCapacityValidation) >= s.scalingInterval {
			s.ValidateCapacity()
		}

		return true
	}
	return false
}

func (s *BaseScheduler) rebalance(newRatio float64) {
	s.pendingSubscribedRatio = newRatio
	s.invalidated = newRatio - s.lastSubscribedRatio
}

func (s *BaseScheduler) MigrateContainer(container *Container, host *Host, b bool) (bool, error) {
	return s.instance.MigrateContainer(container, host, b)
}

// ValidateCapacity validates the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
// Adjust the Cluster's capacity as directed by scaling policy.
func (s *BaseScheduler) ValidateCapacity() {
	load := s.gpuInfo.TotalCommittedGPUs

	// minNumHosts := int32(math.Ceil(float64(load) / s.gpusPerHost))                      // The minimum number of hosts required to satisfy the cluster's current committed GPUs.
	minNumHosts := int32(s.opts.NumReplicas)
	scaledOutNumHosts := int32(math.Ceil(float64(load) * s.scalingFactor / s.gpusPerHost)) // The number of hosts we would scale-out to based on the configured scaling factor.
	limit := int32(math.Ceil(float64(load) * s.scalingLimit / s.gpusPerHost))              // The maximum number of hosts we're permitted to scale-out to.

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool more so from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + s.scalingBufferSize) {
		scaledOutNumHosts = minNumHosts + s.scalingBufferSize
		// s.log.Debug("Adjusted scaledOutNumHosts: %s.", scaledOutNumHosts)
	}
	if limit < minNumHosts+4 {
		limit = minNumHosts + 4
		// s.log.Debug("Adjusted limit: %s.", limit)
	}

	if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		s.log.Debug("Load (CommittedGPUs): %s. Current #Hosts: %s. Minimum #Hosts to Satisfy Load: %s. Target #Hosts: %s. Max Scaled-Out #Hosts: %s.", load, s.currentSize(), minNumHosts, scaledOutNumHosts, limit)
	}
	oldNumHosts := s.currentSize()
	// Only scale-out if that feature is enabled.
	if s.scalingOutEnabled && oldNumHosts < scaledOutNumHosts {
		// Scaling out
		var numProvisioned = 0

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Scaling-out by %d hosts (from %d to %d).", scaledOutNumHosts-oldNumHosts, oldNumHosts, scaledOutNumHosts)
		}

		// This is such a minor optimization, but we cache the size of the active host pool locally so that we don't have to grab it everytime.
		// The size of the pending host pool will grow each time we provision a new host.
		var activeSize = s.activeSize()
		for (activeSize + s.pendingSize()) < scaledOutNumHosts {
			err := s.AddNode()
			if err != nil {
				s.log.Error("Failed to add new host because: %v", err)

				// TODO (Ben): Figure out how to proceed here. For now, just return...
				return
			}

			numProvisioned++
		}

		if numProvisioned > 0 && s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Provisioned %d new hosts based on #CommittedGPUs(%d). Previous #hosts: %s. New #hosts: %s.", numProvisioned, load, oldNumHosts, s.currentSize())
		}
	}

	// Should we scale in?
	if !s.canScalingIn || load <= limit {
		return
	}

	// Scaling in.
	// TODO(Ben): Jingyuan used initial capacity here instead of minimum capacity. But my initial capacity is very high.
	if limit < s.MinimumCapacity() {
		limit = s.MinimumCapacity()
	}

	numToRelease := s.currentSize() - limit
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
			s.log.Debug("Released %d idle hosts based on #CommittedGPUs (%d). Prev #hosts: %s. New #hosts: %s.", numReleased, load, oldNumHosts, s.currentSize())
		}
	}

	s.lastCapacityValidation = time.Now()
}

func (s *BaseScheduler) validate() {
	if s.invalidated > SchedulerInvalidationThreshold {
		// StaticPlacerMaxSubscribedRatio increase, release oversubscribed hosts to undersubscribed hosts.
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Apply subscription ratio change %.4f -> %.4f, add under-subscription hosts to candidate pool", s.lastSubscribedRatio, s.pendingSubscribedRatio)
		}
		// TODO: Implement this.
		panic("Not implemented!")
		//for s.oversubscribed.Len() > 0 && s.oversubscribed.Peek().(*Host).OversubscriptionFactor() < 0.0 {
		//	host := s.oversubscribed.Peek()
		//	heap.Pop(&s.oversubscribed)
		//	s.designateSubscriptionPoolType(host.(*Host), &s.undersubscribed, SchedulerPoolTypeUndersubscribed)
		//}
		//s.lastSubscribedRatio = s.pendingSubscribedRatio
		//s.invalidated = 0.0
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

// ReleaseIdleHosts tries to release n idle hosts. Return the number of hosts that were actually released.
// Error will be nil on success and non-nil if some sort of failure is encountered.
func (s *BaseScheduler) ReleaseIdleHosts(n int32) (int, error) {
	s.validate()

	if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		s.log.Debug("Attempting to release %d idle host(s). There are currently %d host(s) in the Cluster.", n, s.cluster.GetHostManager().Len())
	}

	toBeReleased := make([]*Host, 0, n)
	for int32(len(toBeReleased)) < n && s.idleHosts.Len() > 0 {
		idleHost := s.idleHosts.Peek().(*idleSortedHost)

		if idleHost.IdleGPUs() < idleHost.ResourceSpec().GPU() {
			break
		}

		// TODO: Just mark the Host as un-schedule-able so that we don't try to schedule anything onto it until we're done here.
		// We should not release the Host until we're sure we want to release it.
		//p := s.cluster.ReleaseHost(idleHost.Host.ID())
		panic("Not implemented")
		//err := p.Error()
		//if errors.Is(err, ErrHostNotFound) {
		//	panic(err)
		//}
		//
		//toBeReleased = append(toBeReleased, idleHost.Host)
	}

	var released int
	for i, host := range toBeReleased {
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Releasing idle host %d/%d: Virtual Machine  %s. NumContainers: %d.", i+1, len(toBeReleased), host.ID(), host.NumContainers())
		}

		// If the host has no containers running on it at all, then we can simply release the host.
		if host.NumContainers() > 0 {
			host.containers.Range(func(containerId string, c *Container) (contd bool) {
				// TODO: Migrate Container needs to actually migrate replicas.
				migratedSuccessfully, err := s.MigrateContainer(c, host, true) // Pass true for `noNewHost`, as we don't want to create a new host for this.

				if !migratedSuccessfully {
					if err == nil {
						// We cannot migrate the Container.
						s.log.Warn("Abandoning the release of idle host %d/%d: Virtual Machine %s. There was no error, but the migration failed...", i+1, len(toBeReleased), host.ID())
						panic(fmt.Sprintf("Releasing of idle host failed for unknown reason (no error). We were trying to migrate Container %s [state=%v].", c.ContainerID(), c.Status()))
					} else {
						// We cannot migrate the Container.
						s.log.Warn("Abandoning the release of idle host %d/%d: Virtual Machine %s.", i+1, len(toBeReleased), host.ID())
						s.log.Warn("Reason: %v", err)

						// Add back all the idle hosts that we were going to release, beginning with the host that we should fail to release.
						for j := i; j < len(toBeReleased); j++ {
							host = toBeReleased[j]

							// TODO: Mark the host as schedule-able again.
							panic("Not implemented")
							// s.cluster.ActiveServerfulScheduler().AddResource(host, true)
						}
						return false
					}
				}

				return true
			})
		}

		released += 1
		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Successfully released idle host %d/%d: Virtual Machine  %s.", i+1, len(toBeReleased), host.ID())
		}
	}

	return released, nil
}
