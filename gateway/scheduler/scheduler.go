package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
	corev1 "k8s.io/api/core/v1"
	scheduler "k8s.io/kube-scheduler/extender/v1"
)

var (
	ErrNotImplementedYet = errors.New("this method has not yet been implemented")
)

type aggregateGpuInfo struct {
	*gateway.ClusterActualGpuInfo

	TotalSpecGPUs              int32 `json:"specGPUs,omitempty"`              // The total number of GPUs c onfigured/present on this node.
	TotalIdleGPUs              int32 `json:"idleGPUs,omitempty"`              // The number of GPUs that are uncommitted and therefore available on this node. This quantity is equal to specGPUs - committedGPUs.
	TotalCommittedGPUs         int32 `json:"committedGPUs,omitempty"`         // The number of GPUs that are actively committed and allocated to replicas that are scheduled onto this node.
	TotalPendingGPUs           int32 `json:"pendingGPUs,omitempty"`           // The sum of the outstanding GPUs of all replicas scheduled onto this node. Pending GPUs are not allocated or committed to a particular replica yet. The time at which resources are actually committed to a replica depends upon the policy being used. In some cases, they're committed immediately. In other cases, they're committed only when the replica is actively training.
	TotalNumPendingAllocations int32 `json:"numPendingAllocations,omitempty"` // Number of individual allocations consisting of GPUs that have NOT been fully committed to a kernel.
	TotalNumAllocations        int32 `json:"numAllocations,omitempty"`        // Number of individual allocations such that the GPUs have been committed to a container.
}

type clusterSchedulerImpl struct {
	gateway domain.ClusterGateway

	kubeSchedulerServicePort int               // Port that the Cluster Gateway's HTTP server will listen on. This server is used to receive scheduling decision requests from the Kubernetes Scheduler Extender.
	kubeClient               domain.KubeClient // Kubernetes client.
	gpuInfo                  *aggregateGpuInfo // The current "actual" GPU usage within the cluster.
	lastGpuInfoRefresh       time.Time         // The time at which the 'actual' GPU info was last refreshed.
	nodes                    []corev1.Node     // Cached list of nodes within the Kubernetes cluster. This is NOT necessarily up-to-date; it is refreshed on an interval.
	lastNodeRefreshTime      time.Time         // The time at which the Kubernetes nodes were last refreshed.

	//-//-//-//-//-//-//-//-//-//
	//  Scaling Configuration  //
	//-//-//-//-//-//-//-//-//-//
	gpusPerHost                 float64 // The number of actual GPUs that are available for use on each node/host.
	virtualGpusPerHost          int32   // The number of virtual GPUs per host.
	scalingFactor               float64 // scalingFactor defines how many hosts the cluster will provision based on busy resources.
	maximumHostsToReleaseAtOnce int32   // `maximumHostsToReleaseAtOnce` defines how many hosts the cluster can deprovision during a single scale-in event. This is equivalent to Jingyuan's "scaling-in limit" parameter.
	scalingInterval             int32   // How often to call ValidateCapacity()
	scalingLimit                float64 // scalingLimit defines how many hosts the cluster will provision at maximum based on busy resources.
	canScalingIn                bool    // Can the Cluster/Placer scale-in?
	shouldUpdateRatio           bool    // Should the Placer update its subscription ratio?
	scalingOutEnaled            bool    // If enabled, the scaling manager will attempt to over-provision hosts slightly so as to leave room for fluctation. If disabled, then the Cluster will exclusivel scale-out in response to real-time demand, rather than attempt to have some hosts available in the case that demand surges.
	scalingBufferSize           int32   // How many extra hosts we provision so that we can quickly scale if needed.
	minimumCapacity             int32   // The minimum number of kubernetes nodes we must have available at any time.

	log logger.Logger
}

func NewClusterScheduler(gateway domain.ClusterGateway, kubeClient domain.KubeClient, opts *domain.ClusterSchedulerOptions) domain.ClusterScheduler {
	clusterScheduler := &clusterSchedulerImpl{
		gateway:                     gateway,
		kubeClient:                  kubeClient,
		kubeSchedulerServicePort:    opts.SchedulerHttpPort,
		gpusPerHost:                 float64(opts.GpusPerHost),
		virtualGpusPerHost:          opts.VirtualGpusPerHost,
		scalingFactor:               opts.ScalingFactor,
		scalingLimit:                opts.ScalingLimit,
		maximumHostsToReleaseAtOnce: opts.MaximumHostsToReleaseAtOnce,
		scalingInterval:             opts.ScalingInterval,
		scalingOutEnaled:            opts.ScalingOutEnaled,
		scalingBufferSize:           opts.ScalingBufferSize,
		minimumCapacity:             opts.MinimumNumNodes,
	}
	config.InitLogger(&clusterScheduler.log, clusterScheduler)

	if clusterScheduler.log.GetLevel() == logger.LOG_LEVEL_ALL {
		clusterScheduler.log.Debug("Scheduling Configuration:")
		clusterScheduler.log.Debug("GpusPerHost: %d", clusterScheduler.gpusPerHost)
		clusterScheduler.log.Debug("VirtualGpusPerHost: %d", clusterScheduler.virtualGpusPerHost)
		clusterScheduler.log.Debug("ScalingFactor: %d", clusterScheduler.scalingFactor)
		clusterScheduler.log.Debug("ScalingLimit: %d", clusterScheduler.scalingLimit)
		clusterScheduler.log.Debug("MaximumHostsToReleaseAtOnce: %d", clusterScheduler.maximumHostsToReleaseAtOnce)
		clusterScheduler.log.Debug("ScalingInterval: %d", clusterScheduler.scalingInterval)
		clusterScheduler.log.Debug("ScalingOutEnaled: %d", clusterScheduler.scalingOutEnaled)
		clusterScheduler.log.Debug("ScalingBufferSize: %d", clusterScheduler.scalingBufferSize)
	}

	err := clusterScheduler.RefreshKubernetesNodes()
	if err != nil {
		clusterScheduler.log.Error("Initial retrieval of Kubernetes nodes failed: %v", err)
	}

	return clusterScheduler
}

// Return the associated ClusterGateway.
func (s *clusterSchedulerImpl) ClusterGateway() domain.ClusterGateway {
	return s.gateway
}

// Return the minimum number of nodes we must have available at any time.
func (s *clusterSchedulerImpl) MinimumCapacity() int32 {
	return s.minimumCapacity
}

// This should be called from its own goroutine.
// Start the HTTP HTTP service used to make scheduling decisions.
func (s *clusterSchedulerImpl) StartHttpKubernetesSchedulerService() {
	s.log.Debug("Starting the Cluster Scheduler's HTTP Kubernetes Scheduler service.")

	app := gin.New()

	app.Use(gin.Logger())
	app.Use(cors.Default())

	app.POST(domain.FilterRoute, s.HandleKubeSchedulerFilterRequest)

	s.log.Debug("Cluster Scheduler's HTTP Kubernetes Scheduler service is listening on port %d", s.kubeSchedulerServicePort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", s.kubeSchedulerServicePort), app); err != nil {
		s.log.Error("Cluster Scheduler's HTTP Kubernetes Scheduler service failed because: %v", err)
		panic(err)
	}
}

// Add a new node to the kubernetes cluster.
// We simulate this using node taints.
func (s *clusterSchedulerImpl) AddNode() error {
	return ErrNotImplementedYet
}

// Remove a new from the kubernetes cluster.
// We simulate this using node taints.
func (s *clusterSchedulerImpl) RemoveNode() error {
	return ErrNotImplementedYet
}

// Return the current number of nodes in the kubernetes cluster.
// This includes both nodes that are already running and nodes that are being provisioned.
// TODO(Ben): Implement "node provisioning" (i.e., simulating the time it takes to spin-up a new node).
func (s *clusterSchedulerImpl) currentSize() int32 {
	return int32(len(s.nodes))
}

// The number of active/already-running/already-provisioned hosts within the Cluster.
func (s *clusterSchedulerImpl) activeSize() int32 {
	return int32(len(s.nodes))
}

// The number of hosts currently being provisioned within the Cluster.
func (s *clusterSchedulerImpl) pendingSize() int32 {
	panic("Not implemented yet.")
}

// Refresh all metrics maintained/cached/required by the Cluster Scheduler,
// including the list of current kubernetes nodes, actual and virtual GPU usage information, etc.
//
// Return a slice of any errors that occurred. If an error occurs while refreshing a particular piece of information,
// then the error is recorded, and the refresh proceeds, attempting all refreshes (even if an error occurs during one refresh).
func (s *clusterSchedulerImpl) RefreshAll() []error {
	errors := make([]error, 0)

	err := s.RefreshKubernetesNodes()
	if err != nil {
		errors = append(errors, err)
	}

	err = s.RefreshActualGpuInfo()
	if err != nil {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		s.log.Error("%d error(s) occurred while refreshing all metrics.", len(errors))
	}

	return errors
}

// Refresh the actual GPU usage information.
// Returns nil on success; returns an error on failure.
func (s *clusterSchedulerImpl) RefreshActualGpuInfo() error {
	clusterGpuInfo, err := s.gateway.GetClusterActualGpuInfo(context.TODO(), &gateway.Void{})
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

// Update the cached list of Kubernetes nodes.
// Returns nil on success; returns an error on failure.
func (s *clusterSchedulerImpl) RefreshKubernetesNodes() error {
	nodes, err := s.kubeClient.GetKubernetesNodes()
	if err != nil {
		s.log.Error("Failed to refresh Kubernetes nodes.") // The error is printed by the KubeClient. We don't need to print it again here.
	} else {
		s.nodes = nodes
		s.lastNodeRefreshTime = time.Now()
	}

	return err // Will be nil if no error occurred.
}

// Handle a 'filter' request from the kubernetes scheduler.
func (s *clusterSchedulerImpl) HandleKubeSchedulerFilterRequest(ctx *gin.Context) {
	var (
		extenderArgs         scheduler.ExtenderArgs
		extenderFilterResult *scheduler.ExtenderFilterResult
		err                  error
	)

	err = ctx.BindJSON(&extenderArgs)
	if err != nil {
		s.log.Error("Received FILTER request; however, failed to extract ExtenderArgs because: %v", err)
		ctx.Error(err)
		extenderFilterResult = &scheduler.ExtenderFilterResult{
			Nodes:       nil,
			FailedNodes: nil,
			Error:       err.Error(),
		}
	}

	s.log.Debug("Received FILTER request for Pod \"%s\" with %d node(s).", extenderArgs.Pod.Name, len(extenderArgs.Nodes.Items))

	extenderFilterResult = &scheduler.ExtenderFilterResult{
		Nodes: extenderArgs.Nodes,
	}

	s.log.Debug("Returning %d node(s) without any processing.", len(extenderArgs.Nodes.Items))
	ctx.JSON(http.StatusOK, extenderFilterResult)
}

// Validate the Cluster's capacity according to the scaling policy implemented by the particular ScaleManager.
// Adjust the Cluster's capacity as directed by scaling policy.
func (s *clusterSchedulerImpl) ValidateCapacity() {
	load := s.gpuInfo.TotalCommittedGPUs

	minNumHosts := int32(math.Ceil(float64(load) / s.gpusPerHost))                         // The minimum number of hosts required to satisfy the cluster's current committed GPUs.
	scaledOutNumHosts := int32(math.Ceil(float64(load) * s.scalingFactor / s.gpusPerHost)) // The number of hosts we would scale-out to based on the configured scaling factor.
	limit := int32(math.Ceil(float64(load) * s.scalingLimit / s.gpusPerHost))              // The maximum number of hosts we're permitted to scale-out to.

	// Make some room for fluctuation.
	//
	// TODO(Ben): Is the minimum capacity of the host pool the right value to use here?
	// Jingyuan's code uses the "min buffer size" (which is set within the `StaticPlacerBufferSize` constant in his code),
	// so the minimum capacity of the host pool is the analogous value to use in my code. I'm just not sure if it will
	// result in the intended behavior as I set the minimum capacity of the host pool moreso from an economic standpoint
	// to take advantage of reserved pricing.
	if scaledOutNumHosts < (minNumHosts + s.scalingBufferSize) {
		scaledOutNumHosts = (minNumHosts + s.scalingBufferSize)
		// s.log.Debug("Adjusted scaledOutNumHosts: %s.", scaledOutNumHosts)
	}
	if limit < minNumHosts+4 {
		limit = minNumHosts + 4
		// s.log.Debug("Adjusted limit: %s.", limit)
	}

	if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
		s.log.Debug("Load (CommittedGPUs): %s. Current #Hosts: %s. Minimum #Hosts to Satisfy Load: %s. Target #Hosts: %s. Max Scaled-Out #Hosts: %s.", load, s.currentSize(), minNumHosts, scaledOutNumHosts, limit)
	}
	old_num_hosts := int32(s.currentSize())
	// Only scale-out if that feature is enabled.
	if s.scalingOutEnaled && old_num_hosts < scaledOutNumHosts {
		// Scaling out
		var numProvisioned int = 0

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Scaling-out by %d hosts (from %d to %d).", scaledOutNumHosts-old_num_hosts, old_num_hosts, scaledOutNumHosts)
		}

		// This is such a minor optimization, but we cache the size of the active host pool locally so that we don't have to grab it everytime.
		// The size of the pending host pool will grow each time we provision a new host.
		var activeSize int32 = s.activeSize()
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
			s.log.Debug("Provisioned %d new hosts based on #CommittedGPUs(%d). Previous #hosts: %s. New #hosts: %s.", numProvisioned, load, old_num_hosts, s.currentSize())
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

	num_to_release := s.currentSize() - limit
	if num_to_release > 0 {
		if num_to_release > s.maximumHostsToReleaseAtOnce {
			if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
				s.log.Debug("Decreased the number of idle hosts to release from %d to the maximum allowed value of %s.", num_to_release, s.maximumHostsToReleaseAtOnce)
			}
			num_to_release = s.maximumHostsToReleaseAtOnce
		}

		if s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Scaling in %d hosts", num_to_release)
		}

		num_released, err := s.ReleaseIdleHosts(num_to_release)
		if err != nil {
			s.log.Error("Error while releasing idle hosts: %v", err)
		}

		if num_released > 0 && s.log.GetLevel() == logger.LOG_LEVEL_ALL {
			s.log.Debug("Released %d idle hosts based on #CommittedGPUs (%d). Prev #hosts: %s. New #hosts: %s.", num_released, load, old_num_hosts, s.currentSize())
		}
	}
}

// Try to release n idle hosts. Return the number of hosts that were actually released.
// Error will be nil on success and non-nil if some sort of failure is encountered.
func (s *clusterSchedulerImpl) ReleaseIdleHosts(n int32) (int, error) {
	return 0, ErrNotImplementedYet
}
