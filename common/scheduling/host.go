package scheduling

import (
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ConsecutiveFailuresWarning int = 1
	ConsecutiveFailuresBad     int = 2
)

var (
	ErrRestorationFailed = errors.New("restoration failed for unknown reason")

	ErrRestoreRequired     = errors.New("restore required")
	ErrNodeNameUnspecified = errors.New("no kubernetes node name returned for LocalDaemonClient")
)

// ErrorCallback defines a function to be called if a Host appears to be dead.
type ErrorCallback func(localDaemonId string, nodeName string, errorName string, errorMessage string) error

// ResourceSpec defines the resources available on a particular Host.
type ResourceSpec struct {
	CPUs     float64 `json:"cpus"`
	MemoryGB float64 `json:"memory_gb"`
	GPUs     float64 `json:"gpus"`
}

type PreemptionInfo interface {
	fmt.Stringer

	Penalty() float64
	Candidates() ContainerList
}

// Used by Host's to query
type OversubscriptionQuerierFunction func(ratio decimal.Decimal) decimal.Decimal

type HostMetaKey string

type HostStatistics interface {
	// Priority returns the host's "priority", which is the benefit gained or lost in terms of GPU time per migration.
	Priority(session *Session) float64

	// ScaleInPriority returns the host's "scheduling-in priority", or SIP, which is defined as a * the interactive
	// priority of a given task + b * the sum of the preemption priorities of the preemptible tasks
	ScaleInPriority() float64

	// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
	IdleGPUs() float64

	// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	// Pending GPUs are NOT actively bound to any
	PendingGPUs() float64

	// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
	CommittedGPUs() float64

	// IdleGPUsStat returns the StatFloat64 representing the number of GPUs that the host has not allocated to any Containers.
	//IdleGPUsStat() types.StatFloat64Field

	// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	//PendingGPUsStat() types.StatFloat64Field

	// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound to Containers scheduled on the Host.
	//CommittedGPUsStat() types.StatFloat64Field

	// IdleCPUs returns the number of CPUs that the host has not allocated to any Containers.
	IdleCPUs() float64

	// PendingCPUs returns the number of CPUs that are oversubscribed by Containers scheduled on the Host.
	// Pending CPUs are NOT actively bound to any
	PendingCPUs() float64

	// CommittedCPUs returns the number of CPUs that are actively bound to Containers scheduled on the Host.
	CommittedCPUs() float64

	// IdleCPUsStat returns the StatFloat64 representing the number of CPUs that the host has not allocated to any Containers.
	//IdleCPUsStat() types.StatFloat64Field

	// PendingCPUsStat returns the StatFloat64 representing the number of CPUs that are oversubscribed by Containers scheduled on the Host.
	//PendingCPUsStat() types.StatFloat64Field

	// CommittedCPUsStat returns the StatFloat64 representing the number of CPUs that are actively bound to Containers scheduled on the Host.
	//CommittedCPUsStat() types.StatFloat64Field

	// IdleMemoryMb returns the amount of memory, in megabytes (MB), that the host has not allocated to any Containers.
	IdleMemoryMb() float64

	// PendingMemoryMb returns the amount of memory, in megabytes (MB), that is oversubscribed by Containers scheduled on the Host.
	// Pending MemoryMb are NOT actively bound to any
	PendingMemoryMb() float64

	// CommittedMemoryMb returns the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	CommittedMemoryMb() float64

	// IdleMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that the host has not allocated to any Containers.
	//IdleMemoryMbStat() types.StatFloat64Field

	// PendingMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that is oversubscribed by Containers scheduled on the Host.
	//PendingMemoryMbStat() types.StatFloat64Field

	// CommittedMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	//CommittedMemoryMbStat() types.StatFloat64Field

	// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	LastReschedule() types.StatFloat64Field
}

type HostMeta interface {
	Value(key interface{}) interface{}
}

type cachedPenalty struct {
	penalty     float64
	explain     string
	preemptions ContainerList
	valid       bool
}

func (p *cachedPenalty) Penalty() float64 {
	return p.penalty
}

func (p *cachedPenalty) String() string {
	return p.explain
}

func (p *cachedPenalty) Candidates() ContainerList {
	return p.preemptions[:]
}

type Host struct {
	proto.LocalGatewayClient

	log logger.Logger

	latestGpuInfo          *proto.GpuInfo                       // latestGpuInfo is the latest GPU info of this host scheduler.
	resourceMutex          sync.Mutex                           // resourceMutex controls access to the latest GPU info.
	syncMutex              sync.Mutex                           // syncMutex ensures atomicity of the Host's SynchronizeResourceInformation method.
	schedulingMutex        sync.Mutex                           // schedulingMutex ensures that only a single kernel is scheduled at a time, to prevent over-allocating resources on the Host.
	meta                   hashmap.HashMap[string, interface{}] // meta is a map of metadata.
	conn                   *grpc.ClientConn                     // conn is the gRPC connection to the Host.
	Addr                   string                               // Addr is the Host's address.
	NodeName               string                               // NodeName is the Host's name (for printing/logging).
	Cluster                Cluster                              // Cluster is a reference to the Cluster interface that manages this Host.
	ID                     string                               // ID is the unique ID of this host.
	containers             hashmap.HashMap[string, *Container]  // containers is a map of all the kernel replicas scheduled onto this host.
	trainingContainers     []*Container                         // trainingContainers are the actively-training kernel replicas.
	seenSessions           []string                             // seenSessions are the sessions that have been scheduled onto this host at least once.
	resourceSpec           types.ValidatableResourceSpec        // resourceSpec is the spec describing the total resources available on the Host, not impacted by allocations.
	lastReschedule         types.StatFloat64                    // lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	errorCallback          ErrorCallback                        // errorCallback is a function to be called if a Host appears to be dead.
	pendingContainers      types.StatInt32                      // pendingContainers is the number of Containers that are scheduled on the host.
	enabled                bool                                 // enabled indicates whether the Host is currently enabled and able to serve kernels.
	CreatedAt              time.Time                            // CreatedAt is the time at which the Host was created.
	resourcesWrapper       *resourcesWrapper                    // resourcesWrapper wraps all the Host's resources.
	LastRemoteSync         time.Time                            // lastRemoteSync is the time at which the Host last synchronized its resource counts with the actual remote node that the Host represents.
	IsContainedWithinIndex bool                                 // IsContainedWithinIndex indicates whether this Host is currently contained within a valid ClusterIndex.

	// OversubscriptionQuerierFunction is used to query the oversubscription factor given the host's
	// subscription ratio and the cluster's subscription ratio.
	OversubscriptionQuerierFunction OversubscriptionQuerierFunction

	// Cached penalties
	sip             cache.InlineCache // Scale-in penalty.
	sipSession      *Session          // Scale-in penalty session.
	subscribedRatio types.StatFloat64
	penaltyList     cache.InlineCache
	penalties       []cachedPenalty
	penaltyValidity bool
	idx             int
}

// NewHost creates and returns a new *Host.
func NewHost(id string, addr string, millicpus int32, memMb int32, cluster Cluster, conn *grpc.ClientConn, errorCallback ErrorCallback) (*Host, error) {
	// Create gRPC client.
	localGatewayClient := proto.NewLocalGatewayClient(conn)

	// Set the ID. If this fails, the creation of a new host scheduler fails.
	confirmedId, err := localGatewayClient.SetID(context.Background(), &proto.HostId{Id: id})

	// Validate the response if there's no explicit error.
	if err == nil {
		if confirmedId.NodeName == "" {
			err = ErrNodeNameUnspecified
		} else if confirmedId.Id != id {
			err = ErrRestoreRequired
		}
	}

	// If error is now non-nil, either because there was an explicit error or because the response was invalid,
	// then the host scheduler creation failed, and we return nil and the error.
	if err != nil {
		return nil, err
	}

	// Get the initial GPU info. If this fails, the creation of a new host scheduler fails.
	gpuInfoResp, err := localGatewayClient.GetActualGpuInfo(context.Background(), &proto.Void{})
	if err != nil {
		return nil, err
	}

	// Create the ResourceSpec defining the resources available on the Host.
	resourceSpec := &types.Float64Spec{
		GPUs:     types.GPUSpec(gpuInfoResp.SpecGPUs),
		CPUs:     float64(millicpus),
		MemoryMb: float64(memMb),
	}

	host := &Host{
		LocalGatewayClient:              localGatewayClient,
		latestGpuInfo:                   gpuInfoResp,
		ID:                              id,
		NodeName:                        confirmedId.NodeName,
		Addr:                            addr,
		resourceSpec:                    resourceSpec,
		Cluster:                         cluster,
		conn:                            conn,
		log:                             config.GetLogger(fmt.Sprintf("Host %s ", id)),
		containers:                      hashmap.NewCornelkMap[string, *Container](5),
		trainingContainers:              make([]*Container, 0, int(resourceSpec.GPU())),
		penalties:                       make([]cachedPenalty, int(resourceSpec.GPU())),
		seenSessions:                    make([]string, int(resourceSpec.GPU())),
		meta:                            hashmap.NewCornelkMap[string, interface{}](64),
		errorCallback:                   errorCallback,
		enabled:                         true,
		CreatedAt:                       time.Now(),
		OversubscriptionQuerierFunction: cluster.GetOversubscriptionFactor,
	}

	host.resourcesWrapper = &resourcesWrapper{
		idleResources: &resources{
			resourceStatus: IdleResources,
			millicpus:      decimal.NewFromFloat(resourceSpec.CPU()),
			memoryMB:       decimal.NewFromFloat(resourceSpec.MemoryMB()),
			gpus:           decimal.NewFromFloat(resourceSpec.GPU()),
		},
		pendingResources: &resources{
			resourceStatus: PendingResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
		},
		committedResources: &resources{
			resourceStatus: CommittedResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
		},
		specResources: &resources{
			resourceStatus: SpecResources,
			millicpus:      decimal.NewFromFloat(resourceSpec.CPUs),
			memoryMB:       decimal.NewFromFloat(resourceSpec.MemoryMb),
			gpus:           decimal.NewFromFloat(float64(gpuInfoResp.SpecGPUs)),
		},
	}

	//host.idleGPUs.Store(float64(gpuInfoResp.SpecGPUs))
	//host.pendingGPUs.Store(0)
	//host.committedGPUs.Store(0)
	//
	//host.idleCPUs.Store(resourceSpec.CPUs)
	//host.pendingCPUs.Store(0)
	//host.committedCPUs.Store(0)
	//
	//host.idleMemoryMb.Store(resourceSpec.MemoryMb)
	//host.pendingMemoryMb.Store(0)
	//host.committedMemoryMb.Store(0)

	host.sip.Producer = cache.FormalizeICProducer(host.getSIP)
	host.sip.Validator = GetClockTimeCacheValidator()
	host.penaltyList.Producer = cache.FormalizeChainedICProducer(host.updatePenaltyList)
	host.penaltyList.Validator = host.validatePenaltyList

	host.subscribedRatio.Store(0)

	return host, nil
}

func (h *Host) LockScheduling() {
	h.schedulingMutex.Lock()
}

func (h *Host) TryLockScheduling() bool {
	return h.schedulingMutex.TryLock()
}

func (h *Host) UnlockScheduling() {
	h.schedulingMutex.Unlock()
}

func (h *Host) SubscribedRatio() float64 {
	return h.subscribedRatio.Load()
}

// ToVirtualDockerNode converts a Host struct to a proto.VirtualDockerNode struct and
// returns a pointer to the new proto.VirtualDockerNode.
func (h *Host) ToVirtualDockerNode() *proto.VirtualDockerNode {
	dockerContainers := make([]*proto.DockerContainer, 0, h.containers.Len())
	h.containers.Range(func(_ string, container *Container) (contd bool) {
		dockerContainers = append(dockerContainers, container.ToDockerContainer())
		return true
	})

	return &proto.VirtualDockerNode{
		NodeId:          h.ID,
		NodeName:        h.NodeName,
		Address:         h.Addr,
		CreatedAt:       timestamppb.New(h.CreatedAt),
		Containers:      dockerContainers,
		SpecCpu:         float32(h.resourceSpec.CPU()),
		SpecMemory:      float32(h.resourceSpec.MemoryMB()),
		SpecGpu:         float32(h.resourceSpec.GPU()),
		AllocatedCpu:    float32(h.resourcesWrapper.committedResources.millicpus.InexactFloat64()),
		AllocatedMemory: float32(h.resourcesWrapper.committedResources.memoryMB.InexactFloat64()),
		AllocatedGpu:    float32(h.resourcesWrapper.committedResources.gpus.InexactFloat64()),
		PendingCpu:      float32(h.resourcesWrapper.pendingResources.millicpus.InexactFloat64()),
		PendingMemory:   float32(h.resourcesWrapper.pendingResources.memoryMB.InexactFloat64()),
		PendingGpu:      float32(h.resourcesWrapper.pendingResources.gpus.InexactFloat64()),
	}
}

// NumContainers returns the number of Container instances scheduled on the Host.
func (h *Host) NumContainers() int {
	return h.containers.Len()
}

// SynchronizeResourceInformation queries the remote host via gRPC to request update-to-date resource usage information.
//
// This method is thread-safe. Only one goroutine at a time may execute this method.
func (h *Host) SynchronizeResourceInformation() error {
	h.syncMutex.Lock()
	defer h.syncMutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	//h.log.Debug("Refreshing resource information from remote.")
	resp, err := h.LocalGatewayClient.GetActualGpuInfo(ctx, &proto.Void{})
	if err != nil {
		h.log.Error("Failed to refresh resource information from remote: %v", err)
		return err
	}

	h.updateLocalGpuInfoFromRemote(resp)
	h.LastRemoteSync = time.Now()
	return nil
}

// PlacedMemoryMB returns the total amount of memory scheduled onto the Host, which is computed as the
// sum of the Host's pending memory and the Host's committed memory, in megabytes.
func (h *Host) PlacedMemoryMB() decimal.Decimal {
	return h.resourcesWrapper.pendingResources.memoryMB.Add(h.resourcesWrapper.committedResources.memoryMB)
}

// PlacedGPUs returns the total number of GPUs scheduled onto the Host, which is computed as the
// sum of the Host's pending GPUs and the Host's committed GPUs.
func (h *Host) PlacedGPUs() decimal.Decimal {
	return h.resourcesWrapper.pendingResources.gpus.Add(h.resourcesWrapper.committedResources.gpus)
}

// PlacedCPUs returns the total number of CPUs scheduled onto the Host, which is computed as the
// sum of the Host's pending CPUs and the Host's committed CPUs.
func (h *Host) PlacedCPUs() decimal.Decimal {
	return h.resourcesWrapper.pendingResources.millicpus.Add(h.resourcesWrapper.committedResources.millicpus)
}

// computeHypotheticalSubscriptionRatio computes what the Host's (over)subscription ratios would be for CPU, Memory,
// and GPU, if it were to serve a Container with the given types.Spec resource request/requirements.
func (h *Host) computeHypotheticalSubscriptionRatio(resourceRequest types.Spec) (decimal.Decimal, decimal.Decimal, decimal.Decimal) {
	divisor := h.Cluster.NumReplicasAsDecimal()

	// Convert the given types.Spec to a *types.DecimalSpec.
	var decimalSpec *types.DecimalSpec
	if specAsDecimalSpec, ok := resourceRequest.(*types.DecimalSpec); ok {
		// If the parameter is already a *types.DecimalSpec, then no actual conversion needs to be performed.
		decimalSpec = specAsDecimalSpec
	} else {
		decimalSpec = types.ToDecimalSpec(resourceRequest)
	}

	var cpuRatio, memRatio, gpuRatio decimal.Decimal

	if h.resourcesWrapper.specResources.millicpus.Equals(decimal.Zero) {
		cpuRatio = decimal.Zero
	} else {
		totalCPUs := h.PlacedCPUs().Add(decimalSpec.Millicpus)
		cpuRatio = totalCPUs.Div(h.resourcesWrapper.specResources.millicpus).Div(divisor)
	}

	if h.resourcesWrapper.specResources.memoryMB.Equals(decimal.Zero) {
		memRatio = decimal.Zero
	} else {
		totalMemory := h.PlacedMemoryMB().Add(decimalSpec.MemoryMb)
		memRatio = totalMemory.Div(h.resourcesWrapper.specResources.memoryMB).Div(divisor)
	}

	if h.resourcesWrapper.specResources.gpus.Equals(decimal.Zero) {
		gpuRatio = decimal.Zero
	} else {
		totalGPUs := h.PlacedGPUs().Add(decimalSpec.GPUs)
		gpuRatio = totalGPUs.Div(h.resourcesWrapper.specResources.gpus).Div(divisor)
	}

	return cpuRatio, memRatio, gpuRatio
}

// WillBecomeTooOversubscribed returns a boolean indicating whether the Host will become "too" oversubscribed if it
// were to serve a kernel replica with the given resource requirements / request.
//
// "Too" oversubscribed means that the Host's over-subscription ratio would exceed the configured limit upon
// serving the Container with the given types.Spec resource request/requirements.
func (h *Host) WillBecomeTooOversubscribed(resourceRequest types.Spec) bool {
	cpuRatio, memRatio, gpuRatio := h.computeHypotheticalSubscriptionRatio(resourceRequest)

	willOversubscribeCpu := h.OversubscriptionQuerierFunction(cpuRatio).GreaterThanOrEqual(decimal.Zero)
	willOversubscribeMemory := h.OversubscriptionQuerierFunction(memRatio).GreaterThanOrEqual(decimal.Zero)
	willOversubscribeGpu := h.OversubscriptionQuerierFunction(gpuRatio).GreaterThanOrEqual(decimal.Zero)

	h.log.Debug("Computed over-subscription ratios for resource request: %v.\n"+
		"CPU Ratio: %s (Will Oversubscribe? %v), Memory Ratio: %s (Will Oversubscribe? %v), GPU Ratio: %s (Will Oversubscribe? %v)",
		resourceRequest.String(), cpuRatio.StringFixed(4), willOversubscribeCpu, memRatio.StringFixed(4),
		willOversubscribeMemory, gpuRatio.StringFixed(4), willOversubscribeGpu)

	return willOversubscribeCpu || willOversubscribeMemory || willOversubscribeGpu
}

// CanServeContainer returns a boolean indicating whether this Host could serve a kernel replica with the given
// resource requirements / resource request. This method only checks against the Host's "spec" (i.e., the total
// resources available on the Host, not taking into account current resource allocations).
//
// CanServeContainer returns true when the Host could serve the hypothetical kernel and false when the Host could not.
func (h *Host) CanServeContainer(resourceRequest types.Spec) bool {
	return h.resourcesWrapper.specResources.Validate(resourceRequest)
}

// CanCommitResources returns a boolean indicating whether this Host could commit the specified resource request
// to a kernel scheduled onto the Host right now. Commiting resource requires having sufficiently many idle resources
// available.
//
// CanCommitResources returns true if the Host could commit/reserve the given resources right now.
// Otherwise, CanCommitResources returns false.
func (h *Host) CanCommitResources(resourceRequest types.Spec) bool {
	return h.resourcesWrapper.idleResources.Validate(types.ToDecimalSpec(resourceRequest))
}

// updateLocalGpuInfoFromRemote updates the local info pertaining to GPU usage information
// with the "actual" GPU usage retrieved from the remote host associated with this Host struct.
func (h *Host) updateLocalGpuInfoFromRemote(remoteInfo *proto.GpuInfo) {
	h.resourceMutex.Lock()

	//h.log.Debug("Updating local GPU usage info with data from remote now...")
	numDifferences := 0

	localIdleGpus := h.resourcesWrapper.idleResources.GPUs()
	if localIdleGpus != float64(remoteInfo.IdleGPUs) {
		h.log.Warn("Local idle GPUs (%.0f) do not match latest remote update (%d). Updating local info now...", localIdleGpus, remoteInfo.IdleGPUs)
		h.resourcesWrapper.idleResources.gpus = decimal.NewFromFloat(float64(remoteInfo.IdleGPUs))
		numDifferences += 1
	}

	localPendingGPUs := h.resourcesWrapper.pendingResources.GPUs()
	if localPendingGPUs != float64(remoteInfo.PendingGPUs) {
		h.log.Warn("Local pending GPUs (%.0f) do not match latest remote update (%d). Updating local info now...", localPendingGPUs, remoteInfo.PendingGPUs)
		h.resourcesWrapper.pendingResources.gpus = decimal.NewFromFloat(float64(remoteInfo.PendingGPUs))
		numDifferences += 1
	}

	localCommittedGPUs := h.resourcesWrapper.committedResources.GPUs()
	if localCommittedGPUs != float64(remoteInfo.CommittedGPUs) {
		h.log.Warn("Local committed GPUs (%.0f) do not match latest remote update (%d). Updating local info now...", localCommittedGPUs, remoteInfo.CommittedGPUs)
		h.resourcesWrapper.committedResources.gpus = decimal.NewFromFloat(float64(remoteInfo.CommittedGPUs))
		numDifferences += 1
	}

	if h.ResourceSpec().GPU() != float64(remoteInfo.SpecGPUs) {
		h.log.Warn("Local spec GPUs (%.0f) do not match latest remote update (%d). Updating local info now...", h.ResourceSpec().GPU(), remoteInfo.SpecGPUs)
		h.ResourceSpec().UpdateSpecGPUs(float64(remoteInfo.SpecGPUs))
		numDifferences += 1
	}

	if numDifferences > 0 {
		h.log.Warn("Finished remote-to-local GPU update. Number of differences: %d.", numDifferences)
	}

	h.latestGpuInfo = remoteInfo
	h.resourceMutex.Unlock()
}

// ContainerScheduled is to be called when a Container is scheduled onto the Host.
func (h *Host) ContainerScheduled(container *Container) error {
	h.containers.Store(container.ContainerID(), container)

	h.pendingContainers.Add(1)

	if err := h.resourcesWrapper.pendingResources.Add(types.ToDecimalSpec(container.outstandingResources)); err != nil {
		h.log.Error("Could not schedule Container %s onto Host %s due to resource-related issue: %v",
			container.ContainerID(), h.ID, err)
		return err
	}

	//h.pendingCPUs.Add(container.OutstandingResources().CPU())
	//h.pendingMemoryMb.Add(container.OutstandingResources().MemoryMB())
	//h.pendingGPUs.Add(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was scheduled onto Host %s.", container.String(), h.ID)
	return nil
}

// Restore restores the state of a Host from another Host.
// TODO: Implement this more.
func (h *Host) Restore(restored *Host, callback ErrorCallback) error {
	h.SetErrorCallback(callback)
	h.resourceSpec = restored.resourceSpec
	h.ID = restored.ID
	h.NodeName = restored.NodeName
	h.LocalGatewayClient = restored.LocalGatewayClient
	h.latestGpuInfo = restored.latestGpuInfo

	return nil
}

// Enabled returns a boolean indicating whether the Host is enabled (true) or disabled (false).
func (h *Host) Enabled() bool {
	return h.enabled
}

// Enable enables the Host.
//
// If the Host is already enabled, then this returns an error.
func (h *Host) Enable() error {
	if h.enabled {
		return fmt.Errorf("%w: host \"%s\" is already enabled", ErrInvalidHost, h.ID)
	}

	h.enabled = true
	return nil
}

// Disable disables the Host.
//
// If the Host is already disabled, then this returns an error.
func (h *Host) Disable() error {
	if !h.enabled {
		return fmt.Errorf("%w: host \"%s\" is already disabled", ErrInvalidHost, h.ID)
	}

	h.enabled = false
	return nil
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container *Container) error {
	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot remove specified Container from Host. Container is not on specified Host.")
		return ErrInvalidContainer
	}

	h.containers.Delete(container.ContainerID())

	h.pendingContainers.Sub(1)

	if err := h.resourcesWrapper.pendingResources.Subtract(types.ToDecimalSpec(container.outstandingResources)); err != nil {
		h.log.Error("Could not remove Container %s from Host %s due to resource-related issue: %v",
			container.ContainerID(), h.ID, err)
		return err
	}

	//h.pendingCPUs.Sub(container.OutstandingResources().CPU())
	//h.pendingMemoryMb.Sub(container.OutstandingResources().MemoryMB())
	//h.pendingGPUs.Sub(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was removed from Host %s.", container.String(), h.ID)

	return nil
}

// ErrorCallback returns the Host's ErrorCallback field.
func (h *Host) ErrorCallback() ErrorCallback {
	return h.errorCallback
}

// SetErrorCallback sets the Host's ErrorCallback field.
func (h *Host) SetErrorCallback(callback ErrorCallback) {
	h.errorCallback = callback
}

func (h *Host) getPenalty(cached *cachedPenalty, gpus int) (*cachedPenalty, error) {
	if cached.valid {
		return cached, nil
	}

	list := h.penaltyList.Value().(*PenaltyContainers)
	penalty, preempted, err := list.Penalty(float64(gpus))
	// Cache valid result only
	cached.penalty = penalty
	cached.preemptions = list.ContainerList[:preempted]
	cached.valid = err == nil
	cached.explain = fmt.Sprintf("candidates: %s", list.ContainerList[0].ContainerStatistics().Explain(ExplainPreemptionPriority))
	for i := 1; i < preempted; i++ {
		cached.explain += fmt.Sprintf(", %s", list.ContainerList[i].ContainerStatistics().Explain(ExplainPreemptionPriority))
	}

	h.log.Trace("Cached penalty for %du: %.2f", gpus, cached.penalty)
	return cached, err
}

func (h *Host) Penalty(gpus float64) (float64, PreemptionInfo, error) {
	// Find number of GPUs required to preempt trainings.
	bucket := int(math.Ceil(gpus) - h.IdleGPUs())
	if bucket <= 0 {
		return 0, nil, nil
	}

	penalty, err := h.getPenalty(&h.penalties[bucket-1], bucket)
	if err != nil {
		return 0, nil, err
	}

	return penalty.penalty, penalty, nil
}

func (h *Host) getSIP(sess *Session) float64 {
	numGPUs := sess.ResourceUtilization().NumGpusAsFloat()

	penalty, _, err := h.Penalty(numGPUs)
	if err != nil {
		h.log.Error("Unexpected err on calculating AB: %v", err)
	}
	h.sip.Validator(time.Now())

	rb := h.getRB(sess.SessionStatistics().InteractivePriority(), numGPUs)
	h.log.Debug("Cached sip for session %v: %.2f(%.2f-%.2f). IP: %.4f (%s).", sess, rb-penalty, rb, penalty,
		sess.SessionStatistics().InteractivePriority(), sess.SessionStatistics().Explain(ExplainInteractivePriority))
	return rb - penalty
}

func (h *Host) getRB(sessRB float64, required float64) float64 {
	//idleGPUs := h.idleGPUs.Load()
	idleGPUs := h.resourcesWrapper.idleResources.GPUs()
	extras := 0.0
	if idleGPUs > required {
		//extras = idleGPUs / h.pendingGPUs.Load()
		extras = idleGPUs / h.resourcesWrapper.pendingResources.GPUs()
	}
	rb := sessRB * (extras + 1) / float64(h.pendingContainers.Load())
	h.log.Debug("Calculated RB: %.4f\n", h.ID, rb)
	return rb
}

func (h *Host) validatePenaltyList(_ interface{}) bool {
	return h.penaltyValidity
}

func (h *Host) updatePenaltyList(cached *PenaltyContainers) *PenaltyContainers {
	h.penaltyValidity = true
	if cached == nil {
		cached = &PenaltyContainers{ContainerList: ContainerList(h.trainingContainers)}
	} else {
		cached.ContainerList = h.trainingContainers
	}
	sort.Sort(cached)
	return cached
}

// HasAnyReplicaOfKernel returns true if the Host currently has a Container (i.e., a kernel replica) of the
// specified kernel.
func (h *Host) HasAnyReplicaOfKernel(kernelId string) bool {
	found := false
	h.containers.Range(func(_ string, container *Container) (contd bool) {
		if container.KernelID() == kernelId {
			found = true
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return found
}

// HasSpecificReplicaOfKernel returns true if the Host currently has the Container corresponding
// to the given replica of the given kernel.
func (h *Host) HasSpecificReplicaOfKernel(kernelId string, replicaId int32) bool {
	found := false
	h.containers.Range(func(_ string, container *Container) (contd bool) {
		if container.KernelID() == kernelId && container.ReplicaID() == replicaId {
			found = true
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return found
}

// GetAnyReplicaOfKernel returns the *Container corresponding to any replica of the specified kernel if such a
// Container is currently scheduled/provisioned on this Host. If not, then nil is returned.
func (h *Host) GetAnyReplicaOfKernel(kernelId string) *Container {
	var targetContainer *Container
	h.containers.Range(func(_ string, container *Container) (contd bool) {
		if container.KernelID() == kernelId {
			targetContainer = container
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return targetContainer
}

// GetSpecificReplicaOfKernel returns the *Container corresponding to the specified replica of the specified kernel,
// if that Container is currently scheduled/provisioned on this Host. If not, then nil is returned.
func (h *Host) GetSpecificReplicaOfKernel(kernelId string, replicaId int32) *Container {
	var targetContainer *Container
	h.containers.Range(func(_ string, container *Container) (contd bool) {
		if container.KernelID() == kernelId && container.ReplicaID() == replicaId {
			targetContainer = container
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return targetContainer
}

func (h *Host) SetIdx(idx int) {
	h.idx = idx
}

func (h *Host) String() string {
	return fmt.Sprintf("Host[ID=%s,Name=%s,Addr=%s,Spec=%s]", h.ID, h.NodeName, h.Addr, h.resourceSpec.String())
}

//func (h *Host) ID() string {
//	return h.ID
//}

//func (h *Host) NodeName() string {
//	return h.NodeName
//}

//func (h *Host) Addr() string {
//	return h.Addr
//}

func (h *Host) Conn() *grpc.ClientConn {
	return h.conn
}

func (h *Host) Stats() HostStatistics {
	return h
}

// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
func (h *Host) LastReschedule() types.StatFloat64Field {
	return &h.lastReschedule
}

// TimeSinceLastSynchronizationWithRemote returns a time.Duration indicating how long it has been since its resources
// were refreshed and synchronized from the actual remote Host that this Host struct represents.
func (h *Host) TimeSinceLastSynchronizationWithRemote() time.Duration {
	return time.Since(h.LastRemoteSync)
}

// SetMeta sets the metadata of the host.
func (h *Host) SetMeta(key HostMetaKey, value interface{}) {
	h.meta.Store(string(key), value)
}

// GetMeta return the metadata of the host.
func (h *Host) GetMeta(key HostMetaKey) interface{} {
	if value, ok := h.meta.Load(string(key)); ok {
		return value
	}
	return nil
}

func (h *Host) Priority(session *Session) float64 {
	if session != h.sipSession {
		h.sip.Invalidate()
		h.sipSession = session
	}
	return h.sip.Value(session).(float64)
}

// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
func (h *Host) IdleGPUs() float64 {
	return h.resourcesWrapper.idleResources.GPUs()
}

// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
func (h *Host) PendingGPUs() float64 {
	return h.resourcesWrapper.pendingResources.GPUs()
}

// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
func (h *Host) CommittedGPUs() float64 {
	return h.resourcesWrapper.committedResources.GPUs()
}

func (h *Host) IdleCPUs() float64 {
	return h.resourcesWrapper.idleResources.Millicpus()
}

func (h *Host) PendingCPUs() float64 {
	return h.resourcesWrapper.pendingResources.Millicpus()
}

func (h *Host) CommittedCPUs() float64 {
	return h.resourcesWrapper.committedResources.Millicpus()
}

func (h *Host) IdleMemoryMb() float64 {
	return h.resourcesWrapper.idleResources.MemoryMB()
}

func (h *Host) PendingMemoryMb() float64 {
	return h.resourcesWrapper.pendingResources.MemoryMB()
}

func (h *Host) CommittedMemoryMb() float64 {
	return h.resourcesWrapper.committedResources.MemoryMB()
}

// ResourceSpec the types.Spec defining the resources available on the Host.
func (h *Host) ResourceSpec() types.ValidatableResourceSpec {
	return h.resourceSpec
}

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}

func (h *Host) AddToPendingResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.pendingResources.Add(spec)
}

func (h *Host) AddToIdleResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.idleResources.Add(spec)
}
func (h *Host) AddToCommittedResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.committedResources.Add(spec)
}

func (h *Host) SubtractFromPendingResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.pendingResources.Subtract(spec)
}

func (h *Host) SubtractFromIdleResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.idleResources.Subtract(spec)
}
func (h *Host) SubtractFromCommittedResources(spec *types.DecimalSpec) error {
	return h.resourcesWrapper.committedResources.Subtract(spec)
}
