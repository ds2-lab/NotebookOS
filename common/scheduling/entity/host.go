package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/connectivity"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/cache"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrRestorationFailed = errors.New("restoration failed for unknown reason")

	ErrRestoreRequired                  = errors.New("restore required")
	ErrNodeNameUnspecified              = errors.New("no kubernetes node name returned for LocalDaemonClient")
	ErrReservationNotFound              = errors.New("no resource reservation found for the specified kernel")
	ErrHostAlreadyIncludedForScheduling = errors.New("the specified host is already being included for consideration in scheduling operations")
)

// ResourceSpec defines the ComputeResource available on a particular Host.
type ResourceSpec struct {
	CPUs     float64 `json:"cpus"`
	MemoryGB float64 `json:"memory_gb"`
	GPUs     float64 `json:"gpus"`
}

type SubscriptionQuerier interface {
	// GetOversubscriptionFactor is a function used by Host instances to compare their subscription ratio against the
	// Cluster's subscription ratio/factor. This is used to determine if a Host is fit to serve a Container or not.
	GetOversubscriptionFactor(ratio decimal.Decimal) decimal.Decimal

	// SubscriptionRatio returns the subscription ratio of the Cluster.
	SubscriptionRatio() float64
}

// unsafeApplyResourceSnapshotToHost does the actual work of the ApplyResourceSnapshotToHost function, but
// no locks are acquired.
//
// unsafeApplyResourceSnapshotToHost should only be called if both the syncMutex and schedulingMutex of the specified
// Host are already held.
func unsafeApplyResourceSnapshotToHost(h *Host, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error {
	if h == nil {
		log.Fatalf(utils.RedStyle.Render("Attempted to apply (possibly nil) resource snapshot to nil Host."))
	}

	if snapshot == nil {
		log.Fatalf(utils.RedStyle.Render("Attempted to apply nil resource snapshot to Host %s (ID=%s)."),
			h.NodeName, h.ID)
	}

	if h.lastSnapshot != nil && snapshot.GetSnapshotId() < h.lastSnapshot.GetSnapshotId() {
		h.log.Warn(utils.OrangeStyle.Render("Given snapshot has ID %d < our last applied snapshot (with ID=%d). Rejecting."),
			h.lastSnapshot.GetSnapshotId(), snapshot.GetSnapshotId())
		return fmt.Errorf("%w: last applied snapshot had ID=%d, specified snapshot had ID=%d",
			scheduling.ErrOldSnapshot, h.lastSnapshot.GetSnapshotId(), snapshot.GetSnapshotId())
	}

	err := resource.ApplySnapshotToResourceWrapper(h.resourceManager, snapshot)
	if err != nil {
		h.log.Error("Failed to apply snapshot %s to host %s (ID=%s) because: %v",
			snapshot.String(), h.NodeName, h.ID, err)
		return err
	}

	h.lastSnapshot = snapshot

	return nil
}

type Host struct {
	proto.LocalGatewayClient

	log logger.Logger

	latestGpuInfo                  *proto.GpuInfo                                      // latestGpuInfo is the latest GPU info of this host scheduler.
	syncMutex                      sync.Mutex                                          // syncMutex ensures atomicity of the Host's SynchronizeResourceInformation method.
	schedulingMutex                sync.Mutex                                          // schedulingMutex ensures that only a single kernel is scheduled at a time, to prevent over-allocating ComputeResource on the Host.
	meta                           hashmap.HashMap[string, interface{}]                // meta is a map of metadata.
	conn                           *grpc.ClientConn                                    // conn is the gRPC connection to the Host.
	Addr                           string                                              // Addr is the Host's address.
	NodeName                       string                                              // NodeName is the Host's name (for printing/logging).
	metricsProvider                scheduling.MetricsProvider                          // Provides access to metrics relevant to the Host.
	ID                             string                                              // ID is the unique ID of this host.
	containers                     hashmap.HashMap[string, scheduling.KernelContainer] // containers is a map from kernel ID to the container from that kernel scheduled on this Host.
	reservations                   hashmap.HashMap[string, time.Time]                  // reservations is a map that really just functions as a set, whose keys are kernel IDs. These are kernels for which resources have been reserved, but the Container has not yet been scheduled yet. The values are the times at which the reservation was created, just for logging purposes.
	trainingContainers             []scheduling.KernelContainer                        // trainingContainers are the actively-training kernel replicas.
	seenSessions                   []string                                            // seenSessions are the sessions that have been scheduled onto this host at least once.
	resourceSpec                   *types.DecimalSpec                                  // resourceSpec is the spec describing the total ComputeResource available on the Host, not impacted by allocations.
	lastReschedule                 types.StatFloat64                                   // lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	errorCallback                  scheduling.ErrorCallback                            // errorCallback is a function to be called if a Host appears to be dead.
	pendingContainers              types.StatInt32                                     // pendingContainers is the number of Containers that are scheduled on the host.
	enabled                        bool                                                // enabled indicates whether the Host is currently enabled and able to serve kernels. This is part of an abstraction to simulate dynamically changing the number of nodes in the cluster.
	excludedFromScheduling         bool                                                // ExcludedFromScheduling is a flag that, when true, indicates that the Host should not be considered for scheduling operations at this time.
	isBeingConsideredForScheduling atomic.Int32                                        // IsBeingConsideredForScheduling indicates that the host has been selected as a candidate for scheduling when the value is > 0. The value is how many concurrent scheduling operations are considering this Host.
	CreatedAt                      time.Time                                           // CreatedAt is the time at which the Host was created.
	resourceManager                *resource.Manager                                   // resourcesWrapper wraps all the Host's ComputeResource.
	LastRemoteSync                 time.Time                                           // lastRemoteSync is the time at which the Host last synchronized its resource counts with the actual remote node that the Host represents.
	isContainedWithinIndex         bool                                                // isContainedWithinIndex indicates whether this Host is currently contained within a valid ClusterIndex.
	ProperlyInitialized            bool                                                // Indicates whether this Host was created with all the necessary fields or not. This doesn't happen when we're restoring an existing Host (i.e., we create a Host struct with many fields missing in that scenario).
	numReplicasPerKernel           int                                                 // The number of replicas per kernel.

	// lastSnapshot is the last HostResourceSnapshot to have been applied successfully to this Host.
	lastSnapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]

	// SubscriptionQuerier is used to query the oversubscription factor given the host's
	// subscription ratio and the Cluster's subscription ratio.
	SubscriptionQuerier SubscriptionQuerier

	// Cached penalties
	sip               cache.InlineCache      // Scale-in penalty.
	sipSession        scheduling.UserSession // Scale-in penalty session.
	subscribedRatio   decimal.Decimal
	penaltyList       cache.InlineCache
	penalties         []cachedPenalty
	penaltyValidity   bool
	schedulerPoolType scheduling.SchedulerPoolType
	heapIndex         int
}

// newHostForRestoration creates and returns a new Host to be used only for restoring an existing Host.
// That is, newHostForRestoration should never be used to create a *Host struct for non-restorative purposes.
//
// Restoration occurs when a Local Daemon that was already connected to the Cluster Gateway reconnects, such as
// after suffering from a network partition/lost connection.
//
// newHostForRestoration always returns a non-nil error. It either returns an error returned by the network
// call to retrieve the latest GPU info from the remote host, or it returns an ErrRestoreRequired error
// to ensure that the Cluster Gateway knows to use the returned Host to restore an existing Host.
func newHostForRestoration(localGatewayClient proto.LocalGatewayClient, confirmedId *proto.HostId, millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int) (*Host, error) {
	gpuInfoResp, gpuFetchError := localGatewayClient.GetActualGpuInfo(context.Background(), &proto.Void{})
	if gpuFetchError != nil {
		log.Printf(utils.RedStyle.Render("[ERROR] Failed to fetch latest GPU information from "+
			"existing+reconnecting Local Daemon %s (ID=%s)\n"), confirmedId.NodeName, confirmedId.Id)
		return nil, gpuFetchError
	}

	// Create the ResourceSpec defining the ComputeResource available on the Host.
	resourceSpec := &types.DecimalSpec{
		GPUs:      decimal.NewFromFloat(float64(gpuInfoResp.SpecGPUs)),
		Millicpus: decimal.NewFromFloat(float64(millicpus)),
		MemoryMb:  decimal.NewFromFloat(float64(memMb)),
		VRam:      decimal.NewFromFloat(vramGb),
	}

	// Create a Host struct populated with a few key fields.
	// This Host will be used to "restore" the existing Host struct.
	// That is, the existing Host struct will replace its values for these fields
	// with the values of this new Host struct.
	//
	// The most important is probably the LocalGatewayClient, as that ensures that the
	// existing Host struct has a new, valid connection to the remote Local Daemon.
	host := &Host{
		ID:                   confirmedId.Id,
		resourceSpec:         resourceSpec,
		NodeName:             confirmedId.NodeName,
		latestGpuInfo:        gpuInfoResp,
		LocalGatewayClient:   localGatewayClient,
		ProperlyInitialized:  false,
		numReplicasPerKernel: numReplicasPerKernel,
	}

	return host, ErrRestoreRequired
}

// NewHost creates and returns a new *Host.
//
// If NewHost is called directly, then the conn field of the Host will not be populated. To populate this field,
// call NewHostWithConn instead.
func NewHost(id string, addr string, millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int, querier SubscriptionQuerier,
	metricsProvider scheduling.MetricsProvider, localGatewayClient proto.LocalGatewayClient, errorCallback scheduling.ErrorCallback) (*Host, error) {

	// Set the ID. If this fails, the creation of a new host scheduler fails.
	confirmedId, err := localGatewayClient.SetID(context.Background(), &proto.HostId{Id: id})

	// If error is now non-nil, either because there was an explicit error or because the response was invalid,
	// then the host scheduler creation failed, and we return nil and the error.
	if err != nil {
		log.Printf(utils.OrangeStyle.Render("Error while creating new Host with ID=\"%s\": %v\n"), id, err)
		return nil, err
	}

	// Validate the response if there's no explicit error.
	if confirmedId.NodeName == "" {
		return nil, ErrNodeNameUnspecified
	}

	// If the ID we received back is different, then this is most likely a host that already exists.
	if confirmedId.Id != id {
		log.Printf("[INFO] Confirmed ID and specified ID for new Host differ. "+
			"Confirmed ID: \"%s\". Specified ID: \"%s\".\n", confirmedId.Id, id)

		// The ID we passed does not equal the ID we received back.
		// Replace the ID we were going to use with the ID we received.
		id = confirmedId.Id
	}

	// If the node already exists, then we need to restore it, rather than create an entirely new node.
	if confirmedId.Existing {
		log.Printf("[INFO] New Local Daemon connection is actually from an existing Local Daemon "+
			"(%s, ID=%s) that is reconnecting.\n", confirmedId.NodeName, confirmedId.Id)
		return newHostForRestoration(localGatewayClient, confirmedId, millicpus, memMb, vramGb, numReplicasPerKernel)
	}

	// Get the initial GPU info. If this fails, the creation of a new host scheduler fails.
	gpuInfoResp, err := localGatewayClient.GetActualGpuInfo(context.Background(), &proto.Void{})
	if err != nil {
		return nil, err
	}

	// Create the ResourceSpec defining the ComputeResource available on the Host.
	resourceSpec := &types.DecimalSpec{
		GPUs:      decimal.NewFromFloat(float64(gpuInfoResp.SpecGPUs)),
		Millicpus: decimal.NewFromFloat(float64(millicpus)),
		MemoryMb:  decimal.NewFromFloat(float64(memMb)),
		VRam:      decimal.NewFromFloat(vramGb),
	}

	log.Printf("Registering brand new Local Daemon %s (ID=%s) with the following resource spec: %s.",
		confirmedId.NodeName, confirmedId.Id, resourceSpec.String())

	host := &Host{
		LocalGatewayClient:   localGatewayClient,
		latestGpuInfo:        gpuInfoResp,
		ID:                   id,
		NodeName:             confirmedId.NodeName,
		Addr:                 addr,
		resourceSpec:         resourceSpec,
		numReplicasPerKernel: numReplicasPerKernel,
		metricsProvider:      metricsProvider,
		log:                  config.GetLogger(fmt.Sprintf("Host %s ", id)),
		containers:           hashmap.NewCornelkMap[string, scheduling.KernelContainer](5),
		reservations:         hashmap.NewCornelkMap[string, time.Time](5),
		trainingContainers:   make([]scheduling.KernelContainer, 0, int(resourceSpec.GPU())),
		penalties:            make([]cachedPenalty, int(resourceSpec.GPU())),
		seenSessions:         make([]string, int(resourceSpec.GPU())),
		meta:                 hashmap.NewCornelkMap[string, interface{}](64),
		errorCallback:        errorCallback,
		enabled:              true,
		CreatedAt:            time.Now(),
		SubscriptionQuerier:  querier,
		ProperlyInitialized:  true,
	}

	host.resourceManager = resource.NewManager(resourceSpec)

	host.sip.Producer = cache.FormalizeICProducer(host.getSIP)
	host.sip.Validator = GetClockTimeCacheValidator()
	host.penaltyList.Producer = cache.FormalizeChainedICProducer(host.updatePenaltyList)
	host.penaltyList.Validator = host.validatePenaltyList

	host.subscribedRatio = decimal.Zero

	return host, nil
}

// NewHostWithConn creates and returns a new *Host.
func NewHostWithConn(id string, addr string, millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int, querier SubscriptionQuerier,
	metricsProvider scheduling.MetricsProvider, conn *grpc.ClientConn, errorCallback scheduling.ErrorCallback) (*Host, error) {

	// Create gRPC client.
	localGatewayClient := proto.NewLocalGatewayClient(conn)

	host, err := NewHost(id, addr, millicpus, memMb, vramGb, numReplicasPerKernel, querier, metricsProvider, localGatewayClient, errorCallback)
	if err != nil {
		// We need to return host here, in case the error is ErrRestoreRequired, as a host IS returned in that case.
		// It's a host with only some fields filled-in so that it can be used to restore the existing host.
		return host, err
	}

	// Populate the conn field "retroactively".
	host.conn = conn

	return host, nil
}

func (h *Host) IsContainedWithinIndex() bool {
	return h.isContainedWithinIndex
}

func (h *Host) SetContainedWithinIndex(contained bool) {
	h.isContainedWithinIndex = contained
}

func (h *Host) GetLastRemoteSync() time.Time {
	return h.LastRemoteSync
}

func (h *Host) IsExcludedFromScheduling() bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	return h.excludedFromScheduling
}

// ExcludeFromScheduling attempts to exclude this Host from being considered for scheduling operations.
//
// ExcludeFromScheduling will return true if the Host was successfully excluded.
//
// If ExcludeFromScheduling returns false, then the Host is already being considered for scheduling by one or more
// scheduling operations and thus cannot be excluded at this time.
func (h *Host) ExcludeFromScheduling() bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if numOperationsConsideringHost := h.isBeingConsideredForScheduling.Load(); numOperationsConsideringHost > 0 {
		h.log.Debug("Host %s (ID=%s) cannot be excluded from consideration from scheduling operations as it is "+
			"already being considered by %d scheduling operation(s).", h.NodeName, h.ID, numOperationsConsideringHost)
		return false
	}

	h.log.Debug("Host %s (ID=%s) is now precluded from being considered for scheduling.", h.NodeName, h.ID)
	h.excludedFromScheduling = true
	return true
}

func (h *Host) Containers() hashmap.HashMap[string, scheduling.KernelContainer] {
	return h.containers
}

// IncludeForScheduling designates the target Host as being able to be considered in scheduling operations.
//
// IncludeForScheduling returns nil on success. If the target Host is already allowed to be considered during
// scheduling operations, then IncludeForScheduling will return an ErrHostAlreadyIncludedForScheduling error.
func (h *Host) IncludeForScheduling() error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if !h.excludedFromScheduling {
		return ErrHostAlreadyIncludedForScheduling
	}

	h.excludedFromScheduling = false
	h.log.Debug("Host %s (ID=%s) will be included for consideration in scheduling operations again.", h.NodeName, h.ID)
	return nil
}

func (h *Host) IsBeingConsideredForScheduling() bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	return h.isBeingConsideredForScheduling.Load() > 0
}

func (h *Host) GetNodeName() string {
	return h.NodeName
}

func (h *Host) GetID() string {
	return h.ID
}

// ConsiderForScheduling ensures that this Host is not excluded for scheduling nor will it be excluded from scheduling
// until it is no longer being considered for scheduling.
//
// This will NOT return false if the host is already being considered for scheduling by a separate scheduling operation.
// Concurrently scheduling operations are permitted.
func (h *Host) ConsiderForScheduling() bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if h.excludedFromScheduling {
		h.log.Debug("Cannot consider host %s (ID=%s) for scheduling; it is presently excluded from scheduling.",
			h.NodeName, h.ID)
		return false
	}

	numOperationsConsideringHost := h.isBeingConsideredForScheduling.Add(1)
	h.log.Debug("Host %s (ID=%s) is being considered in a scheduling operation (%d).",
		h.NodeName, h.ID, numOperationsConsideringHost)
	return true
}

func (h *Host) SchedulerPoolType() scheduling.SchedulerPoolType {
	return h.schedulerPoolType
}

func (h *Host) SetSchedulerPoolType(schedulerPoolType scheduling.SchedulerPoolType) {
	h.schedulerPoolType = schedulerPoolType
}

// SetIdx is part of the HeapElement implementation.
func (h *Host) SetIdx(idx int) {
	h.heapIndex = idx
}

func (h *Host) Compare(h2 interface{}) float64 {
	switch h.schedulerPoolType {
	case scheduling.SchedulerPoolTypeUndersubscribed:
		// Max heap.
		switch v := h2.(type) {
		case float64:
			return h.IdleGPUs() - v // Seeking value, simply follow normal logic.
		}

		ret := h2.(*Host).IdleGPUs() - h.IdleGPUs()

		// For the pool to provide all GPUs to one container, idle gpus are either 0 or all.
		if ret != 0.0 {
			return ret
		}

		return h.subscribedRatio.Sub(h2.(*Host).SubscribedRatioAsDecimal()).InexactFloat64()
	default:
		// SchedulerPoolTypeOversubscribed
		// Min heap.
		switch h2.(type) {
		case float64:
			log.Printf("Non-updated schedulerPoolType: host %s", h.ID)
		}
		return h.subscribedRatio.Sub(h2.(*Host).SubscribedRatioAsDecimal()).InexactFloat64()
	}
}

// RecomputeSubscribedRatio forces the Host to recompute its subscription ratio.
// The new value is returned.
func (h *Host) RecomputeSubscribedRatio() decimal.Decimal {
	if h.resourceSpec.GPU() == 0 {
		h.subscribedRatio = decimal.Zero.Copy()
		return h.subscribedRatio
	}

	var divisor decimal.Decimal
	if h.numReplicasPerKernel == 0 {
		divisor = decimal.NewFromFloat(1.0)
	} else {
		divisor = decimal.NewFromFloat(float64(h.numReplicasPerKernel))
	}
	h.subscribedRatio = h.PlacedGPUs().Div(h.resourceSpec.GPUs).Div(divisor)

	return h.subscribedRatio
}

// LastResourcesSnapshot returns the last HostResourceSnapshot to have been applied successfully to this Host.
//
// If the target Host has had no HostResourceSnapshot instances applied successfully, then this method returns nil.
func (h *Host) LastResourcesSnapshot() types.HostResourceSnapshot[types.ArbitraryResourceSnapshot] {
	return h.lastSnapshot
}

// SubscribedRatio returns the current subscription ratio of the Host as a float64.
func (h *Host) SubscribedRatio() float64 {
	return h.subscribedRatio.InexactFloat64()
}

// SubscribedRatioAsDecimal returns the current subscription ratio of the Host as a decimal.Decimal.
func (h *Host) SubscribedRatioAsDecimal() decimal.Decimal {
	return h.subscribedRatio
}

// OversubscriptionFactor returns the result of passing the Host's current subscribedRatio
// to its OversubscriptionQuerierFunction field/function.
func (h *Host) OversubscriptionFactor() decimal.Decimal {
	return h.SubscriptionQuerier.GetOversubscriptionFactor(h.subscribedRatio)
}

// ToVirtualDockerNode converts a Host struct to a proto.VirtualDockerNode struct and
// returns a pointer to the new proto.VirtualDockerNode.
func (h *Host) ToVirtualDockerNode() *proto.VirtualDockerNode {
	dockerContainers := make([]*proto.DockerContainer, 0, h.containers.Len())
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
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
		SpecVRAM:        float32(h.resourceSpec.VRAM()),
		AllocatedCpu:    float32(h.resourceManager.CommittedResources().Millicpus()),
		AllocatedMemory: float32(h.resourceManager.CommittedResources().MemoryMB()),
		AllocatedGpu:    float32(h.resourceManager.CommittedResources().GPUs()),
		AllocatedVRAM:   float32(h.resourceManager.CommittedResources().VRAM()),
		PendingCpu:      float32(h.resourceManager.PendingResources().Millicpus()),
		PendingMemory:   float32(h.resourceManager.PendingResources().MemoryMB()),
		PendingGpu:      float32(h.resourceManager.PendingResources().GPUs()),
		PendingVRAM:     float32(h.resourceManager.PendingResources().VRAM()),
	}
}

// NumContainers returns the number of Container instances scheduled on the Host.
func (h *Host) NumContainers() int {
	return h.containers.Len()
}

// NumReservations returns the number of active reservations on the Host.
func (h *Host) NumReservations() int {
	return h.reservations.Len()
}

// SynchronizeResourceInformation queries the remote host via gRPC to request update-to-date resource usage information.
//
// This method is thread-safe. Only one goroutine at a time may execute this method.
//
// Similarly, once the snapshot is retrieved from the remote Host, scheduling will be temporarily locked
// until the snapshot is applied successfully.
func (h *Host) SynchronizeResourceInformation() error {
	h.syncMutex.Lock()
	defer h.syncMutex.Unlock()

	st := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	snapshotWithContainers, err := h.LocalGatewayClient.ResourcesSnapshot(ctx, &proto.Void{})
	if err != nil {
		h.log.Error(utils.OrangeStyle.Render("Failed to retrieve Resource ManagerSnapshot from remote node %s (ID=%s) because: %v"),
			h.NodeName, h.ID, err)
		return err
	}

	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	// Manager around the protobuf struct so that it satisfies the required interface.
	protoSnapshotWrapper := &resource.ProtoNodeResourcesSnapshotWrapper{
		NodeResourcesSnapshotWithContainers: snapshotWithContainers,
	}

	err = unsafeApplyResourceSnapshotToHost(h, protoSnapshotWrapper)
	if err != nil {
		h.log.Error(utils.OrangeStyle.Render("Failed to apply retrieved Resource ManagerSnapshot from remote node %s (ID=%s) because: %v"),
			h.NodeName, h.ID, err)
		return err
	}

	if h.metricsProvider != nil {
		h.metricsProvider.GetHostRemoteSyncLatencyMicrosecondsHistogram().Observe(float64(time.Since(st).Microseconds()))
	}

	h.RecomputeSubscribedRatio()

	h.LastRemoteSync = time.Now()
	return nil
}

// PlacedMemoryMB returns the total amount of memory scheduled onto the Host, which is computed as the
// sum of the Host's pending memory and the Host's committed memory, in megabytes.
func (h *Host) PlacedMemoryMB() decimal.Decimal {
	return h.resourceManager.PendingResources().MemoryMbAsDecimal().Add(h.resourceManager.CommittedResources().MemoryMbAsDecimal())
}

// PlacedGPUs returns the total number of GPUs scheduled onto the Host, which is computed as the
// sum of the Host's pending GPUs and the Host's committed GPUs.
func (h *Host) PlacedGPUs() decimal.Decimal {
	return h.resourceManager.PendingResources().GPUsAsDecimal().Add(h.resourceManager.CommittedResources().GPUsAsDecimal())
}

// PlacedCPUs returns the total number of Millicpus scheduled onto the Host, which is computed as the
// sum of the Host's pending Millicpus and the Host's committed Millicpus.
func (h *Host) PlacedCPUs() decimal.Decimal {
	return h.resourceManager.PendingResources().MillicpusAsDecimal().Add(h.resourceManager.CommittedResources().MillicpusAsDecimal())
}

// computeHypotheticalSubscriptionRatio computes what the Host's (over)subscription ratios would be for CPU, Memory,
// and GPU, if it were to serve a Container with the given types.Spec resource request/requirements.
func (h *Host) computeHypotheticalSubscriptionRatio(resourceRequest types.Spec) (decimal.Decimal, decimal.Decimal, decimal.Decimal) {
	divisor := decimal.NewFromFloat(float64(h.numReplicasPerKernel))

	// Convert the given types.Spec to a *types.DecimalSpec.
	var decimalSpec *types.DecimalSpec
	if specAsDecimalSpec, ok := resourceRequest.(*types.DecimalSpec); ok {
		// If the parameter is already a *types.DecimalSpec, then no actual conversion needs to be performed.
		decimalSpec = specAsDecimalSpec
	} else {
		decimalSpec = types.ToDecimalSpec(resourceRequest)
	}

	var cpuRatio, memRatio, gpuRatio decimal.Decimal

	if h.resourceManager.SpecResources().MillicpusAsDecimal().Equals(decimal.Zero) {
		cpuRatio = decimal.Zero
	} else {
		totalCPUs := h.PlacedCPUs().Add(decimalSpec.Millicpus)
		cpuRatio = totalCPUs.Div(h.resourceManager.SpecResources().MillicpusAsDecimal()).Div(divisor)
	}

	if h.resourceManager.SpecResources().MemoryMbAsDecimal().Equals(decimal.Zero) {
		memRatio = decimal.Zero
	} else {
		totalMemory := h.PlacedMemoryMB().Add(decimalSpec.MemoryMb)
		memRatio = totalMemory.Div(h.resourceManager.SpecResources().MemoryMbAsDecimal()).Div(divisor)
	}

	if h.resourceManager.SpecResources().GPUsAsDecimal().Equals(decimal.Zero) {
		gpuRatio = decimal.Zero
	} else {
		totalGPUs := h.PlacedGPUs().Add(decimalSpec.GPUs)
		gpuRatio = totalGPUs.Div(h.resourceManager.SpecResources().GPUsAsDecimal()).Div(divisor)
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

	willOversubscribeCpu := h.SubscriptionQuerier.GetOversubscriptionFactor(cpuRatio).GreaterThanOrEqual(decimal.Zero)
	willOversubscribeMemory := h.SubscriptionQuerier.GetOversubscriptionFactor(memRatio).GreaterThanOrEqual(decimal.Zero)
	willOversubscribeGpu := h.SubscriptionQuerier.GetOversubscriptionFactor(gpuRatio).GreaterThanOrEqual(decimal.Zero)

	subscriptionRatio := h.SubscriptionQuerier.SubscriptionRatio()

	h.log.Debug("Computed over-subscription ratios for resource request: %v. Current subscription ratio: %.4f.\n"+
		"CPU Ratio: %s (Will Oversubscribe? %v), Memory Ratio: %s (Will Oversubscribe? %v), GPU Ratio: %s (Will Oversubscribe? %v)",
		resourceRequest.String(), subscriptionRatio, cpuRatio.StringFixed(4), willOversubscribeCpu, memRatio.StringFixed(4),
		willOversubscribeMemory, gpuRatio.StringFixed(4), willOversubscribeGpu)

	return willOversubscribeCpu || willOversubscribeMemory || willOversubscribeGpu
}

// CanServeContainerWithError returns nil if the target Host can serve the resource request.
//
// This method only checks against the Host's "spec" (i.e., the total ComputeResource available on the Host,
// not taking into account current resource allocations).
func (h *Host) CanServeContainerWithError(resourceRequest types.Spec) (bool, error) {
	err := h.resourceManager.SpecResources().ValidateWithError(resourceRequest)
	if err != nil {
		return false, err
	}

	return true, nil
}

// CanServeContainer returns a boolean indicating whether this Host could serve a kernel replica with the given
// resource requirements / resource request. This method only checks against the Host's "spec" (i.e., the total
// ComputeResource available on the Host, not taking into account current resource allocations).
//
// CanServeContainer returns true when the Host could serve the hypothetical kernel and false when the Host could not.
func (h *Host) CanServeContainer(resourceRequest types.Spec) bool {
	return h.resourceManager.SpecResources().Validate(resourceRequest)
}

// CanCommitResources returns a boolean indicating whether this Host could commit the specified resource request
// to a kernel scheduled onto the Host right now. Commiting resource requires having sufficiently many idle ComputeResource
// available.
//
// CanCommitResources returns true if the Host could commit/reserve the given ComputeResource right now.
// Otherwise, CanCommitResources returns false.
func (h *Host) CanCommitResources(resourceRequest types.Spec) bool {
	return h.resourceManager.IdleResources().Validate(types.ToDecimalSpec(resourceRequest))
}

// ReleaseReservation is to be called when a resource reservation should be released because the
// scheduling of the associated replica of the associated kernel is being aborted.
func (h *Host) ReleaseReservation(spec *proto.KernelSpec) error {
	_, loadedReservation := h.reservations.LoadAndDelete(spec.Id)
	if !loadedReservation {
		h.log.Error("Cannot release resource reservation associated with kernel %s; no reservations found.", spec.Id)
		return fmt.Errorf("%w: kernel %s", ErrReservationNotFound, spec.Id)
	}

	// No longer being considered.
	h.isBeingConsideredForScheduling.Add(-1)

	return nil
}

// ReserveResources attempts to reserve the resources required by the specified kernel, returning
// a boolean flag indicating whether the resource reservation was completed successfully.
//
// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
func (h *Host) ReserveResources(spec *proto.KernelSpec) (bool, error) {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	// Check if we're already hosting a replica of the target kernel.
	container, containerLoaded := h.containers.Load(spec.Id)
	if containerLoaded {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; already hosting replica %d of kernel %s.",
			spec.Id, container.ReplicaId(), spec.Id)
		return false, nil
	}

	// Check if there's already a reservation for some (not-yet-scheduled) replica of the target kernel.
	// TODO: If there's an error creating the container, we need to release the resource reservation on the Host.
	reservationCreationTimestamp, reservationLoaded := h.reservations.Load(spec.Id)
	if reservationLoaded {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; have existing reservation for that kernel created %v ago.", spec.Id, time.Since(reservationCreationTimestamp))
		return false, nil
	}

	// Check if the Host could satisfy the resource request for the target kernel.
	if !h.CanServeContainer(spec.ResourceSpec.ToDecimalSpec()) {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s. Kernel is requesting more resources than we have allocatable.", spec.Id)
		return false, nil
	}

	if h.WillBecomeTooOversubscribed(spec.ResourceSpec.ToDecimalSpec()) {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; host would become too oversubscribed.", spec.Id)
		return false, nil
	}

	// Increment the pending resources on the host, which represents the reservation.
	// TODO: Synchronizing will erase this.
	if err := h.resourceManager.PendingResources().Add(spec.DecimalSpecFromKernelSpec()); err != nil {
		h.log.Error("Cannot reserve resources for a replica of kernel %s; error encountered while incrementing host's pending resources: %v.", spec.Id, err)
		return false, err
	}

	oldSubscribedRatio := h.subscribedRatio
	h.RecomputeSubscribedRatio()
	h.reservations.Store(spec.Id, time.Now())

	h.log.Debug("Successfully reserved resources for new replica of kernel %s. Old subscription ratio: %s. New subscription ratio: %s.",
		spec.Id, oldSubscribedRatio.StringFixed(3), h.subscribedRatio.StringFixed(3))

	return true, nil
}

// Restore restores the state of a Host from another Host.
func (h *Host) Restore(restoreFrom *Host, callback scheduling.ErrorCallback) error {
	h.SetErrorCallback(callback)
	h.LocalGatewayClient = restoreFrom.LocalGatewayClient
	h.resourceSpec = restoreFrom.resourceSpec
	h.ID = restoreFrom.ID
	h.NodeName = restoreFrom.NodeName
	h.latestGpuInfo = restoreFrom.latestGpuInfo

	return nil
}

// Enabled returns a boolean indicating whether the Host is enabled (true) or disabled (false).
func (h *Host) Enabled() bool {
	return h.enabled
}

// Enable enables the Host.
//
// If the Host is already enabled, then this returns an error.
func (h *Host) Enable(includeInScheduling bool) error {
	if h.enabled {
		// Even if we're already enabled, we should still call Host::IncludeForScheduling if the includeInScheduling
		// argument was passed as true.
		if includeInScheduling {
			_ = h.IncludeForScheduling()
		}

		return fmt.Errorf("%w: host \"%s\" is already enabled", scheduling.ErrInvalidHost, h.ID)
	}

	h.enabled = true
	if includeInScheduling {
		_ = h.IncludeForScheduling()
	}

	return nil
}

// Disable disables the Host.
//
// If the Host is already disabled, then this returns an error.
func (h *Host) Disable() error {
	if !h.enabled {
		return fmt.Errorf("%w: host \"%s\" is already disabled", scheduling.ErrInvalidHost, h.ID)
	}

	h.enabled = false
	return nil
}

// doContainerRemovedResourceUpdate updates the local resource counts of the target Host following (or as a part of)
// the removal of the parameterized Container.
//
// If there's an error while updating the local view of the resource counts of the Host, then we attempt to
// synchronize with the remote Host and try again. If that fails, then we just return an error.
func (h *Host) doContainerRemovedResourceUpdate(container scheduling.KernelContainer) {
	// TODO: Check for deadlock.
	syncLocked := h.syncMutex.TryLock()

	// If we fail to sync-lock, then we're presumably in the process of synchronizing (or applying a new snapshot),
	// in which case we'll just skip updating our local view of the resources of the remote host, as that update is
	// already underway.
	var err error
	if syncLocked {
		// If our last snapshot from the remote host was received after the container started, and that last snapshot
		// does not show the container as being scheduled on the host, then we can skip updating the state locally,
		// as we've already synchronized with the remote host post-removal.
		if h.lastSnapshot != nil && h.lastSnapshot.GetGoTimestamp().After(container.StartedAt()) {
			containers := h.lastSnapshot.GetContainers()
			found := false

			// Check the containers. If the container being removed is one of 'em, then we'll update the local view
			// of the resources on the remote host. If the container is not one of 'em, then the host had already
			// removed it when we last synchronized with the host, so we can skip the local resource count update
			// (or else we'll be applying it twice).
			for _, containerOnHost := range containers {
				if containerOnHost.GetReplicaId() == container.ReplicaId() && containerOnHost.GetKernelId() == container.KernelID() {
					// Found the host in the snapshot, so the removal of its resources hasn't already been applied.
					// TODO: Race condition (unless we have the locking of syncMutex up above).
					err = h.resourceManager.PendingResources().Subtract(container.ResourceSpec())
					found = true
					break
				}
			}

			if !found {
				h.log.Debug("Did not find container for replica %d of kernel %s in last snapshot from host %s (ID=%s). Skipping local resource update.",
					container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
			}
		} else {
			// We either don't have a snapshot at all, or we don't have a recent-enough snapshot.
			err = h.resourceManager.PendingResources().Subtract(container.ResourceSpec())
		}

		h.syncMutex.Unlock()
	} else {
		h.log.Debug("Failed to sync-lock host %s (ID=%s) while removing container for replica %d of kernel %s. Skipping local resource update.",
			h.NodeName, h.ID, container.ReplicaId(), container.KernelID())
	}

	if err == nil {
		h.log.Debug("Cleanly updated resources after removing container for replica %d of kernel %s from host %s (ID=%s)",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return
	}

	// TODO: Should we bother forcing a synchronization here?

	h.log.Warn("Could not cleanly remove Container %s from Host %s due to resource-related issue (though the container WAS still removed): %v",
		container.ContainerID(), h.ID, err)
}

// ContainerStoppedTraining is to be called when a Container stops training on a Host.
func (h *Host) ContainerStoppedTraining(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.KernelID()); !ok {
		h.log.Error("Cannot find container for replica %d of kernel %s on host %s (ID=%s).",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return ErrInvalidContainer
	}

	if err := h.resourceManager.CommittedResources().Subtract(container.ResourceSpec()); err != nil {
		return err
	}
	if err := h.resourceManager.PendingResources().Add(container.ResourceSpec()); err != nil {
		return err
	}
	if err := h.resourceManager.IdleResources().Add(container.ResourceSpec()); err != nil {
		return err
	}

	return nil
}

// ContainerStartedTraining is to be called when a Container begins training on a Host.
func (h *Host) ContainerStartedTraining(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.KernelID()); !ok {
		h.log.Error("Cannot find container for replica %d of kernel %s on host %s (ID=%s).",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return ErrInvalidContainer
	}

	if err := h.resourceManager.CommittedResources().Add(container.ResourceSpec()); err != nil {
		return err
	}
	if err := h.resourceManager.PendingResources().Subtract(container.ResourceSpec()); err != nil {
		return err
	}
	if err := h.resourceManager.IdleResources().Subtract(container.ResourceSpec()); err != nil {
		return err
	}

	return nil
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.KernelID()); !ok {
		h.log.Error("Cannot remove specified Container from Host. Container is not on specified Host.")
		return ErrInvalidContainer
	}

	h.containers.Delete(container.KernelID())

	h.pendingContainers.Sub(1)

	h.doContainerRemovedResourceUpdate(container)

	h.RecomputeSubscribedRatio()

	return nil
}

// ContainerScheduled is to be called when a Container is scheduled onto the Host.
func (h *Host) ContainerScheduled(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	h.containers.Store(container.KernelID(), container)
	h.pendingContainers.Add(1)

	// Delete the reservation. Log an error message if there is no reservation.
	reservation, loadedReservation := h.reservations.LoadAndDelete(container.KernelID())
	if !loadedReservation {
		h.log.Error("No reservation found for replica of kernel %s; "+
			"however, we just received a notification that replica %d of kernel %s has started on host %s (ID=%s)...",
			container.KernelID(), container.ReplicaId(), container.KernelID(), h.ID, h.NodeName)

		return fmt.Errorf("%w: kernel %s", ErrReservationNotFound, container.KernelID())
	}

	h.log.Debug("Container %s was officially started on onto Host %s %v after reservation was created.",
		container.String(), h.ID, time.Since(reservation))

	// Container was scheduled onto us, so we're no longer being considered for scheduling, as the scheduling
	// operation concluded (and scheduled a replica onto us).
	h.isBeingConsideredForScheduling.Add(-1)

	return nil
}

// ErrorCallback returns the Host's ErrorCallback field.
func (h *Host) ErrorCallback() scheduling.ErrorCallback {
	return h.errorCallback
}

// SetErrorCallback sets the Host's ErrorCallback field.
func (h *Host) SetErrorCallback(callback scheduling.ErrorCallback) {
	h.errorCallback = callback
}

func (h *Host) getPenalty(cached *cachedPenalty, gpus int) (*cachedPenalty, error) {
	if cached.valid {
		return cached, nil
	}

	list := h.penaltyList.Value().(*scheduling.PenaltyContainers)
	penalty, preempted, err := list.Penalty(float64(gpus))
	// Cache valid result only
	cached.penalty = penalty
	cached.preemptions = list.ContainerList[:preempted]
	cached.valid = err == nil
	cached.explain = fmt.Sprintf("candidates: %s", list.ContainerList[0].ContainerStatistics().Explain(scheduling.ExplainPreemptionPriority))
	for i := 1; i < preempted; i++ {
		cached.explain += fmt.Sprintf(", %s", list.ContainerList[i].ContainerStatistics().Explain(scheduling.ExplainPreemptionPriority))
	}

	h.log.Trace("Cached penalty for %du: %.2f", gpus, cached.penalty)
	return cached, err
}

func (h *Host) Penalty(gpus float64) (float64, scheduling.PreemptionInfo, error) {
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

func (h *Host) getSIP(sess scheduling.UserSession) float64 {
	numGPUs := float64(sess.ResourceUtilization().GetNumGpus())

	penalty, _, err := h.Penalty(numGPUs)
	if err != nil {
		h.log.Error("Unexpected err on calculating AB: %v", err)
	}
	h.sip.Validator(time.Now())

	rb := h.getRB(sess.SessionStatistics().InteractivePriority(), numGPUs)
	h.log.Debug("Cached sip for session %v: %.2f(%.2f-%.2f). IP: %.4f (%s).", sess, rb-penalty, rb, penalty,
		sess.SessionStatistics().InteractivePriority(), sess.SessionStatistics().Explain(scheduling.ExplainInteractivePriority))
	return rb - penalty
}

func (h *Host) getRB(sessRB float64, required float64) float64 {
	//idleGPUs := h.idleGPUs.Load()
	idleGPUs := h.resourceManager.IdleResources().GPUs()
	extras := 0.0
	if idleGPUs > required {
		//extras = idleGPUs / h.pendingGPUs.Load()
		extras = idleGPUs / h.resourceManager.PendingResources().GPUs()
	}
	rb := sessRB * (extras + 1) / float64(h.pendingContainers.Load())
	h.log.Debug("Calculated RB: %.4f\n", h.ID, rb)
	return rb
}

func (h *Host) validatePenaltyList(_ interface{}) bool {
	return h.penaltyValidity
}

func (h *Host) updatePenaltyList(cached *scheduling.PenaltyContainers) *scheduling.PenaltyContainers {
	h.penaltyValidity = true
	if cached == nil {
		cached = &scheduling.PenaltyContainers{ContainerList: scheduling.ContainerList(h.trainingContainers)}
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
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
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
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
		if container.KernelID() == kernelId && container.ReplicaId() == replicaId {
			found = true
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return found
}

// GetAnyReplicaOfKernel returns the scheduling.KernelContainer corresponding to any replica of the specified kernel if such a
// Container is currently scheduled/provisioned on this Host. If not, then nil is returned.
func (h *Host) GetAnyReplicaOfKernel(kernelId string) scheduling.KernelContainer {
	var targetContainer scheduling.KernelContainer
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
		if container.KernelID() == kernelId {
			targetContainer = container
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return targetContainer
}

// GetSpecificReplicaOfKernel returns the scheduling.KernelContainer corresponding to the specified replica of the specified kernel,
// if that Container is currently scheduled/provisioned on this Host. If not, then nil is returned.
func (h *Host) GetSpecificReplicaOfKernel(kernelId string, replicaId int32) scheduling.KernelContainer {
	var targetContainer scheduling.KernelContainer
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
		if container.KernelID() == kernelId && container.ReplicaId() == replicaId {
			targetContainer = container
			return false // Stop iterating.
		}

		return true // Continue iterating.
	})

	return targetContainer
}

func (h *Host) String() string {
	return fmt.Sprintf("Host[ID=%s,Name=%s,Addr=%s,Spec=%s]", h.ID, h.NodeName, h.Addr, h.resourceSpec.String())
}

func (h *Host) GetConnectionState() connectivity.State {
	if h.conn == nil {
		return -1
	}

	return h.conn.GetState()
}

func (h *Host) Stats() scheduling.HostStatistics {
	return h
}

// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
func (h *Host) LastReschedule() types.StatFloat64Field {
	return &h.lastReschedule
}

// TimeSinceLastSynchronizationWithRemote returns a time.Duration indicating how long it has been since its ComputeResource
// were refreshed and synchronized from the actual remote Host that this Host struct represents.
func (h *Host) TimeSinceLastSynchronizationWithRemote() time.Duration {
	return time.Since(h.LastRemoteSync)
}

// SetMeta sets the metadata of the host.
func (h *Host) SetMeta(key scheduling.HostMetaKey, value interface{}) {
	h.meta.Store(string(key), value)
}

// GetMeta return the metadata of the host.
func (h *Host) GetMeta(key scheduling.HostMetaKey) interface{} {
	if h.meta == nil {
		log.Printf(utils.OrangeStyle.Render("[WARNING] Cannot retrieve metadata \"%s\" -- metadata dictionary is nil..."), key)
		return nil
	}

	if value, ok := h.meta.Load(string(key)); ok {
		return value
	}
	return nil
}

func (h *Host) Priority(session scheduling.UserSession) float64 {
	if session != h.sipSession {
		h.sip.Invalidate()
		h.sipSession = session
	}
	return h.sip.Value(session).(float64)
}

// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
func (h *Host) IdleGPUs() float64 {
	return h.resourceManager.IdleResources().GPUs()
}

// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
func (h *Host) PendingGPUs() float64 {
	return h.resourceManager.PendingResources().GPUs()
}

// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
func (h *Host) CommittedGPUs() float64 {
	return h.resourceManager.CommittedResources().GPUs()
}

func (h *Host) IdleCPUs() float64 {
	return h.resourceManager.IdleResources().Millicpus()
}

func (h *Host) PendingCPUs() float64 {
	return h.resourceManager.PendingResources().Millicpus()
}

func (h *Host) CommittedCPUs() float64 {
	return h.resourceManager.CommittedResources().Millicpus()
}

func (h *Host) IdleMemoryMb() float64 {
	return h.resourceManager.IdleResources().MemoryMB()
}

func (h *Host) PendingMemoryMb() float64 {
	return h.resourceManager.PendingResources().MemoryMB()
}

func (h *Host) CommittedMemoryMb() float64 {
	return h.resourceManager.CommittedResources().MemoryMB()
}

func (h *Host) IdleVRAM() float64 { return h.resourceManager.IdleResources().MemoryMB() }

func (h *Host) PendingVRAM() float64 { return h.resourceManager.PendingResources().MemoryMB() }

func (h *Host) CommittedVRAM() float64 { return h.resourceManager.CommittedResources().VRAM() }

// ResourceSpec the types.Spec defining the ComputeResource available on the Host.
func (h *Host) ResourceSpec() types.ValidatableResourceSpec {
	return h.resourceSpec
}

// CurrentResourcesToString calls the String method on the Manager of the Host and returns the value
// generated by that String method.
func (h *Host) CurrentResourcesToString() string {
	return h.resourceManager.String()
}

// IdleResources returns a types.Spec encapsulating the IdleResources on the Host.
func (h *Host) IdleResources() *types.DecimalSpec {
	return h.resourceManager.IdleResources().ToDecimalSpec()
}

// PendingResources returns a types.Spec encapsulating the PendingResources on the Host.
func (h *Host) PendingResources() *types.DecimalSpec {
	return h.resourceManager.PendingResources().ToDecimalSpec()
}

// CommittedResources returns a types.Spec encapsulating the idle ComputeResource on the Host.
func (h *Host) CommittedResources() *types.DecimalSpec {
	return h.resourceManager.CommittedResources().ToDecimalSpec()
}

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}

func (h *Host) AddToPendingResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.PendingResources().Add(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) AddToIdleResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.IdleResources().Add(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) AddToCommittedResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.CommittedResources().Add(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) SubtractFromPendingResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.PendingResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) SubtractFromIdleResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.IdleResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) SubtractFromCommittedResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.CommittedResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}
