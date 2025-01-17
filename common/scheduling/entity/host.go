package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/resource"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/connectivity"
	"log"
	"math"
	"sort"
	"strings"
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
	ErrResourcesAlreadyCommitted        = errors.New("we have already committed resources to a replica of the specified kernel on the target host")
)

// ResourceSpec defines the HostResources available on a particular Host.
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

type IndexUpdater interface {
	UpdateIndex(host scheduling.Host) error
}

// unsafeApplyResourceSnapshotToHost does the actual work of the ApplyResourceSnapshotToHost function, but
// no locks are acquired.
//
// unsafeApplyResourceSnapshotToHost should only be called if both the syncMutex and schedulingMutex of the specified
// Host are already held.
func unsafeApplyResourceSnapshotToHost(h *Host, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error {
	if h == nil {
		log.Fatalln(utils.RedStyle.Render("Attempted to apply (possibly nil) resource snapshot to nil Host."))
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

	latestGpuInfo                       *proto.GpuInfo                                      // latestGpuInfo is the latest GPU info of this host scheduler.
	syncMutex                           sync.Mutex                                          // syncMutex ensures atomicity of the Host's SynchronizeResourceInformation method.
	schedulingMutex                     sync.Mutex                                          // schedulingMutex ensures that only a single kernel is scheduled at a time, to prevent over-allocating HostResources on the Host.
	meta                                hashmap.HashMap[string, interface{}]                // meta is a map of metadata.
	conn                                *grpc.ClientConn                                    // conn is the gRPC connection to the Host.
	Addr                                string                                              // Addr is the Host's address.
	NodeName                            string                                              // NodeName is the Host's name (for printing/logging).
	metricsProvider                     scheduling.MetricsProvider                          // Provides access to metrics relevant to the Host.
	ID                                  string                                              // ID is the unique ID of this host.
	containers                          hashmap.HashMap[string, scheduling.KernelContainer] // containers is a map from kernel ID to the container from that kernel scheduled on this Host.
	reservations                        hashmap.HashMap[string, *Reservation]               // reservations is a map that really just functions as a set, whose keys are kernel IDs. These are kernels for which resources have been reserved, but the Container has not yet been scheduled yet. The values are the times at which the reservation was created, just for logging purposes.
	trainingContainers                  []scheduling.KernelContainer                        // trainingContainers are the actively-training kernel replicas.
	seenSessions                        []string                                            // seenSessions are the sessions that have been scheduled onto this host at least once.
	resourceSpec                        *types.DecimalSpec                                  // resourceSpec is the spec describing the total HostResources available on the Host, not impacted by allocations.
	lastReschedule                      types.StatFloat64                                   // lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	errorCallback                       scheduling.ErrorCallback                            // errorCallback is a function to be called if a Host appears to be dead.
	pendingContainers                   types.StatInt32                                     // pendingContainers is the number of Containers that are scheduled on the host.
	enabled                             bool                                                // enabled indicates whether the Host is currently enabled and able to serve kernels. This is part of an abstraction to simulate dynamically changing the number of nodes in the cluster.
	excludedFromScheduling              bool                                                // ExcludedFromScheduling is a flag that, when true, indicates that the Host should not be considered for scheduling operations at this time.
	isBeingConsideredForScheduling      atomic.Int32                                        // IsBeingConsideredForScheduling indicates that the host has been selected as a candidate for scheduling when the value is > 0. The value is how many concurrent scheduling operations are considering this Host.
	CreatedAt                           time.Time                                           // CreatedAt is the time at which the Host was created.
	resourceManager                     *resource.Manager                                   // resourcesWrapper wraps all the Host's HostResources.
	LastRemoteSync                      time.Time                                           // lastRemoteSync is the time at which the Host last synchronized its resource counts with the actual remote node that the Host represents.
	isContainedWithinIndex              bool                                                // isContainedWithinIndex indicates whether this Host is currently contained within a valid ClusterIndex.
	ProperlyInitialized                 bool                                                // Indicates whether this Host was created with all the necessary fields or not. This doesn't happen when we're restoring an existing Host (i.e., we create a Host struct with many fields missing in that scenario).
	numReplicasPerKernel                int                                                 // The number of replicas per kernel.
	resourceBindingMode                 scheduling.ResourceBindingMode                      // resourceBindingMode indicates the time at which resources are (exclusively) committed to containers, and implicitly when they are uncommitted from containers as well.
	kernelsWithCommittedResources       map[string]int32                                    // Map from Kernel ID to int32. Values are replica IDs who have resources committed to them. We use kernel ID as the key, rather than ContainerID, because we use this map when reserving resources (during which we don't necessarily have the replica ID). In these cases, the value will be -1, which just indicates that we weren't able to record the specific replica.
	containersWithPreCommittedResources map[string]scheduling.KernelContainer               // containersWithPreCommittedResources keeps track of kernels for which resources were specifically pre-commited. Keys are container IDs.

	// lastSnapshot is the last HostResourceSnapshot to have been applied successfully to this Host.
	lastSnapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]

	// SubscriptionQuerier is used to query the oversubscription factor given the host's
	// subscription ratio and the Cluster's subscription ratio.
	SubscriptionQuerier SubscriptionQuerier

	indexUpdater IndexUpdater

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
func newHostForRestoration(localGatewayClient proto.LocalGatewayClient, confirmedId *proto.HostId,
	millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int) (*Host, error) {
	gpuInfoResp, gpuFetchError := localGatewayClient.GetActualGpuInfo(context.Background(), &proto.Void{})
	if gpuFetchError != nil {
		log.Printf(utils.RedStyle.Render("[ERROR] Failed to fetch latest GPU information from "+
			"existing+reconnecting Local Daemon %s (ID=%s)\n"), confirmedId.NodeName, confirmedId.Id)
		return nil, gpuFetchError
	}

	// Create the ResourceSpec defining the HostResources available on the Host.
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
func NewHost(id string, addr string, millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int,
	querier SubscriptionQuerier, indexUpdater IndexUpdater, metricsProvider scheduling.MetricsProvider,
	localGatewayClient proto.LocalGatewayClient, resourceBindingMode scheduling.ResourceBindingMode,
	errorCallback scheduling.ErrorCallback) (*Host, error) {

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

	// Create the ResourceSpec defining the HostResources available on the Host.
	resourceSpec := &types.DecimalSpec{
		GPUs:      decimal.NewFromFloat(float64(gpuInfoResp.SpecGPUs)),
		Millicpus: decimal.NewFromFloat(float64(millicpus)),
		MemoryMb:  decimal.NewFromFloat(float64(memMb)),
		VRam:      decimal.NewFromFloat(vramGb),
	}

	log.Printf("Registering brand new Local Daemon %s (ID=%s) with the following resource spec: %s.",
		confirmedId.NodeName, confirmedId.Id, resourceSpec.String())

	host := &Host{
		LocalGatewayClient:                  localGatewayClient,
		latestGpuInfo:                       gpuInfoResp,
		ID:                                  id,
		NodeName:                            confirmedId.NodeName,
		Addr:                                addr,
		resourceSpec:                        resourceSpec,
		numReplicasPerKernel:                numReplicasPerKernel,
		metricsProvider:                     metricsProvider,
		log:                                 config.GetLogger(fmt.Sprintf("Host %s ", id)),
		containers:                          hashmap.NewCornelkMap[string, scheduling.KernelContainer](5),
		reservations:                        hashmap.NewCornelkMap[string, *Reservation](5),
		trainingContainers:                  make([]scheduling.KernelContainer, 0, int(resourceSpec.GPU())),
		penalties:                           make([]cachedPenalty, int(resourceSpec.GPU())),
		seenSessions:                        make([]string, int(resourceSpec.GPU())),
		meta:                                hashmap.NewCornelkMap[string, interface{}](64),
		errorCallback:                       errorCallback,
		enabled:                             true,
		resourceBindingMode:                 resourceBindingMode,
		CreatedAt:                           time.Now(),
		SubscriptionQuerier:                 querier,
		kernelsWithCommittedResources:       make(map[string]int32),
		containersWithPreCommittedResources: make(map[string]scheduling.KernelContainer),
		indexUpdater:                        indexUpdater,
		ProperlyInitialized:                 true,
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
func NewHostWithConn(id string, addr string, millicpus int32, memMb int32, vramGb float64, numReplicasPerKernel int,
	querier SubscriptionQuerier, indexUpdater IndexUpdater, metricsProvider scheduling.MetricsProvider, conn *grpc.ClientConn,
	resourceBindingMode scheduling.ResourceBindingMode, errorCallback scheduling.ErrorCallback) (*Host, error) {

	// Create gRPC client.
	localGatewayClient := proto.NewLocalGatewayClient(conn)

	host, err := NewHost(id, addr, millicpus, memMb, vramGb, numReplicasPerKernel, querier,
		indexUpdater, metricsProvider, localGatewayClient, resourceBindingMode, errorCallback)
	if err != nil {
		// We need to return host here, in case the error is ErrRestoreRequired, as a host IS returned in that case.
		// It's a host with only some fields filled-in so that it can be used to restore the existing host.
		return host, err
	}

	// Populate the conn field "retroactively".
	host.conn = conn

	return host, nil
}

// GetGrpcConnection returns the underlying grpc.ClientConn used to communicate with the remote Local Daemon.
func (h *Host) GetGrpcConnection() *grpc.ClientConn {
	return h.conn
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

// GetIdx returns the target Host's heapIndex.
func (h *Host) GetIdx() int {
	return h.heapIndex
}

func (h *Host) Compare(h2 interface{}) float64 {
	switch h.schedulerPoolType {
	case scheduling.SchedulerPoolTypeUndersubscribed:
		// Max heap.
		switch v := h2.(type) {
		case float64:
			return h.IdleGPUs() - v // Seeking value, simply follow normal logic.
		}

		host2 := h2.(*Host)
		if h == host2 {
			return 0
		}

		ret := h2.(*Host).IdleGPUs() - h.IdleGPUs()

		// For the pool to provide all GPUs to one container, idle gpus are either 0 or all.
		if ret != 0.0 {
			return ret
		}

		diff := h.subscribedRatio.Sub(h2.(*Host).SubscribedRatioAsDecimal()).InexactFloat64()
		if diff != 0 {
			return diff
		}

		// For otherwise equal hosts, compare their IDs for stable ordering
		return float64(strings.Compare(h.ID, host2.ID))
	default:
		// SchedulerPoolTypeOversubscribed
		// Min heap.
		switch h2.(type) {
		case float64:
			log.Printf("Non-updated schedulerPoolType: host %s", h.ID)
		}

		diff := h.subscribedRatio.Sub(h2.(*Host).SubscribedRatioAsDecimal()).InexactFloat64()
		if diff != 0 {
			return diff
		}

		// For otherwise equal hosts, compare their IDs for stable ordering
		return float64(strings.Compare(h.ID, h2.(*Host).ID))
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

func (h *Host) CommittedResourcesAsString() string {
	return h.CommittedResources().String()
}

// ToVirtualDockerNode converts a Host struct to a proto.VirtualDockerNode struct and
// returns a pointer to the new proto.VirtualDockerNode.
func (h *Host) ToVirtualDockerNode() *proto.VirtualDockerNode {
	dockerContainers := make([]*proto.DockerContainer, 0, h.containers.Len())
	h.containers.Range(func(_ string, container scheduling.KernelContainer) (contd bool) {
		dockerContainers = append(dockerContainers, container.ToDockerContainer())
		return true
	})

	committed := h.resourceManager.CommittedResources()
	pending := h.resourceManager.PendingResources()

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
		AllocatedCpu:    float32(committed.Millicpus()),
		AllocatedMemory: float32(committed.MemoryMB()),
		AllocatedGpu:    float32(committed.GPUs()),
		AllocatedVRAM:   float32(committed.VRAM()),
		PendingCpu:      float32(pending.Millicpus()),
		PendingMemory:   float32(pending.MemoryMB()),
		PendingGpu:      float32(pending.GPUs()),
		PendingVRAM:     float32(pending.VRAM()),
		Enabled:         h.Enabled(),
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
// This method only checks against the Host's "spec" (i.e., the total HostResources available on the Host,
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
// HostResources available on the Host, not taking into account current resource allocations).
//
// CanServeContainer returns true when the Host could serve the hypothetical kernel and false when the Host could not.
func (h *Host) CanServeContainer(resourceRequest types.Spec) bool {
	return h.resourceManager.SpecResources().Validate(resourceRequest)
}

// CanCommitResources returns a boolean indicating whether this Host could commit the specified resource request
// to a kernel scheduled onto the Host right now. Commiting resource requires having sufficiently many idle HostResources
// available.
//
// CanCommitResources returns true if the Host could commit/reserve the given HostResources right now.
// Otherwise, CanCommitResources returns false.
func (h *Host) CanCommitResources(resourceRequest types.Spec) bool {
	return h.resourceManager.IdleResources().Validate(types.ToDecimalSpec(resourceRequest))
}

func (h *Host) releaseCommittedReservation(spec *proto.KernelSpec, reservation *Reservation) error {
	h.log.Debug("Releasing committed resources [%s] from reservation made for replica of kernel \"%s\". Current resources: %s.",
		spec.ResourceSpec.String(), spec.Id, h.GetResourceCountsAsString())
	err := h.unsafeUncommitResources(spec.DecimalSpecFromKernelSpec(), spec.Id, false)
	if err != nil {
		h.log.Error("Failed to release committed resource reservation for a replica of kernel %s: %v.",
			spec.Id, err)
		return err
	}

	h.log.Debug("Successfully released committed resources [%s] from reservation made for replica of kernel \"%s\". Updated resources: %s.",
		spec.ResourceSpec.String(), spec.Id, h.GetResourceCountsAsString())

	h.RecomputeSubscribedRatio()
	return nil
}

func (h *Host) releasePendingReservation(spec *proto.KernelSpec) error {
	err := h.subtractFromPendingResources(spec.DecimalSpecFromKernelSpec(), spec.Id, -1)
	if err != nil {
		h.log.Error("Failed to release reserved pending resources associated with replica of kernel \"%s\": %v",
			spec.Id, err)
		return err
	}

	h.log.Debug("Successfully released pending resources [%s] from reservation made for replica of kernel \"%s\". Updated resources: %s.",
		spec.ResourceSpec.String(), spec.Id, h.GetResourceCountsAsString())

	h.RecomputeSubscribedRatio()
	return nil
}

// ReleaseReservation is to be called when a resource reservation should be released because the
// scheduling of the associated replica of the associated kernel is being aborted.
func (h *Host) ReleaseReservation(spec *proto.KernelSpec) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	reservation, loadedReservation := h.reservations.LoadAndDelete(spec.Id)
	if !loadedReservation {
		h.log.Error("Cannot release resource reservation associated with kernel %s; no reservations found.", spec.Id)
		return fmt.Errorf("%w: kernel %s", ErrReservationNotFound, spec.Id)
	}

	// No longer being considered.
	h.isBeingConsideredForScheduling.Add(-1)

	if !reservation.CreatedUsingPendingResources {
		return h.releaseCommittedReservation(spec, reservation)
	}

	return h.releasePendingReservation(spec)
}

// ReserveResources attempts to reserve the resources required by the specified kernel, returning
// a boolean flag indicating whether the resource reservation was completed successfully.
//
// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
func (h *Host) ReserveResources(spec *proto.KernelSpec, usePendingResources bool) (bool, error) {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	h.log.Debug("Creating resource reservation for new replica of kernel \"%s\". UsePending=%v. Request=%s.",
		spec.Id, usePendingResources, spec.ResourceSpec.String())

	// Check if we're already hosting a replica of the target kernel.
	container, containerLoaded := h.containers.Load(spec.Id)
	if containerLoaded {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; already hosting replica %d of kernel %s.",
			spec.Id, container.ReplicaId(), spec.Id)
		return false, nil
	}

	// Check if there's already a reservation for some (not-yet-scheduled) replica of the target kernel.
	// TODO: If there's an error creating the container, we need to release the resource reservation on the Host.
	reservation, reservationLoaded := h.reservations.Load(spec.Id)
	if reservationLoaded {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; have existing reservation for that kernel created %v ago.",
			spec.Id, time.Since(reservation.CreationTimestamp))
		return false, nil
	}

	resourceSpec := spec.ResourceSpec.ToDecimalSpec()
	// Check if the Host could satisfy the resource request for the target kernel.
	if !h.CanServeContainer(resourceSpec) {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s. Kernel is requesting more resources than we have allocatable.",
			spec.Id)
		return false, nil
	}

	if h.WillBecomeTooOversubscribed(resourceSpec) {
		h.log.Debug("Cannot reserve resources for a replica of kernel %s; host would become too oversubscribed.",
			spec.Id)
		return false, nil
	}

	// If we're going to need to commit the resources, then we should check if the host can do that before
	// bothering with the pending reservation (that we'll subsequently upgrade to a committed reservation).
	if !usePendingResources && !h.CanCommitResources(resourceSpec) {
		h.log.Debug("Cannot commit resources for a replica of kernel %s; insufficient idle resources available.",
			spec.Id)
		return false, nil
	}

	var err error
	if usePendingResources {
		err = h.addPendingResources(resourceSpec, spec.Id, -1)
	} else {
		err = h.unsafeCommitResources(resourceSpec, spec.Id, -1, false)
	}

	if err != nil {
		h.log.Debug("Failed to create resource reservation for new replica of kernel %s because: %v [usePendingResources=%v]",
			spec.Id, err, usePendingResources)

		return false, nil // Not an actual error, just didn't have enough resources available.
	}

	oldSubscribedRatio := h.subscribedRatio
	h.RecomputeSubscribedRatio()
	h.log.Debug("Successfully reserved resources for new replica of kernel %s. Old subscription ratio: %s. New subscription ratio: %s.",
		spec.Id, oldSubscribedRatio.StringFixed(3), h.subscribedRatio.StringFixed(3))
	h.reservations.Store(spec.Id, NewReservation(h.ID, spec.Id, time.Now(), usePendingResources, spec.DecimalSpecFromKernelSpec()))

	return true, nil
}

// GetReservation returns the scheduling.ResourceReservation associated with the specified kernel, if one exists.
func (h *Host) GetReservation(kernelId string) (scheduling.ResourceReservation, bool) {
	return h.reservations.Load(kernelId)
}

func (h *Host) GetResourceSpec() types.Spec {
	return h.resourceSpec
}

func (h *Host) GetLatestGpuInfo() *proto.GpuInfo {
	return h.latestGpuInfo
}

func (h *Host) GetLocalGatewayClient() proto.LocalGatewayClient {
	return h.LocalGatewayClient
}

func (h *Host) GetAddress() string {
	return h.Addr
}

// Restore restores the state of a Host from another Host.
func (h *Host) Restore(restoreFrom scheduling.Host, callback scheduling.ErrorCallback) error {
	h.SetErrorCallback(callback)
	h.LocalGatewayClient = restoreFrom.GetLocalGatewayClient()
	h.resourceSpec = types.ToDecimalSpec(restoreFrom.GetResourceSpec())
	h.ID = restoreFrom.GetID()
	h.NodeName = restoreFrom.GetNodeName()
	h.latestGpuInfo = restoreFrom.GetLatestGpuInfo()

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
func (h *Host) doContainerRemovedResourceUpdate(container scheduling.KernelContainer) error {
	if h.resourceBindingMode == scheduling.BindResourcesWhenContainerScheduled {
		h.log.Debug("Releasing committed resources [%s] from replica %d of kernel \"%s\" during eviction process. Current resources: %s.",
			container.ResourceSpec().String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())
		err := h.unsafeUncommitResources(container.ResourceSpec(), container.KernelID(), true)
		if err != nil {
			h.log.Error("Failed to release committed resources %s from container for replica %d of kernel %s during eviction process: %v",
				container.ResourceSpec().String(), container.ReplicaId(), container.KernelID(), err)

			err2 := h.unsafeHandleResourceError()
			if err2 != nil {
				err = errors.Join(err, err2)
			}

			return err
		} else {
			h.log.Debug("Successfully released committed resources [%s] from replica %d of kernel \"%s\" during eviction process. Updated resources: %s.",
				container.ResourceSpec().String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())
		}
	}

	err := h.subtractFromPendingResources(container.ResourceSpec(), container.KernelID(), container.ReplicaId())
	if err != nil {
		err2 := h.unsafeHandleResourceError()
		if err2 != nil {
			err = errors.Join(err, err2)
		}

		return err
	}
	return nil
}

// ContainerStoppedTraining is to be called when a Container stops training on a Host.
func (h *Host) ContainerStoppedTraining(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot find container for replica %d of kernel %s on host %s (ID=%s).",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return ErrInvalidContainer
	}

	// If the resource binding mode is instead BindResourcesWhenContainerScheduled, then we do not
	// uncommit the resources until the container is actually evicted.
	if h.resourceBindingMode == scheduling.BindResourcesAtTrainingStart {
		spec := container.ResourceSpec()

		h.log.Debug("Releasing committed resources [%s] from replica %d of kernel \"%s\". Current resources: %s.",
			spec.String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())

		err := h.unsafeUncommitResources(spec, container.KernelID(), true)
		if err != nil {
			h.log.Error("Failed to deallocate resources from previously-training replica %d of kernel %s: %v",
				container.ReplicaId(), container.KernelID(), err)
			err2 := h.unsafeHandleResourceError()
			if err2 != nil {
				// If we encountered ANOTHER error while trying to rebuild our resource information, then
				// we'll just join them together and return them both.
				err = errors.Join(err2)
			}
		} else {
			h.log.Debug("Released committed resources [%s] from replica %d of kernel \"%s\". Updated resources: %s.",
				spec.String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())
		}

		if _, loaded := h.containersWithPreCommittedResources[container.ContainerID()]; loaded {
			h.log.Debug("Removing 'pre-committed resources' record for replica %d of kernel \"%s\" now that it is done training.",
				container.ReplicaId(), container.KernelID())

			delete(h.containersWithPreCommittedResources, container.ContainerID())
		}

		return err // Will be nil if nothing bad happened when un-committing the resources.
	}

	return nil
}

func (h *Host) unsafeHandleResourceError() error {
	h.log.Warn("Recomputing all resource quantities on host %s", h.ID)

	// Recompute allocated resources.
	idle := h.resourceSpec.CloneDecimalSpec()
	pending := types.NewDecimalSpec(0, 0, 0, 0)
	committed := types.NewDecimalSpec(0, 0, 0, 0)

	h.containers.Range(func(containerId string, container scheduling.KernelContainer) bool {
		containerSpec := types.ToDecimalSpec(container.ResourceSpec())

		_, containerHasPreCommittedResources := h.containersWithPreCommittedResources[container.ContainerID()]

		if containerHasPreCommittedResources || container.IsTraining() {
			committed = types.ToDecimalSpec(committed.Add(containerSpec))
			idle = idle.Subtract(containerSpec)
		} else {
			pending = types.ToDecimalSpec(pending.Add(containerSpec))
		}

		return true
	})

	h.resourceManager.IdleResources().SetTo(idle)
	h.resourceManager.PendingResources().SetTo(pending)
	h.resourceManager.CommittedResources().SetTo(committed)

	h.log.Debug("Recomputed resources: %s", h.GetResourceCountsAsString())

	return nil
}

func (h *Host) IsProperlyInitialized() bool {
	return h.ProperlyInitialized
}

// ContainerStartedTraining is to be called when a Container begins training on a Host.
func (h *Host) ContainerStartedTraining(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot find container for replica %d of kernel %s on host %s (ID=%s).",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return ErrInvalidContainer
	}

	// If the resource binding mode is instead BindResourcesWhenContainerScheduled, then they're already
	// committed to the container, and so we don't have to do anything else and can just return nil,
	// as we do below.
	if h.resourceBindingMode == scheduling.BindResourcesAtTrainingStart {
		if _, loaded := h.kernelsWithCommittedResources[container.KernelID()]; loaded {
			h.log.Debug("Resources are already committed to replica %d of kernel \"%s\" upon training start. Must have been migrated recently.",
				container.ReplicaId(), container.KernelID())

			return nil
		}

		h.log.Debug("Committing resources %v to container for replica %d of kernel \"%s\" so it can train.",
			container.ResourceSpec().String(), container.ReplicaId(), container.KernelID())

		return h.unsafeCommitResources(container.ResourceSpec(), container.KernelID(), container.ReplicaId(), true)
	}

	return nil
}

func (h *Host) unsafeUncommitResources(spec *types.DecimalSpec, kernelId string, incrementPending bool) error {
	if _, loaded := h.kernelsWithCommittedResources[kernelId]; !loaded {
		h.log.Error("Cannot release committed resources from replica of kernel \"%s\". No replica of kernel \"%s\" has resources committed to it. (Requested to release: %s)",
			kernelId, kernelId, spec.String())
		return ErrInvalidContainer
	}

	err := h.resourceManager.RunTransaction(func(state *transaction.State) {
		state.CommittedResources().Subtract(spec)

		if incrementPending {
			state.PendingResources().Add(spec)
		}

		state.IdleResources().Add(spec)
	})

	if err != nil {
		h.log.Error("Failed to release committed resources [%s] from replica of kernel %s.",
			spec.String(), kernelId, kernelId)
		return err
	}

	h.indexUpdater.UpdateIndex(h)

	delete(h.kernelsWithCommittedResources, kernelId)
	return nil
}

// unsafeUncommitResources releases the specified resources and returns nil on success.
// unsafeUncommitResources is not thread safe and should only be called with the schedulingMutex already held.
func (h *Host) unsafeUncommitResourcesOld(spec *types.DecimalSpec, kernelId string) error {
	if _, loaded := h.kernelsWithCommittedResources[kernelId]; !loaded {
		h.log.Error("Cannot release committed resources from replica of kernel \"%s\". No replica of kernel \"%s\" has resources committed to it. (Requested to release: %s)",
			kernelId, kernelId, spec.String())
		return ErrInvalidContainer
	}

	if err := h.resourceManager.CommittedResources().Subtract(spec); err != nil {
		return err
	}

	if err := h.resourceManager.PendingResources().Add(spec); err != nil {
		return err
	}

	if err := h.resourceManager.IdleResources().Add(spec); err != nil {
		return err
	}

	delete(h.kernelsWithCommittedResources, kernelId)

	return nil
}

// PreCommitResources commits resources to the given KernelContainer.
//
// The specified KernelContainer must already be scheduled on the Host.
//
// This method is intended to be used when processing an "execute_request" that is about to be forwarded to
// the Local Schedulers of the kernel replicas. The resources need to be pre-allocated to the KernelContainer
// instances in case one of them wins.
//
// The resources will be released from the KernelContainer upon receiving an "execute_reply" indicating that a
// particular KernelReplica yielded, or after the KernelContainer finishes executing the code in the event that
// it wins its leader election.
//
// PreCommitResources is the inverse/counterpart to ReleasePreCommitedResources.
func (h *Host) PreCommitResources(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, loaded := h.containers.Load(container.ContainerID()); !loaded {
		h.log.Error("Cannot pre-commit resources to container of replica %d of kernel \"%s\"; that container is not scheduled onto this node.",
			container.ReplicaId(), container.KernelID())
		return ErrInvalidContainer
	}

	if _, loaded := h.kernelsWithCommittedResources[container.KernelID()]; loaded {
		h.log.Debug("Resources are already commited to replica %d of kernel \"%s\". No need to pre-commit them.",
			container.ReplicaId(), container.KernelID())
		return nil
	}

	if _, loaded := h.containersWithPreCommittedResources[container.ContainerID()]; loaded {
		h.log.Debug("Resources are already pre-commited to replica %d of kernel \"%s\". No need to pre-commit them.",
			container.ReplicaId(), container.KernelID())
		return nil
	}

	h.log.Debug("Pre-Committing resources [%v] to replica %d of kernel \"%s\" so that it can potentially train.",
		container.ResourceSpec().String(), container.ReplicaId(), container.KernelID())

	err := h.unsafeCommitResources(container.ResourceSpec(), container.KernelID(), container.ReplicaId(), true)
	if err != nil {
		return err
	}

	h.containersWithPreCommittedResources[container.ContainerID()] = container
	h.log.Debug("Pre-Committed the following resources to replica %d of kernel \"%s\": %s. Updated resources on host: %s.",
		container.ReplicaId(), container.KernelID(), container.ResourceSpec().String(), h.GetResourceCountsAsString())
	return nil
}

// GetResourceCountsAsString returns the current resource counts of the Host as a string and is useful for printing.
func (h *Host) GetResourceCountsAsString() string {
	return h.resourceManager.GetResourceCountsAsString()
}

// ReleasePreCommitedResources releases resources that were pre-committed to the given KernelContainer.
//
// ReleasePreCommitedResources is the inverse/counterpart to PreCommitResources.
func (h *Host) ReleasePreCommitedResources(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, loaded := h.containersWithPreCommittedResources[container.ContainerID()]; !loaded {
		h.log.Warn("Resources were not pre-commited to replica %d of kernel \"%s\".",
			container.ReplicaId(), container.KernelID())
		return ErrInvalidContainer
	}

	spec := container.ResourceSpec()
	err := h.unsafeUncommitResources(spec, container.KernelID(), true)
	if err != nil {
		h.log.Error("Failed to release pre-committed resources (%s) from replica %d of kernel \"%s\": %v",
			spec.String(), container.ReplicaId(), container.KernelID(), err)
		return err
	}

	delete(h.containersWithPreCommittedResources, container.ContainerID())
	h.log.Debug("Released pre-committed resources [%s] from replica %d of kernel \"%s\". Updated resource counts: %s.",
		spec.String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())

	return nil
}

func (h *Host) unsafeCommitResources(spec *types.DecimalSpec, kernelId string, replicaId int32, decrementPending bool) (err error) {
	if existingReplicaId, loaded := h.kernelsWithCommittedResources[kernelId]; loaded {
		h.log.Error("Attempting to commit resources [%s] to replica %d of kernel %s, but we've already committed resources to replica %d of kernel %s.",
			spec.String(), replicaId, kernelId, existingReplicaId)
		return fmt.Errorf("%w (replica %d of kernel \"%s\")", ErrResourcesAlreadyCommitted, existingReplicaId, kernelId)
	}

	err = h.resourceManager.RunTransaction(func(state *transaction.State) {
		state.CommittedResources().Add(spec)

		if decrementPending {
			state.PendingResources().Subtract(spec)
		}

		state.IdleResources().Subtract(spec)
	})

	if err != nil {
		h.log.Warn("Failed to commit resources [%s] to replica %d of kernel %s: %v",
			spec.String(), replicaId, kernelId, err)
		return
	}

	h.indexUpdater.UpdateIndex(h)

	h.kernelsWithCommittedResources[kernelId] = replicaId
	return
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot remove specified Container from Host. Container is not on specified Host.")
		return ErrInvalidContainer
	}

	h.containers.Delete(container.ContainerID())

	h.pendingContainers.Sub(1)

	err := h.doContainerRemovedResourceUpdate(container)
	if err != nil {
		h.log.Error("Error while updating resources of host while evicting container %s: %v",
			container.ContainerID(), err)

		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// ContainerScheduled is to be called when a Container is scheduled onto the Host.
func (h *Host) ContainerScheduled(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	h.containers.Store(container.ContainerID(), container)
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
		container.ContainerID(), h.ID, time.Since(reservation.CreationTimestamp))

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
	numGPUs := sess.ResourceSpec().GPU()

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

// KernelAdjustedItsResourceRequestCoordinated when the ResourceSpec of a KernelContainer that is already scheduled on
// this Host is updated or changed. This ensures that the Host's resource counts are up to date.
//
// This version runs in a coordination fashion and is used when updating the resources of multi-replica kernels.
func (h *Host) KernelAdjustedItsResourceRequestCoordinated(updatedSpec types.Spec, oldSpec types.Spec, container scheduling.KernelContainer, coordinatedTransaction *transaction.CoordinatedTransaction) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	// Sanity check.
	if _, loaded := h.containers.Load(container.ContainerID()); !loaded {
		return fmt.Errorf("the specified KernelContainer is not running on the target Host")
	}

	// Ensure that we're even allowed to do this (based on the scheduling policy).
	if h.resourceBindingMode == scheduling.BindResourcesWhenContainerScheduled {
		return scheduling.ErrDynamicResourceAdjustmentProhibited
	}

	oldSubscribedRatio := h.subscribedRatio
	h.log.Debug("Updating resource reservation for %s", container.ContainerID())

	oldSpecDecimal := types.ToDecimalSpec(oldSpec)
	newSpecDecimal := types.ToDecimalSpec(updatedSpec)

	initialState, commit := h.resourceManager.GetTransactionData()

	err := coordinatedTransaction.RegisterParticipant(container.ReplicaId(), initialState,
		func(state *transaction.State) {
			state.PendingResources().Subtract(oldSpecDecimal)
			state.PendingResources().Add(newSpecDecimal)
		}, commit)
	if err != nil {
		h.log.Error("Received error upon registering for coordination transaction when updating spec of replica %d of kernel %s from [%s] to [%s]: %v",
			container.ReplicaId(), container.KernelID(), oldSpec.String(), updatedSpec.String(), err)
		return err
	}

	succeeded := coordinatedTransaction.Wait()
	if succeeded {
		h.RecomputeSubscribedRatio()
		h.log.Debug("Successfully updated ResourceRequest for replica %d of kernel %s. Old subscription ratio: %s. New subscription ratio: %s.",
			container.ReplicaId(), container.KernelID(), oldSubscribedRatio.StringFixed(3), h.subscribedRatio.StringFixed(3))
		return nil
	}

	h.log.Debug("Failed to update ResourceRequest for replica %d of kernel %s (possibly because of another replica).",
		container.ReplicaId(), container.KernelID())

	return nil
}

// KernelAdjustedItsResourceRequest when the ResourceSpec of a KernelContainer that is already scheduled on this
// Host is updated or changed. This ensures that the Host's resource counts are up to date.
func (h *Host) KernelAdjustedItsResourceRequest(updatedSpec types.Spec, oldSpec types.Spec, container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	// Sanity check.
	if _, loaded := h.containers.Load(container.ContainerID()); !loaded {
		return fmt.Errorf("the specified KernelContainer is not running on the target Host")
	}

	// Ensure that we're even allowed to do this (based on the scheduling policy).
	if h.resourceBindingMode == scheduling.BindResourcesWhenContainerScheduled {
		return scheduling.ErrDynamicResourceAdjustmentProhibited
	}

	oldSubscribedRatio := h.subscribedRatio
	h.log.Debug("Updating resource reservation for %s", container.ContainerID())

	oldSpecDecimal := types.ToDecimalSpec(oldSpec)
	newSpecDecimal := types.ToDecimalSpec(updatedSpec)

	err := h.resourceManager.RunTransaction(func(state *transaction.State) {
		state.PendingResources().Subtract(oldSpecDecimal)
		state.PendingResources().Add(newSpecDecimal)
	})

	if err != nil {
		h.log.Warn("Resource update failed for replica %d of kernel \"%s\": %v",
			container.ReplicaId(), container.KernelID(), err)

		return err
	}

	h.RecomputeSubscribedRatio()
	h.log.Debug("Successfully updated ResourceRequest for replica %d of kernel %s. Old subscription ratio: %s. New subscription ratio: %s.",
		container.ReplicaId(), container.KernelID(), oldSubscribedRatio.StringFixed(3), h.subscribedRatio.StringFixed(3))

	return nil
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

// TimeSinceLastSynchronizationWithRemote returns a time.Duration indicating how long it has been since its HostResources
// were refreshed and synchronized from the actual remote Host that this Host struct represents.
func (h *Host) TimeSinceLastSynchronizationWithRemote() time.Duration {
	return time.Since(h.LastRemoteSync)
}

// SetMeta sets the metadata of the host.
func (h *Host) SetMeta(key types.HeapElementMetadataKey, value interface{}) {
	h.meta.Store(string(key), value)
}

// GetMeta return the metadata of the host.
func (h *Host) GetMeta(key types.HeapElementMetadataKey) interface{} {
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

// ResourceSpec the types.Spec defining the HostResources available on the Host.
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

// CommittedResources returns a types.Spec encapsulating the idle HostResources on the Host.
func (h *Host) CommittedResources() *types.DecimalSpec {
	return h.resourceManager.CommittedResources().ToDecimalSpec()
}

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}

// AddToPendingResources is only meant to be used during unit tests.
func (h *Host) AddToPendingResources(spec *types.DecimalSpec) error {
	h.log.Debug("Incrementing pending resources by [%v]. Current pending: %s.",
		spec.String(), h.resourceManager.PendingResources().String())
	err := h.resourceManager.PendingResources().Add(spec)

	if err != nil {
		h.log.Debug("Failed to increment pending resources by [%v]. Current pending: %s.",
			spec.String(), h.resourceManager.PendingResources().String())
		return err
	}

	h.log.Debug("Successfully incremented pending resources by [%v]. Current pending: %s.",
		spec.String(), h.resourceManager.PendingResources().String())

	h.RecomputeSubscribedRatio()
	return nil
}

// subtractFromPendingResources is a utility function that subtracts the given resources from the Host's pending
// resource quantity. subtractFromPendingResources prints before and after so we don't have to do that anywhere else.
func (h *Host) subtractFromPendingResources(spec *types.DecimalSpec, kernelId string, containerId int32) error {
	h.log.Debug(utils.DecrementPendingStyle.Render("Decrementing pending resources by [%v] for replica %d of kernel \"%s\". Current pending: %s."),
		spec.String(), containerId, kernelId, h.resourceManager.PendingResources().String())

	err := h.resourceManager.PendingResources().Subtract(spec)

	if err != nil {
		h.log.Debug(utils.LightOrangeStyle.Render("Failed to decrement pending resources by [%v] for replica %d of kernel \"%s\" because: %v"),
			spec.String(), containerId, kernelId, err)
		return err
	}

	h.log.Debug(utils.DecrementPendingStyle.Render("Successfully decremented pending resources by [%v] for replica %d of kernel \"%s\". Current pending: %s."),
		spec.String(), containerId, kernelId, h.resourceManager.PendingResources().String())
	return nil
}

// addPendingResources is a utility function that adds resources to the Host's pending resource quantity.
// It prints before and after so we don't have to do that anywhere else.
func (h *Host) addPendingResources(spec *types.DecimalSpec, kernelId string, containerId int32) error {
	h.log.Debug(utils.IncrementPendingStyle.Render("Incrementing pending resources by [%v] for replica %d of kernel \"%s\". Current pending: %s."),
		spec.String(), containerId, kernelId, h.resourceManager.PendingResources().String())

	err := h.resourceManager.PendingResources().Add(spec)

	if err != nil {
		h.log.Warn(utils.LightOrangeStyle.Render("Failed to increment pending resources by [%v] for replica %d of kernel \"%s\" because: %v"),
			spec.String(), containerId, kernelId, err)
		return err
	}

	h.log.Debug(utils.IncrementPendingStyle.Render("Successfully incremented pending resources by [%v] for replica %d of kernel \"%s\". Current pending: %s."),
		spec.String(), containerId, kernelId, h.resourceManager.PendingResources().String())
	return nil
}

// GetCreatedAt returns the time at which the Host was created.
func (h *Host) GetCreatedAt() time.Time {
	return h.CreatedAt
}

// AddToCommittedResources is only intended to be used during unit tests.
func (h *Host) AddToCommittedResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.CommittedResources().Add(spec)
	h.RecomputeSubscribedRatio()
	return err
}

// SubtractFromIdleResources is only intended to be used during unit tests.
func (h *Host) SubtractFromIdleResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.IdleResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}

// AddToIdleResources is only intended to be used during unit tests.
func (h *Host) AddToIdleResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.IdleResources().Add(spec)
	h.RecomputeSubscribedRatio()
	return err
}

// SubtractFromCommittedResources is only intended to be used during unit tests.
func (h *Host) SubtractFromCommittedResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.CommittedResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}

func (h *Host) SubtractFromPendingResources(spec *types.DecimalSpec) error {
	err := h.resourceManager.PendingResources().Subtract(spec)
	h.RecomputeSubscribedRatio()
	return err
}
