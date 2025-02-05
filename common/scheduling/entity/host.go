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
	ErrWillOversubscribe                = errors.New("cannot reserve or allocate requested resources: host will become too oversubscribed")
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

// containerWithPreCommittedResources encapsulates a scheduling.KernelContainer and the "msg_id" of the Jupyter
// "execute_request" message that contained the user-submitted code associated with this pre-allocation.
type containerWithPreCommittedResources struct {
	scheduling.KernelContainer

	PreCommittedResources *types.DecimalSpec

	// ExecutionId is the "msg_id" of the Jupyter "execute_request" message that contained the
	// user-submitted code associated with this pre-allocation.
	ExecutionId  string
	AllocationId string
}

type containerWithCommittedResources struct {
	CommittedAt        time.Time
	ResourcesCommitted types.Spec
	AllocationId       string
	KernelId           string
	ReplicaId          int32
}

type Host struct {

	// Cached penalties
	sip            cache.InlineCache // Scale-in penalty.
	penaltyList    cache.InlineCache
	CreatedAt      time.Time // CreatedAt is the time at which the Host was created.
	LastRemoteSync time.Time // lastRemoteSync is the time at which the Host last synchronized its resource counts with the actual remote node that the Host represents.
	proto.LocalGatewayClient

	log logger.Logger

	allocationManager scheduling.AllocationManager         // allocationManager manages the resources of the Host.
	meta              hashmap.HashMap[string, interface{}] // meta is a map of metadata.
	metricsProvider   scheduling.MetricsProvider           // Provides access to metrics relevant to the Host.
	indexUpdater      IndexUpdater
	schedulingPolicy  scheduling.Policy // schedulingPolicy is the scheduling policy configured for the cluster.

	sipSession scheduling.UserSession // Scale-in penalty session.

	// containers is a map from kernel ID to the container from that kernel scheduled on this Host.
	containers hashmap.HashMap[string, scheduling.KernelContainer]

	// SubscriptionQuerier is used to query the over-subscription factor given the host's
	// subscription ratio and the Cluster's subscription ratio.
	SubscriptionQuerier         SubscriptionQuerier
	conn                        *grpc.ClientConn         // conn is the gRPC connection to the Host.
	resourceSpec                *types.DecimalSpec       // resourceSpec is the spec describing the total HostResources available on the Host, not impacted by allocations.
	errorCallback               scheduling.ErrorCallback // errorCallback is a function to be called if a Host appears to be dead.
	HeapIndexes                 map[types.HeapElementMetadataKey]int
	Addr                        string          // Addr is the Host's address.
	NodeName                    string          // NodeName is the Host's name (for printing/logging).
	ID                          string          // ID is the unique ID of this host.
	numReplicasPerKernelDecimal decimal.Decimal // numReplicasPerKernelDecimal is a cached decimal.Decimal of numReplicasPerKernel.
	subscribedRatio             decimal.Decimal
	seenSessions                []string // seenSessions are the sessions that have been scheduled onto this host at least once.
	penalties                   []cachedPenalty

	// trainingContainers are the actively-training kernel replicas.
	trainingContainers []scheduling.KernelContainer

	lastReschedule       types.StatFloat64 // lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	numReplicasPerKernel int               // The number of replicas per kernel.
	schedulerPoolType    scheduling.SchedulerPoolType

	schedulingMutex                sync.Mutex // schedulingMutex ensures that only a single kernel is scheduled at a time, to prevent over-allocating HostResources on the Host.
	heapIndexesMutex               sync.Mutex
	pendingContainers              types.StatInt32 // pendingContainers is the number of Containers that are scheduled on the host.
	excludedFromScheduling         atomic.Bool     // ExcludedFromScheduling is a flag that, when true, indicates that the Host should not be considered for scheduling operations at this time.
	isBeingConsideredForScheduling atomic.Int32    // IsBeingConsideredForScheduling indicates that the host has been selected as a candidate for scheduling when the value is > 0. The value is how many concurrent scheduling operations are considering this Host.
	enabled                        bool            // enabled indicates whether the Host is currently enabled and able to serve kernels. This is part of an abstraction to simulate dynamically changing the number of nodes in the cluster.
	isContainedWithinIndex         bool            // isContainedWithinIndex indicates whether this Host is currently contained within a valid ClusterIndex.
	ProperlyInitialized            bool            // Indicates whether this Host was created with all the necessary fields or not. This doesn't happen when we're restoring an existing Host (i.e., we create a Host struct with many fields missing in that scenario).
	penaltyValidity                bool
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
//
// Old parameters: (millicpus int32, memMb int32, vramGb float64)
func newHostForRestoration(localGatewayClient proto.LocalGatewayClient, confirmedId *proto.HostId, numReplicasPerKernel int) (*Host, error) {
	infoResponse, infoErr := localGatewayClient.GetLocalDaemonInfo(context.Background(), &proto.Void{})
	if infoErr != nil {
		log.Printf(utils.RedStyle.Render("[ERROR] Failed to fetch latest information from "+
			"existing+reconnecting Local Daemon %s (ID=%s)\n"), confirmedId.NodeName, confirmedId.Id)
		return nil, infoErr
	}

	// Create a Host struct populated with a few key fields.
	// This Host will be used to "restore" the existing Host struct.
	// That is, the existing Host struct will replace its values for these fields
	// with the values of this new Host struct.
	//
	// The most important is probably the LocalGatewayClient, as that ensures that the
	// existing Host struct has a new, valid connection to the remote Local Daemon.
	host := &Host{
		ID:                          confirmedId.Id,
		resourceSpec:                infoResponse.SpecResources.ToDecimalSpec(),
		NodeName:                    confirmedId.NodeName,
		LocalGatewayClient:          localGatewayClient,
		ProperlyInitialized:         false,
		numReplicasPerKernel:        numReplicasPerKernel,
		numReplicasPerKernelDecimal: decimal.NewFromFloat(float64(numReplicasPerKernel)),
	}

	return host, ErrRestoreRequired
}

// NewHost creates and returns a new *Host.
//
// If NewHost is called directly, then the conn field of the Host will not be populated. To populate this field,
// call NewHostWithConn instead.
func NewHost(id string, addr string, numReplicasPerKernel int, querier SubscriptionQuerier, indexUpdater IndexUpdater,
	metricsProvider scheduling.MetricsProvider, localGatewayClient proto.LocalGatewayClient,
	schedulingPolicy scheduling.Policy, errorCallback scheduling.ErrorCallback) (*Host, error) {

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
		return newHostForRestoration(localGatewayClient, confirmedId, numReplicasPerKernel) // millicpus, memMb, vramGb,
	}

	// Create the ResourceSpec defining the HostResources available on the Host.
	resourceSpec := confirmedId.SpecResources.ToDecimalSpec()

	host := &Host{
		LocalGatewayClient:          localGatewayClient,
		ID:                          id,
		NodeName:                    confirmedId.NodeName,
		Addr:                        addr,
		resourceSpec:                resourceSpec,
		numReplicasPerKernel:        numReplicasPerKernel,
		numReplicasPerKernelDecimal: decimal.NewFromFloat(float64(numReplicasPerKernel)),
		metricsProvider:             metricsProvider,
		log:                         config.GetLogger(fmt.Sprintf("Host %s ", confirmedId.NodeName)),
		containers:                  hashmap.NewCornelkMap[string, scheduling.KernelContainer](5),
		trainingContainers:          make([]scheduling.KernelContainer, 0, int(resourceSpec.GPU())),
		penalties:                   make([]cachedPenalty, int(resourceSpec.GPU())),
		seenSessions:                make([]string, int(resourceSpec.GPU())),
		meta:                        hashmap.NewCornelkMap[string, interface{}](64),
		errorCallback:               errorCallback,
		enabled:                     true,
		schedulingPolicy:            schedulingPolicy,
		CreatedAt:                   time.Now(),
		SubscriptionQuerier:         querier,
		indexUpdater:                indexUpdater,
		ProperlyInitialized:         true,
		allocationManager:           resource.NewAllocationManager(resourceSpec, schedulingPolicy, id, confirmedId.NodeName),
		subscribedRatio:             decimal.Zero,
	}

	host.log.Debug("Registering brand new Local Daemon %s (ID=%s) with the following resource spec: %s.",
		confirmedId.NodeName, confirmedId.Id, resourceSpec.String())

	host.sip.Producer = cache.FormalizeICProducer(host.getSIP)
	host.sip.Validator = GetClockTimeCacheValidator()
	host.penaltyList.Producer = cache.FormalizeChainedICProducer(host.updatePenaltyList)
	host.penaltyList.Validator = host.validatePenaltyList

	host.allocationManager.SetUpdateSubscriptionRatio(host.RecomputeSubscribedRatio)
	host.allocationManager.SetUpdateIndex(func(replicaId int32, kernelId string) error {
		err = host.indexUpdater.UpdateIndex(host)
		if err != nil {
			host.log.Error("Failed to update index for host %s after committing resources to replica %d of kernel %s: %v",
				host.NodeName, replicaId, kernelId, err)

			return err
		}

		return nil
	})

	return host, nil
}

// NewHostWithConn creates and returns a new *Host.
func NewHostWithConn(id string, addr string, numReplicasPerKernel int, querier SubscriptionQuerier,
	indexUpdater IndexUpdater, metricsProvider scheduling.MetricsProvider, conn *grpc.ClientConn,
	schedulingPolicy scheduling.Policy, errorCallback scheduling.ErrorCallback) (*Host, error) {

	// Create gRPC client.
	localGatewayClient := proto.NewLocalGatewayClient(conn)

	host, err := NewHost(id, addr, numReplicasPerKernel, querier, indexUpdater, metricsProvider, localGatewayClient, schedulingPolicy, errorCallback)
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
	return h.excludedFromScheduling.Load()
}

func (h *Host) SetSubscriptionQuerier(querier SubscriptionQuerier) {
	h.SubscriptionQuerier = querier
}

// ExcludeFromScheduling attempts to exclude this Host from being considered for scheduling operations.
//
// ExcludeFromScheduling will return true if the Host was successfully excluded.
//
// If ExcludeFromScheduling returns false, then the Host is already being considered for scheduling by one or more
// scheduling operations and thus cannot be excluded at this time.
func (h *Host) ExcludeFromScheduling() bool {
	if numOperationsConsideringHost := h.isBeingConsideredForScheduling.Load(); numOperationsConsideringHost > 0 {
		h.log.Debug("Host %s (ID=%s) cannot be excluded from consideration from scheduling operations as it is "+
			"already being considered by %d scheduling operation(s).", h.NodeName, h.ID, numOperationsConsideringHost)
		return false
	}

	h.log.Debug("Host %s (ID=%s) is now precluded from being considered for scheduling.", h.NodeName, h.ID)
	h.excludedFromScheduling.Store(true)
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

	if !h.excludedFromScheduling.Load() {
		return ErrHostAlreadyIncludedForScheduling
	}

	h.excludedFromScheduling.Store(false)
	h.log.Debug("Host %s (ID=%s) will be included for consideration in scheduling operations again.", h.NodeName, h.ID)
	return nil
}

// NumActiveSchedulingOperations returns the number of scheduling operations in which the target Host
// is presently being considered.
func (h *Host) NumActiveSchedulingOperations() int32 {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	return h.isBeingConsideredForScheduling.Load()
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

	if h.excludedFromScheduling.Load() {
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
func (h *Host) SetIdx(key types.HeapElementMetadataKey, idx int) {
	h.heapIndexesMutex.Lock()
	defer h.heapIndexesMutex.Unlock()

	if h.HeapIndexes == nil {
		h.HeapIndexes = make(map[types.HeapElementMetadataKey]int)
	}

	h.HeapIndexes[key] = idx

	//h.log.Debug("Updated heap index \"%s\": %d", key, idx)
}

// GetIdx returns the target Host's heapIndex.
func (h *Host) GetIdx(key types.HeapElementMetadataKey) int {
	h.heapIndexesMutex.Lock()
	defer h.heapIndexesMutex.Unlock()

	if h.HeapIndexes == nil {
		h.HeapIndexes = make(map[types.HeapElementMetadataKey]int)
		return -1
	}

	idx, loaded := h.HeapIndexes[key]
	if loaded {
		return idx
	}

	return -1
}

func (h *Host) Compare(h2 interface{}) float64 {
	switch h.schedulerPoolType {
	case scheduling.SchedulerPoolTypeUndersubscribed:
		// Max heap.
		switch v := h2.(type) {
		case float64:
			return h.IdleGPUs() - v // Seeking value, simply follow normal logic.
		}

		host2 := h2.(scheduling.Host)
		if h == host2 {
			return 0
		}

		ret := h2.(scheduling.Host).IdleGPUs() - h.IdleGPUs()

		// For the pool to provide all GPUs to one container, idle gpus are either 0 or all.
		if ret != 0.0 {
			return ret
		}

		diff := h.subscribedRatio.Sub(h2.(scheduling.Host).SubscribedRatioAsDecimal()).InexactFloat64()
		if diff != 0 {
			return diff
		}

		// For otherwise equal hosts, compare their IDs for stable ordering
		return float64(strings.Compare(h.ID, host2.GetID()))
	default:
		// SchedulerPoolTypeOversubscribed
		// Min heap.
		switch h2.(type) {
		case float64:
			log.Printf("Non-updated schedulerPoolType: host %s", h.ID)
		}

		diff := h.subscribedRatio.Sub(h2.(scheduling.Host).SubscribedRatioAsDecimal()).InexactFloat64()
		if diff != 0 {
			return diff
		}

		// For otherwise equal hosts, compare their IDs for stable ordering
		return float64(strings.Compare(h.ID, h2.(scheduling.Host).GetID()))
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

	committed := h.allocationManager.CommittedResources()
	pending := h.allocationManager.PendingResources()

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
		AllocatedCpu:    float32(committed.CPU()),
		AllocatedMemory: float32(committed.MemoryMB()),
		AllocatedGpu:    float32(committed.GPU()),
		AllocatedVRAM:   float32(committed.VRAM()),
		PendingCpu:      float32(pending.CPU()),
		PendingMemory:   float32(pending.MemoryMB()),
		PendingGpu:      float32(pending.GPU()),
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
	return h.allocationManager.NumReservations()
}

// PlacedMemoryMB returns the total amount of memory scheduled onto the Host, which is computed as the
// sum of the Host's pending memory and the Host's committed memory, in megabytes.
func (h *Host) PlacedMemoryMB() decimal.Decimal {
	return h.allocationManager.PlacedMemoryMB()
}

// PlacedGPUs returns the total number of GPUs scheduled onto the Host, which is computed as the
// sum of the Host's pending GPUs and the Host's committed GPUs.
func (h *Host) PlacedGPUs() decimal.Decimal {
	return h.allocationManager.PlacedGPUs()
}

// PlacedVRAM returns the total amount of VRAM in GB scheduled onto the Host, which is computed as the
// sum of the Host's pending VRAM and the Host's committed VRAM.
func (h *Host) PlacedVRAM() decimal.Decimal {
	return h.allocationManager.PlacedVRAM()
}

// PlacedCPUs returns the total number of Millicpus scheduled onto the Host, which is computed as the
// sum of the Host's pending Millicpus and the Host's committed Millicpus.
func (h *Host) PlacedCPUs() decimal.Decimal {
	return h.allocationManager.PlacedCPUs()
}

// computeHypotheticalSubscriptionRatio computes what the Host's (over)subscription ratios would be for CPU, Memory,
// and GPU, if it were to serve a Container with the given types.Spec resource request/requirements.
func (h *Host) computeHypotheticalSubscriptionRatio(resourceRequest types.Spec) (decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal) {
	numReplicasPerKernel := h.numReplicasPerKernel
	if h.numReplicasPerKernel == 0 {
		h.numReplicasPerKernel = 1
	}

	divisor := decimal.NewFromFloat(float64(numReplicasPerKernel))

	// Convert the given types.Spec to a *types.DecimalSpec.
	var decimalSpec *types.DecimalSpec
	if specAsDecimalSpec, ok := resourceRequest.(*types.DecimalSpec); ok {
		// If the parameter is already a *types.DecimalSpec, then no actual conversion needs to be performed.
		decimalSpec = specAsDecimalSpec
	} else {
		decimalSpec = types.ToDecimalSpec(resourceRequest)
	}

	var cpuRatio, memRatio, gpuRatio, vramRatio decimal.Decimal

	if h.allocationManager.SpecResources().Millicpus.IsZero() {
		cpuRatio = decimal.Zero
	} else {
		totalCPUs := h.PlacedCPUs().Add(decimalSpec.Millicpus)
		cpuRatio = totalCPUs.Div(h.allocationManager.SpecResources().Millicpus).Div(divisor)
	}

	if h.allocationManager.SpecResources().MemoryMb.IsZero() {
		memRatio = decimal.Zero
	} else {
		totalMemory := h.PlacedMemoryMB().Add(decimalSpec.MemoryMb)
		memRatio = totalMemory.Div(h.allocationManager.SpecResources().MemoryMb).Div(divisor)
	}

	if h.allocationManager.SpecResources().GPUs.IsZero() {
		gpuRatio = decimal.Zero
	} else {
		totalGPUs := h.PlacedGPUs().Add(decimalSpec.GPUs)
		gpuRatio = totalGPUs.Div(h.allocationManager.SpecResources().GPUs).Div(divisor)
	}

	if h.allocationManager.SpecResources().VRam.IsZero() {
		vramRatio = decimal.Zero
	} else {
		totalVRAM := h.PlacedVRAM().Add(decimalSpec.VRam)
		vramRatio = totalVRAM.Div(h.allocationManager.SpecResources().VRam).Div(divisor)
	}

	return cpuRatio, memRatio, gpuRatio, vramRatio
}

// WillBecomeTooOversubscribed returns a boolean indicating whether the Host will become "too" oversubscribed if it
// were to serve a kernel replica with the given resource requirements / request.
//
// "Too" oversubscribed means that the Host's over-subscription ratio would exceed the configured limit upon
// serving the Container with the given types.Spec resource request/requirements.
func (h *Host) WillBecomeTooOversubscribed(resourceRequest types.Spec) bool {
	cpuRatio, memRatio, gpuRatio, vramRatio := h.computeHypotheticalSubscriptionRatio(resourceRequest)

	var willOversubscribeCpu, willOversubscribeMemory, willOversubscribeGpu, willOversubscribeVRAM bool

	if h.numReplicasPerKernel == 1 {
		willOversubscribeCpu = h.SubscriptionQuerier.GetOversubscriptionFactor(cpuRatio).
			GreaterThan(decimal.Zero)

		willOversubscribeMemory = h.SubscriptionQuerier.GetOversubscriptionFactor(memRatio).
			GreaterThan(decimal.Zero)

		willOversubscribeGpu = h.SubscriptionQuerier.GetOversubscriptionFactor(gpuRatio).
			GreaterThan(decimal.Zero)

		willOversubscribeVRAM = h.SubscriptionQuerier.GetOversubscriptionFactor(vramRatio).
			GreaterThan(decimal.Zero)
	} else {
		willOversubscribeCpu = h.SubscriptionQuerier.GetOversubscriptionFactor(cpuRatio).
			GreaterThanOrEqual(decimal.Zero)

		willOversubscribeMemory = h.SubscriptionQuerier.GetOversubscriptionFactor(memRatio).
			GreaterThanOrEqual(decimal.Zero)

		willOversubscribeGpu = h.SubscriptionQuerier.GetOversubscriptionFactor(gpuRatio).
			GreaterThanOrEqual(decimal.Zero)

		willOversubscribeVRAM = h.SubscriptionQuerier.GetOversubscriptionFactor(vramRatio).
			GreaterThanOrEqual(decimal.Zero)
	}

	subscriptionRatio := h.SubscriptionQuerier.SubscriptionRatio()

	h.log.Debug("Computed over-subscription ratios for resource request: %v. Current subscription ratio: %.4f.\n"+
		"CPU Ratio: %s (Will Oversubscribe? %v), Memory Ratio: %s (Will Oversubscribe? %v), GPU Ratio: %s (Will Oversubscribe? %v)"+
		", VRAM Ratio: %s (Will Oversubscribe? %v)", resourceRequest.String(), subscriptionRatio, cpuRatio.StringFixed(4),
		willOversubscribeCpu, memRatio.StringFixed(4), willOversubscribeMemory, gpuRatio.StringFixed(4), willOversubscribeGpu,
		vramRatio.StringFixed(4), willOversubscribeVRAM)

	return willOversubscribeCpu || willOversubscribeMemory || willOversubscribeGpu || willOversubscribeVRAM
}

// CanServeContainerWithError returns nil if the target Host can serve the resource request.
//
// This method only checks against the Host's "spec" (i.e., the total HostResources available on the Host,
// not taking into account current resource allocations).
func (h *Host) CanServeContainerWithError(resourceRequest types.Spec) (bool, error) {
	return h.allocationManager.CanServeContainerWithError(resourceRequest)
}

// CanServeContainer returns a boolean indicating whether this Host could serve a kernel replica with the given
// resource requirements / resource request. This method only checks against the Host's "spec" (i.e., the total
// HostResources available on the Host, not taking into account current resource allocations).
//
// CanServeContainer returns true when the Host could serve the hypothetical kernel and false when the Host could not.
func (h *Host) CanServeContainer(resourceRequest types.Spec) bool {
	return h.allocationManager.CanServeContainer(resourceRequest)
}

// CanCommitResources returns a boolean indicating whether this Host could commit the specified resource request
// to a kernel scheduled onto the Host right now. Commiting resource requires having sufficiently many idle HostResources
// available.
//
// CanCommitResources returns true if the Host could commit/reserve the given HostResources right now.
// Otherwise, CanCommitResources returns false.
func (h *Host) CanCommitResources(resourceRequest types.Spec) bool {
	return h.allocationManager.CanCommitResources(resourceRequest)
}

// ReleaseReservation is to be called when a resource reservation should be released because the
// scheduling of the associated replica of the associated kernel is being aborted.
func (h *Host) ReleaseReservation(spec *proto.KernelSpec) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	err := h.allocationManager.ReleaseReservation(spec)
	if err != nil {
		panic(err)
	}

	// No longer being considered.
	h.isBeingConsideredForScheduling.Add(-1)

	return nil
}

// ReserveResourcesForSpecificReplica attempts to reserve the resources required by the specified kernel replica,
// returning a boolean flag indicating whether the resource reservation was completed successfully.
//
// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
func (h *Host) ReserveResourcesForSpecificReplica(replicaSpec *proto.KernelReplicaSpec, usePendingResources bool) (bool, error) {
	err := h.reserveResources(replicaSpec.ReplicaId, replicaSpec.Kernel.Id, replicaSpec.ResourceSpec().ToDecimalSpec(), usePendingResources)
	if err != nil {
		return false, err
	}

	return true, nil
}

// ReserveResources attempts to reserve the resources required by the specified kernel, returning
// a boolean flag indicating whether the resource reservation was completed successfully.
//
// If the Host is already hosting a replica of this kernel, then ReserveResources immediately returns false.
func (h *Host) ReserveResources(spec *proto.KernelSpec, usePendingResources bool) (bool, error) {
	err := h.reserveResources(resource.ReplicaIdForReservation, spec.Id, spec.ResourceSpec.ToDecimalSpec(), usePendingResources)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (h *Host) reserveResources(replicaId int32, kernelId string, resourceRequest *types.DecimalSpec, usePending bool) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	h.log.Debug("Creating resource reservation for new replica %d of kernel \"%s\". UsePending=%v. Request=%s. Current resources on host: %v.",
		replicaId, kernelId, usePending, resourceRequest.String(), h.GetResourceCountsAsString())

	if h.WillBecomeTooOversubscribed(resourceRequest) {
		h.log.Debug("Cannot commit resources for a replica of kernel %s; insufficient idle resources available.",
			kernelId)
		return ErrWillOversubscribe
	}

	err := h.allocationManager.ReserveResources(replicaId, kernelId, resourceRequest, usePending)
	if err != nil {
		h.log.Debug("Failed to create resource reservation for new replica of kernel %s because: %v [usePending=%v]",
			kernelId, err, usePending)
		return err
	}

	oldSubscribedRatio := h.subscribedRatio
	h.RecomputeSubscribedRatio()

	h.log.Debug("Successfully reserved resources for new replica of kernel %s. Old subscription ratio: %s. New subscription ratio: %s. Updated resources: %v.",
		kernelId, oldSubscribedRatio.StringFixed(3), h.subscribedRatio.StringFixed(3), h.GetResourceCountsAsString())

	return nil
}

// GetReservation returns the scheduling.ResourceReservation associated with the specified kernel, if one exists.
func (h *Host) GetReservation(kernelId string) (scheduling.Allocation, bool) {
	return h.allocationManager.GetReservation(kernelId)
}

func (h *Host) GetResourceSpec() types.Spec {
	return h.resourceSpec
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

		return fmt.Errorf("%w (host %s): %w", scheduling.ErrInvalidHost, h.ID, scheduling.ErrHostAlreadyEnabled)
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
		return fmt.Errorf("%w (host %s): %w", scheduling.ErrInvalidHost, h.ID, scheduling.ErrHostAlreadyDisabled)
	}

	h.enabled = false
	return nil
}

// ContainerStoppedTraining is to be called when a Container stops training on a Host.
func (h *Host) ContainerStoppedTraining(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot find container for replica %d of kernel %s on host %s (ID=%s).",
			container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
		return fmt.Errorf("%w: cannot find container for replica %d of kernel %s on host %s (ID=%s)",
			ErrInvalidContainer, container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
	}

	// If the resource binding mode is instead BindResourcesWhenContainerScheduled, then we do not
	// uncommit the resources until the container is actually evicted.
	if h.schedulingPolicy.ResourceBindingMode() == scheduling.BindResourcesAtTrainingStart {
		spec := container.ResourceSpec()

		h.log.Debug("Releasing committed resources [%s] from replica %d of kernel \"%s\". Current resources: %s.",
			spec.String(), container.ReplicaId(), container.KernelID(), h.GetResourceCountsAsString())

		// TODO: Check against execute request ID.
		err := h.allocationManager.ReleaseCommittedResources(container.ReplicaId(), container.KernelID(), scheduling.DefaultExecutionId)

		return err // Will be nil if nothing bad happened when un-committing the resources.
	}

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
		return fmt.Errorf("%w: cannot find container for replica %d of kernel %s on host %s (ID=%s)",
			ErrInvalidContainer, container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
	}

	// If the resource binding mode is instead BindResourcesWhenContainerScheduled, then they're already
	// committed to the container, and so we don't have to do anything else and can just return nil,
	// as we do below.
	if !h.allocationManager.ReplicaHasCommittedResources(container.ReplicaId(), container.KernelID()) {
		panic(fmt.Sprintf("Replica %d of kernel %s has started training. Resources should be committed.",
			container.ReplicaId(), container.KernelID()))
	}

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
// The executionId parameter is used to ensure that, if messages are received out-of-order, that we do not
// pre-release resources when we shouldn't have.
//
// For example, let's say we submit EXECUTION_1 to a Kernel. We received the main execute_reply from the leader,
// but there's a delay for the replies from the followers. In the meantime, we submit EXECUTION_2 to the Kernel.
// EXECUTION_2 required we pre-allocate some resources again. Now if we receive the delayed replies to EXECUTION_1,
// we may release the pre-committed resources for EXECUTION_2.
//
// By passing the executionId, which is stored with the pre-committed resource allocation, we can simply ignore
// the de-allocation request if it is outdated.
//
// PreCommitResources is the inverse/counterpart to ReleasePreCommitedResources.
func (h *Host) PreCommitResources(container scheduling.KernelContainer, executionId string, gpuDeviceIds []int) ([]int, error) {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	kernelId := container.KernelID()
	replicaId := container.ReplicaId()

	return h.allocationManager.PreCommitResourcesToExistingContainer(
		replicaId, kernelId, executionId, container.ResourceSpec(), gpuDeviceIds)
}

// GetResourceCountsAsString returns the current resource counts of the Host as a string and is useful for printing.
func (h *Host) GetResourceCountsAsString() string {
	return h.allocationManager.GetResourceCountsAsString()
}

// ReleasePreCommitedResources releases resources that were pre-committed to the given KernelContainer.
//
// ReleasePreCommitedResources is the inverse/counterpart to PreCommitResources.
//
// The executionId parameter is used to ensure that, if messages are received out-of-order, that we do not
// pre-release resources when we shouldn't have.
//
// For example, let's say we submit EXECUTION_1 to a Kernel. We received the main execute_reply from the leader,
// but there's a delay for the replies from the followers. In the meantime, we submit EXECUTION_2 to the Kernel.
// EXECUTION_2 required we pre-allocate some resources again. Now if we receive the delayed replies to EXECUTION_1,
// we may release the pre-committed resources for EXECUTION_2.
//
// By passing the executionId, which is stored with the pre-committed resource allocation, we can simply ignore
// the de-allocation request if it is outdated.
func (h *Host) ReleasePreCommitedResources(container scheduling.KernelContainer, executionId string) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	err := h.allocationManager.ReleaseCommittedResources(container.ReplicaId(), container.KernelID(), executionId)

	// We know we found a pre-committed resource allocation.
	// Let's check if the execution ID matches.
	// If not, we'll assume that we already released it and just return.
	if errors.Is(err, resource.ErrInvalidAllocationType) {
		return nil
	}

	// It's OK for there to be an error in some cases -- if we receive the "execute_reply" from the leader
	// first, and we pre-committed resources to all replicas, then we release the pre-commitments at that
	// point. So, if we attempt to release the same pre-commitment when we receive "execute_reply" from
	// the follower replicas, then that release will fail (because it will have already occurred).
	return err // Will be nil on success.
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	if _, ok := h.containers.Load(container.ContainerID()); !ok {
		h.log.Error("Cannot remove specified Container for replica %d of kernel %s from Host. Container is not on specified Host.",
			container.ReplicaId(), container.KernelID())
		return fmt.Errorf("%w: cannot find container for replica %d of kernel %s on host %s (ID=%s) for removal",
			ErrInvalidContainer, container.ReplicaId(), container.KernelID(), h.NodeName, h.ID)
	}

	h.containers.Delete(container.ContainerID())
	h.pendingContainers.Sub(1)

	err := h.allocationManager.ReplicaEvicted(container.ReplicaId(), container.KernelID())
	if err != nil {
		h.log.Error("Error while updating resources of host while evicting container %s: %v",
			container.ContainerID(), err)

		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// ContainerStartedRunningOnHost is to be called when a Container officially begins running on the target Host.
func (h *Host) ContainerStartedRunningOnHost(container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	h.log.Debug("Container for replica %d of kernel %s has officially started running on host %s.",
		container.ReplicaId(), container.KernelID(), h.NodeName)

	h.containers.Store(container.ContainerID(), container)
	h.pendingContainers.Add(1)

	err := h.allocationManager.ContainerStartedRunningOnHost(container.ReplicaId(), container.KernelID(), container.ResourceSpec())
	if err != nil {
		panic(err)
	}

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

// AdjustKernelResourceRequestCoordinated when the ResourceSpec of a KernelContainer that is already scheduled on
// this Host is updated or changed. This ensures that the Host's resource counts are up to date.
//
// This version runs in a coordination fashion and is used when updating the resources of multi-replica kernels.
func (h *Host) AdjustKernelResourceRequestCoordinated(updatedSpec types.Spec, oldSpec types.Spec,
	container scheduling.KernelContainer, tx scheduling.CoordinatedTransaction) error {

	// The CoordinatedTransaction will lock this mutex.
	// We just need to unlock it.
	defer h.schedulingMutex.Unlock()

	oldSubscribedRatio := h.subscribedRatio
	h.log.Debug("Coordinated Transaction: updating resource reservation for for replica %d of kernel %s from [%v] to [%v]. Current resource counts: %v.",
		container.ReplicaId(), container.KernelID(), oldSpec.String(), updatedSpec.String(), h.GetResourceCountsAsString())

	err := h.allocationManager.AdjustKernelResourceRequestCoordinated(updatedSpec, oldSpec, container, &h.schedulingMutex, tx)
	if err != nil {
		h.log.Debug("Failed to update ResourceRequest for replica %d of kernel %s (possibly because of another replica).",
			container.ReplicaId(), container.KernelID())
		return err
	}

	h.log.Debug("Successfully updated ResourceRequest for replica %d of kernel %s. Subscription ratio: %s → %s. Updated resource counts: %v.",
		container.ReplicaId(), container.KernelID(), oldSubscribedRatio.StringFixed(3),
		h.subscribedRatio.StringFixed(3), h.GetResourceCountsAsString())
	return nil
}

// AdjustKernelResourceRequest when the ResourceSpec of a KernelContainer that is already scheduled on this
// Host is updated or changed. This ensures that the Host's resource counts are up to date.
func (h *Host) AdjustKernelResourceRequest(updatedSpec types.Spec, oldSpec types.Spec, container scheduling.KernelContainer) error {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	kernelId := container.KernelID()
	replicaId := container.ReplicaId()
	oldSubRatio := h.subscribedRatio

	h.log.Debug("Updating resource reservation for just replica %d of kernel %s from [%v] to [%v].",
		replicaId, kernelId, oldSpec.String(), updatedSpec.String())

	err := h.allocationManager.AdjustKernelResourceRequest(updatedSpec, oldSpec, container)
	if err != nil {
		h.log.Warn("Failed to update resource reservation for just replica %d of kernel %s from [%v] to [%v].",
			replicaId, kernelId, oldSpec.String(), updatedSpec.String())
		return err
	}

	h.log.Debug("Successfully updated ResourceRequest for replica %d of kernel %s. Subscription ratio: %s → %s.",
		replicaId, kernelId, oldSubRatio.StringFixed(3), h.subscribedRatio.StringFixed(3))
	return nil
}

func (h *Host) getRB(sessRB float64, required float64) float64 {
	idleGPUs := h.allocationManager.IdleResources().GPU()
	extras := 0.0
	if idleGPUs > required {
		extras = idleGPUs / h.allocationManager.PendingResources().GPU()
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

// HasResourcesCommittedToKernel returns true if the Host has resources committed to a replica of the specified kernel.
func (h *Host) HasResourcesCommittedToKernel(kernelId string) bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	return h.allocationManager.KernelHasCommittedResources(kernelId)
}

// HasReservationForKernel returns true if the target Host has a reservation for the specified kernel.
func (h *Host) HasReservationForKernel(kernelId string) bool {
	h.schedulingMutex.Lock()
	defer h.schedulingMutex.Unlock()

	return h.allocationManager.HasReservationForKernel(kernelId)
}

// HasAnyReplicaOfKernel returns true if the target Host currently has a Container (i.e., a kernel replica) of the
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
	return h.allocationManager.IdleResources().GPU()
}

// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
func (h *Host) PendingGPUs() float64 {
	return h.allocationManager.PendingResources().GPU()
}

// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
func (h *Host) CommittedGPUs() float64 {
	return h.allocationManager.CommittedResources().GPU()
}

func (h *Host) IdleCPUs() float64 {
	return h.allocationManager.IdleResources().CPU()
}

func (h *Host) PendingCPUs() float64 {
	return h.allocationManager.PendingResources().CPU()
}

func (h *Host) CommittedCPUs() float64 {
	return h.allocationManager.CommittedResources().CPU()
}

func (h *Host) IdleMemoryMb() float64 {
	return h.allocationManager.IdleResources().MemoryMB()
}

func (h *Host) PendingMemoryMb() float64 {
	return h.allocationManager.PendingResources().MemoryMB()
}

func (h *Host) CommittedMemoryMb() float64 {
	return h.allocationManager.CommittedResources().MemoryMB()
}

func (h *Host) IdleVRAM() float64 { return h.allocationManager.IdleResources().VRAM() }

func (h *Host) PendingVRAM() float64 { return h.allocationManager.PendingResources().VRAM() }

func (h *Host) CommittedVRAM() float64 { return h.allocationManager.CommittedResources().VRAM() }

// ResourceSpec the types.Spec defining the HostResources available on the Host.
func (h *Host) ResourceSpec() types.ValidatableResourceSpec {
	return h.resourceSpec
}

// CurrentResourcesToString calls the String method on the Manager of the Host and returns the value
// generated by that String method.
func (h *Host) CurrentResourcesToString() string {
	return h.allocationManager.CurrentResourcesToString()
}

// IdleResources returns a types.Spec encapsulating the IdleResources on the Host.
func (h *Host) IdleResources() *types.DecimalSpec {
	return h.allocationManager.IdleResources()
}

// PendingResources returns a types.Spec encapsulating the PendingResources on the Host.
func (h *Host) PendingResources() *types.DecimalSpec {
	return h.allocationManager.PendingResources()
}

// CommittedResources returns a types.Spec encapsulating the idle HostResources on the Host.
func (h *Host) CommittedResources() *types.DecimalSpec {
	return h.allocationManager.CommittedResources()
}

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}

// GetCreatedAt returns the time at which the Host was created.
func (h *Host) GetCreatedAt() time.Time {
	return h.CreatedAt
}

func (h *Host) GetGpuDeviceIdsAssignedToReplica(replicaId int32, kernelId string) ([]int, error) {
	return h.allocationManager.GetGpuDeviceIdsAssignedToReplica(replicaId, kernelId)
}

// UnitTestingHost is a wrapper around Host that exposes some additional methods that allow for the direct
// manipulation of the Host's resources. This is useful for unit testing and not much else.
type UnitTestingHost struct {
	*Host
}

// NewUnitTestingHost creates a new UnitTestingHost struct wrapping the given Host and returns a pointer to the
// new UnitTestingHost struct.
func NewUnitTestingHost(host *Host) *UnitTestingHost {
	// Wrap the Host's allocation manager in a scheduling.UnitTestingAllocationManager.
	host.allocationManager = resource.NewUnitTestingAllocationManager(host.allocationManager)

	return &UnitTestingHost{
		Host: host,
	}
}

// AddToIdleResources is only intended to be used during unit tests.
func (h *UnitTestingHost) AddToIdleResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).AddToIdleResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// SubtractFromIdleResources is only intended to be used during unit tests.
func (h *UnitTestingHost) SubtractFromIdleResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).SubtractFromIdleResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// AddToCommittedResources is only intended to be used during unit tests.
func (h *UnitTestingHost) AddToCommittedResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).AddToCommittedResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// SubtractFromCommittedResources is only intended to be used during unit tests.
func (h *UnitTestingHost) SubtractFromCommittedResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).SubtractFromCommittedResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// AddToPendingResources is only meant to be used during unit tests.
func (h *UnitTestingHost) AddToPendingResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).AddToPendingResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// SubtractFromPendingResources is only intended to be used during unit tests.
func (h *UnitTestingHost) SubtractFromPendingResources(spec *types.DecimalSpec) error {
	err := h.allocationManager.(scheduling.UnitTestingAllocationManager).SubtractFromPendingResources(spec)
	if err != nil {
		return err
	}

	h.RecomputeSubscribedRatio()
	return nil
}

// AllocationManager returns the resource.AllocationManager that manages the resources of the target UnitTestingHost.
func (h *UnitTestingHost) AllocationManager() scheduling.AllocationManager {
	return h.allocationManager
}

// AddGpuDeviceIds makes the specified GPU device IDs available for allocation on the target UnitTestingHost.
func (h *UnitTestingHost) AddGpuDeviceIds(gpuDeviceIds []int) {
	h.allocationManager.(scheduling.UnitTestingAllocationManager).AddGpuDeviceIds(gpuDeviceIds)
}
