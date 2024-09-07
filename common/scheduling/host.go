package scheduling

import (
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/grpc/connectivity"
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
	IdleGPUsStat() types.StatFloat64Field

	// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	PendingGPUsStat() types.StatFloat64Field

	// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound to Containers scheduled on the Host.
	CommittedGPUsStat() types.StatFloat64Field

	// IdleCPUs returns the number of CPUs that the host has not allocated to any Containers.
	IdleCPUs() float64

	// PendingCPUs returns the number of CPUs that are oversubscribed by Containers scheduled on the Host.
	// Pending CPUs are NOT actively bound to any
	PendingCPUs() float64

	// CommittedCPUs returns the number of CPUs that are actively bound to Containers scheduled on the Host.
	CommittedCPUs() float64

	// IdleCPUsStat returns the StatFloat64 representing the number of CPUs that the host has not allocated to any Containers.
	IdleCPUsStat() types.StatFloat64Field

	// PendingCPUsStat returns the StatFloat64 representing the number of CPUs that are oversubscribed by Containers scheduled on the Host.
	PendingCPUsStat() types.StatFloat64Field

	// CommittedCPUsStat returns the StatFloat64 representing the number of CPUs that are actively bound to Containers scheduled on the Host.
	CommittedCPUsStat() types.StatFloat64Field

	// IdleMemoryMb returns the amount of memory, in megabytes (MB), that the host has not allocated to any Containers.
	IdleMemoryMb() float64

	// PendingMemoryMb returns the amount of memory, in megabytes (MB), that is oversubscribed by Containers scheduled on the Host.
	// Pending MemoryMb are NOT actively bound to any
	PendingMemoryMb() float64

	// CommittedMemoryMb returns the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	CommittedMemoryMb() float64

	// IdleMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that the host has not allocated to any Containers.
	IdleMemoryMbStat() types.StatFloat64Field

	// PendingMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that is oversubscribed by Containers scheduled on the Host.
	PendingMemoryMbStat() types.StatFloat64Field

	// CommittedMemoryMbStat returns the StatFloat64 representing the amount of memory, in megabytes (MB), that is actively bound to Containers scheduled on the Host.
	CommittedMemoryMbStat() types.StatFloat64Field

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
	gpuInfoMutex           sync.Mutex                           // gpuInfoMutex controls access to the latest GPU info.
	gpuInfoRefreshInterval time.Duration                        // gpuInfoRefreshInterval specifies how frequently to poll the remote scheduler nodes for updated GPU info.
	meta                   hashmap.HashMap[string, interface{}] // meta is a map of metadata.
	conn                   *grpc.ClientConn                     // conn is the gRPC connection to the Host.
	addr                   string                               // addr is the Host's address.
	nodeName               string                               // nodeName is the Host's name (for printing/logging).
	cluster                Cluster                              // cluster is a reference to the Cluster interface that manages this Host.
	id                     string                               // id is the unique ID of this host.
	containers             hashmap.HashMap[string, *Container]  // containers is a map of all the kernel replicas scheduled onto this host.
	trainingContainers     []*Container                         // trainingContainers are the actively-training kernel replicas.
	seenSessions           []string                             // seenSessions are the sessions that have been scheduled onto this host at least once.
	resourceSpec           types.Spec                           // resourceSpec is the spec describing the total resources available on the Host, not impacted by allocations.
	lastReschedule         types.StatFloat64                    // lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	errorCallback          ErrorCallback                        // errorCallback is a function to be called if a Host appears to be dead.
	pendingContainers      types.StatInt32                      // pendingContainers is the number of Containers that are scheduled on the host.
	createdAt              time.Time                            // createdAt is the time at which the Host was created.

	// TODO: Synchronize these values what what the ClusterDaemon retrieves periodically.

	idleCPUs          types.StatFloat64 // IdleCPUs returns the number of CPUs that the host has not allocated to any Containers.
	pendingCPUs       types.StatFloat64 // PendingCPUs returns the number of CPUs that are oversubscribed by Containers scheduled on the Host.
	committedCPUs     types.StatFloat64 // CommittedCPUs returns the number of CPUs that are actively bound to Containers scheduled on the Host.
	idleMemoryMb      types.StatFloat64 // IdleMemoryMb returns the amount of memory (in megabytes) that the host has not allocated to any Containers.
	pendingMemoryMb   types.StatFloat64 // PendingMemoryMb returns the amount of memory (in megabytes) that is oversubscribed by Containers scheduled on the Host.
	committedMemoryMb types.StatFloat64 // CommittedMemoryMb returns the amount of memory (in megabytes) that is actively bound to Containers scheduled on the Host.
	idleGPUs          types.StatFloat64 // IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
	pendingGPUs       types.StatFloat64 // PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	committedGPUs     types.StatFloat64 // CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.

	// Cached penalties
	sip             cache.InlineCache // Scale-in penalty.
	sipSession      *Session          // Scale-in penalty session.
	penaltyList     cache.InlineCache
	penalties       []cachedPenalty
	penaltyValidity bool
}

// NewHost creates and returns a new *Host.
func NewHost(id string, addr string, millicpus int32, memMb int32, gpuInfoRefreshInterval time.Duration, cluster Cluster, conn *grpc.ClientConn, errorCallback ErrorCallback) (*Host, error) {
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
	resourceSpec := &types.FullSpec{
		GPUs:     types.GPUSpec(gpuInfoResp.SpecGPUs),
		CPUs:     float64(millicpus),
		MemoryMb: float64(memMb),
	}

	host := &Host{
		LocalGatewayClient:     localGatewayClient,
		gpuInfoRefreshInterval: gpuInfoRefreshInterval,
		latestGpuInfo:          gpuInfoResp,
		id:                     id,
		nodeName:               confirmedId.NodeName,
		addr:                   addr,
		resourceSpec:           resourceSpec,
		cluster:                cluster,
		conn:                   conn,
		log:                    config.GetLogger(fmt.Sprintf("Host %s ", id)),
		containers:             hashmap.NewCornelkMap[string, *Container](5),
		trainingContainers:     make([]*Container, 0, int(resourceSpec.GPU())),
		penalties:              make([]cachedPenalty, int(resourceSpec.GPU())),
		seenSessions:           make([]string, int(resourceSpec.GPU())),
		meta:                   hashmap.NewCornelkMap[string, interface{}](64),
		errorCallback:          errorCallback,
		createdAt:              time.Now(),
	}

	host.sip.Producer = cache.FormalizeICProducer(host.getSIP)
	host.sip.Validator = GetClockTimeCacheValidator()
	host.penaltyList.Producer = cache.FormalizeChainedICProducer(host.updatePenaltyList)
	host.penaltyList.Validator = host.validatePenaltyList

	// Start the goroutine that polls for updated GPU info on an interval.
	go host.pollForGpuInfo()

	return host, nil
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
		NodeId:          h.id,
		NodeName:        h.nodeName,
		Address:         h.addr,
		CreatedAt:       timestamppb.New(h.createdAt),
		Containers:      dockerContainers,
		SpecCpu:         float32(h.resourceSpec.CPU()),
		SpecMemory:      float32(h.resourceSpec.MemoryMB()),
		SpecGpu:         float32(h.resourceSpec.GPU()),
		AllocatedCpu:    float32(h.committedCPUs.Load()),
		AllocatedGpu:    float32(h.committedGPUs.Load()),
		AllocatedMemory: float32(h.committedMemoryMb.Load()),
		PendingCpu:      float32(h.pendingCPUs.Load()),
		PendingMemory:   float32(h.pendingMemoryMb.Load()),
		PendingGpu:      float32(h.pendingGPUs.Load()),
	}
}

// pollForGpuInfo runs a loop that continuously requests GPU usage statistics from all the host schedulers.
func (h *Host) pollForGpuInfo() {
	numConsecutiveFailures := 0
	for {
		resp, err := h.LocalGatewayClient.GetActualGpuInfo(context.Background(), &proto.Void{})
		if err != nil {
			h.log.Error("Failed to refresh GPU info from Scheduler %s on Node %s: %v", h.ID(), h.NodeName(), err)
			numConsecutiveFailures += 1

			// If we've failed 3 or more consecutive times, then we may just assume that the scheduler is dead.
			if numConsecutiveFailures >= ConsecutiveFailuresWarning {
				// If the gRPC connection to the scheduler is in the transient failure or shutdown state, then we'll just assume it is dead.
				if h.Conn().GetState() == connectivity.TransientFailure || h.Conn().GetState() == connectivity.Shutdown {
					errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, and gRPC client connection is in state %v. Assuming scheduler %s is dead.", numConsecutiveFailures, h.ID(), h.NodeName(), h.Conn().GetState().String(), h.ID())
					h.log.Error(errorMessage)
					_ = h.ErrorCallback()(h.ID(), h.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					return
				} else if numConsecutiveFailures >= ConsecutiveFailuresBad { // If we've failed 5 or more times, then we'll assume it is dead regardless of the state of the gRPC connection.
					errorMessage := fmt.Sprintf("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s. Although gRPC client connection is in state %v, we're assuming scheduler %s is dead.", numConsecutiveFailures, h.ID(), h.NodeName(), h.Conn().GetState().String(), h.ID())
					h.log.Error(errorMessage)
					_ = h.ErrorCallback()(h.ID(), h.NodeName(), "Local Daemon Connectivity Error", errorMessage)
					return
				} else { // Otherwise, we won't assume it is dead yet...
					h.log.Warn("Failed %d consecutive times to retrieve GPU info from scheduler %s on node %s, but gRPC client connection is in state %v. Not assuming scheduler is dead yet...", numConsecutiveFailures, h.ID(), h.NodeName(), h.Conn().GetState().String())
				}
			}

			// Sleep for a shorter period of time in order to detect failure more quickly.
			// We'll sleep for longer depending on the number of consecutive failures we've encountered.
			// We clamp the maximum sleep to the standard refresh interval.
			shortenedSleepInterval := time.Duration(math.Max(float64(h.gpuInfoRefreshInterval), float64((time.Second*2)*time.Duration(numConsecutiveFailures))))
			time.Sleep(shortenedSleepInterval)
		} else {
			h.gpuInfoMutex.Lock()
			h.latestGpuInfo = resp

			// TODO: This is overwriting the manual adjustments from the scheduling stuff.
			// How best to handle this?
			h.Stats().IdleGPUsStat().Store(float64(resp.IdleGPUs))
			h.Stats().PendingGPUsStat().Store(float64(resp.PendingGPUs))
			h.Stats().CommittedGPUsStat().Store(float64(resp.CommittedGPUs))
			if h.ResourceSpec().GPU() != float64(resp.SpecGPUs) {
				h.log.Warn("Current spec GPUs (%.0f) do not match latest result from polling (%.0f). Updating spec now...", h.ResourceSpec().GPU(), resp.SpecGPUs)
				h.ResourceSpec().UpdateSpecGPUs(float64(resp.SpecGPUs))
			}

			h.gpuInfoMutex.Unlock()

			numConsecutiveFailures = 0
			time.Sleep(h.gpuInfoRefreshInterval)
		}
	}
}

// ContainerScheduled is to be called when a Container is scheduled onto the Host.
func (h *Host) ContainerScheduled(container *Container) {
	h.containers.Store(container.ContainerID(), container)

	h.pendingContainers.Add(1)

	h.pendingCPUs.Add(container.OutstandingResources().CPU())
	h.pendingMemoryMb.Add(container.OutstandingResources().MemoryMB())
	h.pendingGPUs.Add(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was scheduled onto Host %s.", container.String(), h.ID())
}

// Restore restores the state of a Host from another Host.
// TODO: Implement this more.
func (h *Host) Restore(restored *Host, callback ErrorCallback) error {
	h.SetErrorCallback(callback)
	h.resourceSpec = restored.resourceSpec
	h.id = restored.id
	h.nodeName = restored.nodeName
	h.LocalGatewayClient = restored.LocalGatewayClient
	h.latestGpuInfo = restored.latestGpuInfo
	h.gpuInfoRefreshInterval = restored.gpuInfoRefreshInterval

	// TODO: Make sure the other goroutine is no longer active.
	go h.pollForGpuInfo()

	return nil
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container *Container) {
	h.containers.Delete(container.ContainerID())

	h.pendingContainers.Sub(1)

	h.pendingCPUs.Sub(container.OutstandingResources().CPU())
	h.pendingMemoryMb.Sub(container.OutstandingResources().MemoryMB())
	h.pendingGPUs.Sub(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was removed from Host %s.", container.String(), h.ID())
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
	idleGPUs := h.idleGPUs.Load()
	extras := 0.0
	if idleGPUs > required {
		extras = idleGPUs / h.pendingGPUs.Load()
	}
	rb := sessRB * (extras + 1) / float64(h.pendingContainers.Load())
	h.log.Debug("Calculated RB: %.4f\n", h.id, rb)
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

func (h *Host) String() string {
	return fmt.Sprintf("Host[ID=%s,Name=%s,Addr=%s,Spec=%s]", h.id, h.nodeName, h.addr, h.resourceSpec.String())
}

func (h *Host) ID() string {
	return h.id
}

func (h *Host) NodeName() string {
	return h.nodeName
}

func (h *Host) Addr() string {
	return h.addr
}

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
	return h.idleGPUs.Load()
}

// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
func (h *Host) PendingGPUs() float64 {
	return h.pendingGPUs.Load()
}

// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
func (h *Host) CommittedGPUs() float64 {
	return h.committedGPUs.Load()
}

// IdleGPUsStat returns the StatFloat64 representing the number of GPUs that the host
// has not allocated to any Containers.
func (h *Host) IdleGPUsStat() types.StatFloat64Field {
	return &h.idleGPUs
}

// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed
// by Containers scheduled on the Host.
func (h *Host) PendingGPUsStat() types.StatFloat64Field {
	return &h.pendingGPUs
}

// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound
// to Containers scheduled on the Host.
func (h *Host) CommittedGPUsStat() types.StatFloat64Field {
	return &h.committedGPUs
}

func (h *Host) IdleCPUs() float64 {
	return h.idleCPUs.Load()
}

func (h *Host) PendingCPUs() float64 {
	return h.pendingCPUs.Load()
}

func (h *Host) CommittedCPUs() float64 {
	return h.committedCPUs.Load()
}

func (h *Host) IdleCPUsStat() types.StatFloat64Field {
	return &h.idleCPUs
}

func (h *Host) PendingCPUsStat() types.StatFloat64Field {
	return &h.pendingCPUs
}

func (h *Host) CommittedCPUsStat() types.StatFloat64Field {
	return &h.committedCPUs
}

func (h *Host) IdleMemoryMb() float64 {
	return h.idleMemoryMb.Load()
}

func (h *Host) PendingMemoryMb() float64 {
	return h.pendingMemoryMb.Load()
}

func (h *Host) CommittedMemoryMb() float64 {
	return h.committedMemoryMb.Load()
}

func (h *Host) IdleMemoryMbStat() types.StatFloat64Field {
	return &h.idleMemoryMb
}

func (h *Host) PendingMemoryMbStat() types.StatFloat64Field {
	return &h.pendingMemoryMb
}

func (h *Host) CommittedMemoryMbStat() types.StatFloat64Field {
	return &h.committedMemoryMb
}

// ResourceSpec the types.Spec defining the resources available on the Host.
func (h *Host) ResourceSpec() types.Spec {
	return h.resourceSpec
}

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}
