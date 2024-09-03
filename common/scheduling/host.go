package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc"
	"math"
	"sort"
	"time"
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

	meta               hashmap.BaseHashMap[string, interface{}] // meta is a map of metadata.
	conn               *grpc.ClientConn                         // gRPC connection to the Host.
	addr               string                                   // The Host's address.
	nodeName           string                                   // The Host's name (for printing/logging).
	cluster            Cluster                                  // Reference to the Cluster interface that manages this Host.
	id                 string                                   // Unique ID of this host.
	containers         hashmap.BaseHashMap[string, *Container]  // All kernel replicas scheduled onto this host.
	trainingContainers []*Container                             // Actively-training kernel replicas.
	seenSessions       []string                                 // Sessions that have been scheduled onto this host at least once.
	resourceSpec       types.Spec                               // The resources available on the Host.

	// TODO: Synchronize these values what what the ClusterDaemon retrieves periodically.

	// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
	idleGPUs *types.StatFloat64

	// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
	pendingGPUs *types.StatFloat64

	// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	committedGPUs *types.StatFloat64

	// lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	lastReschedule *types.StatFloat64

	pendingContainers types.StatInt32

	// Cached penalties
	sip             cache.InlineCache
	sipSession      *Session
	penaltyList     cache.InlineCache
	penalties       []cachedPenalty
	penaltyValidity bool

	// A function to be called if a Host appears to be dead.
	errorCallback ErrorCallback
}

// NewHost creates and returns a new *Host.
func NewHost(id string, nodeName string, addr string, spec types.Spec, cluster Cluster, conn *grpc.ClientConn, errorCallback ErrorCallback) *Host {
	host := &Host{
		id:                 id,
		nodeName:           nodeName,
		addr:               addr,
		resourceSpec:       spec,
		cluster:            cluster,
		conn:               conn,
		log:                config.GetLogger(fmt.Sprintf("Host %s", id)),
		containers:         hashmap.NewCornelkMap[string, *Container](5),
		trainingContainers: make([]*Container, 0, int(spec.GPU())),
		penalties:          make([]cachedPenalty, int(spec.GPU())),
		seenSessions:       make([]string, int(spec.GPU())),
		errorCallback:      errorCallback,
	}

	host.sip.Producer = cache.FormalizeICProducer(host.getSIP)
	host.sip.Validator = GetClockTimeCacheValidator()
	host.penaltyList.Producer = cache.FormalizeChainedICProducer(host.updatePenaltyList)
	host.penaltyList.Validator = host.validatePenaltyList

	return host
}

// ContainerScheduled is to be called when a Container is scheduled onto the Host.
func (h *Host) ContainerScheduled(container *Container) {
	h.containers.Store(container.ID(), container)
	h.pendingContainers.Add(1)
	h.pendingGPUs.Add(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was scheduled onto Host %s.", container.String(), h.ID())
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *Host) ContainerRemoved(container *Container) {
	h.containers.Delete(container.ID())
	h.pendingContainers.Sub(1)
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
	return h.idleGPUs
}

// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed
// by Containers scheduled on the Host.
func (h *Host) PendingGPUsStat() types.StatFloat64Field {
	return h.pendingGPUs
}

// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound
// to Containers scheduled on the Host.
func (h *Host) CommittedGPUsStat() types.StatFloat64Field {
	return h.committedGPUs
}

// ResourceSpec the types.Spec defining the resources available on the Host.
func (h *Host) ResourceSpec() types.Spec {
	return h.resourceSpec
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

func (h *Host) Restore(*Host) error {
	panic("The Restore method is not implemented in Host.")
}

func (h *Host) Stats() HostStatistics {
	return h
}

// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
func (h *Host) LastReschedule() types.StatFloat64Field {
	return h.lastReschedule
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

func (h *Host) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}
