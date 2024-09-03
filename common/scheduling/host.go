package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils/hashmap"
	"google.golang.org/grpc"
	"math"
	"sort"
	"time"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
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
	Priority(session Session) float64

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
	IdleGPUsStat() StatFloat64Field

	// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	PendingGPUsStat() StatFloat64Field

	// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound to Containers scheduled on the Host.
	CommittedGPUsStat() StatFloat64Field

	// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	LastReschedule() StatFloat64Field
}

type HostMeta interface {
	Value(key interface{}) interface{}
}

// Host defines the interface for a host scheduler that is responsible for:
// 1. Provisioning host-local jupyter kernels.
// 2. Providing statistics of the host for cluster indexing.
type Host interface {
	gateway.LocalGatewayClient
	fmt.Stringer

	// ID returns the host id.
	ID() string

	// NodeName returns the name of the Kubernetes host that the node is running on.
	NodeName() string

	// Addr returns the host address.
	Addr() string

	// Restore restores the host connection.
	Restore(Host, ErrorCallback) error

	// Stats returns the statistics of the host.
	Stats() HostStatistics

	// SetMeta sets the metadata of the host.
	SetMeta(key HostMetaKey, value interface{})

	// GetMeta return the metadata of the host.
	GetMeta(key HostMetaKey) interface{}

	// ResourceSpec the types.Spec defining the resources available on the Host.
	ResourceSpec() types.Spec

	// ContainerScheduled is to be called when a Container is scheduled onto the Host.
	ContainerScheduled(container Container)

	// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
	ContainerRemoved(container Container)
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

type BaseHost struct {
	gateway.LocalGatewayClient

	log logger.Logger

	meta               hashmap.BaseHashMap[string, interface{}] // meta is a map of metadata.
	conn               *grpc.ClientConn                         // gRPC connection to the BaseHost.
	addr               string                                   // The BaseHost's address.
	nodeName           string                                   // The BaseHost's name (for printing/logging).
	cluster            Cluster                                  // Reference to the Cluster interface that manages this Host.
	id                 string                                   // Unique ID of this host.
	containers         hashmap.BaseHashMap[string, Container]   // All kernel replicas scheduled onto this host.
	trainingContainers []Container                              // Actively-training kernel replicas.
	seenSessions       []string                                 // Sessions that have been scheduled onto this host at least once.
	resourceSpec       types.Spec                               // The resources available on the Host.

	// TODO: Synchronize these values what what the ClusterDaemon retrieves periodically.

	// IdleGPUs returns the number of GPUs that the host has not allocated to any Containers.
	idleGPUs *StatFloat64

	// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
	pendingGPUs *StatFloat64

	// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
	committedGPUs *StatFloat64

	// lastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
	lastReschedule *StatFloat64

	pendingContainers StatInt32

	// Cached penalties
	sip             cache.InlineCache
	sipSession      Session
	penaltyList     cache.InlineCache
	penalties       []cachedPenalty
	penaltyValidity bool

	// A function to be called if a Host appears to be dead.
	errorCallback ErrorCallback
}

// NewBaseHost creates and returns a new *BaseHost.
func NewBaseHost(id string, nodeName string, addr string, spec types.Spec, cluster Cluster, conn *grpc.ClientConn, errorCallback ErrorCallback) *BaseHost {
	host := &BaseHost{
		id:                 id,
		nodeName:           nodeName,
		addr:               addr,
		resourceSpec:       spec,
		cluster:            cluster,
		conn:               conn,
		log:                config.GetLogger(fmt.Sprintf("Host %s", id)),
		containers:         hashmap.NewCornelkMap[string, Container](5),
		trainingContainers: make([]Container, 0, int(spec.GPU())),
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
func (h *BaseHost) ContainerScheduled(container Container) {
	h.containers.Store(container.ID(), container)
	h.pendingContainers.Add(1)
	h.pendingGPUs.Add(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was scheduled onto Host %s.", container.String(), h.ID())
}

// ContainerRemoved is to be called when a Container is stopped and removed from the Host.
func (h *BaseHost) ContainerRemoved(container Container) {
	h.containers.Delete(container.ID())
	h.pendingContainers.Sub(1)
	h.pendingGPUs.Sub(container.OutstandingResources().GPU())

	h.log.Debug("Container %s was removed from Host %s.", container.String(), h.ID())
}

// ErrorCallback returns the BaseHost's ErrorCallback field.
func (h *BaseHost) ErrorCallback() ErrorCallback {
	return h.errorCallback
}

// SetErrorCallback sets the BaseHost's ErrorCallback field.
func (h *BaseHost) SetErrorCallback(callback ErrorCallback) {
	h.errorCallback = callback
}

func (h *BaseHost) getPenalty(cached *cachedPenalty, gpus int) (*cachedPenalty, error) {
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

func (h *BaseHost) Penalty(gpus float64) (float64, PreemptionInfo, error) {
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

func (h *BaseHost) getSIP(sess Session) float64 {
	numGPUs := sess.ResourceUtilization().NumGpusAsFloat()

	penalty, _, err := h.Penalty(numGPUs)
	if err != nil {
		h.log.Error("Unexpected err on calculating AB: %v", err)
	}
	h.sip.Validator(time.Now())

	rb := h.getRB(sess.SessionStatistics().InteractivePriority(), numGPUs)
	h.log.Debug("Cached sip for session %v: %.2f(%.2f-%.2f). IP: %.4f (%s).", sess, rb-penalty, rb, penalty, sess.SessionStatistics().InteractivePriority(), sess.SessionStatistics().Explain(ExplainInteractivePriority))
	return rb - penalty
}

func (h *BaseHost) getRB(sessRB float64, required float64) float64 {
	idleGPUs := h.idleGPUs.Load()
	extras := 0.0
	if idleGPUs > required {
		extras = idleGPUs / h.pendingGPUs.Load()
	}
	rb := sessRB * (extras + 1) / float64(h.pendingContainers.Load())
	h.log.Debug("Calculated RB: %.4f\n", h.id, rb)
	return rb
}

func (h *BaseHost) validatePenaltyList(_ interface{}) bool {
	return h.penaltyValidity
}

func (h *BaseHost) updatePenaltyList(cached *PenaltyContainers) *PenaltyContainers {
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
func (h *BaseHost) IdleGPUs() float64 {
	return h.idleGPUs.Load()
}

// PendingGPUs returns the number of GPUs that are oversubscribed by Containers scheduled on the Host.
func (h *BaseHost) PendingGPUs() float64 {
	return h.pendingGPUs.Load()
}

// CommittedGPUs returns the number of GPUs that are actively bound to Containers scheduled on the Host.
func (h *BaseHost) CommittedGPUs() float64 {
	return h.committedGPUs.Load()
}

// IdleGPUsStat returns the StatFloat64 representing the number of GPUs that the host
// has not allocated to any Containers.
func (h *BaseHost) IdleGPUsStat() StatFloat64Field {
	return h.idleGPUs
}

// PendingGPUsStat returns the StatFloat64 representing the number of GPUs that are oversubscribed
// by Containers scheduled on the Host.
func (h *BaseHost) PendingGPUsStat() StatFloat64Field {
	return h.pendingGPUs
}

// CommittedGPUsStat returns the StatFloat64 representing the number of GPUs that are actively bound
// to Containers scheduled on the Host.
func (h *BaseHost) CommittedGPUsStat() StatFloat64Field {
	return h.committedGPUs
}

// ResourceSpec the types.Spec defining the resources available on the Host.
func (h *BaseHost) ResourceSpec() types.Spec {
	return h.resourceSpec
}

func (h *BaseHost) String() string {
	return fmt.Sprintf("Host[ID=%s,Name=%s,Addr=%s,Spec=%s]", h.id, h.nodeName, h.addr, h.resourceSpec.String())
}

func (h *BaseHost) ID() string {
	return h.id
}

func (h *BaseHost) NodeName() string {
	return h.nodeName
}

func (h *BaseHost) Addr() string {
	return h.addr
}

func (h *BaseHost) Conn() *grpc.ClientConn {
	return h.conn
}

func (h *BaseHost) Restore(Host) error {
	panic("The Restore method is not implemented in BaseHost.")
}

func (h *BaseHost) Stats() HostStatistics {
	return h
}

// LastReschedule returns the scale-out priority of the last Container to be migrated/evicted (I think?)
func (h *BaseHost) LastReschedule() StatFloat64Field {
	return h.lastReschedule
}

// SetMeta sets the metadata of the host.
func (h *BaseHost) SetMeta(key HostMetaKey, value interface{}) {
	h.meta.Store(string(key), value)
}

// GetMeta return the metadata of the host.
func (h *BaseHost) GetMeta(key HostMetaKey) interface{} {
	if value, ok := h.meta.Load(string(key)); ok {
		return value
	}
	return nil
}

func (h *BaseHost) Priority(session Session) float64 {
	if session != h.sipSession {
		h.sip.Invalidate()
		h.sipSession = session
	}
	return h.sip.Value(session).(float64)
}

func (h *BaseHost) ScaleInPriority() float64 {
	return h.sip.Value().(float64)
}
