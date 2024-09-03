package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"sync/atomic"
	"time"
)

const (
	ContainerStateTraining  ContainerState = "SESSION_TRAINING"  // Indicates that the Container is actively running AND is actively training.
	ContainerStateStopped   ContainerState = "SESSION_STOPPED"   // Indicates that the Container is permanently stopped.
	ContainerStateIdle      ContainerState = "SESSION_IDLE"      // Indicates that the Container is actively running on a Host and is NOT actively performing a task.
	ContainerStateMigrating ContainerState = "SESSION_MIGRATING" // Indicates that the Container is currently migrating to a new Host.
)

type ContainerState string

type ContainerStatistics interface {
	Explainer

	// PreemptionPriority returns the Container's preemption priority, which is equal to 0 when the Container is idle.
	// When the Container is actively training, its preemption priority is equal to its Session's preemption priority.
	PreemptionPriority() float64

	// InteractivePriority returns the Container's interactive priority metric.
	InteractivePriority() float64

	// ScaleOutPriority returns the host's "scheduling-out priority", or SOP, which is defined as the time of the
	// last rescheduling operation plus the frequency of training tasks multiplied by the interactive priority of the
	// potential training task plus the sum of the preemption priorities of the preemptible tasks.
	ScaleOutPriority() float64
}

type Container interface {
	client.KernelReplicaClient

	// ID returns the kernel ID of the Container.
	ID() string

	// Session returns the Session associated with the Container.
	Session() Session

	// Host returns the Host on which the Container is currently scheduled.
	Host() Host

	// ContainerStatistics returns the statistics of the Container.
	ContainerStatistics() ContainerStatistics

	// ContainerState returns the Container's current state.
	ContainerState() ContainerState

	// TrainingStarted should be called when the Container begins training.
	TrainingStarted() error

	// TrainingStopped should be called when the Container stops training.
	TrainingStopped() error

	// IsTraining returns true if the Container is actively training.
	// Otherwise, IsTraining returns false.
	IsTraining() bool

	// IsStopped returns true if the Container has been terminated.
	IsStopped() bool

	// IsIdle returns true if the Container is currently idle, meaning that it is not currently training.
	IsIdle() bool

	// IsMigrating returns true if the Container is currently migrating from one Host to another.
	IsMigrating() bool

	// GetClient returns the client.KernelReplicaClient associated with the Container.
	GetClient() client.KernelReplicaClient

	// SetClient sets/updates the client.KernelReplicaClient associated with the Container.
	SetClient(client client.KernelReplicaClient)
}

type BasicContainer struct {
	client.KernelReplicaClient

	log logger.Logger

	session         Session        // The Session associated with the Container.
	host            Host           // The Host on which the Container is currently scheduled.
	id              string         // The kernel ID of the Container.
	containerState  ContainerState // The current state of the Container.
	executions      atomic.Int32   // The number of training events processed by the Container.
	outstandingGPUs types.GPUSpec  // The number of GPUs required by the Container to train.
	isTraining      bool           // Flag indicating whether the Container is actively training (true) or not (false).

	spec     types.Spec
	lastSpec types.Spec

	interactivePriorityBase        float64
	interactivePriority            cache.InlineCache
	interactivePriorityExplanation string
}

// NewBasicContainer creates and returns a new *BasicContainer.
func NewBasicContainer(session Session, kernelReplica client.KernelReplicaClient) *BasicContainer {
	id := session.ID()
	container := &BasicContainer{
		KernelReplicaClient: kernelReplica,
		id:                  id,
		session:             session,
		log:                 config.GetLogger(fmt.Sprintf("Container %s", id)),
		containerState:      ContainerStateIdle,
		spec:                session.ResourceSpec(),
	}

	container.executions.Store(0)
	container.interactivePriority.Producer = cache.FormalizeICProducer(container.getInteractivePriority)
	container.interactivePriority.Validator = GetClockTimeCacheValidator()

	return container
}

// GetClient returns the client.KernelReplicaClient associated with the Container.
func (c *BasicContainer) GetClient() client.KernelReplicaClient {
	return c.KernelReplicaClient
}

// SetClient sets/updates the client.KernelReplicaClient associated with the Container.
func (c *BasicContainer) SetClient(client client.KernelReplicaClient) {
	c.KernelReplicaClient = client
}

func (c *BasicContainer) ContainerStatistics() ContainerStatistics {
	return c
}

func (c *BasicContainer) ID() string {
	return c.id
}

func (c *BasicContainer) Session() Session {
	return c.session
}

func (c *BasicContainer) Host() Host {
	return c.host
}

func (c *BasicContainer) getInteractivePriority() float64 {
	c.interactivePriority.Validator(time.Now())
	required := c.session.ResourceUtilization().NumGpusAsFloat() // float64(c.Session().Meta().GPU.GPUs)
	idleGPUs := c.host.Stats().IdleGPUs()
	extras := 0.0
	extraExplain := "0.0"
	if idleGPUs > required {
		extras = idleGPUs / c.host.Stats().PendingGPUs()
		extraExplain = fmt.Sprintf("%f / %f", idleGPUs, c.host.Stats().PendingGPUs())
	}
	// interactivePriority := float64(c.executions) * c.session.Stats().IP() * idleGPUs / c.host.Stats().PendingGPUs().Load()
	stats := c.session.SessionStatistics()
	interactivePriority := stats.InteractivePriority() * (extras + 1)
	c.interactivePriorityExplanation = fmt.Sprintf("%s( * (1 + %s))", stats.Explain(ExplainInteractivePriority), extraExplain)
	// log.Printf("%v: updated cached interactivePriority %f, container:%v, potentials: %f, pending containers: %d\n", ClockTime, interactivePriority, c.session, extras+1, c.host.Stats().PendingContainers().Load())
	return interactivePriority
}

// InteractivePriority returns the Container's interactive priority metric.
func (c *BasicContainer) InteractivePriority() float64 {
	return c.interactivePriority.Value().(float64)
}

func (c *BasicContainer) InvalidateInteractivePriority() {
	c.interactivePriority.Invalidate()
}

// PreemptionPriority returns the Container's preemption priority, which is equal to 0 when the Container is idle.
// When the Container is actively training, its preemption priority is equal to its Session's preemption priority.
func (c *BasicContainer) PreemptionPriority() float64 {
	if c.IsTraining() {
		return c.Session().SessionStatistics().PreemptionPriority()
	}

	return 0.0
}

// Explain returns an explanation for how the latest metric (specified using the ExplainerKey argument) was computed.
func (c *BasicContainer) Explain(key ExplainerEntry) string {
	switch key {
	case ExplainInteractivePriority:
		return c.interactivePriorityExplanation
	case ExplainPreemptionPriority:
		if c.IsTraining() {
			return c.Session().SessionStatistics().Explain(ExplainPreemptionPriority)
		} else {
			return "not training"
		}
	case ExplainScaleOutPriority:
		return fmt.Sprintf("calculated(%f + %d * %f)", c.interactivePriorityBase, c.executions.Load(), c.ContainerStatistics().InteractivePriority())
	default:
		return ""
	}
}

// ContainerState returns the Container's current state.
func (c *BasicContainer) ContainerState() ContainerState {
	return c.containerState
}

// IsStopped returns true if the Session has been terminated.
func (c *BasicContainer) IsStopped() bool {
	return c.containerState == ContainerStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (c *BasicContainer) IsIdle() bool {
	return c.containerState == ContainerStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (c *BasicContainer) IsMigrating() bool {
	return c.containerState == ContainerStateMigrating
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (c *BasicContainer) IsTraining() bool {
	return c.containerState == ContainerStateIdle
}

func (c *BasicContainer) transition(targetState ContainerState) error {
	if c.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidTransition, c.containerState, targetState)
	}

	c.containerState = targetState
	return nil
}

// ScaleOutPriority returns the host's "scheduling-out priority", or SOP, which is defined as the time of the
// last rescheduling operation plus the frequency of training tasks multiplied by the interactive priority of the
// potential training task plus the sum of the preemption priorities of the preemptible tasks.
//
// SOP(h) = Last Rescheduling Clock + Freq(h) * IP(h) + SUM PP(h').
// To schedule out a potential task, we need to weight benefits of migration(IP) and penalty of preempting running task(s) if stay(PP).
func (c *BasicContainer) ScaleOutPriority() float64 {
	return (c.interactivePriorityBase + 1) * c.InteractivePriority()
}

// TrainingStarted should be called when the Container begins training.
func (c *BasicContainer) TrainingStarted() error {
	c.lastSpec = c.spec

	// Update resource data on the Host.
	c.host.Stats().PendingGPUsStat().Sub(c.outstandingGPUs.GPU())
	c.host.Stats().IdleGPUsStat().Sub(c.outstandingGPUs.GPU())

	c.spec.UpdateSpecGPUs(float64(c.Session().ResourceUtilization().NumGpus))
	c.outstandingGPUs = types.GPUSpec(0)

	// Processing a new training event.
	c.executions.Add(1)

	c.interactivePriorityBase = c.host.Stats().LastReschedule().Load()

	if err := c.transition(ContainerStateTraining); err != nil {
		c.log.Error("Failed to transition to state %v because: %v", ContainerStateTraining, err)
		return err
	}

	return nil
}

// TrainingStopped should be called when the Container stops training.
func (c *BasicContainer) TrainingStopped() error {
	if err := c.transition(ContainerStateIdle); err != nil {
		c.log.Error("Failed to transition to state %v because: %v", ContainerStateIdle, err)
		return err
	}

	c.outstandingGPUs = types.GPUSpec(c.spec.GPU() - c.lastSpec.GPU())
	c.spec = c.lastSpec

	c.host.Stats().PendingGPUsStat().Add(c.outstandingGPUs.GPU())
	c.host.Stats().IdleGPUsStat().Add(c.outstandingGPUs.GPU())

	return nil
}
