package scheduling

import (
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
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
}

type Container interface {
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

	// IsTraining returns true if the Container is actively training.
	// Otherwise, IsTraining returns false.
	IsTraining() bool

	// IsStopped returns true if the Container has been terminated.
	IsStopped() bool

	// IsIdle returns true if the Container is currently idle, meaning that it is not currently training.
	IsIdle() bool

	// IsMigrating returns true if the Container is currently migrating from one Host to another.
	IsMigrating() bool
}

type BasicContainer struct {
	log logger.Logger

	session        Session        // The Session associated with the Container.
	host           Host           // The Host on which the Container is currently scheduled.
	id             string         // The kernel ID of the Container.
	containerState ContainerState // The current state of the Container.

	executions atomic.Int32
	ipBase     float64
	ip         cache.InlineCache
	ipExplain  string
}

func NewBasicContainer(session Session) *BasicContainer {
	id := session.ID()
	container := &BasicContainer{
		id:             id,
		session:        session,
		log:            config.GetLogger(fmt.Sprintf("Container %s", id)),
		containerState: ContainerStateIdle,
	}

	container.executions.Store(0)
	container.ip.Producer = cache.FormalizeICProducer(container.getIP)
	container.ip.Validator = GetClockTimeCacheValidator()

	return container
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

func (c *BasicContainer) getIP() float64 {
	c.ip.Validator(time.Now())
	required := c.session.ResourceUtilization().NumGpusAsFloat() // float64(c.Session().Meta().GPU.GPUs)
	idleGPUs := c.host.Stats().IdleGPUs().Load()
	extras := 0.0
	extraExplain := "0.0"
	if idleGPUs > required {
		extras = idleGPUs / c.host.Stats().PendingGPUs().Load()
		extraExplain = fmt.Sprintf("%f / %f", idleGPUs, c.host.Stats().PendingGPUs().Load())
	}
	// ip := float64(c.executions) * c.session.Stats().IP() * idleGPUs / c.host.Stats().PendingGPUs().Load()
	stats := c.session.SessionStatistics()
	ip := stats.InteractivePriority() * (extras + 1)
	c.ipExplain = fmt.Sprintf("%s( * (1 + %s))", stats.Explain(ExplainInteractivePriority), extraExplain)
	// log.Printf("%v: updated cached ip %f, container:%v, potentials: %f, pending containers: %d\n", ClockTime, ip, c.session, extras+1, c.host.Stats().PendingContainers().Load())
	return ip
}

// PreemptionPriority returns the Container's preemption priority, which is equal to 0 when the Container is idle.
// When the Container is actively training, its preemption priority is equal to its Session's preemption priority.
func (c *BasicContainer) PreemptionPriority() float64 {
	if c.IsTraining() {
		return c.Session().SessionStatistics().PreemptionPriority()
	}

	return 0.0
}

// InteractivePriority returns the Container's interactive priority metric.
func (c *BasicContainer) InteractivePriority() float64 {
	return c.ip.Value().(float64)
}

// Explain returns an explanation for how the latest metric (specified using the ExplainerKey argument) was computed.
func (c *BasicContainer) Explain(key ExplainerEntry) string {
	switch key {
	case ExplainInteractivePriority:
		return c.ipExplain
	case ExplainPreemptionPriority:
		if c.IsTraining() {
			return c.Session().SessionStatistics().Explain(ExplainPreemptionPriority)
		} else {
			return "not training"
		}
	case ExplainScaleOutPriority:
		return fmt.Sprintf("calculated(%f + %d * %f)", c.ipBase, c.executions.Load(), c.ContainerStatistics().InteractivePriority())
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
