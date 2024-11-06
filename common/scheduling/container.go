package scheduling

import (
	"fmt"
	"github.com/Scusemua/go-utils/cache"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"log"
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

type Container struct {
	KernelReplica

	log logger.Logger

	session              *Session       // The Session associated with the Container.
	host                 *Host          // The Host on which the Container is currently scheduled.
	id                   string         // The kernel ID of the Container.
	containerState       ContainerState // The current state of the Container.
	executions           atomic.Int32   // The number of training events processed by the Container.
	outstandingResources types.Spec     // The number of GPUs required by the Container to train.
	trainingStartedAt    time.Time      // The time at which the Container started training.
	startedAt            time.Time      // The time at which the Container was created.
	addr                 string         // The address of the Container.

	spec     types.Spec
	lastSpec types.Spec

	interactivePriorityBase        float64
	interactivePriority            cache.InlineCache
	interactivePriorityExplanation string

	// dockerId             string         // The Docker container ID of the Container.
}

// NewContainer creates and returns a new *Container.
func NewContainer(session *Session, kernelReplica KernelReplica, host *Host, kernelIp string) *Container {
	id := session.ID()
	container := &Container{
		KernelReplica:        kernelReplica,
		id:                   id,
		host:                 host,
		session:              session,
		log:                  config.GetLogger(fmt.Sprintf("Container %s-%d ", kernelReplica.ID(), kernelReplica.ReplicaID())),
		containerState:       ContainerStateIdle,
		spec:                 session.ResourceSpec().Clone(),
		outstandingResources: kernelReplica.ResourceSpec().Clone(),
		startedAt:            time.Now(),
		addr:                 kernelIp,
	}

	container.executions.Store(0)
	container.interactivePriority.Producer = cache.FormalizeICProducer(container.getInteractivePriority)
	container.interactivePriority.Validator = GetClockTimeCacheValidator()

	return container
}

func (c *Container) ToDockerContainer() *proto.DockerContainer {
	return &proto.DockerContainer{
		ContainerName:   c.ContainerID(),
		ContainerAge:    time.Now().Sub(c.StartedAt()).String(),
		ContainerIp:     c.addr,
		ContainerStatus: "running", // TODO: This may be inaccurate during migrations and such.
		Valid:           true,
	}
}

// StartedAt returns a time.Time encoding the time at which the Container was created.
// (Specifically, it is the time at which the Container struct was instantiated.)
func (c *Container) StartedAt() time.Time {
	return c.startedAt
}

// Address returns the address/IP of the Container.
func (c *Container) Address() string {
	return c.addr
}

// GetClient returns the KernelReplica associated with the Container.
func (c *Container) GetClient() KernelReplica {
	return c.KernelReplica
}

// OutstandingResources returns the Resources required by the Container to begin training.
func (c *Container) OutstandingResources() types.Spec {
	return c.outstandingResources
}

// SetClient sets/updates the KernelReplica associated with the Container.
func (c *Container) SetClient(client KernelReplica) {
	c.KernelReplica = client
}

func (c *Container) ContainerStatistics() ContainerStatistics {
	return c
}

//// DockerContainerID returns the Docker container ID of the Container.
//func (c *Container) DockerContainerID() string {
//	return c.dockerId
//}
//
//// SetDockerContainerID sets the Docker container ID of the Container to the specified value.
//func (c *Container) SetDockerContainerID(dockerId string) {
//	c.dockerId = dockerId
//}

// ContainerID returns the "container ID", which is a combination of the kernel ID and the replica ID.
func (c *Container) ContainerID() string {
	return fmt.Sprintf("%s-%d", c.id, c.KernelReplica.ReplicaID())
}

func (c *Container) KernelID() string {
	return c.id
}

func (c *Container) String() string {
	return fmt.Sprintf("Container[ID=%s,ReplicaID=%d,State=%v,StartedAt=%v,Host=%v]",
		c.id, c.KernelReplica.ReplicaID(), c.containerState, c.startedAt, c.host)
}

func (c *Container) Session() *Session {
	return c.session
}

func (c *Container) Host() *Host {
	return c.host
}

func (c *Container) getInteractivePriority() float64 {
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
func (c *Container) InteractivePriority() float64 {
	return c.interactivePriority.Value().(float64)
}

func (c *Container) InvalidateInteractivePriority() {
	c.interactivePriority.Invalidate()
}

// PreemptionPriority returns the Container's preemption priority, which is equal to 0 when the Container is idle.
// When the Container is actively training, its preemption priority is equal to its Session's preemption priority.
func (c *Container) PreemptionPriority() float64 {
	if c.IsTraining() {
		return c.Session().SessionStatistics().PreemptionPriority()
	}

	return 0.0
}

// Explain returns an explanation for how the latest metric (specified using the ExplainerKey argument) was computed.
func (c *Container) Explain(key ExplainerEntry) string {
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
func (c *Container) ContainerState() ContainerState {
	return c.containerState
}

// IsStopped returns true if the Session has been terminated.
func (c *Container) IsStopped() bool {
	return c.containerState == ContainerStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (c *Container) IsIdle() bool {
	return c.containerState == ContainerStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (c *Container) IsMigrating() bool {
	return c.containerState == ContainerStateMigrating
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (c *Container) IsTraining() bool {
	return c.containerState == ContainerStateIdle
}

func (c *Container) transition(targetState ContainerState) error {
	if c.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidStateTransition, c.containerState, targetState)
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
func (c *Container) ScaleOutPriority() float64 {
	return (c.interactivePriorityBase + 1) * c.InteractivePriority()
}

// TrainingStartedInContainer should be called when the Container begins training.
func TrainingStartedInContainer(c *Container, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error {
	c.lastSpec = c.spec

	if snapshot != nil {
		err := ApplyResourceSnapshotToHost(c.host, snapshot)
		if err != nil {
			c.log.Warn("Failed to apply Resource Snapshot: %v", err)
		}
	} else {
		log.Fatalf("Container %s did not receive Resource Snapshot on TrainingStartedInContainer...", c.id)
	}

	c.trainingStartedAt = time.Now()
	c.spec.UpdateSpecGPUs(float64(c.Session().ResourceUtilization().NumGpus))
	c.spec.UpdateSpecCPUs(c.Session().ResourceUtilization().CpuUtilization)
	c.spec.UpdateSpecMemoryMB(c.Session().ResourceUtilization().MemoryUsageMb)
	c.outstandingResources = &types.Float64Spec{
		GPUs:      0,
		Millicpus: 0,
		MemoryMb:  0,
		VRam:      0,
	}

	// Processing a new training event.
	c.executions.Add(1)

	c.interactivePriorityBase = c.host.Stats().LastReschedule().Load()

	if err := c.transition(ContainerStateTraining); err != nil {
		c.log.Error("Failed to transition to state %v because: %v", ContainerStateTraining, err)
		return err
	}

	return nil
}

// ContainerStoppedTraining should be called when the Container stops training.
//
// This should be called by the Session's SessionStoppedTraining method.
func ContainerStoppedTraining(c *Container, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) error {
	if err := c.transition(ContainerStateIdle); err != nil {
		c.log.Error("Failed to transition Container to state %v because: %v", ContainerStateIdle, err)
		return err
	}

	c.log.Debug("Training stopping after %v. Outputting Resources before training officially stops.", time.Since(c.trainingStartedAt))
	c.log.Debug("Outstanding CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.outstandingResources.CPU(), c.outstandingResources.MemoryMB(), c.outstandingResources.GPU(), c.outstandingResources.VRAM())
	c.log.Debug("Pending CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().PendingCPUs(), c.host.Stats().PendingMemoryMb(), c.host.Stats().PendingGPUs(), c.host.Stats().PendingVRAM())
	c.log.Debug("Idle CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().IdleCPUs(), c.host.Stats().IdleMemoryMb(), c.host.Stats().IdleGPUs(), c.host.Stats().IdleVRAM())
	c.log.Debug("Committed CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().CommittedCPUs(), c.host.Stats().CommittedMemoryMb(), c.host.Stats().CommittedGPUs(), c.host.Stats().CommittedVRAM())

	c.outstandingResources = &types.Float64Spec{
		GPUs:      c.spec.GPU(),
		Millicpus: c.spec.CPU(),
		MemoryMb:  c.spec.MemoryMB(),
		VRam:      c.spec.VRAM(),
	}
	c.spec = c.lastSpec

	if snapshot != nil {
		err := ApplyResourceSnapshotToHost(c.host, snapshot)
		if err != nil {
			c.log.Warn("Failed to apply Resource Snapshot: %v", err)
		} else {
			c.log.Debug("Training stopped. Outputting Resources now that training has officially stopped.")
			c.log.Debug("Outstanding CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
				c.outstandingResources.CPU(), c.outstandingResources.MemoryMB(), c.outstandingResources.GPU(), c.outstandingResources.VRAM())
			c.log.Debug("Pending CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
				c.host.Stats().PendingCPUs(), c.host.Stats().PendingMemoryMb(), c.host.Stats().PendingGPUs(), c.host.Stats().PendingVRAM())
			c.log.Debug("Idle CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
				c.host.Stats().IdleCPUs(), c.host.Stats().IdleMemoryMb(), c.host.Stats().IdleGPUs(), c.host.Stats().IdleVRAM())
			c.log.Debug("Committed CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
				c.host.Stats().CommittedCPUs(), c.host.Stats().CommittedMemoryMb(), c.host.Stats().CommittedGPUs(), c.host.Stats().CommittedVRAM())
		}
	} else {
		c.log.Warn("Received nil ResourceSnapshot. This should only happen if we're about to be terminated.")
	}

	return nil
}

// ContainedStopped should be called when the Container is stopped, such as when its Session is stopped.
func (c *Container) ContainedStopped() error {
	//
	// IMPORTANT
	// If I change the order of anything here, then I will need to update DistributedKernelClient::RemoveReplica,
	// as I manually call Host::ContainerRemoved from DistributedKernelClient::RemoveReplica if
	// Container::ContainerStopped returns either an ErrInvalidStateTransition error or an ErrNilHost error.
	//
	// (ErrInvalidStateTransition is returned by Container::transition in the event that an illegal transition
	// is attempted, and ErrNilHost is returned directly by ContainedStopped if the Container's host field is nil.)
	//

	if err := c.transition(ContainerStateStopped); err != nil {
		c.log.Error("Failed to transition Container to state %v because: %v", ContainerStateStopped, err)
		return err
	}

	if c.host == nil {
		c.log.Error("Failed to cleanly stop Container as its host is nil...")
		return ErrNilHost
	}

	err := c.host.ContainerRemoved(c)
	if err != nil {
		c.log.Error("Failed to cleanly stop Container due to error during removal-from-host: %v", err)
		return err
	}

	c.log.Debug("scheduling.Container for replica %d of kernel %s has stopped.", c.ReplicaID(), c.id)

	return nil
}
