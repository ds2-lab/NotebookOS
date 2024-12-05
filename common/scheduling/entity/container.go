package entity

import (
	"fmt"
	"github.com/Scusemua/go-utils/cache"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"sync/atomic"
	"time"
)

type Container struct {
	scheduling.KernelReplica

	log logger.Logger

	session                    scheduling.UserSession    // The Session associated with the Container.
	replicaId                  int32                     // The SMR node ID of the kernel replica running within the Container.
	host                       scheduling.Host           // The Host on which the Container is currently scheduled.
	id                         string                    // The kernel ID of the Container.
	containerState             scheduling.ContainerState // The current state of the Container.
	executions                 atomic.Int32              // The number of training events processed by the Container.
	trainingStartedAt          time.Time                 // The time at which the Container started training.
	startedAt                  time.Time                 // The time at which the Container was created.
	addr                       string                    // The address of the Container.
	numTrainingEventsProcessed int                       // numTrainingEventsProcessed is the number of training events processed by this Container.

	spec *types.DecimalSpec
	// lastSpec *types.DecimalSpec

	interactivePriorityBase        float64
	interactivePriority            cache.InlineCache
	interactivePriorityExplanation string
}

type PlaceholderContainer struct {
	*Container
}

// NewContainer creates and returns a new *Container.
func NewContainer(session scheduling.UserSession, kernelReplica scheduling.KernelReplica, host scheduling.Host, kernelIp string) *Container {
	id := session.ID()
	container := &Container{
		KernelReplica:  kernelReplica,
		id:             id,
		replicaId:      kernelReplica.ReplicaID(),
		host:           host,
		session:        session,
		log:            config.GetLogger(fmt.Sprintf("Container %s-%d ", session.ID(), kernelReplica.ReplicaID())),
		containerState: scheduling.ContainerStateIdle,
		spec:           types.ToDecimalSpec(session.ResourceSpec()),
		startedAt:      time.Now(),
		addr:           kernelIp,
	}

	container.executions.Store(0)
	container.interactivePriority.Producer = cache.FormalizeICProducer(container.getInteractivePriority)
	container.interactivePriority.Validator = GetClockTimeCacheValidator()

	return container
}

func (c *Container) ToDockerContainer() *proto.DockerContainer {
	return &proto.DockerContainer{
		ContainerName:   c.ContainerID(),
		ContainerAge:    time.Since(c.StartedAt()).String(),
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
func (c *Container) GetClient() scheduling.KernelReplica {
	return c.KernelReplica
}

// SetClient sets/updates the KernelReplica associated with the Container.
func (c *Container) SetClient(client scheduling.KernelReplica) {
	c.KernelReplica = client
}

func (c *Container) ContainerStatistics() scheduling.ContainerStatistics {
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
	return fmt.Sprintf("%s-%d", c.id, c.replicaId)
}

func (c *Container) ReplicaId() int32 {
	return c.replicaId
}

func (c *Container) KernelID() string {
	return c.id
}

func (c *Container) String() string {
	return fmt.Sprintf("Container[ID=%s,ReplicaID=%d,State=%v,StartedAt=%v,Host=%v]",
		c.id, c.replicaId, c.containerState, c.startedAt, c.host)
}

func (c *Container) Session() scheduling.UserSession {
	return c.session
}

func (c *Container) Host() scheduling.Host {
	return c.host
}

func (c *Container) getInteractivePriority() float64 {
	c.interactivePriority.Validator(time.Now())
	required := float64(c.session.ResourceUtilization().GetNumGpus())
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
	c.interactivePriorityExplanation = fmt.Sprintf("%s( * (1 + %s))", stats.Explain(scheduling.ExplainInteractivePriority), extraExplain)
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
func (c *Container) Explain(key scheduling.ExplainerEntry) string {
	switch key {
	case scheduling.ExplainInteractivePriority:
		return c.interactivePriorityExplanation
	case scheduling.ExplainPreemptionPriority:
		if c.IsTraining() {
			return c.Session().SessionStatistics().Explain(scheduling.ExplainPreemptionPriority)
		} else {
			return "not training"
		}
	case scheduling.ExplainScaleOutPriority:
		return fmt.Sprintf("calculated(%f + %d * %f)", c.interactivePriorityBase, c.executions.Load(), c.ContainerStatistics().InteractivePriority())
	default:
		return ""
	}
}

// ContainerState returns the Container's current state.
func (c *Container) ContainerState() scheduling.ContainerState {
	return c.containerState
}

// IsStopped returns true if the Session has been terminated.
func (c *Container) IsStopped() bool {
	return c.containerState == scheduling.ContainerStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (c *Container) IsIdle() bool {
	return c.containerState == scheduling.ContainerStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (c *Container) IsMigrating() bool {
	return c.containerState == scheduling.ContainerStateMigrating
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (c *Container) IsTraining() bool {
	return c.containerState == scheduling.ContainerStateIdle
}

func (c *Container) transition(targetState scheduling.ContainerState) error {
	if c.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", scheduling.ErrInvalidStateTransition, c.containerState, targetState)
	}

	c.containerState = targetState
	return nil
}

func (c *Container) ResourceSpec() *types.DecimalSpec {
	return c.spec
}

func (c *Container) UpdateResourceSpec(spec *types.DecimalSpec) {
	c.spec = spec
}

// ScaleOutPriority returns the host's "scheduling-out priority", or SOP, which is defined as the time of the
// last rescheduling operation plus the frequency of training tasks multiplied by the interactive priority of the
// potential training task plus the sum of the preemption priorities of the pre-emptible tasks.
//
// SOP(h) = Last Rescheduling Clock + Freq(h) * IP(h) + SUM PP(h').
// To schedule out a potential task, we need to weight benefits of migration(IP) and penalty of preempting running task(s) if stay(PP).
func (c *Container) ScaleOutPriority() float64 {
	return (c.interactivePriorityBase + 1) * c.InteractivePriority()
}

// TrainingStartedInContainer should be called when the Container begins training.
func (c *Container) TrainingStartedInContainer( /*snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]*/ ) error {
	err := c.host.ContainerStartedTraining(c)
	if err != nil {
		return err
	}

	c.trainingStartedAt = time.Now()
	c.spec.UpdateSpecGPUs(float64(c.Session().ResourceUtilization().GetNumGpus()))
	c.spec.UpdateSpecCPUs(c.Session().ResourceUtilization().GetCpuUtilization())
	c.spec.UpdateSpecMemoryMB(c.Session().ResourceUtilization().GetMemoryUsageMb())

	// Processing a new training event.
	c.executions.Add(1)

	c.interactivePriorityBase = c.host.Stats().LastReschedule().Load()

	if err := c.transition(scheduling.ContainerStateTraining); err != nil {
		c.log.Error("Failed to transition to state %v because: %v", scheduling.ContainerStateTraining, err)
		return err
	}

	c.log.Debug("Container for replica %d of kernel \"%s\" has successfully started training. ResourceSpec: %v.",
		c.replicaId, c.id, c.spec.String())

	return nil
}

// ContainerStoppedTraining should be called when the Container stops training.
//
// This should be called by the Session's SessionStoppedTraining method.
func (c *Container) ContainerStoppedTraining( /*snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]*/ ) error {
	if err := c.transition(scheduling.ContainerStateIdle); err != nil {
		c.log.Error("Failed to transition Container to state %v because: %v", scheduling.ContainerStateIdle, err)
		return err
	}

	c.log.Debug("Training stopping after %v. Outputting Resources before training officially stops. ResourceSpec of %s: %s", time.Since(c.trainingStartedAt), c.ContainerID(), c.spec.String())
	c.log.Debug("Pending CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().PendingCPUs(), c.host.Stats().PendingMemoryMb(), c.host.Stats().PendingGPUs(), c.host.Stats().PendingVRAM())
	c.log.Debug("Idle CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().IdleCPUs(), c.host.Stats().IdleMemoryMb(), c.host.Stats().IdleGPUs(), c.host.Stats().IdleVRAM())
	c.log.Debug("Committed CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().CommittedCPUs(), c.host.Stats().CommittedMemoryMb(), c.host.Stats().CommittedGPUs(), c.host.Stats().CommittedVRAM())

	err := c.host.ContainerStoppedTraining(c)
	if err != nil {
		return err
	}

	c.log.Debug("Training stopped. Outputting Resources now that training has officially stopped.")
	c.log.Debug("Pending CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().PendingCPUs(), c.host.Stats().PendingMemoryMb(), c.host.Stats().PendingGPUs(), c.host.Stats().PendingVRAM())
	c.log.Debug("Idle CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().IdleCPUs(), c.host.Stats().IdleMemoryMb(), c.host.Stats().IdleGPUs(), c.host.Stats().IdleVRAM())
	c.log.Debug("Committed CPU: %.0f, Memory: %.2f, GPUs: %.0f, VRAM: %.2f.",
		c.host.Stats().CommittedCPUs(), c.host.Stats().CommittedMemoryMb(), c.host.Stats().CommittedGPUs(), c.host.Stats().CommittedVRAM())

	c.numTrainingEventsProcessed += 1

	return nil
}

// ContainerStopped should be called when the Container is stopped, such as when its Session is stopped.
func (c *Container) ContainerStopped() error {
	//
	// IMPORTANT
	// If I change the order of anything here, then I will need to update DistributedKernelClient::RemoveReplica,
	// as I manually call Host::ContainerRemoved from DistributedKernelClient::RemoveReplica if
	// Container::ContainerStopped returns either an ErrInvalidStateTransition error or an ErrNilHost error.
	//
	// (ErrInvalidStateTransition is returned by Container::transition in the event that an illegal transition
	// is attempted, and ErrNilHost is returned directly by ContainerStopped if the Container's host field is nil.)
	//

	if err := c.transition(scheduling.ContainerStateStopped); err != nil {
		c.log.Error("Failed to transition Container to state %v because: %v", scheduling.ContainerStateStopped, err)
		return err
	}

	if c.host == nil {
		c.log.Error("Failed to cleanly stop Container as its host is nil...")
		return scheduling.ErrNilHost
	}

	err := c.host.ContainerRemoved(c)
	if err != nil {
		c.log.Error("Failed to cleanly stop Container due to error during removal-from-host: %v", err)
		return err
	}

	c.log.Debug("scheduling.Container for replica %d of kernel %s has stopped.", c.replicaId, c.id)

	return nil
}

// NumTrainingEventsProcessed returns the number of training events processed by this particular Container.
// This is NOT (necessarily) equal to the total number of training events processed by the UserSession.
func (c *Container) NumTrainingEventsProcessed() int {
	return c.numTrainingEventsProcessed
}
