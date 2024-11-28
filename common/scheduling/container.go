package scheduling

import (
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/types"
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
type KernelContainer interface {
	Address() string
	ContainerStopped() error
	ContainerID() string
	ContainerState() ContainerState
	ContainerStatistics() ContainerStatistics
	ContainerStoppedTraining() error
	Explain(key ExplainerEntry) string
	GetClient() KernelReplica
	Host() Host
	InteractivePriority() float64
	InvalidateInteractivePriority()
	IsIdle() bool
	IsMigrating() bool
	IsStopped() bool
	IsTraining() bool
	KernelID() string
	PreemptionPriority() float64
	ReplicaId() int32
	ResourceSpec() *types.DecimalSpec
	ScaleOutPriority() float64
	Session() UserSession
	SetClient(client KernelReplica)
	SetHost(Host)
	StartedAt() time.Time
	String() string
	ToDockerContainer() *proto.DockerContainer
	TrainingStartedInContainer() error
}
