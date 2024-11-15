package scheduling

import (
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
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
	ToDockerContainer() *proto.DockerContainer
	StartedAt() time.Time
	Address() string
	GetClient() KernelReplica
	SetClient(client KernelReplica)
	ContainerStatistics() ContainerStatistics
	ContainerID() string
	ReplicaId() int32
	KernelID() string
	String() string
	Session() UserSession
	Host() Host
	InteractivePriority() float64
	InvalidateInteractivePriority()
	PreemptionPriority() float64
	Explain(key ExplainerEntry) string
	ContainerState() ContainerState
	IsStopped() bool
	IsIdle() bool
	IsMigrating() bool
	IsTraining() bool
	ResourceSpec() *types.DecimalSpec
	ScaleOutPriority() float64
	ContainedStopped() error
}
