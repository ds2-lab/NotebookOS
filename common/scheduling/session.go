package scheduling

import (
	"github.com/Scusemua/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"time"
)

const (
	SessionStateInit              SessionState = "SESSION_INIT"               // Indicates that the Session has just been created, but its replicas have not yet been scheduled onto Hosts.
	SessionStateTraining          SessionState = "SESSION_TRAINING"           // Indicates that the Session is actively running AND one of its replicas is actively training.
	SessionStateStopped           SessionState = "SESSION_STOPPED"            // Indicates that the Session is permanently stopped.
	SessionStateIdle              SessionState = "SESSION_IDLE"               // Indicates that the Session is actively running on a Host and is NOT actively performing a task.
	SessionStateExpectingTraining SessionState = "SESSION_EXPECTING_TRAINING" // Indicates that the Session is expecting to begin training shortly, as a "execute_request" message has been forwarded, but the training has not yet began.
	SessionStateMigrating         SessionState = "SESSION_MIGRATING"          // Indicates that one or more replicas are currently migrating to new Hosts.
)

type SessionState string

func (s SessionState) String() string {
	return string(s)
}

type SessionStatistic interface {
	Add(val float64)
	Sum() float64
	Window() int64
	N() int64
	Avg() float64
	Last() float64
	LastN(n int64) float64
}

type UserSession interface {
	Lock()
	Unlock()
	AddReplica(container KernelContainer) error
	RemoveReplica(container KernelContainer) error
	RemoveReplicaById(replicaId int32) error
	ResourceSpec() types.CloneableSpec
	ID() string
	Context() context.Context
	SetContext(ctx context.Context)
	ResourceUtilization() Utilization
	SetResourceUtilization(util Utilization)
	KernelSpec() *proto.KernelSpec
	String() string
	SetExpectingTraining() promise.Promise
	MigrationStarted() promise.Promise
	MigrationComplete() promise.Promise
	SessionStatistics() SessionStatistics
	GetState() SessionState
	SessionStarted() promise.Promise
	SessionStopped() promise.Promise
	IsStopped() bool
	IsIdle() bool
	IsMigrating() bool
	IsTraining() bool
	Explain(key ExplainerEntry) string
	TrainingTime() SessionStatistic
	MigrationTime() float64
	InteractivePriority() float64
	PreemptionPriority() float64
	StartedAt() time.Time
	Duration() time.Duration
	SessionStartedTraining(container KernelContainer, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise
	SessionStoppedTraining(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise
	GetReplicaContainer(replicaId int32) (KernelContainer, bool)
}

type SessionStatistics interface {
	Explainer

	// TrainingTime returns a SessionStatistic representing the length of time that the Session typically trains for.
	TrainingTime() SessionStatistic

	// MigrationTime returns a scalar representing the length of time that the Session (or more specifically, Kernel
	// replicas of the Session) has/have historically taken to be migrated.
	MigrationTime() float64

	// InteractivePriority returns the Session's interactive priority metric.
	InteractivePriority() float64

	// PreemptionPriority returns the currently-cached value of the Session's preemption priority metric.
	// This may prompt a recalculation of the metric if the cached value is no longer valid.
	PreemptionPriority() float64

	// StartedAt returns the time.Time at which the Session began running.
	StartedAt() time.Time

	// Duration returns the time.Duration indicating how long the Session has been running.
	Duration() time.Duration
}
