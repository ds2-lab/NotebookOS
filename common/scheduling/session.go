package scheduling

import (
	"github.com/Scusemua/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/resource"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"golang.org/x/net/context"
	"time"
)

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
	AddReplica(container *Container) error
	RemoveReplica(container *Container) error
	RemoveReplicaById(replicaId int32) error
	ResourceSpec() types.CloneableSpec
	ID() string
	Context() context.Context
	SetContext(ctx context.Context)
	ResourceUtilization() *resource.Utilization
	SetResourceUtilization(util *resource.Utilization)
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
	SessionStartedTraining(container *Container, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise
	SessionStoppedTraining(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise
	GetReplicaContainer(replicaId int32) (*Container, bool)
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
