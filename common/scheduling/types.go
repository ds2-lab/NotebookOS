package scheduling

import (
	"context"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"time"
)

// AbstractSession is just an extraction of the exported methods of the Session
// struct into an interface so that we can mock the interface during unit tests.
type AbstractSession interface {
	Lock()
	Unlock()
	AddReplica(container *Container) error
	RemoveReplica(container *Container) error
	RemoveReplicaById(replicaId int32) error
	ResourceSpec() types.CloneableSpec
	ID() string
	Context() context.Context
	SetContext(ctx context.Context)
	GetCluster() Cluster
	ResourceUtilization() *ResourceUtilization
	SetResourceUtilization(util *ResourceUtilization)
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
}
