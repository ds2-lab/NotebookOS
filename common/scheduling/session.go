package scheduling

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/client"
	"math"
	"time"
)

const (
	SessionStateInit      SessionState = "SESSION_INIT"      // Indicates that the Session has just been created, but its replicas have not yet been scheduled onto Hosts.
	SessionStateTraining  SessionState = "SESSION_TRAINING"  // Indicates that the Session is actively-running AND is actively performing a task locally.
	SessionStateStopped   SessionState = "SESSION_STOPPED"   // Indicates that the Session is permanently stopped.
	SessionStateIdle      SessionState = "SESSION_IDLE"      // Indicates that the Session is actively-running on a Host and is NOT actively performing a task.
	SessionStateMigrating SessionState = "SESSION_MIGRATING" // Indicates that one or more replicas are currently migrating to new Hosts.

	ExplainInteractivePriority ExplainerEntry = "explain_ip"
	ExplainPreemptionPriority  ExplainerEntry = "explain_pp"
)

var (
	ErrInvalidTransition           = errors.New("invalid session state transition requested")
	ErrInvalidExplanationRequested = errors.New("invalid explanation requested")
)

type SessionState string

type ExplainerEntry string

type Explainer interface {
	Explain(key ExplainerEntry) string
}

type SessionStatistics interface {
	Explainer

	// TrainingTime returns a SessionStatistic representing the length of time that the Session typically trains for.
	TrainingTime() SessionStatistic

	// MigrationTime returns a scalar representing the length of time that the Session (or more specifically, Kernel
	// replicas of the Session) has/have historically taken to be migrated.
	MigrationTime() float64

	// InteractivePriority returns the UserSession's interactive priority metric.
	InteractivePriority() float64

	// PreemptionPriority returns the currently-cached value of the UserSession's preemption priority metric.
	// This may prompt a recalculation of the metric if the cached value is no longer valid.
	PreemptionPriority() float64

	// StartedAt returns the time.Time at which the Session began running.
	StartedAt() time.Time

	// Duration returns the time.Duration indicating how long the Session has been running.
	Duration() time.Duration
}

type Session interface {
	Kernel

	// SetID sets the id of the session.
	// Session will be created first and acquires id from kernel_info_request.
	// SetID(string)

	// IsTraining returns true if the Session is actively training.
	// Otherwise, IsTraining returns false.
	IsTraining() bool

	// IsStopped returns true if the Session has been terminated.
	IsStopped() bool

	// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
	IsIdle() bool

	// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
	IsMigrating() bool

	// TrainingStarted should be called when one of the Session's Kernel replicas begins training.
	TrainingStarted(host Host) promise.Promise

	// TrainingStopped should be called when the actively-training Kernel replica of the Session stops training.
	TrainingStopped() promise.Promise

	// MigrationStarted should be called when one of the replicas of the Session begins the
	// process of migrating from its current Host to another Host.
	MigrationStarted() promise.Promise

	// MigrationComplete should be called when the migrating replica of the Session has finished its migration
	// to another host.
	MigrationComplete() promise.Promise

	// SessionStopped should be called when the Session is terminated.
	SessionStopped() promise.Promise

	// SessionStatistics returns the SessionStatistics instance associated with the Session.
	SessionStatistics() SessionStatistics

	// GetState returns the current state of the Session in the form of a SessionState.
	GetState() SessionState
}

type UserSession struct {
	client.DistributedKernelClient

	instance      Session
	ctx           context.Context // The Session's context.
	id            string          // Session/kernel ID.
	sessionState  SessionState    // The current state of the Session.
	trainingStart time.Time       // Time at which the current training began.

	////////////////////////
	// Session Statistics //
	////////////////////////

	resourceUtilization            *ResourceUtilization // Current/latest resource usage statistics.
	startedAt                      time.Time            // Time at which the session began running.
	trainingTime                   SessionStatistic     // Moving average of training times.
	migrationTime                  SessionStatistic     // Moving average of migration times.
	interactivePriority            float64              // Interactivity Priority
	interactivePriorityExplanation string               // Explanation of current Interactivity Priority value.
	preemptionPriority             cache.InlineCache    // Preemption Priority
	preemptionPriorityExplanation  string               // Explanation of current  Preemption Priority value.

	ipHistory            *ValueHistory[float64]
	ppHistory            *ValueHistory[float64]
	trainingTimeHistory  *ValueHistory[time.Duration]
	migrationTimeHistory *ValueHistory[time.Duration]

	log logger.Logger
}

func (s *UserSession) Context() context.Context {
	return s.ctx
}

func (s *UserSession) SetContext(ctx context.Context) {
	s.ctx = ctx
}

func NewUserSession(ctx context.Context, kernel client.DistributedKernelClient, resourceUtilization *ResourceUtilization, opts *CoreOptions) *UserSession {
	session := &UserSession{
		DistributedKernelClient: kernel,
		ctx:                     ctx,
		id:                      kernel.ID(),
		log:                     config.GetLogger(fmt.Sprintf("Session %s ", kernel.ID())),
		sessionState:            SessionStateInit,
		startedAt:               time.Now(),
		trainingTime:            NewSessionStatistic(opts.ExecutionTimeSamplingWindow),
		migrationTime:           NewSessionStatistic(opts.MigrationTimeSamplingWindow),
		resourceUtilization:     resourceUtilization,
		ipHistory:               NewValueHistory[float64]("Interactive Priority", "float64"),
		ppHistory:               NewValueHistory[float64]("Preemption Priority", "float64"),
		trainingTimeHistory:     NewValueHistory[time.Duration]("Training Time", "time.Duration"),
		migrationTimeHistory:    NewValueHistory[time.Duration]("Migration Time", "time.Duration"),
	}

	session.updateInteractivePriority("session started")

	session.preemptionPriority.Producer = cache.FormalizeICProducer(session.calculatePreemptionPriority)
	session.preemptionPriority.Validator = GetClockTimeCacheValidator()
	session.instance = session
	return session
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (s *UserSession) IsTraining() bool {
	return s.sessionState == SessionStateIdle
}

// TrainingStarted should be called when one of the Session's Kernel replicas begins training.
func (s *UserSession) TrainingStarted(host Host) promise.Promise {
	if err := s.transition(SessionStateTraining); err != nil {
		s.log.Warn("Failed to start training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	s.trainingStart = time.Now()

	return promise.Resolved(s.instance)
}

// TrainingStopped should be called when the actively-training Kernel replica of the Session stops training.
func (s *UserSession) TrainingStopped() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to stop training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	trainingDuration := time.Now().Sub(s.trainingStart)
	s.trainingTime.Add(float64(trainingDuration) / float64(time.Second))

	s.log.Debug("Session %s stopped training after %v.", s.id, trainingDuration)

	s.trainingTimeHistory.AddValue(trainingDuration)

	return promise.Resolved(s.instance)
}

// MigrationStarted should be called when one of the replicas of the Session begins the
// process of migrating from its current Host to another Host.
func (s *UserSession) MigrationStarted() promise.Promise {
	if err := s.transition(SessionStateMigrating); err != nil {
		s.log.Warn("Failed to initiate migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// MigrationComplete should be called when the migrating replica of the Session has finished its migration
// to another host.
func (s *UserSession) MigrationComplete() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to conclude migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// SessionStatistics returns the SessionStatistics instance associated with the Session.
func (s *UserSession) SessionStatistics() SessionStatistics {
	return s
}

// GetState returns the current state of the Session in the form of a SessionState.
func (s *UserSession) GetState() SessionState {
	return s.sessionState
}

// SessionStopped should be called when the Session is terminated.
func (s *UserSession) SessionStopped() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// IsStopped returns true if the Session has been terminated.
func (s *UserSession) IsStopped() bool {
	return s.sessionState == SessionStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (s *UserSession) IsIdle() bool {
	return s.sessionState == SessionStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (s *UserSession) IsMigrating() bool {
	return s.sessionState == SessionStateMigrating
}

func (s *UserSession) transition(targetState SessionState) error {
	if s.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidTransition, s.sessionState, targetState)
	}

	s.sessionState = targetState
	return nil
}

////////////////////////
// Session Statistics //
////////////////////////

func (s *UserSession) Explain(key ExplainerEntry) string {
	switch key {
	case ExplainInteractivePriority:
		return s.interactivePriorityExplanation
	case ExplainPreemptionPriority:
		return s.preemptionPriorityExplanation
	default:
		return fmt.Errorf("%w: \"%s\"", ErrInvalidExplanationRequested, key).Error()
	}
}

func (s *UserSession) TrainingTime() SessionStatistic {
	return s.trainingTime
}

func (s *UserSession) MigrationTime() float64 {
	return s.migrationTime.Avg()
}

// InteractivePriority returns the UserSession's interactive priority metric.
func (s *UserSession) InteractivePriority() float64 {
	return s.interactivePriority
}

// updateInteractivePriority recalculates and subsequently returns the UserSession's InteractivePriority
// statistic/metric.
//
// This should be called when the UserSession stops training.
func (s *UserSession) updateInteractivePriority(reason string) float64 {
	if s.TrainingTime().N() < 1 {
		s.interactivePriority = 100000 // obsoleted: float64(s.meta.GPU.GPUs) * s.MigrationTime()
		s.interactivePriorityExplanation = "initialization(no training history)"
	} else {
		s.interactivePriority = float64(s.resourceUtilization.NumGpus) * math.Pow(s.MigrationTime(), 2.0) / s.TrainingTime().Avg()
		s.interactivePriorityExplanation = fmt.Sprintf("update after %s(%d * %.2f^2 / %.2f)", reason, s.resourceUtilization.NumGpus, s.MigrationTime(), s.TrainingTime().Avg())
	}

	return s.interactivePriority
}

// PreemptionPriority returns the currently-cached value of the UserSession's preemption priority metric.
// This may prompt a recalculation of the metric if the cached value is no longer valid.
func (s *UserSession) PreemptionPriority() float64 {
	return s.preemptionPriority.Value().(float64)
}

// calculatePreemptionPriority manually calculates and returns the preemption priority of the Session.
// This is also used by the cache.InlineCache that "automatically" maintains the PreemptionPriority of the Session.
func (s *UserSession) calculatePreemptionPriority() float64 {
	s.preemptionPriority.Validator(time.Now())

	if !s.IsTraining() {
		s.preemptionPriorityExplanation = "is not training"
		return 0.0
	} else {
		s.preemptionPriorityExplanation = "is training"

		return float64(s.resourceUtilization.NumGpus) * s.MigrationTime()
	}
}

func (s *UserSession) StartedAt() time.Time {
	return s.startedAt
}

func (s *UserSession) Duration() time.Duration {
	return time.Now().Sub(s.startedAt)
}
