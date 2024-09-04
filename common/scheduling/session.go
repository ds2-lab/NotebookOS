package scheduling

import (
	"context"
	"errors"
	"fmt"
	"github.com/mason-leap-lab/go-utils/cache"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/mason-leap-lab/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"math"
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

var (
	ErrInvalidTransition           = errors.New("invalid session state transition requested")
	ErrInvalidExplanationRequested = errors.New("invalid explanation requested")
	ErrInvalidContainer            = errors.New("the specified or provided container is not valid")
)

type SessionState string

type SessionStatistic interface {
	Add(val float64)
	Sum() float64
	Window() int64
	N() int64
	Avg() float64
	Last() float64
	LastN(n int64) float64
}

func NewSessionStatistic(window int64) SessionStatistic {
	if window > 0 {
		return types.NewMovingStat(window, 0, make([]float64, window), 0, [2]float64{0, 0}, 0, 1)
	} else if window < 0 {
		panic("Negative window size (meaning no window) is not yet supported.")
	} else {
		panic("Window size cannot be 0.")
	}
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

type Session struct {
	instance *Session

	cluster           Cluster         // The Cluster in which this Session exists.
	ctx               context.Context // The Session's context.
	id                string          // Session/kernel ID.
	sessionState      SessionState    // The current state of the Session.
	trainingStart     time.Time       // Time at which the current training began.
	migrationStart    time.Time       // Time at which the migration began.
	containers        []*Container    // The kernel replicas belonging to this Session.
	trainingContainer *Container      // The Container that is actively training.
	resourceSpec      types.Spec      // The (current) resource requirements of the Session.

	////////////////////////
	// Session Statistics //
	////////////////////////

	kernelSpec                     *proto.KernelSpec    // The kernel resourceSpec of the associated kernel.
	resourceUtilization            *ResourceUtilization // Current/latest resource usage statistics.
	startedAt                      time.Time            // Time at which the session began running.
	trainingTime                   SessionStatistic     // Moving average of training times.
	migrationTime                  SessionStatistic     // Moving average of migration times.
	interactivePriority            float64              // Interactivity Priority
	interactivePriorityExplanation string               // Explanation of current Interactivity Priority value.
	preemptionPriority             cache.InlineCache    // Preemption Priority
	preemptionPriorityExplanation  string               // Explanation of current  Preemption Priority value.

	interactivePriorityHistory *ValueHistory[float64]
	preemptionPriorityHistory  *ValueHistory[float64]
	trainingTimeHistory        *ValueHistory[time.Duration]
	migrationTimeHistory       *ValueHistory[time.Duration]

	log logger.Logger
}

func NewUserSession(ctx context.Context, id string, kernelSpec *proto.KernelSpec, resourceUtilization *ResourceUtilization, cluster Cluster, opts *CoreOptions) *Session {
	session := &Session{
		kernelSpec:          kernelSpec,
		resourceSpec:        types.FullSpecFromKernelSpec(kernelSpec),
		ctx:                 ctx,
		cluster:             cluster,
		id:                  id,
		log:                 config.GetLogger(fmt.Sprintf("Session %s ", id)),
		sessionState:        SessionStateInit,
		startedAt:           time.Now(),
		trainingTime:        NewSessionStatistic(opts.ExecutionTimeSamplingWindow),
		migrationTime:       NewSessionStatistic(opts.MigrationTimeSamplingWindow),
		resourceUtilization: resourceUtilization,

		interactivePriorityHistory: NewValueHistory[float64]("Interactive Priority", "float64"),
		preemptionPriorityHistory:  NewValueHistory[float64]("Preemption Priority", "float64"),
		trainingTimeHistory:        NewValueHistory[time.Duration]("Training Time", "time.Duration"),
		migrationTimeHistory:       NewValueHistory[time.Duration]("Migration Time", "time.Duration"),
		containers:                 make([]*Container, 0, opts.NumReplicas),
	}

	initialInteractivePriority := session.updateInteractivePriority("session started")
	session.interactivePriorityHistory.AddValue(initialInteractivePriority)

	session.preemptionPriority.Producer = cache.FormalizeICProducer(session.calculatePreemptionPriority)
	session.preemptionPriority.Validator = GetClockTimeCacheValidator()
	session.instance = session

	return session
}

func (s *Session) ResourceSpec() types.Spec {
	return s.resourceSpec
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) Context() context.Context {
	return s.ctx
}

func (s *Session) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// GetCluster returns the Cluster in which this Session exists.
func (s *Session) GetCluster() Cluster {
	return s.cluster
}

// ResourceUtilization returns the current ResourceUtilization of the Session.
func (s *Session) ResourceUtilization() *ResourceUtilization {
	return s.resourceUtilization
}

func (s *Session) KernelSpec() *proto.KernelSpec {
	return s.kernelSpec
}

func (s *Session) String() string {
	return fmt.Sprintf("Session[ID=%s]", s.id)
}

// SetExpectingTraining attempts to transition the Session to the SessionStateExpectingTraining state.
//
// Returns a promise.Promise, which will be resolved with an error if the Session is in any of the following states:
// SessionStateStopped, SessionStateTraining.
func (s *Session) SetExpectingTraining() promise.Promise {
	if s.IsTraining() {
		s.log.Error("Session cannot transition to state \"%s\" -- Session is already training!", SessionStateExpectingTraining)
		err := fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidTransition, s.sessionState, SessionStateExpectingTraining)
		return promise.Resolved(s.instance, err)
	}

	if err := s.transition(SessionStateExpectingTraining); err != nil {
		s.log.Error("Could not transition to state \"%s\" because: %v", SessionStateExpectingTraining, err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// TrainingStarted should be called when one of the Session's Kernel replicas begins training.
func (s *Session) TrainingStarted(container *Container) promise.Promise {
	if err := s.transition(SessionStateTraining); err != nil {
		s.log.Warn("Failed to start training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	// Verify that the specified Container is indeed one of our replica containers.
	found := false
	for _, replica := range s.containers {
		if replica == container {
			found = true
			break
		}
	}

	// If the specified Container is NOT one of our replica containers, then we'll resolve with an error.
	if !found {
		s.log.Error("Specified container for training is not found in replicas: %v", container)
		return promise.Resolved(s.instance, ErrInvalidContainer)
	}

	s.trainingContainer = container
	if err := s.trainingContainer.TrainingStarted(); err != nil {
		s.log.Error("Failed to start training in container %s: %v", container.String(), err)
		return promise.Resolved(s.instance, err)
	}

	s.trainingStart = time.Now()

	s.log.Debug("Container %s began training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().ID())

	return promise.Resolved(s.instance)
}

// TrainingStopped should be called when the actively-training Kernel replica of the Session stops training.
func (s *Session) TrainingStopped() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to stop training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if err := s.trainingContainer.TrainingStopped(); err != nil {
		s.log.Error("Failed to stop training in active container: %v", err)
		return promise.Resolved(s.instance, err)
	}

	trainingDuration := time.Now().Sub(s.trainingStart)
	s.trainingTimeHistory.AddValue(trainingDuration)
	s.trainingTime.Add(float64(trainingDuration) / float64(time.Second))

	latestInteractivePriority := s.updateInteractivePriority("training stopped")
	s.interactivePriorityHistory.AddValue(latestInteractivePriority)

	s.log.Debug("Container %s has stopped training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().ID())
	return promise.Resolved(s.instance)
}

// MigrationStarted should be called when one of the replicas of the Session begins the
// process of migrating from its current Host to another Host.
func (s *Session) MigrationStarted() promise.Promise {
	if err := s.transition(SessionStateMigrating); err != nil {
		s.log.Warn("Failed to initiate migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	s.migrationStart = time.Now()
	return promise.Resolved(s.instance)
}

// MigrationComplete should be called when the migrating replica of the Session has finished its migration
// to another host.
func (s *Session) MigrationComplete() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to conclude migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	migrationDuration := time.Now().Sub(s.migrationStart)
	s.migrationTimeHistory.AddValue(migrationDuration)
	s.migrationTime.Add(float64(migrationDuration) / float64(time.Second))
	s.log.Debug("Migration completed in %v.", migrationDuration)
	return promise.Resolved(s.instance)
}

// SessionStatistics returns the SessionStatistics instance associated with the Session.
func (s *Session) SessionStatistics() SessionStatistics {
	return s
}

// GetState returns the current state of the Session in the form of a SessionState.
func (s *Session) GetState() SessionState {
	return s.sessionState
}

// SessionStarted should be called when the Session is scheduled successfully, meaning that all the Session's
// replicas (Containers) have successfully been scheduled and started running.
func (s *Session) SessionStarted() promise.Promise {
	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// SessionStopped should be called when the Session is terminated.
func (s *Session) SessionStopped() promise.Promise {
	if err := s.transition(SessionStateStopped); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// IsStopped returns true if the Session has been terminated.
func (s *Session) IsStopped() bool {
	return s.sessionState == SessionStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (s *Session) IsIdle() bool {
	return s.sessionState == SessionStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (s *Session) IsMigrating() bool {
	return s.sessionState == SessionStateMigrating
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (s *Session) IsTraining() bool {
	return s.sessionState == SessionStateTraining
}

func (s *Session) transition(targetState SessionState) error {
	if s.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidTransition, s.sessionState, targetState)
	}

	s.log.Debug("Attempting to transition from state \"%v\" to state \"%v\"", s.sessionState, targetState)
	s.sessionState = targetState
	s.log.Debug("Successfully transitioned from state \"%v\" to state \"%v\"", s.sessionState, targetState)
	return nil
}

////////////////////////
// Session Statistics //
////////////////////////

func (s *Session) Explain(key ExplainerEntry) string {
	switch key {
	case ExplainInteractivePriority:
		return s.interactivePriorityExplanation
	case ExplainPreemptionPriority:
		return s.preemptionPriorityExplanation
	default:
		return fmt.Errorf("%w: \"%s\"", ErrInvalidExplanationRequested, key).Error()
	}
}

func (s *Session) TrainingTime() SessionStatistic {
	return s.trainingTime
}

func (s *Session) MigrationTime() float64 {
	return s.migrationTime.Avg()
}

// InteractivePriority returns the Session's interactive priority metric.
func (s *Session) InteractivePriority() float64 {
	return s.interactivePriority
}

// updateInteractivePriority recalculates and subsequently returns the Session's InteractivePriority statistic/metric.
//
// This should be called when the Session stops training.
func (s *Session) updateInteractivePriority(reason string) float64 {
	if s.TrainingTime().N() < 1 {
		s.interactivePriority = 100000 // obsoleted: float64(s.meta.GPU.GPUs) * s.MigrationTime()
		s.interactivePriorityExplanation = "initialization(no training history)"
	} else {
		s.interactivePriority = float64(s.resourceUtilization.NumGpus) * math.Pow(s.MigrationTime(), 2.0) / s.TrainingTime().Avg()
		s.interactivePriorityExplanation = fmt.Sprintf("update after %s(%d * %.2f^2 / %.2f)", reason, s.resourceUtilization.NumGpus, s.MigrationTime(), s.TrainingTime().Avg())
	}

	return s.interactivePriority
}

// PreemptionPriority returns the currently-cached value of the Session's preemption priority metric.
// This may prompt a recalculation of the metric if the cached value is no longer valid.
func (s *Session) PreemptionPriority() float64 {
	return s.preemptionPriority.Value().(float64)
}

// calculatePreemptionPriority manually calculates and returns the preemption priority of the Session.
// This is also used by the cache.InlineCache that "automatically" maintains the PreemptionPriority of the Session.
func (s *Session) calculatePreemptionPriority() (preemptionPriority float64) {
	s.preemptionPriority.Validator(time.Now())

	if !s.IsTraining() {
		s.preemptionPriorityExplanation = "is not training"
		preemptionPriority = 0.0
	} else {
		s.preemptionPriorityExplanation = "is training"

		preemptionPriority = float64(s.resourceUtilization.NumGpus) * s.MigrationTime()
	}

	s.preemptionPriorityHistory.AddValue(preemptionPriority)
	return
}

func (s *Session) StartedAt() time.Time {
	return s.startedAt
}

func (s *Session) Duration() time.Duration {
	return time.Now().Sub(s.startedAt)
}
