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
	"log"
	"math"
	"sync"
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
	ErrInvalidContainer            = errors.New("the specified or provided container is invalid")
	ErrMissingTrainingContainer    = errors.New("session is training, but its \"training container\" is nil")
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

	cluster           Cluster              // The Cluster in which this Session exists.
	ctx               context.Context      // The Session's context.
	id                string               // Session/kernel ID.
	sessionState      SessionState         // The current state of the Session.
	trainingStart     time.Time            // Time at which the current training began.
	migrationStart    time.Time            // Time at which the migration began.
	containers        map[int32]*Container // The kernel replicas belonging to this Session.
	trainingContainer *Container           // The Container that is actively training.
	resourceSpec      types.CloneableSpec  // The (current) resource requirements of the Session.

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

	mu sync.Mutex

	log logger.Logger
}

func NewUserSession(ctx context.Context, id string, kernelSpec *proto.KernelSpec, resourceUtilization *ResourceUtilization, cluster Cluster, opts *ClusterSchedulerOptions) *Session {
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
		containers:                 make(map[int32]*Container),
	}

	initialInteractivePriority := session.updateInteractivePriority("session started")
	session.interactivePriorityHistory.AddValue(initialInteractivePriority)

	session.preemptionPriority.Producer = cache.FormalizeICProducer(session.calculatePreemptionPriority)
	session.preemptionPriority.Validator = GetClockTimeCacheValidator()
	session.instance = session

	return session
}

// Lock locks the Session.
func (s *Session) Lock() {
	s.mu.Lock()
}

// Unlock unlocks the Session.
func (s *Session) Unlock() {
	s.mu.Unlock()
}

// AddReplica adds a given Container to the Session's slice of Containers.
//
// AddReplica will return an error if the specified Container is nil, the ID of the Container does not match the Session's
// ID (as the IDs correspond to the kernel IDs and thus should be equal), or if there is already a Container with the
// same replica ID (i.e., SMR node ID) registered with the Session.
//
// On success, AddReplica will return nil.
//
// Note: this method is thread-safe.
func (s *Session) AddReplica(container *Container) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if container == nil {
		return fmt.Errorf("%w: container is nil", ErrInvalidContainer)
	}

	if container.KernelID() != s.id {
		return fmt.Errorf("%w: ID mismatch (session ID=\"%s\", container ID=\"%s\")", ErrInvalidContainer, s.id, container.KernelID())
	}

	// Ensure we don't already have a replica with this SMR Node ID.
	if _, loaded := s.containers[container.ReplicaID()]; loaded {
		return fmt.Errorf("%w: session already has container for replica %d registered", ErrInvalidContainer, container.ReplicaID())
	}

	s.containers[container.ReplicaID()] = container

	//for _, replica := range s.containers {
	//	if replica.ReplicaID() == container.ReplicaID() {
	//		return fmt.Errorf("%w: session already has container for replica %d registered", ErrInvalidContainer, container.ReplicaID())
	//	}
	//}

	//s.containers = append(s.containers, container)

	return nil
}

// unsafeRemoveReplica removes the specified Container from the Session's replicas.
//
// unsafeRemoveReplica does not perform any sort of safety or validity checks, and no locks are acquired.
//
// Important: unsafeRemoveReplica must be called from a context in which the Session's mutex has already been acquired.
func (s *Session) unsafeRemoveReplica(container *Container) {
	if container == nil {
		log.Fatalf("Cannot remove nil Container from Session %s", s.id)
	}

	delete(s.containers, container.ReplicaID())
}

// RemoveReplica removes the specified Container from the Session's replicas.
//
// RemoveReplica returns an error if the specified Container is not one of the Session's replicas.
// On success, RemoveReplica returns nil.
//
// Note: this method is thread-safe.
func (s *Session) RemoveReplica(container *Container) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	replicaId := container.ReplicaID()
	if _, loaded := s.containers[replicaId]; !loaded {
		s.log.Error("Cannot remove replica %d from Session %s. No replica found with specified SMR node ID.",
			replicaId, s.id)
		return fmt.Errorf("%w: session %s does not have a replica with ID=%d",
			ErrReplicaNotFound, s.id, replicaId)
	}

	s.unsafeRemoveReplica(container)
	return nil
}

// RemoveReplicaById removes the Container with the specified SMR node ID from the Session's replicas.
//
// RemoveReplica returns an error if there is no Container with the specified ID within the Session's replicas.
// On success, RemoveReplica returns nil.
//
// Note: this method is thread-safe.
func (s *Session) RemoveReplicaById(replicaId int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var targetContainer *Container
	for _, container := range s.containers {
		if container.ReplicaID() == replicaId {
			targetContainer = container
			break
		}
	}

	if targetContainer == nil {
		s.log.Error("Cannot remove replica %d from Session %s. No replica found with specified SMR node ID.",
			replicaId, s.id)
		return fmt.Errorf("%w: session %s does not have a replica with ID=%d",
			ErrReplicaNotFound, s.id, replicaId)
	}

	s.unsafeRemoveReplica(targetContainer)
	return nil
}

func (s *Session) ResourceSpec() types.CloneableSpec {
	return s.resourceSpec
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) Context() context.Context {
	return s.ctx
}

// SetContext sets the Session's context.Context.
//
// Note: this method is thread-safe.
func (s *Session) SetContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// SetResourceUtilization sets the value of the Session's resourceUtilization field to the given value.
//
// Note: this method is thread-safe.
func (s *Session) SetResourceUtilization(util *ResourceUtilization) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resourceUtilization = util
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
//
// Note: this method is thread-safe.
func (s *Session) SetExpectingTraining() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

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
//
// Note: this method is thread-safe.
func (s *Session) TrainingStarted(container *Container) promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(SessionStateTraining); err != nil {
		s.log.Warn("Failed to start training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if container == nil {
		s.log.Error("Specified container for training is nil.")
		return promise.Resolved(s.instance, fmt.Errorf("%w: container is nil", ErrInvalidContainer))
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
		return promise.Resolved(s.instance,
			fmt.Errorf("%w: container not in session's replicas (container=%v)", ErrInvalidContainer, container))
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
//
// Note: this method is thread-safe.
func (s *Session) TrainingStopped() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to stop training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if s.trainingContainer == nil {
		s.log.Error("Session is supposedly training, but its \"training container\" is nil...")
		return promise.Resolved(s.instance, ErrMissingTrainingContainer)
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

	s.log.Debug("%s has stopped training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().ID())
	return promise.Resolved(s.instance)
}

// MigrationStarted should be called when one of the replicas of the Session begins the
// process of migrating from its current Host to another Host.
//
// Note: this method is thread-safe.
func (s *Session) MigrationStarted() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(SessionStateMigrating); err != nil {
		s.log.Warn("Failed to initiate migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	s.migrationStart = time.Now()
	return promise.Resolved(s.instance)
}

// MigrationComplete should be called when the migrating replica of the Session has finished its migration
// to another host.
//
// Note: this method is thread-safe.
func (s *Session) MigrationComplete() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

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
//
// Note: this method is thread-safe.
func (s *Session) SessionStarted() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// SessionStopped should be called when the Session is terminated.
//
// Note: this method is thread-safe.
func (s *Session) SessionStopped() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log.Debug("Stopping scheduling.Session now.")

	// If the Session is training, then we need to first call TrainingStopped to ensure that our local view(s) of
	// resource usage on the Host on which the Session's active replica is training are consistent/correct.
	if s.IsTraining() {
		s.log.Debug("scheduling.Session %s is training. Stopping training before stopping scheduling.Session.")
		s.TrainingStopped()
	}

	// Transition to the 'SessionStateStopped' state.
	if err := s.transition(SessionStateStopped); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	// Stop each replica of the Session, collecting any errors that we encounter.
	errs := make([]error, 0)
	for i, container := range s.containers {
		s.log.Debug("Stopping replica scheduling.Container %d/%d of scheduling.Session.", i+1, len(s.containers))
		if err := container.ContainedStopped(); err != nil {
			s.log.Error("Failed to stop scheduling.Container %s (%d/%d) because: %v",
				container.ContainerID(), i+1, len(s.containers), err)
			errs = append(errs, err)
		}
	}

	// Return all the errors joined together via errors.Join if there were 1 or more errors.
	if len(errs) > 0 {
		s.log.Error("Encountered %d error(s) while stopping replica scheduling.Containers.", len(errs))
		return promise.Resolved(s.instance, errors.Join(errs...))
	} else {
		return promise.Resolved(s.instance)
	}
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

	originalState := s.sessionState
	//s.log.Debug("Attempting to transition from state \"%v\" to state \"%v\"", originalState, targetState)
	s.sessionState = targetState
	s.log.Debug("Successfully transitioned from state \"%v\" to state \"%v\"", originalState, targetState)
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
