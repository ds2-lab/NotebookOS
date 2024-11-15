package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/cache"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	"github.com/zhangjyr/distributed-notebook/common/scheduling/resource"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"math"
	"sync"
	"time"
)

var (
	ErrInvalidStateTransition      = errors.New("invalid session state transition requested")
	ErrInvalidExplanationRequested = errors.New("invalid explanation requested")
	ErrInvalidContainer            = errors.New("the specified or provided container is invalid")
	ErrMissingTrainingContainer    = errors.New("session is training, but its \"training container\" is nil")
)

func NewMovingStatistic(window int64) *types.MovingStat {
	if window > 0 {
		return types.NewMovingStat(window, 0, make([]float64, window), 0, [2]float64{0, 0}, 0, 1)
	} else if window < 0 {
		panic("Negative window size (meaning no window) is not yet supported.")
	} else {
		panic("Window size cannot be 0.")
	}
}

// sessionStateTransition encapsulates some data regarding state transitions.
type sessionStateTransition struct {
	PrevState       scheduling.SessionState `json:"prev_state"`
	NewState        scheduling.SessionState `json:"new_state"`
	Timestamp       time.Time               `json:"timestamp"`
	TimeInPrevState time.Duration           `json:"time_in_prev_state"`
}

type Session struct {
	instance *Session

	ctx               context.Context           // The Session's context.
	id                string                    // Session/kernel ID.
	sessionState      scheduling.SessionState   // The current state of the Session.
	trainingStart     time.Time                 // Time at which the current training began.
	migrationStart    time.Time                 // Time at which the migration began.
	containers        map[int32]*Container      // The kernel replicas belonging to this Session.
	trainingContainer *Container                // The Container that is actively training.
	resourceSpec      types.CloneableSpec       // The (current) resource requirements of the Session.
	stateTransitions  []*sessionStateTransition // History of state transitions performed by the Session.

	////////////////////////
	// Session Statistics //
	////////////////////////

	kernelSpec                     *proto.KernelSpec           // The kernel resourceSpec of the associated kernel.
	resourceUtilization            *resource.Utilization       // Current/latest resource usage statistics.
	startedAt                      time.Time                   // Time at which the session began running.
	trainingTime                   scheduling.SessionStatistic // Moving average of training times.
	migrationTime                  scheduling.SessionStatistic // Moving average of migration times.
	interactivePriority            float64                     // Interactivity Priority
	interactivePriorityExplanation string                      // Explanation of current Interactivity Priority value.
	preemptionPriority             cache.InlineCache           // Preemption Priority
	preemptionPriorityExplanation  string                      // Explanation of current  Preemption Priority value.

	interactivePriorityHistory *ValueHistory[float64]
	preemptionPriorityHistory  *ValueHistory[float64]
	trainingTimeHistory        *ValueHistory[time.Duration]
	migrationTimeHistory       *ValueHistory[time.Duration]

	mu sync.Mutex

	log logger.Logger
}

type SessionBuilder struct {
	ctx                           context.Context
	id                            string
	kernelSpec                    *proto.KernelSpec
	resourceUtilization           *resource.Utilization
	trainingTimeSampleWindowSize  int64
	migrationTimeSampleWindowSize int64
}

// NewSessionBuilder initializes a new SessionBuilder
func NewSessionBuilder() *SessionBuilder {
	return &SessionBuilder{}
}

// WithContext sets the context for the user session
func (b *SessionBuilder) WithContext(ctx context.Context) *SessionBuilder {
	b.ctx = ctx
	return b
}

// WithTrainingTimeSampleWindowSize sets the window size for sampling the training time (as a moving average).
func (b *SessionBuilder) WithTrainingTimeSampleWindowSize(windowSize int64) *SessionBuilder {
	b.trainingTimeSampleWindowSize = windowSize
	return b
}

// WithMigrationTimeSampleWindowSize sets the window size for sampling the migration time (as a moving average).
func (b *SessionBuilder) WithMigrationTimeSampleWindowSize(windowSize int64) *SessionBuilder {
	b.migrationTimeSampleWindowSize = windowSize
	return b
}

// WithID sets the ID for the user session
func (b *SessionBuilder) WithID(id string) *SessionBuilder {
	b.id = id
	return b
}

// WithKernelSpec sets the kernel specification for the user session
func (b *SessionBuilder) WithKernelSpec(kernelSpec *proto.KernelSpec) *SessionBuilder {
	b.kernelSpec = kernelSpec
	return b
}

// WithResourceUtilization sets the resource utilization for the user session
func (b *SessionBuilder) WithResourceUtilization(resourceUtilization *resource.Utilization) *SessionBuilder {
	b.resourceUtilization = resourceUtilization
	return b
}

// Build constructs a Session with the specified options
func (b *SessionBuilder) Build() *Session {
	session := &Session{
		ctx:                        b.ctx,
		id:                         b.id,
		kernelSpec:                 b.kernelSpec,
		resourceSpec:               b.kernelSpec.DecimalSpecFromKernelSpec(),
		resourceUtilization:        b.resourceUtilization,
		log:                        config.GetLogger(fmt.Sprintf("Session %s ", b.id)),
		sessionState:               SessionStateInit,
		startedAt:                  time.Now(),
		trainingTime:               NewMovingStatistic(b.trainingTimeSampleWindowSize),
		migrationTime:              NewMovingStatistic(b.migrationTimeSampleWindowSize),
		stateTransitions:           make([]*sessionStateTransition, 0),
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
	if existingContainer, loaded := s.containers[container.ReplicaID()]; loaded {
		s.log.Error("Cannot add/register scheduling.Container for replica %d of kernel \"%s\" with associated scheduling.Session; "+
			"session already has container for replica %d registered...", container.ReplicaID(), s.id, container.ReplicaID())
		s.log.Error("Existing scheduling.Container for replica %d of kernel \"%s\": %s", container.ReplicaID(), s.id, existingContainer.String())
		return fmt.Errorf("%w: session already has container for replica %d registered", ErrInvalidContainer, container.ReplicaID())
	}

	s.containers[container.ReplicaID()] = container

	s.log.Debug("Successfully added/registered scheduling.Container for replica %d of kernel \"%s\" with associated scheduling.Session",
		container.ReplicaID(), s.id)

	return nil
}

// RemoveReplica removes the specified Container from the Session's replicas.
//
// RemoveReplica returns an error if the specified Container is not one of the Session's replicas.
// On success, RemoveReplica returns nil.
//
// Note: this method is thread-safe.
func (s *Session) RemoveReplica(container *Container) error {
	return s.RemoveReplicaById(container.ReplicaID())
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

	// Check first to see if we have a replica with that ID registered already.
	// If not, we'll return an error.
	var (
		loaded bool
	)
	if _, loaded = s.containers[replicaId]; !loaded {
		s.log.Error("Cannot remove replica %d from Session %s. No replica found with specified SMR node ID.",
			replicaId, s.id)
		return fmt.Errorf("%w: session %s does not have a replica with ID=%d",
			scheduling.ErrReplicaNotFound, s.id, replicaId)
	}

	delete(s.containers, replicaId)

	s.log.Debug("Removed/unregistered scheduling.Container for replica %d with scheduling.Session \"%s\"",
		replicaId, s.id)

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

// GetReplicaContainer returns the Container with the given replica ID (i.e., SMR node ID).
//
// If the Session does not presently have a Container with the specified ID, then nil is returned along with false.
func (s *Session) GetReplicaContainer(replicaId int32) (*Container, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	container, loaded := s.containers[replicaId]
	return container, loaded
}

// SetContext sets the Session's context.Context.
//
// Note: this method is thread-safe.
func (s *Session) SetContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ctx = ctx
}

// ResourceUtilization returns the current ResourceUtilization of the Session.
func (s *Session) ResourceUtilization() *resource.Utilization {
	return s.resourceUtilization
}

// SetResourceUtilization sets the value of the Session's resourceUtilization field to the given value.
//
// Note: this method is thread-safe.
func (s *Session) SetResourceUtilization(util *resource.Utilization) {
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
		err := fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidStateTransition, s.sessionState, SessionStateExpectingTraining)
		return promise.Resolved(s.instance, err)
	}

	if err := s.transition(SessionStateExpectingTraining); err != nil {
		s.log.Error("Could not transition to state \"%s\" because: %v", SessionStateExpectingTraining, err)
		return promise.Resolved(s.instance, err)
	}

	return promise.Resolved(s.instance)
}

// SessionStartedTraining should be called when one of the Session's Kernel replicas begins training.
//
// Note: this method is thread-safe.
//
// In the Local Daemon, this won't be called, as the Local Daemon does not track ComputeResource in this way.
//
// In the Cluster Gateway, this is called in the SessionStartedTraining method of the Kernel.
// The Kernel's SessionStartedTraining method is called in the handleSmrLeadTaskMessage method
// of DistributedKernelClient.
//
// DistributedKernelClient::handleSmrLeadTaskMessage --> Kernel::TrainingStartedInContainer --> Session::TrainingStartedInContainer.
func (s *Session) SessionStartedTraining(container *Container, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise {
	s.log.Debug("Training starting. Current state: %s.", s.GetState().String())

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
	if err := TrainingStartedInContainer(s.trainingContainer, snapshot); err != nil {
		s.log.Error("Failed to start training in container %s: %v", container.String(), err)
		return promise.Resolved(s.instance, err)
	}

	s.trainingStart = time.Now()

	s.log.Debug("Container %s began training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().ID)

	return promise.Resolved(s.instance)
}

// SessionStoppedTraining should be called when the actively-training Kernel replica of the Session stops training.
//
// This should be called by the Kernel's KernelStoppedTraining method.
//
// Note: this method is thread-safe.
func (s *Session) SessionStoppedTraining(snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	return UnsafeTrainingStopped(s, snapshot)
}

// UnsafeTrainingStopped performs the work of SessionStoppedTraining. It is to be called by SessionStoppedTraining, and sometimes
// SessionStopped, after the Session's mutex has already been acquired.
//
// Note: this method is NOT thread-safe.
func UnsafeTrainingStopped(s *Session, snapshot types.HostResourceSnapshot[types.ArbitraryResourceSnapshot]) promise.Promise {
	s.log.Debug("Training stopping. Current state: %s.", s.sessionState.String())

	if err := s.transition(SessionStateIdle); err != nil {
		s.log.Warn("Failed to stop training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if s.trainingContainer == nil {
		s.log.Error("Session is supposedly training, but its \"training container\" is nil...")
		return promise.Resolved(s.instance, ErrMissingTrainingContainer)
	}

	if err := ContainerStoppedTraining(s.trainingContainer, snapshot); err != nil {
		s.log.Error("Failed to stop training in active container: %v", err)
		return promise.Resolved(s.instance, err)
	}

	trainingDuration := time.Now().Sub(s.trainingStart)
	s.trainingTimeHistory.AddValue(trainingDuration)
	s.trainingTime.Add(float64(trainingDuration) / float64(time.Second))

	latestInteractivePriority := s.updateInteractivePriority("training stopped")
	s.interactivePriorityHistory.AddValue(latestInteractivePriority)

	s.log.Debug("%s has stopped training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().ID)
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
func (s *Session) SessionStatistics() scheduling.SessionStatistics {
	return s
}

// GetState returns the current state of the Session in the form of a scheduling.SessionState.
func (s *Session) GetState() scheduling.SessionState {
	return s.sessionState
}

// SessionStarted should be called when the Session is scheduled successfully, meaning that all the Session's
// replicas (Containers) have successfully been scheduled and started running.
//
// Note: this method is thread-safe.
func (s *Session) SessionStarted() promise.Promise {
	s.log.Debug("Session starting. Current state: %s.", s.sessionState.String())

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
	s.log.Debug("Session stopping. Current state: %s.", s.sessionState.String())

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.IsTraining() {
		s.log.Debug("Currently training. Stopping training before stopping scheduling.Session.")
		p := UnsafeTrainingStopped(s, nil) // Call UnsafeTrainingStopped directly, as we already have the mutex.
		if err := p.Error(); err != nil {
			return promise.Resolved(nil, err)
		}
	}

	// Transition to the 'SessionStateStopped' state.
	if err := s.transition(SessionStateStopped); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(nil, err)
	}

	// Stop each replica of the Session, collecting any errors that we encounter.
	errs := make([]error, 0)
	for i, container := range s.containers {
		s.log.Debug("Stopping replica %d/%d of scheduling.Session.", i, len(s.containers))
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

func (s *Session) transition(targetState scheduling.SessionState) error {
	if s.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", ErrInvalidStateTransition, s.sessionState, targetState)
	}

	// Some bookkeeping about state transitions, like how long we were in the previous state,
	// and when we last performed a state transition.
	timeInPrevState := time.Duration(0)
	lastStateTransitionAt := "N/A"
	if len(s.stateTransitions) > 0 {
		lastStateTransition := s.stateTransitions[len(s.stateTransitions)-1]
		lastStateTransitionAt = lastStateTransition.Timestamp.String()
		timeInPrevState = time.Since(lastStateTransition.Timestamp)
	} else {
		timeInPrevState = time.Since(s.startedAt)
	}

	stateTransition := &sessionStateTransition{
		Timestamp:       time.Now(),
		PrevState:       s.sessionState,
		NewState:        targetState,
		TimeInPrevState: timeInPrevState,
	}

	s.stateTransitions = append(s.stateTransitions, stateTransition)
	originalState := s.sessionState
	s.sessionState = targetState
	s.log.Debug("Successfully transitioned from state \"%v\" to state \"%v\". Last transition was at %v. Time spent in previous (\"%v\") state: %v.",
		originalState, targetState, originalState, lastStateTransitionAt, stateTransition.TimeInPrevState)
	return nil
}

////////////////////////
// Session Statistics //
////////////////////////

func (s *Session) Explain(key scheduling.ExplainerEntry) string {
	switch key {
	case scheduling.ExplainInteractivePriority:
		return s.interactivePriorityExplanation
	case scheduling.ExplainPreemptionPriority:
		return s.preemptionPriorityExplanation
	default:
		return fmt.Errorf("%w: \"%s\"", ErrInvalidExplanationRequested, key).Error()
	}
}

func (s *Session) TrainingTime() scheduling.SessionStatistic {
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
