package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/cache"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/Scusemua/go-utils/promise"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"math"
	"sync"
	"time"
)

var (
	ErrInvalidExplanationRequested = errors.New("invalid explanation requested")
	ErrInvalidContainer            = errors.New("the container is invalid")
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

	ctx                    context.Context                      // The Session's context.
	id                     string                               // Session/kernel ID.
	sessionState           scheduling.SessionState              // The current state of the Session.
	trainingStart          time.Time                            // Time at which the current training began.
	idleStartTime          time.Time                            // idleStartTime is the time at which the Distributed Kernel Client last began idling.
	migrationStart         time.Time                            // Time at which the migration began.
	containers             map[int32]scheduling.KernelContainer // The kernel replicas belonging to this Session.
	trainingContainer      scheduling.KernelContainer           // The Container that is actively training.
	resourceSpec           types.CloneableSpec                  // The (current) resource requirements of the Session.
	stateTransitions       []*sessionStateTransition            // History of state transitions performed by the Session.
	cumulativeTrainingTime time.Duration                        // cumulativeTrainingTime is the sum of time that this Session has spent training, excluding any associated overheads.

	////////////////////////
	// Session Statistics //
	////////////////////////

	kernelSpec                     *proto.KernelSpec           // The kernel resourceSpec of the associated kernel.
	startedAt                      time.Time                   // Time at which the session began running.
	trainingTimeWithOverheads      scheduling.SessionStatistic // Moving average of training times.
	migrationTime                  scheduling.SessionStatistic // Moving average of migration times.
	interactivePriority            float64                     // Interactivity Priority
	interactivePriorityExplanation string                      // Explanation of current Interactivity Priority value.
	preemptionPriority             cache.InlineCache           // Preemption Priority
	preemptionPriorityExplanation  string                      // Explanation of current  Preemption Priority value.
	numTrainingEventsProcessed     int                         // numTrainingEventsProcessed is the number of training events processed by this Session.

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

// Build constructs a Session with the specified options
func (b *SessionBuilder) Build() *Session {
	session := &Session{
		ctx:          b.ctx,
		id:           b.id,
		kernelSpec:   b.kernelSpec,
		resourceSpec: b.kernelSpec.DecimalSpecFromKernelSpec(),

		log:                        config.GetLogger(fmt.Sprintf("Session %s ", b.id)),
		sessionState:               scheduling.SessionStateInit,
		startedAt:                  time.Now(),
		trainingTimeWithOverheads:  NewMovingStatistic(b.trainingTimeSampleWindowSize),
		migrationTime:              NewMovingStatistic(b.migrationTimeSampleWindowSize),
		stateTransitions:           make([]*sessionStateTransition, 0),
		interactivePriorityHistory: NewValueHistory[float64]("Interactive Priority", "float64"),
		preemptionPriorityHistory:  NewValueHistory[float64]("Preemption Priority", "float64"),
		trainingTimeHistory:        NewValueHistory[time.Duration]("Training Time", "time.Duration"),
		migrationTimeHistory:       NewValueHistory[time.Duration]("Migration Time", "time.Duration"),
		containers:                 make(map[int32]scheduling.KernelContainer),
		numTrainingEventsProcessed: 0,
		idleStartTime:              time.Now(),
	}

	initialInteractivePriority := session.updateInteractivePriority("session started")
	session.interactivePriorityHistory.AddValue(initialInteractivePriority)

	session.preemptionPriority.Producer = cache.FormalizeICProducer(session.calculatePreemptionPriority)
	session.preemptionPriority.Validator = GetClockTimeCacheValidator()
	session.instance = session

	return session
}

// CumulativeTrainingTime returns the sum of time that this Session has spent training, excluding any associated overheads.
func (s *Session) CumulativeTrainingTime() time.Duration {
	return s.cumulativeTrainingTime
}

// Lock locks the Session.
func (s *Session) Lock() {
	s.mu.Lock()
}

// Unlock unlocks the Session.
func (s *Session) Unlock() {
	s.mu.Unlock()
}

// NumTrainingEventsProcessed returns the number of training events processed by this Session.
func (s *Session) NumTrainingEventsProcessed() int {
	return s.numTrainingEventsProcessed
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
func (s *Session) AddReplica(container scheduling.KernelContainer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if container == nil {
		return fmt.Errorf("%w: container is nil", ErrInvalidContainer)
	}

	if container.KernelID() != s.id {
		return fmt.Errorf("%w: ID mismatch (session ID=\"%s\", container ID=\"%s\")", ErrInvalidContainer, s.id, container.KernelID())
	}

	// Ensure we don't already have a replica with this SMR Node ID.
	if existingContainer, loaded := s.containers[container.ReplicaId()]; loaded {
		s.log.Error("Cannot add/register scheduling.Container for replica %d of kernel \"%s\" with associated scheduling.Session; "+
			"session already has container for replica %d registered...", container.ReplicaId(), s.id, container.ReplicaId())
		s.log.Error("Existing scheduling.Container for replica %d of kernel \"%s\": %s", container.ReplicaId(), s.id, existingContainer.String())
		return fmt.Errorf("%w: session already has container for replica %d registered", ErrInvalidContainer, container.ReplicaId())
	}

	s.containers[container.ReplicaId()] = container

	s.log.Debug("Successfully added/registered scheduling.Container for replica %d of kernel \"%s\" with associated scheduling.Session",
		container.ReplicaId(), s.id)

	return nil
}

// RemoveReplica removes the specified Container from the Session's replicas.
//
// RemoveReplica returns an error if the specified Container is not one of the Session's replicas.
// On success, RemoveReplica returns nil.
//
// Note: this method is thread-safe.
func (s *Session) RemoveReplica(container scheduling.KernelContainer) error {
	return s.RemoveReplicaById(container.ReplicaId())
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

func (s *Session) UpdateResourceSpec(newSpec types.CloneableSpec) {
	s.log.Debug("Updating ResourceSpec of Session \"%s\" from %v to %v.",
		s.id, s.resourceSpec.String(), newSpec.String())

	s.resourceSpec = newSpec

	s.kernelSpec.ResourceSpec.Gpu = int32(newSpec.GPU())
	s.kernelSpec.ResourceSpec.Cpu = int32(newSpec.CPU())
	s.kernelSpec.ResourceSpec.Vram = float32(newSpec.VRAM())
	s.kernelSpec.ResourceSpec.Memory = float32(newSpec.MemoryMB())
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
func (s *Session) GetReplicaContainer(replicaId int32) (scheduling.KernelContainer, bool) {
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

func (s *Session) KernelSpec() *proto.KernelSpec {
	return s.kernelSpec
}

func (s *Session) String() string {
	return fmt.Sprintf("Session[ID=%s,ResourceRequest=%v,NumReplicas=%d]", s.id, s.resourceSpec, len(s.containers))
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
		s.log.Error("Session cannot transition to state \"%s\" -- Session is already training!", scheduling.SessionStateExpectingTraining)
		err := fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", scheduling.ErrInvalidStateTransition, s.sessionState, scheduling.SessionStateExpectingTraining)
		return promise.Resolved(s.instance, err)
	}

	if err := s.transition(scheduling.SessionStateExpectingTraining); err != nil {
		s.log.Error("Could not transition to state \"%s\" because: %v", scheduling.SessionStateExpectingTraining, err)
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
func (s *Session) SessionStartedTraining(container scheduling.KernelContainer) promise.Promise {
	s.log.Debug("Training starting. Current state: %s.", s.GetState().String())

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(scheduling.SessionStateTraining); err != nil {
		s.log.Warn("Failed to start training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if container == nil {
		s.log.Error("Specified container for training is nil.")

		// Try to go back to idle...
		err := s.transition(scheduling.SessionStateIdle)
		if err != nil {
			s.log.Error("Failed to revert back to idle state after failing to start training: %v", err)
		}

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

		// Try to go back to idle...
		err := s.transition(scheduling.SessionStateIdle)
		if err != nil {
			s.log.Error("Failed to revert back to idle state after failing to start training: %v", err)
		}

		return promise.Resolved(s.instance,
			fmt.Errorf("%w: container not in session's replicas (container=%v)", ErrInvalidContainer, container))
	}

	s.trainingContainer = container
	if err := s.trainingContainer.TrainingStartedInContainer(); err != nil {
		s.log.Error("Failed to start training in container %s: %v", container.String(), err)

		// Try to go back to idle...
		err := s.transition(scheduling.SessionStateIdle)
		if err != nil {
			s.log.Error("Failed to revert back to idle state after failing to start training: %v", err)
		}

		return promise.Resolved(s.instance, err)
	}

	s.trainingStart = time.Now()

	s.log.Debug("Container %s began training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().GetID())

	return promise.Resolved(s.instance)
}

// SessionStoppedTraining should be called when the actively-training Kernel replica of the Session stops training.
//
// This should be called by the Kernel's KernelStoppedTraining method.
//
// Note: this method is thread-safe.
func (s *Session) SessionStoppedTraining(reason string) promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.unsafeTrainingStopped(reason)
}

// UnsafeTrainingStopped performs the work of SessionStoppedTraining. It is to be called by SessionStoppedTraining, and sometimes
// SessionStopped, after the Session's mutex has already been acquired.
//
// Note: this method is NOT thread-safe.
func (s *Session) unsafeTrainingStopped(reason string) promise.Promise {
	s.log.Debug("Training stopping. Current state: %s. Reason: %s.", s.sessionState.String(), reason)

	if err := s.transition(scheduling.SessionStateIdle); err != nil {
		s.log.Warn("Failed to stop training because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	if s.trainingContainer == nil {
		s.log.Error("Session is supposedly training, but its \"training container\" is nil...")
		return promise.Resolved(s.instance, ErrMissingTrainingContainer)
	}

	if err := s.trainingContainer.ContainerStoppedTraining(); err != nil {
		s.log.Error("Failed to stop training in active container: %v", err)
		return promise.Resolved(s.instance, err)
	}

	trainingDuration := time.Since(s.trainingStart)
	s.trainingTimeHistory.AddValue(trainingDuration)
	s.trainingTimeWithOverheads.Add(float64(trainingDuration) / float64(time.Second))
	s.idleStartTime = time.Now()
	s.numTrainingEventsProcessed += 1

	latestInteractivePriority := s.updateInteractivePriority("training stopped")
	s.interactivePriorityHistory.AddValue(latestInteractivePriority)

	s.log.Debug("%s has stopped training on Host %s.", s.trainingContainer.String(), s.trainingContainer.Host().GetID())
	return promise.Resolved(s.instance)
}

// IdleTime returns the time that the Session has been idle (i.e., not training), as well as a flag indicating
// whether the Session is currently idle.
func (s *Session) IdleTime() (time.Duration, bool) {
	if s.IsIdle() {
		return time.Since(s.idleStartTime), true
	}

	return time.Duration(-1), false
}

// MigrationStarted should be called when one of the replicas of the Session begins the
// process of migrating from its current Host to another Host.
//
// Note: this method is thread-safe.
func (s *Session) MigrationStarted() promise.Promise {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transition(scheduling.SessionStateMigrating); err != nil {
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

	if err := s.transition(scheduling.SessionStateIdle); err != nil {
		s.log.Warn("Failed to conclude migration because: %v", err)
		return promise.Resolved(s.instance, err)
	}

	migrationDuration := time.Since(s.migrationStart)
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

	if err := s.transition(scheduling.SessionStateIdle); err != nil {
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
		p := s.unsafeTrainingStopped("Session is stopping.") // Call UnsafeTrainingStopped directly, as we already have the mutex.
		if err := p.Error(); err != nil {
			return promise.Resolved(nil, err)
		}
	}

	// Transition to the 'SessionStateStopped' state.
	if err := s.transition(scheduling.SessionStateStopped); err != nil {
		s.log.Warn("Failed to terminate session because: %v", err)
		return promise.Resolved(nil, err)
	}

	// Stop each replica of the Session, collecting any errors that we encounter.
	errs := make([]error, 0)
	for i, container := range s.containers {
		s.log.Debug("Stopping replica %d/%d of scheduling.Session.", i, len(s.containers))
		if err := container.ContainerStopped(); err != nil {
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
	return s.sessionState == scheduling.SessionStateStopped
}

// IsIdle returns true if the Session is currently idle, meaning that none of its replicas are currently training.
func (s *Session) IsIdle() bool {
	return s.sessionState == scheduling.SessionStateIdle
}

// IsMigrating returns true if one or more replicas are currently migrating from one Host to another.
func (s *Session) IsMigrating() bool {
	return s.sessionState == scheduling.SessionStateMigrating
}

// IsTraining returns true if the Session is actively training.
// Otherwise, IsTraining returns false.
func (s *Session) IsTraining() bool {
	return s.sessionState == scheduling.SessionStateTraining
}

func (s *Session) transition(targetState scheduling.SessionState) error {
	if s.IsStopped() {
		return fmt.Errorf("%w: cannot transition from state '%s' to state '%s'", scheduling.ErrInvalidStateTransition, s.sessionState, targetState)
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
	return s.trainingTimeWithOverheads
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
		s.interactivePriority = s.resourceSpec.GPU() * math.Pow(s.MigrationTime(), 2.0) / s.TrainingTime().Avg()
		s.interactivePriorityExplanation = fmt.Sprintf("update after %s(%.0f * %.2f^2 / %.2f)", reason, s.resourceSpec.GPU(), s.MigrationTime(), s.TrainingTime().Avg())
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

		preemptionPriority = s.resourceSpec.GPU() * s.MigrationTime()
	}

	s.preemptionPriorityHistory.AddValue(preemptionPriority)
	return
}

func (s *Session) StartedAt() time.Time {
	return s.startedAt
}

func (s *Session) Duration() time.Duration {
	return time.Since(s.startedAt)
}
