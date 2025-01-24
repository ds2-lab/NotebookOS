package execution

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"sync"
	"time"
)

// validateRequest ensures that the given *messaging.JupyterMessage is either an "execute_request" message
// or a "yield_request" message.
func validateRequest(msg *messaging.JupyterMessage) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		return fmt.Errorf("%w: message provided is of type \"%s\"", ErrInvalidMessage, msg.JupyterMessageType())
	}

	return nil
}

// validateReply ensures that the given *messaging.JupyterMessage is an "execute_reply"
func validateReply(msg *messaging.JupyterMessage) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteReply {
		return fmt.Errorf("%w: message provided is of type \"%s\"", ErrInvalidMessage, msg.JupyterMessageType())
	}

	return nil
}

// Manager manages the Execution
type Manager struct {
	log logger.Logger
	mu  sync.Mutex

	// ActiveExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// ActiveExecutions contains only code submissions that have not yet completed.
	// There should typically just be one entry in the ActiveExecutions map at a time,
	// but because messages can be weirdly delayed and reordered, there may be multiple.
	ActiveExecutions map[string]*Execution

	// FinishedExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// FinishedExecutions contains only Execution structs that represent code submissions
	// that completed successfully, without error.
	FinishedExecutions map[string]*Execution

	// ErredExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// ErredExecutions contains only Execution structs that represent code submissions
	// that failed to complete successfully and were abandoned.
	ErredExecutions map[string]*Execution

	// AllExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// AllExecutions contains an entry for every single Execution, regardless of
	// the State of the Execution.
	AllExecutions map[string]*Execution

	// Kernel is the kernel associated with the Manager.
	Kernel Kernel

	// NumReplicas is how many replicas the Kernel has.
	NumReplicas int
}

// NewManager creates a new Manager struct to be associated with the given Kernel.
//
// NewManager returns a pointer to the new Manager struct.
func NewManager(kernel Kernel, numReplicas int) *Manager {
	manager := &Manager{
		ActiveExecutions:   make(map[string]*Execution),
		FinishedExecutions: make(map[string]*Execution),
		ErredExecutions:    make(map[string]*Execution),
		AllExecutions:      make(map[string]*Execution),
		NumReplicas:        numReplicas,
		Kernel:             kernel,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

// registerExecutionAttempt registers a new attempt for an existing execution.
func (m *Manager) registerExecutionAttempt(msg *messaging.JupyterMessage, existingExecution *Execution) *Execution {
	requestId := msg.JupyterMessageId()
	nextAttemptNumber := existingExecution.AttemptNumber + 1

	// Create the next execution attempt.
	nextExecutionAttempt := NewActiveExecution(m.Kernel.ID(), nextAttemptNumber, m.NumReplicas, msg)

	m.log.Debug("Registering new attempt (%d) for execution \"%s\"", nextAttemptNumber, requestId)

	// Link the previous active execution with the current one (in both directions).
	nextExecutionAttempt.LinkPreviousAttempt(existingExecution)
	existingExecution.LinkNextAttempt(nextExecutionAttempt)

	// Replace the entry in the mapping with the next attempt.
	// We can still access the previous attempt by following the "previous attempt" link.
	m.ActiveExecutions[requestId] = nextExecutionAttempt

	// Return the next execution attempt.
	return nextExecutionAttempt
}

// RegisterExecution registers a newly-submitted "execute_request" with the Manager.
func (m *Manager) RegisterExecution(msg *messaging.JupyterMessage) (*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := validateRequest(msg)
	if err != nil {
		return nil, err
	}

	requestId := msg.JupyterMessageId()
	if _, loaded := m.FinishedExecutions[requestId]; loaded {
		m.log.Error("Attempt made to re-register completed execution \"%s\"", requestId)
		return nil, fmt.Errorf("%w: execution ID=\"%s\"", ErrDuplicateExecution, requestId)
	}

	existingExecution, loaded := m.ActiveExecutions[requestId]
	if loaded {
		nextExecutionAttempt := m.registerExecutionAttempt(msg, existingExecution)
		return nextExecutionAttempt, nil
	}

	execution := NewActiveExecution(m.Kernel.ID(), 1, m.NumReplicas, msg)
	m.AllExecutions[requestId] = execution
	m.ActiveExecutions[requestId] = execution

	m.log.Debug("Registered new execution \"%s\" for kernel \"%s\"", requestId, m.Kernel.ID())

	return execution, nil
}

// YieldProposalReceived is to be called when a YieldProposal is issued by a replica of the associated Kernel.
func (m *Manager) YieldProposalReceived(msg *messaging.JupyterMessage) error {
	return nil
}

// LeadProposalReceived is to be called when a LeadProposal is issued by a replica of the associated Kernel.
func (m *Manager) LeadProposalReceived(msg *messaging.JupyterMessage) error {
	return nil
}

// ExecutionComplete should be called by the Kernel associated with the target Manager when an "execute_reply"
// message is received.
//
// ExecutionComplete returns nil on success.
func (m *Manager) ExecutionComplete(msg *messaging.JupyterMessage) (*Execution, error) {
	err := validateReply(msg)
	if err != nil {
		return nil, err
	}

	requestId := msg.JupyterParentMessageId()

	// Attempt to load the execution from the "active" map.
	execution, loaded := m.ActiveExecutions[requestId]
	if !loaded {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, requestId)
	}

	if execution.HasValidOriginalSentTimestamp() {
		latency := time.Since(execution.OriginallySentAt)
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d. Total time elapsed since submission: %v.",
			execution.ExecuteRequestMessageId, m.Kernel.ID(), msg.ReplicaId, latency)
	} else {
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d.",
			execution.ExecuteRequestMessageId, m.Kernel.ID(), msg.ReplicaId)
	}

	err = execution.ReceivedLeadNotification(msg.ReplicaId)
	if err != nil {
		return nil, err
	}

	execution.SetExecuted()

	// Update the execution's state.
	execution.State = Completed

	// Remove the execution from the "active" map.
	delete(m.ActiveExecutions, requestId)

	// Store the execution in the "finished" map.
	m.FinishedExecutions[requestId] = execution

	return execution, nil
}

// GetActiveExecution returns a pointer to the Execution struct identified by the given message ID,
// or nil if no such Execution exists.
func (m *Manager) GetActiveExecution(msgId string) *Execution {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.AllExecutions[msgId]
}

// NumActiveExecutionOperations returns the number of active Execution structs registered with the Manager.
//
// This method is thread safe.
func (m *Manager) NumActiveExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.ActiveExecutions)
}

// TotalNumExecutionOperations returns the total number of Execution structs registered with the Manager.
//
// This method is thread safe.
func (m *Manager) TotalNumExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.AllExecutions)
}
