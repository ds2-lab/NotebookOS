package client

import (
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/execution"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"sync"
	"time"
)

// validateRequest ensures that the given *messaging.JupyterMessage is either an "execute_request" message
// or a "yield_request" message.
func validateRequest(msg *messaging.JupyterMessage) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		return fmt.Errorf("%w: message provided is of type \"%s\"", execution.ErrInvalidMessage, msg.JupyterMessageType())
	}

	return nil
}

// validateReply ensures that the given *messaging.JupyterMessage is an "execute_reply"
func validateReply(msg *messaging.JupyterMessage) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteReply {
		return fmt.Errorf("%w: message provided is of type \"%s\"", execution.ErrInvalidMessage, msg.JupyterMessageType())
	}

	return nil
}

// ExecutionManager manages the Execution instances associated with a DistributedKernelClient / scheduling.Kernel.
type ExecutionManager struct {
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

	// Kernel is the kernel associated with the ExecutionManager.
	Kernel scheduling.Kernel

	// NumReplicas is how many replicas the Kernel has.
	NumReplicas int

	// LastPrimaryReplica is the KernelReplica that served as the primary replica for the previous
	// code execution. It will be nil if no code executions have occurred.
	LastPrimaryReplica scheduling.KernelReplica

	// notificationCallback is used to send notifications to the frontend dashboard from this kernel/client.
	notificationCallback scheduling.NotificationCallback

	// executionFailedCallback is a callback for when execution fails (such as all replicas proposing 'YIELD').
	executionFailedCallback scheduling.ExecutionFailedCallback

	// ExecutionLatencyCallback is provided by the internalCluster Gateway to each scheduling.Kernel and
	// subsequently the scheduling.Kernel's ExecutionManager.
	//
	// When a scheduling.Kernel receives a notification that a kernel has started execution user-submitted code,
	// the scheduling.Kernel will check if its ActiveExecution struct has the original "sent-at" timestamp
	// of the original "execute_request". If it does, then it can calculate the latency between submission and when
	// the code began executing on the kernel. This interval is computed and passed to the ExecutionLatencyCallback,
	// so that a relevant Prometheus metric can be updated.
	executionLatencyCallback scheduling.ExecutionLatencyCallback
}

// NewExecutionManager creates a new ExecutionManager struct to be associated with the given Kernel.
//
// NewExecutionManager returns a pointer to the new ExecutionManager struct.
func NewExecutionManager(kernel scheduling.Kernel, numReplicas int, execFailCallback scheduling.ExecutionFailedCallback,
	notifyCallback scheduling.NotificationCallback, latencyCallback scheduling.ExecutionLatencyCallback) *ExecutionManager {

	manager := &ExecutionManager{
		ActiveExecutions:         make(map[string]*Execution),
		FinishedExecutions:       make(map[string]*Execution),
		ErredExecutions:          make(map[string]*Execution),
		AllExecutions:            make(map[string]*Execution),
		NumReplicas:              numReplicas,
		Kernel:                   kernel,
		executionFailedCallback:  execFailCallback,
		notificationCallback:     notifyCallback,
		executionLatencyCallback: latencyCallback,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

// RegisterExecution registers a newly-submitted "execute_request" with the ExecutionManager.
func (m *ExecutionManager) RegisterExecution(msg *messaging.JupyterMessage) (*Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := validateRequest(msg)
	if err != nil {
		return nil, err
	}

	requestId := msg.JupyterMessageId()
	if _, loaded := m.FinishedExecutions[requestId]; loaded {
		m.log.Error("Attempt made to re-register completed execution \"%s\"", requestId)
		return nil, fmt.Errorf("%w: execution ID=\"%s\"", execution.ErrDuplicateExecution, requestId)
	}

	existingExecution, loaded := m.ActiveExecutions[requestId]
	if loaded {
		nextExecutionAttempt := m.registerExecutionAttempt(msg, existingExecution)
		return nextExecutionAttempt, nil
	}

	newExecution := NewActiveExecution(m.Kernel.ID(), 1, m.NumReplicas, msg)
	m.AllExecutions[requestId] = newExecution
	m.ActiveExecutions[requestId] = newExecution

	m.log.Debug("Registered new execution \"%s\" for kernel \"%s\"", requestId, m.Kernel.ID())

	return newExecution, nil
}

// registerExecutionAttempt registers a new attempt for an existing execution.
func (m *ExecutionManager) registerExecutionAttempt(msg *messaging.JupyterMessage, existingExecution *Execution) *Execution {
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

// YieldProposalReceived is called when we receive a YieldProposal from a replica of a kernel.
//
// YieldProposalReceived registers the YieldProposal with the kernel's associated Execution struct.
//
// If we find that we've received all three proposals, and they were ALL YieldProposal, then we'll invoke the
// "failure handler", which will  handle the situation according to the cluster's configured scheduling policy.
func (m *ExecutionManager) YieldProposalReceived(replica scheduling.KernelReplica, msg *messaging.JupyterMessage,
	msgErr *messaging.MessageErrorWithYieldReason) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteReply {
		return fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			execution.ErrInvalidMessage, messaging.ShellExecuteReply, msg.JupyterMessageType())
	}

	m.log.Debug("Received 'YIELD' proposal from replica %d for execution \"%s\"",
		replica.ReplicaID(), msg.JupyterParentMessageId())

	// targetExecuteRequestId is the Jupyter message ID of the "execute_request" message associated
	// with the 'YIELD' proposal that we just received.
	targetExecuteRequestId := msg.JupyterParentMessageId()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the associated Execution struct.
	activeExecution, loaded := m.ActiveExecutions[targetExecuteRequestId]
	if !loaded {
		m.log.Warn("Could not find active Execution with ID \"%s\"...", targetExecuteRequestId)

		// Check the other executions in case we received the 'YIELD' message after the response from the leader.
		activeExecution, loaded = m.AllExecutions[targetExecuteRequestId]
		if loaded {
			m.log.Warn("Instead, found %s Execution with ID \"%s\"...",
				activeExecution.State.String(), targetExecuteRequestId)
		}
	}

	if activeExecution == nil {
		m.log.Error("Could not find any Execution with ID \"%s\"...", targetExecuteRequestId)
		return fmt.Errorf("%w: \"%s\"", execution.ErrUnknownActiveExecution, targetExecuteRequestId)
	}

	// This will return an error if the replica did not have any pre-committed resources.
	// So, we can just ignore the error.
	_ = m.Kernel.ReleasePreCommitedResourcesFromReplica(replica, msg)

	replica.ReceivedExecuteReply(msg)

	// It's possible we received a 'YIELD' proposal for an Execution different from the current one.
	// So, retrieve the Execution associated with the 'YIELD' proposal (using the "execute_request" message IDs).
	associatedActiveExecution := m.GetActiveExecution(targetExecuteRequestId)

	// If we couldn't find the associated active execution at all, then we should return an error, as that is bad.
	if associatedActiveExecution == nil {
		m.log.Error(utils.RedStyle.Render("Received 'YIELD' proposal from replica %d of kernel %s targeting unknown "+
			"Execution associated with an \"execute_request\" message with msg_id=\"%s\"..."),
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId)

		return fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, targetExecuteRequestId)
	}

	// Mark that we received the 'YIELD' proposal for the associated Execution.
	err := associatedActiveExecution.ReceivedYieldNotification(replica.ReplicaID(), msgErr.YieldReason)

	m.log.Debug("Received 'YIELD' proposal from replica %d of kernel %s for Execution associated with "+
		"\"execute_request\" \"%s\". Received %d/%d proposals from replicas of kernel %s.",
		replica.ReplicaID(), replica.ID(), targetExecuteRequestId, associatedActiveExecution.NumRolesReceived(),
		associatedActiveExecution.NumReplicas, replica.ID())

	// If we have a non-nil error, and it isn't just that all the replicas proposed YIELD, then return it directly.
	if err != nil && !errors.Is(err, execution.ErrExecutionFailedAllYielded) {
		m.log.Error("Encountered error while processing 'YIELD' proposal from replica %d of kernel %s for Execution associated with \"execute_request\" \"%s\": %v",
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId, err)

		return err
	}

	// If we have a non-nil error, and it's just that all replicas proposed YIELD, then we'll call the handler.
	if errors.Is(err, execution.ErrExecutionFailedAllYielded) {
		// Concatenate all the yield reasons. We'll return them along with the error returned by
		// handleFailedExecutionAllYielded if handleFailedExecutionAllYielded returns a non-nil error.
		yieldErrors := make([]error, 0, 4)
		associatedActiveExecution.RangeRoles(func(i int32, proposal *Proposal) bool {
			yieldError := fmt.Errorf("replica %d proposed \"YIELD\" because: %s", i, proposal.Reason)
			yieldErrors = append(yieldErrors, yieldError)
			return true
		})

		m.log.Debug("All %d replicas of kernel \"%s\" proposed 'YIELD' for execution \"%s\".",
			m.Kernel.Size(), m.Kernel.ID(), targetExecuteRequestId)

		// Call the handler. If it returns an error, then we'll join that error with the YIELD errors, and return
		// them all together.
		handlerError := m.executionFailedCallback(m.Kernel, msg)
		if handlerError != nil {
			allErrors := append([]error{handlerError}, yieldErrors...)
			return errors.Join(allErrors...)
		}
	}

	return nil
}

// HandleSmrLeadTaskMessage is to be called when a LeadProposal is issued by a replica of the associated Kernel.
func (m *ExecutionManager) HandleSmrLeadTaskMessage(msg *messaging.JupyterMessage, kernelReplica scheduling.KernelReplica) error {
	if msg.JupyterMessageType() != messaging.MessageTypeSMRLeadTask {
		return fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			ErrInvalidMessage, messaging.MessageTypeSMRLeadTask, msg.JupyterMessageType())
	}

	m.log.Debug(utils.LightBlueStyle.Render("Received \"%s\" message from %v: %s"),
		messaging.MessageTypeSMRLeadTask, kernelReplica.String(), msg.StringFormatted())

	// Decode the jupyter.MessageSMRLeadTask message.
	leadMessage, err := m.decodeLeadMessageContent(msg)
	if err != nil {
		return err
	}

	// The ID of the Jupyter "execute_request" message that initiated the associated training.
	executeRequestMsgId := leadMessage.ExecuteRequestMsgId

	activeExecution := m.GetActiveExecution(executeRequestMsgId)
	if activeExecution == nil {
		errorMessage := fmt.Sprintf(
			"Cannot find active execution with \"execute_request\" message ID of \"%s\" associated with kernel \"%s\"...\n",
			executeRequestMsgId, m.Kernel.ID())
		m.log.Error(utils.RedStyle.Render(errorMessage))

		if m.notificationCallback != nil {
			go m.notificationCallback("Cannot Find Active Execution", errorMessage, messaging.ErrorNotification)
		}

		return fmt.Errorf("could not find active execution with jupyter request ID of \"%s\" associated with kernel \"%s\"",
			executeRequestMsgId, m.Kernel.ID())
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	activeExecution.ActiveReplica = kernelReplica
	m.LastPrimaryReplica = kernelReplica

	// We pass (as the second argument) the time at which the kernel replica began executing the code.
	m.processExecutionStartLatency(activeExecution, time.UnixMilli(leadMessage.UnixMilliseconds))

	// Record that the kernel has started training.
	if err = kernelReplica.KernelStartedTraining(); err != nil {
		m.log.Error("Failed to start training for kernel replica %s-%d: %v", m.Kernel.ID(),
			kernelReplica.ReplicaID(), err)

		if m.notificationCallback != nil {
			go m.notificationCallback(fmt.Sprintf("Failed to Start Training for Kernel \"%s\"",
				m.Kernel.ID()), err.Error(), messaging.ErrorNotification)
		}

		return err
	}

	m.log.Debug("Session \"%s\" has successfully started training on replica %d.",
		m.Kernel.ID(), kernelReplica.ReplicaID())

	return nil
}

// ExecutionComplete should be called by the Kernel associated with the target ExecutionManager when an "execute_reply"
// message is received.
//
// ExecutionComplete returns nil on success.
func (m *ExecutionManager) ExecutionComplete(msg *messaging.JupyterMessage) (*Execution, error) {
	err := validateReply(msg)
	if err != nil {
		return nil, err
	}

	requestId := msg.JupyterParentMessageId()

	// Attempt to load the execution from the "active" map.
	activeExecution, loaded := m.ActiveExecutions[requestId]
	if !loaded {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, requestId)
	}

	if activeExecution.HasValidOriginalSentTimestamp() {
		latency := time.Since(activeExecution.OriginallySentAt)
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %m. Total time elapsed since submission: %v.",
			activeExecution.ExecuteRequestMessageId, m.Kernel.ID(), msg.ReplicaId, latency)
	} else {
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %m.",
			activeExecution.ExecuteRequestMessageId, m.Kernel.ID(), msg.ReplicaId)
	}

	err = activeExecution.ReceivedLeadNotification(msg.ReplicaId)
	if err != nil {
		return nil, err
	}

	activeExecution.SetExecuted()

	// Update the execution's state.
	activeExecution.State = Completed

	// Remove the execution from the "active" map.
	delete(m.ActiveExecutions, requestId)

	// Store the execution in the "finished" map.
	m.FinishedExecutions[requestId] = activeExecution

	if activeExecution.ActiveReplica == nil {
		m.log.Warn("Execution \"%s\" does not have its ActiveReplica specified, despite having just finished...")

	}

	reason := "Received \"execute_reply\" message, indicating that the training has stopped."
	err = activeExecution.ActiveReplica.KernelStoppedTraining(reason)
	if err != nil {
		m.log.Error("Error while calling KernelStoppedTraining on active replica %d for execution \"%s\": %v",
			activeExecution.ActiveReplica.ReplicaID(), msg.JupyterParentMessageId(), err)
		return nil, err
	}

	return activeExecution, nil
}

// GetActiveExecution returns a pointer to the Execution struct identified by the given message ID,
// or nil if no such Execution exists.
func (m *ExecutionManager) GetActiveExecution(msgId string) *Execution {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.AllExecutions[msgId]
}

// NumActiveExecutionOperations returns the number of active Execution structs registered with the ExecutionManager.
//
// This method is thread safe.
func (m *ExecutionManager) NumActiveExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.ActiveExecutions)
}

// TotalNumExecutionOperations returns the total number of Execution structs registered with the ExecutionManager.
//
// This method is thread safe.
func (m *ExecutionManager) TotalNumExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.AllExecutions)
}

// decodeLeadMessageContent decodes the content frame of the given *messaging.JupyterMessage into a
// messaging.MessageSMRLeadTask struct and returns the messaging.MessageSMRLeadTask struct.
func (m *ExecutionManager) decodeLeadMessageContent(msg *messaging.JupyterMessage) (*messaging.MessageSMRLeadTask, error) {
	var leadMessage *messaging.MessageSMRLeadTask
	if err := msg.JupyterFrames.DecodeContent(&leadMessage); err != nil {
		m.log.Error(utils.RedStyle.Render("Failed to decode content of SMR LeadProposal ZMQ message: %v\n"), err)

		if m.notificationCallback != nil {
			go m.notificationCallback("Failed to Decode \"smr_lead_task\" Message",
				err.Error(), messaging.ErrorNotification)
		}

		return nil, err
	}

	return leadMessage, nil
}

// processExecutionStartLatency attempts to compute the "execution start latency", which is a core metric for
// interactivity, from the metadata of the active execution and the metadata included in the "smr_lead_task" message.
func (m *ExecutionManager) processExecutionStartLatency(activeExecution *Execution, startedProcessingAt time.Time) {
	if activeExecution.HasValidOriginalSentTimestamp() {
		// Measure of the interactivity.
		// The latency here is calculated as the difference between when the kernel replica began executing the
		// user-submitted code, and the time at which the user's Jupyter client sent the "execute_request" message.
		latency := startedProcessingAt.Sub(activeExecution.OriginallySentAt)

		// Record metrics in Prometheus.
		if activeExecution.HasValidWorkloadId() {
			m.executionLatencyCallback(latency, activeExecution.WorkloadId, m.Kernel.ID())
		} else {
			m.log.Warn("Execution for \"execute_request\" \"%s\" had \"sent-at\" timestamp, but no workload ID...",
				activeExecution.ExecuteRequestMessageId)
		}
	} else {
		m.log.Warn("Execution for \"execute_request\" \"%s\" did not have original \"send\" timestamp available.",
			activeExecution.ExecuteRequestMessageId)
	}
}
