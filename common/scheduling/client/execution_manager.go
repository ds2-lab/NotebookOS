package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"sync"
	"sync/atomic"
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
		return fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			ErrInvalidMessage, messaging.ShellExecuteReply, msg.JupyterMessageType())
	}

	return nil
}

// ExecutionManager manages the Execution instances associated with a DistributedKernelClient / scheduling.Kernel.
type ExecutionManager struct {
	log logger.Logger

	// Kernel is the kernel associated with the ExecutionManager.
	Kernel scheduling.Kernel

	// lastPrimaryReplica is the KernelReplica that served as the primary replica for the previous
	// code execution. It will be nil if no code executions have occurred.
	lastPrimaryReplica scheduling.KernelReplica

	// statisticsProvider exposes two functions: one for updating *statistics.ClusterStatistics and another
	// for updating Prometheus metrics.
	statisticsProvider scheduling.StatisticsProvider

	// activeExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// activeExecutions contains only code submissions that have not yet completed.
	// There should typically just be one entry in the activeExecutions map at a time,
	// but because messages can be weirdly delayed and reordered, there may be multiple.
	activeExecutions map[string]*Execution

	// failedExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// failedExecutions contains only code submissions that have failed and are in the
	// process of being migrated and resubmitted.
	failedExecutions map[string]*Execution

	// finishedExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// finishedExecutions contains only Execution structs that represent code submissions
	// that completed successfully, without error.
	finishedExecutions map[string]*Execution

	// erredExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// erredExecutions contains only Execution structs that represent code submissions
	// that failed to complete successfully and were abandoned.
	erredExecutions map[string]*Execution

	// allExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// allExecutions contains an entry for every single Execution, regardless of
	// the State of the Execution.
	allExecutions map[string]*Execution

	// lastTrainingEndedAt is the time at which the last completed training ended/stopped.
	lastTrainingEndedAt time.Time

	// lastTrainingStartedAt is the time at which the last training that entered the 'active' state did so.
	lastTrainingStartedAt time.Time

	// lastTrainingSubmittedAt is the time at which the last training to be submitted to a kernel was submitted.
	lastTrainingSubmittedAt time.Time

	// ExecutionIndices is a map from Execution ID (i.e., the "msg_id" of the associated "execute_request" [or
	// "yield_request"] message) to the ExecutionIndex of that Execution.
	//
	// An Execution's ExecutionIndex uniquely identifies the Execution and enables a total ordering between
	// all Execution structs.
	executionIndices map[string]int32

	// executionIndicesToExecutions is a mapping from ExecutionIndex to *Execution.
	executionIndicesToExecutions map[int32]*Execution

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

	// NumReplicas is how many replicas the Kernel has.
	NumReplicas int

	mu sync.Mutex

	// nextExecutionIndex is the index of the next "execute_request" to be submitted.
	nextExecutionIndex atomic.Int32

	// submittedExecutionIndex is used to decide if a KernelReplicaClient should actually transition into training upon
	// receiving a "smr_lead_task" message, or if it should essentially just ignore the message.
	//
	// "smr_lead_task" messages are submitted as soon as training begins as an IOPub message by the kernel; however,
	// it's possible for the "execute_reply" -- which is sent when training ends -- to be received BEFORE the
	// "smr_lead_task", as they're sent on separate channels and thus their order is not guaranteed.
	//
	// If an "execute_reply" message is received before the kernel has started to train, then we just assume that we
	// missed the "smr_lead_task" -- it could have been dropped or delayed, we don't know. If we later receive the
	// (apparently delayed) "smr_lead_task" message, then we just ignore it.
	//
	// And we know to ignore it by comparing the execution indices.
	//
	// submittedExecutionIndex is specifically the execution index of the most up-to-date training for which we received
	// the "smr_lead_task" message. By up to date, we mean that the training occurred most recently in terms of what
	// the client is doing/submitting.
	//
	// completedExecutionIndex, a related field, is the execution index of the most up-to-date training for which
	// we received an "execute_reply". By up to date, we mean that the training occurred most recently in terms of what
	//	// the client is doing/submitting.
	submittedExecutionIndex int32

	// activeExecutionIndex works together with the activeExecutionIndex and completedExecutionIndex fields to achieve
	// the goals outlined in the documentation of the submittedExecutionIndex field.
	// See the submittedExecutionIndex field for more info.
	//
	// activeExecutionIndex is the execution index of the most up-to-date training for which we received a
	// "smr_lead_task". By up to date, we mean that the training occurred most recently in terms of what the client is
	// doing/submitting.
	//
	// completedExecutionIndex, a related field, is the execution index of the most up-to-date training for which
	// we received an "execute_reply". By up to date, we mean that the training occurred most recently in terms of what
	// the client is doing/submitting.
	//
	// submittedExecutionIndex, a related field, is specifically the execution index of the most up-to-date training for
	// which we received the "smr_lead_task" message. By up to date, we mean that the training occurred most recently
	// in terms of what the client is doing/submitting.
	activeExecutionIndex int32

	// completedExecutionIndex works together with the submittedExecutionIndex and activeExecutionIndex fields to
	// achieve the goals outlined in the documentation of the submittedExecutionIndex field.
	// See the submittedExecutionIndex field for more info.
	//
	// completedExecutionIndex is the execution index of the most up-to-date training for which we received an
	// "execute_reply". By up to date, we mean that the training occurred most recently in terms of what the client is
	// doing/submitting.
	//
	// submittedExecutionIndex, a related field, is specifically the execution index of the most up-to-date training for
	// which we received the "smr_lead_task" message. By up to date, we mean that the training occurred most recently
	// in terms of what the client is doing/submitting.
	completedExecutionIndex int32
}

// NewExecutionManager creates a new ExecutionManager struct to be associated with the given kernel.
//
// NewExecutionManager returns a pointer to the new ExecutionManager struct.
func NewExecutionManager(kernel scheduling.Kernel, numReplicas int, execFailCallback scheduling.ExecutionFailedCallback,
	notifyCallback scheduling.NotificationCallback, latencyCallback scheduling.ExecutionLatencyCallback,
	statsProvider scheduling.StatisticsProvider) *ExecutionManager {

	manager := &ExecutionManager{
		activeExecutions:             make(map[string]*Execution),
		finishedExecutions:           make(map[string]*Execution),
		erredExecutions:              make(map[string]*Execution),
		allExecutions:                make(map[string]*Execution),
		executionIndicesToExecutions: make(map[int32]*Execution),
		executionIndices:             make(map[string]int32),
		failedExecutions:             make(map[string]*Execution),
		NumReplicas:                  numReplicas,
		Kernel:                       kernel,
		submittedExecutionIndex:      -1,
		activeExecutionIndex:         -1,
		completedExecutionIndex:      -1,
		executionFailedCallback:      execFailCallback,
		notificationCallback:         notifyCallback,
		executionLatencyCallback:     latencyCallback,
		statisticsProvider:           statsProvider,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

// ExecutionIndexIsLarger returns true if the given executionIndex is larger than all 3 of the execution-index-related
// fields of the KernelReplicaClient, namely submittedExecutionIndex, activeExecutionIndex, and completedExecutionIndex.
func (m *ExecutionManager) ExecutionIndexIsLarger(executionIndex int32) bool {
	return executionIndex > m.submittedExecutionIndex && executionIndex > m.activeExecutionIndex && executionIndex > m.completedExecutionIndex
}

// SendingExecuteRequest records that an "execute_request" (or "yield_request") message is being sent.
//
// SendingExecuteRequest should be called RIGHT BEFORE the "execute_request" message is ACTUALLY sent.
func (m *ExecutionManager) SendingExecuteRequest(msg *messaging.JupyterMessage) error {
	err := validateRequest(msg)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	requestId := msg.JupyterMessageId()

	executionIndex, loaded := m.executionIndices[requestId]
	if !loaded {
		return fmt.Errorf("%w: could not find execution index associated with request \"%s\"",
			ErrUnknownActiveExecution, requestId)
	}

	if executionIndex < m.submittedExecutionIndex {
		execution := m.executionIndicesToExecutions[m.submittedExecutionIndex]
		if execution == nil { // Sanity check.
			m.log.Error(utils.RedStyle.Render("Expected to find Execution associated with last-submitted index %d."),
				m.submittedExecutionIndex)

			err = fmt.Errorf("%w: submitted execution \"%s\" with index %d, and cannot find newer execution with index %d",
				ErrInvalidState, msg.JupyterMessageId(), executionIndex, m.submittedExecutionIndex)

			m.sendNotification("Execution Manager in Invalid TransactionState", err.Error(), messaging.ErrorNotification, true)

			return err
		}

		m.log.Error("Submitting execute request \"%s\" with index=%d; however, last submitted execution had index=%d and ID=%s.",
			requestId, executionIndex, m.submittedExecutionIndex, execution.ExecuteRequestMessageId)

		err = fmt.Errorf("%w: submitting execute request \"%s\" with index=%d; however, last submitted execution had index=%d and ID=%s",
			ErrInconsistentExecutionIndices, requestId, executionIndex, m.submittedExecutionIndex, execution.ExecuteRequestMessageId)

		m.sendNotification("Inconsistent Execution Indices", err.Error(), messaging.ErrorNotification, true)

		return err
	}

	m.submittedExecutionIndex = executionIndex
	m.lastTrainingSubmittedAt = time.Now()

	return nil
}

// RegisterExecution registers a newly-submitted "execute_request" with the ExecutionManager.
func (m *ExecutionManager) RegisterExecution(msg *messaging.JupyterMessage) (scheduling.Execution, error) {
	err := validateRequest(msg)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	requestId := msg.JupyterMessageId()
	if _, loaded := m.finishedExecutions[requestId]; loaded {
		m.log.Error("Attempt made to re-register completed execution \"%s\"", requestId)
		return nil, fmt.Errorf("%w: execution ID=\"%s\"", ErrDuplicateExecution, requestId)
	}

	existingExecution, loaded := m.failedExecutions[requestId]
	if loaded {
		nextExecutionAttempt := m.registerExecutionAttempt(msg, existingExecution)
		return nextExecutionAttempt, nil
	}

	executionIndex := m.nextExecutionIndex.Add(1)
	m.executionIndices[requestId] = executionIndex

	newExecution := NewExecution(m.Kernel.ID(), 1, m.NumReplicas, executionIndex, msg)
	m.allExecutions[requestId] = newExecution
	m.activeExecutions[requestId] = newExecution
	m.executionIndicesToExecutions[executionIndex] = newExecution

	m.log.Debug("Registered new execution \"%s\" (idx=%d) for kernel \"%s\"",
		requestId, executionIndex, m.Kernel.ID())

	return newExecution, nil
}

func (m *ExecutionManager) ExecutionFailedCallback() scheduling.ExecutionFailedCallback {
	return m.executionFailedCallback
}

// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
// code execution, or nil if no code executions have occurred.
func (m *ExecutionManager) LastPrimaryReplica() scheduling.KernelReplica {
	return m.lastPrimaryReplica
}

// YieldProposalReceived is called when we receive a YieldProposal from a replica of a kernel.
//
// YieldProposalReceived registers the YieldProposal with the kernel's associated Execution struct.
//
// If we find that we've received all three proposals, and they were ALL YieldProposal, then we'll invoke the
// "failure handler", which will  handle the situation according to the cluster's configured scheduling policy.
func (m *ExecutionManager) YieldProposalReceived(replica scheduling.KernelReplica,
	executeReplyMsg *messaging.JupyterMessage, msgErr *messaging.MessageErrorWithYieldReason) error {

	replica.ReceivedExecuteReply(executeReplyMsg, true)

	if err := validateReply(executeReplyMsg); err != nil {
		return err
	}

	m.log.Debug("Received 'YIELD' proposal from replica %d for execution \"%s\"",
		replica.ReplicaID(), executeReplyMsg.JupyterParentMessageId())

	// targetExecuteRequestId is the Jupyter message ID of the "execute_request" message associated
	// with the 'YIELD' proposal that we just received.
	targetExecuteRequestId := executeReplyMsg.JupyterParentMessageId()

	m.mu.Lock()

	// Find the associated Execution struct.
	activeExecution, loaded := m.activeExecutions[targetExecuteRequestId]
	if !loaded {
		m.log.Warn("Could not find active Execution with ID \"%s\"...", targetExecuteRequestId)

		// Check the other executions in case we received the 'YIELD' message after the response from the leader.
		activeExecution, loaded = m.allExecutions[targetExecuteRequestId]
		if loaded {
			m.log.Warn("Instead, found %s Execution with ID \"%s\"...",
				activeExecution.State.String(), targetExecuteRequestId)
		}
	}

	if activeExecution == nil {
		m.log.Error("Could not find any Execution with ID \"%s\"...", targetExecuteRequestId)

		m.mu.Unlock()
		return fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, targetExecuteRequestId)
	}

	// This will return an error if the replica did not have any pre-committed resources.
	// So, we can just ignore the error.
	_ = m.Kernel.ReleasePreCommitedResourcesFromReplica(replica, executeReplyMsg)

	// It's possible we received a 'YIELD' proposal for an Execution different from the current one.
	// So, retrieve the Execution associated with the 'YIELD' proposal (using the "execute_request" message IDs).
	associatedActiveExecution := m.getActiveExecution(targetExecuteRequestId)

	// If we couldn't find the associated active execution at all, then we should return an error, as that is bad.
	if associatedActiveExecution == nil {
		m.log.Error(utils.RedStyle.Render("Received 'YIELD' proposal from replica %d of kernel %s targeting unknown "+
			"Execution associated with an \"execute_request\" message with msg_id=\"%s\"..."),
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId)

		m.mu.Unlock()
		return fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, targetExecuteRequestId)
	}

	// Mark that we received the 'YIELD' proposal for the associated Execution.
	err := associatedActiveExecution.ReceivedYieldNotification(replica.ReplicaID(), msgErr.YieldReason)

	m.log.Debug("Received 'YIELD' proposal from replica %d of kernel %s for Execution associated with "+
		"\"execute_request\" \"%s\". Received %d/%d proposals from replicas of kernel %s.",
		replica.ReplicaID(), replica.ID(), targetExecuteRequestId, associatedActiveExecution.NumRolesReceived(),
		associatedActiveExecution.GetNumReplicas(), replica.ID())

	// If we have a non-nil error, and it isn't just that all the replicas proposed YIELD, then return it directly.
	if err != nil && !errors.Is(err, ErrExecutionFailedAllYielded) {
		m.log.Error("Encountered error while processing 'YIELD' proposal from replica %d of kernel %s for Execution associated with \"execute_request\" \"%s\": %v",
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId, err)

		m.mu.Unlock()
		return err
	}

	// If we have a non-nil error, and it's just that all replicas proposed YIELD, then we'll call the handler.
	if errors.Is(err, ErrExecutionFailedAllYielded) {
		// Concatenate all the yield reasons.
		//
		// We'll return them along with the error returned by handleFailedExecutionAllYielded if
		// handleFailedExecutionAllYielded returns a non-nil error.
		yieldErrors := make([]error, 0, 4)
		associatedActiveExecution.RangeRoles(func(i int32, proposal scheduling.Proposal) bool {
			yieldError := fmt.Errorf("replica %d proposed \"YIELD\" because: %s", i, proposal.GetReason())
			yieldErrors = append(yieldErrors, yieldError)
			return true
		})

		m.log.Debug("All %d replicas of kernel \"%s\" proposed 'YIELD' for execution \"%s\".",
			m.Kernel.Size(), m.Kernel.ID(), targetExecuteRequestId)

		delete(m.activeExecutions, targetExecuteRequestId)
		m.failedExecutions[targetExecuteRequestId] = associatedActiveExecution

		executeRequestMsg, err := m.getExecuteRequestForResubmission(executeReplyMsg)
		if err != nil {
			m.log.Error("Could not find original \"execute_request\" message for execution \"%s\" (index=%d).",
				targetExecuteRequestId, associatedActiveExecution.ExecutionIndex)
		}

		// Call the handler.
		//
		// If it returns an error, then we'll join that error with the YIELD errors, and return them all together.
		m.mu.Unlock()
		handlerError := m.executionFailedCallback(m.Kernel, executeRequestMsg)
		if handlerError != nil {
			allErrors := append([]error{handlerError}, yieldErrors...)
			return errors.Join(allErrors...)
		}

		return nil // Explicitly return here, so we can safely stick another m.mu.Unlock() call down below.
	}

	m.mu.Unlock()
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

	return m.handleSmrLeadTaskMessage(kernelReplica, msg)
}

// HandleExecuteReplyMessage is called by a scheduling.Kernel when an "execute_reply" message is received.
//
// HandleExecuteReplyMessage returns a bool flag indicating whether the message is a 'YIELD' message or ot.
func (m *ExecutionManager) HandleExecuteReplyMessage(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (bool, error) {
	if err := validateReply(msg); err != nil {
		return false, err
	}

	kernelId := m.Kernel.ID()
	m.log.Debug("Received \"execute_reply\" with JupyterID=\"%s\" from replica %d of kernel %s.",
		msg.JupyterMessageId(), replica.ReplicaID(), kernelId)

	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if msg.JupyterFrames.LenWithoutIdentitiesFrame(true) < 5 {
		m.log.Error("Received invalid Jupyter message from replica %d of kernel %s (detected in extractShellError)",
			replica.ReplicaID(), kernelId)
		return false, messaging.ErrInvalidJupyterMessage
	}

	if len(*msg.JupyterFrames.ContentFrame()) == 0 {
		m.log.Warn("Received shell '%v' response with empty content.", msg.JupyterMessageType())
		return false, nil
	}

	var msgErr *messaging.MessageErrorWithYieldReason
	if err := json.Unmarshal(*msg.JupyterFrames.ContentFrame(), &msgErr); err != nil {
		m.log.Error("Failed to unmarshal shell message received from replica %d of kernel %s because: %v",
			replica.ReplicaID(), kernelId, err)
		return false, err
	}

	isYieldProposal := msgErr.ErrName == messaging.MessageErrYieldExecution

	activeExec := m.getActiveExecution(msg.JupyterParentMessageId())
	if activeExec != nil {
		err := activeExec.RegisterReply(replica.ReplicaID(), msg, true)
		if err != nil {
			m.log.Error("Failed to register \"execute_reply\" message: %v", err)
			return isYieldProposal, err
		}
	}

	if isYieldProposal {
		err := m.YieldProposalReceived(replica, msg, msgErr)
		if err != nil {
			msg.IsFailedExecuteRequest = true
			return true, errors.Join(err, fmt.Errorf("%s: %s", msgErr.ErrName, msgErr.ErrValue))
		}

		return true, messaging.ErrExecutionYielded
	}

	_, err := m.ExecutionComplete(msg, replica)

	return false, err // Will be nil if everything went OK in the call to ExecutionComplete
}

// ExecutionComplete should be called by the Kernel associated with the target ExecutionManager when an "execute_reply"
// message is received.
//
// ExecutionComplete returns nil on success.
func (m *ExecutionManager) ExecutionComplete(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (scheduling.Execution, error) {
	err := validateReply(msg)
	if err != nil {
		return nil, err
	}

	executeRequestId := msg.JupyterParentMessageId()

	// Attempt to load the execution from the "active" map.
	activeExecution, loaded := m.activeExecutions[executeRequestId]
	if !loaded {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, executeRequestId)
	}

	if activeExecution.HasValidOriginalSentTimestamp() {
		latency := time.Since(activeExecution.OriginallySentAt)
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d. Total time elapsed since submission: %v.",
			activeExecution.GetExecuteRequestMessageId(), m.Kernel.ID(), msg.ReplicaId, latency)
	} else {
		m.log.Debug("Execution %s targeting kernel %s has been completed successfully by replica %d.",
			activeExecution.GetExecuteRequestMessageId(), m.Kernel.ID(), msg.ReplicaId)
	}

	err = activeExecution.ReceivedLeadNotification(msg.ReplicaId)
	if err != nil {
		return nil, err
	}

	activeExecution.SetExecuted()

	// Update the execution's state.
	activeExecution.State = Completed

	// Remove the execution from the "active" map.
	delete(m.activeExecutions, executeRequestId)

	// Store the execution in the "finished" map.
	m.finishedExecutions[executeRequestId] = activeExecution

	if m.statisticsProvider != nil && m.statisticsProvider.PrometheusMetricsEnabled() {
		m.statisticsProvider.IncrementNumTrainingEventsCompletedCounterVec()
	}

	if m.statisticsProvider != nil {
		m.statisticsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.CompletedTrainings += 1
			statistics.NumIdleSessions += 1
		})
	}

	if activeExecution.ExecutionIndex == m.submittedExecutionIndex {
		m.log.Debug("Received \"execute_reply\" for execution \"%s\" with index=%d matching last-submitted execution's index. 'Completed' execution index %d → %d.",
			executeRequestId, activeExecution.ExecutionIndex, m.completedExecutionIndex, activeExecution.ExecutionIndex)

		m.completedExecutionIndex = activeExecution.ExecutionIndex
		m.lastTrainingEndedAt = time.Now()
	} else {
		m.log.Warn("Received \"execute_reply\" for execution \"%s\" with index=%d; however, latest submitted execution has index=%d.",
			executeRequestId, activeExecution.ExecutionIndex, m.submittedExecutionIndex)

		if activeExecution.ExecutionIndex > m.completedExecutionIndex {
			m.log.Warn("\"execute_reply\" for execution \"%s\" with index=%d is still greater than last completed index (%d). 'Completed' execution index %d → %d.",
				executeRequestId, activeExecution.ExecutionIndex, m.completedExecutionIndex, m.completedExecutionIndex, activeExecution.ExecutionIndex)

			m.completedExecutionIndex = activeExecution.ExecutionIndex
		}
	}

	if activeExecution.ExecutionIndex != m.activeExecutionIndex {
		m.log.Warn("\"execute_reply\" with index %d does not match last-known active index %d...",
			activeExecution.ExecutionIndex, m.activeExecutionIndex)
	}

	if activeExecution.ActiveReplica == nil {
		m.log.Warn("ActiveReplica unspecified for exec \"%s\", despite having just finished...", executeRequestId)

		activeExecution.SetActiveReplica(replica)
		m.lastPrimaryReplica = replica
	}

	if activeExecution.ActiveReplica.ReplicaID() != replica.ReplicaID() {
		return m.handleInconsistentPrimaryReplicas(msg, replica, activeExecution)
	}

	reason := "Received \"execute_reply\" message, indicating that the training has stopped."
	err = activeExecution.ActiveReplica.KernelStoppedTraining(reason)
	if err != nil {
		m.log.Error("Error while calling KernelStoppedTraining on active replica %d for execution \"%s\": %v",
			activeExecution.ActiveReplica.ReplicaID(), msg.JupyterParentMessageId(), err)
		return nil, err
	}

	// We'll notify ALL replicas that we received a response, so the next execute requests can be sent (at least
	// to the local daemons).
	for _, kernelReplica := range m.Kernel.Replicas() {
		m.log.Debug("Notifying non-primary replica %d of kernel %s that execution \"%s\" has concluded successfully.",
			kernelReplica.ReplicaID(), kernelReplica.ID(), executeRequestId)

		kernelReplica.ReceivedExecuteReply(msg, kernelReplica.ReplicaID() == replica.ReplicaID())

		// Make sure all pre-committed resources for this request are released.
		// Skip the active replica, as we already called these methods on/for that replica.
		if kernelReplica.ReplicaID() != activeExecution.ActiveReplica.ReplicaID() {
			container := kernelReplica.Container()
			_ = container.Host().ReleasePreCommitedResources(container, executeRequestId)
		}
	}

	if activeExecution.ActiveReplica.ReplicaID() != replica.ReplicaID() {
		return m.handleInconsistentPrimaryReplicas(msg, replica, activeExecution)
	}

	return activeExecution, nil
}

// LastTrainingSubmittedAt returns the time at which the last training to occur was submitted to the kernel.
// If there is an active training when LastTrainingSubmittedAt is called, then LastTrainingSubmittedAt will return
// the time at which the active training was submitted to the kernel.
func (m *ExecutionManager) LastTrainingSubmittedAt() time.Time {
	return m.lastTrainingSubmittedAt
}

// LastTrainingStartedAt returns the time at which the last training to occur began. If there is an active
// training when LastTrainingStartedAt is called, then LastTrainingStartedAt will return the time at which
// the active training began.
func (m *ExecutionManager) LastTrainingStartedAt() time.Time {
	return m.lastTrainingStartedAt
}

// LastTrainingEndedAt returns the time at which the last completed training ended.
//
// If the kernel is currently training, then TrainingEndedAt returns the time at which the previous training ended.
func (m *ExecutionManager) LastTrainingEndedAt() time.Time {
	return m.lastTrainingEndedAt
}

// GetActiveExecution returns a pointer to the Execution struct identified by the given message ID,
// or nil if no such Execution exists.
//
// GetActiveExecution is thread-safe.
func (m *ExecutionManager) GetActiveExecution(msgId string) scheduling.Execution {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.allExecutions[msgId]
}

// NumActiveExecutionOperations returns the number of active Execution structs registered with the ExecutionManager.
//
// This method is thread safe.
func (m *ExecutionManager) NumActiveExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.activeExecutions)
}

// TotalNumExecutionOperations returns the total number of Execution structs registered with the ExecutionManager.
//
// This method is thread safe.
func (m *ExecutionManager) TotalNumExecutionOperations() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.allExecutions)
}

// handleSmrLeadTaskMessage is the critical section of HandleSmrLeadTaskMessage.
func (m *ExecutionManager) handleSmrLeadTaskMessage(replica scheduling.KernelReplica, msg *messaging.JupyterMessage) error {
	// Decode the jupyter.MessageSMRLeadTask message.
	leadMessage, err := m.decodeLeadMessageContent(msg)
	if err != nil {
		return err
	}

	// The ID of the Jupyter "execute_request" message that initiated the associated training.
	executeRequestMsgId := leadMessage.ExecuteRequestMsgId

	m.mu.Lock()
	defer m.mu.Unlock()

	activeExecution := m.getActiveExecution(executeRequestMsgId)
	if activeExecution == nil {
		errorMessage := fmt.Sprintf(
			"Cannot find active activeExecution with \"execute_request\" message ID of \"%s\" associated with kernel \"%s\"...\n",
			executeRequestMsgId, m.Kernel.ID())
		m.log.Error(utils.RedStyle.Render(errorMessage))

		m.sendNotification("Cannot Find Active Execution", errorMessage,
			messaging.ErrorNotification, true)

		return fmt.Errorf("could not find active activeExecution with jupyter request ID of \"%s\" associated with kernel \"%s\"",
			executeRequestMsgId, m.Kernel.ID())
	}

	executionIndex := activeExecution.GetExecutionIndex()

	// If the execution index is less than the index of the most-recently-submitted execution, then this is an old
	// "smr_lead_task" message, and we can simply discard it.
	if executionIndex < m.submittedExecutionIndex {
		moreRecentExecution := m.executionIndicesToExecutions[m.submittedExecutionIndex]

		m.log.Warn("Execution \"%s\" is old (index=%d). We've since submitted execution \"%s\" (index=%d).",
			executeRequestMsgId, executionIndex, moreRecentExecution.ExecuteRequestMessageId, moreRecentExecution.ExecutionIndex)
		m.log.Warn("Discarding \"smr_lead_task\" message \"%s\" associated with (old) execution \"%s\".",
			msg.JupyterMessageId(), executeRequestMsgId)

		// TODO: Should we still check if this "smr_lead_task" message is more recent than whatever the last one we
		//       received? And if so, then should we update the associated field, even if we're discarding the message?
		return nil
	}

	// If the execution index associated with the "smr_lead_task" message matches that of the last-submitted
	// "execute_request" message, then we can update the 'activeExecutionIndex' field.
	if executionIndex == m.submittedExecutionIndex {
		m.log.Debug("Execution index associated with \"smr_lead_task\" message (%d) for execution \"%s\" matches last-submitted index.",
			executionIndex, executeRequestMsgId)

		m.activeExecutionIndex = executionIndex
	}

	activeExecution.SetActiveReplica(replica)
	m.lastPrimaryReplica = replica

	// We pass (as the second argument) the time at which the kernel replica began executing the code.
	m.processExecutionStartLatency(activeExecution, time.UnixMilli(leadMessage.UnixMilliseconds))

	// If the 'completed' execution index is greater than or equal to the execution index of the "smr_lead_task"
	// message, then that means that we received the "smr_lead_task" AFTER receiving the "execute_reply". So,
	// we've already processed the execution has having completed. We should NOT call KernelStartedTraining, or
	// very bad things will happen.
	if m.completedExecutionIndex >= executionIndex {
		m.log.Debug("Execution index of \"smr_lead_task\" message for execution \"%s\" %d <= 'completed' execution index %d. This training has already completed.",
			executeRequestMsgId, executionIndex, m.completedExecutionIndex)

		// The training is already over. We can just return. No need to return an error, as messages can be delayed
		// sometimes. It's not a big deal.
		return nil
	}

	// This is the most up-to-date training event to begin training, so we'll record the timestamp in
	// the 'lastTrainingStartedAt' field.
	m.lastTrainingStartedAt = time.UnixMilli(leadMessage.UnixMilliseconds)

	// Record that the kernel has started training.
	if err = replica.KernelStartedTraining(time.UnixMilli(leadMessage.UnixMilliseconds)); err != nil {
		m.log.Error("Failed to start training for kernel replica %s-%d: %v", m.Kernel.ID(),
			replica.ReplicaID(), err)

		m.sendNotification(fmt.Sprintf("Failed to Start Training for kernel \"%s\"",
			m.Kernel.ID()), err.Error(), messaging.ErrorNotification, true)

		return err
	}

	m.log.Debug("Session \"%s\" has successfully started training on replica %d.",
		m.Kernel.ID(), replica.ReplicaID())

	return nil
}

// handleInconsistentPrimaryReplicas is called when we received a valid "execute_reply" from a primary replica,
// but the ID of the replica that sent the "execute_reply" does not match the ID of the ActiveReplica field of
// the associated Execution struct. This indicates that the replica that sent the "smr_lead_task" message is
// not the same as the replica that sent the valid "execute_reply" message, which should really never happen.
func (m *ExecutionManager) handleInconsistentPrimaryReplicas(msg *messaging.JupyterMessage,
	replica scheduling.KernelReplica, activeExecution *Execution) (scheduling.Execution, error) {

	requestId := msg.JupyterParentMessageId()

	m.log.Error("Received 'execute_reply' from primary replica %d for execution \"%s\", "+
		"but we previously recorded that replica %d was the primary replica for this execution...",
		activeExecution.ActiveReplica.ReplicaID(), requestId, replica.ReplicaID())

	m.sendNotification(
		fmt.Sprintf("Inconsistent Primary Replicas for Completed Code Execution \"%s\" of kernel \"%s\"",
			requestId, m.Kernel.ID()),
		fmt.Sprintf("Received 'execute_reply' from primary replica %d for execution \"%s\", "+
			"but we previously recorded that replica %d was the primary replica for this execution...",
			activeExecution.ActiveReplica.ReplicaID(), requestId, replica.ReplicaID()),
		messaging.ErrorNotification,
		true,
	)

	reason := "Received \"execute_reply\" message, indicating that the training has stopped."

	// We'll attempt to call 'stop training' on both replicas in attempt to salvage things,
	// buuuut we're probably screwed.

	var err1, err2 error
	if activeExecution.ActiveReplica.IsTraining() {
		m.log.Warn("Calling KernelStoppedTraining on the replica recorded on the Execution struct for execution '%s'",
			requestId)

		err1 = activeExecution.ActiveReplica.KernelStoppedTraining(reason)
	}

	if replica.IsTraining() {
		m.log.Warn("Calling KernelStoppedTraining on the replica that sent the \"execute_reply\" message for execution '%s'",
			requestId)

		err2 = replica.KernelStoppedTraining(reason)
	}

	// We recorded the errors of each call to KernelStoppedTraining separately.
	// If just one of the errors was non-nil, then we'll just return that single error.
	// If they were both non-nil, then we'll join them and return the joined error.
	// If they were both nil, then we'll also return nil (for our error return value).
	var err error
	if err1 != nil && err2 == nil {
		err = err1
	} else if err1 == nil && err2 != nil {
		err = err2
	} else if err2 != nil && err1 != nil {
		err = errors.Join(err1, err2)
	}

	if err != nil {
		m.log.Error("Error while calling KernelStoppedTraining on active replica %d for execution \"%s\": %v",
			activeExecution.ActiveReplica.ReplicaID(), msg.JupyterParentMessageId(), err)
		return activeExecution, err
	}

	return activeExecution, err
}

// getActiveExecution returns a pointer to the Execution struct identified by the given message ID,
// or nil if no such Execution exists.
//
// getActiveExecution is NOT thread-safe. The thread-safe version is GetActiveExecution.
func (m *ExecutionManager) getActiveExecution(msgId string) *Execution {
	return m.allExecutions[msgId]
}

// registerExecutionAttempt registers a new attempt for an existing execution.
func (m *ExecutionManager) registerExecutionAttempt(msg *messaging.JupyterMessage, existingExecution scheduling.Execution) *Execution {
	requestId := msg.JupyterMessageId()
	nextAttemptNumber := existingExecution.GetAttemptNumber() + 1

	// Create the next execution attempt.
	nextExecutionAttempt := NewExecution(m.Kernel.ID(), nextAttemptNumber, m.NumReplicas,
		existingExecution.GetExecutionIndex(), msg)

	m.log.Debug("Registering new attempt (%d) for execution \"%s\"", nextAttemptNumber, requestId)

	// Link the previous active execution with the current one (in both directions).
	nextExecutionAttempt.LinkPreviousAttempt(existingExecution)
	existingExecution.LinkNextAttempt(nextExecutionAttempt)

	// Replace the entry in the mapping with the next attempt.
	// We can still access the previous attempt by following the "previous attempt" link.
	m.activeExecutions[requestId] = nextExecutionAttempt
	m.executionIndicesToExecutions[nextExecutionAttempt.ExecutionIndex] = nextExecutionAttempt
	m.allExecutions[requestId] = nextExecutionAttempt
	delete(m.failedExecutions, requestId)

	// Return the next execution attempt.
	return nextExecutionAttempt
}

// decodeLeadMessageContent decodes the content frame of the given *messaging.JupyterMessage into a
// messaging.MessageSMRLeadTask struct and returns the messaging.MessageSMRLeadTask struct.
func (m *ExecutionManager) decodeLeadMessageContent(msg *messaging.JupyterMessage) (*messaging.MessageSMRLeadTask, error) {
	var leadMessage *messaging.MessageSMRLeadTask
	if err := msg.JupyterFrames.DecodeContent(&leadMessage); err != nil {
		m.log.Error(utils.RedStyle.Render("Failed to decode content of SMR LeadProposal ZMQ message: %v\n"), err)

		m.sendNotification("Failed to Decode \"smr_lead_task\" Message", err.Error(), messaging.ErrorNotification, true)

		return nil, err
	}

	return leadMessage, nil
}

// processExecutionStartLatency attempts to compute the "execution start latency", which is a core metric for
// interactivity, from the metadata of the active execution and the metadata included in the "smr_lead_task" message.
func (m *ExecutionManager) processExecutionStartLatency(activeExecution scheduling.Execution, startedProcessingAt time.Time) {
	if activeExecution.HasValidOriginalSentTimestamp() {
		// Measure of the interactivity.
		// The latency here is calculated as the difference between when the kernel replica began executing the
		// user-submitted code, and the time at which the user's Jupyter client sent the "execute_request" message.
		latency := startedProcessingAt.Sub(activeExecution.GetOriginallySentAtTime())

		// Record metrics in Prometheus.
		if activeExecution.HasValidWorkloadId() {
			m.executionLatencyCallback(latency, activeExecution.GetWorkloadId(), m.Kernel.ID())
		} else {
			m.log.Warn("Execution for \"execute_request\" \"%s\" had \"sent-at\" timestamp, but no workload ID...",
				activeExecution.GetExecuteRequestMessageId())
		}
	} else {
		m.log.Warn("Execution for \"execute_request\" \"%s\" did not have original \"send\" timestamp available.",
			activeExecution.GetExecuteRequestMessageId())
	}
}

func (m *ExecutionManager) sendNotification(title string, content string, typ messaging.NotificationType, useSeparateGoroutine bool) {
	if m.notificationCallback != nil {
		if useSeparateGoroutine {
			go m.notificationCallback(title, content, typ)
		} else {
			m.notificationCallback(title, content, typ)
		}
	}
}

// getExecuteRequestForResubmission returns the original "execute_request" message associated with
// the given "execute_reply" message so that it can be re-submitted, such as after a migration.
//
// IMPORTANT: By itself, GetExecuteRequestForResubmission is NOT thread safe. The method does NOT acquire any locks.
// However, GetExecuteRequestForResubmission is meant to be called from the ExecutionManager's YieldProposalReceived
// method, which IS thread safe. So, the ExecutionManager's mutex should already be held by the calling thread when
// GetExecuteRequestForResubmission is called, assuming the current goroutine's stack includes a previous call to
// the ExecutionManager's YieldProposalReceived method.
func (m *ExecutionManager) getExecuteRequestForResubmission(executeReply *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	if err := validateReply(executeReply); err != nil {
		return nil, err
	}

	executeRequestId := executeReply.JupyterParentMessageId()
	execution, loaded := m.failedExecutions[executeRequestId]

	if !loaded {
		m.log.Error("Could not find execution \"%s\". Cannot provide original \"execute_request\" message.",
			executeRequestId)
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, executeRequestId)
	}

	m.log.Debug("Returning original \"execute_request\" message for execution \"%s\" (index=%d): %v",
		executeRequestId, execution.ExecutionIndex, execution.JupyterMessage.StringFormatted())

	return execution.JupyterMessage, nil
}
