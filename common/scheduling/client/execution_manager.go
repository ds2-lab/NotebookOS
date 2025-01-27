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
	mu  sync.Mutex

	// Kernel is the kernel associated with the ExecutionManager.
	Kernel scheduling.Kernel

	// NumReplicas is how many replicas the Kernel has.
	NumReplicas int

	// lastPrimaryReplica is the KernelReplica that served as the primary replica for the previous
	// code execution. It will be nil if no code executions have occurred.
	lastPrimaryReplica scheduling.KernelReplica

	// activeExecutions is a map from Jupyter "msg_id" to the Execution encapsulating
	// the code submission with the aforementioned ID.
	//
	// activeExecutions contains only code submissions that have not yet completed.
	// There should typically just be one entry in the activeExecutions map at a time,
	// but because messages can be weirdly delayed and reordered, there may be multiple.
	activeExecutions map[string]*Execution

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

	// ExecutionIndices is a map from Execution ID (i.e., the "msg_id" of the associated "execute_request" [or
	// "yield_request"] message) to the ExecutionIndex of that Execution.
	//
	// An Execution's ExecutionIndex uniquely identifies the Execution and enables a total ordering between
	// all Execution structs.
	executionIndices map[string]int32

	// executionIndicesToExecutions is a mapping from ExecutionIndex to *Execution.
	executionIndicesToExecutions map[int32]*Execution

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

	// statisticsProvider exposes two functions: one for updating *statistics.ClusterStatistics and another
	// for updating Prometheus metrics.
	statisticsProvider scheduling.StatisticsProvider
}

// NewExecutionManager creates a new ExecutionManager struct to be associated with the given Kernel.
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
			panic(fmt.Sprintf("Expected to find Execution associated with last-submitted index %d.",
				m.submittedExecutionIndex))
		}

		m.log.Error("Submitting execute request \"%s\" with index=%d; however, last submitted execution had index=%d and ID=%s.",
			requestId, executionIndex, m.submittedExecutionIndex, execution.ExecuteRequestMessageId)
		// TODO: Return error?
	}

	m.submittedExecutionIndex = executionIndex

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

	existingExecution, loaded := m.activeExecutions[requestId]
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

// registerExecutionAttempt registers a new attempt for an existing execution.
func (m *ExecutionManager) registerExecutionAttempt(msg *messaging.JupyterMessage, existingExecution scheduling.Execution) scheduling.Execution {
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
	replica.ReceivedExecuteReply(msg)

	if err := validateReply(msg); err != nil {
		return err
	}

	m.log.Debug("Received 'YIELD' proposal from replica %d for execution \"%s\"",
		replica.ReplicaID(), msg.JupyterParentMessageId())

	// targetExecuteRequestId is the Jupyter message ID of the "execute_request" message associated
	// with the 'YIELD' proposal that we just received.
	targetExecuteRequestId := msg.JupyterParentMessageId()

	m.mu.Lock()
	defer m.mu.Unlock()

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
		return fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, targetExecuteRequestId)
	}

	// This will return an error if the replica did not have any pre-committed resources.
	// So, we can just ignore the error.
	_ = m.Kernel.ReleasePreCommitedResourcesFromReplica(replica, msg)

	// It's possible we received a 'YIELD' proposal for an Execution different from the current one.
	// So, retrieve the Execution associated with the 'YIELD' proposal (using the "execute_request" message IDs).
	associatedActiveExecution := m.getActiveExecution(targetExecuteRequestId)

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
		associatedActiveExecution.GetNumReplicas(), replica.ID())

	// If we have a non-nil error, and it isn't just that all the replicas proposed YIELD, then return it directly.
	if err != nil && !errors.Is(err, ErrExecutionFailedAllYielded) {
		m.log.Error("Encountered error while processing 'YIELD' proposal from replica %d of kernel %s for Execution associated with \"execute_request\" \"%s\": %v",
			replica.ReplicaID(), replica.ID(), targetExecuteRequestId, err)

		return err
	}

	// If we have a non-nil error, and it's just that all replicas proposed YIELD, then we'll call the handler.
	if errors.Is(err, ErrExecutionFailedAllYielded) {
		// Concatenate all the yield reasons. We'll return them along with the error returned by
		// handleFailedExecutionAllYielded if handleFailedExecutionAllYielded returns a non-nil error.
		yieldErrors := make([]error, 0, 4)
		associatedActiveExecution.RangeRoles(func(i int32, proposal scheduling.Proposal) bool {
			yieldError := fmt.Errorf("replica %d proposed \"YIELD\" because: %s", i, proposal.GetReason())
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

	return m.handleSmrLeadTaskMessage(kernelReplica, msg)
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

		if m.notificationCallback != nil {
			go m.notificationCallback("Cannot Find Active Execution", errorMessage, messaging.ErrorNotification)
		}

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

	activeExecution.SetActiveReplica(replica)
	m.lastPrimaryReplica = replica

	// We pass (as the second argument) the time at which the kernel replica began executing the code.
	m.processExecutionStartLatency(activeExecution, time.UnixMilli(leadMessage.UnixMilliseconds))

	// Record that the kernel has started training.
	if err := replica.KernelStartedTraining(); err != nil {
		m.log.Error("Failed to start training for kernel replica %s-%d: %v", m.Kernel.ID(),
			replica.ReplicaID(), err)

		if m.notificationCallback != nil {
			go m.notificationCallback(fmt.Sprintf("Failed to Start Training for Kernel \"%s\"",
				m.Kernel.ID()), err.Error(), messaging.ErrorNotification)
		}

		return err
	}

	m.log.Debug("Session \"%s\" has successfully started training on replica %m.",
		m.Kernel.ID(), replica.ReplicaID())

	return nil
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

	m.mu.Lock()
	defer m.mu.Unlock()

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

	if m.notificationCallback != nil {
		go m.notificationCallback(
			fmt.Sprintf("Inconsistent Primary Replicas for Completed Code Execution \"%s\" of Kernel \"%s\"",
				requestId, m.Kernel.ID()),
			fmt.Sprintf("Received 'execute_reply' from primary replica %d for execution \"%s\", "+
				"but we previously recorded that replica %d was the primary replica for this execution...",
				activeExecution.ActiveReplica.ReplicaID(), requestId, replica.ReplicaID()),
			messaging.ErrorNotification,
		)
	}

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

// ExecutionComplete should be called by the Kernel associated with the target ExecutionManager when an "execute_reply"
// message is received.
//
// ExecutionComplete returns nil on success.
func (m *ExecutionManager) ExecutionComplete(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (scheduling.Execution, error) {
	replica.ReceivedExecuteReply(msg)

	err := validateReply(msg)
	if err != nil {
		return nil, err
	}

	requestId := msg.JupyterParentMessageId()

	// Attempt to load the execution from the "active" map.
	activeExecution, loaded := m.activeExecutions[requestId]
	if !loaded {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, requestId)
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
	delete(m.activeExecutions, requestId)

	// Store the execution in the "finished" map.
	m.finishedExecutions[requestId] = activeExecution

	if m.statisticsProvider != nil && m.statisticsProvider.PrometheusMetricsEnabled() {
		m.statisticsProvider.IncrementNumTrainingEventsCompletedCounterVec()
	}

	if m.statisticsProvider != nil {
		m.statisticsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.CompletedTrainings += 1
			statistics.NumIdleSessions += 1
		})
	}

	if activeExecution.ActiveReplica == nil {
		m.log.Warn("Execution \"%s\" does not have its ActiveReplica specified, despite having just finished...")
		activeExecution.ActiveReplica = replica
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

	if activeExecution.ActiveReplica.ReplicaID() != replica.ReplicaID() {
		return m.handleInconsistentPrimaryReplicas(msg, replica, activeExecution)
	}

	return activeExecution, nil
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

// getActiveExecution returns a pointer to the Execution struct identified by the given message ID,
// or nil if no such Execution exists.
//
// getActiveExecution is NOT thread-safe. The thread-safe version is GetActiveExecution.
func (m *ExecutionManager) getActiveExecution(msgId string) scheduling.Execution {
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
