package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-viper/mapstructure/v2"
	"github.com/google/uuid"
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
	if !messaging.IsExecuteOrYieldRequest(msg) {
		return fmt.Errorf("%w: message provided is of type \"%s\"",
			ErrInvalidExecuteRegistrationMessage, msg.JupyterMessageType())
	}

	return nil
}

// validateReply ensures that the given *messaging.JupyterMessage is an "execute_reply"
func validateReply(msg *messaging.JupyterMessage) error {
	if msg.JupyterMessageType() != messaging.ShellExecuteReply { // && msg.JupyterMessageType() != messaging.MessageTypeExecutionStatistics {
		return fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			ErrInvalidExecuteRegistrationMessage, messaging.ShellExecuteReply, msg.JupyterMessageType())
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
	lastPrimaryReplica   scheduling.KernelReplica
	lastPrimaryReplicaId int32

	// statisticsProvider exposes two functions: one for updating *statistics.ClusterStatistics and another
	// for updating Prometheus metrics.
	statisticsProvider scheduling.MetricsProvider

	// numTimesAsPrimaryReplicaMap keeps track of the number of times that each replica served as the primary
	// replica and successfully executed user-submitted code.
	numTimesAsPrimaryReplicaMap map[int32]int

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

	// NumReplicas is how many replicas the Kernel has.
	NumReplicas int

	mu sync.Mutex

	// nextExecutionIndex is the index of the next "execute_request" to be submitted.
	nextExecutionIndex atomic.Int32

	// executeReplyMessages maintains a mapping from jupyter message ID to the associated "execute_reply" message
	// that was sent when an execution completed.
	executeReplyMessages map[string]*messaging.JupyterMessage

	// smrLeadTaskMessages is a map from Jupyter msg ID for the associated "execute_request" to the "smr_lead_task"
	// message that was sent by the primary replica that handled the "execute_request".
	smrLeadTaskMessages map[string]*messaging.JupyterMessage

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

	// hasActiveTraining is true if the associated Kernel has an active training -- meaning that the Kernel has
	// submitted an "execute_request" and is still awaiting a response.
	//
	// Having an "active" training does not necessarily mean that the Kernel is running code right now.
	// It simply means that an execution has been submitted to the Kernel.
	//
	// Having an active training prevents a Kernel from being idle-reclaimed.
	hasActiveTraining atomic.Bool

	// callbackProvider provides a number of important callbacks.
	callbackProvider scheduling.CallbackProvider
}

// NewExecutionManager creates a new ExecutionManager struct to be associated with the given kernel.
//
// NewExecutionManager returns a pointer to the new ExecutionManager struct.
func NewExecutionManager(kernel scheduling.Kernel, numReplicas int, statsProvider scheduling.MetricsProvider,
	callbackProvider scheduling.CallbackProvider) *ExecutionManager {

	manager := &ExecutionManager{
		activeExecutions:             make(map[string]*Execution),
		finishedExecutions:           make(map[string]*Execution),
		erredExecutions:              make(map[string]*Execution),
		allExecutions:                make(map[string]*Execution),
		executionIndicesToExecutions: make(map[int32]*Execution),
		executionIndices:             make(map[string]int32),
		failedExecutions:             make(map[string]*Execution),
		smrLeadTaskMessages:          make(map[string]*messaging.JupyterMessage),
		executeReplyMessages:         make(map[string]*messaging.JupyterMessage),
		numTimesAsPrimaryReplicaMap:  make(map[int32]int),
		NumReplicas:                  numReplicas,
		Kernel:                       kernel,
		submittedExecutionIndex:      -1,
		activeExecutionIndex:         -1,
		completedExecutionIndex:      -1,
		callbackProvider:             callbackProvider,
		statisticsProvider:           statsProvider,
		lastPrimaryReplicaId:         -1,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

// GetSmrLeadTaskMessage returns the "smr_lead_task" IO pub message that was sent by the primary replica of the
// execution triggered by the "execute_request" message with the specified ID.
func (m *ExecutionManager) GetSmrLeadTaskMessage(executeRequestId string) (*messaging.JupyterMessage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, ok := m.smrLeadTaskMessages[executeRequestId]
	return msg, ok
}

// GetExecuteReplyMessage returns the "execute_reply" message that was sent in response to the "execute_request"
// message with the specified ID, if one exists. Specifically, it would be the "execute_reply" sent by the primary
// replica when it finished executing the user-submitted code.
func (m *ExecutionManager) GetExecuteReplyMessage(executeRequestId string) (*messaging.JupyterMessage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, ok := m.executeReplyMessages[executeRequestId]
	return msg, ok
}

// ExecutionIndexIsLarger returns true if the given executionIndex is larger than all 3 of the execution-index-related
// fields of the KernelReplicaClient, namely submittedExecutionIndex, activeExecutionIndex, and completedExecutionIndex.
func (m *ExecutionManager) ExecutionIndexIsLarger(executionIndex int32) bool {
	return executionIndex > m.submittedExecutionIndex && executionIndex > m.activeExecutionIndex && executionIndex > m.completedExecutionIndex
}

// NumExecutionsByReplica returns the number of times that the specified replica served as the primary replica
// and successfully executed user-submitted code.
func (m *ExecutionManager) NumExecutionsByReplica(replicaId int32) int {
	numTimes, loaded := m.numTimesAsPrimaryReplicaMap[replicaId]
	if !loaded {
		return 0
	}

	return numTimes
}

// SendingExecuteRequest records that an "execute_request" (or "yield_request") message is being sent.
//
// SendingExecuteRequest should be called RIGHT BEFORE the "execute_request" message is ACTUALLY sent.
func (m *ExecutionManager) SendingExecuteRequest(messages []*messaging.JupyterMessage) error {
	if len(messages) == 0 {
		m.log.Error("ExecutionManager::SendingExecuteRequest: messages slice is empty.")
		return ErrEmptyMessagesSlice
	}

	for _, msg := range messages {
		err := validateRequest(msg)
		if err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	requestId := messages[0].JupyterMessageId()

	executionIndex, loaded := m.executionIndices[requestId]
	if !loaded {
		return fmt.Errorf("%w: could not find execution index associated with request \"%s\"",
			ErrUnknownActiveExecution, requestId)
	}

	if executionIndex < m.submittedExecutionIndex {
		execution := m.executionIndicesToExecutions[m.submittedExecutionIndex]
		if execution == nil { // Sanity check.
			m.log.Error(
				utils.RedStyle.Render(
					"ExecutionManager::SendingExecuteRequest: Expected to find Execution with last-submitted index %d."),
				m.submittedExecutionIndex)

			err := fmt.Errorf("%w: submitted execution \"%s\" with index %d, and cannot find newer execution with index %d",
				ErrInvalidState, requestId, executionIndex, m.submittedExecutionIndex)

			m.sendNotification("Execution Manager in Invalid TransactionState",
				err.Error(), messaging.ErrorNotification, true)

			return err
		}

		m.log.Error(
			"ExecutionManager::SendingExecuteRequest: Submitting execute request \"%s\" with index=%d; however, last submitted execution had index=%d and ID=%s.",
			requestId, executionIndex, m.submittedExecutionIndex, execution.ExecuteRequestMessageId)

		err := fmt.Errorf("%w: submitting execute request \"%s\" with index=%d; however, last submitted execution had index=%d and ID=%s",
			ErrInconsistentExecutionIndices, requestId, executionIndex, m.submittedExecutionIndex, execution.ExecuteRequestMessageId)

		m.sendNotification("Inconsistent Execution Indices", err.Error(), messaging.ErrorNotification, true)

		return err
	}

	m.hasActiveTraining.Store(true)
	m.submittedExecutionIndex = executionIndex
	m.lastTrainingSubmittedAt = time.Now()

	if m.callbackProvider != nil {
		m.callbackProvider.IncrementNumActiveExecutions()
	}

	activeExecution := m.activeExecutions[requestId]
	if activeExecution == nil {
		return fmt.Errorf("%w: \"%s\"", ErrUnknownActiveExecution, requestId)
	}

	err := m.checkAndSetTargetReplica(messages, activeExecution)
	if err != nil {
		m.log.Error("ExecutionManager::SendingExecuteRequest: error while checking & setting target replica for execution \"%s\" (index=%d): %v",
			messages[0].JupyterMessageId(), activeExecution.GetExecutionIndex(), err)
		return err
	}

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
	if completedExecution, loaded := m.finishedExecutions[requestId]; loaded {
		m.log.Error("Attempt made to re-register completed execution \"%s\"", requestId)

		err = m.encodeExecutionIndexInExecuteRequest(msg, completedExecution.GetExecutionIndex())
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("%w: execution ID=\"%s\"", ErrDuplicateExecution, requestId)
	}

	existingExecution, loaded := m.failedExecutions[requestId]
	if loaded {
		nextExecutionAttempt := m.registerExecutionAttempt(msg, existingExecution)

		err = m.encodeExecutionIndexInExecuteRequest(msg, existingExecution.GetExecutionIndex())
		if err != nil {
			return nextExecutionAttempt, err
		}

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

	err = m.encodeExecutionIndexInExecuteRequest(msg, executionIndex)
	if err != nil {
		return newExecution, err
	}

	return newExecution, nil
}

// encodeExecutionIndexInExecuteRequest encodes the execution index that is or will be associated with the execution
// in the given, associated messaging.JupyterMessage.
func (m *ExecutionManager) encodeExecutionIndexInExecuteRequest(msg *messaging.JupyterMessage, index int32) error {
	metadata, err := msg.DecodeMetadata()
	if err != nil {
		m.log.Error("Failed to decode metadata of \"execute_request\" message \"%s\": %v",
			msg.JupyterMessageId(), err)
		return err
	}

	metadata["execution_index"] = index

	err = msg.EncodeMetadata(metadata)
	if err != nil {
		m.log.Error("Failed to re-encode metadata of \"execute_request\" message \"%s\" after adding execution index %d: %v",
			msg.JupyterMessageId(), index, err)
		return err
	}

	m.log.Debug("Successfully encoded execution index %d in \"execute_request\" message \"%s\" targeting kernel \"%s\".",
		index, msg.JupyterMessageId(), m.Kernel.ID())

	return nil
}

func (m *ExecutionManager) ExecutionFailedCallback() scheduling.ExecutionFailedCallback {
	if m.callbackProvider == nil {
		return nil
	}

	return m.callbackProvider.ExecutionFailedCallback
}

// LastPrimaryReplica returns the KernelReplica that served as the primary replica for the previous
// code execution, or nil if no code executions have occurred.
func (m *ExecutionManager) LastPrimaryReplica() scheduling.KernelReplica {
	return m.lastPrimaryReplica
}

// LastPrimaryReplicaId returns the SMR node ID of the KernelReplica that served as the primary replica for the
// previous code execution, or nil if no code executions have occurred.
//
// LastPrimaryReplicaId is preserved even if the last primary replica is removed/migrated.
func (m *ExecutionManager) LastPrimaryReplicaId() int32 {
	return m.lastPrimaryReplicaId
}

// YieldProposalReceived is called when we receive a YieldProposal from a replica of a kernel.
//
// YieldProposalReceived registers the YieldProposal with the kernel's associated Execution struct.
//
// If we find that we've received all three proposals, and they were ALL YieldProposal, then we'll invoke the
// "failure handler", which will  handle the situation according to the cluster's configured scheduling policy.
func (m *ExecutionManager) YieldProposalReceived(replica scheduling.KernelReplica,
	executeReplyMsg *messaging.JupyterMessage, msgErr *messaging.MessageErrorWithYieldReason) error {

	if err := validateReply(executeReplyMsg); err != nil {
		return err
	}

	replica.ReceivedExecuteReply(executeReplyMsg, true)

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

		var executeRequestMsg *messaging.JupyterMessage
		executeRequestMsg, err = m.getExecuteRequestForResubmission(executeReplyMsg)
		if err != nil {
			m.log.Error("Could not find original \"execute_request\" message for execution \"%s\" (index=%d).",
				targetExecuteRequestId, associatedActiveExecution.ExecutionIndex)

			title := fmt.Sprintf("Cannot Find \"execute_request\" Message \"%s\" Targeting Kernel \"%s\" for Resubmission",
				activeExecution.GetExecuteRequestMessageId(), m.Kernel.ID())
			content := fmt.Sprintf("Could not find original \"execute_request\" message for execution \"%s\" (index=%d).",
				targetExecuteRequestId, associatedActiveExecution.ExecutionIndex)
			m.sendNotification(
				title,
				content,
				messaging.ErrorNotification,
				true,
			)
		}

		// As a sort of sanity check, validate that there was no selected target replica, because if there was, then
		// the election should not have failed.
		targetReplicaId := activeExecution.GetTargetReplicaId()
		if targetReplicaId != -1 && targetReplicaId != m.activeExecutionIndex {
			m.log.Error("Target replica of execution \"%s\" was replica %d; however, all replicas proposed 'yield'.",
				activeExecution.GetExecuteRequestMessageId(), targetReplicaId, replica.ReplicaID())

			title := fmt.Sprintf("All Replicas Proposed 'YIELD' Despite Selecting Target Replica %d", targetReplicaId)
			content := fmt.Sprintf("Execution \"%s\" targeting kernel \"%s\" failed, even though Cluster Gateway selected target replica %d.",
				activeExecution.GetExecuteRequestMessageId(), m.Kernel.ID(), targetReplicaId)
			m.sendNotification(
				title,
				content,
				messaging.ErrorNotification,
				true,
			)
		}

		// Call the handler.
		//
		// If it returns an error, then we'll join that error with the YIELD errors, and return them all together.
		m.mu.Unlock()
		handlerError := m.callbackProvider.ExecutionFailedCallback(m.Kernel, executeRequestMsg)
		if handlerError != nil {
			allErrors := append([]error{handlerError}, yieldErrors...)
			return errors.Join(allErrors...)
		}

		return nil // Explicitly return here, so we can safely stick another m.placementMu.Unlock() call down below.
	}

	m.mu.Unlock()
	return nil
}

// HandleSmrLeadTaskMessage is to be called when a LeadProposal is issued by a replica of the associated Kernel.
func (m *ExecutionManager) HandleSmrLeadTaskMessage(msg *messaging.JupyterMessage, kernelReplica scheduling.KernelReplica) error {
	if msg.JupyterMessageType() != messaging.MessageTypeSMRLeadTask {
		return fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			ErrInvalidExecuteRegistrationMessage, messaging.MessageTypeSMRLeadTask, msg.JupyterMessageType())
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
	m.log.Debug("Received \"%s\" message with JupyterID=\"%s\" from replica %d of kernel %s.",
		msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), kernelId)

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

	var isYieldProposal bool
	if msgErr.Status != messaging.MessageStatusOK {
		isYieldProposal = (msgErr.ErrName == messaging.MessageErrYieldExecution) || msgErr.YieldReason != "" || msgErr.Yielded

		if msgErr.ErrName != messaging.MessageErrYieldExecution {
			m.log.Error("Received error in \"%s\" message \"%s\" from replica %d of kernel \"%s\": %s %s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), replica.ID(), msgErr.ErrName,
				msgErr.ErrValue)

			title := "Kernel Encountered Error During Code Execution"
			m.sendNotification(title, fmt.Sprintf("Received error in \"%s\" message \"%s\" from replica %d of kernel \"%s\": %s %s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), replica.ID(), msgErr.ErrName,
				msgErr.ErrValue), messaging.ErrorNotification, true)
		}
	}

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

func (m *ExecutionManager) HandleExecuteStatisticsMessage(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) error {
	if msg.JupyterMessageType() != messaging.MessageTypeExecutionStatistics {
		return /* false, */ fmt.Errorf("%w: expected message of type \"%s\", received message of type \"%s\"",
			ErrInvalidExecuteRegistrationMessage, messaging.MessageTypeExecutionStatistics, msg.JupyterMessageType())
	}

	kernelId := m.Kernel.ID()
	m.log.Debug("Received \"%s\" message with JupyterID=\"%s\" from replica %d of kernel %s.",
		msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), kernelId)

	// 0: <IDS|MSG>, 1: Signature, 2: Header, 3: ParentHeader, 4: Metadata, 5: Content[, 6: Buffers]
	if msg.JupyterFrames.LenWithoutIdentitiesFrame(true) < 5 {
		m.log.Error("Received invalid Jupyter message from replica %d of kernel %s (detected in extractShellError)",
			replica.ReplicaID(), kernelId)
		return /* false, */ messaging.ErrInvalidJupyterMessage
	}

	if len(*msg.JupyterFrames.ContentFrame()) == 0 {
		m.log.Warn("Received shell '%v' response with empty content.", msg.JupyterMessageType())
		return /* false, */ nil
	}

	//msgErr, isYieldProposal, decodeErr := m.checkIfMessageIsYield(msg, replica)
	//if decodeErr != nil {
	//	m.log.Error("Decoding failed while checking for yield message: %v", decodeErr)
	//	return false, decodeErr
	//}
	//
	//m.mu.Lock()
	//defer m.mu.Unlock()
	//
	//activeExec := m.getActiveExecution(msg.JupyterParentMessageId())
	//if activeExec != nil {
	//	err := activeExec.RegisterReply(replica.ReplicaID(), msg, true)
	//	if err != nil && !errors.Is(err, ErrReplyAlreadyRegistered) {
	//		m.log.Error("Failed to register \"execute_reply\" message: %v", err)
	//		return isYieldProposal, err
	//	}
	//}
	//
	//if isYieldProposal {
	//	err := m.YieldProposalReceived(replica, msg, msgErr)
	//	if err != nil {
	//		msg.IsFailedExecuteRequest = true
	//		return true, errors.Join(err, fmt.Errorf("%s: %s", msgErr.ErrName, msgErr.ErrValue))
	//	}
	//
	//	return true, messaging.ErrExecutionYielded
	//}
	//
	//_, err := m.unsafeExecutionComplete(msg, replica)

	// Update the ClusterStatistics using the data encoded in the "execute_statistics" message.
	//
	// We'll only update the parts of the ClusterStatistics that won't/wouldn't be updated
	// upon receiving the "execute_reply" sent for the same execution. We're also operating
	// under the assumption that we only receive "execute_statistics" IOPub messages while
	// using SMR-enabled, multiple-replica-based policies, such as "static" and "dynamic".
	m.statisticsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		// TODO: Implement me!
		panic("Implement me!")
	})

	return nil /* false, err */ // Will be nil if everything went OK in the call to ExecutionComplete
}

func (m *ExecutionManager) checkIfExecuteReplyMessageIsYield(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (*messaging.MessageErrorWithYieldReason, bool, error) {
	var msgErr *messaging.MessageErrorWithYieldReason
	if err := json.Unmarshal(*msg.JupyterFrames.ContentFrame(), &msgErr); err != nil {
		m.log.Error("Failed to unmarshal shell message received from replica %d of kernel %s because: %v",
			replica.ReplicaID(), replica.ID(), err)
		return nil, false, err
	}

	var isYieldProposal bool
	if msgErr.Status != messaging.MessageStatusOK {
		isYieldProposal = (msgErr.ErrName == messaging.MessageErrYieldExecution) || msgErr.YieldReason != "" || msgErr.Yielded

		if msgErr.ErrName != messaging.MessageErrYieldExecution {
			m.log.Error("Received error in \"%s\" message \"%s\" from replica %d of kernel \"%s\": %s %s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), replica.ID(), msgErr.ErrName,
				msgErr.ErrValue)

			title := "Kernel Encountered Error During Code Execution"
			m.sendNotification(title, fmt.Sprintf("Received error in \"%s\" message \"%s\" from replica %d of kernel \"%s\": %s %s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), replica.ReplicaID(), replica.ID(), msgErr.ErrName,
				msgErr.ErrValue), messaging.ErrorNotification, true)
		}
	}

	return msgErr, isYieldProposal, nil
}

func (m *ExecutionManager) checkIfMessageIsYield(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (*messaging.MessageErrorWithYieldReason, bool, error) {
	if msg.JupyterMessageType() == messaging.ShellExecuteReply {
		return m.checkIfExecuteReplyMessageIsYield(msg, replica)
	}

	var content map[string]interface{}
	err := msg.JupyterFrames.DecodeContent(&content)
	if err != nil {
		m.log.Error("Failed to decode content of \"%s\" message \"%s\": %v",
			msg.JupyterMessageType(), msg.JupyterMessageId(), err)

		return nil, false, err
	}

	var (
		msgErr          messaging.MessageErrorWithYieldReason
		isYieldProposal bool
		loaded          bool
		val             interface{}
	)

	val, loaded = content["status"]
	if !loaded {
		return nil, false, nil
	}
	msgErr.Status = val.(string)

	// If the "status" field is "ok", then it's not a YIELD message.
	if msgErr.Status == messaging.MessageStatusOK {
		return nil, false, nil
	}

	val, loaded = content["ename"]
	if !loaded {
		return nil, false, nil
	}
	msgErr.ErrName = val.(string)

	val, loaded = content["evalue"]
	if !loaded {
		return nil, false, nil
	}
	msgErr.ErrValue = val.(string)

	val, loaded = content["yield-reason"]
	if loaded {
		msgErr.YieldReason = val.(string)
	}

	val, loaded = content["yielded"]
	if loaded {
		msgErr.Yielded = val.(bool)
	}

	isYieldProposal = (msgErr.ErrName == messaging.MessageErrYieldExecution) || msgErr.YieldReason != "" || msgErr.Yielded
	return &msgErr, isYieldProposal, nil
}

// ExecutionComplete should be called by the Kernel associated with the target ExecutionManager when an "execute_reply"
// message is received.
//
// ExecutionComplete returns nil on success.
func (m *ExecutionManager) ExecutionComplete(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (scheduling.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.unsafeExecutionComplete(msg, replica)
}

func (m *ExecutionManager) unsafeExecutionComplete(msg *messaging.JupyterMessage, replica scheduling.KernelReplica) (scheduling.Execution, error) {
	receivedAt := time.Now()

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

	// If the execution is already marked as complete, then we'll log an error.
	// We can't just return because we shouldn't have found the active execution struct in the "activeExecutions"
	// map if it was already completed.
	if activeExecution.IsCompleted() {
		m.log.Error("Execution \"%s\" was already completed %v ago...",
			executeRequestId, time.Since(activeExecution.GetReceivedExecuteReplyAt()))
	}

	activeExecution.SetExecuted(receivedAt)

	// Update the execution's state.
	activeExecution.State = Completed

	// RemoveHost the execution from the "active" map.
	delete(m.activeExecutions, executeRequestId)

	// Store the execution in the "finished" map.
	m.finishedExecutions[executeRequestId] = activeExecution
	m.executeReplyMessages[executeRequestId] = msg

	if numTimes, loadedNumTimes := m.numTimesAsPrimaryReplicaMap[replica.ReplicaID()]; loadedNumTimes {
		m.numTimesAsPrimaryReplicaMap[replica.ReplicaID()] = numTimes + 1
	} else {
		m.numTimesAsPrimaryReplicaMap[replica.ReplicaID()] = 1
	}

	// If our statistics provider field is non-nil, then we'll update some statistics.
	if m.statisticsProvider != nil {
		// If prometheus metrics are enabled, then we'll also update some prometheus metrics.
		if m.statisticsProvider.PrometheusMetricsEnabled() {
			m.statisticsProvider.IncrementNumTrainingEventsCompletedCounterVec()
		}

		m.statisticsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.CompletedTrainings.Add(1)
			statistics.NumIdleSessions.Add(1)

			resourceRequest := replica.ResourceSpec()

			statistics.BusyCPUs.Sub(resourceRequest.CPU())
			statistics.BusyMemory.Sub(resourceRequest.MemoryMB())
			statistics.BusyGPUs.Sub(resourceRequest.GPU())
			statistics.BusyVRAM.Sub(resourceRequest.VRAM())
		})
	}

	// If the execution index is equal to the submitted execution index, then that means that the "execute_reply" that
	// we just received is in response to the most-recently-submitted "execute_request" message.
	if activeExecution.ExecutionIndex == m.submittedExecutionIndex {
		m.log.Debug("Received \"execute_reply\" for execution \"%s\" with index=%d matching last-submitted execution's index. 'Completed' execution index %d → %d.",
			executeRequestId, activeExecution.ExecutionIndex, m.completedExecutionIndex, activeExecution.ExecutionIndex)

		m.completedExecutionIndex = activeExecution.ExecutionIndex
		m.lastTrainingEndedAt = receivedAt

		// No longer waiting for "execute_reply" for most-recently-submitted "execute_request".
		m.hasActiveTraining.Store(false)

		if m.callbackProvider != nil {
			m.callbackProvider.DecrementNumActiveExecutions()
		}
	} else {
		// The "execute_reply" that we received is... for an old "execute_reply" message?
		// This generally shouldn't happen.
		m.log.Warn("Received \"execute_reply\" for execution \"%s\" with index=%d; however, latest submitted execution has index=%d.",
			executeRequestId, activeExecution.ExecutionIndex, m.submittedExecutionIndex)

		if activeExecution.ExecutionIndex > m.completedExecutionIndex {
			m.log.Warn("\"execute_reply\" for execution \"%s\" with index=%d is still greater than last completed index (%d). 'Completed' execution index %d → %d.",
				executeRequestId, activeExecution.ExecutionIndex, m.completedExecutionIndex, m.completedExecutionIndex, activeExecution.ExecutionIndex)

			m.completedExecutionIndex = activeExecution.ExecutionIndex
		}
	}

	// Similar to the above -- if the execution index of the "execute_reply" does not match the most up-to-date
	// execution index that the ExecutionManager is aware of, then something is wrong (potentially).
	//
	// In particular, if we're configured to send "execute_request" messages one-at-a-time, then this definitely
	// should not happen.
	//
	// TODO: Couldn't it just be the case that we receive the reply before the "smr lead task"?
	//		 This isn't a bad thing; in fact, it happens pretty regularly.
	if activeExecution.ExecutionIndex != m.activeExecutionIndex {
		m.log.Warn("\"execute_reply\" with index %d does not match last-known active index %d...",
			activeExecution.ExecutionIndex, m.activeExecutionIndex)
	}

	// As a sort of sanity check, validate that the target replica matches the primary replica, assuming that a
	// target replica was explicitly set for this execution.
	targetReplicaId := activeExecution.GetTargetReplicaId()
	if targetReplicaId != -1 && targetReplicaId != replica.ReplicaID() {
		m.log.Error("Target replica of execution \"%s\" was replica %d; however, replica %d was the primary.",
			activeExecution.GetExecuteRequestMessageId(), targetReplicaId, replica.ReplicaID())

		title := fmt.Sprintf("Unexpected Primary Replica for Execution \"%s\" Targeting Kernel \"%s\"",
			activeExecution.GetExecuteRequestMessageId(), m.Kernel.ID())
		content := fmt.Sprintf("Target replica: %d. Primary replica: %d.", targetReplicaId, replica.ReplicaID())
		m.sendNotification(
			title,
			content,
			messaging.ErrorNotification,
			true,
		)
	}

	if activeExecution.ActiveReplica == nil {
		m.log.Warn("ActiveReplica unspecified for exec \"%s\", despite having just finished...", executeRequestId)

		activeExecution.SetActiveReplica(replica)
		m.lastPrimaryReplica = replica
		m.lastPrimaryReplicaId = replica.ReplicaID()
	}

	if activeExecution.ActiveReplica.ReplicaID() != replica.ReplicaID() {
		return m.handleInconsistentPrimaryReplicas(msg, replica, activeExecution)
	}

	reason := "Received \"execute_reply\" message, indicating that the training has stopped."
	err = activeExecution.ActiveReplica.KernelStoppedTraining(reason, activeExecution)
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

// NumCompletedTrainings returns the number of training events that have been completed successfully.
func (m *ExecutionManager) NumCompletedTrainings() int {
	return len(m.finishedExecutions)
}

// ReplicaRemoved is used to notify the ExecutionManager that a particular KernelReplica has been removed.
// This allows the ExecutionManager to set the LastPrimaryReplica field to nil if the removed KernelReplica
// is the LastPrimaryReplica.
func (m *ExecutionManager) ReplicaRemoved(replica scheduling.KernelReplica) {
	if m.lastPrimaryReplica == replica {
		m.log.Debug("The previous primary replica of kernel \"%s\" has been removed.", m.Kernel.ID())
		m.lastPrimaryReplica = nil
	}
}

// handleSmrLeadTaskMessage is the critical section of HandleSmrLeadTaskMessage.
func (m *ExecutionManager) handleSmrLeadTaskMessage(replica scheduling.KernelReplica, msg *messaging.JupyterMessage) error {
	receivedAt := time.Now()

	// Decode the jupyter.MessageSMRLeadTask message.
	leadMessage, err := m.decodeLeadMessageContent(msg)
	if err != nil {
		return err
	}

	// The ID of the Jupyter "execute_request" message that initiated the associated training.
	executeRequestMsgId := leadMessage.ExecuteRequestMsgId

	m.mu.Lock()
	defer m.mu.Unlock()

	m.smrLeadTaskMessages[executeRequestMsgId] = msg

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

	err = activeExecution.ReceivedSmrLeadTaskMessage(replica, receivedAt)
	if err != nil {
		return err
	}

	var (
		samePrimaryReplicaAsLastTime bool
		isFirstTraining              = m.lastPrimaryReplica == nil
	)

	if m.lastPrimaryReplica != nil {
		samePrimaryReplicaAsLastTime = m.lastPrimaryReplica.ReplicaID() == replica.ReplicaID()
	}

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

	if m.statisticsProvider != nil {
		m.statisticsProvider.UpdateClusterStatistics(func(stats *metrics.ClusterStatistics) {
			stats.NumTrainingSessions.Add(1)

			resourceRequest := replica.ResourceSpec()

			stats.BusyCPUs.Add(resourceRequest.CPU())
			stats.BusyMemory.Add(resourceRequest.MemoryMB())
			stats.BusyGPUs.Add(resourceRequest.GPU())
			stats.BusyVRAM.Add(resourceRequest.VRAM())

			var (
				gpuDeviceIds     []int
				hostId, hostName string
			)

			// Get the name and ID of the host on which the active replica is running,
			// if that information is available right now.
			if activeExecution != nil {
				activeReplica := activeExecution.ActiveReplica
				if activeReplica != nil {
					host := activeReplica.Host()
					if host != nil {
						hostId = host.GetID()
						hostName = host.GetNodeName()
					}
				}

				gpuDeviceIds = activeExecution.GetGpuDeviceIDs()
			}

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.KernelTrainingStarted,
				KernelId:            m.Kernel.ID(),
				ReplicaId:           replica.ReplicaID(),
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Metadata: map[string]interface{}{
					"resource_request":                resourceRequest.ToMap(),
					"is_first_training":               isFirstTraining,
					"reused_previous_primary_replica": samePrimaryReplicaAsLastTime,
					"num_viable_replicas":             activeExecution.NumViableReplicas,
					"migration_required":              activeExecution.MigrationWasRequired,
					"gpu_device_ids":                  gpuDeviceIds,
					"host_id":                         hostId,
					"host_name":                       hostName,
				},
			})
		})
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

	// We'll attempt to call 'stop training' on both replicas in attempt to salvage things, but we're probably screwed.
	var err1, err2 error
	if activeExecution.ActiveReplica.IsTraining() {
		m.log.Warn("Calling KernelStoppedTraining on the replica recorded on the Execution struct for execution '%s'",
			requestId)

		err1 = activeExecution.ActiveReplica.KernelStoppedTraining(reason, activeExecution)
	}

	if replica.IsTraining() {
		m.log.Warn("Calling KernelStoppedTraining on the replica that sent the \"execute_reply\" message for execution '%s'",
			requestId)

		err2 = replica.KernelStoppedTraining(reason, activeExecution)
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
	} else if err2 != nil /* && err1 != nil */ /* condition is always true, so I commented it out */ {
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
			m.callbackProvider.ExecutionLatencyCallback(latency, activeExecution.GetWorkloadId(), m.Kernel.ID())
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
	if m.callbackProvider != nil && m.callbackProvider.NotificationCallback != nil {
		if useSeparateGoroutine {
			go m.callbackProvider.NotificationCallback(title, content, typ)
		} else {
			m.callbackProvider.NotificationCallback(title, content, typ)
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

// HasActiveTraining returns true if the target DistributedKernelClient has an active training -- meaning that the
// Kernel has submitted an "execute_request" and is still awaiting a response.
//
// Having an "active" training does not necessarily mean that the Kernel is running code right now.
// It simply means that an execution has been submitted to the Kernel.
//
// Having an active training prevents a Kernel from being idle-reclaimed.
func (m *ExecutionManager) HasActiveTraining() bool {
	return m.hasActiveTraining.Load()
}

// checkAndSetTargetReplica checks if exactly one of the messaging.JupyterMessage structs has type
// messaging.ShellExecuteRequest, as opposed to multiple having type messaging.ShellExecuteRequest or all having type
// messaging.ShellYieldRequest.
//
// If a single target replica is identified, then the ExecutionManager will register this with the associated Execution.
func (m *ExecutionManager) checkAndSetTargetReplica(messages []*messaging.JupyterMessage, activeExecution scheduling.Execution) error {
	if len(messages) == 0 {
		m.log.Error("ExecutionManager::SendingExecuteRequest: messages slice is empty.")
		return ErrEmptyMessagesSlice
	}

	if activeExecution == nil {
		m.log.Error("ExecutionManager::checkAndSetTargetReplica: scheduling.Execution argument is nil.")
		return fmt.Errorf("execution argument is nil")
	}

	execRequestIndex := -1
	for i, msg := range messages {
		if msg.JupyterMessageType() != messaging.ShellExecuteRequest {
			continue
		}

		// If the execution index is already set, then we've found two messages of type "execute_request".
		if execRequestIndex != -1 {
			m.log.Debug("Messages %d and %d are both of type \"%s\". No single target replica identified for execution \"%s\" (index=%d).",
				execRequestIndex, i, messaging.ShellExecuteRequest, messages[0].JupyterMessageId(), activeExecution.GetExecutionIndex())
			return nil
		}

		execRequestIndex = i
	}

	if execRequestIndex == -1 {
		m.log.Debug("No single target replica identified for execution \"%s\" (index=%d).",
			messages[0].JupyterMessageId(), activeExecution.GetExecutionIndex())
		return nil
	}

	targetReplicaId := int32(execRequestIndex + 1) // Replica IDs start at 1.
	m.log.Debug("Identified target replica for execution \"%s\" (index=%d): replica %d.",
		messages[0].JupyterMessageId(), activeExecution.GetExecutionIndex(), targetReplicaId)

	err := activeExecution.SetTargetReplica(targetReplicaId)
	if err != nil {
		m.log.Error("Failed to set target replica for active execution \"%s\": %v",
			activeExecution.GetExecuteRequestMessageId(), err)
		return err
	}

	// If we already have assigned the GPU device IDs, then we can just return.
	if activeExecution.GetGpuDeviceIDs() != nil && len(activeExecution.GetGpuDeviceIDs()) > 0 {
		return nil
	}

	execRequest := messages[execRequestIndex]

	var metadataDict map[string]interface{}
	err = execRequest.JupyterFrames.DecodeMetadata(&metadataDict)
	if err != nil {
		m.log.Error("Failed to decode the metadata dictionary of \"%s\" message \"%s\": %v",
			execRequest.JupyterMessageType(), execRequest.JupyterMessageId(), err)
		return err
	}

	// Attempt to decode it this way.
	var requestMetadata *messaging.ExecuteRequestMetadata
	err = mapstructure.Decode(metadataDict, &requestMetadata)
	if err == nil {
		activeExecution.SetGpuDeviceIDs(requestMetadata.GpuDeviceIds)
	} else {
		// Fallback if the mapstructure way is broken.
		m.log.Warn(utils.OrangeStyle.Render("[WARNING] Failed to decode request metadata via mapstructure: %v\n"), err)

		deviceIds, ok := metadataDict["gpu_device_ids"]
		if ok {
			activeExecution.SetGpuDeviceIDs(deviceIds.([]int))
		}
	}

	return nil
}

// IsExecutionComplete returns true if the execution associated with the given message ID is complete.
func (m *ExecutionManager) IsExecutionComplete(executeRequestId string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	exec, loaded := m.activeExecutions[executeRequestId]
	if !loaded {
		return false
	}

	return exec.IsCompleted()
}
