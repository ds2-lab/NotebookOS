package execution_failed

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"golang.org/x/net/context"
	"math/rand"
	"sync"
)

const (
	ForceReprocessArg = "force_reprocess"
)

// ExecutionFailedCallback is a callback to handle a case where an execution failed because all replicas yielded.
type ExecutionFailedCallback func(c scheduling.Kernel, executeRequestMsg *messaging.JupyterMessage) (*messaging.JupyterMessage, error)

type Handler struct {
	// executionFailedCallback is the ClusterGatewayImpl's scheduling.ExecutionFailedCallback (i.e., recovery callback for panics).
	// The primary purpose is simply to send a notification to the dashboard that a panic occurred before exiting.
	// This makes error detection easier (i.e., it's immediately obvious when the system breaks as we're notified
	// visually of the panic in the cluster dashboard).
	executionFailedCallback ExecutionFailedCallback

	notifier domain.Notifier

	opts *domain.ClusterGatewayOptions

	log logger.Logger
}

func NewHandler(opts *domain.ClusterGatewayOptions, notifier domain.Notifier) *Handler {
	handler := &Handler{
		opts:     opts,
		notifier: notifier,
	}

	config.InitLogger(&handler.log, handler)

	return handler
}

func (h *Handler) HandleFailedExecution(c scheduling.Kernel, executeRequestMsg *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	return h.executionFailedCallback(c, executeRequestMsg)
}

// defaultFailureHandler is invoked when an "execute_request" cannot be processed when using the Default
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) defaultFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	h.log.Warn("There is no failure handler for the DEFAULT scheduling policy.")
	return nil, fmt.Errorf("there is no failure handler for the DEFAULT scheduling policy; cannot handle error")
}

// fcfsBatchSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the FCFS
// Batch scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) fcfsBatchSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	h.log.Warn("There is no failure handler for the FCFS Batch scheduling policy.")
	return nil, fmt.Errorf("there is no failure handler for the FCFS Batch policy; cannot handle error")
}

// middleGroundSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the FCFS
// Batch scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) middleGroundSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	h.log.Warn("There is no failure handler for the 'Middle Ground' scheduling policy.")
	return nil, fmt.Errorf("there is no failure handler for the 'Middle Ground policy; cannot handle error")
}

// reservationSchedulingFailureHandler is invoked when an "execute_request" cannot be processed when using the
// Reservation scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) reservationSchedulingFailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	h.log.Warn("There is no failure handler for the Reservation scheduling policy.")
	return nil, fmt.Errorf("there is no failure handler for the Reservation scheduling policy; cannot handle error")
}

// staticSchedulingFailureHandler is a callback to be invoked when all replicas of a
// kernel propose 'YIELD' while static scheduling is set as the configured scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) staticSchedulingFailureHandler(kernel scheduling.Kernel, executeRequestMsg *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	// Dynamically migrate one of the existing replicas to another node.
	//
	// Randomly select a replica to migrate.
	targetReplicaId := rand.Intn(kernel.Size()) + 1
	h.log.Debug(utils.LightBlueStyle.Render("Static Failure Handler: migrating replica %d of kernel %s now."),
		targetReplicaId, kernel.ID())

	// Notify the cluster dashboard that we're performing a migration.
	go h.notifier.NotifyDashboardOfInfo(fmt.Sprintf("All Replicas of kernel \"%s\" Have Proposed 'YIELD'", kernel.ID()),
		fmt.Sprintf("All replicas of kernel %s proposed 'YIELD' during code execution.", kernel.ID()))

	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    int32(targetReplicaId),
			PersistentId: kernel.PersistentID(),
		},
		ForTraining:  true,
		TargetNodeId: nil,
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	errorChan := make(chan error, 1)

	// Start the migration operation in another thread so that we can do some stuff while we wait.
	go func() {
		resp, err := h.MigrateKernelReplica(context.TODO(), req)

		if err != nil {
			h.log.Warn(utils.OrangeStyle.Render("Static Failure Handler: failed to migrate replica %d of kernel %s because: %s"),
				targetReplicaId, kernel.ID(), err.Error())

			var migrationError error

			if errors.Is(err, scheduling.ErrInsufficientHostsAvailable) {
				migrationError = errors.Join(scheduling.ErrMigrationFailed, err)
			} else {
				migrationError = err
			}

			errorChan <- migrationError
		} else {
			h.log.Debug(utils.GreenStyle.Render("Static Failure Handler: successfully migrated replica %d of kernel %s to host %s."),
				targetReplicaId, kernel.ID(), resp.Hostname)
		}

		waitGroup.Done()
	}()

	metadataDict, err := executeRequestMsg.DecodeMetadata()
	if err != nil {
		h.log.Warn("Failed to unmarshal metadata frame for \"execute_request\" message \"%s\" (JupyterID=\"%s\"): %v",
			executeRequestMsg.RequestId, executeRequestMsg.JupyterMessageId(), executeRequestMsg)

		// We'll assume the metadata frame was empty, and we'll create a new dictionary to use as the metadata frame.
		metadataDict = make(map[string]interface{})
	}

	// Specify the target replica.
	metadataDict[messaging.TargetReplicaArg] = targetReplicaId
	metadataDict[ForceReprocessArg] = true
	err = executeRequestMsg.EncodeMetadata(metadataDict)
	if err != nil {
		h.log.Error("Failed to encode metadata frame because: %v", err)
		return err
	}

	// If this is a "yield_request" message, then we need to convert it to an "execute_request" before resubmission.
	if executeRequestMsg.JupyterMessageType() == messaging.ShellYieldRequest {
		err = executeRequestMsg.SetMessageType(messaging.ShellExecuteRequest, true)
		if err != nil {
			h.log.Error("Failed to re-encode message header while converting \"yield_request\" message \"%s\" to \"execute_request\" before resubmitting it: %v",
				executeRequestMsg.JupyterMessageId(), err)
			return err
		}

		h.log.Debug("Successfully converted \"yield_request\" message \"%s\" to \"execute_request\" before resubmitting it.",
			executeRequestMsg.JupyterMessageId())
	}

	signatureScheme := kernel.ConnectionInfo().SignatureScheme
	if signatureScheme == "" {
		h.log.Warn("kernel %s's signature scheme is blank. Defaulting to \"%s\"", messaging.JupyterSignatureScheme)
		signatureScheme = messaging.JupyterSignatureScheme
	}

	// Regenerate the signature.
	if _, err := executeRequestMsg.JupyterFrames.Sign(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		// Ignore the error; just log it.
		h.log.Warn("Failed to sign frames because %v", err)
	}

	// Ensure that the frames are now correct.
	if err := executeRequestMsg.JupyterFrames.Verify(signatureScheme, []byte(kernel.ConnectionInfo().Key)); err != nil {
		h.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v': %v",
			signatureScheme, kernel.ConnectionInfo().Key, err)
		h.log.Error("This message will likely be rejected by the kernel:\n%v", executeRequestMsg)
		return jupyter.ErrFailedToVerifyMessage
	}

	// Now, we wait for the migration operation to proceed.
	waitGroup.Wait()
	select {
	case err := <-errorChan:
		{
			// If there was an error during execution, then we'll return that error rather than proceed.
			go h.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to Migrate Replica of kernel \"%s\"",
				kernel.ID()), err.Error())

			return err
		}
	default:
		{
			// Do nothing. The migration operation completed successfully.
		}
	}

	h.log.Debug(utils.LightBlueStyle.Render("Resubmitting 'execute_request' message targeting kernel %s now."), kernel.ID())
	err = h.ShellHandler(kernel, executeRequestMsg)

	if errors.Is(err, types.ErrKernelNotFound) {
		h.log.Error("ShellHandler couldn't identify kernel \"%s\"...", kernel.ID())

		h.kernels.Store(executeRequestMsg.DestinationId, kernel)
		h.kernels.Store(executeRequestMsg.JupyterSession(), kernel)

		kernel.BindSession(executeRequestMsg.JupyterSession())

		err = h.executeRequestHandler(kernel, executeRequestMsg)
	}

	if err != nil {
		h.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go h.notifier.NotifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

// dynamicV3FailureHandler is invoked when an "execute_request" cannot be processed when using the Dynamic v3
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) dynamicV3FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// dynamicV4FailureHandler is invoked when an "execute_request" cannot be processed when using the Dynamic v4
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) dynamicV4FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	panic("The 'DYNAMIC' scheduling policy is not yet supported.")
}

// gandivaV4FailureHandler is invoked when an "execute_request" cannot be processed when using the Gandiva
// scheduling policy.
//
// The first argument is the associated kernel, and the second is the original "execute_request" message that was
// submitted to the kernel -- NOT the "execute_reply" that may have been received.
func (h *Handler) gandivaV4FailureHandler(_ scheduling.Kernel, _ *messaging.JupyterMessage) (*messaging.JupyterMessage, error) {
	panic("The 'GANDIVA' scheduling policy is not yet supported.")
}
