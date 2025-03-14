package kernel

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-viper/mapstructure/v2"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"golang.org/x/net/context"
	"reflect"
	"strconv"
	"time"
)

// ExecuteRequestHandler is responsible for forwarding "execute_request" messages to kernels.
type ExecuteRequestHandler struct {
	kernelManager    *Manager
	kernelProvider   *Provider
	cluster          scheduling.Cluster
	schedulingPolicy scheduling.Policy
	notifier         domain.Notifier // notifier is used to send notifications to the cluster dashboard.
	metricsProvider  metricsProvider // metricsProvider provides all metrics to the members of the scheduling package.
	debugMode        bool            // debugMode causes more information to be included in Jupyter requests, among other things.

	// responseForwarder is responsible for forwarding responses from scheduling.Kernel and scheduling.KernelReplica
	// instances back to the associated Jupyter client.
	responseForwarder ResponseForwarder

	// executeRequestForwarder forwards "execute_request" (or "yield_request") messages to Kernels one-at-a-time.
	executeRequestForwarder *client.ExecuteRequestForwarder[[]*messaging.JupyterMessage]

	// submitExecuteRequestsOneAtATime indicates whether the client.ExecuteRequestForwarder should be used to submit
	// execute requests, which forces requests to be submitted one-at-a-time.
	submitExecuteRequestsOneAtATime bool

	log logger.Logger
}

func NewExecuteRequestHandler(kernelManager *Manager, kernelProvider *Provider, cluster scheduling.Cluster, notifier domain.Notifier,
	metricsProvider metricsProvider, responseForwarder ResponseForwarder, opts *domain.ClusterGatewayOptions) *ExecuteRequestHandler {

	handler := &ExecuteRequestHandler{
		kernelManager:                   kernelManager,
		kernelProvider:                  kernelProvider,
		cluster:                         cluster,
		schedulingPolicy:                cluster.Scheduler().Policy(),
		notifier:                        notifier,
		metricsProvider:                 metricsProvider,
		responseForwarder:               responseForwarder,
		debugMode:                       opts.DebugMode,
		submitExecuteRequestsOneAtATime: opts.SubmitExecuteRequestsOneAtATime,
	}

	config.InitLogger(&handler.log, handler)

	handler.executeRequestForwarder = client.NewExecuteRequestForwarder[[]*messaging.JupyterMessage](
		handler.notifier.NotifyDashboard, nil)

	return handler
}

// handleExecuteRequest is a specialized version of ShellHandler that is used explicitly/exclusively for
// "execute_request" messages. It first calls processExecuteRequest before forwarding the "execute_request"
// to the replicas (well, to the Local Schedulers first).
func (handler *ExecuteRequestHandler) handleExecuteRequest(kernel scheduling.Kernel, jMsg *messaging.JupyterMessage) error {
	// First, update the kernel's resource request (for replica-based policies) if there's an updated
	// resource request in the metadata of the "execute_request" message.
	//
	// We do this before even checking if the replicas are scheduled.
	// If they aren't scheduled, then it would be best for the spec to be up to date before we bother scheduling them.
	// And if they're already scheduled, then their specs will be updated.
	err := handler.processExecuteRequestMetadata(jMsg, kernel)
	if err != nil {
		jMsg.IsFailedExecuteRequest = true
		// We'll send an error message to the associated client here.
		go func() {
			sendErr := handler.responseForwarder.SendErrorResponse(kernel, jMsg, err, messaging.ShellMessage)
			if sendErr != nil {
				handler.log.Error("handleExecuteRequest: Failed to send error response for shell \"%s\" message \"%s\": %v",
					jMsg.JupyterMessageType(), jMsg.JupyterMessageId(), sendErr)
			}
		}()
		return err
	}

	// Now we check if the replicas are scheduled. For static and dynamic, they will be, as well as with reservation,
	// unless idle session reclamation is enabled.
	//
	// For FCFS, they will not already be scheduled. (I say "they", but for FCFS, there's just 1 replica.)
	_, replicasAlreadyScheduled, err := handler.kernelManager.ensureKernelReplicasAreScheduled(kernel, jMsg, messaging.ShellMessage)
	if err != nil {
		handler.log.Warn("handleExecuteRequest: Error encountered while ensuring replica container(s) of kernel %s are scheduled in order to handle shell \"%s\" message: %v",
			kernel.ID(), jMsg.JupyterMessageType(), err)

		// We'll send an error message to the associated client here.
		go func() {
			sendErr := handler.responseForwarder.SendErrorResponse(kernel, jMsg, err, messaging.ShellMessage)
			if sendErr != nil {
				handler.log.Error("handleExecuteRequest: Failed to send error response for shell \"%s\" message \"%s\": %v",
					jMsg.JupyterMessageType(), jMsg.JupyterMessageId(), sendErr)
			}
		}()
		return err
	}

	// For policies that create replicas on-demand each time code is submitted,
	// 'replicasAlreadyScheduled' will always be false.
	if !replicasAlreadyScheduled {
		handler.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.NumTimesKernelReplicaNotAvailableImmediately.Add(1)
		})
	}

	targetReplica, processingError := handler.processExecuteRequest(jMsg, kernel)
	if processingError != nil {
		// Send a response with the error as the content.
		_ = handler.responseForwarder.SendErrorResponse(kernel, jMsg, processingError, messaging.ShellMessage)
		return processingError
	}

	if targetReplica != nil {
		handler.log.Debug("handleExecuteRequest: Identified target replica %d of kernel '%s' to lead \"execute_request\" \"%s\"",
			targetReplica.ReplicaID(), targetReplica.ID(), jMsg.JupyterMessageId())
	}

	// Broadcast an "execute_request" to all eligible replicas and a "yield_request" to all ineligible replicas.
	err = handler.forwardExecuteRequest(jMsg, kernel, targetReplica)
	if err != nil {
		_ = handler.responseForwarder.SendErrorResponse(kernel, jMsg, err, messaging.ShellMessage)
	}

	return err // Will be nil on success.
}

// processExecuteRequest is an important step of the path of handling an "execute_request".
//
// processExecuteRequest handles pre-committing resources, migrating a replica if no replicas are viable, etc.
func (handler *ExecuteRequestHandler) processExecuteRequest(msg *messaging.JupyterMessage, kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	kernelId := kernel.ID()
	handler.log.Debug("Processing shell \"execute_request\" message targeting kernel %s: %s", kernelId, msg.StringFormatted())

	// Get the session associated with the kernel.
	session, ok := handler.cluster.GetSession(kernelId)
	if !ok {
		handler.log.Error("Could not find scheduling.Session associated with kernel \"%s\"...", kernelId)
		msg.IsFailedExecuteRequest = true
		return nil, fmt.Errorf("%w: kernelID=\"%s\"", ErrSessionNotFound, kernelId)
	}

	// Verify that the session isn't already training.
	if session.IsTraining() {
		handler.log.Debug("Session %s is already training.", session.ID())
		msg.IsFailedExecuteRequest = true
		return nil, fmt.Errorf("session \"%s\" is already training", kernel.ID())
	}

	// Transition the session to the "expecting to start training soon" state.
	err := session.SetExpectingTraining().Error()
	if err != nil {
		handler.notifier.NotifyDashboardOfError("Failed to Set Session to 'Expecting Training'", err.Error())
		msg.IsFailedExecuteRequest = true
		return nil, err
	}

	// Register the execution with the kernel.
	activeExecution, registrationError := kernel.RegisterActiveExecution(msg)
	if registrationError != nil {
		handler.log.Error("Failed to register new active execution \"%s\" targeting kernel \"%s\": %v",
			msg.JupyterMessageId(), kernel.ID(), registrationError)
		return nil, registrationError
	}

	if kernel.SupposedToYieldNextExecutionRequest() {
		return nil, nil
	}

	// Find a "ready" replica to handle this execution request.
	targetReplica, err := handler.selectTargetReplicaForExecuteRequest(msg, kernel)
	if err != nil {
		// If an error is returned, then we should return the error here so that we send an
		// error message back to the client.
		handler.log.Error("Error while searching for ready replica of kernel '%s': %v", kernel.ID(), err)
		msg.IsFailedExecuteRequest = true
		return nil, err
	}

	// If the target replica is non-nil at this point, then we can just return it.
	if targetReplica != nil {
		// If we're using a long-running, replica-based scheduling policy, then we'll
		// increment the metric about a kernel replica being available right away.
		if handler.schedulingPolicy.ContainerLifetime() == scheduling.LongRunning {
			handler.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
				statistics.NumTimesKernelReplicaAvailableImmediately.Add(1)
			})
		}

		// If the kernel has a valid (i.e., non-nil) "previous primary replica", then we'll update another statistic...
		if kernel.LastPrimaryReplica() != nil {
			handler.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
				// If we selected the same replica again, then update the corresponding metric.
				if kernel.LastPrimaryReplica().ReplicaID() == targetReplica.ReplicaID() {
					statistics.NumTimesPreviousPrimaryReplicaSelectedConsecutively.Add(1)
				} else {
					statistics.NumTimesPreviousPrimaryReplicaUnavailable.Add(1)
				}
			})
		}

		activeExecution.SetMigrationRequired(false) // Record that we didn't have to migrate a replica.

		// Determine how many replicas could have served the request (including the selected target replica).
		replicas := kernel.Replicas()
		numViableReplicas := 1
		resourceRequest := targetReplica.ResourceSpec()
		for idx, replica := range replicas {
			// If the replica is nil for some reason, then we just skip it.
			if replica == nil {
				handler.log.Warn("Replica #%d of kernel \"%s\" is nil...", idx+1, kernel.ID())
				continue
			}

			// We initialized numViableReplicas to 1, so we just skip the target replica.
			if replica.ReplicaID() == targetReplica.ReplicaID() {
				continue
			}

			// If the host is nil (which it shouldn't be), then we'll just skip it.
			host := replica.Host()
			if host == nil {
				handler.log.Warn("Host of replica #%d (idx=%d) of kernel \"%s\" is nil...",
					replica.ReplicaID(), idx, kernel.ID())
				continue
			}

			// If the host of the other replica (i.e., the non-target replica) could commit resources
			// to the replica, then we'll increment the numViableReplicas counter.
			if host.CanCommitResources(resourceRequest) {
				numViableReplicas += 1
			}
		}

		activeExecution.SetNumViableReplicas(numViableReplicas)

		return targetReplica, nil
	} else {
		activeExecution.SetMigrationRequired(true) // Record that we did (or will) have to migrate a replica.
		activeExecution.SetNumViableReplicas(0)
	}

	if handler.schedulingPolicy.ContainerLifetime() == scheduling.LongRunning {
		handler.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.NumTimesKernelReplicaNotAvailableImmediately.Add(1)
		})
	}

	// TODO: Update this code.
	// 		 Specifically, the dynamic policies should return a list of replicas to migrate.
	targetReplica, err = handler.tryPerformMigration(kernel, msg)
	if err != nil {
		return nil, err
	}

	handler.log.Debug("Returning eligible replica %d of kernel '%s' for \"execute_request\" message %s.",
		targetReplica.ReplicaID(), kernel.ID(), msg.JupyterMessageId())
	return targetReplica, nil
}

// tryPerformMigration attempts to migrate one of the replicas of the specified kernel during the handling of
// a code execution request. If successful, tryPerformMigration will return the ID of the migrated replica.
func (handler *ExecuteRequestHandler) tryPerformMigration(kernel scheduling.Kernel, msg *messaging.JupyterMessage) (scheduling.KernelReplica, error) {
	handler.log.Debug("All %d replicas of kernel \"%s\" are ineligible to execute code. Initiating migration.",
		len(kernel.Replicas()), kernel.ID())

	targetReplica, err := handler.cluster.Scheduler().SelectReplicaForMigration(kernel)
	if targetReplica == nil {
		return nil, fmt.Errorf("could not identify replica eligible for migration because: %w", err)
	}

	handler.log.Debug(utils.LightBlueStyle.Render("Preemptively migrating replica %d of kernel %s now."),
		targetReplica.ReplicaID(), kernel.ID())
	req := &proto.MigrationRequest{
		TargetReplica: &proto.ReplicaInfo{
			KernelId:     kernel.ID(),
			ReplicaId:    targetReplica.ReplicaID(),
			PersistentId: kernel.PersistentID(),
		},
		ForTraining:      true,
		CanCreateNewHost: true,
		TargetNodeId:     nil,
	}

	resp, migrationError := handler.kernelManager.MigrateKernelReplica(context.Background(), req)
	if migrationError != nil {
		handler.log.Warn("Failed to preemptively migrate replica %d of kernel \"%s\": %v",
			targetReplica.ReplicaID(), kernel.ID(), migrationError)
		msg.IsFailedExecuteRequest = true
		return nil, migrationError
	}

	handler.log.Debug("Successfully, preemptively migrated replica %d of kernel \"%s\" to host \"%s\"",
		targetReplica.ReplicaID(), kernel.ID(), resp.NewNodeId)

	return targetReplica, nil
}

// selectTargetReplicaForExecuteRequest selects a target scheduling.KernelReplica of the given scheduling.Kernel for
// the specified "execute_request" message.
//
// selectTargetReplicaForExecuteRequest checks if there is a target replica specified in the request's metadata. If so,
// then that replica is selected. If not, then selectTargetReplicaForExecuteRequest will invoke the scheduling.Scheduler
// and the configured scheduling.Policy to select a target scheduling.KernelReplica.
func (handler *ExecuteRequestHandler) selectTargetReplicaForExecuteRequest(msg *messaging.JupyterMessage, kernel scheduling.Kernel) (scheduling.KernelReplica, error) {
	metadata, err := msg.DecodeMetadata()
	if err != nil {
		handler.log.Error("Failed to decode metadata of \"%s\" request \"%s\": %v", msg.JupyterMessageType(),
			msg.JupyterParentMessageId(), err)

		return nil, err
	}

	// Check if a specific replica was explicitly specified.
	val, loaded := metadata[messaging.TargetReplicaArg]
	if !loaded {
		handler.log.Debug("Target replica unspecified for execution \"%s\" targeting kernel \"%s\".",
			msg.JupyterMessageId(), kernel.ID())
		return handler.cluster.Scheduler().FindReadyReplica(kernel, msg.JupyterMessageId())
	}

	var targetReplicaId int32
	switch val.(type) {
	case string:
		var targetReplicaIdAsInt int
		targetReplicaIdAsInt, err = strconv.Atoi(val.(string))

		if err != nil {
			handler.log.Error("Failed to convert string target replica ID \"%s\" to valid integer: %v", val, err)
			return nil, err
		} else {
			targetReplicaId = int32(targetReplicaIdAsInt)
		}
	case float32:
		targetReplicaId = int32(val.(float32))
	case float64:
		targetReplicaId = int32(val.(float64))
	case int:
		targetReplicaId = int32(val.(int))
	case int32:
		targetReplicaId = val.(int32)
	case int64:
		targetReplicaId = int32(val.(int64))
	default:
		errorMessage := fmt.Sprintf("Unknown or unexpected type of target replica ID found in metadata of \"%s\" request \"%s\": %v",
			msg.JupyterMessageId(), kernel.ID(), reflect.TypeOf(val).Name())
		handler.log.Error(errorMessage)
		handler.notifier.NotifyDashboardOfError("Failed to Extract Target Replica ID", errorMessage)
		panic(errorMessage)
	}

	// If the specified target replica is invalid (e.g., less than or equal to 0), then we'll just
	// use the scheduler/scheduling policy to select a target replica.
	//
	// Typically, a value of -1 is specified when no explicit target is indicated, so this is an
	// expected outcome.
	if targetReplicaId <= 0 {
		handler.log.Debug("Target replica unspecified for execution \"%s\" targeting kernel \"%s\".",
			msg.JupyterMessageId(), kernel.ID())
		return handler.cluster.Scheduler().FindReadyReplica(kernel, msg.JupyterMessageId())
	}

	handler.log.Debug("Target replica specified as replica %d for execution \"%s\" targeting kernel \"%s\".",
		targetReplicaId, msg.JupyterMessageId(), kernel.ID())

	// TODO: Could there be a race here where we migrate the new replica right after scheduling it, such as
	// 		 while using dynamic scheduling? (Yes, almost certainly.)
	targetReplica, err := kernel.GetReplicaByID(targetReplicaId)
	if err != nil {
		handler.log.Error("Failed to get replica %d of kernel \"%s\": %v", targetReplicaId, kernel.ID(), err)
		return nil, err
	}

	// Reserve resources for the target kernel if resources are not already reserved.
	if !targetReplica.Host().HasResourcesCommittedToKernel(kernel.ID()) {
		handler.log.Debug("Specified target replica %d of kernel \"%s\" does not have resources committed to it on host %s yet. Pre-committing resources now.",
			targetReplica.ReplicaID(), kernel.ID(), targetReplica.Host().GetNodeName())

		// Attempt to pre-commit resources on the specified replica, or return an error if we cannot do so.
		_, err = targetReplica.Host().PreCommitResources(targetReplica.Container(), msg.JupyterMessageId(), nil)
		if err != nil {
			handler.log.Error("Failed to reserve resources for replica %d of kernel \"%s\" for execution \"%s\": %v",
				targetReplica.ReplicaID(), kernel.ID(), msg.JupyterMessageId(), err)
			return nil, err
		}

		handler.log.Debug("Successfully pre-committed resources for explicitly-specified target replica %d of kernel \"%s\" on host %s.",
			targetReplica.ReplicaID(), kernel.ID(), targetReplica.Host().GetNodeName())
	}

	return targetReplica, nil
}

// updateTargetedExecuteRequestMetadata is used to embed the GPU device IDs in the metadata frame of the given
// "execute_request" message targeting the specified scheduling.KernelReplica.
//
// Warning: this modifies the given messaging.JupyterMessage.
func (handler *ExecuteRequestHandler) updateTargetedExecuteRequestMetadata(jMsg *messaging.JupyterMessage, targetReplica scheduling.KernelReplica) error {
	// Validate that the message is of the proper type.
	if jMsg.JupyterMessageType() != messaging.ShellExecuteRequest {
		return fmt.Errorf("%w: expected message of type \"%s\"; however, message \"%s\" targeting kernel \"%s\" is of type \"%s\"",
			client.ErrInvalidExecuteRegistrationMessage, messaging.ShellExecuteRequest, jMsg.JupyterMessageId(), targetReplica.ID(), jMsg.JupyterMessageType())
	}

	// Deserialize the message's metadata frame into a dictionary.
	var metadataDict map[string]interface{}
	if err := jMsg.JupyterFrames.DecodeMetadata(&metadataDict); err != nil {
		handler.log.Error("Failed to decode metadata frame of \"execute_request\" message \"%s\" targeting kernel \"%s\" with JSON: %v",
			jMsg.JupyterMessageId(), targetReplica.ID(), err)
		return err
	}

	// Get the GPU device IDs assigned to the target kernel replica.
	gpuDeviceIds, err := targetReplica.Host().GetGpuDeviceIdsAssignedToReplica(targetReplica.ReplicaID(), targetReplica.ID())
	if err != nil {
		handler.log.Error("Failed to retrieve GPU device IDs assigned to replica %d of kernel \"%s\" because: %v",
			targetReplica.ReplicaID(), targetReplica.ID(), err)

		return err
	}

	// Embed the GPU device IDs in the metadata dictionary, which we'll re-encode into the message's metadata frame.
	metadataDict[GpuDeviceIdsArg] = gpuDeviceIds
	metadataDict[messaging.TargetReplicaArg] = targetReplica.ReplicaID()

	// Re-encode the metadata frame. It will have the number of idle GPUs available,
	// as well as the reason that the request was yielded (if it was yielded).
	err = jMsg.EncodeMetadata(metadataDict)
	if err != nil {
		handler.log.Error("Failed to encode metadata frame because: %v", err)
		handler.notifier.NotifyDashboardOfError("Failed to Encode Metadata Frame", err.Error())
		panic(err)
	}

	// Regenerate the signature.
	_, err = jMsg.JupyterFrames.Sign(targetReplica.ConnectionInfo().SignatureScheme, []byte(targetReplica.ConnectionInfo().Key))
	if err != nil {
		message := fmt.Sprintf("Failed to sign updated JupyterFrames for \"%s\" message because: %v",
			jMsg.JupyterMessageType(), err)
		handler.notifier.NotifyDashboardOfError("Failed to Sign JupyterFrames", message)
		panic(err)
	}

	// Validate the updated message/frames.
	verified := messaging.ValidateFrames([]byte(targetReplica.ConnectionInfo().Key),
		targetReplica.ConnectionInfo().SignatureScheme, jMsg.JupyterFrames)
	if !verified {
		handler.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'",
			targetReplica.ConnectionInfo().SignatureScheme, targetReplica.ConnectionInfo().Key)
		handler.log.Error("This message will likely be rejected by the kernel:\n%v", jMsg.StringFormatted())
	}

	return nil
}

// processExecuteRequestMetadata processes the metadata frame of an "execute_request" message.
// The main thing we do here is possibly update the resource request of the associated kernel.
func (handler *ExecuteRequestHandler) processExecuteRequestMetadata(msg *messaging.JupyterMessage, kernel scheduling.Kernel) error {
	// If there is nothing in the message's metadata frame, then we just return immediately.
	if len(*msg.JupyterFrames.MetadataFrame()) == 0 {
		return nil
	}

	metadataDict, err := msg.DecodeMetadata()
	if err != nil {
		handler.log.Error("processExecuteRequestMetadata: Failed to decode metadata frame of \"execute_request\" message \"%s\" with JSON: %v",
			msg.JupyterMessageId(), err)
		return err
	}

	var requestMetadata *messaging.ExecuteRequestMetadata
	if err := mapstructure.Decode(metadataDict, &requestMetadata); err != nil {
		handler.log.Error("processExecuteRequestMetadata: Failed to parse decoded metadata frame of \"execute_request\" message \"%s\" with mapstructure: %v",
			msg.JupyterMessageId(), err)
		return err
	}

	handler.log.Debug("processExecuteRequestMetadata: Decoded metadata of \"execute_request\" message \"%s\": %s",
		msg.JupyterMessageId(), requestMetadata.String())

	// If there is no resource request embedded in the request metadata, then we can just return at this point.
	if requestMetadata.ResourceRequest == nil {
		return nil
	}

	// Are we permitted to dynamically change the resource request(s) of kernels? If not, then we'll just return.
	if !handler.schedulingPolicy.SupportsDynamicResourceAdjustments() {
		return nil
	}

	// If there is a resource request in the metadata, but it is equal to the kernel's current resources,
	// then we can just return.
	specsAreEqual, firstUnequalField := kernel.ResourceSpec().EqualsWithField(requestMetadata.ResourceRequest)
	if specsAreEqual {
		handler.log.Debug("processExecuteRequestMetadata: Current spec [%v] and new spec [%v] for kernel \"%s\" are equal. No need to update.",
			requestMetadata.ResourceRequest.String(), kernel.ResourceSpec().String(), kernel.ID())
		return nil
	}

	handler.log.Debug("processExecuteRequestMetadata: Found new resource request for kernel \"%s\" in \"execute_request\" message \"%s\". "+
		"Old spec: %v. New spec: %v. Differ in field '%v' [old=%f, new=%f].",
		kernel.ID(), msg.JupyterMessageId(), kernel.ResourceSpec().String(), requestMetadata.ResourceRequest.String(),
		firstUnequalField, kernel.ResourceSpec().GetResourceQuantity(firstUnequalField),
		requestMetadata.ResourceRequest.GetResourceQuantity(firstUnequalField))

	err = handler.updateKernelResourceSpec(kernel, requestMetadata.ResourceRequest)
	if err != nil {
		handler.log.Warn("processExecuteRequestMetadata: Failed to update resource spec of kernel \"%s\": %v",
			kernel.ID(), err)
		return err
	}

	return nil
}

// forwardExecuteRequest forwards the given "execute_request" message to all eligible replicas of the
// specified kernel and a converted "yield_request" to all ineligible replicas of the kernel.
func (handler *ExecuteRequestHandler) forwardExecuteRequest(originalJupyterMessage *messaging.JupyterMessage, kernel scheduling.Kernel,
	targetReplica scheduling.KernelReplica) error {

	targetReplicaId := int32(-1)
	if targetReplica != nil {
		targetReplicaId = targetReplica.ReplicaID()
	}

	replicas := kernel.Replicas()

	originalJupyterMessage.AddDestFrameIfNecessary(kernel.ID())

	jupyterMessages := make([]*messaging.JupyterMessage, kernel.Size())
	for _, replica := range replicas {
		// TODO: If we make it so we can toggle on/off the gateway-assisted replica selection,
		// 		 then we need to update the logic here to only convert a message to a yield request
		//		 if gateway-assisted replica selection is enabled and the target replica is nil.
		// 		 This is because if gateway-assisted replica selection is disabled, then target replica
		//		 will be nil, but that'll be okay -- with gateway-assisted replica selection disabled,
		//		 the target replica is supposed to be nil.
		if replica.ReplicaID() == targetReplicaId {
			jupyterMessage := originalJupyterMessage.Clone()

			err := handler.updateTargetedExecuteRequestMetadata(jupyterMessage, replica)
			if err != nil {
				handler.log.Error("Failed to embed GPU device IDs in \"%s\" message \"%s\" targeting replica %d of kernel \"%s\": %v",
					jupyterMessage.JupyterMessageType(), jupyterMessage.JupyterMessageId(), replica.ReplicaID(), kernel.ID(), err)
				return err
			}

			jupyterMessages[replica.ReplicaID()-1] = jupyterMessage
			continue
		}

		// Convert the "execute_request" message to a "yield_request" message.
		// The returned message is initially created as a clone of the target message.
		jupyterMessage, err := originalJupyterMessage.CreateAndReturnYieldRequestMessage(targetReplicaId)
		if err != nil {
			handler.log.Error("Failed to convert \"execute_request\" message \"%s\" to a \"yield_request\" message: %v",
				originalJupyterMessage.JupyterMessageId(), err)

			handler.log.Error("Original \"execute_request\" message that we failed to convert: %v", originalJupyterMessage)

			handler.notifier.NotifyDashboard("Failed to Convert Message of Type \"execute_request\" to a \"yield_request\" Message",
				err.Error(), messaging.ErrorNotification)

			originalJupyterMessage.IsFailedExecuteRequest = true

			// We'll send an error message to the associated client here.
			_ = handler.responseForwarder.SendErrorResponse(kernel, originalJupyterMessage, err, messaging.ShellMessage)

			return err
		}

		handler.log.Debug("Converted \"execute_request\" \"%s\" to a \"yield_request\" message for replica %d of kernel \"%s\" [targetReplicaId=%d]: %v",
			originalJupyterMessage.JupyterMessageId(), replica.ReplicaID(), replica.ID(), targetReplicaId, jupyterMessage.JupyterFrames.StringFormatted())

		// We subtract 1 because replica IDs start at 1.
		jupyterMessages[replica.ReplicaID()-1] = jupyterMessage
	}

	var numExecRequests, numYieldRequests int
	for idx, msg := range jupyterMessages {
		handler.log.Debug("Execution request \"%s\" targeting replica %d of kernel \"%s\" is a(n) \"%s\" message.",
			msg.JupyterMessageId(), idx+1, kernel.ID(), msg.JupyterMessageType())

		if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
			numExecRequests += 1
		} else {
			numYieldRequests += 1
		}
	}

	if kernel.SupposedToYieldNextExecutionRequest() {
		// Sanity check.
		if numExecRequests > 0 {
			handler.log.Error("Kernel \"%s\" is supposed to yield/fail its next execution, but only %d/%d replica-specific messages have type \"%s\"...",
				kernel.ID(), numYieldRequests, numExecRequests+numYieldRequests, messaging.ShellYieldRequest)

			return fmt.Errorf("ClusterGatewayImpl::forwardExecuteRequest: kernel \"%s\" should be yielding next execution, but it's not")
		}

		// Record that we yielded the request.
		kernel.YieldedNextExecutionRequest()
	}

	if handler.submitExecuteRequestsOneAtATime {
		return handler.forwardExecuteRequestOneAtATime(jupyterMessages, kernel)
	}

	// We're not using the request forwarder, apparently. So, we'll call RequestWithHandlerAndReplicas ourselves.
	// We call RequestWithHandlerAndReplicas instead of RequestWithHandler because RequestWithHandler essentially does
	// what we just did up above before calling RequestWithHandlerAndReplicas; however, RequestWithHandler assumes that
	// all replicas are to receive an identical message.
	//
	// That's obviously not what we want to happen here, and so we manually created the different messages for the
	// different replicas ourselves.
	return kernel.RequestWithHandlerAndReplicas(context.Background(), "Forwarding", messaging.ShellMessage, jupyterMessages,
		handler.kernelReplicaResponseForwarder, nil, replicas...)
}

func (handler *ExecuteRequestHandler) kernelReplicaResponseForwarder(info scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	return handler.responseForwarder.ForwardResponse(info, typ, msg)
}

func (handler *ExecuteRequestHandler) forwardExecuteRequestOneAtATime(jupyterMessages []*messaging.JupyterMessage, kernel scheduling.Kernel) error {
	jupyterMessageId := jupyterMessages[0].JupyterMessageId()

	handler.log.Debug("Enqueuing \"execute_request\" \"%s\" targeting kernel \"%s\" with \"execute_request\" forwarder.",
		jupyterMessageId, kernel.ID())

	resultChan, closeFlag, err := handler.executeRequestForwarder.EnqueueRequest(jupyterMessages, kernel, jupyterMessageId)

	if err != nil {
		handler.log.Error("Failed to enqueue \"%s\" message(s) \"%s\" targeting kernel \"%s\": %v",
			messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID(), err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*8)
	defer cancel()

	handleRes := func(res interface{}) error {
		// Return the result as an error or nil if there was no error.
		switch res.(type) {
		case error:
			return res.(error)
		default:
			return nil
		}
	}

	// Wait for the result.
	// We need to wait for the result, or the execute forwarder (for this particular kernel) will block.
	select {
	case res := <-resultChan:
		{
			return handleRes(res)
		}
	case <-ctx.Done():
		{
			ctxErr := ctx.Err()
			handler.log.Error("Timed-out waiting for response to \"%s\" message \"%s\" targeting kernel \"%s\": %v.",
				messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID(), ctxErr)

			if kernel.IsTraining() {
				handler.log.Warn("Kernel \"%s\" is still training, supposedly.", kernel.ID())
			} else {
				handler.log.Error("Kernel \"%s\" is not even training.", kernel.ID())

				// TODO: Release resources from that replica.
				// TODO: Fix this
				activeExecution := kernel.GetExecutionManager().GetActiveExecution(jupyterMessageId)

				if activeExecution != nil {
					targetReplicaId := activeExecution.GetTargetReplicaId()
					replica, _ := kernel.GetReplicaByID(targetReplicaId)

					if replica != nil {
						host := replica.Host()

						if host != nil {
							releaseResErr := host.ForceReleaseResources(replica.Container(), jupyterMessageId)

							if releaseResErr != nil {
								handler.log.Error("Failed to forcibly release resources for replcia %d of kernel %s from host %s: %v",
									replica.ReplicaID(), replica.ID(), host.GetNodeName(), releaseResErr)
							}
						}
					}
				}
			}

			// Record that we're giving up, so if a result comes later, the execute request forwarder won't get
			// stuck trying to send it over the channel when we're never going to be around to receive it.
			if !closeFlag.CompareAndSwap(0, 1) {
				handler.log.Warn("Failed to flip ClosedFlag. There should be a result available now for \"%s\" message \"%s\" targeting kernel \"%s\".",
					messaging.ShellExecuteRequest, jupyterMessageId, kernel.ID())

				res := <-resultChan
				return handleRes(res)
			}

			return ctxErr
		}
	}
}

// updateKernelResourceSpec attempts to update the resource spec of the specified kernel.
//
// updateKernelResourceSpec will return nil on success. updateKernelResourceSpec will return an error if the kernel
// presently has resources committed to it, and the adjustment cannot occur due to resource contention.
func (handler *ExecuteRequestHandler) updateKernelResourceSpec(kernel scheduling.Kernel, newSpec types.CloneableSpec) error {
	if !handler.schedulingPolicy.SupportsDynamicResourceAdjustments() {
		handler.log.Debug("Cannot update resource spec of kernel \"%s\" as \"%s\" scheduling policy prohibits this.",
			kernel.ID(), handler.schedulingPolicy.Name())
		return nil
	}

	if newSpec.GPU() < 0 || newSpec.CPU() < 0 || newSpec.VRAM() < 0 || newSpec.MemoryMB() < 0 {
		handler.log.Error("Requested updated resource spec for kernel %s is invalid, as one or more quantities are negative: %s",
			kernel.ID(), newSpec.String())
		return fmt.Errorf("%w: %s", client.ErrInvalidResourceSpec, newSpec.String())
	}

	if newSpec.Equals(kernel.ResourceSpec()) {
		handler.log.Debug("Current spec [%v] and new spec [%v] for kernel \"%s\" are equal. No need to update.",
			newSpec.String(), kernel.ResourceSpec().String(), kernel.ID())
		return nil
	}

	handler.log.Debug("Attempting to update resource request for kernel %s from %s to %s.",
		kernel.ID(), kernel.ResourceSpec().String(), newSpec.String())

	return kernel.UpdateResourceSpec(newSpec)
}
