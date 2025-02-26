package kernel

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel/provisioner"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync/atomic"
	"time"
)

const (
	// initialMapSize is the initial size used when instantiating the map elements of the Manager struct.
	initialMapSize = 128

	forwarding = "Forwarding"
)

var (
	// gRPC Errors

	ErrEmptyKernelId  = errors.New("kernel ID is empty")
	ErrNotImplemented = status.Error(codes.Unimplemented, "not implemented in daemon")

	// Internal Errrors

	ErrKernelNotReady = status.Error(codes.Unavailable, "kernel not ready")
)

func errorf(err error) error {
	if err == nil {
		return nil
	}

	_, ok := status.FromError(err)
	if ok {
		return err
	}

	return status.Errorf(codes.Internal, err.Error())
}

func statusErrorf(status jupyter.KernelStatus, err error) (*proto.KernelStatus, error) {
	if err != nil {
		return nil, errorf(err)
	}
	return &proto.KernelStatus{Status: int32(status)}, nil
}

func getMessageAndSocketTypeForPing(in *proto.PingInstruction) (msgTyp string, socketTyp messaging.MessageType, err error) {
	if in.SocketType == "shell" {
		socketTyp = messaging.ShellMessage
		msgTyp = "ping_kernel_shell_request"
	} else if in.SocketType == "control" {
		socketTyp = messaging.ControlMessage
		msgTyp = "ping_kernel_ctrl_request"
	} else {
		err = fmt.Errorf("%w: \"%s\"", types.ErrInvalidSocketType, in.SocketType)
	}

	return
}

type MessageHandler func(kernel scheduling.Kernel, socketType messaging.MessageType, msg *messaging.JupyterMessage) error

// ResponseForwarder is an interface that provides the means to forward responses from scheduling.Kernel and
// scheduling.KernelReplica instances back to the associated Jupyter client.
type ResponseForwarder interface {
	// ForwardResponse forwards a response from a scheduling.Kernel / scheduling.KernelReplica
	// back to the Jupyter client.
	ForwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error
}

// Manager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
// scheduling.KernelReplica instances running within the cluster.
type Manager struct {
	id string

	log logger.Logger

	// debugMode causes more information to be included in Jupyter requests, among other things.
	debugMode bool

	// kernels is a mapping from kernel ID to scheduling.Kernel.
	// There may be duplicate values (i.e., multiple sessions mapping to the same kernel).
	kernels hashmap.HashMap[string, scheduling.Kernel]

	// kernelIdToKernel is a map from kernel ID to client.DistributedKernelClient.
	kernelIdToKernel hashmap.HashMap[string, scheduling.Kernel]

	// sessions is a mapping from Jupyter session ID to scheduling.Kernel.
	sessions hashmap.HashMap[string, scheduling.Kernel]

	handlers map[messaging.MessageType]MessageHandler

	// requestTracingEnabled controls whether we embed proto.RequestTrace structs within Jupyter requests and replies.
	requestTracingEnabled bool

	requestLog *metrics.RequestLog

	// metricsProvider provides all metrics to the members of the scheduling package.
	metricsProvider *metrics.ClusterMetricsProvider

	// responseForwarder is responsible for forwarding responses from scheduling.Kernel and scheduling.KernelReplica
	// instances back to the associated Jupyter client.
	responseForwarder ResponseForwarder

	// idleSessionReclaimer reclaims idle kernels.
	idleSessionReclaimer *IdleSessionReclaimer

	// kernelProvisioner handles the creation of kernels.
	kernelProvisioner *provisioner.Provisioner

	// executeRequestForwarder forwards "execute_request" (or "yield_request") messages to kernels one-at-a-time.
	executeRequestForwarder *client.ExecuteRequestForwarder[[]*messaging.JupyterMessage]

	// numActiveKernels is the number of actively-running kernels.
	numActiveKernels atomic.Int32

	cluster scheduling.Cluster

	// notifier is used to send notifications to the cluster dashboard.
	notifier domain.Notifier
}

// NewManager creates a new Manager struct and returns a pointer to it.
func NewManager(id string, cluster scheduling.Cluster, requestLog *metrics.RequestLog, responseForwarder ResponseForwarder,
	metricsProvider *metrics.ClusterMetricsProvider, networkProvider provisioner.NetworkProvider, notifier domain.Notifier,
	opts *domain.ClusterGatewayOptions) *Manager {

	manager := &Manager{
		id:                id,
		kernels:           hashmap.NewThreadsafeCornelkMap[string, scheduling.Kernel](initialMapSize),
		sessions:          hashmap.NewThreadsafeCornelkMap[string, scheduling.Kernel](initialMapSize),
		responseForwarder: responseForwarder,
		handlers:          make(map[messaging.MessageType]MessageHandler),
		cluster:           cluster,
		requestLog:        requestLog,
		debugMode:         opts.DebugMode,
		metricsProvider:   metricsProvider,
	}

	manager.handlers[messaging.ControlMessage] = manager.controlHandler
	manager.handlers[messaging.ShellMessage] = manager.shellHandler
	manager.handlers[messaging.StdinMessage] = manager.stdinHandler
	manager.handlers[messaging.HBMessage] = manager.heartbeatHandler

	kernelProvisioner := provisioner.NewProvisioner(id, cluster, notifier, metricsProvider, networkProvider,
		manager.shellHandlerWrapper, manager.CallbackProvider(), opts)

	manager.kernelProvisioner = kernelProvisioner

	if opts.IdleSessionReclamationEnabled && opts.IdleSessionReclamationIntervalSec > 0 {
		interval := time.Duration(opts.IdleSessionReclamationIntervalSec) * time.Second
		numReplicasPerKernel := manager.cluster.Scheduler().Policy().NumReplicas()

		manager.idleSessionReclaimer = NewIdleSessionReclaimer(manager.kernels, interval, numReplicasPerKernel, nil)

		manager.idleSessionReclaimer.Start()
	}

	manager.executeRequestForwarder = client.NewExecuteRequestForwarder[[]*messaging.JupyterMessage](
		manager.notifier.NotifyDashboard, nil)

	config.InitLogger(&manager.log, manager)

	return manager
}

// ForwardRequestToKernel forwards the given *messaging.JupyterMessage to the specified scheduling.Kernel using the
// specified socket type (i.e., messaging.MessageType).
func (km *Manager) ForwardRequestToKernel(kernelOrSessionId string, msg *messaging.JupyterMessage, socketTyp messaging.MessageType) error {
	// Validate argument.
	if msg == nil {
		panic("msg cannot be nil")
	}

	// Validate argument.
	if kernelOrSessionId == "" {
		km.log.Error("ForwardingError: kernel/session ID is empty [MsgId=\"%s\", MsgTyp=\"%s\", SocketType=\"%s\"]",
			msg.JupyterMessageId(), msg.JupyterMessageType(), socketTyp.String())

		return ErrEmptyKernelId
	}

	// Locate kernel.
	kernel, found := km.tryGetKernel(kernelOrSessionId)
	if kernel == nil || !found {
		km.log.Error("ForwardingError: kernel/session \"%s\" not found", kernelOrSessionId)

		return types.ErrKernelNotFound
	}

	handler, ok := km.handlers[socketTyp]
	if !ok {
		panic(fmt.Sprintf("Manager::ForwardRequestToKernel: unknown socket type '%d'", socketTyp))
	}

	return handler(kernel, socketTyp, msg)
}

func (km *Manager) StartKernel(ctx context.Context, in *proto.KernelSpec) (*proto.KernelConnectionInfo, error) {
	startTime := time.Now()

	// Try to find existing kernel by session id first.
	// The kernel that associated with the session id will not be clear during restart.
	existingKernel, _ := km.kernels.Load(in.Id)

	kernel, resp, err := km.kernelProvisioner.StartKernel(ctx, in, existingKernel)
	if err != nil {
		return nil, errorf(err)
	}

	km.kernelIdToKernel.Store(in.Id, kernel)
	km.kernels.Store(in.Id, kernel)

	// Make sure to associate the Jupyter Session with the kernel.
	kernel.BindSession(in.Session)
	km.kernels.Store(in.Session, kernel)

	// Map all the sessions to the kernel client.
	for _, sess := range kernel.Sessions() {
		km.log.Debug("Storing kernel %v under session ID \"%s\".", kernel, sess)
		km.kernels.Store(sess, kernel)
	}

	km.newKernelCreated(startTime, in.Id)

	km.registerKernelWithExecReqForwarder(kernel)

	return resp, nil
}

// GetKernelStatus returns the status of a particular kernel as specified in the proto.KernelId parameter.
func (km *Manager) GetKernelStatus(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := km.kernels.Load(in.Id)
	if !ok {
		km.log.Warn("GetKernelStatus: unknown kernel \"%s\". Returning KernelStatusExited (%v).",
			in.Id, jupyter.KernelStatusExited)

		return statusErrorf(jupyter.KernelStatusExited, nil)
	}

	kernelStatus := kernel.Status()
	return statusErrorf(kernelStatus, nil)
}

func (km *Manager) KillKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	km.log.Debug("↪ KillKernel[KernelId=%s]", in.Id)

	err := km.stopKernel(ctx, in)
	if err != nil {
		return proto.VOID, err
	}

	km.kernelStopped(in.Id)
	km.log.Debug(utils.DarkGreenStyle.Render("↩ KillKernel[KernelId=%s]"), in.Id)
	return proto.VOID, nil
}

func (km *Manager) StopKernel(ctx context.Context, in *proto.KernelId) (*proto.Void, error) {
	km.log.Debug("↪ StopKernel[KernelId=%s]", in.Id)

	err := km.stopKernel(ctx, in)
	if err != nil {
		return proto.VOID, errorf(err) // Ensure the error is gRPC-compatible.
	}

	km.kernelStopped(in.Id)
	km.log.Debug(utils.DarkGreenStyle.Render("↩ StopKernel[KernelId=%s]"), in.Id)
	return proto.VOID, nil
}

func (km *Manager) MigrateKernelReplica(ctx context.Context, in *proto.MigrationRequest) (*proto.MigrateKernelResponse, error) {
	startTime := time.Now()
	replicaInfo := in.TargetReplica
	targetNodeId := in.GetTargetNodeId()

	km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelMigrationStarted,
			KernelId:            replicaInfo.KernelId,
			ReplicaId:           replicaInfo.ReplicaId,
			Timestamp:           startTime,
			TimestampUnixMillis: startTime.UnixMilli(),
			Metadata:            map[string]interface{}{"target_node_id": targetNodeId},
		})
	})

	kernel, loaded := km.kernels.Load(replicaInfo.KernelId)
	if !loaded {
		km.log.Error("Could not find target of migration, kernel \"%s\"", replicaInfo.KernelId)
		go km.notifier.NotifyDashboardOfError("Cannot Migrate kernel.",
			fmt.Sprintf("Cannot find kernel with ID=%s.", replicaInfo.KernelId))
		return nil, fmt.Errorf("%w: kernel \"%s\"", types.ErrKernelNotFound, replicaInfo.KernelId)
	}

	kernelReplica, err := kernel.GetReplicaByID(replicaInfo.ReplicaId)
	if err != nil {
		km.log.Error("kernel %s does not have a replica with SMR node ID of %km. Cannot migrate.",
			replicaInfo.KernelId, replicaInfo.ReplicaId)
		go km.notifier.NotifyDashboardOfError("Cannot Migrate kernel.",
			fmt.Sprintf("kernel %s does not have a replica with ID=%km.",
				replicaInfo.KernelId, replicaInfo.ReplicaId))
		return nil, err
	}

	resp, reason, err := km.cluster.Scheduler().MigrateKernelReplica(ctx, kernelReplica, targetNodeId, in.ForTraining,
		in.CanCreateNewHost)

	duration := time.Since(startTime)
	if err != nil || reason != nil {
		targetNodeIdForLogging := "unspecified"
		if resp != nil && resp.NewNodeId != "" && resp.NewNodeId != " " {
			targetNodeIdForLogging = resp.NewNodeId
		} else if targetNodeId != "" {
			targetNodeIdForLogging = targetNodeId
		} else {
			targetNodeId = in.GetTargetNodeId()
		}

		if reason != nil { // simply couldn't migrate the container, presumably due to insufficient resources available
			km.log.Warn("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
				replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeIdForLogging, duration, reason.Error())
		} else { // actual error
			km.log.Error("Migration operation of replica %d of kernel %s to target node %s failed after %v because: %s",
				replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeIdForLogging, duration, err)
		}

		if km.metricsProvider.PrometheusMetricsEnabled() {
			km.metricsProvider.GetGatewayPrometheusManager().NumFailedMigrations.Inc()
		}

		km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.NumFailedMigrations += 1

			now := time.Now()
			statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.KernelMigrationComplete,
				KernelId:            replicaInfo.KernelId,
				ReplicaId:           replicaInfo.ReplicaId,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            duration,
				DurationMillis:      duration.Milliseconds(),
				Metadata:            map[string]interface{}{"target_node_id": targetNodeIdForLogging, "succeeded": "true"},
			})
		})

		if !errors.Is(reason, scheduling.ErrMigrationFailed) {
			reason = errors.Join(scheduling.ErrMigrationFailed, reason)
		}

		return nil, reason
	} else {
		km.log.Debug("Migration operation of replica %d of kernel %s to target node %s completed successfully after %v.",
			replicaInfo.ReplicaId, replicaInfo.KernelId, targetNodeId, duration)

		if km.metricsProvider.PrometheusMetricsEnabled() {
			km.metricsProvider.GetGatewayPrometheusManager().NumSuccessfulMigrations.Inc()
		}

		km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
			statistics.NumSuccessfulMigrations += 1

			now := time.Now()
			statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.KernelMigrationComplete,
				KernelId:            replicaInfo.KernelId,
				ReplicaId:           replicaInfo.ReplicaId,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            duration,
				DurationMillis:      duration.Milliseconds(),
				Metadata:            map[string]interface{}{"target_node_id": targetNodeId, "succeeded": "false"},
			})
		})
	}

	if km.metricsProvider.PrometheusMetricsEnabled() {
		km.metricsProvider.GetGatewayPrometheusManager().KernelMigrationLatencyHistogram.Observe(float64(duration.Milliseconds()))
	}

	// If there was an error, then err will be non-nil.
	// If there was no error, then err will be nil.
	return resp, err
}

func (km *Manager) NotifyKernelRegistered(ctx context.Context, in *proto.KernelRegistrationNotification) (*proto.KernelRegistrationNotificationResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	receivedAt := time.Now()
	kernelId := in.KernelId

	messageType, socketType, err := getMessageAndSocketTypeForPing(in)
	if err != nil {
		km.log.Error("PigKernel: %v", err)
		return &proto.Pong{Id: kernelId, Success: false, RequestTraces: nil}, errorf(types.ErrInvalidSocketType)
	}

	kernel, loaded := km.kernels.Load(kernelId)
	if !loaded {
		km.log.Error("Received 'ping-kernel' request for unknown kernel \"%s\"...", kernelId)
		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, types.ErrKernelNotFound
	}

	// If we're using, for example, batch FCFS scheduling, then the replicas may not be scheduled.
	// In this case, we'll just return a result directly.
	if !kernel.ReplicasAreScheduled() {
		if km.shouldReplicasBeRunning(kernel) {
			return nil, ErrKernelNotReady
		}

		return &proto.Pong{
			Id:            kernelId,
			Success:       true,
			RequestTraces: nil,
		}, nil
	}

	var (
		msgId = uuid.NewString()
		msg   zmq4.Msg
	)
	// frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.sessions()[0])
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messageType, kernel.ID())

	// If DebugMode is enabled, then add a buffers frame with a RequestTrace.
	var requestTrace *proto.RequestTrace
	if km.debugMode {
		frames.Frames = append(frames.Frames, make([]byte, 0))

		requestTrace = proto.NewRequestTrace(msgId, messageType, kernelId)

		// Then we'll populate the sort of metadata fields of the RequestTrace.
		requestTrace.RequestReceivedByGateway = receivedAt.UnixMilli()

		// Create the wrapper/frame itself.
		wrapper := &proto.JupyterRequestTraceFrame{RequestTrace: requestTrace}

		marshalledFrame, err := json.Marshal(&wrapper)
		if err != nil {
			km.log.Error("Failed to marshall RequestTraceWrapper when creating %s \"%s\" request \"%s\".",
				msgId, socketType.String(), messageType)
			return nil, errorf(err)
		}

		frames.Frames[messaging.JupyterFrameRequestTrace] = marshalledFrame
	}

	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		km.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernelId, kernel.ConnectionInfo().SignatureScheme, err)

		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, errorf(jupyter.ErrFailedToVerifyMessage)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	respChan := make(chan interface{}, km.numReplicasPerKernel())

	startTime := time.Now()
	var numRepliesReceived atomic.Int32
	responseHandler := func(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)

		// Notify that all replies have been received.
		if msg.RequestTrace != nil {
			requestTrace := msg.RequestTrace
			if requestTrace.ReplicaId != -1 && requestTrace.ReplicaId != from.ReplicaID() {
				km.log.Warn("Overwriting existing replica ID of %d with %d in RequestTrace for %s \"%s\" message %s (JupyterID=\"%s\")",
					requestTrace.ReplicaId, from.ReplicaID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
			}

			requestTrace.ReplicaId = from.ReplicaID()

			km.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v. Request trace: %s.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, kernel.Size(), time.Since(startTime),
				msg.RequestTrace.String())

			respChan <- requestTrace
		} else {
			km.log.Debug("Received %s ping_reply from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v.",
				typ.String(), from.String(), msgId, latestNumRepliesReceived, kernel.Size(), time.Since(startTime))

			respChan <- struct{}{}
		}

		return nil
	}

	jMsg := messaging.NewJupyterMessage(&msg)
	err = kernel.RequestWithHandler(ctx, "Forwarding", socketType, jMsg, responseHandler, nil)
	if err != nil {
		km.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v",
			socketType.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)

		return &proto.Pong{
			Id:            kernelId,
			Success:       false,
			RequestTraces: nil,
		}, err
	}

	if km.debugMode && km.requestLog != nil {
		err = km.requestLog.AddEntry(jMsg, socketType, requestTrace)
		if err != nil {
			km.log.Error("Failed to add entry to RequestLog for %s PingKernel: %v", socketType.String(), err)
		}
	}

	requestTraces := make([]*proto.RequestTrace, 0, km.numReplicasPerKernel())

	for numRepliesReceived.Load() < int32(km.numReplicasPerKernel()) {
		select {
		case <-ctx.Done():
			{
				ctxError := ctx.Err()
				var errorMessage string
				if ctxError != nil {
					errorMessage = fmt.Sprintf("%v 'ping-kernel' request for kernel %s failed after receiving %d/%d replies: %v",
						socketType.String(), kernelId, numRepliesReceived.Load(), kernel.Size(), ctxError)
				} else {
					errorMessage = fmt.Sprintf("%v 'ping-kernel' request for kernel %s timed-out after receiving %d/%d replies.",
						socketType.String(), kernelId, numRepliesReceived.Load(), kernel.Size())
				}
				km.log.Error(errorMessage)

				return &proto.Pong{
					Id:            kernelId,
					Success:       false,
					Msg:           errorMessage,
					RequestTraces: requestTraces,
				}, types.ErrRequestTimedOut
			}
		case v := <-respChan:
			{
				reqTrace, ok := v.(*proto.RequestTrace)
				if ok {
					requestTraces = append(requestTraces, reqTrace)
				}
			}
		}
	}

	// Include the "ReplySentByGateway" entry, since we're returning the response via gRPC,
	// and thus it won't be added automatically by the ZMQ-forwarder server.
	replySentByGateway := time.Now().UnixMilli()
	for _, reqTrace := range requestTraces {
		reqTrace.ReplySentByGateway = replySentByGateway
	}

	km.log.Debug("Received all 3 %v 'ping_reply' responses from replicas of kernel %s for ping message \"%s\" in %v.",
		socketType.String(), kernelId, msgId, time.Since(startTime))
	return &proto.Pong{
		Id:            kernelId,
		Success:       true,
		RequestTraces: requestTraces,
	}, nil
}

func (km *Manager) SmrReady(_ context.Context, in *proto.SmrReadyNotification) (*proto.Void, error) {
	err := km.kernelProvisioner.SmrReady(in)
	if err != nil {
		km.log.Error("SmrReady: %v", err)
		return proto.VOID, err
	}

	return proto.VOID, nil
}

// IsKernelActivelyTraining is used to query whether a particular kernel is actively training.
func (km *Manager) IsKernelActivelyTraining(_ context.Context, in *proto.KernelId) (*proto.IsKernelTrainingReply, error) {
	kernel, loaded := km.kernels.Load(in.Id)

	if !loaded {
		km.log.Warn("IsKernelActivelyTraining: queried training status of unknown kernel \"%s\"", in.Id)

		return nil, errorf(fmt.Errorf("%w: \"%s\"", types.ErrKernelNotFound, in.Id))
	}

	resp := &proto.IsKernelTrainingReply{
		KernelId:   in.Id,
		IsTraining: kernel.IsTraining(),
	}

	return resp, nil
}

// SmrNodeAdded is an RPC function called by the Local Daemon to the Cluster Gateway when the Local Daemon
// receives a "smr_node_added" IOPub message.
//
// NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called. We use SMR_READY instead.
func (km *Manager) SmrNodeAdded(_ context.Context, _ *proto.ReplicaInfo) (*proto.Void, error) {
	return nil, ErrNotImplemented
}

func (km *Manager) stopKernel(ctx context.Context, in *proto.KernelId) error {
	kernel, ok := km.kernels.Load(in.Id)
	if !ok {
		km.log.Error("Could not find kernel %s; cannot stop kernel.", in.GetId())
		return types.ErrKernelNotFound
	}

	restart := false
	if in.Restart != nil {
		restart = *in.Restart
	}
	km.log.Info("Stopping %v, restart=%v", kernel, restart)

	notifyChan := make(chan interface{}, 1)

	go func() {
		err := kernel.Shutdown(km.cluster.Placer().Reclaim, restart)
		if err != nil && errors.Is(err, client.ErrAlreadyShuttingDown) {
			notifyChan <- err
			return
		}

		if err != nil {
			km.log.Warn("Failed to close kernel: %s", err.Error())
			notifyChan <- err
			return
		}

		km.log.Debug("Clearing session records for kernel %s.", kernel)

		// Clear session records.
		for _, sess := range kernel.Sessions() {
			km.kernels.Delete(sess)
		}

		km.log.Debug("Cleaned sessions %v after replicas stopped.", kernel.Sessions())
		if restart {
			// Keep kernel records if restart is requested.
			kernel.ClearSessions()
		} else {
			km.kernels.Delete(kernel.ID())
			km.kernelIdToKernel.Delete(kernel.ID())

			km.log.Debug("Cleaned kernel %s after kernel stopped.", kernel.ID())
		}

		notifyChan <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		{
			km.log.Error("Context cancelled while deleting kernel \"%s\": %v", in.Id, ctx.Err())
			return ctx.Err()
		}
	case v := <-notifyChan:
		{
			err, isError := v.(error)
			if !isError {
				break // No error
			}

			if errors.Is(err, client.ErrAlreadyShuttingDown) {
				km.log.Warn("Kernel \"%s\" is already in the process of shutting down...", kernel.ID())
				return err
			}

			km.log.Error("Failed to stop kernel \"%s\": %v", in.Id, err)
			return err
		}
	}

	km.log.Debug("Finished deleting kernel %s.", kernel.ID())

	//if !restart && km.KubernetesMode() /* Only delete CloneSet if we're in Kubernetes mode */ {
	//	km.log.Debug("Deleting CloneSet of deleted kernel %s now.", kernel.ID())
	//
	//	// Delete the CloneSet.
	//	err := km.kubeClient.DeleteCloneset(kernel.ID())
	//
	//	if err != nil {
	//		km.log.Error("Error encountered while deleting k8s CloneSet for kernel %s: %v", kernel.ID(), err)
	//	} else {
	//		km.log.Debug("Successfully deleted k8s CloneSet of deleted kernel %s.", kernel.ID())
	//	}
	//}

	go km.notifier.NotifyDashboard(
		"kernel Stopped", fmt.Sprintf("kernel %s has been terminated successfully.",
			kernel.ID()), messaging.SuccessNotification)

	if km.metricsProvider.PrometheusMetricsEnabled() {
		km.metricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Sub(1)
	}

	stopped := km.executeRequestForwarder.UnregisterKernel(in.Id)
	if !stopped {
		km.log.Warn("Failed to stop 'execute_request' forwarder for kernel \"%s\"...", in.Id)
	}

	return nil
}

// kernelStopped removes the associated scheduling.Session and updates some statistics.
func (km *Manager) kernelStopped(kernelId string) {
	numActiveKernels := km.numActiveKernels.Add(-1)

	session := km.cluster.RemoveSession(kernelId)

	km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		statistics.NumStoppedSessions += 1

		if session != nil {
			lifetimeSeconds := time.Since(session.StartedAt()).Seconds()
			statistics.AggregateSessionLifetimeSec.Add(lifetimeSeconds)
			statistics.AggregateSessionLifetimesSec = append(statistics.AggregateSessionLifetimesSec, lifetimeSeconds)
		}

		now := time.Now()
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelStopped,
			KernelId:            kernelId,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})
	})

	if km.metricsProvider.PrometheusMetricsEnabled() {
		km.metricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))
	}
}

// ensureReplicasScheduled ensures that the scheduling.KernelReplica instances of the specified scheduling.Kernel are
// scheduled so that a message can be forwarded.
func (km *Manager) ensureReplicasScheduled(kernel scheduling.Kernel) error {
	panic("Implement me")
}

// forwardResponseFromKernel forwards the given messaging.JupyterMessage response from the given
// scheduling.KernelReplica to the Jupyter client.
func (km *Manager) forwardResponseFromKernel(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	km.updateRequestTraceReplicaId(from, msg)

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		// TODO: Implement this.
		// km.cleanUpBeforeForwardingExecuteReply(from, msg)

		panic("Implement me!")
	}

	return km.responseForwarder.ForwardResponse(from, typ, msg)
}

// updateRequestTraceReplicaId updates the ReplicaId field of the proto.RequestTrace embedded in the given *messaging.JupyterMessage.
func (km *Manager) updateRequestTraceReplicaId(from scheduling.KernelReplicaInfo, msg *messaging.JupyterMessage) {
	if !km.requestTracingEnabled {
		return
	}

	if msg.RequestTrace != nil {
		msg.RequestTrace.ReplicaId = from.ReplicaID()
	}
}

// tryGetKernel attempts to retrieve the scheduling.Kernel with the given kernel or Jupyter session ID.
//
// tryGetKernel searches by first treating the kernelOrSessionId parameter as a kernel ID before searching again
// while treating the kernelOrSessionId parameter as a Jupyter session ID (if the target scheduling.Kernel is not
// found the first time).
//
// If the target scheduling.Kernel is found, then it is returned, along with true.
//
// If the target scheduling.Kernel is not found, then nil is returned, along with false.
func (km *Manager) tryGetKernel(kernelOrSessionId string) (scheduling.Kernel, bool) {
	// First, search in the kernels mapping.
	kernel, ok := km.kernels.Load(kernelOrSessionId)
	if ok {
		return kernel, true
	}

	// Now try by searching the sessions mapping.
	return km.sessions.Load(kernelOrSessionId)
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) controlHandler(kernel scheduling.Kernel, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	if socketTyp != messaging.ControlMessage {
		panic(fmt.Sprintf("Manager::heartbeatHandler: invalid socket type '%d'", socketTyp))
	}

	km.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	panic("Implement me")
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) shellHandler(kernel scheduling.Kernel, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	if socketTyp != messaging.ShellMessage {
		panic(fmt.Sprintf("Manager::heartbeatHandler: invalid socket type '%d'", socketTyp))
	}

	km.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	panic("Implement me")
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) shellHandlerWrapper(kernel scheduling.KernelInfo, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	return km.shellHandler(kernel.(scheduling.Kernel), socketTyp, msg)
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) stdinHandler(kernel scheduling.Kernel, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	if socketTyp != messaging.StdinMessage {
		panic(fmt.Sprintf("Manager::heartbeatHandler: invalid socket type '%d'", socketTyp))
	}

	return kernel.RequestWithHandler(context.Background(), forwarding, socketTyp, msg, km.forwardResponseFromKernel, nil)
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) heartbeatHandler(kernel scheduling.Kernel, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	if socketTyp != messaging.HBMessage {
		panic(fmt.Sprintf("Manager::heartbeatHandler: invalid socket type '%d'", socketTyp))
	}

	return kernel.RequestWithHandler(context.Background(), forwarding, socketTyp, msg, km.forwardResponseFromKernel, nil)
}

// numReplicasPerKernel returns the number of replicas that each scheduling.Kernel is supposed to have.
func (km *Manager) numReplicasPerKernel() int {
	return km.cluster.Scheduler().Policy().NumReplicas()
}

// shouldReplicasBeRunning returns a flag indicating whether the containers of kernels should be running already.
//
// For scheduling policies in which the ContainerLifetime is scheduling.LongRunning, this is true.
func (km *Manager) shouldReplicasBeRunning(kernel scheduling.Kernel) bool {
	// If the kernel has been idle-reclaimed, then its replicas should not be running.
	if kernel.IsIdleReclaimed() {
		return false
	}

	// If the containers are in the process of being scheduled, then it's OK that they aren't running yet.
	//
	// If the caller is handling a message that can be spoofed, then it'll be spoofed.
	//
	// If the caller is handling something like an "execute_request", then the "execute_request" handler will end
	// up waiting for the container creation operation to complete.
	if kernel.ReplicaContainersAreBeingScheduled() {
		return false
	}

	return km.cluster.Scheduler().Policy().ContainerLifetime() == scheduling.LongRunning
}

// newKernelCreated is to be called from StartKernel if and when the procedure succeeds.
//
// newKernelCreated pushes some metrics to Kubernetes and sends a notification to the Dashboard.
func (km *Manager) newKernelCreated(startTime time.Time, kernelId string) {
	// Tell the Dashboard that the kernel has successfully started running.
	go km.notifier.NotifyDashboard("kernel Started",
		fmt.Sprintf("kernel %s has started running. Launch took approximately %v from when the cluster Gateway began processing the 'create kernel' request.",
			kernelId, time.Since(startTime)), messaging.SuccessNotification)

	numActiveKernels := km.numActiveKernels.Add(1)

	km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		statistics.NumIdleSessions += 1

		now := time.Now()
		statistics.ClusterEvents = append(statistics.ClusterEvents, &metrics.ClusterEvent{
			EventId:             uuid.NewString(),
			Name:                metrics.KernelCreationComplete,
			KernelId:            kernelId,
			ReplicaId:           -1,
			Timestamp:           now,
			TimestampUnixMillis: now.UnixMilli(),
		})
	})

	if km.metricsProvider.PrometheusMetricsEnabled() {
		km.metricsProvider.GetGatewayPrometheusManager().NumActiveKernelReplicasGaugeVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Set(float64(numActiveKernels))

		km.metricsProvider.GetGatewayPrometheusManager().TotalNumKernelsCounterVec.
			With(prometheus.Labels{"node_id": "cluster", "node_type": string(metrics.ClusterGateway)}).
			Inc()

		km.metricsProvider.GetGatewayPrometheusManager().KernelCreationLatencyHistogram.
			Observe(float64(time.Since(startTime).Milliseconds()))
	}
}

func (km *Manager) registerKernelWithExecReqForwarder(kernel scheduling.Kernel) {
	// Create a wrapper around the kernel's RequestWithHandlerAndReplicas method.
	forwarder := func(ctx context.Context, op string, typ messaging.MessageType, jupyterMessages []*messaging.JupyterMessage,
		handler scheduling.KernelReplicaMessageHandler, done func()) error {
		return kernel.RequestWithHandlerAndReplicas(ctx, op, typ, jupyterMessages, handler, done, kernel.Replicas()...)
	}

	km.executeRequestForwarder.RegisterKernel(kernel, forwarder, km.forwardResponseFromKernel)
}

func (km *Manager) CallbackProvider() scheduling.CallbackProvider {
	return km
}

func (km *Manager) ExecutionLatencyCallback(latency time.Duration, workloadId string, kernelId string) {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) ExecutionFailedCallback(c scheduling.Kernel, executeRequestMsg *messaging.JupyterMessage) error {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) NotificationCallback(title string, content string, notificationType messaging.NotificationType) {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) IncrementNumActiveExecutions() {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) DecrementNumActiveExecutions() {
	//TODO implement me
	panic("implement me")
}

func (km *Manager) NumActiveExecutions() int32 {
	//TODO implement me
	panic("implement me")
}
