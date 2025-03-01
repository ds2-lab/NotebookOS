package kernel

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/google/uuid"
	"github.com/petermattis/goid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/client"
	"github.com/scusemua/distributed-notebook/common/scheduling/prewarm"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"github.com/scusemua/distributed-notebook/gateway/internal/domain"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel/execution_failed"
	"github.com/scusemua/distributed-notebook/gateway/internal/kernel/provisioner"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"runtime/debug"
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

	ErrEmptyKernelId  = status.Error(codes.InvalidArgument, "kernel ID is empty")
	ErrNotImplemented = status.Error(codes.Unimplemented, "not implemented in daemon")

	// Internal Errrors

	ErrKernelNotReady                          = errors.New("kernel not ready")
	ErrKernelIDRequired                        = errors.New("kernel id frame is required for kernel_info_request")
	ErrUnsupportedMsgTypeForArtificialResponse = errors.New("unsupported message type for artificial response")
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

// resetKernelReply is used by the ClusterGatewayImpl's resetKernel and processResetKernelReplies methods.
//
// resetKernelReply associates a particular scheduling.KernelReplica with a "reset_kernel_reply" message.
type resetKernelReply struct {
	KernelReplica scheduling.KernelReplica
	Reply         *messaging.JupyterMessage
}

type MessageHandler func(kernel scheduling.Kernel, socketType messaging.MessageType, msg *messaging.JupyterMessage) error

// ResponseForwarder is an interface that provides the means to forward responses from scheduling.Kernel and
// scheduling.KernelReplica instances back to the associated Jupyter client.
type ResponseForwarder interface {
	// ForwardResponse forwards a response from a scheduling.Kernel / scheduling.KernelReplica
	// back to the Jupyter client.
	ForwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error

	// SendErrorResponse is used to respond to a shell message immediately, before we've routed it to any local
	// schedulers or kernel replicas, because we encountered an unrecoverable error while (pre)processing the message.
	SendErrorResponse(kernel scheduling.Kernel, request *messaging.JupyterMessage, errContent error, typ messaging.MessageType) error
}

// Manager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
// scheduling.KernelReplica instances running within the cluster.
type Manager struct {
	id string

	log logger.Logger

	// debugMode causes more information to be included in Jupyter requests, among other things.
	debugMode bool

	// sessions is a mapping from Jupyter session ID to scheduling.Kernel.
	sessions hashmap.HashMap[string, scheduling.Kernel]

	handlers map[messaging.MessageType]MessageHandler

	failedExecutionHandler *execution_failed.Handler

	// requestTracingEnabled controls whether we embed proto.RequestTrace structs within Jupyter requests and replies.
	requestTracingEnabled bool

	requestLog *metrics.RequestLog

	// metricsProvider provides all metrics to the members of the scheduling package.
	metricsProvider *metrics.ClusterMetricsProvider

	// responseForwarder is responsible for forwarding responses from scheduling.Kernel and scheduling.KernelReplica
	// instances back to the associated Jupyter client.
	responseForwarder ResponseForwarder

	// idleSessionReclaimer reclaims idle Kernels.
	idleSessionReclaimer *IdleSessionReclaimer

	// kernelProvisioner handles the creation of Kernels.
	kernelProvisioner *provisioner.Provisioner

	// Map of kernels that are starting for the first time.
	//
	// We add an entry to this map at the beginning of ClusterDaemon::StartKernel.
	//
	// We remove an entry from this map when all replicas of that kernel have joined their SMR cluster.
	// We also send a notification on the channel mapped by the kernel's key when all replicas have joined their SMR cluster.
	kernelsStarting hashmap.HashMap[string, chan struct{}]

	// executeRequestForwarder forwards "execute_request" (or "yield_request") messages to Kernels one-at-a-time.
	executeRequestForwarder *client.ExecuteRequestForwarder[[]*messaging.JupyterMessage]

	// numActiveKernels is the number of actively-running Kernels.
	numActiveKernels atomic.Int32

	// numActiveTrainings is the cluster-wide number of active training events.
	numActiveTrainings atomic.Int32

	cluster scheduling.Cluster

	// notifier is used to send notifications to the cluster dashboard.
	notifier domain.Notifier

	provider *Provider
}

// NewManager creates a new Manager struct and returns a pointer to it.
func NewManager(id string, cluster scheduling.Cluster, requestLog *metrics.RequestLog, responseForwarder ResponseForwarder,
	metricsProvider *metrics.ClusterMetricsProvider, networkProvider provisioner.NetworkProvider, notifier domain.Notifier,
	opts *domain.ClusterGatewayOptions) *Manager {

	manager := &Manager{
		id:                id,
		provider:          NewProvider(),
		sessions:          hashmap.NewConcurrentMap[scheduling.Kernel](32),
		kernelsStarting:   hashmap.NewCornelkMap[string, chan struct{}](64),
		handlers:          make(map[messaging.MessageType]MessageHandler),
		responseForwarder: responseForwarder,
		cluster:           cluster,
		requestLog:        requestLog,
		debugMode:         opts.DebugMode,
		metricsProvider:   metricsProvider,
	}

	manager.handlers[messaging.ControlMessage] = manager.controlHandler
	manager.handlers[messaging.ShellMessage] = manager.shellHandler
	manager.handlers[messaging.StdinMessage] = manager.stdinHandler
	manager.handlers[messaging.HBMessage] = manager.heartbeatHandler

	failedExecutionHandler := execution_failed.NewHandler(opts, manager, notifier)
	manager.failedExecutionHandler = failedExecutionHandler

	kernelProvisioner := provisioner.NewProvisioner(id, cluster, notifier, metricsProvider, networkProvider,
		manager.shellHandlerWrapper, manager.provider, manager.CallbackProvider(), opts)
	manager.kernelProvisioner = kernelProvisioner

	if opts.IdleSessionReclamationEnabled && opts.IdleSessionReclamationIntervalSec > 0 {
		interval := time.Duration(opts.IdleSessionReclamationIntervalSec) * time.Second
		numReplicasPerKernel := manager.cluster.Scheduler().Policy().NumReplicas()

		manager.idleSessionReclaimer = NewIdleSessionReclaimer(
			manager.provider.Kernels, interval, numReplicasPerKernel, nil)

		manager.idleSessionReclaimer.Start()
	}

	manager.executeRequestForwarder = client.NewExecuteRequestForwarder[[]*messaging.JupyterMessage](
		manager.notifier.NotifyDashboard, nil)

	config.InitLogger(&manager.log, manager)

	return manager
}

func (km *Manager) CallbackProvider() scheduling.CallbackProvider {
	return km
}

func (km *Manager) ExecutionLatencyCallback(latency time.Duration, workloadId string, kernelId string) {
	milliseconds := float64(latency.Milliseconds())

	if km.metricsProvider.PrometheusMetricsEnabled() {
		km.metricsProvider.GetGatewayPrometheusManager().JupyterTrainingStartLatency.
			With(prometheus.Labels{
				"workload_id": workloadId,
				"kernel_id":   kernelId,
			}).
			Observe(milliseconds)
	}

	km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		statistics.JupyterTrainingStartLatencyMillis.Add(milliseconds)
		statistics.JupyterTrainingStartLatenciesMillis = append(
			statistics.JupyterTrainingStartLatenciesMillis, milliseconds)
	})
}

func (km *Manager) ExecutionFailedCallback(kernel scheduling.Kernel, originalExecRequest *messaging.JupyterMessage) error {
	msg, err := km.failedExecutionHandler.HandleFailedExecution(kernel, originalExecRequest)
	if err != nil {
		return err
	}

	km.log.Debug(utils.LightBlueStyle.Render("Resubmitting 'execute_request' message targeting kernel %s now."), kernel.ID())
	err = km.ForwardRequestToKernel(kernel.ID(), msg, messaging.ShellMessage)

	if errors.Is(err, types.ErrKernelNotFound) {
		km.log.Error("ShellHandler couldn't identify kernel \"%s\"...", kernel.ID())

		km.provider.Kernels.Store(msg.DestinationId, kernel)
		km.provider.Kernels.Store(msg.JupyterSession(), kernel)
		km.provider.Kernels.Store(kernel.ID(), kernel)

		kernel.BindSession(msg.JupyterSession())

		err = km.ForwardRequestToKernel(kernel.ID(), msg, messaging.ShellMessage)
	}

	if err != nil {
		km.log.Error("Resubmitted 'execute_request' message erred: %s", err.Error())
		go km.notifier.NotifyDashboardOfError("Resubmitted 'execute_request' Erred", err.Error())
		return err
	}

	return nil
}

func (km *Manager) NotificationCallback(title string, content string, notificationType messaging.NotificationType) {
	km.notifier.NotifyDashboard(title, content, notificationType)
}

// IncrementNumActiveExecutions increments the global counter of the number of active executions.
func (km *Manager) IncrementNumActiveExecutions() {
	km.numActiveTrainings.Add(1)
}

// DecrementNumActiveExecutions decrements the global counter of the number of active executions.
func (km *Manager) DecrementNumActiveExecutions() {
	km.numActiveTrainings.Add(-1)
}

// NumActiveExecutions returns the global number of active executions.
func (km *Manager) NumActiveExecutions() int32 {
	return km.numActiveTrainings.Load()
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
	existingKernel, _ := km.provider.Kernels.Load(in.Id)

	kernel, resp, err := km.kernelProvisioner.StartKernel(ctx, in, existingKernel)
	if err != nil {
		return nil, errorf(err)
	}

	km.provider.KernelIdToKernel.Store(in.Id, kernel)
	km.provider.Kernels.Store(in.Id, kernel)

	// Make sure to associate the Jupyter Session with the kernel.
	kernel.BindSession(in.Session)
	km.provider.Kernels.Store(in.Session, kernel)

	// Map all the sessions to the kernel client.
	for _, sess := range kernel.Sessions() {
		km.log.Debug("Storing kernel %v under session ID \"%s\".", kernel, sess)
		km.provider.Kernels.Store(sess, kernel)
	}

	km.newKernelCreated(startTime, in.Id)

	km.registerKernelWithExecReqForwarder(kernel)

	return resp, nil
}

// GetKernelStatus returns the status of a particular kernel as specified in the proto.KernelId parameter.
func (km *Manager) GetKernelStatus(_ context.Context, in *proto.KernelId) (*proto.KernelStatus, error) {
	kernel, ok := km.provider.Kernels.Load(in.Id)
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

	kernel, loaded := km.provider.Kernels.Load(replicaInfo.KernelId)
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
	return km.kernelProvisioner.NotifyKernelRegistered(ctx, in)
}

func (km *Manager) PingKernel(ctx context.Context, in *proto.PingInstruction) (*proto.Pong, error) {
	receivedAt := time.Now()
	kernelId := in.KernelId

	messageType, socketType, err := getMessageAndSocketTypeForPing(in)
	if err != nil {
		km.log.Error("PigKernel: %v", err)
		return &proto.Pong{Id: kernelId, Success: false, RequestTraces: nil}, errorf(types.ErrInvalidSocketType)
	}

	kernel, loaded := km.provider.Kernels.Load(kernelId)
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
	kernel, loaded := km.provider.Kernels.Load(in.Id)

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
	kernel, ok := km.provider.Kernels.Load(in.Id)
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
			km.provider.Kernels.Delete(sess)
		}

		km.log.Debug("Cleaned sessions %v after replicas stopped.", kernel.Sessions())
		if restart {
			// Keep kernel records if restart is requested.
			kernel.ClearSessions()
		} else {
			km.provider.Kernels.Delete(kernel.ID())
			km.provider.KernelIdToKernel.Delete(kernel.ID())

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

// forwardResponseFromKernel forwards the given messaging.JupyterMessage response from the given
// scheduling.KernelReplica to the Jupyter client.
func (km *Manager) forwardResponseFromKernel(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	km.updateRequestTraceReplicaId(from, msg)

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		km.cleanUpBeforeForwardingExecuteReply(from, msg)
	}

	return km.responseForwarder.ForwardResponse(from, typ, msg)
}

// cleanUpBeforeForwardingExecuteReply is called when forwarding an "execute_reply" message back to the client.
//
// cleanUpBeforeForwardingExecuteReply performs any necessary "clean up" steps. The steps that are required ultimately
// depend upon the configured scheduling.Policy.
//
// For example, scheduling.Policy instances in which the scheduling.ContainerLifetime is scheduling.SingleTrainingEvent
// will either terminate the scheduling.KernelContainer instance(s) or return them to the warm container pool.
func (km *Manager) cleanUpBeforeForwardingExecuteReply(from router.Info, execReplyMsg *messaging.JupyterMessage) {
	// If the scheduling policy isn't a single-training-event policy, then we can just return immediately.
	if km.cluster.Scheduler().Policy().ContainerLifetime() != scheduling.SingleTrainingEvent {
		return
	}

	// Attempt to load the kernel. If we do, and we find that the kernel has no replicas and the message is designated
	// as being a failed "execute_request" message, then we can just return. There are no replicas to clean up, and
	// the execution failed.
	kernel, loaded := km.provider.GetKernel(from.ID())
	if !loaded {
		km.log.Error("Could not find Distributed Kernel Client for kernel \"%s\"...", from.ID())
		return
	}

	if kernel.Size() == 0 && execReplyMsg.IsFailedExecuteRequest {
		return
	}

	km.log.Debug("Kernel \"%s\" has finished training. Removing container.", from.ID())

	if !km.cluster.Scheduler().Policy().ReuseWarmContainers() {
		_ = km.removeAllReplicasOfKernel(kernel, true, false, false)
		return
	}

	// For the "middle ground" policy, we return the kernel's container to the warm container pool.
	if km.cluster.Scheduler().Policy().ReuseWarmContainers() {
		km.log.Debug("Reusing warm kernel container.")

		// Send 'reset' request.
		err := km.resetKernel(kernel, true)
		if err != nil {
			km.log.Error("Failed to reset kernel \"%s\": %v", kernel.ID(), err)
		}
	}
}

// updateRequestTraceReplicaId updates the ReplicaId field of the proto.RequestTrace embedded in the given *messaging.JupyterMessage.
func (km *Manager) updateRequestTraceReplicaId(from scheduling.KernelReplicaInfo, msg *messaging.JupyterMessage) {
	if !km.requestTracingEnabled {
		return
	}

	if msg.RequestTrace != nil {
		msg.RequestTrace.ReplicaId = from.ReplicaID()

		km.updateStatisticsFromShellExecuteReply(msg.RequestTrace)
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
	// First, search in the Kernels mapping.
	kernel, ok := km.provider.Kernels.Load(kernelOrSessionId)
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
		panic(fmt.Sprintf("Manager::controlHandler: invalid socket type '%d'", socketTyp))
	}

	km.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	goroutineId := goid.Get()

	var err error
	if kernel == nil {
		kernel, _ /* messageType */, err = domain.KernelAndTypeFromMsg(msg, km.provider)

		if kernel == nil {
			return types.ErrKernelNotFound
		}
	}

	if err != nil {
		km.log.Error("[gid=%d] Failed to extract kernel and/or message type from %v message. Error: %v. Message: %v.",
			goroutineId, socketTyp, err, msg)
		return err
	}

	if kernel.Status() != jupyter.KernelStatusRunning && km.shouldReplicasBeRunning(kernel) {
		return ErrKernelNotReady
	}

	resp, _, err := km.ensureKernelReplicasAreScheduled(kernel, msg, socketTyp)
	if err != nil {
		km.log.Warn("Error encountered while ensuring replica container(s) of kernel %s are scheduled in order to handle shell \"%s\" message: %v",
			kernel.ID(), msg.JupyterMessageType(), err)
		_ = km.responseForwarder.SendErrorResponse(kernel, msg, err, messaging.ShellMessage)
		return err
	}

	if connInfo := kernel.ConnectionInfo(); connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	if km.debugMode && km.requestLog != nil && msg.RequestTrace != nil {
		// If we added a RequestTrace for the first time, then let's also add an entry to our RequestLog.
		err = km.requestLog.AddEntry(msg, socketTyp, msg.RequestTrace)
		if err != nil {
			km.log.Warn("Failed to add entry to RequestLog for Jupyter %s \"%s\" message %s (JupyterID=%s) because: %v",
				socketTyp.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)
		}
	}

	// If resp is non-nil, then one or more replicas are not scheduled.
	if resp != nil {
		km.log.Debug("Replying with artificial \"%s\" response for Jupyter %s \"%s\" message \"%s\" (JupyterID=\"%s\") for kernel \"%s\".",
			resp.JupyterMessageType(), socketTyp.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID())

		err = km.forwardResponseFromKernel(kernel.TemporaryKernelReplicaClient(), socketTyp, resp)
		if err != nil {
			km.log.Error(utils.RedStyle.Render("Failed to forward %v \"%s\" response \"%s\" (JupyterID=\"%s\") to client of kernel %s: %v"),
				socketTyp, msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), kernel.ID(), resp)
			return err
		}

		return nil
	}

	return kernel.RequestWithHandler(context.Background(), "Forwarding", socketTyp, msg, km.forwardResponseFromKernel, nil)
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *Manager) shellHandler(kernel scheduling.Kernel, socketTyp messaging.MessageType, msg *messaging.JupyterMessage) error {
	if socketTyp != messaging.ShellMessage {
		panic(fmt.Sprintf("Manager::shellHandler: invalid socket type '%d'", socketTyp))
	}

	km.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	kernel, ok := km.provider.GetKernel(msg.JupyterSession())

	// Couldn't load the kernel. Let's check if we've registered the kernel under a different ID that
	// is also encoded within the Jupyter message.
	if !ok && (msg.JupyterMessageType() == messaging.KernelInfoRequest || msg.JupyterMessageType() == messaging.ShellExecuteRequest) {
		// Register kernel on KernelInfoRequest
		if msg.DestinationId == "" {
			km.log.Error("Shell '%s' message '%s' does not contain a destination ID:\n%s",
				msg.JupyterMessageType(), msg.JupyterMessageId(), msg.StringFormatted())

			// We don't know which kernel this came from, so we can't really send an error message in response.
			return ErrKernelIDRequired
		}

		kernel, ok = km.provider.GetKernel(msg.DestinationId)
		if !ok {
			km.log.Error("Could not find kernel or session \"%s\" while handling shell message %v of type '%v', session=%v",
				msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

			km.log.Error("Valid kernels/sessions are (%d):", km.provider.NumKernels())
			km.provider.GetKernels().Range(func(id string, kernel scheduling.Kernel) (contd bool) {
				km.log.Error("%s (kernel \"%s\")", id, kernel.ID())
				return true
			})

			// We don't know which kernel this came from, so we can't really send an error message in response.
			return fmt.Errorf("%w: could not find kernel associated with session \"%s\" or destination \"%s\"",
				types.ErrKernelNotFound, msg.JupyterSession(), msg.DestinationId)
		}

		kernel.BindSession(msg.JupyterSession())
		km.provider.StoreKernelBySessionId(msg.JupyterSession(), kernel)
	}

	// If the kernel is still nil, then that's a bug.
	if kernel == nil {
		km.log.Error("Could not find kernel or session \"%s\" while handling shell message %v of type '%v', session=%v",
			msg.DestinationId, msg.JupyterMessageId(), msg.JupyterMessageType(), msg.JupyterSession())

		km.log.Error("Valid kernels/sessions are (%d):", km.provider.NumKernels())
		km.provider.GetKernels().Range(func(id string, kernel scheduling.Kernel) (contd bool) {
			km.log.Error("%s (kernel \"%s\")", id, kernel.ID())
			return true
		})

		// If there's no message ID, then something is really wrong.
		// We'll print the stack so we can better trace through what happened while handling this message.
		if len(msg.DestinationId) == 0 {
			km.log.Error("Extracted empty kernel ID from ZMQ \"%s\" message: %v", msg.JupyterMessageType(), msg)
			debug.PrintStack()
		}

		// We don't know which kernel this came from, so we can't really send an error message in response.
		return fmt.Errorf("%w: could not find kernel associated with session \"%s\" or destination \"%s\"",
			types.ErrKernelNotFound, msg.JupyterSession(), msg.DestinationId)
	}

	connInfo := kernel.ConnectionInfo()
	if connInfo != nil {
		msg.SetSignatureSchemeIfNotSet(connInfo.SignatureScheme)
		msg.SetKeyIfNotSet(connInfo.Key)
	}

	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		// executeRequestHandler handles sending an error response if it encounters an error.
		return km.executeRequestHandler(kernel, msg)
	}

	km.log.Debug("Forwarding shell message to kernel %s: %s", msg.DestinationId, msg.StringFormatted())
	err := km.forwardRequest(kernel, messaging.ShellMessage, msg)
	if err != nil {
		km.log.Error("Error while handling/forwarding shell \"%s\" message \"%s\" (JupyterID=\"%s\"): %v.",
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), err)

		// We'll send an error message to the associated client here, though it's possible that we were able to
		// send a reply, and the error came from something that occurred after sending our response (I think?).
		_ = km.responseForwarder.SendErrorResponse(kernel, msg, err, messaging.ShellMessage)

		return err
	}

	return nil
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

// shouldReplicasBeRunning returns a flag indicating whether the containers of Kernels should be running already.
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

func (km *Manager) updateStatisticsFromShellExecuteReply(trace *proto.RequestTrace) {
	if trace == nil {
		km.log.Warn("RequestTrace is nil when attempting to extract metrics/statistics from shell \"execute_reply\"")
		return
	}

	km.metricsProvider.UpdateClusterStatistics(func(statistics *metrics.ClusterStatistics) {
		if trace.CudaInitMicroseconds > 0 {
			statistics.CumulativeCudaInitMicroseconds.Add(float64(trace.CudaInitMicroseconds))
			statistics.NumCudaRuntimesInitialized.Add(1)
		}

		if trace.ReplayTimeMicroseconds > 0 {
			statistics.CumulativeReplayTimeMicroseconds.Add(float64(trace.ReplayTimeMicroseconds))
			statistics.TotalNumReplays.Add(1)

			session, loaded := km.cluster.GetSession(trace.KernelId)
			if !loaded || session == nil {
				km.log.Warn("Could not find session \"%s\" specified in RequestTrace: %s", trace.KernelId, trace.String())
			} else {
				// Subtract 1 to exclude the last training event that just completed.
				statistics.TotalNumCellsReplayed.Add(int64(session.NumTrainingEventsProcessed() - 1))
			}
		}

		if trace.DownloadModelMicroseconds > 0 {
			statistics.CumulativeTimeDownloadModelMicroseconds.Add(float64(trace.DownloadDatasetMicroseconds))
			statistics.NumTimesDownloadModelMicroseconds.Add(1)
		}

		// Downloading the dataset overhead.
		if trace.DownloadDatasetMicroseconds > 0 {
			statistics.CumulativeTimeDownloadTrainingDataMicroseconds.Add(float64(trace.DownloadDatasetMicroseconds))
			statistics.NumTimesDownloadTrainingDataMicroseconds.Add(1)
		}

		// Tokenization overhead. Only relevant for NLP datasets.
		if trace.TokenizeDatasetMicroseconds > 0 {
			statistics.CumulativeTokenizeDatasetMicroseconds.Add(float64(trace.TokenizeDatasetMicroseconds))
			statistics.NumTimesTokenizeDatasetMicroseconds.Add(1)
		}

		if trace.UploadModelAndTrainingDataMicroseconds > 0 {
			statistics.CumulativeTimeUploadModelAndTrainingDataMicroseconds.Add(float64(trace.UploadModelAndTrainingDataMicroseconds))
			statistics.NumTimesUploadModelAndTrainingDataMicroseconds.Add(1)
		}

		if trace.CopyFromCpuToGpuMicroseconds > 0 {
			statistics.CumulativeTimeCopyDataHostToDeviceMicroseconds.Add(float64(trace.CopyFromCpuToGpuMicroseconds))
			statistics.NumTimesCopyDataHostToDeviceMicroseconds.Add(1)
		}

		if trace.CopyFromGpuToCpuMicroseconds > 0 {
			statistics.CumulativeTimeCopyDataDeviceToHostMicroseconds.Add(float64(trace.CopyFromGpuToCpuMicroseconds))
			statistics.NumTimesCopyDataDeviceToHostMicroseconds.Add(1)
		}

		if trace.LeaderElectionTimeMicroseconds > 0 {
			statistics.CumulativeLeaderElectionTimeMicroseconds.Add(float64(trace.LeaderElectionTimeMicroseconds))
		}

		if trace.RequestReceivedByKernelReplica > 0 && trace.ElectionCreationTime > 0 {
			preprocessDuration := trace.ElectionCreationTime - trace.RequestReceivedByKernelReplica
			statistics.CumulativeKernelCreateElectionMillis.Add(float64(preprocessDuration))
		}

		if trace.ElectionCreationTime > 0 && trace.ElectionProposalPhaseStartTime > 0 {
			electionCreationDuration := trace.ElectionProposalPhaseStartTime - trace.ElectionCreationTime
			statistics.CumulativeKernelCreateElectionMillis.Add(float64(electionCreationDuration))
		}

		if trace.ElectionProposalPhaseStartTime > 0 && trace.ElectionExecutionPhaseStartTime > 0 {
			proposalVotePhaseDuration := trace.ElectionExecutionPhaseStartTime - trace.ElectionProposalPhaseStartTime
			statistics.CumulativeKernelProposalVotePhaseMillis.Add(float64(proposalVotePhaseDuration))
		}

		if trace.ReplySentByKernelReplica > 0 && trace.ExecutionEndUnixMillis > 0 {
			postprocessDuration := trace.ReplySentByKernelReplica - trace.ExecutionEndUnixMillis
			statistics.CumulativeKernelPostprocessMillis.Add(float64(postprocessDuration))
		}

		statistics.CumulativeExecutionTimeMicroseconds.Add(float64(trace.ExecutionTimeMicroseconds))

		if trace.MessageType == messaging.ShellExecuteRequest || trace.MessageType == messaging.ShellExecuteReply || trace.MessageType == messaging.ShellYieldRequest {
			statistics.ExecuteRequestTraces = append(statistics.ExecuteRequestTraces, trace)
		}
	})
}

// resetKernel sends a "reset_kernel_request" to the specified kernel. This message instructs the kernel to, at a
// minimum, completely wipe its user namespace, removing all user-defined data/variables.
//
// The "reset_kernel_request" may also instruct the kernel replica to revert to a scheduling.PrewarmContainer (or to
// become a scheduling.PrewarmContainer, if it had never been one before).
func (km *Manager) resetKernel(kernel scheduling.Kernel, revertToPrewarm bool) error {
	km.log.Debug("Preparing to send \"%s\" to kernel \"%s\" with revertToPrewarm=%v.",
		messaging.ControlResetKernelRequest, kernel.ID(), revertToPrewarm)

	msgId := uuid.NewString()
	frames := messaging.NewJupyterFramesWithHeaderAndSpecificMessageId(msgId, messaging.ControlResetKernelRequest, kernel.ID())

	content := map[string]interface{}{
		"revert_to_prewarm": revertToPrewarm,
	}

	err := frames.EncodeContent(&content)
	if err != nil {
		km.log.Error("Failed to encode content of IOPub status message for kernel \"%s\": %v", kernel.ID(), err)
		return err
	}

	var msg zmq4.Msg
	msg.Frames, err = frames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		km.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return jupyter.ErrFailedToVerifyMessage
	}

	respChan := make(chan *resetKernelReply, km.cluster.Scheduler().Policy().NumReplicas())
	startTime := time.Now()
	numRepliesReceived := atomic.Int32{}

	responseHandler := func(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		latestNumRepliesReceived := numRepliesReceived.Add(1)

		// Notify that all replies have been received.
		km.log.Debug("Received %s \"%s\" from %s for message \"%s\". Received %d/%d replies. Time elapsed: %v.",
			typ.String(), messaging.ControlResetKernelReply, from.String(), msgId, latestNumRepliesReceived, kernel.Size(),
			time.Since(startTime))

		var replica scheduling.KernelReplica

		id := from.ReplicaID()
		if id >= 1 {
			replica, _ = kernel.GetReplicaByID(id)
		}

		resp := &resetKernelReply{
			KernelReplica: replica,
			Reply:         msg,
		}

		respChan <- resp

		return nil
	}

	jMsg := messaging.NewJupyterMessage(&msg)
	msgTyp := messaging.ControlMessage

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	err = kernel.RequestWithHandler(ctx, "Forwarding", msgTyp, jMsg, responseHandler, nil)
	if err != nil {
		km.log.Error("Error while issuing %s '%s' request %s (JupyterID=%s) to kernel %s: %v",
			msgTyp.String(), jMsg.JupyterMessageType(), jMsg.RequestId, jMsg.JupyterMessageId(), kernel.ID(), err)
		return err
	}

	replies := make([]*resetKernelReply, 0, kernel.Size())
	for numRepliesReceived.Load() < int32(km.cluster.Scheduler().Policy().NumReplicas()) {
		select {
		case <-ctx.Done():
			{
				ctxError := ctx.Err()

				nRepliesReceived := numRepliesReceived.Load()

				var errorMessage string
				if ctxError != nil {
					errorMessage = fmt.Sprintf("%v '%s' request for kernel '%s' failed after receiving %d/%d replies: %v",
						msgTyp.String(), jMsg.JupyterMessageId(), kernel.ID(), nRepliesReceived, kernel.Size(), ctxError)
				} else {
					errorMessage = fmt.Sprintf("%v '%s' request for kernel '%s' timed-out after receiving %d/%d replies.",
						msgTyp.String(), jMsg.JupyterMessageId(), kernel.ID(), nRepliesReceived, kernel.Size())
				}
				km.log.Error(errorMessage)

				err = types.ErrRequestTimedOut

				// If we received any replies, then we'll process the ones that we did receive.
				if len(replies) > 0 {
					processReplyErr := km.processResetKernelReplies(kernel, replies)
					if processReplyErr != nil {
						km.log.Error("Error while processing the %d reply/replies we did receive while resetting kernel \"%s\": %v",
							nRepliesReceived, kernel.ID(), processReplyErr)
						err = errors.Join(err, processReplyErr)
					}
				}

				return err
			}
		case resp := <-respChan:
			{
				replies = append(replies, resp)
			}
		}
	}

	return km.processResetKernelReplies(kernel, replies)
}

// removeAllReplicasOfKernel is used to de-schedule the replicas of the given kernel without removing the kernel itself.
//
// This does not remove the kernel itself.
func (km *Manager) removeAllReplicasOfKernel(kernel scheduling.Kernel, inSeparateGoroutine bool,
	isIdleReclaim bool, noop bool) error {

	var (
		startedRemoving   bool
		descheduleAttempt scheduling.RemoveReplicaContainersAttempt
	)

	// We'll keep executing this loop as long as the replicas of the target kernel are not removed.
	// We break from the loop internally if (a) we claim ownership over a container removal descheduleAttempt, in which
	// case we  break out so that we can orchestrate the container removal descheduleAttempt, or (b) if we find that the
	// replicas are in fact removed. This may occur if, for example, a previous descheduleAttempt concludes.
	for {
		// Try to start a new descheduleAttempt at scheduling the replica container(s) of this kernel.
		startedRemoving, descheduleAttempt = kernel.InitRemoveReplicaContainersOperation()

		// If we started a new descheduleAttempt, then we'll break out of the loop and orchestrate the removal of the
		// containers of the replicas of the target kernel.
		if startedRemoving {
			km.log.Debug(
				utils.LightBlueStyle.Render(
					"Started 'descheduling' attempt to remove %d replica container(s) for kernel \"%s\"."),
				km.cluster.Scheduler().Policy().NumReplicas(), kernel.ID())
			break
		}

		// We didn't start a new removal descheduleAttempt.
		// If the returned descheduleAttempt is also nil, then that means that there was also not an active
		// descheduleAttempt. So, the replicas are apparently already removed.
		if descheduleAttempt == nil {
			km.log.Debug("Tried to start descheduleAttempt to remove replica container(s) for kernel \"%s\", "+
				"but apparently they're already removed.", kernel.ID())

			// Double-check that the kernel's replicas are removed. If they are, then we'll just return entirely.
			kernelSize := kernel.Size()
			if kernelSize == 0 {
				return nil
			}

			// This would be truly bizarre, but if this occurs, then we'll just sleep briefly and then try again...
			km.log.Error("Thought kernel \"%s\" was fully descheduled, but kernel has %d replica(s).",
				kernel.ID(), kernelSize)

			time.Sleep(time.Millisecond * (5 + time.Duration(rand.Intn(25))))
			continue
		}

		// If we did not start a new descheduleAttempt, then a previous descheduleAttempt must still be active.
		// We'll just wait for the descheduleAttempt to conclude.
		// If the scheduling is successful, then this will eventually return nil.
		// If the context passed to scheduleReplicas has a time-out, and we time out, then this will return an error.
		km.log.Debug("Found existing 'create replica containers' operation for kernel %s that began %v ago. "+
			"Waiting for operation to complete.", kernel.ID(), descheduleAttempt.TimeElapsed())

		return km.waitForDeschedulingToEnd(kernel, descheduleAttempt)
	}

	// doRemoveReplicas removes the kernel's replicas and returns an error if one occurs.
	doRemoveReplicas := func() error {
		err := kernel.RemoveAllReplicas(km.cluster.Placer().Reclaim, noop, isIdleReclaim)
		if err != nil {
			km.log.Error("Failed to remove all replicas of kernel \"%s\" because: %v", kernel.ID(), err)
		}

		setDoneErr := descheduleAttempt.SetDone(err /* will be nil on success */)
		if setDoneErr != nil {
			km.log.Error("Error while calling SetDone on deschedule attempt for kernel \"%s\": %v",
				kernel.ID(), setDoneErr)
		}

		return err // Will be nil on success.
	}

	// Spawn a separate goroutine to execute the doRemoveReplicas function if we've been instructed to do so.
	if inSeparateGoroutine {
		go func() {
			// RemoveHost the replicas.
			_ = doRemoveReplicas()
		}()

		return nil
	}

	// RemoveHost the replicas.
	err := doRemoveReplicas()

	// This will be nil if de-schedule was successful,
	// or if the caller specified that we should use a separate goroutine for the replica removal.
	if err != nil {
		go km.notifier.NotifyDashboardOfError(fmt.Sprintf("Failed to RemoveHost One or More Replicas of kernel \"%s\"",
			kernel.ID()), err.Error())

		return err
	}

	return nil
}

// waitForDeschedulingToEnd is called while handling an "execute_request" if it is found that there is a "descheduling
// attempt" in progress for the target scheduling.Kernel.
//
// waitForDeschedulingToEnd first checks if the attempt has finished. If so, then waitForDeschedulingToEnd will simply
// delete the record of it and return.
//
// If the attempt is not finished, then waitForDeschedulingToEnd will wait for (as of the time of writing this
// comment) 7.5 minutes. If after 6 minutes, the attempt has not completed, then waitForDeschedulingToEnd will return
// an error.
//
// It's possible that there is simply a large network I/O occurring or something like that, so the fact that the attempt
// has not resolved is not necessarily indicative that something is wrong.
func (km *Manager) waitForDeschedulingToEnd(kernel scheduling.Kernel, removalAttempt scheduling.RemoveReplicaContainersAttempt) error {
	// If the removal attempt is nil, then there is nothing to wait for.
	if removalAttempt == nil {
		km.log.Debug("Nil removal attempt passed for kernel \"%s\". Nothing to wait for.", kernel.ID())
		return nil
	}

	// First, check if this attempt has finished. If so, then we'll simply delete the record of it and return.
	if removalAttempt.IsComplete() {
		return nil
	}

	km.log.Debug("Target kernel \"%s\" is being descheduled as of %v ago.",
		kernel.ID(), removalAttempt.TimeElapsed())

	startTime := time.Now()

	// We do this in a loop so we can incrementally print warning messages after we've been waiting for a while,
	// in the event that the descheduling operation does not resolve quickly.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*150 /* 2.5 minutes */)

		// Wait for the replicas to finish being descheduled.
		err := removalAttempt.Wait(ctx)
		cancel()

		// No error means that the descheduling attempt concluded successfully.
		if err == nil {
			km.log.Debug("Descheduling complete for kernel \"%s\". Waited: %v. Time to reschedule.",
				kernel.ID(), time.Since(startTime))
			return nil
		}

		// Not necessarily an error. Context timeout is set to a low value so that we can incrementally print
		// warning messages after we've been waiting for a while, in the event that the descheduling operation
		// does not resolve quickly.
		km.log.Warn("Awaiting the completion of replica descheduling for kernel \"%s\". Time elapsed: %v.",
			kernel.ID(), time.Since(startTime))

		// But if the error is NOT a context.DeadlineExceeded error, then something went wrong.
		if !errors.Is(err, context.DeadlineExceeded) {
			km.log.Error("Attempt to remove replicas of kernel %s resulted in an error: %v",
				kernel.ID(), err)

			errorTitle := fmt.Sprintf("Failed to RemoveHost Replicas of Kernel \"%s\"", kernel.ID())
			go km.notifier.NotifyDashboardOfError(errorTitle, err.Error())

			return err
		}
	}

	return fmt.Errorf("%w: kernel \"%s\" has been stuck in a state of being de-scheduled for %v",
		ErrKernelNotReady, kernel.ID(), time.Since(removalAttempt.StartedAt()))
}

// processResetKernelReplies is called by resetKernel to process the replies send by the kernel replicas.
func (km *Manager) processResetKernelReplies(kernel scheduling.Kernel, replies []*resetKernelReply) error {
	prewarmer := km.cluster.Scheduler().ContainerPrewarmer()
	if prewarmer == nil {
		km.log.Warn("Container Prewarmer is nil...")
		return nil
	}

	// First, remove the replicas from the kernel.
	_ = km.removeAllReplicasOfKernel(kernel, true, false, true)

	errs := make([]error, 0, len(replies))
	for _, reply := range replies {
		msg := reply.Reply
		replica := reply.KernelReplica

		host := replica.Host()
		if host == nil {
			km.log.Error("Replica %d of kernel \"%s\" has a nil Host...", replica.ReplicaID(), replica.ID())
			errs = append(errs, scheduling.ErrNilHost)
			continue
		}

		var content map[string]interface{}
		err := msg.JupyterFrames.DecodeContent(&content)
		if err != nil {
			km.log.Error("Failed to decode content of \"%s\" message from replica %d of kernel \"%s\": %v",
				msg.JupyterMessageType(), replica.ReplicaID(), replica.ID(), err)
			errs = append(errs, err)
			continue
		}

		var kernelId string
		val, loaded := content["kernel_id"]
		if loaded {
			kernelId = val.(string)
		} else {
			kernelId = replica.ID()
		}

		kernelReplicaSpec := replica.KernelReplicaSpec().Clone()
		kernelReplicaSpec.Kernel.Id = kernelId

		prewarmedContainer := prewarm.NewPrewarmedContainerBuilder().
			WithHost(host).
			WithKernelConnectionInfo(jupyter.KernelConnectionInfoFromJupyterConnectionInfo(replica.ConnectionInfo())).
			WithKernelReplicaSpec(kernelReplicaSpec).
			WithPrewarmedContainerUsedCallback(nil).
			Build()

		err = prewarmer.ReturnPrewarmContainer(prewarmedContainer)
		if err != nil {
			km.log.Error("Failed to return container to pre-warm pool after resetting: %v.", err)
			errs = append(errs, err)
		}

		err = replica.Close()
		if err != nil {
			km.log.Error("Failed to close replica %d of kernel \"%s\" after demoting it to a %v container: %v",
				replica.ReplicaID(), replica.ID(), scheduling.PrewarmContainer, err)
			errs = append(errs, err)
		}
	}

	if len(errs) == 1 {
		return errs[0]
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// ensureKernelReplicasAreScheduled ensures that the replicas of the specified scheduling.Kernel are already scheduled.
//
// If they're not, then ensureKernelReplicasAreScheduled will either schedule the replicas, if the given msg is an
// "execute_request" message, or it will simply return an artificial response, in the case of all other message types.
//
// ensureKernelReplicasAreScheduled possibly returns a dummy message response generated by the Cluster Gateway,
// a bool flag indicating whether the replicas were already scheduled (true) or not (false), and an optional error.
//
// Important: in general, if ensureKernelReplicasAreScheduled returns an error, then that error should be sent back
// to the client by the caller of ensureKernelReplicasAreScheduled.
func (km *Manager) ensureKernelReplicasAreScheduled(kernel scheduling.Kernel, msg *messaging.JupyterMessage, typ messaging.MessageType) (*messaging.JupyterMessage, bool, error) {
	km.log.Debug("Verifying that replicas of kernel %s are all scheduled before processing %s \"%s\" request \"%s\"",
		kernel.ID(), typ.String(), msg.JupyterMessageType(), msg.JupyterMessageId())

	// Check if any replicas are being migrated and, if so, then wait for them to finish being migrated.
	migrationCtx, migrateCancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer migrateCancel()

	err := kernel.WaitForMigrationsToComplete(migrationCtx)
	if err != nil {
		return nil, false, err
	}

	// Check if the kernel is being descheduled. If so, then we'll wait for a bit for it to finish being descheduled.
	_, removalAttempt := kernel.ReplicaContainersAreBeingRemoved()
	if err = km.waitForDeschedulingToEnd(kernel, removalAttempt); err != nil {
		return nil, false, err
	}

	// If the replica(s) are scheduled, then we have nothing to do.
	if kernel.ReplicasAreScheduled() {
		km.log.Debug("Replicas of kernel %s are scheduled.", kernel.ID())
		return nil, true, nil
	}

	// For any message that isn't an "execute_request" message, we'll return an artificial response when the
	// replicas of the kernel are not actively running.
	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		km.log.Debug("Replicas of kernel %s are NOT scheduled. Generating artificial response to %s \"%s\" message %s (JupyterID=\"%s\").",
			kernel.ID(), typ.String(), msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId())
		resp, err := km.generateArtificialResponse(kernel, msg, typ)
		return resp, false, err
	}

	km.log.Debug("Replicas of kernel %s are NOT scheduled. Scheduling replicas now before handling Jupyter \"execute_request\" message %s (JupyterID=\"%s\").",
		kernel.ID(), msg.RequestId, msg.JupyterMessageId())

	// We'll wait up to 5 minutes for this operation to complete successfully.
	// That's a long time.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// We'll use this as a means for the other goroutine to communicate the result of the scheduling operation to us.
	notifyChan := make(chan interface{}, 1)
	startTime := time.Now()

	// Create the kernel containers in another goroutine.
	go func() {
		scheduleError := km.kernelProvisioner.ScheduleReplicas(ctx, kernel, kernel.KernelSpec(), nil)

		if scheduleError == nil {
			// No error. Just send a struct{}{}.
			notifyChan <- struct{}{}
		} else {
			// Send the error.
			notifyChan <- scheduleError
		}
	}()

	// Wait for either the context to time out, for the operation to succeed, or for the operation to fail explicitly
	// for some other reason.
	select {
	case <-ctx.Done():
		{
			// If there's an error attached to the context, then we'll return it.
			if err = ctx.Err(); err != nil {
				km.log.Error("Timed-out waiting for replica container(s) of kernel \"%s\" to be created after %v: %v",
					kernel.ID(), time.Since(startTime), err)
			} else {
				km.log.Error("Timed-out waiting for replica container(s) of kernel \"%s\" to be created after %v.",
					kernel.ID(), time.Since(startTime))

				// We'll return an error indicating that the operation timed out.
				err = fmt.Errorf("%w: creation of replica container(s) for kernel \"%s\" timed out after %v",
					types.ErrRequestTimedOut, kernel.ID(), time.Since(startTime))
			}

			return nil, false, err
		}
	case v := <-notifyChan:
		{
			// If we received an error over the channel, then we'll log an error message and return the error.
			if err, ok := v.(error); ok {
				km.log.Warn("Failed to schedule replica container(s) of kernel \"%s\" after receiving Jupyter \"%s\" message: %v",
					kernel.ID(), msg.JupyterMessageType(), err)
				return nil, false, err
			} else {
				// Not an error. Operation must have succeeded.
				return nil, false, nil
			}
		}
	}
}

// generateArtificialResponse returns an artificial messaging.JupyterMessage response when a kernel receives a message and
// its replica container(s) is/are not actively scheduled.
func (km *Manager) generateArtificialResponse(kernel scheduling.Kernel, msg *messaging.JupyterMessage, typ messaging.MessageType) (*messaging.JupyterMessage, error) {
	jupyterMsgType := msg.JupyterMessageType()
	resp := msg.Clone()

	if typ == messaging.ShellMessage {
		ioMsg, err := km.kernelProvisioner.SendStatusMessage(kernel, "busy")
		if err != nil {
			km.log.Error("Failed to send IOPub \"busy\" status message to client of kernel \"%s\": %v",
				kernel.ID(), err)
			return nil, err
		} else {
			km.log.Debug("Successfully sent IOPub \"busy\" status message to client of kernel \"%s\": %v", kernel.ID(), ioMsg)
		}
	}

	var (
		content      map[string]interface{}
		responseType messaging.JupyterMessageType
	)
	if jupyterMsgType == messaging.KernelInfoRequest {
		content = getArtificialKernelInfoReply()
		responseType = messaging.KernelInfoReply
	} else {
		return nil, fmt.Errorf("%w: \"%s\"", ErrUnsupportedMsgTypeForArtificialResponse, jupyterMsgType)
	}

	header, err := resp.GetHeader()
	if err != nil {
		km.log.Error("Failed to get header of artificial \"%s\" message: %v", jupyterMsgType, err)
		return nil, err
	}

	err = resp.JupyterFrames.EncodeParentHeader(&header)
	if err != nil {
		km.log.Error("Failed to encode parent header of artificial \"%s\" message: %v", jupyterMsgType, err)
		return nil, err
	}

	// Create the message header.
	header = &messaging.MessageHeader{
		Date:     time.Now().UTC().Format(messaging.JavascriptISOString),
		MsgID:    uuid.NewString(),
		MsgType:  responseType,
		Session:  resp.JupyterSession(),
		Username: resp.JupyterUsername(),
		Version:  resp.JupyterVersion(),
	}

	err = resp.EncodeMessageHeader(header)
	if err != nil {
		km.log.Error("Failed to encode new header for artificial response to Jupyter %s \"%s\" message: %v",
			typ.String, jupyterMsgType, err)
		return nil, err
	}

	err = resp.JupyterFrames.EncodeContent(&content)
	if err != nil {
		km.log.Error("Failed to encode content for artificial response to Jupyter %s \"%s\" message: %v",
			typ.String, jupyterMsgType, err)
		return nil, err
	}

	resp.JupyterFrames.Frames, err = resp.JupyterFrames.SignByConnectionInfo(kernel.ConnectionInfo())
	if err != nil {
		km.log.Error("Failed to sign Jupyter message for kernel %s with signature scheme \"%s\" because: %v",
			kernel.ID(), kernel.ConnectionInfo().SignatureScheme, err)
		return nil, err
	}

	km.log.Debug("Returning artificial response to %s \"%s\" message \"%s\" (JupyterID=\"%s\"): %v",
		typ.String(), jupyterMsgType, resp.RequestId, resp.JupyterMessageId(), resp)

	return resp, nil
}

// getArtificialKernelInfoReply creates and returns a "kernel_info_reply"
func getArtificialKernelInfoReply() map[string]interface{} {
	content := make(map[string]interface{})

	langaugeInfo := make(map[string]interface{})
	langaugeInfo["name"] = "Any text"
	langaugeInfo["mimetype"] = "text/plain"
	langaugeInfo["file_extension"] = ".txt"

	content["status"] = "ok"
	content["protocol_version"] = "5.3"
	content["implementation"] = "Distributed Python 3"
	content["implementation_version"] = "0.2"
	content["language_info"] = langaugeInfo
	content["banner"] = "Distributed kernel - as useful as a parrot"
	content["help_links"] = []map[string]string{
		{
			"text": "Python Reference",
			"url":  "https://docs.python.org/3.12/",
		},
		{
			"text": "IPython Reference",
			"url":  "https://ipython.org/documentation.html",
		},
		{
			"text": "NumPy Reference",
			"url":  "https://docs.scipy.org/doc/numpy/reference/",
		},
		{
			"text": "SciPy Reference",
			"url":  "https://docs.scipy.org/doc/scipy/reference/",
		},
		{
			"text": "Matplotlib Reference",
			"url":  "https://matplotlib.org/contents.html/",
		},
		{
			"text": "SymPy Reference",
			"url":  "http://docs.sympy.org/latest/index.html",
		},
		{
			"text": "pandas Reference",
			"url":  "https://pandas.pydata.org/pandas-docs/stable/",
		},
	}

	return content
}
