package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/petermattis/goid"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/go-zeromq/zmq4"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/server"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils"
)

var (
	heartbeatInterval = time.Second

	ErrInvalidResourceSpec = errors.New("resource spec contains one or more invalid resource quantities")
	ErrDeadlineExceeded    = errors.New("deadline for parent context has already been exceeded")
	//ErrResourceSpecAlreadySet = errors.New("kernel already has a resource spec set")
)

type SMRNodeReadyNotificationCallback func(replica scheduling.KernelReplica)
type SMRNodeUpdatedNotificationCallback func(*messaging.MessageSMRNodeUpdated) // For node-added or node-removed notifications.

// ConnectionRevalidationFailedCallback defines a special type of callback function.
//
// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
type ConnectionRevalidationFailedCallback func(replica scheduling.KernelReplica, msg *messaging.JupyterMessage, err error)

// ResubmissionAfterSuccessfulRevalidationFailedCallback defines a special type of callback function.
//
// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
type ResubmissionAfterSuccessfulRevalidationFailedCallback func(replica scheduling.KernelReplica, msg *messaging.JupyterMessage, err error)

// KernelReplicaClient is an implementation of the KernelReplicaClient interface.
//
// All sockets except IOPub are connected on dialing.
//
// Each replica of a particular Distributed Kernel will have a corresponding KernelReplicaClient.
// These KernelClients are then wrapped/managed by a distributedKernelClientImpl, which is only
// used by the Gateway.
//
// Used by both the Gateway and Local Daemon components.
type KernelReplicaClient struct {
	*server.BaseServer
	scheduling.SessionManager
	client *server.AbstractServer

	// destMutex                 sync.Mutex
	id                               string
	replicaId                        int32
	persistentId                     string
	spec                             *proto.KernelSpec
	replicaSpec                      *proto.KernelReplicaSpec
	status                           jupyter.KernelStatus
	busyStatus                       string
	lastBStatusMsg                   *messaging.JupyterMessage
	iobroker                         *MessageBroker[scheduling.KernelReplica, *messaging.JupyterMessage, *messaging.JupyterFrames]
	shell                            *messaging.Socket                                  // Listener.
	iopub                            *messaging.Socket                                  // Listener.
	numResendAttempts                int                                                // Number of times to try resending a message before giving up.
	PodOrContainerName               string                                             // Name of the Pod or Container housing the associated distributed kernel replica container.
	nodeName                         string                                             // Name of the node that the Pod or Container is running on.
	ready                            bool                                               // True if the replica has registered and joined its SMR cluster. Only used by the internalCluster Gateway, not by the Local Daemon.
	yieldNextExecutionRequest        bool                                               // If true, then we will yield the next 'execute_request'.
	host                             scheduling.Host                                    // The host that the kernel replica is running on.
	hostId                           string                                             // The ID of the scheduling.Host that this KernelReplicaClient is running on. This field exists because we don't actually use scheduling.Host structs on Local Daemons.
	workloadId                       string                                             // workloadId is the ID of the workload associated with this kernel, if this kernel was created within a workload. This is populated after extracting the ID from the metadata frame of a Jupyter message.
	workloadIdSet                    bool                                               // workloadIdSet is a flag indicating whether workloadId has been assigned a "meaningful" value or not.
	trainingStartedAt                time.Time                                          // trainingStartedAt is the time at which the kernel associated with this client began actively training.
	idleStartedAt                    time.Time                                          // idleStartedAt is the time at which the kernel last began idling
	lastTrainingTimePrometheusUpdate time.Time                                          // lastTrainingTimePrometheusUpdate records the current time as the last instant in which we published an updated training time metric to Prometheus. We use this to determine how much more to increment the training time Prometheus metric when we stop training, since any additional training time since the last scheduled publish won't be pushed to Prometheus automatically by the publisher-goroutine.
	isTraining                       bool                                               // isTraining indicates whether the kernel replica associated with this client is actively training.
	pendingExecuteRequestIds         hashmap.HashMap[string, *messaging.JupyterMessage] // pendingExecuteRequestIds is a map from Jupyter message ID to the message, containing execute requests sent to this kernel (whose replies have not yet been received).
	receivedExecuteRequestReplies    hashmap.HashMap[string, *messaging.JupyterMessage] // receivedExecuteRequestReplies is a map from Jupyter message ID (of execute_request messages) to the responses.
	pendingExecuteRequestIdsMutex    sync.Mutex                                         // pendingExecuteRequestIdsMutex ensures atomic access to pendingExecuteRequestIds.
	pendingExecuteRequestCond        *sync.Cond                                         // Used to notify goroutines when the number of outstanding/pending "execute_request" messages reaches 0.
	trainingFinishedCond             *sync.Cond                                         // Used to notify the goroutine responsible for sending "execute_requests" to the kernel that the kernel has finished training.
	trainingFinishedMu               sync.Mutex                                         // Goes with the trainingFinishedCond field.
	submitRequestsOneAtATime         bool

	connectionRevalidationFailedCallback ConnectionRevalidationFailedCallback // Callback for when we try to forward a message to a kernel replica, don't get back any ACKs, and then fail to reconnect.

	// If we successfully reconnect to a kernel and then fail to send the message again, then we call this.
	resubmissionAfterSuccessfulRevalidationFailedCallback ResubmissionAfterSuccessfulRevalidationFailedCallback

	smrNodeReadyCallback SMRNodeReadyNotificationCallback
	smrNodeAddedCallback SMRNodeUpdatedNotificationCallback

	// If true, then this client exists on the internalCluster Gateway.
	// If false, then this client exists on the Local Daemon.
	//
	// To be clear, this indicates whether this client struct exists within the memory of the internalCluster Gateway.
	// This is NOT referring to whether the remote client (i.e., the client that this KernelClient is connected to) is on the cluster gateway or local daemon.
	isGatewayClient bool

	// The Container associated with this KernelReplicaClient.
	container scheduling.KernelContainer

	// prometheusManager is an interface that enables the recording of metrics observed by the KernelReplicaClient.
	messagingMetricsProvider server.MessagingMetricsProvider

	// Used to update the fields of the Cluster Gateway's GatewayStatistics struct atomically.
	// The Cluster Gateway locks modifications to the GatewayStatistics struct before calling whatever function
	// we pass to the statisticsUpdaterProvider.
	statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics))

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelReplicaClient creates a new KernelReplicaClient.
// The client will initialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
//
// If the proto.KernelReplicaSpec argument is nil, or the proto.KernelSpec field of the proto.KernelReplicaSpec
// argument is nil, then NewKernelReplicaClient will panic.
func NewKernelReplicaClient(ctx context.Context, spec *proto.KernelReplicaSpec, info *jupyter.ConnectionInfo, componentId string,
	numResendAttempts int, podOrContainerName string, nodeName string, smrNodeReadyCallback SMRNodeReadyNotificationCallback,
	smrNodeAddedCallback SMRNodeUpdatedNotificationCallback, messageAcknowledgementsEnabled bool, persistentId string, hostId string,
	host scheduling.Host, nodeType metrics.NodeType, shouldAckMessages bool, isGatewayClient bool, debugMode bool,
	messagingMetricsProvider server.MessagingMetricsProvider, connRevalFailedCallback ConnectionRevalidationFailedCallback,
	resubmissionAfterSuccessfulRevalidationFailedCallback ResubmissionAfterSuccessfulRevalidationFailedCallback,
	statisticsUpdaterProvider func(func(statistics *metrics.ClusterStatistics)), submitRequestsOneAtATime bool) *KernelReplicaClient {

	// Validate that the `spec` argument is non-nil.
	if spec == nil {
		log.Fatalln(utils.RedStyle.Render("Cannot create new KernelClient, as spec is nil."))
	}

	// Validate that the `Kernel` field of the `spec` argument is non-nil.
	if spec.Kernel == nil {
		log.Fatalf(utils.RedStyle.Render(
			"Cannot create new KernelClient for replica %d of unknown kernel, as spec.Kernel is nil.\n"),
			spec.ReplicaId)
	}

	client := &KernelReplicaClient{
		id:                                   spec.Kernel.Id,
		persistentId:                         persistentId,
		replicaId:                            spec.ReplicaId,
		spec:                                 spec.Kernel,
		replicaSpec:                          spec,
		messagingMetricsProvider:             messagingMetricsProvider,
		PodOrContainerName:                   podOrContainerName,
		nodeName:                             nodeName,
		smrNodeReadyCallback:                 smrNodeReadyCallback,
		smrNodeAddedCallback:                 smrNodeAddedCallback,
		numResendAttempts:                    numResendAttempts,
		yieldNextExecutionRequest:            false,
		host:                                 host,
		hostId:                               hostId,
		pendingExecuteRequestIds:             hashmap.NewCornelkMap[string, *messaging.JupyterMessage](64),
		receivedExecuteRequestReplies:        hashmap.NewCornelkMap[string, *messaging.JupyterMessage](64),
		isGatewayClient:                      isGatewayClient,
		connectionRevalidationFailedCallback: connRevalFailedCallback,
		statisticsUpdaterProvider:            statisticsUpdaterProvider,
		submitRequestsOneAtATime:             submitRequestsOneAtATime,
		resubmissionAfterSuccessfulRevalidationFailedCallback: resubmissionAfterSuccessfulRevalidationFailedCallback,
		client: server.New(ctx, info, nodeType, func(s *server.AbstractServer) {
			var remoteComponentName string
			if isGatewayClient {
				remoteComponentName = "LD"
			} else {
				remoteComponentName = "Kernel"
			}

			// We do not set handlers of the sockets here. So no server routine will be started on dialing.
			s.Sockets.Control = messaging.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.ControlPort, messaging.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Ctrl[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.Shell = messaging.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.ShellPort, messaging.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Shell[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.Stdin = messaging.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.StdinPort, messaging.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Stdin[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.HB = messaging.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.HBPort, messaging.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-HB[%s]", remoteComponentName, spec.Kernel.Id))
			s.ReconnectOnAckFailure = true
			s.PrependId = false
			s.ComponentId = componentId
			s.MessageAcknowledgementsEnabled = messageAcknowledgementsEnabled
			s.StatisticsAndMetricsProvider = messagingMetricsProvider
			s.Name = fmt.Sprintf("Kernel-%s", spec.Kernel.Id)
			s.DebugMode = debugMode

			/* Kernel clients should ACK messages that they're forwarding when the local kernel client lives on the Local Daemon. */
			s.ShouldAckMessages = shouldAckMessages
			// s.Sockets.Ack = messaging.NewSocket(Socket: zmq4.NewReq(s.Ctx), Port: info.AckPort}
			// IOPub is lazily initialized for different subclasses.
			if spec.ReplicaId == 0 {
				config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Kernel.Id))
			} else {
				config.InitLogger(&s.Log, fmt.Sprintf("Replica %s:%d ", spec.Kernel.Id, spec.ReplicaId))
			}
		}),
		status: jupyter.KernelStatusInitializing,
	}
	client.BaseServer = client.client.Server()
	client.SessionManager = NewSessionManager(spec.Kernel.Session)
	client.log = client.client.Log

	client.log.Debug("Created new Kernel Client with spec %v, connection info %v.", spec, info)

	client.trainingFinishedCond = sync.NewCond(&client.trainingFinishedMu)
	client.pendingExecuteRequestCond = sync.NewCond(&client.pendingExecuteRequestIdsMutex)

	return client
}

func (c *KernelReplicaClient) Host() scheduling.Host {
	if c.container == nil {
		return nil
	}

	return c.container.Host()
}

func (c *KernelReplicaClient) Container() scheduling.KernelContainer {
	return c.container
}

func (c *KernelReplicaClient) SetContainer(container scheduling.KernelContainer) {
	c.container = container
}

// IsTraining returns a bool indicating whether the kernel associated with this client is actively training.
//
// IsTraining checks the value of the isTraining field atomically.
func (c *KernelReplicaClient) IsTraining() bool {
	c.trainingFinishedMu.Lock()
	defer c.trainingFinishedMu.Unlock()

	return c.isTraining
}

// WaitForTrainingToStop blocks until the kernel stops training (via the KernelReplicaClient's sync.Cond variable).
//
// If the kernel is already not training when WaitForTrainingToStop is called, then WaitForTrainingToStop will
// return immediately.
func (c *KernelReplicaClient) WaitForTrainingToStop() {
	gid := goid.Get()

	// The trainingFinishedCond field of the Kernel uses the trainingFinishedMu mutex.
	c.trainingFinishedMu.Lock()
	defer c.trainingFinishedMu.Unlock()

	// If the kernel is not training right now, then we can return immediately.
	if !c.isTraining {
		return
	}

	// Block until we're woken up and notified that the kernel is done training.
	for c.isTraining {
		c.log.Debug("[gid=%d] Replica %d of kernel %s is currently training. Waiting for training to stop.", gid, c.replicaId, c.id)
		c.trainingFinishedCond.Wait()
	}
}

// UpdateResourceSpec updates the ResourceSpec of the KernelReplica, the UserSession of the KernelReplica, and the
// KernelContainer of the KernelReplica.
//
// It also ensures that the updated ResourceSpec is propagated to the Host of the KernelContainer / KernelReplica.
//
// UpdateResourceSpec should only be used to update the ResourceSpec of an existing KernelReplica. When
// instantiating/initializing (the ResourceSpec of) a new KernelReplica, you should use the InitializeResourceSpec
// method instead of UpdateResourceSpec.
//
// An error is returned if any of the resource quantities in the provided types.Spec are negative.
//
// On success, nil is returned.
//
// Note for internal usage: this method is thread safe. Do not call this method if the lock for the kernel
// is already held. If the lock is already held, then call the unsafeUpdateResourceSpec method instead.
func (c *KernelReplicaClient) UpdateResourceSpec(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.unsafeUpdateResourceSpec(newSpec, tx)
}

// UpdateResourceSpec updates the resource spec of the target KernelReplicaClient to the resource
// quantities of the specified types.Spec.
//
// An error is returned if any of the resource quantities in the provided types.Spec are negative.
//
// On success, nil is returned.
//
// Note: this method is thread safe. Do not call this method if the lock for the kernel is already held.
func (c *KernelReplicaClient) unsafeUpdateResourceSpec(newSpec types.Spec, tx *transaction.CoordinatedTransaction) error {
	if newSpec.GPU() < 0 || newSpec.CPU() < 0 || newSpec.VRAM() < 0 || newSpec.MemoryMB() < 0 {
		err := fmt.Errorf("%w: %s", ErrInvalidResourceSpec, newSpec.String())
		return err
	}

	oldSpec := c.ResourceSpec()
	c.log.Debug("Updating ResourceSpec of replica %d of kernel %s now. Changing from %s to %s.",
		c.replicaId, c.id, c.spec.ResourceSpec.String(), newSpec.String())

	specAsDecimalSpec := types.ToDecimalSpec(newSpec)
	if c.Container() != nil { // Will be nil in local daemon.
		container := c.Container()

		var err error
		if tx != nil {
			err = container.Host().AdjustKernelResourceRequestCoordinated(newSpec, oldSpec, container, tx)
		} else {
			err = container.Host().AdjustKernelResourceRequest(newSpec, oldSpec, container)
		}

		if err != nil {
			return err
		}

		container.UpdateResourceSpec(specAsDecimalSpec)
	} else if tx != nil {
		c.log.Warn("Replica %d of kernel %s does not have a non-nil container, but coordinated tx is non-nil...",
			c.replicaId, c.id)
	}

	c.spec.ResourceSpec.Gpu = int32(newSpec.GPU())
	c.spec.ResourceSpec.Cpu = int32(newSpec.CPU())
	c.spec.ResourceSpec.Vram = float32(newSpec.VRAM())
	c.spec.ResourceSpec.Memory = float32(newSpec.MemoryMB())

	// This part should be redundant because Kernel is a pointer to c.spec, right?
	c.replicaSpec.Kernel.ResourceSpec.Gpu = int32(newSpec.GPU())
	c.replicaSpec.Kernel.ResourceSpec.Cpu = int32(newSpec.CPU())
	c.replicaSpec.Kernel.ResourceSpec.Vram = float32(newSpec.VRAM())
	c.replicaSpec.Kernel.ResourceSpec.Memory = float32(newSpec.MemoryMB())

	c.log.Debug("Successfully updated ResourceSpec of replica %d of kernel %s from %s to %s.",
		c.replicaId, c.id, oldSpec.String(), newSpec.String())

	return nil
}

// WaitForPendingExecuteRequests blocks until all outstanding/pending "execute_request" messages sent to the kernel
// have received their "execute_reply" response.
//
// If there are no outstanding/pending "execute_request" messages WaitForRepliesToPendingExecuteRequests is called, then
// WaitForRepliesToPendingExecuteRequests will return immediately.
func (c *KernelReplicaClient) WaitForPendingExecuteRequests() {
	gid := goid.Get()

	// The trainingFinishedCond field of the Kernel uses the trainingFinishedMu mutex.
	c.pendingExecuteRequestIdsMutex.Lock()
	defer c.pendingExecuteRequestIdsMutex.Unlock()

	// If the kernel is not training right now, then we can return immediately.
	if c.pendingExecuteRequestIds.Len() == 0 {
		return
	}

	// Block until we're woken up and notified that the kernel is done training.
	for c.pendingExecuteRequestIds.Len() > 0 {
		c.log.Debug("[gid=%d] Replica %d of kernel %s currently has %d outstanding \"execute_request\" message(s). "+
			"Waiting for \"execute_reply\" responses to be received.", gid, c.replicaId, c.id, c.pendingExecuteRequestIds.Len())
		c.pendingExecuteRequestIds.Range(func(s string, message *messaging.JupyterMessage) (contd bool) {
			c.log.Debug("Waiting on pending \"execute_request\" message \"%s\".", s)
			return true
		})
		c.pendingExecuteRequestCond.Wait()
	}
}

// IsSomeReplicaTraining returns a bool indicating  whether any replica of the kernel associated with this client is
// actively training, even if it is not the specific replica associated with this client.
//func (c *Kernel) IsSomeReplicaTraining() bool {
//	return c.isSomeReplicaTraining
//}

// SetLastTrainingTimePrometheusUpdate records the current time as the last instant in which we published an updated
// training time metric to Prometheus. We use this to determine how much more to increment the training time Prom
// metric when we stop training, since any additional training time since the last scheduled publish won't be pushed
// to Prometheus automatically by the publisher-goroutine.
func (c *KernelReplicaClient) SetLastTrainingTimePrometheusUpdate() {
	c.lastTrainingTimePrometheusUpdate = time.Now()
}

// LastTrainingTimePrometheusUpdate returns the last instant in which we published an updated training time metric to
// Prometheus. We use this to determine how much more to increment the training time Prometheus metric when we stop
// training, since any additional training time since the last scheduled publish won't be pushed to Prometheus
// automatically by the publisher-goroutine.
func (c *KernelReplicaClient) LastTrainingTimePrometheusUpdate() time.Time {
	return c.lastTrainingTimePrometheusUpdate
}

// KernelStartedTraining should be called when the kernel associated with this client begins actively training.
//
// In the Local Daemon, this is called in the handleSMRLeadTask method.
//
// In the internalCluster Gateway, this is called in the handleSmrLeadTaskMessage method of DistributedKernelClient.
func (c *KernelReplicaClient) KernelStartedTraining() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// It's possible for isTraining to change values after we check here, but if it does, then that's fine.
	// unsafeKernelStoppedTraining will just return immediately without an error if isTraining is false.
	if c.IsTraining() {
		c.log.Warn("Replica %d of kernel %s is already training as of %v. Ending current training now. "+
			"Will discard future \"execute_reply\" message if we do end up receiving it...", c.replicaId, c.id, c.trainingStartedAt)

		// We already locked the kernel above, so we can call the unsafe method directly here.
		err := c.unsafeKernelStoppedTraining("Need to start next training event, so current training event must be stopped first.")
		if err != nil {
			c.log.Error("Couldn't cleanly stop training for replica %d of kernel %s: %v", c.replicaId, c.id, err)
			return err
		}
	}

	c.trainingFinishedMu.Lock()
	c.isTraining = true
	c.trainingFinishedMu.Unlock()
	c.trainingStartedAt = time.Now()

	// The following code is only executed within the internalCluster Gateway.
	container := c.Container()
	if container != nil { // Container will be nil on Local Daemons; they don't track resources this way.
		p := container.Session().SessionStartedTraining(container)
		if err := p.Error(); err != nil {
			c.log.Error("Failed to start training for session %s: %v", container.Session().ID(), err)
			return err
		}
	}

	c.log.Debug(utils.PurpleStyle.Render("Replica %d of kernel \"%s\" has STARTED training."), c.replicaId, c.id)

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			stats.NumTrainingSessions += 1
			stats.CumulativeSessionIdleTime += time.Since(c.idleStartedAt).Seconds()

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.KernelTrainingStarted,
				KernelId:            c.id,
				ReplicaId:           c.replicaId,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Metadata: map[string]interface{}{
					"resource_request": container.ResourceSpec().ToMap(),
				},
			})
		})
	}

	return nil
}

// NumPendingExecuteRequests returns the number of "execute_request" ZMQ messages that have been sent/forwarded to
// the kernel, for which we have not yet received a response (i.e., an "execute_reply" message).
func (c *KernelReplicaClient) NumPendingExecuteRequests() int {
	c.pendingExecuteRequestIdsMutex.Lock()
	defer c.pendingExecuteRequestIdsMutex.Unlock()

	return c.pendingExecuteRequestIds.Len()
}

// SendingExecuteRequest records that an "execute_request" message has been sent to the kernel.
//
// SendingExecuteRequest will panic if the given messaging.JupyterMessage is not an "execute_request"
// message or a "yield_request" message.
func (c *KernelReplicaClient) SendingExecuteRequest(msg *messaging.JupyterMessage) {
	c.pendingExecuteRequestIdsMutex.Lock()
	defer c.pendingExecuteRequestIdsMutex.Unlock()

	if msg.JupyterMessageType() != messaging.ShellExecuteRequest && msg.JupyterMessageType() != messaging.ShellYieldRequest {
		// This really shouldn't happen.
		c.log.Error(utils.RedStyle.Render("[ERROR] Invalid message type: \"%s\"\n"), msg.JupyterMessageType())
		return
	}

	c.pendingExecuteRequestIds.Store(msg.JupyterMessageId(), msg)

	numOutstandingRequests := c.pendingExecuteRequestIds.Len()
	if numOutstandingRequests > 1 {
		// Print a warning, as we shouldn't be sending concurrent execute requests to the kernel, and we just recorded
		// that an execute request has been resolved, yet there is still at least one outstanding "execute_request"...
		c.log.Warn(utils.OrangeStyle.Render("Recorded that \"%s\" message \"%s\" has been (or is about to be) sent to kernel %s. "+
			"Updated number of outstanding \"execute_request\" msg: %d."), msg.JupyterMessageType(),
			msg.JupyterMessageId(), c.id, c.pendingExecuteRequestIds.Len())
	} else {
		c.log.Debug("Recorded that \"%s\" message \"%s\" has been (or is about to be) sent to kernel %s. "+
			"Updated number of outstanding \"execute_request\" msg: %d.", msg.JupyterMessageType(),
			msg.JupyterMessageId(), c.id, c.pendingExecuteRequestIds.Len())
	}
}

// ReceivedExecuteReply is used to record that an "execute_reply" message has been received from the kernel.
//
// ReceivedExecuteReply will panic if the given messaging.JupyterMessage is not an "execute_reply" message.
//
// If there is no associated "execute_request" or "yield_request" message to match against, then this will just
// print an error message but will not panic (for now).
func (c *KernelReplicaClient) ReceivedExecuteReply(msg *messaging.JupyterMessage, own bool) {
	c.pendingExecuteRequestIdsMutex.Lock()
	defer c.pendingExecuteRequestIdsMutex.Unlock()

	if msg.JupyterMessageType() != messaging.ShellExecuteReply {
		// This really shouldn't happen.
		c.log.Error(utils.RedStyle.Render("Invalid message type: \"%s\"\n"), msg.JupyterMessageType())
		return
	}

	executeRequestId := msg.JupyterParentMessageId()

	_, found := c.pendingExecuteRequestIds.LoadAndDelete(executeRequestId)

	// If this is the replica's own response, then we'll keep track of that.
	if own {
		c.receivedExecuteRequestReplies.Store(executeRequestId, msg)
	}

	numOutstandingRequests := c.pendingExecuteRequestIds.Len()
	if numOutstandingRequests == 0 {
		c.log.Debug("\"execute_reply\" for message request \"%s\" received from kernel %s. Outstanding \"execute_request\" messages: %d.",
			executeRequestId, c.id, c.pendingExecuteRequestIds.Len())

		if found {
			// Notify any waiters that there are no more outstanding requests.
			c.pendingExecuteRequestCond.Broadcast()
		} else {
			c.log.Debug("Because there was no matching \"execute_request\" or \"yield_request\" request matching " +
				"the given \"execute_reply\", we will not be broadcasting on the associated condition variable...")
		}
	} else {
		// Print a warning, as we shouldn't be sending concurrent execute requests to the kernel, and we just recorded
		// that an execute request has been resolved, yet there is still at least one outstanding "execute_request"...
		c.log.Debug("\"execute_reply\" for message request \"%s\" received from kernel %s. Outstanding \"execute_request\" messages: %d.",
			executeRequestId, c.id, c.pendingExecuteRequestIds.Len())
	}
}

// unsafeKernelStoppedTraining does the work of KernelStoppedTraining without acquiring the KernelReplicaClient's
// mutex first (hence "unsafe"). This function DOES acquire the mutex that controls access to the isTraining field
// of the KernelReplicaClient, however. It may also acquire other mutexes -- just not the "main" mutex of the
// KernelReplicaClient struct.
//
// If the kernel is already not training, then this method just returns immediately (without an error).
func (c *KernelReplicaClient) unsafeKernelStoppedTraining(reason string) error {
	c.trainingFinishedMu.Lock()
	if !c.isTraining {
		c.log.Warn("Cannot stop training; already not training.")
		c.trainingFinishedMu.Unlock()
		return nil
	}

	c.isTraining = false
	c.idleStartedAt = time.Now()
	// Notify everybody that the kernel has finished training.
	c.trainingFinishedCond.Broadcast()
	c.trainingFinishedMu.Unlock()

	// The following code executes only on the internalCluster Gateway.
	//
	// If the Container is actively-training, then we need to call SessionStoppedTraining
	// before removing it so that the resources are all returned appropriately.
	container := c.Container()
	if container != nil {
		p := container.Session().SessionStoppedTraining(reason)
		if err := p.Error(); err != nil {
			c.log.Error("Failed to stop training on scheduling.Container %s-%d during replica removal because: %v",
				c.ID(), c.ReplicaID(), err)
			return err
		}
	}

	c.log.Debug(utils.LightPurpleStyle.Render("Replica %d of kernel \"%s\" has STOPPED training after %v. Reason: %s"),
		c.replicaId, c.id, time.Since(c.trainingStartedAt), reason)

	if c.statisticsUpdaterProvider != nil {
		c.statisticsUpdaterProvider(func(stats *metrics.ClusterStatistics) {
			stats.NumTrainingSessions -= 1
			stats.CumulativeSessionTrainingTime += time.Since(c.trainingStartedAt).Seconds()

			var metadata map[string]interface{}
			if container != nil {
				metadata = map[string]interface{}{
					"resource_request": container.ResourceSpec().ToMap(),
				}
			}

			now := time.Now()
			stats.ClusterEvents = append(stats.ClusterEvents, &metrics.ClusterEvent{
				EventId:             uuid.NewString(),
				Name:                metrics.KernelTrainingEnded,
				KernelId:            c.id,
				ReplicaId:           c.replicaId,
				Timestamp:           now,
				TimestampUnixMillis: now.UnixMilli(),
				Duration:            time.Since(c.trainingStartedAt),
				DurationMillis:      time.Since(c.trainingStartedAt).Milliseconds(),
				Metadata:            metadata,
			})
		})
	}

	return nil
}

// KernelStoppedTraining should be called when the kernel associated with this client stops actively training.
func (c *KernelReplicaClient) KernelStoppedTraining(reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.unsafeKernelStoppedTraining(reason)
}

// TrainingStartedAt returns the time at which the kernel associated with this client began actively training.
func (c *KernelReplicaClient) TrainingStartedAt() time.Time {
	return c.trainingStartedAt
}

// Recreate and return the kernel's control socket.
// This reuses the handler on the existing/previous control socket.
func (c *KernelReplicaClient) recreateControlSocket() *messaging.Socket {
	handler, err := c.closeSocket(messaging.ControlMessage)
	if err != nil {
		c.log.Error("Could not find control socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = messaging.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Ctrl[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Ctrl[%s]", c.id)
	}

	newSocket := messaging.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.ControlPort, messaging.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", c.id), remoteName, handler)
	c.client.Sockets.Control = newSocket
	c.client.Sockets.All[messaging.ControlMessage] = newSocket
	return newSocket
}

// WorkloadId returns the ID of the workload associated with this kernel, if this kernel was created within a workload.
// This is populated after extracting the ID from the metadata frame of a Jupyter message.
func (c *KernelReplicaClient) WorkloadId() string {
	return c.workloadId
}

// SetWorkloadId sets the WorkloadId of the KernelReplicaClient.
func (c *KernelReplicaClient) SetWorkloadId(workloadId string) {
	if c.workloadIdSet {
		c.log.Warn("Workload ID has already been set to \"%s\". Will replace it with (possibly identical) new ID: \"%s\"", c.workloadId, workloadId)
	}

	c.workloadId = workloadId
}

// WorkloadIdSet returns a flag indicating whether the KernelReplicaClient's workloadId has been assigned a "meaningful" value or not.
func (c *KernelReplicaClient) WorkloadIdSet() bool {
	return c.workloadIdSet
}

// Recreate and return the kernel's shell socket.
// This reuses the handler on the existing/previous shell socket.
func (c *KernelReplicaClient) recreateShellSocket() *messaging.Socket {
	handler, err := c.closeSocket(messaging.ShellMessage)
	if err != nil {
		c.log.Error("Could not find shell socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = messaging.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Shell[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Shell[%s]", c.id)
	}

	newSocket := messaging.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.ShellPort, messaging.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", c.id), remoteName, handler)
	c.client.Sockets.Shell = newSocket
	c.client.Sockets.All[messaging.ShellMessage] = newSocket
	return newSocket
}

func (c *KernelReplicaClient) ShouldAckMessages() bool {
	return c.client.ShouldAckMessages
}

// Recreate and return the kernel's stdin socket.
// This reuses the handler on the existing/previous stdin socket.
func (c *KernelReplicaClient) recreateStdinSocket() *messaging.Socket {
	handler, err := c.closeSocket(messaging.StdinMessage)
	if err != nil {
		c.log.Error("Could not find stdin socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = messaging.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Stdin[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Stdin[%s]", c.id)
	}

	newSocket := messaging.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.StdinPort, messaging.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", c.id), remoteName, handler)
	c.client.Sockets.Stdin = newSocket
	c.client.Sockets.All[messaging.StdinMessage] = newSocket
	return newSocket
}

// Recreate and return the kernel's heartbeat socket.
// This reuses the handler on the existing/previous heartbeat socket.
func (c *KernelReplicaClient) recreateHeartbeatSocket() *messaging.Socket {
	handler, err := c.closeSocket(messaging.HBMessage)
	if err != nil {
		c.log.Error("Could not find heartbeat socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = messaging.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-HB[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-HB[%s]", c.id)
	}

	newSocket := messaging.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.HBPort, messaging.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", c.id), remoteName, handler)
	c.client.Sockets.HB = newSocket
	c.client.Sockets.All[messaging.HBMessage] = newSocket
	return newSocket
}

// Close the socket of the specified type, returning the handler set for that socket.
func (c *KernelReplicaClient) closeSocket(typ messaging.MessageType) (messaging.MessageHandler, error) {
	if c.client != nil && c.client.Sockets.All[typ] != nil {
		oldSocket := c.client.Sockets.All[typ]
		handler := oldSocket.Handler

		if atomic.LoadInt32(&oldSocket.Serving) == 1 {
			c.log.Debug("Sending 'stop-serving' notification to %s socket.", typ.String())
			oldSocket.StopServingChan <- struct{}{}
			c.log.Debug("Sent 'stop-serving' notification to %s socket.", typ.String())
		}

		err := oldSocket.Close()
		if err != nil {
			// Print the error, but that's all. We're recreating the socket anyway.
			c.log.Warn("Error while closing %s socket: %v", typ.String(), err)
		}

		return handler, nil
	} else {
		return nil, messaging.ErrSocketNotAvailable
	}
}

// This is called when the replica ID is set/changed, so that the logger's prefix reflects the replica ID.
func (c *KernelReplicaClient) updateLogPrefix() {
	if c.client.Log == nil {
		return
	}

	c.client.Log.(*logger.ColorLogger).Prefix = fmt.Sprintf("Replica %s:%d ", c.id, c.replicaId)
}

// GetPodOrContainerName returns the name of the Kubernetes Pod hosting the replica.
func (c *KernelReplicaClient) GetPodOrContainerName() string {
	return c.PodOrContainerName
}

func (c *KernelReplicaClient) SetPodOrContainerName(name string) {
	c.PodOrContainerName = name
}

func (c *KernelReplicaClient) SetNodeName(name string) {
	c.nodeName = name
}

// NodeName returns the name of the node that the Pod is running on.
func (c *KernelReplicaClient) NodeName() string {
	return c.nodeName
}

func (c *KernelReplicaClient) ShellListenPort() int {
	return c.client.GetSocketPort(messaging.ShellMessage)
}

func (c *KernelReplicaClient) IOPubListenPort() int {
	return c.client.GetSocketPort(messaging.IOMessage)
}

// YieldNextExecutionRequest takes note that we should yield the next execution request.
func (c *KernelReplicaClient) YieldNextExecutionRequest() {
	c.yieldNextExecutionRequest = true
}

// YieldedNextExecutionRequest is called after successfully yielding the next execution request.
// This flips the KernelReplicaClient::yieldNextExecutionRequest
// flag to false so that the kernel replica isn't forced to yield future requests.
func (c *KernelReplicaClient) YieldedNextExecutionRequest() {
	c.yieldNextExecutionRequest = false
}

func (c *KernelReplicaClient) SupposedToYieldNextExecutionRequest() bool {
	return c.yieldNextExecutionRequest
}

// ID returns the kernel ID.
func (c *KernelReplicaClient) ID() string {
	return c.id
}

func (c *KernelReplicaClient) SourceKernelID() string {
	return c.id
}

// ReplicaID returns the replica ID.
func (c *KernelReplicaClient) ReplicaID() int32 {
	return c.replicaId
}

func (c *KernelReplicaClient) SetReplicaID(replicaId int32) {
	c.replicaId = replicaId

	c.updateLogPrefix()
}

// SetPersistentID sets the value of the persistentId field.
// This will panic if the persistentId has already been set to something other than the empty string.
func (c *KernelReplicaClient) SetPersistentID(persistentId string) {
	if c.persistentId != "" {
		panic(fmt.Sprintf("Cannot set persistent ID of kernel %s. Persistent ID already set to value: '%s'", c.id, c.persistentId))
	}
	c.persistentId = persistentId
}

// PersistentID returns the persistent ID.
func (c *KernelReplicaClient) PersistentID() string {
	return c.persistentId
}

// ResourceSpec returns the resource spec
func (c *KernelReplicaClient) ResourceSpec() *types.DecimalSpec {
	if c.spec == nil {
		log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid (i.e., non-nil) spec...\n"),
			c.replicaId, c.id)
	}

	return c.spec.DecimalSpecFromKernelSpec()
}

// InitializeResourceSpec sets the ResourceSpec of the KernelReplica.
//
// This does NOT propagate the updated spec to any UserSession or KernelContainer or Host.
// As such, SetReplicaSpec should only be called when instantiating/initializing a new KernelReplica.
//
// If you wish to update the ResourceSpec of an existing KernelReplica, then you should use the
// UpdateResourceSpec method.
func (c *KernelReplicaClient) InitializeResourceSpec(spec *proto.ResourceSpec) {
	c.spec.ResourceSpec = spec
	c.replicaSpec.Kernel.ResourceSpec = spec // Might/should be redundant bc Kernel is a pointer to c.spec, right?
}

// KernelSpec returns the kernel spec.
func (c *KernelReplicaClient) KernelSpec() *proto.KernelSpec {
	return c.spec
}

func (c *KernelReplicaClient) KernelReplicaSpec() *proto.KernelReplicaSpec {
	return c.replicaSpec
}

// Address returns the address of the kernel.
func (c *KernelReplicaClient) Address() string {
	return c.client.Meta.IP
}

// String returns a string representation of the client.
func (c *KernelReplicaClient) String() string {
	if c.replicaId == 0 {
		return fmt.Sprintf("kernel(%s)", c.id)
	} else {
		return fmt.Sprintf("replica(%s:%d)", c.id, c.replicaId)
	}
}

// IsReady returns true if the replica has registered and joined its SMR cluster.
// Only used by the internalCluster Gateway, not by the Local Daemon.
func (c *KernelReplicaClient) IsReady() bool {
	return c.ready
}

// HostId returns the ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
func (c *KernelReplicaClient) HostId() string {
	return c.hostId
}

// SetReady designates the replica as ready.
// SetReady is only used by the internalCluster Gateway, not by the Local Daemon.
func (c *KernelReplicaClient) SetReady() {
	c.log.Debug("Kernel %s-%d has been designated as ready.", c.id, c.replicaId)
	c.ready = true
}

// Socket returns the serve socket the kernel is listening on.
func (c *KernelReplicaClient) Socket(typ messaging.MessageType) *messaging.Socket {
	switch typ {
	case messaging.IOMessage:
		return c.iopub
	case messaging.ShellMessage:
		return c.shell
	default:
		return nil
	}
}

// ConnectionInfo returns the connection info.
func (c *KernelReplicaClient) ConnectionInfo() *jupyter.ConnectionInfo {
	return c.client.Meta
}

// Status returns the kernel status.
func (c *KernelReplicaClient) Status() jupyter.KernelStatus {
	return c.status
}

// BusyStatus returns the kernel busy status.
func (c *KernelReplicaClient) BusyStatus() (string, *messaging.JupyterMessage) {
	return c.busyStatus, c.lastBStatusMsg
}

// BindSession binds a session ID to the client.
func (c *KernelReplicaClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	c.log.Info("Binded session %s to kernel client", sess)
}

// ReconnectSocket recreates and redials a particular socket.
func (c *KernelReplicaClient) ReconnectSocket(typ messaging.MessageType) (*messaging.Socket, error) {
	var socket *messaging.Socket

	c.log.Debug("Recreating %s socket.", typ.String())
	switch typ {
	case messaging.ControlMessage:
		{
			socket = c.recreateControlSocket()
		}
	case messaging.ShellMessage:
		{
			socket = c.recreateShellSocket()
		}
	case messaging.HBMessage:
		{
			socket = c.recreateStdinSocket()
		}
	case messaging.StdinMessage:
		{
			socket = c.recreateHeartbeatSocket()
		}
	default:
		return nil, fmt.Errorf("invalid socket type: \"%d\"", typ)
	}

	timer := time.NewTimer(0)

	for {
		select {
		case <-c.client.Ctx.Done():
			c.log.Debug("Could not reconnect %s socket; kernel has exited.", typ.String())
			c.status = jupyter.KernelStatusExited
			return nil, jupyter.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()

			c.log.Debug("Attempting to reconnect on %s socket.", typ.String())
			if err := c.dial(socket); err != nil {
				c.client.Log.Error("Failed to reconnect %v socket: %v", typ, err)
				_ = c.Close()
				c.status = jupyter.KernelStatusExited
				c.mu.Unlock()
				return nil, err
			}

			c.mu.Unlock()
			return socket, nil
		}
	}
}

// Validate validates the kernel connections. If IOPub has been initialized, it will also validate the IOPub connection and start the IOPub forwarder.
func (c *KernelReplicaClient) Validate() error {
	if c.status >= jupyter.KernelStatusRunning { /* If we're already connected, then just return, unless `forceReconnect` is true */
		return nil
	}

	timer := time.NewTimer(0)

	for {
		select {
		case <-c.client.Ctx.Done():
			c.status = jupyter.KernelStatusExited
			return jupyter.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()
			// Wait for heartbeat connection.
			if err := c.dial(nil, c.client.Sockets.HB); err != nil {
				c.client.Log.Warn("Failed to dial heartbeat (%v:%v), retrying...", c.client.Sockets.HB.Addr(), err)
				timer.Reset(heartbeatInterval)
				c.mu.Unlock()
				break // break select
			}
			c.client.Log.Debug("Heartbeat connected")

			// Dial all other sockets.
			// If IO socket is set previously and has not been dialed because kernel is not ready, then dial it now.
			if err := c.dial(c.client.Sockets.All[messaging.HBMessage+1:]...); err != nil {
				c.client.Log.Error("Failed to dial at least one socket: %v", err)
				_ = c.Close()
				c.status = jupyter.KernelStatusExited
				c.mu.Unlock()
				return err
			}

			c.status = jupyter.KernelStatusRunning
			c.mu.Unlock()
			return nil
		}
	}
}

func (c *KernelReplicaClient) InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*messaging.Socket, error) {
	c.log.Debug("Initializing shell forwarder for kernel client.")

	shell := messaging.NewSocket(zmq4.NewRouter(c.client.Ctx), 0, messaging.ShellMessage, fmt.Sprintf("K-Router-ShellForwrder[%s]", c.id))
	if err := c.client.Listen(shell); err != nil {
		return nil, err
	}

	c.shell = shell
	go c.client.Serve(c, shell, func(srv messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
		msg.AddDestinationId(c.id)
		return handler(c, typ, msg)
	})

	return shell, nil
}

// func (c *Kernel) Unlock() {
// 	c.destMutex.Unlock()
// }

// func (c *Kernel) Lock() {
// 	c.destMutex.Lock()
// }

// InitializeIOForwarder initializes the IOPub serving.
// Returns Pub socket, Sub socket, error.
func (c *KernelReplicaClient) InitializeIOForwarder() (*messaging.Socket, error) {
	iopub := messaging.NewSocket(zmq4.NewPub(c.client.Ctx), 0, messaging.IOMessage, fmt.Sprintf("K-Pub-IOForwrder[%s]", c.id))

	c.log.Debug("Created ZeroMQ PUB socket with port %d.", iopub.Port)

	if err := c.client.Listen(iopub); err != nil {
		return nil, err
	}

	c.iopub = iopub
	c.iobroker = NewMessageBroker[scheduling.KernelReplica](ExtractIOTopicFrame)
	c.iobroker.Subscribe(MessageBrokerAllTopics, c.forwardIOMessage) // Default to forward all messages.
	c.iobroker.Subscribe(messaging.IOTopicStatus, c.HandleIOKernelStatus)
	c.iobroker.Subscribe(messaging.IOTopicSMRReady, c.handleIOKernelSMRReady)
	c.iobroker.Subscribe(messaging.IOTopicSMRNodeAdded, c.handleIOKernelSMRNodeAdded)
	return iopub, nil
}

// AddIOHandler adds a handler for a specific IOPub topic.
// The handler should return ErrStopPropagation to avoid msg being forwarded to the client.
func (c *KernelReplicaClient) AddIOHandler(topic string, handler scheduling.MessageBrokerHandler[scheduling.KernelReplica, *messaging.JupyterFrames, *messaging.JupyterMessage]) error {
	if c.iobroker == nil {
		return ErrIOPubNotStarted
	}
	c.iobroker.Subscribe(topic, handler)
	return nil
}

// RequestWithHandler sends a request and handles the response.
func (c *KernelReplicaClient) RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error {
	return c.RequestWithHandlerAndWaitOptionGetter(ctx, typ, msg, handler, c.getWaitResponseOption, done)
}

func (c *KernelReplicaClient) RequestWithHandlerAndWaitOptionGetter(parentContext context.Context, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, getOption server.WaitResponseOptionGetter, done func()) error {
	if c.status < jupyter.KernelStatusRunning {
		return jupyter.ErrKernelNotReady
	}

	jupyterMsgTyp := msg.JupyterMessageType()
	if jupyterMsgTyp == messaging.ShellExecuteRequest || jupyterMsgTyp == messaging.ShellYieldRequest {
		// This ensures that we send "execute_request" messages one-at-a-time.
		// We wait until any pending "execute_request" messages receive an "execute_reply"
		// response before we can forward this next "execute_request".
		if c.submitRequestsOneAtATime {
			c.WaitForPendingExecuteRequests()
		}

		c.SendingExecuteRequest(msg)
	}

	// Use a default "done" handler.
	if done == nil {
		done = messaging.DefaultDoneHandler
	}

	socket := c.client.Sockets.All[typ]
	if socket == nil {
		return messaging.ErrSocketNotAvailable
	}

	wrappedHandler := func(server messaging.JupyterServerInfo, respType messaging.MessageType, respMsg *messaging.JupyterMessage) (err error) {
		// Kernel frame is automatically removed.
		if handler != nil {
			err = handler(server.(*KernelReplicaClient), respType, respMsg)
		}
		return err
	}

	// If we're actively training, or if there are pending "execute_request" messages (i.e., "execute_request" messages
	// that we've sent to the kernel but for which we have not yet received the corresponding "execute_reply"), then
	// we should not require an ACK, as the kernel will be busy either executing user-submitted code or blocking until
	// the leader replica has finished executing code (or they'll return with a failed election, either way).
	requiresAck := messaging.ShouldMessageRequireAck(typ)
	pendingExecRequests := c.NumPendingExecuteRequests()
	if (c.IsTraining() || pendingExecRequests > 0) && typ == messaging.ShellMessage {
		// If we're currently training and the message is a Shell message, then we'll just not require an ACK.
		// Jupyter doesn't normally use ACKs, so it's fine. The message can simply be resubmitted if necessary.
		requiresAck = false

		c.log.Debug(utils.PurpleStyle.Render("Shell '%s' message %s (JupyterID=\"%s\") targeting kernel %s will NOT require an ACK. IsTraining: %v. NumPendingExecRequests: %d."),
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), c.id, c.IsTraining(), pendingExecRequests)
	}

	// If the context is merely cancellable (without a specific deadline/timeout), then we'll just use the default request timeout.
	timeout := messaging.DefaultRequestTimeout

	// Try to get the deadline from the parent context.
	if deadline, ok := parentContext.Deadline(); ok {
		// If there's a deadline associated with the parent context, then we'll use that to compute a timeout for this request (that matches the deadline of the parent).
		timeout = time.Until(deadline)

		if timeout < 0 {
			c.log.Error("The deadline of %v for parent context of %v request has already been exceeded (current time = %v): %v", deadline, typ.String(), time.Now(), msg)
			return ErrDeadlineExceeded
		}
	}

	builder := messaging.NewRequestBuilder(parentContext, c.id, c.id, c.client.Meta).
		WithAckRequired(requiresAck && c.MessageAcknowledgementsEnabled()).
		WithMessageType(typ).
		WithBlocking(true).
		WithTimeout(timeout).
		WithDoneCallback(done).
		WithMessageHandler(wrappedHandler).
		WithNumAttempts(c.numResendAttempts).
		WithJMsgPayload(msg).
		WithSocketProvider(c).
		WithRemoveDestFrame(getOption(jupyter.WROptionRemoveDestFrame).(bool))
	request, err := builder.BuildRequest()
	if err != nil {
		c.log.Error(utils.RedStyle.Render("Error while building request: %v"), err)
		return err
	}

	err = c.client.Request(request, socket)

	if err != nil {
		c.log.Warn("Failed to send %s \"%s\" request %s (JupyterID=%s) because: %s", socket.Type.String(),
			request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), err.Error())

		// If the error is that we didn't receive any ACKs, then we'll try to reconnect to the kernel.
		// If we reconnect successfully, then we'll try to send it again.
		// If we still fail to send the message at that point, then we'll just give up (for now).
		if errors.Is(err, jupyter.ErrNoAck) {
			c.log.Warn("Connectivity with remote kernel client may have been lost. Will attempt to reconnect and resubmit %v message.", typ)

			if _, reconnectionError := c.ReconnectSocket(typ); reconnectionError != nil {
				c.log.Error("Failed to reconnect to remote kernel client because: %v", reconnectionError)
				c.connectionRevalidationFailedCallback(c, msg, reconnectionError)
				return errors.Join(err, reconnectionError)
			}

			c.log.Debug("Successfully reconnected with remote kernel client on %v socket. Will attempt to resubmit %v message now.", typ.String(), typ.String())

			// Need to update the message's header before resubmitting to avoid duplicate signature errors.
			updateHeaderError := request.PrepareForResubmission()
			if updateHeaderError != nil {
				c.log.Error("Failed to update the header for %s \"%s\" message %s (JupyterID=%s): %v",
					typ.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), updateHeaderError)
				c.resubmissionAfterSuccessfulRevalidationFailedCallback(c, request.Payload(), updateHeaderError)
				return errors.Join(err, updateHeaderError)
			}

			recreatedSocket := c.client.Sockets.All[typ]
			if recreatedSocket == nil {
				return messaging.ErrSocketNotAvailable
			} else if recreatedSocket == socket {
				panic(fmt.Sprintf("Recreated %v socket is equal to original %v socket for replica %d of kernel %s.", typ.String(), typ.String(), c.replicaId, c.id))
			}

			secondAttemptErr := c.client.Request(request, recreatedSocket)
			if secondAttemptErr != nil {
				c.log.Error("Failed to resubmit %v message after successfully reconnecting: %v", typ, secondAttemptErr)
				c.resubmissionAfterSuccessfulRevalidationFailedCallback(c, request.Payload(), secondAttemptErr)
				return errors.Join(err, secondAttemptErr)
			}
		}
	}

	return err
}

// Close closes the zmq sockets.
func (c *KernelReplicaClient) Close() error {
	err := c.BaseServer.Close()
	if err != nil {
		c.log.Warn("Error while closing server of replica %d of kernel %s: %v",
			c.replicaId, c.id, err)
	}

	for _, socket := range c.client.Sockets.All {
		if socket == nil {
			continue
		}

		socketCloseErr := socket.Close()
		if socketCloseErr != nil {
			c.log.Warn("Error while closing %s socket of replica %d of kernel %s: %v",
				socket.Type.String(), c.replicaId, c.id, err)

			if err != nil {
				err = errors.Join(err, socketCloseErr)
			} else {
				err = socketCloseErr
			}
		}
	}
	if c.iopub != nil {
		ioPubCloseError := c.iopub.Close()
		if ioPubCloseError != nil {
			c.log.Warn("Error while closing %s socket of replica %d of kernel %s: %v",
				c.iopub.Type.String(), c.replicaId, c.id, err)

			if err != nil {
				err = errors.Join(err, ioPubCloseError)
			} else {
				err = ioPubCloseError
			}
		}

		c.iopub = nil
	}

	return err
}

// InitializeIOSub initializes the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
// If the provided messaging.MessageHandler parameter is nil, then we will use the default handler.
// (The default handler is KernelReplicaClient::InitializeIOSub.)
//
// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
func (c *KernelReplicaClient) InitializeIOSub(handler messaging.MessageHandler, subscriptionTopic string) (*messaging.Socket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Default to Kernel::handleMsg if the provided handler is null.
	if handler == nil {
		c.log.Debug("Creating ZeroMQ SUB socket: default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
		handler = c.handleMsg
	} else {
		c.log.Debug("Creating ZeroMQ SUB socket: non-default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
	}

	// Handler is set, so server routing will be started on dialing.
	c.client.Sockets.IO = messaging.NewSocketWithHandler(zmq4.NewSub(c.client.Ctx), c.client.Meta.IOPubPort /* sub socket for client */, messaging.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s]", c.id), handler)

	_ = c.client.Sockets.IO.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
	c.client.Sockets.All[messaging.IOMessage] = c.client.Sockets.IO

	if c.status == jupyter.KernelStatusRunning {
		if err := c.dial(c.client.Sockets.IO); err != nil {
			return nil, err
		}
	}

	c.log.Debug("ZeroMQ SUB socket has port %d", c.client.Sockets.IO.Port)

	return c.client.Sockets.IO, nil
}

// dial connects to specified sockets
func (c *KernelReplicaClient) dial(sockets ...*messaging.Socket) error {
	// Start listening on all specified sockets.
	address := fmt.Sprintf("%v://%v:%%v", c.client.Meta.Transport, c.client.Meta.IP)
	for _, socket := range sockets {
		if socket == nil {
			continue
		}

		addressWithPort := fmt.Sprintf(address, socket.Port)
		c.log.Debug("Dialing %s socket at %s now...", socket.Type.String(), addressWithPort)

		err := socket.Socket.Dial(addressWithPort)
		if err != nil {
			return fmt.Errorf("could not connect to kernel %v socket at address %s: %w", socket.Type.String(), addressWithPort, err)
		}

		c.log.Debug("Successfully dialed %s socket at %s.", socket.Type.String(), addressWithPort)
	}

	// Using a second loop to start serving after all sockets are connected.
	for _, socket := range sockets {
		if socket != nil && socket.Handler != nil {
			c.log.Debug("Beginning to serve socket %v.", socket.Type.String())
			go c.client.Serve(c, socket, socket.Handler)
		} else if socket != nil {
			c.log.Debug("Not serving socket %v.", socket.Type.String())
		}
	}

	return nil
}

func (c *KernelReplicaClient) handleMsg(_ messaging.JupyterServerInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	// c.log.Debug("Received message of type %v: \"%v\"", typ.String(), msg)
	switch typ {
	case messaging.IOMessage:
		{
			if c.iopub != nil {
				return c.iobroker.Publish(c, msg)
			} else {
				return ErrIOPubNotStarted
			}
		}
	default:
		return ErrHandlerNotImplemented
	}
}

func (c *KernelReplicaClient) getWaitResponseOption(key string) interface{} {
	switch key {
	case jupyter.WROptionRemoveDestFrame:
		return c.shell != nil
	}

	return nil
}

func (c *KernelReplicaClient) forwardIOMessage(kernel scheduling.KernelReplica, _ *messaging.JupyterFrames, msg *messaging.JupyterMessage) error {
	return kernel.Socket(messaging.IOMessage).Send(*msg.GetZmqMsg())
}

func (c *KernelReplicaClient) HandleIOKernelStatus(_ scheduling.KernelReplica, frames *messaging.JupyterFrames, msg *messaging.JupyterMessage) error {
	var status messaging.MessageKernelStatus
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling IO kernel status: %v", err)
		return err
	}
	err := frames.DecodeContent(&status)
	if err != nil {
		return err
	}

	// c.log.Debug("Handling IO Kernel Status for Kernel %v, Status %v.", kernel.ID(), status.Status)

	c.busyStatus = status.Status
	c.lastBStatusMsg = msg
	// Return nil so that we forward the message to the client.
	return nil
}

// handleIOKernelSMRNodeAdded is the handler for "smr_node_added" IOPub messages.
//
// NOTE: As of right now (5:39pm EST, Oct 11, 2024), this method is not actually used/called, as "smr_node_added" are
// currently not sent. (Specifically, we don't use the "add_replica_request" CONTROL message at the moment, so none
// of this ever happens.)
func (c *KernelReplicaClient) handleIOKernelSMRNodeAdded(_ scheduling.KernelReplica, frames *messaging.JupyterFrames, _ *messaging.JupyterMessage) error {
	var nodeAddedMessage messaging.MessageSMRNodeUpdated
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling kernel SMR node added: %v", err)
		return err
	}
	err := frames.DecodeContent(&nodeAddedMessage)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel SMR Node-Added message for replica %d of kernel %s.", nodeAddedMessage.NodeID, nodeAddedMessage.KernelId)

	if c.smrNodeAddedCallback != nil {
		c.smrNodeAddedCallback(&nodeAddedMessage)
	}

	return types.ErrStopPropagation
}

func (c *KernelReplicaClient) handleIOKernelSMRReady(kernel scheduling.KernelReplica, frames *messaging.JupyterFrames, _ *messaging.JupyterMessage) error {
	var nodeReadyMessage messaging.MessageSMRReady
	if err := frames.Validate(); err != nil {
		c.log.Error("Failed to validate message frames while handling kernel SMR ready: %v", err)
		return err
	}
	err := frames.DecodeContent(&nodeReadyMessage)
	if err != nil {
		return err
	}

	c.log.Debug("Handling IO Kernel SMR Ready for Kernel %v, PersistentID %v.", kernel.ID(), nodeReadyMessage.PersistentID)

	c.persistentId = nodeReadyMessage.PersistentID
	c.log.Debug("Persistent ID confirmed: %v", c.persistentId)

	if c.smrNodeReadyCallback != nil {
		c.smrNodeReadyCallback(c)
	}

	return types.ErrStopPropagation
}
