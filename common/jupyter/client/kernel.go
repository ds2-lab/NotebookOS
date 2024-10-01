package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"github.com/zhangjyr/distributed-notebook/common/metrics"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/server"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
	"github.com/zhangjyr/distributed-notebook/common/scheduling"
	commonTypes "github.com/zhangjyr/distributed-notebook/common/types"
	"github.com/zhangjyr/distributed-notebook/common/utils"
)

var (
	heartbeatInterval = time.Second

	ErrDeadlineExceeded = errors.New("deadline for parent context has already been exceeded")
	//ErrResourceSpecAlreadySet = errors.New("kernel already has a resource spec set")
)

type SMRNodeReadyNotificationCallback func(*KernelReplicaClient)
type SMRNodeUpdatedNotificationCallback func(*types.MessageSMRNodeUpdated) // For node-added or node-removed notifications.

// ConnectionRevalidationFailedCallback defines a special type of callback function.
//
// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we do not reconnect successfully, then this method is called.
type ConnectionRevalidationFailedCallback func(replica *KernelReplicaClient, msg *types.JupyterMessage, err error)

// ResubmissionAfterSuccessfulRevalidationFailedCallback defines a special type of callback function.
//
// When we fail to forward a request to a kernel (in that we did not receive an ACK after the maximum number of attempts),
// we try to reconnect to that kernel (and then resubmit the request, if we reconnect successfully).
//
// If we are able to reconnect successfully, but then the subsequent resubmission/re-forwarding of the request fails,
// then this method is called.
type ResubmissionAfterSuccessfulRevalidationFailedCallback func(replica *KernelReplicaClient, msg *types.JupyterMessage, err error)

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
	SessionManager
	client *server.AbstractServer

	// destMutex                 sync.Mutex
	id                               string
	replicaId                        int32
	persistentId                     string
	spec                             *proto.KernelSpec
	status                           types.KernelStatus
	busyStatus                       string
	lastBStatusMsg                   *types.JupyterMessage
	iobroker                         *MessageBroker[scheduling.Kernel, *types.JupyterMessage, types.JupyterFrames]
	shell                            *types.Socket    // Listener.
	iopub                            *types.Socket    // Listener.
	numResendAttempts                int              // Number of times to try resending a message before giving up.
	addSourceKernelFrames            bool             // If true, then the SUB-type ZMQ socket, which is used as part of the Jupyter IOPub Socket, will set its subscription option to the KernelReplicaClient's kernel ID.
	shellListenPort                  int              // Port that the KernelReplicaClient::shell socket listens on.
	iopubListenPort                  int              // Port that the KernelReplicaClient::iopub socket listens on.
	podOrContainerName               string           // Name of the Pod or Container housing the associated distributed kernel replica container.
	nodeName                         string           // Name of the node that the Pod or Container is running on.
	ready                            bool             // True if the replica has registered and joined its SMR cluster. Only used by the internalCluster Gateway, not by the Local Daemon.
	yieldNextExecutionRequest        bool             // If true, then we will yield the next 'execute_request'.
	hostId                           string           // The ID of the host that we're running on (actually, it is the ID of the local daemon running on our host, specifically).
	host                             *scheduling.Host // The host that the kernel replica is running on.
	workloadId                       string           // workloadId is the ID of the workload associated with this kernel, if this kernel was created within a workload. This is populated after extracting the ID from the metadata frame of a Jupyter message.
	workloadIdSet                    bool             // workloadIdSet is a flag indicating whether workloadId has been assigned a "meaningful" value or not.
	trainingStartedAt                time.Time        // trainingStartedAt is the time at which the kernel associated with this client began actively training.
	lastTrainingTimePrometheusUpdate time.Time        // lastTrainingTimePrometheusUpdate records the current time as the last instant in which we published an updated training time metric to Prometheus. We use this to determine how much more to increment the training time Prometheus metric when we stop training, since any additional training time since the last scheduled publish won't be pushed to Prometheus automatically by the publisher-goroutine.
	isTraining                       bool             // isTraining indicates whether the kernel replica associated with this client is actively training.
	// isSomeReplicaTraining            bool             // isSomeReplicaTraining indicates whether any replica of the kernel associated with this client is actively training, even if it is not the specific replica associated with this client.

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
	container *scheduling.Container

	// prometheusManager is an interface that enables the recording of metrics observed by the KernelReplicaClient.
	messagingMetricsProvider metrics.MessagingMetricsProvider

	log logger.Logger
	mu  sync.Mutex
}

// NewKernelReplicaClient creates a new KernelReplicaClient.
// The client will initialize all sockets except IOPub. Call InitializeIOForwarder() to add IOPub support.
//
// If the proto.KernelReplicaSpec argument is nil, or the proto.KernelSpec field of the proto.KernelReplicaSpec
// argument is nil, then NewKernelReplicaClient will panic.
func NewKernelReplicaClient(ctx context.Context, spec *proto.KernelReplicaSpec, info *types.ConnectionInfo, componentId string,
	addSourceKernelFrames bool, numResendAttempts int, shellListenPort int, iopubListenPort int, podOrContainerName string, nodeName string,
	smrNodeReadyCallback SMRNodeReadyNotificationCallback, smrNodeAddedCallback SMRNodeUpdatedNotificationCallback,
	persistentId string, hostId string, host *scheduling.Host, nodeType metrics.NodeType, shouldAckMessages bool, isGatewayClient bool,
	messagingMetricsProvider metrics.MessagingMetricsProvider, connectionRevalidationFailedCallback ConnectionRevalidationFailedCallback,
	resubmissionAfterSuccessfulRevalidationFailedCallback ResubmissionAfterSuccessfulRevalidationFailedCallback) *KernelReplicaClient {

	// Validate that the `spec` argument is non-nil.
	if spec == nil {
		log.Fatalf(utils.RedStyle.Render("Cannot create new KernelClient, as spec is nil.\n"))
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
		addSourceKernelFrames:                addSourceKernelFrames,
		shellListenPort:                      shellListenPort,
		messagingMetricsProvider:             messagingMetricsProvider,
		iopubListenPort:                      iopubListenPort,
		podOrContainerName:                   podOrContainerName,
		nodeName:                             nodeName,
		smrNodeReadyCallback:                 smrNodeReadyCallback,
		smrNodeAddedCallback:                 smrNodeAddedCallback,
		numResendAttempts:                    numResendAttempts,
		yieldNextExecutionRequest:            false,
		host:                                 host,
		hostId:                               hostId,
		isGatewayClient:                      isGatewayClient,
		connectionRevalidationFailedCallback: connectionRevalidationFailedCallback,
		resubmissionAfterSuccessfulRevalidationFailedCallback: resubmissionAfterSuccessfulRevalidationFailedCallback,
		client: server.New(ctx, info, nodeType, func(s *server.AbstractServer) {
			var remoteComponentName string
			if isGatewayClient {
				remoteComponentName = "LD"
			} else {
				remoteComponentName = "Kernel"
			}

			// We do not set handlers of the sockets here. So no server routine will be started on dialing.
			s.Sockets.Control = types.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.ControlPort, types.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Ctrl[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.Shell = types.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.ShellPort, types.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Shell[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.Stdin = types.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.StdinPort, types.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-Stdin[%s]", remoteComponentName, spec.Kernel.Id))
			s.Sockets.HB = types.NewSocketWithRemoteName(zmq4.NewDealer(s.Ctx), info.HBPort, types.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", spec.Kernel.Id), fmt.Sprintf("K-%s-HB[%s]", remoteComponentName, spec.Kernel.Id))
			s.ReconnectOnAckFailure = true
			s.PrependId = false
			s.ComponentId = componentId
			s.MessagingMetricsProvider = messagingMetricsProvider
			s.Name = fmt.Sprintf("KernelReplicaClient-%s", spec.Kernel.Id)

			/* Kernel clients should ACK messages that they're forwarding when the local kernel client lives on the Local Daemon. */
			s.ShouldAckMessages = shouldAckMessages
			// s.Sockets.Ack = types.NewSocket(Socket: zmq4.NewReq(s.Ctx), Port: info.AckPort}
			// IOPub is lazily initialized for different subclasses.
			if spec.ReplicaId == 0 {
				config.InitLogger(&s.Log, fmt.Sprintf("Kernel %s ", spec.Kernel.Id))
			} else {
				config.InitLogger(&s.Log, fmt.Sprintf("Replica %s:%d ", spec.Kernel.Id, spec.ReplicaId))
			}
		}),
		status: types.KernelStatusInitializing,
	}
	client.BaseServer = client.client.Server()
	client.SessionManager = NewSessionManager(spec.Kernel.Session)
	client.log = client.client.Log

	client.log.Debug("Created new Kernel Client with spec %v, connection info %v.", spec, info)

	return client
}

func (c *KernelReplicaClient) Container() *scheduling.Container {
	return c.container
}

func (c *KernelReplicaClient) SetContainer(container *scheduling.Container) {
	c.container = container
}

// IsTraining returns a bool indicating whether the kernel associated with this client is actively training.
func (c *KernelReplicaClient) IsTraining() bool {
	return c.isTraining
}

// IsSomeReplicaTraining returns a bool indicating  whether any replica of the kernel associated with this client is
// actively training, even if it is not the specific replica associated with this client.
//func (c *KernelReplicaClient) IsSomeReplicaTraining() bool {
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
func KernelStartedTraining[T commonTypes.ArbitraryResourceSnapshot](c *KernelReplicaClient, snapshot commonTypes.HostResourceSnapshot[T]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isTraining {
		c.log.Warn("Replica %d of kernel %s is already training as of %v. Ending current training now. "+
			"Will discard future \"execute_reply\" message if we do end up receiving it...", c.replicaId, c.id, c.trainingStartedAt)

		// We already locked the kernel above, so we can call the unsafe method directly here.
		err := unsafeKernelStoppedTraining[T](c, snapshot)
		if err != nil {
			c.log.Error("Couldn't cleanly stop training for replica %d of kernel %s: %v", c.replicaId, c.id, err)
			return err
		}
	}

	c.isTraining = true
	c.trainingStartedAt = time.Now()

	// The following code is only executed within the internalCluster Gateway.
	container := c.Container()
	if container != nil { // Container will be nil on Local Daemons; they don't track resources this way.
		p := scheduling.SessionStartedTraining(container.Session(), container, snapshot)
		if err := p.Error(); err != nil {
			c.log.Error("Failed to start training for session %s: %v", container.Session().ID(), err)
			return err
		}
	}

	c.log.Debug(utils.PurpleStyle.Render("Replica %d of kernel \"%s\" has STARTED training."), c.replicaId, c.id)

	return nil
}

// unsafeKernelStoppedTraining does the work of KernelStoppedTraining without acquiring the KernelReplicaClient's
// mutex first (hence "unsafe").
func unsafeKernelStoppedTraining[T commonTypes.ArbitraryResourceSnapshot](c *KernelReplicaClient, snapshot commonTypes.HostResourceSnapshot[T]) error {
	if !c.isTraining {
		c.log.Error("Cannot stop training; already not training.")
		return fmt.Errorf("cannot stop training; replica %d of kernel %s is already not training", c.replicaId, c.id)
	}

	c.isTraining = false

	// The following code executes only on the internalCluster Gateway.
	//
	// If the Container is actively-training, then we need to call SessionStoppedTraining
	// before removing it so that the resources are all returned appropriately.
	if container := c.Container(); container != nil {
		p := scheduling.SessionStoppedTraining[T](container.Session(), snapshot)
		if err := p.Error(); err != nil {
			c.log.Error("Failed to stop training on scheduling.Container %s-%d during replica removal because: %v",
				c.ID(), c.ReplicaID(), err)
			return err
		}
	}

	c.log.Debug(utils.LightPurpleStyle.Render("Replica %d of kernel \"%s\" has STOPPED training after %v."),
		c.replicaId, c.id, time.Since(c.trainingStartedAt))

	return nil
}

// KernelStoppedTraining should be called when the kernel associated with this client stops actively training.
func (c *KernelReplicaClient) KernelStoppedTraining(snapshot commonTypes.HostResourceSnapshot[*scheduling.ResourceSnapshot]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return unsafeKernelStoppedTraining[*scheduling.ResourceSnapshot](c, snapshot)
}

// TrainingStartedAt returns the time at which the kernel associated with this client began actively training.
func (c *KernelReplicaClient) TrainingStartedAt() time.Time {
	return c.trainingStartedAt
}

// Recreate and return the kernel's control socket.
// This reuses the handler on the existing/previous control socket.
func (c *KernelReplicaClient) recreateControlSocket() *types.Socket {
	handler, err := c.closeSocket(types.ControlMessage)
	if err != nil {
		c.log.Error("Could not find control socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = types.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Ctrl[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Ctrl[%s]", c.id)
	}

	newSocket := types.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.ControlPort, types.ControlMessage, fmt.Sprintf("K-Dealer-Ctrl[%s]", c.id), remoteName, handler)
	c.client.Sockets.Control = newSocket
	c.client.Sockets.All[types.ControlMessage] = newSocket
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
func (c *KernelReplicaClient) recreateShellSocket() *types.Socket {
	handler, err := c.closeSocket(types.ShellMessage)
	if err != nil {
		c.log.Error("Could not find shell socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = types.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Shell[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Shell[%s]", c.id)
	}

	newSocket := types.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.ShellPort, types.ShellMessage, fmt.Sprintf("K-Dealer-Shell[%s]", c.id), remoteName, handler)
	c.client.Sockets.Shell = newSocket
	c.client.Sockets.All[types.ShellMessage] = newSocket
	return newSocket
}

func (c *KernelReplicaClient) ShouldAckMessages() bool {
	return c.client.ShouldAckMessages
}

// Recreate and return the kernel's stdin socket.
// This reuses the handler on the existing/previous stdin socket.
func (c *KernelReplicaClient) recreateStdinSocket() *types.Socket {
	handler, err := c.closeSocket(types.StdinMessage)
	if err != nil {
		c.log.Error("Could not find stdin socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = types.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-Stdin[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-Stdin[%s]", c.id)
	}

	newSocket := types.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.StdinPort, types.StdinMessage, fmt.Sprintf("K-Dealer-Stdin[%s]", c.id), remoteName, handler)
	c.client.Sockets.Stdin = newSocket
	c.client.Sockets.All[types.StdinMessage] = newSocket
	return newSocket
}

// Recreate and return the kernel's heartbeat socket.
// This reuses the handler on the existing/previous heartbeat socket.
func (c *KernelReplicaClient) recreateHeartbeatSocket() *types.Socket {
	handler, err := c.closeSocket(types.HBMessage)
	if err != nil {
		c.log.Error("Could not find heartbeat socket on Client... Handler of new socket will be nil.")
	}

	var remoteName = types.RemoteNameUnspecified
	if c.isGatewayClient {
		// The remote client is the kernel client on one of the Local Daemons.
		remoteName = fmt.Sprintf("K-LD-HB[%s]", c.id)
	} else {
		// The remote client is the Jupyter kernel replica itself.
		remoteName = fmt.Sprintf("K-Kernel-HB[%s]", c.id)
	}

	newSocket := types.NewSocketWithHandlerAndRemoteName(zmq4.NewDealer(c.client.Ctx), c.client.Meta.HBPort, types.HBMessage, fmt.Sprintf("K-Dealer-HB[%s]", c.id), remoteName, handler)
	c.client.Sockets.HB = newSocket
	c.client.Sockets.All[types.HBMessage] = newSocket
	return newSocket
}

// Close the socket of the specified type, returning the handler set for that socket.
func (c *KernelReplicaClient) closeSocket(typ types.MessageType) (types.MessageHandler, error) {
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
		return nil, types.ErrSocketNotAvailable
	}
}

// This is called when the replica ID is set/changed, so that the logger's prefix reflects the replica ID.
func (c *KernelReplicaClient) updateLogPrefix() {
	if c.client.Log == nil {
		return
	}

	c.client.Log.(*logger.ColorLogger).Prefix = fmt.Sprintf("Replica %s:%d ", c.id, c.replicaId)
}

// PodName returns the name of the Kubernetes Pod hosting the replica.
func (c *KernelReplicaClient) PodName() string {
	return c.podOrContainerName
}

// NodeName returns the name of the node that the Pod is running on.
func (c *KernelReplicaClient) NodeName() string {
	return c.nodeName
}

func (c *KernelReplicaClient) ShellListenPort() int {
	return c.shellListenPort
}

func (c *KernelReplicaClient) IOPubListenPort() int {
	return c.iopubListenPort
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
func (c *KernelReplicaClient) ResourceSpec() *commonTypes.DecimalSpec {
	if c.spec == nil {
		log.Fatalf(utils.RedStyle.Render("Replica %d of kernel %s does not have a valid (i.e., non-nil) spec...\n"),
			c.replicaId, c.id)
	}

	return c.spec.DecimalSpecFromKernelSpec()
}

func (c *KernelReplicaClient) SetResourceSpec(spec *proto.ResourceSpec) {
	c.spec.ResourceSpec = spec
}

// KernelSpec returns the kernel spec.
func (c *KernelReplicaClient) KernelSpec() *proto.KernelSpec {
	return c.spec
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
func (c *KernelReplicaClient) Socket(typ types.MessageType) *types.Socket {
	switch typ {
	case types.IOMessage:
		return c.iopub
	case types.ShellMessage:
		return c.shell
	default:
		return nil
	}
}

// ConnectionInfo returns the connection info.
func (c *KernelReplicaClient) ConnectionInfo() *types.ConnectionInfo {
	return c.client.Meta
}

// Status returns the kernel status.
func (c *KernelReplicaClient) Status() types.KernelStatus {
	return c.status
}

// BusyStatus returns the kernel busy status.
func (c *KernelReplicaClient) BusyStatus() (string, *types.JupyterMessage) {
	return c.busyStatus, c.lastBStatusMsg
}

// BindSession binds a session ID to the client.
func (c *KernelReplicaClient) BindSession(sess string) {
	c.SessionManager.BindSession(sess)
	c.log.Info("Binded session %s to kernel client", sess)
}

// ReconnectSocket recreates and redials a particular socket.
func (c *KernelReplicaClient) ReconnectSocket(typ types.MessageType) (*types.Socket, error) {
	var socket *types.Socket

	c.log.Debug("Recreating %s socket.", typ.String())
	switch typ {
	case types.ControlMessage:
		{
			socket = c.recreateControlSocket()
		}
	case types.ShellMessage:
		{
			socket = c.recreateShellSocket()
		}
	case types.HBMessage:
		{
			socket = c.recreateStdinSocket()
		}
	case types.StdinMessage:
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
			c.status = types.KernelStatusExited
			return nil, types.ErrKernelClosed
		case <-timer.C:
			c.mu.Lock()

			c.log.Debug("Attempting to reconnect on %s socket.", typ.String())
			if err := c.dial(socket); err != nil {
				c.client.Log.Error("Failed to reconnect %v socket: %v", typ, err)
				_ = c.Close()
				c.status = types.KernelStatusExited
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
	if c.status >= types.KernelStatusRunning { /* If we're already connected, then just return, unless `forceReconnect` is true */
		return nil
	}

	timer := time.NewTimer(0)

	for {
		select {
		case <-c.client.Ctx.Done():
			c.status = types.KernelStatusExited
			return types.ErrKernelClosed
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
			if err := c.dial(c.client.Sockets.All[types.HBMessage+1:]...); err != nil {
				c.client.Log.Error("Failed to dial at least one socket: %v", err)
				_ = c.Close()
				c.status = types.KernelStatusExited
				c.mu.Unlock()
				return err
			}

			c.status = types.KernelStatusRunning
			c.mu.Unlock()
			return nil
		}
	}
}

func (c *KernelReplicaClient) InitializeShellForwarder(handler scheduling.KernelMessageHandler) (*types.Socket, error) {
	c.log.Debug("Initializing shell forwarder for kernel client.")

	shell := types.NewSocket(zmq4.NewRouter(c.client.Ctx), c.shellListenPort, types.ShellMessage, fmt.Sprintf("K-Router-ShellForwrder[%s]", c.id))
	if err := c.client.Listen(shell); err != nil {
		return nil, err
	}

	c.shell = shell
	go c.client.Serve(c, shell, func(srv types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
		// msg.Frames, _ = types.AddDestFrame(msg.Frames, c.id, jupyter.JOffsetAutoDetect)
		msg.AddDestinationId(c.id)
		return handler(c, typ, msg)
	})

	return shell, nil
}

// func (c *KernelReplicaClient) Unlock() {
// 	c.destMutex.Unlock()
// }

// func (c *KernelReplicaClient) Lock() {
// 	c.destMutex.Lock()
// }

// InitializeIOForwarder initializes the IOPub serving.
// Returns Pub socket, Sub socket, error.
func (c *KernelReplicaClient) InitializeIOForwarder() (*types.Socket, error) {
	iopub := types.NewSocket(zmq4.NewPub(c.client.Ctx), c.iopubListenPort /* c.client.Meta.IOSubPort */, types.IOMessage, fmt.Sprintf("K-Pub-IOForwrder[%s]", c.id))

	c.log.Debug("Created ZeroMQ PUB socket with port %d.", iopub.Port)

	if err := c.client.Listen(iopub); err != nil {
		return nil, err
	}

	c.iopub = iopub
	c.iobroker = NewMessageBroker[scheduling.Kernel](c.extractIOTopicFrame)
	c.iobroker.Subscribe(MessageBrokerAllTopics, c.forwardIOMessage) // Default to forward all messages.
	c.iobroker.Subscribe(types.IOTopicStatus, c.handleIOKernelStatus)
	c.iobroker.Subscribe(types.IOTopicSMRReady, c.handleIOKernelSMRReady)
	c.iobroker.Subscribe(types.IOTopicSMRNodeAdded, c.handleIOKernelSMRNodeAdded)
	// c.iobroker.Subscribe(types.IOTopicSMRNodeRemoved, c.handleIOKernelSMRNodeRemoved)
	return iopub, nil
}

// AddIOHandler adds a handler for a specific IOPub topic.
// The handler should return ErrStopPropagation to avoid msg being forwarded to the client.
func (c *KernelReplicaClient) AddIOHandler(topic string, handler MessageBrokerHandler[scheduling.Kernel, types.JupyterFrames, *types.JupyterMessage]) error {
	if c.iobroker == nil {
		return ErrIOPubNotStarted
	}
	c.iobroker.Subscribe(topic, handler)
	return nil
}

// RequestWithHandler sends a request and handles the response.
func (c *KernelReplicaClient) RequestWithHandler(ctx context.Context, _ string, typ types.MessageType, msg *types.JupyterMessage, handler scheduling.KernelMessageHandler, done func()) error {
	// c.log.Debug("%s %v request(%p): %v", prompt, typ, msg, msg)
	return c.requestWithHandler(ctx, typ, msg, handler, c.getWaitResponseOption, done)
}

func (c *KernelReplicaClient) requestWithHandler(parentContext context.Context, typ types.MessageType, msg *types.JupyterMessage, handler scheduling.KernelMessageHandler, getOption server.WaitResponseOptionGetter, done func()) error {
	if c.status < types.KernelStatusRunning {
		return types.ErrKernelNotReady
	}

	// Use a default "done" handler.
	if done == nil {
		done = types.DefaultDoneHandler
	}

	socket := c.client.Sockets.All[typ]
	if socket == nil {
		return types.ErrSocketNotAvailable
	}

	wrappedHandler := func(server types.JupyterServerInfo, respType types.MessageType, respMsg *types.JupyterMessage) (err error) {
		// Kernel frame is automatically removed.
		if handler != nil {
			err = handler(server.(*KernelReplicaClient), respType, respMsg)
		}
		return err
	}

	requiresAck := types.ShouldMessageRequireAck(typ)
	if c.isTraining && typ == types.ShellMessage {
		// If we're currently training and the message is a Shell message, then we'll just not require an ACK.
		// Jupyter doesn't normally use ACKs, so it's fine. The message can simply be resubmitted if necessary.
		requiresAck = false

		c.log.Warn(utils.YellowStyle.Render("Shell '%s' message %s (JupyterID=\"%s\") targeting actively-training kernel %s will NOT require an ACK."),
			msg.JupyterMessageType(), msg.RequestId, msg.JupyterMessageId(), c.id)
	}

	// If the context is merely cancellable (without a specific deadline/timeout), then we'll just use the default request timeout.
	timeout := types.DefaultRequestTimeout

	// Try to get the deadline from the parent context.
	if deadline, ok := parentContext.Deadline(); ok {
		// If there's a deadline associated with the parent context, then we'll use that to compute a timeout for this request (that matches the deadline of the parent).
		timeout = time.Until(deadline)

		if timeout < 0 {
			c.log.Error("The deadline of %v for parent context of %v request has already been exceeded (current time = %v): %v", deadline, typ.String(), time.Now(), msg)
			return ErrDeadlineExceeded
		}
	}

	builder := types.NewRequestBuilder(parentContext, c.id, c.id, c.client.Meta).
		WithAckRequired(requiresAck).
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

	sendRequest := func(req types.Request, sock *types.Socket) error {
		if req == nil {
			panic("Cannot send nil request.")
		}

		if sock == nil {
			panic(fmt.Sprintf("Cannot send request on nil socket. Request: %v", sock))
		}

		return c.client.Request(req, sock)
	}

	// Add timeout if necessary.
	err = sendRequest(request, socket)

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
				c.log.Error("Failed to update the header for %s \"%s\" message %s (JupyterID=%s): %v", typ.String(), request.JupyterMessageType(), request.RequestId(), request.JupyterMessageId(), updateHeaderError)
				c.resubmissionAfterSuccessfulRevalidationFailedCallback(c, request.Payload(), updateHeaderError)
				return errors.Join(err, updateHeaderError)
			}

			recreatedSocket := c.client.Sockets.All[typ]
			if recreatedSocket == nil {
				return types.ErrSocketNotAvailable
			} else if recreatedSocket == socket {
				panic(fmt.Sprintf("Recreated %v socket is equal to original %v socket for replica %d of kernel %s.", typ.String(), typ.String(), c.replicaId, c.id))
			}

			// Create a new child context, as the previous child context will have been cancelled.
			// childContext, _ := context.WithCancel(parentContext)
			// defer cancel2()

			secondAttemptErr := sendRequest(request, recreatedSocket)
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
	c.BaseServer.Close()
	for _, socket := range c.client.Sockets.All {
		if socket != nil {
			_ = socket.Close()
		}
	}
	if c.iopub != nil {
		_ = c.iopub.Close()
		c.iopub = nil
	}
	return nil
}

// GetHost returns the Host on which the replica is hosted.
func (c *KernelReplicaClient) GetHost() *scheduling.Host {
	return c.host
}

// SetHost sets the Host of the kernel.
func (c *KernelReplicaClient) SetHost(host *scheduling.Host) {
	c.host = host
}

// InitializeIOSub initializes the ZMQ SUB socket for handling IO messages from the Jupyter kernel.
// If the provided types.MessageHandler parameter is nil, then we will use the default handler.
// (The default handler is KernelReplicaClient::InitializeIOSub.)
//
// The ZMQ socket is subscribed to the specified topic, which should be "" (i.e., the empty string) if no subscription is desired.
func (c *KernelReplicaClient) InitializeIOSub(handler types.MessageHandler, subscriptionTopic string) (*types.Socket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Default to KernelReplicaClient::handleMsg if the provided handler is null.
	if handler == nil {
		c.log.Debug("Creating ZeroMQ SUB socket: default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
		handler = c.handleMsg
	} else {
		c.log.Debug("Creating ZeroMQ SUB socket: non-default handler, port %d, subscribe-topic \"%s\"", c.client.Meta.IOPubPort, subscriptionTopic)
	}

	// Handler is set, so server routing will be started on dialing.
	c.client.Sockets.IO = types.NewSocketWithHandler(zmq4.NewSub(c.client.Ctx), c.client.Meta.IOPubPort /* sub socket for client */, types.IOMessage, fmt.Sprintf("K-Sub-IOSub[%s]", c.id), handler)

	_ = c.client.Sockets.IO.SetOption(zmq4.OptionSubscribe, subscriptionTopic)
	c.client.Sockets.All[types.IOMessage] = c.client.Sockets.IO

	if c.status == types.KernelStatusRunning {
		if err := c.dial(c.client.Sockets.IO); err != nil {
			return nil, err
		}
	}

	c.log.Debug("ZeroMQ SUB socket has port %d", c.client.Sockets.IO.Port)

	return c.client.Sockets.IO, nil
}

// dial connects to specified sockets
func (c *KernelReplicaClient) dial(sockets ...*types.Socket) error {
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

func (c *KernelReplicaClient) handleMsg(_ types.JupyterServerInfo, typ types.MessageType, msg *types.JupyterMessage) error {
	// c.log.Debug("Received message of type %v: \"%v\"", typ.String(), msg)
	switch typ {
	case types.IOMessage:
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

func (c *KernelReplicaClient) extractIOTopicFrame(msg *types.JupyterMessage) (topic string, jFrames types.JupyterFrames) {
	// _, jOffset := types.SkipIdentitiesFrame(msg.Frames)
	// jFrames = msg.Frames[jOffset:]
	// if jOffset == 0 {
	// 	return "", jFrames
	// }

	jFrames = msg.ToJFrames()
	if msg.Offset == 0 {
		return "", jFrames
	}

	rawTopic := msg.Frames[ /*jOffset*/ msg.Offset-1]
	matches := types.IOTopicStatusRecognizer.FindSubmatch(rawTopic)
	if len(matches) > 0 {
		return string(matches[2]), jFrames
	}

	return string(rawTopic), jFrames
}

func (c *KernelReplicaClient) forwardIOMessage(kernel scheduling.Kernel, _ types.JupyterFrames, msg *types.JupyterMessage) error {
	return kernel.Socket(types.IOMessage).Send(*msg.Msg)
}

func (c *KernelReplicaClient) handleIOKernelStatus(_ scheduling.Kernel, frames types.JupyterFrames, msg *types.JupyterMessage) error {
	var status types.MessageKernelStatus
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

// func (c *KernelReplicaClient) handleIOKernelSMRNodeRemoved(kernel scheduling.Kernel, frames types.JupyterFrames, msg *types.JupyterMessage) error {
// 	c.log.Debug("Handling IO Kernel SMR Node-Removed message...")
// 	var node_removed_message types.MessageSMRNodeUpdated
// 	if err := frames.Validate(); err != nil {
// 		return err
// 	}
// 	err := frames.DecodeContent(&node_removed_message)
// 	if err != nil {
// 		return err
// 	}

// 	c.log.Debug("Handling IO Kernel SMR Node-Removed message for replica %d of kernel %s.", node_removed_message.NodeID, node_removed_message.KernelId)

// 	if c.smrNodeRemovedCallback != nil {
// 		c.smrNodeRemovedCallback(&node_removed_message)
// 	}

// 	return types.ErrStopPropagation
// }

func (c *KernelReplicaClient) handleIOKernelSMRNodeAdded(_ scheduling.Kernel, frames types.JupyterFrames, _ *types.JupyterMessage) error {
	var nodeAddedMessage types.MessageSMRNodeUpdated
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

	return commonTypes.ErrStopPropagation
}

func (c *KernelReplicaClient) handleIOKernelSMRReady(kernel scheduling.Kernel, frames types.JupyterFrames, _ *types.JupyterMessage) error {
	var nodeReadyMessage types.MessageSMRReady
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

	return commonTypes.ErrStopPropagation
}
