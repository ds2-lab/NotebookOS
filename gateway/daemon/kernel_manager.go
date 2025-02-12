package daemon

import (
	"errors"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/jupyter/router"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"golang.org/x/net/context"
)

const (
	// initialMapSize is the initial size used when instantiating the map elements of the KernelManager struct.
	initialMapSize = 128

	forwarding = "Forwarding"
)

var (
	ErrEmptyKernelId = errors.New("kernel ID is empty")
)

// ResponseForwarder is an interface that provides the means to forward responses from scheduling.Kernel and
// scheduling.KernelReplica instances back to the associated Jupyter client.
type ResponseForwarder interface {
	// ForwardResponse forwards a response from a scheduling.Kernel / scheduling.KernelReplica
	// back to the Jupyter client.
	ForwardResponse(from router.Info, typ messaging.MessageType, msg *messaging.JupyterMessage) error
}

// KernelManager is responsible for creating, maintaining, and routing messages to scheduling.Kernel and
// scheduling.KernelReplica instances running within the cluster.
type KernelManager struct {
	log logger.Logger

	// Kernels is a mapping from kernel ID to scheduling.Kernel.
	Kernels hashmap.HashMap[string, scheduling.Kernel]

	// Sessions is a mapping from Jupyter session ID to scheduling.Kernel.
	Sessions hashmap.HashMap[string, scheduling.Kernel]

	// RequestTracingEnabled controls whether we embed proto.RequestTrace structs within Jupyter requests and replies.
	RequestTracingEnabled bool

	ResponseForwarder ResponseForwarder
}

// NewKernelManager creates a new KernelManager struct and returns a pointer to it.
func NewKernelManager(responseForwarder ResponseForwarder) *KernelManager {
	manager := &KernelManager{
		Kernels:           hashmap.NewThreadsafeCornelkMap[string, scheduling.Kernel](initialMapSize),
		Sessions:          hashmap.NewThreadsafeCornelkMap[string, scheduling.Kernel](initialMapSize),
		ResponseForwarder: responseForwarder,
	}

	config.InitLogger(&manager.log, manager)

	return manager
}

// ForwardRequestToKernel forwards the given *messaging.JupyterMessage to the specified scheduling.Kernel using the
// specified socket type (i.e., messaging.MessageType).
func (km *KernelManager) ForwardRequestToKernel(kernelOrSessionId string, msg *messaging.JupyterMessage, socketTyp messaging.MessageType) error {
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

	return kernel.RequestWithHandler(context.Background(), forwarding, socketTyp, msg, km.forwardResponseFromKernel, nil)
}

// ensureReplicasScheduled ensures that the scheduling.KernelReplica instances of the specified scheduling.Kernel are
// scheduled so that a message can be forwarded.
func (km *KernelManager) ensureReplicasScheduled(kernel scheduling.Kernel) error {
	panic("Implement me")
}

// forwardResponseFromKernel forwards the given messaging.JupyterMessage response from the given
// scheduling.KernelReplica to the Jupyter client.
func (km *KernelManager) forwardResponseFromKernel(from scheduling.KernelReplicaInfo, typ messaging.MessageType, msg *messaging.JupyterMessage) error {
	km.updateRequestTraceReplicaId(from, msg)

	// If we just processed an "execute_reply" (without error, or else we would've returned earlier), and the
	// scheduling policy indicates that the kernel container(s) should be stopped after processing a training
	// event, then let's stop the kernel container(s).
	if msg.JupyterMessageType() == messaging.ShellExecuteRequest {
		// TODO: Implement this.
		// km.cleanUpBeforeForwardingExecuteReply(from, msg)

		panic("Implement me!")
	}

	return km.ResponseForwarder.ForwardResponse(from, typ, msg)
}

// updateRequestTraceReplicaId updates the ReplicaId field of the proto.RequestTrace embedded in the given *messaging.JupyterMessage.
func (km *KernelManager) updateRequestTraceReplicaId(from scheduling.KernelReplicaInfo, msg *messaging.JupyterMessage) {
	if !km.RequestTracingEnabled {
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
func (km *KernelManager) tryGetKernel(kernelOrSessionId string) (scheduling.Kernel, bool) {
	// First, search in the Kernels mapping.
	kernel, ok := km.Kernels.Load(kernelOrSessionId)
	if ok {
		return kernel, true
	}

	// Now try by searching the Sessions mapping.
	return km.Sessions.Load(kernelOrSessionId)
}

// ControlHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *KernelManager) controlHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	km.log.Debug("Forwarding CONTROL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// ShellHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *KernelManager) shellHandler(_ router.Info, msg *messaging.JupyterMessage) error {
	km.log.Debug("Forwarding SHELL [MsgId='%s', MsgTyp='%s'].",
		msg.JupyterMessageId(), msg.JupyterMessageType())

	return nil
}

// StdinHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *KernelManager) stdinHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}

// HBHandler is responsible for forwarding a message received on the CONTROL socket to
// the appropriate/targeted scheduling.Kernel.
func (km *KernelManager) heartbeatHandler(_ router.Info, msg *messaging.JupyterMessage) error {

	return nil
}
