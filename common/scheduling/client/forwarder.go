package client

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/jupyter/messaging"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/utils"
	"github.com/scusemua/distributed-notebook/common/utils/hashmap"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

const (
	// DefaultExecuteRequestQueueSize is the default capacity of outgoing "execute_request" message queues.
	//
	// Important note: if there are more concurrent execute_request messages sent than the capacity of the buffered
	// channel that serves as the message queue, then the first-come, first-serve ordering of the messages cannot be
	// guaranteed. Specifically, are enqueued is non-deterministic. (Once enqueued, the messages will be served in
	// a first-come, first-serve manner.)
	DefaultExecuteRequestQueueSize = 128
)

// enqueuedRequest encapsulates an "execute_request" or "yield_request" *messaging.JupyterMessage and a
// chan interface{} used to notify the caller when the request has been submitted and a result has been returned.
type enqueuedRequest[MessageType any] struct {
	Msg           MessageType
	Kernel        MessageRecipient
	ResultChannel chan interface{}
	MsgId         string
}

// MessageRecipient is an interface that enables ExecuteRequestForwarder to work with both scheduling.Kernel
// and scheduling.KernelReplica instances.
type MessageRecipient interface {
	// ID returns the kernel ID of the target MessageRecipient.
	ID() string

	// IsTraining returns true if the target MessageRecipient is actively training.
	IsTraining() bool

	// LastTrainingStartedAt returns the time at which the target MessageRecipient last began training.
	LastTrainingStartedAt() time.Time
}

type RequestHandler[MessageType any] func(ctx context.Context, _ string, typ messaging.MessageType, msg MessageType, handler scheduling.KernelReplicaMessageHandler, done func()) error

type ProcessMessageFunc[MessageType any] func(msg MessageType, kernel MessageRecipient) MessageType

// ExecuteRequestForwarder ensures that "execute_request" (and "yield_request") messages are sent one-at-a-time.
type ExecuteRequestForwarder[MessageType any] struct {
	log logger.Logger

	// outgoingExecuteRequestQueue is used to send "execute_request" messages one-at-a-time to their target kernels.
	outgoingExecuteRequestQueue hashmap.HashMap[string, chan *enqueuedRequest[MessageType]]

	// outgoingExecuteRequestQueueMutexes is a map of mutexes. Keys are kernel IDs. Values are the mutex for the
	// associated kernel's outgoing "execute_request" message queue.
	outgoingExecuteRequestQueueMutexes hashmap.HashMap[string, *sync.Mutex]

	// executeRequestQueueStopChannels is a map from kernel ID to the channel used to tell the goroutine responsible
	// for forwarding that kernel's "execute_request" messages to stop running (such as when we are stopping
	// the kernel replica).
	executeRequestQueueStopChannels hashmap.HashMap[string, chan interface{}]

	// notificationCallback is an optional callback used to submit notifications back to the frontend.
	notificationCallback scheduling.NotificationCallback

	// processCallback is an optional callback used to process messages before sending them.
	processCallback ProcessMessageFunc[MessageType]
	mu              sync.Mutex
}

// NewExecuteRequestForwarder creates a new ExecuteRequestForwarder struct and returns a pointer to it.
func NewExecuteRequestForwarder[MessageType any](notifyCallback scheduling.NotificationCallback,
	processCallback ProcessMessageFunc[MessageType]) *ExecuteRequestForwarder[MessageType] {

	submitter := &ExecuteRequestForwarder[MessageType]{
		outgoingExecuteRequestQueue:        hashmap.NewCornelkMap[string, chan *enqueuedRequest[MessageType]](128),
		outgoingExecuteRequestQueueMutexes: hashmap.NewCornelkMap[string, *sync.Mutex](128),
		executeRequestQueueStopChannels:    hashmap.NewCornelkMap[string, chan interface{}](128),
		notificationCallback:               notifyCallback,
		processCallback:                    processCallback,
		log:                                config.GetLogger("executeRequestForwarder "),
	}

	return submitter
}

func (s *ExecuteRequestForwarder[MessageType]) EnqueueRequest(msg MessageType, kernel MessageRecipient, msgId string) <-chan interface{} {
	mutex, loaded := s.outgoingExecuteRequestQueueMutexes.Load(kernel.ID())
	if !loaded {
		errorMessage := fmt.Sprintf("Could not find \"execute_request\" queue mutex for kernel \"%s\"", kernel.ID())

		if s.notificationCallback != nil {
			s.notificationCallback(
				"Could Not Find \"execute_request\" Queue Mutex", errorMessage, messaging.ErrorNotification)
		}

		s.log.Error(errorMessage)

		return nil
	}

	// Begin transaction on the outgoing "execute_request" queue for the target kernel.
	mutex.Lock()
	defer mutex.Unlock()

	queue, loaded := s.outgoingExecuteRequestQueue.Load(kernel.ID())
	if !loaded {
		// Should already have been made when kernel replica was first created.
		s.log.Warn("No \"execute_request\" queue found for replica of kernel %s. Creating one now.", kernel.ID())
		queue = make(chan *enqueuedRequest[MessageType], DefaultExecuteRequestQueueSize)
		s.outgoingExecuteRequestQueue.Store(kernel.ID(), queue)
	}

	resultChan := make(chan interface{})

	// This could conceivably block, which would be fine.
	queue <- &enqueuedRequest[MessageType]{
		Msg:           msg,
		MsgId:         msgId,
		ResultChannel: resultChan,
		Kernel:        kernel,
	}

	s.log.Debug("Enqueued \"execute_request\" message(s) with kernel \"%s\"", kernel.ID())

	return resultChan
}

// UnregisterKernel is used to stop the goroutine that is forwarding messages to a particular kernel.
//
// UnregisterKernel will return true if the "stop" channel for the specified kernel is loaded and a notification
// to stop is successfully sent over the channel.
//
// UnregisterKernel is non-blocking.
func (s *ExecuteRequestForwarder[MessageType]) UnregisterKernel(kernelId string) bool {
	stopChan, loaded := s.executeRequestQueueStopChannels.Load(kernelId)
	if !loaded {
		s.log.Warn("Unable to load \"stop channel\" for \"execute_request\" forwarder for kernel %s", kernelId)
		return false
	}

	// Tell the goroutine to stop.
	stopChan <- struct{}{}
	return true
}

// RegisterKernel creates the necessary internal infrastructure to send "execute_request" messages to a kernel.
func (s *ExecuteRequestForwarder[MessageType]) RegisterKernel(kernel MessageRecipient,
	requestHandler RequestHandler[MessageType], responseHandler scheduling.KernelReplicaMessageHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kernelId := kernel.ID()

	executeRequestStopChan := make(chan interface{}, 1)
	executeRequestQueue := make(chan *enqueuedRequest[MessageType], DefaultExecuteRequestQueueSize)

	s.outgoingExecuteRequestQueue.Store(kernelId, executeRequestQueue)
	s.outgoingExecuteRequestQueueMutexes.Store(kernelId, &sync.Mutex{})
	s.executeRequestQueueStopChannels.Store(kernelId, executeRequestStopChan)

	s.log.Debug("Registered kernel \"%s\" with executeRequestForwarder.", kernelId)

	go s.forwardRequests(executeRequestQueue, executeRequestStopChan, kernel, requestHandler, responseHandler)
}

// executeRequestForwarderLoop forwards Jupyter "execute_request" messages to a particular kernel replica
// in a first-come, first-serve manner.
//
// executeRequestForwarderLoop is meant to be called in its own goroutine.
//
// Important: if there are more concurrent execute_request messages sent than the capacity of the buffered channel that
// serves as the message queue, then the first-come, first-serve ordering of the messages cannot be guaranteed.
// Specifically, the order in which the messages are enqueued is non-deterministic.
// (Once enqueued, the messages will be served in a first-come, first-serve manner.)
func (s *ExecuteRequestForwarder[MessageType]) forwardRequests(queue chan *enqueuedRequest[MessageType],
	stopChan chan interface{}, kernel MessageRecipient, requestHandler RequestHandler[MessageType],
	responseHandler scheduling.KernelReplicaMessageHandler) {

	for {
		select {
		case enqueuedMessage := <-queue:
			{
				s.forwardExecuteRequest(enqueuedMessage, kernel, requestHandler, responseHandler)
			}
		case <-stopChan:
			{
				return
			}
		}
	}
}

// forwardExecuteRequest forwards an "execute_request" (or "yield_request") message to the target kernel.
//
// forwardExecuteRequest is called by executeRequestForwarderLoop.
func (s *ExecuteRequestForwarder[MessageType]) forwardExecuteRequest(message *enqueuedRequest[MessageType],
	kernel MessageRecipient, requestHandler RequestHandler[MessageType], handler scheduling.KernelReplicaMessageHandler) {
	s.log.Debug("Forwarding \"execute_request\" message(s) to kernel \"%s\"", kernel.ID())

	// Sanity check.
	// Ensure that the message is meant for the same kernel replica that this thread is responsible for.
	if message.Kernel.ID() != kernel.ID() {
		errorMessage := fmt.Sprintf("Found enqueued message \"%s\" with mismatched kernel ID. "+
			"Enqueued message kernel ID: \"%s\". Expected kernel ID: \"%s\"", message.MsgId, message.Kernel.ID(), kernel.ID())

		if s.notificationCallback != nil {
			s.notificationCallback("Enqueued Message with Mismatched kernel ID", errorMessage, messaging.ErrorNotification)
		}

		s.log.Error(errorMessage)

		return // We'll panic before this line is executed in the local daemon.
	}

	s.log.Debug("Dequeued message %s targeting kernel %s.",
		message.MsgId, message.Kernel.ID())

	// Process the message.
	processedMessage := message.Msg
	if s.processCallback != nil {
		processedMessage = s.processCallback(processedMessage, message.Kernel)
	}

	s.log.Debug("Forwarding \"execute_request\" message \"%s\" to kernel %s: %s",
		message.MsgId, message.Kernel.ID(), processedMessage)

	// Sanity check.
	if message.Kernel.IsTraining() {
		log.Fatalf(utils.RedStyle.Render("kernel %s is already training, even though we haven't sent the "+
			"next \"execute_request\"/\"yield_execute\" request yet. Started training at: %v (i.e., %v ago)."),
			message.Kernel.ID(), message.Kernel.LastTrainingStartedAt(), time.Since(message.Kernel.LastTrainingStartedAt()))
	}

	// Send the message and post the result back to the caller via the channel included within
	// the enqueued "execute_request" message.
	ctx, cancel := context.WithCancel(context.Background())
	err := requestHandler(
		ctx, "Forwarding", messaging.ShellMessage, processedMessage, handler, func() {
			s.log.Debug("SetDone() called for shell execute/yield message \"%s\" targeting kernel %s. "+
				"Cancelling (though request may have succeeded already).", message.MsgId, message.Kernel.ID())
			cancel()
		})

	if err != nil {
		// Send the error back to the caller.
		message.ResultChannel <- err
		return
	}

	// General notification that we're done, and there was no error.
	message.ResultChannel <- struct{}{}

	s.log.Debug("Finished forwarding \"execute_request\" message \"%s\" to kernel \"%s\".",
		message.MsgId, message.Kernel.ID())
}
