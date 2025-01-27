package daemon

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
type enqueuedRequest struct {
	Msg           *messaging.JupyterMessage
	ResultChannel chan interface{}
	Kernel        MessageRecipient
}

// MessageRecipient is an interface that enables ExecutionSubmitter to work with both scheduling.Kernel
// and scheduling.KernelReplica instances.
type MessageRecipient interface {
	// ID returns the kernel ID of the target MessageRecipient.
	ID() string

	// SendingExecuteRequest records that an "execute_request" message has been sent to the kernel.
	//
	// SendingExecuteRequest will panic if the given messaging.JupyterMessage is not an "execute_request"
	// message or a "yield_request" message.
	SendingExecuteRequest(msg *messaging.JupyterMessage)

	// IsTraining returns true if the target MessageRecipient is actively training.
	IsTraining() bool

	// TrainingStartedAt returns the time at which the target MessageRecipient last began training.
	TrainingStartedAt() time.Time

	RequestWithHandler(ctx context.Context, _ string, typ messaging.MessageType, msg *messaging.JupyterMessage, handler scheduling.KernelReplicaMessageHandler, done func()) error
}

type ProcessMessageFunc func(msg *messaging.JupyterMessage, kernel MessageRecipient) *messaging.JupyterMessage

// ExecutionSubmitter ensures that "execute_request" (and "yield_request") messages are sent one-at-a-time.
type ExecutionSubmitter struct {
	mu  sync.Mutex
	log logger.Logger

	// outgoingExecuteRequestQueue is used to send "execute_request" messages one-at-a-time to their target kernels.
	outgoingExecuteRequestQueue hashmap.HashMap[string, chan *enqueuedRequest]

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
	processCallback ProcessMessageFunc
}

// NewExecutionSubmitter creates a new ExecutionSubmitter struct and returns a pointer to it.
func NewExecutionSubmitter(notifyCallback scheduling.NotificationCallback, processCallback ProcessMessageFunc) *ExecutionSubmitter {
	submitter := &ExecutionSubmitter{
		outgoingExecuteRequestQueue:        hashmap.NewCornelkMap[string, chan *enqueuedRequest](128),
		outgoingExecuteRequestQueueMutexes: hashmap.NewCornelkMap[string, *sync.Mutex](128),
		executeRequestQueueStopChannels:    hashmap.NewCornelkMap[string, chan interface{}](128),
		notificationCallback:               notifyCallback,
		processCallback:                    processCallback,
	}

	config.InitLogger(&submitter.log, submitter)

	return submitter
}

func (s *ExecutionSubmitter) EnqueueRequest(msg *messaging.JupyterMessage, kernel MessageRecipient) <-chan interface{} {
	msgType := msg.JupyterMessageType()
	if msgType != messaging.ShellExecuteRequest && msgType != messaging.ShellYieldRequest {
		s.log.Error("Invalid message type \"%s\" in outgoing \"execute_request\" queue of kernel \"%s\".",
			msgType, kernel.ID())
		return nil
	}

	mutex, loaded := s.outgoingExecuteRequestQueueMutexes.Load(kernel.ID())
	if !loaded {
		errorMessage := fmt.Sprintf("Could not find \"execute_request\" queue mutex for kernel \"%s\"", kernel.ID())
		s.notificationCallback(
			"Could Not Find \"execute_request\" Queue Mutex", errorMessage, messaging.ErrorNotification)
		return nil
	}

	// Begin transaction on the outgoing "execute_request" queue for the target kernel.
	mutex.Lock()
	defer mutex.Unlock()

	queue, loaded := s.outgoingExecuteRequestQueue.Load(kernel.ID())
	if !loaded {
		// Should already have been made when kernel replica was first created.
		s.log.Warn("No \"execute_request\" queue found for replica of kernel %s. Creating one now.", kernel.ID())
		queue = make(chan *enqueuedRequest, DefaultExecuteRequestQueueSize)
		s.outgoingExecuteRequestQueue.Store(kernel.ID(), queue)
	}

	resultChan := make(chan interface{})

	queueLength := float64(len(queue))
	warningThreshold := 0.75 * float64(cap(queue))
	if queueLength > warningThreshold {
		// If the queue is quite full, then we'll print a warning.
		// Once the queue is full, the order in which future requests are processed is no longer guaranteed.
		// Specifically, the order in which new items are added to the queue is non-deterministic.
		// (Once in the queue, requests will still be processed in a FCFS manner.)
		s.log.Warn("Enqueuing outbound \"%s\" %s targeting kernel %s. Queue is almost full: %d/%d. IsTraining: %v.",
			msg.JupyterMessageType(), msg.JupyterMessageId(), kernel.ID(), int(queueLength), cap(queue), kernel.IsTraining())
	} else {
		s.log.Debug("Enqueuing outbound \"%s\" %s targeting kernel %s. Queue size: %d/%d. IsTraining: %v.",
			msg.JupyterMessageType(), msg.JupyterMessageId(), kernel.ID(), int(queueLength), cap(queue), kernel.IsTraining())
	}

	// This could conceivably block, which would be fine.
	queue <- &enqueuedRequest{
		Msg:           msg,
		ResultChannel: resultChan,
		Kernel:        kernel,
	}

	return resultChan
}

// RegisterKernel creates the necessary internal infrastructure to send "execute_request" messages to a kernel.
func (s *ExecutionSubmitter) RegisterKernel(kernel MessageRecipient, handler scheduling.KernelReplicaMessageHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kernelId := kernel.ID()

	executeRequestStopChan := make(chan interface{}, 1)
	executeRequestQueue := make(chan *enqueuedRequest, DefaultExecuteRequestQueueSize)

	s.outgoingExecuteRequestQueue.Store(kernelId, executeRequestQueue)
	s.outgoingExecuteRequestQueueMutexes.Store(kernelId, &sync.Mutex{})
	s.executeRequestQueueStopChannels.Store(kernelId, executeRequestStopChan)

	s.log.Debug("Registered kernel \"%s\" with ExecutionSubmitter.", kernelId)

	go s.forwardRequests(executeRequestQueue, executeRequestStopChan, kernel, handler)
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
func (s *ExecutionSubmitter) forwardRequests(queue chan *enqueuedRequest, stopChan chan interface{},
	kernel MessageRecipient, handler scheduling.KernelReplicaMessageHandler) {

	for {
		select {
		case enqueuedMessage := <-queue:
			{
				s.forwardExecuteRequest(enqueuedMessage, kernel, handler)
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
func (s *ExecutionSubmitter) forwardExecuteRequest(message *enqueuedRequest, kernel MessageRecipient,
	handler scheduling.KernelReplicaMessageHandler) {

	// Sanity check.
	// Ensure that the message is meant for the same kernel replica that this thread is responsible for.
	if message.Kernel.ID() != kernel.ID() {
		errorMessage := fmt.Sprintf("Found enqueued \"%s\" message with mismatched kernel ID. "+
			"Enqueued message kernel ID: \"%s\". Expected kernel ID: \"%s\"",
			message.Msg.JupyterMessageType(), message.Kernel.ID(), kernel.ID())

		s.notificationCallback(fmt.Sprintf("Enqueued \"%s\" with Mismatched Kernel ID",
			message.Msg.JupyterMessageType()), errorMessage, messaging.ErrorNotification)

		return // We'll panic before this line is executed.
	}

	s.log.Debug("Dequeued shell \"%s\" message %s (JupyterID=%s) targeting kernel %s.",
		message.Msg.JupyterMessageType(), message.Msg.RequestId, message.Msg.JupyterMessageId(), message.Kernel.ID())

	// Process the message.
	processedMessage := s.processCallback(message.Msg, message.Kernel) // , header, offset)
	s.log.Debug("Forwarding shell \"%s\" to kernel %s: %s",
		message.Msg.JupyterMessageType(), message.Kernel.ID(), processedMessage)

	// Sanity check.
	if message.Kernel.IsTraining() {
		log.Fatalf(utils.RedStyle.Render("Kernel %s is already training, even though we haven't sent the "+
			"next \"execute_request\"/\"yield_execute\" request yet. Started training at: %v (i.e., %v ago)."),
			message.Kernel.ID(), message.Kernel.TrainingStartedAt(), time.Since(message.Kernel.TrainingStartedAt()))
	}

	// Record that we've sent this (although technically we haven't yet).
	kernel.SendingExecuteRequest(processedMessage)

	// Send the message and post the result back to the caller via the channel included within
	// the enqueued "execute_request" message.
	ctx, cancel := context.WithCancel(context.Background())
	err := message.Kernel.RequestWithHandler(
		ctx, "Forwarding", messaging.ShellMessage, processedMessage, handler, func() {
			s.log.Debug("Done() called for shell \"%s\" message targeting kernel %s. "+
				"Cancelling (though request may have succeeded already).",
				processedMessage.JupyterMessageType(), message.Kernel.ID())
			cancel()
		})

	if err != nil {
		// Send the error back to the caller.
		message.ResultChannel <- err
		return
	}

	// General notification that we're done, and there was no error.
	message.ResultChannel <- struct{}{}
}
