package types

import (
	"context"
	"errors"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	// The request has been created, but it has not yet been submitted.
	RequestStateInit RequestState = "RequestInitializing"

	// The request has been created and submitted, but we've not yet received an ACK.
	RequestStateSubmitted RequestState = "RequestSubmitted"

	// The request has been created, submitted, and ACK'd.
	// We've not yet received a response for the request.
	RequestStateProcessing RequestState = "RequestProcessing"

	// An error state.
	// The request timed-out, either because no ACK was received in time, or because
	// no result was received in time (despite the request being ACK'd).
	RequestStateTimedOut RequestState = "RequestTimedOut"

	// The request was created, submitted, ACK'd (if ACKs are required), and the result was received.
	// If no result is required, then a request enters the RequestStateComplete state after receiving an ACK.
	// If no result is required AND no ACK is required, then a request enters the RequestStateComplete state after being sent without an error.
	RequestStateComplete RequestState = "RequestComplete"

	// The request was explicitly cancelled by the client.
	RequestStateExplicitlyCancelled RequestState = "RequestExplicitlyCancelled"

	// The request encountered some irrecoverable error while being sent (other than timing out).
	RequestStateErred RequestState = "RequestErred"
)

var (
	ErrValidationFailed = errors.New("failed to validate message frames")
)

// Helper function. Only shell and control messages should require ACKs.
func ShouldMessageRequireAck(typ MessageType) bool {
	return typ == ShellMessage || typ == ControlMessage
}

// Default 'done' handler for requests.
// Does nothing.
func DefaultDoneHandler() {}

// Default message/response handler for requests.
// Simply returns nil.
func DefaultMessageHandler(server JupyterServerInfo, typ MessageType, msg *zmq4.Msg) error {
	return nil
}

type RequestState string

// Interface representing a Request to be passed to Server::Request.
// This interface is designed to encapsulate a number of options that may be passed to the Server::Request method.
type Request interface {
	// Return the associated Context.
	Context() context.Context

	// Return the associated cancel function, if one exists. Otherwise, return nil.
	GetCancelFunc() context.CancelFunc

	// Indicates whether the call to Server::Request block when issuing this request
	// True indicates blocking; false indicates non-blocking (i.e., Server::Request will return immediately, rather than wait for a response for returning)
	// Default: true
	IsBlocking() bool

	// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
	// Default: infinite.
	//
	// If the returned bool is true, then the timeout is valid.
	// If it is false, then the timeout is invalid, meaning the request does not timeout.
	Timeout() (time.Duration, bool)

	// The maximum number of attempts allowed before giving up on sending the request.
	// Default: 3
	MaxNumAttempts() int

	// This is not configurable.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	RequestId() string

	// Should the destination frame be automatically removed?
	// Default: true
	ShouldDestFrameBeRemoved() bool

	// Should the request require ACKs
	RequiresAck() bool

	// The message itself.
	Payload() *JupyterMessage

	// Return the message ID taken from the Jupyter header of the message.
	JupyterMessageId() string

	// Return the message type taken from the Jupyter header of the message.
	JupyterMessageType() string

	// Return the timestamp taken from the Jupyter header of the message.
	JupyterTimestamp() (ts time.Time, err error)

	// Return the "done" callback for this request.
	// This callback is executed when the response is received and the request is handled.
	DoneCallback() MessageDone

	// Return the handler that is called to process the response to this request.
	MessageHandler() MessageHandler

	// The ID associated with the source of the message.
	// This will typically be a kernel ID.
	SourceID() string

	// DestID extracted from the request payload.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	DestinationId() string

	// Offset/index of start of Jupyter frames within message frames.
	Offset() (jOffset int)

	// Return the associated Context and the associated cancel function, if one exists.
	ContextAndCancel() (context.Context, context.CancelFunc)

	// Return the current state of the request.
	RequestState() RequestState

	// Return true if the request has been ACK'd.
	HasBeenAcknowledged() bool

	// Return true if the request was completed successfully.
	HasCompleted() bool

	// Return true if the request timed-out.
	// If it timed-out and was later submitted successfully, then this will be true.
	TimedOut() bool

	// Return true if the request is currently in the timed-out state.
	IsTimedOut() bool

	// Return true if the request was explicitly cancelled by the user.
	WasExplicitlyCancelled() bool

	// Update the timestamp of the message'r header so that it is signed with a different signature.
	// This is used when re-sending un-ACK'd (unacknowledged) messages.
	PrepareForResubmission() error

	// Return the entity responsible for providing access to sockets in the request handler.
	SocketProvider() JupyterServerInfo
}

// Encapsulates the state of a live, active request.
type liveRequestState struct {
	requestState RequestState

	// Flag indicating whether the request was ever ACK'd.
	// This allows us to recover this information if the request enters the RequestTimedOut state.
	hasBeenAcknowledged bool

	// Flag indicating whether the request timed out at least once.
	timedOut bool

	// Flag indicating whether the request encountered some irrecoverable error while being sent (other than timing out).
	// If the request only timed out, then erred will be false.
	erred bool

	// The irrecoverable error encountered by the request. This will be nil if no such error was encountered.
	err error

	// Flag indicating whether the request was explicitly cancelled by the client.
	wasExplicitlyCancelled bool
}

type basicRequest struct {
	*liveRequestState

	log logger.Logger

	// We keep a reference to the parent context that was passed to the RequestBuilder
	// so that we can create a new child context if we're resubmitted.
	parentContext context.Context
	ctx           context.Context

	cancel context.CancelFunc

	// True indicates that the request must be ACK'd by the recipient.
	requiresAck bool

	// How long to wait for the request to complete successfully.
	// Completion is a stronger requirement than simply being ACK'd.
	timeout time.Duration

	// This is flipped to true when a timeout is explicitly configured within the RequestBuilder.
	// We use this to determine if we should recreate a context during resubmission via Context::WithTimeout or Context::WithCancel.
	hasTimeout bool

	// Should the call to Server::Request block when issuing this request?
	// True if yes; otherwise, false.
	isBlocking bool

	// The maximum number of attempts allowed before giving up on sending the request.
	maxNumAttempts int

	// String that uniquely identifies this set of request options.
	// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
	requestId string

	// Should the destination frame be automatically removed?
	// If yes, then this should be true.
	shouldDestFrameBeRemoved bool

	// The actual payload.
	payload *JupyterMessage

	// This callback is executed when the response is received and the request is handled.
	// TODO: Might be better to turn this into more of a "clean-up"? Or something?
	doneCallback MessageDone

	// The handler that is called to process the response to this request.
	messageHandler MessageHandler

	// The ID associated with the source of the message.
	// This will typically be a kernel ID.
	sourceId string

	// DestID extracted from the request payload.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	destinationId string

	// The connection info of the remote target of the request.
	connectionInfo *ConnectionInfo

	// The entity responsible for providing access to sockets in the request handler.
	socketProvider JupyterServerInfo
}

// Should the call to Server::Request block when issuing this request?
func (r *basicRequest) IsBlocking() bool {
	return r.isBlocking
}

// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
// Default: infinite.
//
// If the returned bool is true, then the timeout is valid.
// If it is false, then the timeout is invalid, meaning the request does not timeout.
func (r *basicRequest) Timeout() (time.Duration, bool) {
	return r.timeout, r.hasTimeout
}

// The maximum number of attempts allowed before giving up on sending the request.
func (r *basicRequest) MaxNumAttempts() int {
	return r.maxNumAttempts
}

// String that uniquely identifies this set of request options.
// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
func (r *basicRequest) RequestId() string {
	return r.requestId
}

// Should the destination frame be automatically removed?
func (r *basicRequest) ShouldDestFrameBeRemoved() bool {
	return r.shouldDestFrameBeRemoved
}

// Should the request require ACKs
func (r *basicRequest) RequiresAck() bool {
	return r.requiresAck
}

// The message itself.
func (r *basicRequest) Payload() *JupyterMessage {
	return r.payload
}

// Return the "done" callback for this request.
// This callback is executed when the response is received and the request is handled.
func (r *basicRequest) DoneCallback() MessageDone {
	return r.doneCallback
}

// Return the handler that is called to process the response to this request.
func (r *basicRequest) MessageHandler() MessageHandler {
	return r.messageHandler
}

// The ID associated with the source of the message.
// This will typically be a kernel ID.
func (r *basicRequest) SourceID() string {
	return r.sourceId
}

// DestID extracted from the request payload.
// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
func (r *basicRequest) DestinationId() string {
	return r.destinationId
}

// Return the associated Context.
func (r *basicRequest) Context() context.Context {
	return r.ctx
}

// Return the associated cancel function.
func (r *basicRequest) GetCancelFunc() context.CancelFunc {
	return r.cancel
}

// Cancel the request and return nil.
// If the request is not cancellable, then return ErrNoCancelConfigured.
// If the request has already been completed, then return ErrRequestAlreadyCompleted.
func (r *basicRequest) Cancel() error {
	if r.cancel != nil {
		r.cancel()
		r.liveRequestState.wasExplicitlyCancelled = true
		return nil
	}

	// This probably shouldn't happen.
	return ErrNoCancelConfigured
}

// Return the associated Context and the associated cancel function, if one exists.
func (r *basicRequest) ContextAndCancel() (context.Context, context.CancelFunc) {
	return r.ctx, r.cancel
}

// Offset/index of start of Jupyter frames within message frames.
func (r *basicRequest) Offset() (jOffset int) {
	_, _, jOffset = ExtractDestFrame(r.payload.Frames)
	return
}

// Return the current state of the request.
func (r *basicRequest) RequestState() RequestState {
	return r.requestState
}

// Return true if the request has been ACK'd.
func (r *basicRequest) HasBeenAcknowledged() bool {
	return r.liveRequestState.hasBeenAcknowledged
}

// Return true if the request was completed successfully.
func (r *basicRequest) HasCompleted() bool {
	return r.requestState == RequestStateComplete
}

// Return true if the request ever timed-out.
// If it timed-out and was later submitted successfully, then this will be true.
func (r *basicRequest) TimedOut() bool {
	return r.liveRequestState.timedOut
}

// Return true if the request is currently in the timed-out state.
func (r *basicRequest) IsTimedOut() bool {
	return r.requestState == RequestStateTimedOut
}

// Return true if the request was explicitly cancelled by the user.
func (r *basicRequest) WasExplicitlyCancelled() bool {
	return r.liveRequestState.wasExplicitlyCancelled
}

// Return t message ID taken from the Jupyter header of the message.
func (r *basicRequest) JupyterMessageId() string {
	return r.payload.Header.MsgID
}

// Return t message type taken from the Jupyter header of the message.
func (r *basicRequest) JupyterMessageType() string {
	return r.payload.Header.MsgType
}

// Return the timestamp taken from the Jupyter header of the message.
func (r *basicRequest) JupyterTimestamp() (ts time.Time, err error) {
	ts, err = time.Parse(time.RFC3339Nano, r.payload.Header.Date)
	if err != nil {
		return time.Time{}, err
	}

	return ts, err
}

// Return the entity responsible for providing access to sockets in the request handler.
func (r *basicRequest) SocketProvider() JupyterServerInfo {
	return r.socketProvider
}

// Update the timestamp of the message'r header so that it is signed with a different signature.
// This is used when re-sending un-ACK'd (unacknowledged) messages.
func (r *basicRequest) PrepareForResubmission() error {
	// Get the date.
	date, _ := r.JupyterTimestamp()

	// Add a single microsecond to the date.
	modifiedDate := date.Add(time.Microsecond)

	// Change the date in the header.
	r.payload.Header.Date = modifiedDate.Format(time.RFC3339Nano)

	// Re-encode the header.
	jFrames := JupyterFrames(r.payload.Frames)
	jOffset := r.Offset()

	err := jFrames[jOffset:].EncodeHeader(r.payload.Header)
	if err != nil {
		return err
	}

	// Regenerate the signature.
	_, err = jFrames[jOffset:].Sign(r.connectionInfo.SignatureScheme, []byte(r.connectionInfo.Key))
	if err != nil {
		return err
	}

	r.payload.Frames = jFrames

	// We're going to recreate the context.
	if r.cancel != nil {
		r.cancel()
	}

	// Recreate the context using the parent context that was provided to us when we were first created.
	// If we're meant to have a finite timeout, then we'll create the context using Context::WithTimeout.
	// Alternatively, we'll create the context via Context::WithCancel.
	if r.hasTimeout {
		r.ctx, r.cancel = context.WithTimeout(r.parentContext, r.timeout)
	} else {
		r.ctx, r.cancel = context.WithCancel(r.parentContext)
	}

	if verified := ValidateFrames([]byte(r.connectionInfo.Key), r.connectionInfo.SignatureScheme, jOffset, jFrames); !verified {
		r.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'", r.connectionInfo.SignatureScheme, r.connectionInfo.Key)
		r.log.Error("This message will be rejected by the kernel unless the kernel has been configured to skip validating/verifying the messages:\n%v", jFrames)

		return ErrValidationFailed
	}

	return nil
}
