package messaging

import (
	"context"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/jupyter"
	"slices"
	"sync"
	"time"

	"github.com/Scusemua/go-utils/logger"
)

const (
	// RequestStateInit indicates that the request has been created, but it has not yet been submitted.
	RequestStateInit RequestState = "RequestInitializing"

	// RequestStateSubmitted indicates that the request has been created and submitted, but we've not yet received an ACK.
	RequestStateSubmitted RequestState = "RequestSubmitted"

	// RequestStateProcessing indicates that the request has been created, submitted, and acknowledged.
	// We've not yet received a response for the request.
	RequestStateProcessing RequestState = "RequestProcessing"

	// RequestStateTimedOut is an error state.
	// The request timed-out, either because no ACK was received in time, or because
	// no result was received in time (despite the request being acknowledged).
	RequestStateTimedOut RequestState = "RequestTimedOut"

	// RequestStateComplete indicates that the request was created, submitted, acknowledged (if ACKs are required),
	// and the result was received.
	//
	// If no result is required, then a request enters the RequestStateComplete state after receiving an ACK.
	//
	// If no result is required AND no ACK is required, then a request enters the RequestStateComplete state after being
	// sent without an error.
	RequestStateComplete RequestState = "RequestComplete"

	// RequestStateExplicitlyCancelled indicates that the request was explicitly cancelled by the client.
	RequestStateExplicitlyCancelled RequestState = "RequestExplicitlyCancelled"

	// RequestStateErred indicates that the request encountered some irrecoverable error while being sent (other than timing out).
	RequestStateErred RequestState = "RequestErred"
)

var (
	ErrValidationFailed = errors.New("failed to validate message frames")

	ErrIllegalTransition = errors.New("illegal transition")

	// IllegalSourceStates is a map from "destination" or "to" state to a slice of illegal "source" or "from" states.
	// For example, RequestStateTimedOut is a key that is mapped to a slice of the states from which it is illegal to transition to the RequestStateTimedOut state.
	IllegalSourceStates = map[RequestState][]RequestState{
		RequestStateInit:                {RequestStateSubmitted, RequestStateProcessing, RequestStateProcessing, RequestStateTimedOut, RequestStateComplete, RequestStateExplicitlyCancelled, RequestStateErred},
		RequestStateSubmitted:           {RequestStateComplete},
		RequestStateProcessing:          {RequestStateComplete},
		RequestStateTimedOut:            {RequestStateComplete},
		RequestStateComplete:            {RequestStateComplete},
		RequestStateExplicitlyCancelled: {RequestStateComplete},
		RequestStateErred:               {RequestStateComplete},
	}

	// IllegalDestinationStates is a map from "source" or "from" state to a slice of illegal "destination" or "to" states.
	// For example, RequestStateInit is a key that is mapped to a slice of the states to which it is illegal to transition from the RequestStateInit state.
	IllegalDestinationStates = map[RequestState][]RequestState{
		RequestStateInit:                {RequestStateInit},
		RequestStateSubmitted:           {RequestStateInit},
		RequestStateProcessing:          {RequestStateInit},
		RequestStateTimedOut:            {RequestStateInit},
		RequestStateComplete:            {RequestStateInit},
		RequestStateExplicitlyCancelled: {RequestStateInit},
		RequestStateErred:               {RequestStateInit},
	}
)

// ShouldMessageRequireAck is a helper function. Only shell and control messages should require ACKs.
func ShouldMessageRequireAck(typ MessageType) bool {
	return typ == ShellMessage || typ == ControlMessage
}

// DefaultDoneHandler is the default 'done' handler for requests.
// Does nothing.
func DefaultDoneHandler() {}

// DefaultMessageHandler is the default message/response handler for requests.
// Simply returns nil.
func DefaultMessageHandler(_ JupyterServerInfo, _ MessageType, _ *JupyterMessage) error {
	return nil
}

type RequestState string

// Request is an interface representing a Request to be passed to Server::Request.
// This interface is designed to encapsulate a number of options that may be passed to the Server::Request method.
type Request interface {
	// Context returns the associated Context.
	Context() context.Context

	// GetCancelFunc returns the associated cancel function, if one exists. Otherwise, return nil.
	GetCancelFunc() context.CancelFunc

	// IsBlocking indicates whether the call to Server::Request block when issuing this request
	// True indicates blocking; false indicates non-blocking (i.e., Server::Request will return immediately, rather than wait for a response for returning)
	//
	// Default: true
	IsBlocking() bool

	// BeganSendingAt returns the time at which the Sender began submitting the Request.
	//
	// If false is also returned, then the Request hasn't started being sent yet,
	// so the returned time.Time is meaningless.
	BeganSendingAt() (time.Time, bool)

	// SendStarting should be called when the Sender begins submitting the Request for the first time.
	SendStarting()

	// Timeout returns how long to wait for the request to complete successfully. Completion is a stronger requirement
	// than simply being acknowledged.
	//
	// Default: infinite.
	//
	// If the returned bool is true, then the timeout is valid.
	// If it is false, then the timeout is invalid, meaning the request does not time out.
	Timeout() (time.Duration, bool)

	// MaxNumAttempts returns the maximum number of attempts allowed before giving up on sending the request.
	//
	// Default: 3
	MaxNumAttempts() int

	// RequestId is not configurable.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	RequestId() string

	// MessageType is the MessageType of the request.
	MessageType() MessageType

	// ShouldDestFrameBeRemoved returns a flag indicating whether the destination frame be automatically removed?
	//
	// Default: true
	ShouldDestFrameBeRemoved() bool

	// RequiresAck returns a flag indicating whether the request require ACKs
	RequiresAck() bool

	// AckTimeout is the amount of time that the Sender should wait for the Request to be acknowledged by the
	// recipient before either resubmitting the Request or returning an error.
	//
	// Default: 5 seconds.
	AckTimeout() time.Duration

	// Payload returns the message itself.
	Payload() *JupyterMessage

	// JupyterMessageId returns the message ID taken from the Jupyter header of the message.
	JupyterMessageId() string

	// JupyterMessageType returns the message type taken from the Jupyter header of the message.
	JupyterMessageType() string

	// JupyterTimestamp returns the timestamp taken from the Jupyter header of the message.
	JupyterTimestamp() (ts time.Time, err error)

	// DoneCallback returns the "done" callback for this request.
	// This callback is executed when the response is received and the request is handled.
	DoneCallback() MessageDone

	// MessageHandler returns the handler that is called to process the response to this request.
	MessageHandler() MessageHandler

	// SourceID returns the ID associated with the source of the message.
	// This will typically be a kernel ID.
	SourceID() string

	// DestinationId returns the DestID extracted from the request payload.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	DestinationId() string

	// Offset returns the offset/index of start of Jupyter frames within message frames.
	Offset() (jOffset int)

	// ContextAndCancel returns the associated Context and the associated cancel function, if one exists.
	ContextAndCancel() (context.Context, context.CancelFunc)

	// RequestState returns the current state of the request.
	RequestState() RequestState

	// HasBeenAcknowledged returns true if the request has been acknowledged.
	HasBeenAcknowledged() bool

	// HasCompleted returns true if the request was completed successfully.
	HasCompleted() bool

	// TimedOut returns true if the request timed-out.
	// If it timed-out and was later submitted successfully, then this will be true.
	TimedOut() bool

	// IsTimedOut returns true if the request is currently in the timed-out state.
	IsTimedOut() bool

	// WasExplicitlyCancelled returns true if the request was explicitly cancelled by the user.
	//
	// This will return true if the request was explicitly cancelled at any point.
	// If the request was explicitly cancelled and then successfully resubmitted, then this will still return true.
	//
	// If you want to know if the request is currently in the RequestStateExplicitlyCancelled state, then use the Request::IsExplicitlyCancelled function.
	WasExplicitlyCancelled() bool

	// IsExplicitlyCancelled returns true if the request was explicitly cancelled by the user AND is presently in the RequestStateExplicitlyCancelled state.
	//
	// IsExplicitlyCancelled will return true only if the request is currently in the explicitly cancelled state.
	// If the request was explicitly cancelled and then was later successfully resubmitted, then this will return false.
	//
	// If you want to know if the request was ever explicitly cancelled by the user, regardless of its present state, then use the Request::WasExplicitlyCancelled function.
	IsExplicitlyCancelled() bool

	// PrepareForResubmission updates the timestamp of the message's header so that it is signed with a different signature.
	// This is used when re-sending unacknowledged messages.
	PrepareForResubmission() error

	// SocketProvider returns the entity responsible for providing access to sockets in the request handler.
	SocketProvider() JupyterServerInfo

	// SetSubmitted transitions to the `RequestStateSubmitted` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateSubmitted' is invalid), then this will return an error.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetSubmitted() (bool, error)

	// SetProcessing transitions to the `RequestStateProcessing` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateProcessing' is invalid), then this will return an error.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetProcessing() (bool, error)

	// SetTimedOut transitions to the `RequestStateTimedOut` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateTimedOut' is invalid), then this will return an error.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetTimedOut() (bool, error)

	// SetComplete transitions to the `RequestStateComplete` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateComplete' is invalid), then this will return an error.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetComplete() (bool, error)

	// SetExplicitlyCancelled transitions to the `RequestStateExplicitlyCancelled` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateExplicitlyCancelled' is invalid), then this will return an error.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetExplicitlyCancelled() (bool, error)

	// SetErred transitions to the `RequestStateErred` state.
	// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateErred' is invalid), then this will return an error.
	//
	// Updates the `err` field of the live request state.
	//
	// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
	SetErred(err error) (bool, error)

	// String returns a string representation of the request.
	String() string

	// CurrentAttemptNumber returns the current send-attempt number of the Request.
	CurrentAttemptNumber() int

	// IncrementAttemptNumber increments the Request's send-attempt number and returns the new value.
	IncrementAttemptNumber() int
}

// Encapsulates the state of a live, active request.
type liveRequestState struct {
	requestState RequestState

	// Flag indicating whether the request has been acknowledged.
	// This allows us to recover this information if the request enters the RequestTimedOut state.
	hasBeenAcknowledged bool

	// Flag indicating whether the request timed out at least once.
	timedOut bool

	// Flag indicating whether the request encountered some irrecoverable error while being sent (other than timing out).
	// If the request only timed out, then erred will be false.
	erred bool

	// The irrecoverable error encountered by the request. This will be nil if no such error was encountered.
	err error

	// beganSendingAt is the time at which the Sender began sending the Request for the first time.
	beganSendingAt time.Time

	// sendingHasStarted indicates whether the Sender has started sending the Request.
	sendingHasStarted bool

	// Flag indicating whether the request was explicitly cancelled by the client.
	wasExplicitlyCancelled bool
}

type BasicRequest struct {
	*liveRequestState

	log logger.Logger

	// We keep a reference to the parent context that was passed to the RequestBuilder
	// so that we can create a new child context if we're resubmitted.
	parentContext context.Context
	ctx           context.Context

	cancel context.CancelFunc

	// The MessageType of the request.
	messageType MessageType

	// True indicates that the request must be acknowledged by the recipient.
	requiresAck bool

	// ackTimeout is the amount of time that the Sender should wait for the Request to be acknowledged by the
	// recipient before either resubmitting the Request or returning an error.
	//
	// Default: 5 seconds.
	ackTimeout time.Duration

	// How long to wait for the request to complete successfully.
	// Completion is a stronger requirement than simply being acknowledged.
	timeout time.Duration

	// This is flipped to true when a timeout is explicitly configured within the RequestBuilder.
	// We use this to determine if we should recreate a context during resubmission via Context::WithTimeout or Context::WithCancel.
	hasTimeout bool

	// Should the call to Server::Request block when issuing this request?
	// True if yes; otherwise, false.
	isBlocking bool

	// maxNumAttempts is the maximum number of attempts allowed before giving up on sending the request.
	maxNumAttempts int

	// currentAttemptNumber is the current send-attempt number.
	currentAttemptNumber int

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
	connectionInfo *jupyter.ConnectionInfo

	// The entity responsible for providing access to sockets in the request handler.
	socketProvider JupyterServerInfo

	// Synchronizes access to the request's state.
	stateMutex sync.Mutex
}

// SendStarting should be called when the Sender begins submitting the Request for the first time.
func (r *BasicRequest) SendStarting() {
	r.beganSendingAt = time.Now()
	r.sendingHasStarted = true
}

// BeganSendingAt returns the time at which the Sender began submitting the Request.
//
// If false is also returned, then the Request hasn't started being sent yet,
// so the returned time.Time is meaningless.
func (r *BasicRequest) BeganSendingAt() (time.Time, bool) {
	if r.sendingHasStarted {
		return r.beganSendingAt, true
	}

	return time.Time{}, false
}

// CurrentAttemptNumber returns the current send-attempt number of the Request.
func (r *BasicRequest) CurrentAttemptNumber() int {
	return r.currentAttemptNumber
}

// IncrementAttemptNumber increments the Request's send-attempt number and returns the new value.
func (r *BasicRequest) IncrementAttemptNumber() int {
	r.currentAttemptNumber += 1
	return r.currentAttemptNumber
}

// Return a string representation of the request.
func (r *BasicRequest) String() string {
	return fmt.Sprintf("Request[ID=%s, DestID=%s, State=%s, RequiresAck=%v, Timeout=%v, MaxNumAttempts=%d]", r.requestId, r.destinationId, r.requestState, r.requiresAck, r.timeout, r.maxNumAttempts)
}

// IsBlocking returns a bool indicating whether we should the call to Server::Request block when issuing this request?
func (r *BasicRequest) IsBlocking() bool {
	return r.isBlocking
}

// Timeout returns a time.Duration indicating how long to wait for the request to complete successfully.
// Completion is a stronger requirement than simply being acknowledged.
// Default: infinite.
//
// If the returned bool is true, then the timeout is valid.
// If it is false, then the timeout is invalid, meaning the request does not time out.
func (r *BasicRequest) Timeout() (time.Duration, bool) {
	return r.timeout, r.hasTimeout
}

// MaxNumAttempts returns the maximum number of attempts allowed before giving up on sending the request.
func (r *BasicRequest) MaxNumAttempts() int {
	return r.maxNumAttempts
}

// RequestId returns a string identifier that uniquely identifies this set of request options.
// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
func (r *BasicRequest) RequestId() string {
	return r.requestId
}

// ShouldDestFrameBeRemoved returns a bool indicating whether we should the destination frame be automatically removed?
func (r *BasicRequest) ShouldDestFrameBeRemoved() bool {
	return r.shouldDestFrameBeRemoved
}

// AckTimeout is the amount of time that the Sender should wait for the Request to be acknowledged by the
// recipient before either resubmitting the Request or returning an error.
//
// Default: 5 seconds.
func (r *BasicRequest) AckTimeout() time.Duration {
	return r.ackTimeout
}

// RequiresAck returns a bool indicating whether the request require ACKs
func (r *BasicRequest) RequiresAck() bool {
	// Sanity check. If we're set to require ACKs, then just validate that we're either a Control or a Shell message.
	// If we're not, then that indicates a bug.
	if r.requiresAck && !ShouldMessageRequireAck(r.messageType) {
		panic(fmt.Sprintf("Illegal request. Type is %s, yet ACKs are required: %v", r.messageType, r.payload.msg))
	}

	return r.requiresAck
}

// Payload returns the message itself.
func (r *BasicRequest) Payload() *JupyterMessage {
	return r.payload
}

// DoneCallback returns the "done" callback for this request.
// This callback is executed when the response is received and the request is handled.
func (r *BasicRequest) DoneCallback() MessageDone {
	return r.doneCallback
}

// MessageHandler returns the handler that is called to process the response to this request.
func (r *BasicRequest) MessageHandler() MessageHandler {
	return r.messageHandler
}

// SourceID returns the ID associated with the source of the message, which will typically be a kernel ID.
func (r *BasicRequest) SourceID() string {
	return r.sourceId
}

// DestinationId returns the destination ID extracted from the request payload.
// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
// Destination IDs are typically/always the ID of the target kernel.
func (r *BasicRequest) DestinationId() string {
	return r.destinationId
}

// Context returns the associated Context.
func (r *BasicRequest) Context() context.Context {
	return r.ctx
}

// GetCancelFunc returns the associated cancel function.
func (r *BasicRequest) GetCancelFunc() context.CancelFunc {
	return r.cancel
}

// Cancel the request and return nil.
// If the request is not cancellable, then return ErrNoCancelConfigured.
// If the request has already been completed, then return ErrRequestAlreadyCompleted.
func (r *BasicRequest) Cancel() error {
	if r.cancel != nil {
		r.cancel()
		r.liveRequestState.wasExplicitlyCancelled = true
		return nil
	}

	// This probably shouldn't happen.
	return jupyter.ErrNoCancelConfigured
}

// ContextAndCancel returns the associated Context and the associated cancel function, if one exists.
func (r *BasicRequest) ContextAndCancel() (context.Context, context.CancelFunc) {
	return r.ctx, r.cancel
}

// Offset returns the offset/index of start of Jupyter frames within message frames.
func (r *BasicRequest) Offset() (jOffset int) {
	// This forces the offset to be recomputed.
	_, jOffset = r.payload.JupyterFrames.SkipIdentitiesFrame()
	return
}

// RequestState returns the current state of the request.
func (r *BasicRequest) RequestState() RequestState {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.requestState
}

// HasBeenAcknowledged returns true if the request has been acknowledged.
func (r *BasicRequest) HasBeenAcknowledged() bool {
	return r.liveRequestState.hasBeenAcknowledged
}

// HasCompleted return true if the request was completed successfully.
func (r *BasicRequest) HasCompleted() bool {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.requestState == RequestStateComplete
}

// TimedOut returns true if the request ever timed-out.
// If it timed-out and was later submitted successfully, then this will be true.
func (r *BasicRequest) TimedOut() bool {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.liveRequestState.timedOut
}

// IsTimedOut returns true if the request is currently in the timed-out state.
func (r *BasicRequest) IsTimedOut() bool {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.requestState == RequestStateTimedOut
}

// WasExplicitlyCancelled returns true if the request was explicitly cancelled by the user.
//
// This will return true if the request was explicitly cancelled at any point.
// If the request was explicitly cancelled and then successfully resubmitted, then this will still return true.
//
// If you want to know if the request is currently in the RequestStateExplicitlyCancelled state,
// then use the Request::IsExplicitlyCancelled function.
func (r *BasicRequest) WasExplicitlyCancelled() bool {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.liveRequestState.wasExplicitlyCancelled
}

// IsExplicitlyCancelled returns true if the request was explicitly cancelled by the user AND is presently in the
// RequestStateExplicitlyCancelled state.
//
// This will return true only if the request is currently in the explicitly cancelled state.
// If the request was explicitly cancelled and then was later successfully resubmitted, then this will return false.
//
// If you want to know if the request was ever explicitly cancelled by the user, regardless of its present state,
// then use the Request::WasExplicitlyCancelled function.
func (r *BasicRequest) IsExplicitlyCancelled() bool {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	return r.requestState == RequestStateExplicitlyCancelled
}

// JupyterMessageId returns the message ID taken from the Jupyter header of the message.
func (r *BasicRequest) JupyterMessageId() string {
	return r.payload.JupyterMessageId()
}

// JupyterMessageType returns the message type taken from the Jupyter header of the message.
func (r *BasicRequest) JupyterMessageType() string {
	return r.payload.JupyterMessageType()
}

// JupyterTimestamp returns the timestamp taken from the Jupyter header of the message.
func (r *BasicRequest) JupyterTimestamp() (ts time.Time, err error) {
	ts, err = time.Parse(time.RFC3339Nano, r.payload.JupyterMessageDate())
	if err != nil {
		return time.Time{}, err
	}

	return ts, err
}

// SocketProvider returns the entity responsible for providing access to sockets in the request handler.
func (r *BasicRequest) SocketProvider() JupyterServerInfo {
	return r.socketProvider
}

// PrepareForResubmission updates the timestamp of the message's header so that it is signed with a different signature.
// This is used when re-sending unacknowledged (unacknowledged) messages.
func (r *BasicRequest) PrepareForResubmission() error {
	// Get the date.
	date, _ := r.JupyterTimestamp()

	// Add a single microsecond to the date.
	modifiedDate := date.Add(time.Microsecond)

	// Change the date in the header.
	_ = r.payload.SetDate(modifiedDate.Format(time.RFC3339Nano), false)

	// Re-encode the header.
	header, err := r.payload.GetHeader()
	if err != nil {
		return err
	}

	err = r.payload.JupyterFrames.EncodeHeader(&header)
	if err != nil {
		return err
	}

	// Regenerate the signature. Don't include the buffer frames as part of the signature.
	_, err = r.payload.JupyterFrames.Sign(r.connectionInfo.SignatureScheme, []byte(r.connectionInfo.Key))
	if err != nil {
		return err
	}

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

	if verified := ValidateFrames([]byte(r.connectionInfo.Key), r.connectionInfo.SignatureScheme, r.payload.JupyterFrames); !verified {
		r.log.Error("Failed to verify modified message with signature scheme '%v' and key '%v'", r.connectionInfo.SignatureScheme, r.connectionInfo.Key)
		r.log.Error("This message will be rejected by the kernel unless the kernel has been configured to skip validating/verifying the messages:\n%v", r.payload.JupyterFrames)

		return ErrValidationFailed
	}

	return nil
}

// The MessageType of the request.
func (r *BasicRequest) MessageType() MessageType {
	return r.messageType
}

// Transition the request to the specified state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the specified state is invalid), then this will return an error.
// If the specified destination state does not exist or is otherwise unrecognized, then this will panic.
//
// If the request is currently in any of the states specified in the `abortStates` parameter, then the transition is aborted without an error.
//
// If the transition completes successfully, then true is returned with a nil error.
// If the transition is not completed for any reason, then false will be returned, along with an error if an error occurred.
func (r *BasicRequest) transitionTo(to RequestState, abortStates []RequestState) (bool, error) {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	from := r.requestState

	if abortStates != nil && slices.Contains(abortStates, from) {
		// We're in one of the 'abort' states.
		// We abort the transition, so we return false.
		// But since there was no error, we return `nil` for the error.
		return false, nil
	}

	// Ensure that the state transition is valid.
	illegalSourceStates, ok := IllegalSourceStates[to]
	if !ok {
		panic(fmt.Sprintf("Unexpected destination state for request transition: \"%s\"", to))
	}

	// If our current state is contained within the slice of illegal source states (with respect to the specified state), then we'll return an error.
	if slices.Contains(illegalSourceStates, from) {
		return false, fmt.Errorf("%w: [%v] --> %v", ErrIllegalTransition, from, to)
	}

	// If the "to" state is contained within the slice of illegal destination states (with respect to our current state), then we'll return an error.
	illegalDestinationStates := IllegalDestinationStates[from]
	if slices.Contains(illegalDestinationStates, to) {
		return false, fmt.Errorf("%w: %v --> [%v]", ErrIllegalTransition, from, to)
	}

	// Transition the state.
	r.requestState = to
	return true, nil
}

// SetSubmitted transitions to the `RequestStateSubmitted` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateSubmitted' is invalid), then this will return an error.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetSubmitted() (bool, error) {
	// We'll abort the transition if we're already in the 'completed' state or the 'processing' state, meaning we received an ACK or the response before we were able to transition to 'submitted'.
	return r.transitionTo(RequestStateSubmitted, []RequestState{RequestStateComplete, RequestStateProcessing})
}

// SetProcessing transitions to the `RequestStateProcessing` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateProcessing' is invalid), then this will return an error.
//
// This transition will be cancelled if the request is in the RequestComplete state when the transition is attempted.
// This is to account for race conditions between when we elect to set the state to processing, and when a notification that the request has been completed is received.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetProcessing() (bool, error) {
	// We'll abort the transition if we're already in the 'completed' state, meaning we received a response before we were able to transition to 'processing'.
	return r.transitionTo(RequestStateProcessing, []RequestState{RequestStateComplete})
}

// SetTimedOut transitions to the `RequestStateTimedOut` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateTimedOut' is invalid), then this will return an error.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetTimedOut() (bool, error) {
	return r.transitionTo(RequestStateTimedOut, nil)
}

// SetComplete transitions to the `RequestStateComplete` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateComplete' is invalid), then this will return an error.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetComplete() (bool, error) {
	return r.transitionTo(RequestStateComplete, nil)
}

// SetExplicitlyCancelled transitions to the `RequestStateExplicitlyCancelled` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateExplicitlyCancelled' is invalid), then this will return an error.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetExplicitlyCancelled() (bool, error) {
	return r.transitionTo(RequestStateExplicitlyCancelled, nil)
}

// SetErred transitions to the `RequestStateErred` state.
// If the proposed transition is illegal (i.e., transitioning from the current state to the 'RequestStateErred' is invalid), then this will return an error.
//
// Updates the `err` field of the live request state.
//
// Return a tuple in which the first element is a flag indicating whether the transition occurred and the second is an error that is non-nil if an error occurred (which prevented the transition from occurring).
func (r *BasicRequest) SetErred(err error) (bool, error) {
	r.liveRequestState.err = err
	return r.transitionTo(RequestStateErred, nil)
}
