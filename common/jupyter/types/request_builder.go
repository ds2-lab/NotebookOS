package types

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

var (
	errPayloadMissing        = errors.New("request payload")
	errDoneCallbackMissing   = errors.New("done callback")
	errMessageHandlerMissing = errors.New("message handler")
)

type RequestBuilder struct {
	log logger.Logger

	parentContext context.Context

	///////////////
	/// OPTIONAL //
	///////////////

	// Should the request require ACKs
	// Default: true
	requiresAck bool

	// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
	// Default: 120 seconds (i.e., 2 minutes)
	timeout time.Duration

	// Should the call to Server::Request block when issuing this request?
	// Default: true
	isBlocking bool

	// The maximum number of attempts allowed before giving up on sending the request.
	// Default: 3
	maxNumAttempts int

	// Should the destination frame be automatically removed?
	// Default: true
	shouldDestFrameBeRemoved bool

	//////////////
	// REQUIRED //
	//////////////

	// The actual payload.
	payload *JupyterMessage

	// This callback is executed when the response is received and the request is handled.
	// TODO: Might be better to turn this into more of a "clean-up"? Or something?
	doneCallback MessageDone

	// The handler that is called to process the response to this request.
	handler MessageHandler

	// The jupyter server instance that will be passed to the handler to get the socket for forwarding the response.
	server JupyterServerInfo

	// The client socket to forward the request.
	socket *Socket

	// Entity that implements the SourceKernel interface and thus can add the SourceKernel frame to the message.
	sourceKernel SourceKernel

	// The info of request destination that the WaitResponse can use to track individual request.
	dest RequestDest

	// The function to get the options.
	getOption WaitResponseOptionGetter

	/////////////////////////////////////////////////////////////////////////////////////////
	// AUTOMATIC                                                                           //
	// These are automatically populated during the population of the required parameters. //
	// They are not optional, but they do not require explicit configuration themselves.   //
	/////////////////////////////////////////////////////////////////////////////////////////

	// String that uniquely identifies this set of request options.
	// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
	requestId string

	// DestID extracted from the request payload.
	// It is extracted from the request payload when the request is built via RequestBuilder::BuildRequest.
	destinationId string

	// Offset/index of start of Jupyter frames within message frames.
	jOffset int

	// This is flipped to true when a timeout is explicitly configured.
	hasTimeout bool
}

// Create a new RequestBuilder, passing in an optional parent context.
func NewRequestBuilder(parentContext context.Context) *RequestBuilder {
	builder := &RequestBuilder{
		requiresAck:              true,
		isBlocking:               true,
		timeout:                  time.Second * 120,
		maxNumAttempts:           3,
		shouldDestFrameBeRemoved: true,
	}

	if parentContext != nil {
		builder.parentContext = parentContext
	} else {
		builder.parentContext = context.Background()
	}

	config.InitLogger(&builder.log, builder)

	return builder
}

//////////////
// Optional //
//////////////

// Configure whether the request should require an ACK to be sent by the recipient.
//
// Configuring this option is OPTIONAL. By default, requests will require ACKs.
func (b *RequestBuilder) WithAckRequired(required bool) *RequestBuilder {
	b.requiresAck = required
	return b
}

// Configure the timeout of the request.
//
// Configuring this option is OPTIONAL. By default, requests timeout after 120 seconds.
func (b *RequestBuilder) WithTimeout(timeout time.Duration) *RequestBuilder {
	b.timeout = timeout
	b.hasTimeout = true
	return b
}

// Configure whether the request will be issued in a blocking manner.
//
// Configuring this option is OPTIONAL. By default, requests are blocking.
func (b *RequestBuilder) WithBlocking(blocking bool) *RequestBuilder {
	b.isBlocking = blocking
	return b
}

// Specify the number of "high-level" retries for this request.
// These "high-level" retries are distinct from retries related to message ACKs.
// This is the number of times an ACK'd message will be resubmitted after timing out.
//
// Note that this option is irrelevant if no response is expected for the request.
//
// Configuring this option is OPTIONAL. By default, the request will be retried a total of 3 times.
func (b *RequestBuilder) WithNumAttempts(numRetries int) *RequestBuilder {
	b.maxNumAttempts = numRetries
	return b
}

// Configure whether the request should have the DEST frame automatically removed.
//
// Configuring this option is OPTIONAL. By default, the DEST frame is automatically removed.
func (b *RequestBuilder) WithRemoveDestFrame(shouldDestFrameBeRemoved bool) *RequestBuilder {
	b.shouldDestFrameBeRemoved = shouldDestFrameBeRemoved
	return b
}

//////////////
// Required //
//////////////

// Set the payload of the messasge.
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithPayload(destId string, msg *zmq4.Msg) *RequestBuilder {
	msg, reqId, jOffset := b.extractDestFrame(destId, msg)

	b.payload = NewJupyterMessage(msg)
	b.requestId = reqId
	b.jOffset = jOffset
	b.destinationId = destId

	return b
}

// Configure the callback that is executed when the response is received and the request is handled.
// TODO: Might be better to turn this into more of a "clean-up"? Or something?
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithDoneCallback(doneCallback MessageDone) *RequestBuilder {
	b.doneCallback = doneCallback
	return b
}

// Configure handler that is called to process the response to this request.
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithMessageHandler(handler MessageHandler) *RequestBuilder {
	b.handler = handler
	return b
}

// Extract the DEST frame from the request's frames.
func (b *RequestBuilder) extractDestFrame(destId string, msg *zmq4.Msg) (*zmq4.Msg, string, int) {
	// Normalize the request, we do not assume that the types.RequestDest implements the auto-detect feature.
	_, reqId, jOffset := ExtractDestFrame(msg.Frames)
	if reqId == "" {
		msg.Frames, reqId = AddDestFrame(msg.Frames, destId, jOffset)
	}

	return msg, reqId, jOffset
}

// Build the request as configured.
// This will panic if any required fields are missing.
//
// Note that the timeout associated with the Context will become "active" as soon as the request is created.
func (b *RequestBuilder) BuildRequest() (Request, error) {
	missingConfigurationParameters := make([]any, 0)

	// Verify that all of the required arguments have been specified.
	// We'll return an error if one or more of these arguments are left unspecified.

	if b.payload == nil {
		missingConfigurationParameters = append(missingConfigurationParameters, errPayloadMissing)
	}

	if b.doneCallback == nil {
		missingConfigurationParameters = append(missingConfigurationParameters, errDoneCallbackMissing)
	}

	if b.handler == nil {
		missingConfigurationParameters = append(missingConfigurationParameters, errMessageHandlerMissing)
	}

	if len(missingConfigurationParameters) > 0 {
		var errorMsgBuilder strings.Builder
		errorMsgBuilder.WriteString("one or more required configuration parameters are missing: ")

		// Construct the error/error message.
		missingConfigsAsStrings := make([]string, 0, len(missingConfigurationParameters))
		for i := 0; i < len(missingConfigurationParameters); i++ {
			missingConfigsAsStrings[i] = missingConfigurationParameters[i].(error).Error()

			if i+1 < len(missingConfigurationParameters) {
				// There will be another error, so add a common and space.
				errorMsgBuilder.WriteString("%w, ")
			} else {
				// Last one, so no comma or space.
				errorMsgBuilder.WriteString("%w")
			}
		}

		b.log.Error("Missing %d required configuration parameter(s): %v", len(missingConfigsAsStrings), strings.Join(missingConfigsAsStrings, ", "))
		return nil, fmt.Errorf(errorMsgBuilder.String(), missingConfigurationParameters...)
	}

	req := &basicRequest{
		liveRequestState: &liveRequestState{
			requestState:        RequestStateInit,
			hasBeenAcknowledged: false,
			timedOut:            false,
			erred:               false,
			err:                 nil,
		},
		requestId:                b.payload.RequestId,
		requiresAck:              b.requiresAck,
		timeout:                  b.timeout,
		isBlocking:               b.isBlocking,
		maxNumAttempts:           b.maxNumAttempts,
		shouldDestFrameBeRemoved: b.shouldDestFrameBeRemoved,
		payload:                  b.payload,
		doneCallback:             b.doneCallback,
		messageHandler:           b.handler,
		destinationId:            b.payload.DestinationId,
		kernelId:                 b.payload.KernelId,
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if b.hasTimeout {
		ctx, cancel = context.WithTimeout(b.parentContext, b.timeout)
	} else {
		ctx, cancel = context.WithCancel(b.parentContext)
	}

	req.ctx = ctx
	req.cancel = cancel

	return req, nil
}
