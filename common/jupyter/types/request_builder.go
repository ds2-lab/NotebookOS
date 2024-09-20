package types

import (
	"context"
	"errors"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/jupyter"
	"strings"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

const (
	DefaultRequestTimeout = time.Second * 120
	DefaultAckTimeout     = time.Second * 5
)

var (
	errPayloadMissing        = errors.New("request payload")
	errDoneCallbackMissing   = errors.New("done callback")
	errMessageHandlerMissing = errors.New("message handler")
	errSocketProviderMissing = errors.New("socket provider")
	errConnectionInfoMissing = errors.New("connection info")

	errInvalidParameter = errors.New("invalid value for configuration parameter")
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

	// ackTimeout is the amount of time that the Sender should wait for the Request to be acknowledged by the
	// recipient before either resubmitting the Request or returning an error.
	//
	// Default: 5 seconds.
	ackTimeout time.Duration

	// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
	// Default: infinite.
	timeout time.Duration

	// Should the call to Server::Request block when issuing this request?
	// Default: true
	isBlocking bool

	// The maximum number of attempts allowed before giving up on sending the request.
	// This must be strictly greater than 0.
	//
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

	// The entity responsible for providing access to sockets in the request handler.
	socketProvider JupyterServerInfo

	// The MessageType of the request.
	messageType MessageType

	// The function to get the options.
	// getOption WaitResponseOptionGetter

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

	// The ID associated with the source of the message.
	// This will typically be a kernel ID.
	sourceId string

	// This is flipped to true when a timeout is explicitly configured.
	// We use this to determine if we should create the context for the request via Context::WithTimeout or Context::WithCancel.
	hasTimeout bool

	// The connection info of the remote target of the request.
	connectionInfo *ConnectionInfo
}

// NewRequestBuilder creates a new RequestBuilder struct, passing in an optional parent context and the ID of the
// source of the message, which will usually be a kernel.
//
// NewRequestBuilder returns a pointer to the newly-created RequestBuilder struct.
func NewRequestBuilder(parentContext context.Context, sourceId string, destId string, connectionInfo *ConnectionInfo) *RequestBuilder {
	builder := &RequestBuilder{
		// requiresAck:              true,
		isBlocking:               true,
		timeout:                  DefaultRequestTimeout, // Default value
		hasTimeout:               false,
		maxNumAttempts:           3,
		shouldDestFrameBeRemoved: true,
		sourceId:                 sourceId,
		destinationId:            destId,
		connectionInfo:           connectionInfo,
		ackTimeout:               DefaultAckTimeout,
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

// WithAckRequired configures whether the request should require an ACK to be sent by the recipient.
//
// Configuring this option is OPTIONAL. By default, requests will require ACKs.
func (b *RequestBuilder) WithAckRequired(required bool) *RequestBuilder {
	b.requiresAck = required
	return b
}

// WithAckTimeout configures the AckTimeout of the Request.
//
// Configuring this option is OPTIONAL. By default, the AckTimeout is set to 5 seconds.
func (b *RequestBuilder) WithAckTimeout(ackTimeout time.Duration) *RequestBuilder {
	b.ackTimeout = ackTimeout
	return b
}

// WithTimeout configures the timeout of the request.
//
// Configuring this option is OPTIONAL. By default, requests do not time out.
func (b *RequestBuilder) WithTimeout(timeout time.Duration) *RequestBuilder {
	b.timeout = timeout
	b.hasTimeout = true
	return b
}

// WithBlocking configures whether the request will be issued in a blocking manner.
//
// Configuring this option is OPTIONAL. By default, requests are blocking.
func (b *RequestBuilder) WithBlocking(blocking bool) *RequestBuilder {
	b.isBlocking = blocking
	return b
}

// WithNumAttempts specifies the number of "high-level" retries for this request.
// These "high-level" retries are distinct from retries related to message ACKs.
// This is the number of times an acknowledged message will be resubmitted after timing out.
//
// TODO: I think the above is out-dated, and that these retries are all one and the same. Is this true?
//
// Note that this option is irrelevant if no response is expected for the request.
//
// Configuring this option is OPTIONAL. By default, the request will be retried a total of 3 times.
func (b *RequestBuilder) WithNumAttempts(numRetries int) *RequestBuilder {
	b.maxNumAttempts = numRetries
	return b
}

// WithRemoveDestFrame configures whether the request should have the DEST frame automatically removed.
//
// Configuring this option is OPTIONAL. By default, the DEST frame is automatically removed.
func (b *RequestBuilder) WithRemoveDestFrame(shouldDestFrameBeRemoved bool) *RequestBuilder {
	b.shouldDestFrameBeRemoved = shouldDestFrameBeRemoved
	return b
}

//////////////
// Required //
//////////////

// WithPayload sets the payload of the message.
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithPayload(msg *zmq4.Msg) *RequestBuilder {
	if msg == nil {
		panic(fmt.Sprintf("Cannot assign nil payload for request. SourceID: %s. DestID: %s. ConnectionInfo: %v.", b.sourceId, b.destinationId, b.connectionInfo))
	}

	msg, reqId, _ := b.extractAndAddDestFrame(b.destinationId, msg)

	b.payload = NewJupyterMessage(msg)
	b.requestId = reqId

	return b
}

// WithJMsgPayload sets the payload of the message.
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithJMsgPayload(msg *JupyterMessage) *RequestBuilder {
	if msg == nil {
		panic(fmt.Sprintf("Cannot assign nil JMsg payload for request. SourceID: %s. DestID: %s. ConnectionInfo: %v.", b.sourceId, b.destinationId, b.connectionInfo))
	}

	var (
		requestId string
		jOffset   = -1
	)
	if len(msg.DestinationId) == 0 {
		requestId, jOffset = msg.AddDestinationId(b.destinationId)
	}

	b.payload = msg
	b.requestId = msg.RequestId

	// Sanity checks.
	if len(requestId) > 0 && requestId != msg.RequestId {
		panic(fmt.Sprintf("Request ID field of JupyterMessage does not match return value of JupyterMessage::AddDestinationId. Field: \"%s\". Return value: \"%s\".", msg.RequestId, requestId))
	}

	// Sanity checks.
	if jOffset != -1 && jOffset != msg.Offset {
		panic(fmt.Sprintf("Offset field of JupyterMessage does not match return value of JupyterMessage::AddDestinationId. Field: \"%d\". Return value: \"%d\".", msg.Offset, jOffset))
	}

	return b
}

// WithMessageType configures the message type of the request.
func (b *RequestBuilder) WithMessageType(messageType MessageType) *RequestBuilder {
	b.messageType = messageType
	return b
}

// WithSocketProvider configures the JupyterServerInfo of the request.
func (b *RequestBuilder) WithSocketProvider(socketProvider JupyterServerInfo) *RequestBuilder {
	b.socketProvider = socketProvider

	return b
}

// WithDoneCallback configures the callback that is executed when the response is received and the request is handled.
// TODO: Might be better to turn this into more of a "clean-up"? Or something?
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithDoneCallback(doneCallback MessageDone) *RequestBuilder {
	b.doneCallback = doneCallback
	return b
}

// WithMessageHandler configures handler that is called to process the response to this request.
//
// Configuring this option is REQUIRED (i.e., there is no default; it must be configured explicitly.)
func (b *RequestBuilder) WithMessageHandler(handler MessageHandler) *RequestBuilder {
	b.handler = handler
	return b
}

// Extract the DEST frame from the request's frames.
// If there is no DEST frame already contained within the message, then add the DEST frame.
func (b *RequestBuilder) extractAndAddDestFrame(destId string, msg *zmq4.Msg) (*zmq4.Msg, string, int) {
	// Normalize the request, we do not assume that the types.RequestDest implements the auto-detect feature.
	_, reqId, jOffset := ExtractDestFrame(msg.Frames)
	if reqId == "" {
		msg.Frames, reqId, jOffset = AddDestFrame(msg.Frames, destId, jupyter.JOffsetAutoDetect)
	}

	return msg, reqId, jOffset
}

// BuildRequest builds the request as configured.
// This will panic if any required fields are missing.
//
// Note that the timeout associated with the Context will become "active" as soon as the request is created.
func (b *RequestBuilder) BuildRequest() (*BasicRequest, error) {
	missingConfigurationParameters := make([]any, 0)

	// Verify that all the required arguments have been specified.
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

	if b.socketProvider == nil {
		missingConfigurationParameters = append(missingConfigurationParameters, errSocketProviderMissing)
	}

	if b.connectionInfo == nil {
		missingConfigurationParameters = append(missingConfigurationParameters, errConnectionInfoMissing)
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

	// Validate the maxNumAttempts configuration parameter.
	// It must be strictly greater than 0.
	if b.maxNumAttempts <= 0 {
		b.log.Error("Invalid value for MaxNumAttempts: %d. Value must be > 0.", b.maxNumAttempts)
		return nil, fmt.Errorf("%w: MaxNumAttempts is equal to %d; value must be > 0", errInvalidParameter, b.maxNumAttempts)
	}

	// Although this is a little complicated, this IS a possibility.
	// Depending on where this request is originating, we may or may not want to require ACKs for this message.
	//
	// A majority of the time, we will want ACKs for Shell and Control messages.
	// Once exception is when we know the target kernel is training, and we're sending a Shell message.
	// We don't know how long the kernel will be training for, and the Shell message will be blocked until the
	// training completes, so we don't want there to be unnecessary time-outs and resubmissions.
	//
	// In this case, it's the sender's responsibility to manually/explicitly resubmit the message if they don't
	// hear back (but the replica may just be training, and that's why the sender isn't hearing back).
	if (b.messageType == ShellMessage || b.messageType == ControlMessage) && !b.requiresAck {
		b.log.Warn("Request is of type %v; however, requiresACK is false.", b.messageType)
		// return nil, fmt.Errorf("%w: Request is of type %v; however, requiresACK is false", errInvalidParameter, b.messageType)
	}

	req := &BasicRequest{
		liveRequestState: &liveRequestState{
			requestState:        RequestStateInit,
			hasBeenAcknowledged: false,
			timedOut:            false,
			erred:               false,
			err:                 nil,
		},
		requestId:                b.payload.RequestId,
		timeout:                  b.timeout,
		isBlocking:               b.isBlocking,
		maxNumAttempts:           b.maxNumAttempts,
		shouldDestFrameBeRemoved: b.shouldDestFrameBeRemoved,
		payload:                  b.payload,
		doneCallback:             b.doneCallback,
		messageHandler:           b.handler,
		destinationId:            b.payload.DestinationId,
		sourceId:                 b.sourceId,
		connectionInfo:           b.connectionInfo,
		parentContext:            b.parentContext,
		socketProvider:           b.socketProvider,
		messageType:              b.messageType,
		requiresAck:              b.requiresAck,
		ackTimeout:               b.ackTimeout,
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

	config.InitLogger(&req.log, fmt.Sprintf("Request-%s", req.requestId))

	return req, nil
}
