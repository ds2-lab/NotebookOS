package server

import (
	"context"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

// Interface representing a Request to be passed to Server::Request.
// This interface is designed to encapsulate a number of options that may be passed to the Server::Request method.
//
// Default values are as follows:
// RequiresAck: TRUE
// Blocking: TRUE
type RequestOptions interface {
	// Should the request require ACKs
	// Default: true
	RequiresAck() bool

	// Indicates whether the call to Server::Request block when issuing this request
	// True indicates blocking; false indicates non-blocking (i.e., Server::Request will return immediately, rather than wait for a response for returning)
	// Default: true
	IsBlocking() bool

	// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
	// Default: 120 seconds (i.e., 2 minutes)
	Timeout() time.Duration

	// The maximum number of attempts allowed before giving up on sending the request.
	// Default: 3
	MaxNumRetries() int

	// Indicates whether we expect a response from the receiver after the receiver processes the request.
	// This "response" is distinct from an ACK.
	// Default: true
	ResponseExpected() bool

	// The request's context
	Context() context.Context

	// String that uniquely identifies this set of request options.
	// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
	InternalId() string
}

// Used to build new Requests.
//
// The following fields are REQUIRED and MUST be configured/set explicitly (if they are not set explicitly, then RequestBuilder::BuildRequest will panic):
//
// - server:    the jupyter server instance that will be passed to the handler to get the socket for forwarding the response.
//
// - socket:    the client socket to forward the request.
//
// - msg:       the request to be sent.
//
// - source:    entity that implements the SourceKernel interface and thus can add the SourceKernel frame to the message.
//
// - dest:      the info of request destination that the WaitResponse can use to track individual request.
//
// - handler:   the handler to handle the response.
//
// Several fields have default values and do not need to be explicitly configured:
//
// - RequiresAck:   true
//
// - Blocking:      true
//
// - Timeout:       120 seconds (i.e., 2 minutes)
//
// - MaxNumRetries: 3
type RequestBuilder struct {
	log logger.Logger

	///////////////
	/// OPTIONAL //
	///////////////

	// Should the request require ACKs
	// Default: true
	requiresAck bool
	// Should the call to Server::Request block when issuing this request?
	// Default: true
	isBlocking bool
	// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
	// Default: 120 seconds (i.e., 2 minutes)
	timeout time.Duration
	// The maximum number of attempts allowed before giving up on sending the request.
	// Default: 3
	maxNumRetries int
	// Indicates whether we expect a response from the receiver after the receiver processes the request.
	// This "response" is distinct from an ACK.
	// Default: true
	responseExpected

	///////////////
	/// REQUIRED //
	///////////////

	ctx context.Context
	// The jupyter server instance that will be passed to the handler to get the socket for forwarding the response.
	server types.JupyterServerInfo
	// The client socket to forward the request.
	socket *types.Socket
	// The request to be sent.
	msg *zmq4.Msg
	// Entity that implements the SourceKernel interface and thus can add the SourceKernel frame to the message.
	sourceKernel SourceKernel
	// The info of request destination that the WaitResponse can use to track individual request.
	dest RequestDest
	// The handler to handle the response.
	handler types.MessageHandler
	// The function to get the options.
	getOption WaitResponseOptionGetter
}

func NewRequestBuilder(ctx context.Context) *RequestBuilder {
	builder := &RequestBuilder{
		ctx:           ctx,
		requiresAck:   true,
		isBlocking:    true,
		timeout:       time.Second * 120,
		maxNumRetries: 3,
	}

	config.InitLogger(&builder.log, builder)

	return builder
}

// The request WILL require ACKs.
// By default, requests will require ACKs.
func (b *RequestBuilder) WithAckRequired() *RequestBuilder {
	b.requiresAck = true
	return b
}

// The request will NOT require ACKs.
// By default, requests will require ACKs.
func (b *RequestBuilder) NoAckRequired() *RequestBuilder {
	b.requiresAck = false
	return b
}

// The request will be issued in a blocking manner.
// By default, requests are blocking.
func (b *RequestBuilder) Blocking() *RequestBuilder {
	b.isBlocking = true
	return b
}

// The request will be issued in a non-blocking manner.
// By default, requests are blocking.
func (b *RequestBuilder) NonBlocking() *RequestBuilder {
	b.isBlocking = false
	return b
}

// Specify the number of "high-level" retries for this request.
// These "high-level" retries are distinct from retries related to message ACKs.
// This is the number of times an ACK'd message will be resubmitted after timing out.
//
// Note that this option is irrelevant if no response is expected for the request.
//
// By default, the request will be retried a total of 3 times.
func (b *RequestBuilder) WithNumRetries(numRetries int) *RequestBuilder {
	b.maxNumRetries = numRetries
	return b
}

// Designate the request as not expecting/requiring a response.
// By default, requests do expect a response.
func (b *RequestBuilder) WithResponseExpected() *RequestBuilder {
	b.responseExpected = true
	return b
}

// Designate the request as not expecting/requiring a response.
// By default, requests do expect a response.
func (b *RequestBuilder) WithNoResponseExpected() *RequestBuilder {
	b.responseExpected = false
	return b
}

// Build the request as configured.
// This will panic if any required fields are missing.
func (b *RequestBuilder) BuildRequest() RequestOptions {
	if !b.requiresAck && b.responseExpected {
		// This is a bit unexpected.
		// Typically, if a request expects a result/reply after the receiver processes it, then the sender would like for the request to be ACK'd.
		// This is because the ACK informs the sender that the request has been received and is being processed.
		b.log.Warn("Configuring request which does not expect any ACKs but DOES expect a response after processing.")
	}

	return &requestOptionsImpl{
		requiresAck:      b.requiresAck,
		isBlocking:       b.isBlocking,
		timeout:          b.timeout,
		maxNumRetries:    b.maxNumRetries,
		responseExpected: b.responseExpected,
		internalId:       uuid.NewString(),
	}
}

// Concrete implementation of the Request interface.
type requestOptionsImpl struct {
	ctx              context.Context
	requiresAck      bool
	isBlocking       bool
	timeout          time.Duration
	maxNumRetries    int
	responseExpected bool
	internalId       string
}

// The request's context
func (r *requestOptionsImpl) Context() context.Context {
	return r.ctx
}

// Should the request require ACKs
func (r *requestOptionsImpl) RequiresAck() bool {
	return r.requiresAck
}

// Should the call to Server::Request block when issuing this request?
func (r *requestOptionsImpl) IsBlocking() bool {
	return r.isBlocking
}

// How long to wait for the request to complete successfully. Completion is a stronger requirement than simply being ACK'd.
func (r *requestOptionsImpl) Timeout() time.Duration {
	return r.timeout
}

// The maximum number of attempts allowed before giving up on sending the request.
func (r *requestOptionsImpl) MaxNumRetries() int {
	return r.maxNumRetries
}

// Indicates whether we expect a response from the receiver after the receiver processes the request.
// This "response" is distinct from an ACK.
// Default: true
func (r *requestOptionsImpl) ResponseExpected() bool {
	return r.responseExpected
}

// String that uniquely identifies this set of request options.
// This is not configurable; it is auto-generated when the request is built via RequestBuilder::BuildRequest.
func (r *requestOptionsImpl) InternalId() string {
	return r.internalId
}
