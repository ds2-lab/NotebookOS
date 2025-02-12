package domain

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// gRPC errors
	// ErrNotFound         = errors.New("function not defined: %s")

	ErrNoHandler        = status.Errorf(codes.NotFound, "handler not defined")
	ErrNotImplemented   = status.Errorf(codes.Unimplemented, "not implemented in SchedulerDaemon")
	ErrInvalidParameter = status.Errorf(codes.InvalidArgument, "invalid parameter")
	ErrRequestFailed    = status.Errorf(codes.DeadlineExceeded, "could not complete kernel request in-time")

	// Internal errors

	ErrHeaderNotFound                        = errors.New("message header not found")
	ErrKernelNotReady                        = errors.New("kernel not ready")
	ErrKernelIDRequired                      = errors.New("kernel id frame is required for kernel_info_request")
	ErrUnexpectedZMQMessageType              = errors.New("received ZMQ message of unexpected type")
	ErrKernelRegistrationNotificationFailure = errors.New("could not notify gateway of kernel registration")
)
