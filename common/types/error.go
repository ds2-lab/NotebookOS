package types

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrStopPropagation            = fmt.Errorf("stop propagation")
	ErrInvalidSocketType          = status.Error(codes.Internal, "invalid socket type specified")
	ErrIncompatibleDeploymentMode = status.Error(codes.FailedPrecondition, "current deployment mode is incompatible with the requested action")
	ErrRequestTimedOut            = status.Error(codes.Unavailable, "request timed out")
)
