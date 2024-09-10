package types

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrStopPropagation            = fmt.Errorf("stop propagation")
	ErrIncompatibleDeploymentMode = status.Error(codes.FailedPrecondition, "current deployment mode is incompatible with the requested action")
)
