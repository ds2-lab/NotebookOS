package scheduling

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidTargetNumHosts      = status.Error(codes.InvalidArgument, "requested operation would result in an invalid or illegal number of nodes")
	ErrInsufficientHostsAvailable = status.Error(codes.Internal, "insufficient hosts available")
	ErrHostNotFound               = status.Error(codes.Internal, "host not found")
	ErrReplicaNotFound            = fmt.Errorf("replica not found")
	ErrHostNotViable              = status.Error(codes.Internal, "host is not viable; cannot host specified kernel replica")
	ErrInvalidHost                = status.Error(codes.InvalidArgument, "invalid host specified")
	ErrNilHost                    = errors.New("host is nil when attempting to place kernel")
	ErrNilConnectionInfo          = errors.New("host returned no error and no connection info after starting kernel replica")
	ErrOldSnapshot                = errors.New("the given snapshot is older than the last snapshot applied to the target host")
	ErrNotImplementedYet          = errors.New("this method has not yet been implemented")
)
