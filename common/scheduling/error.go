package scheduling

import (
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
)
