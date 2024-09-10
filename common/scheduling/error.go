package scheduling

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInsufficientHostsAvailable = status.Error(codes.Internal, "insufficient hosts available")
)
