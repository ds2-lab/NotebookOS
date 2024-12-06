package scheduling

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrUnsupportedOperation                = errors.New("the requested operation is not supported")
	ErrInvalidTargetNumHosts               = status.Error(codes.InvalidArgument, "requested operation would result in an invalid or illegal number of nodes")
	ErrInsufficientHostsAvailable          = status.Error(codes.Internal, "insufficient hosts available")
	ErrHostNotFound                        = status.Error(codes.Internal, "host not found")
	ErrReplicaNotFound                     = fmt.Errorf("replica not found")
	ErrHostNotViable                       = status.Error(codes.Internal, "host is not viable; cannot host specified kernel replica")
	ErrInvalidHost                         = status.Error(codes.InvalidArgument, "invalid host specified")
	ErrNilHost                             = errors.New("host is nil when attempting to place kernel")
	ErrNilConnectionInfo                   = errors.New("host returned no error and no connection info after starting kernel replica")
	ErrOldSnapshot                         = errors.New("the given snapshot is older than the last snapshot applied to the target host")
	ErrNotImplementedYet                   = errors.New("this method has not yet been implemented")
	ErrInvalidStateTransition              = errors.New("invalid session state transition requested")
	ErrExecutionFailedAllYielded           = errors.New("an execution failed; all replicas proposed 'YIELD'")
	ErrScalingProhibitedBySchedulingPolicy = status.Error(codes.FailedPrecondition, "scaling is not supported under the configured scheduling policy")
	ErrDynamicResourceAdjustmentProhibited = errors.New("dynamically adjusting resources is disabled by the configured scheduling policy")
	ErrInvalidSchedulingPolicy             = errors.New("unknown, unspecified, or invalid scheduling policy")
	ErrInvalidIdleSessionReclamationPolicy = errors.New("unknown, unspecified, or invalid idle session reclamation policy")
	// ErrMigrationFailed indicates that a migration failed for a "valid" reason, such as there simply not being a
	// viable target Host available. Importantly, it does NOT indicate that the Cluster is in an error state.
	ErrMigrationFailed = errors.New("failed to migrate kernel replica")
)
