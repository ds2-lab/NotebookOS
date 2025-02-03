package resource

import (
	"errors"
)

var (
	// ErrInvalidAllocationRequest indicates that an Allocation could not be created/satisfied due to an issue
	// with the request itself.
	//
	// The issue is not something of the nature that there are just insufficient HostResources available to satisfy the
	// request. Instead, ErrInvalidAllocationRequest indicates that the request itself was illegal or issued under
	// invalid circumstances, such as there being no existing Allocation of type PendingAllocation when
	// attempting to commit HostResources to a particular kernel replica. Alternatively, a kernel replica may be getting
	// evicted, but no existing Allocation is found for that particular kernel replica.
	ErrInvalidAllocationRequest = errors.New("the resource allocation could not be completed due to the request being invalid")

	ErrReservationNotFound = errors.New("no reservation found for any replicas of the specified kernel")

	ErrIllegalResourceAdjustment = errors.New("requested resource adjustment is illegal")
	ErrAllocationNotFound        = errors.New("could not find the resource allocation for specified kernel replica")
	ErrInvalidAllocationType     = errors.New("allocation for target kernel replica is not of expected/correct type")
	ErrNoPendingAllocationFound  = errors.New("a pending allocation could not be found when allocating actual GPUs")
)
