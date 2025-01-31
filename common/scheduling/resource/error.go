package resource

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
)

var (
	// ErrInvalidAllocationRequest indicates that a Allocation could not be created/satisfied due to an issue
	// with the request itself.
	//
	// The issue is not something of the nature that there are just insufficient HostResources available to satisfy the
	// request. Instead, ErrInvalidAllocationRequest indicates that the request itself was illegal or issued under
	// invalid circumstances, such as there being no existing Allocation of type PendingAllocation when
	// attempting to commit HostResources to a particular kernel replica. Alternatively, a kernel replica may be getting
	// evicted, but no existing Allocation is found for that particular kernel replica.
	ErrInvalidAllocationRequest = errors.New("the resource allocation could not be completed due to the request being invalid")

	ErrReservationNotFound = errors.New("no reservation found for any replicas of the specified kernel")

	ErrIllegalGpuAdjustment     = errors.New("requested gpu adjustment is illegal")
	ErrAllocationNotFound       = errors.New("could not find the requested GPU allocation")
	ErrInvalidAllocationType    = errors.New("allocation for target kernel replica is not of expected/correct type")
	ErrNoPendingAllocationFound = errors.New("a pending allocation could not be found when allocating actual GPUs")
)

// InconsistentResourcesError is a custom error type used to indicate that some resource quantity within
// the AllocationManager is in an inconsistent or invalid/illegal state.
//
// A InconsistentResourcesError contains the information to describe exactly what is wrong, in terms of which
// quantity or quantities or involved, what the nature of the inconsistency or illegal state is, etc.
type InconsistentResourcesError struct {
	// ResourceKind indicates which kind of resource is in an inconsistent or invalid state.
	ResourceKind ResourceKind

	// ResourceStatus indicates which status of resource (idle, pending, or committed) is in an inconsistent or invalid state.
	ResourceStatus Status

	// ResourceInconsistency defines the various ways in which HostResources can be in an inconsistent or illegal state.
	// Examples include a resource being negative, a resource quantity being larger than the total available HostResources
	// of that kind on the node, and so on.
	ResourceInconsistency Inconsistency

	// Quantity is the value of the inconsistent/invalid resource.
	Quantity decimal.Decimal

	// ReferenceQuantity is the value against which Quantity is being compared and, as a result, is in
	// an invalid or inconsistent state.
	//
	// For example, if the CPU resource is in an invalid or inconsistent state with the ResourceInconsistency
	// specified as QuantityGreaterThanSpec, then the ReferenceQuantity will be set to the appropriate
	// Quantity of the associated scheduling.Host instance's types.Spec.
	ReferenceQuantity decimal.Decimal

	// ReferenceQuantityIsMeaningful indicates that the value of ReferenceQuantity is meaningful, and not just
	// a default value used in cases where there is no ReferenceQuantity, such as when the Quantity is simply
	// a negative number.
	ReferenceQuantityIsMeaningful bool
}

// NewInconsistentResourcesError creates a new InconsistentResourcesError struct and returns a pointer to it.
//
// This function sets the ReferenceQuantityIsMeaningful field to false.
func NewInconsistentResourcesError(kind ResourceKind, inconsistency Inconsistency, status Status, quantity decimal.Decimal) *InconsistentResourcesError {

	return &InconsistentResourcesError{
		ResourceKind:                  kind,
		ResourceInconsistency:         inconsistency,
		Quantity:                      quantity,
		ResourceStatus:                status,
		ReferenceQuantity:             decimal.Zero.Copy(),
		ReferenceQuantityIsMeaningful: false,
	}
}

// NewInconsistentResourcesErrorWithResourceQuantity creates a new InconsistentResourcesError struct and
// returns a pointer to it.
//
// This function sets the ReferenceQuantityIsMeaningful field to true.
func NewInconsistentResourcesErrorWithResourceQuantity(kind ResourceKind, inconsistency Inconsistency,
	status Status, quantity decimal.Decimal, referenceQuantity decimal.Decimal) *InconsistentResourcesError {

	return &InconsistentResourcesError{
		ResourceKind:                  kind,
		ResourceInconsistency:         inconsistency,
		Quantity:                      quantity,
		ResourceStatus:                status,
		ReferenceQuantity:             referenceQuantity,
		ReferenceQuantityIsMeaningful: true,
	}
}

// AsError returns the InconsistentResourcesError as an error.
func (e *InconsistentResourcesError) AsError() error {
	return e
}

func (e *InconsistentResourcesError) Error() string {
	if e.ReferenceQuantityIsMeaningful {
		return fmt.Sprintf("%s resource \"%s\" is an inconsistent or invalid state: \"%s\" (quantity=%s, referenceQuantity=%s)",
			e.ResourceStatus, e.ResourceKind, e.ResourceInconsistency, e.Quantity, e.ReferenceQuantity)
	} else {
		return fmt.Sprintf("%s resource \"%s\" is an inconsistent or invalid state: \"%s\" (quantity=%s)",
			e.ResourceStatus, e.ResourceKind, e.ResourceInconsistency, e.Quantity)
	}
}

// InsufficientResourcesError is a custom error type that is used to indicate that HostResources could not be
// allocated because there are insufficient HostResources available for one or more HostResources (CPU, GPU, or RAM).
type InsufficientResourcesError struct {
	// AvailableResources are the HostResources that were available on the node at the time that the
	// failed allocation was attempted.
	AvailableResources types.Spec
	// RequestedResources are the HostResources that were requested, and that could not be fulfilled in their entirety.
	RequestedResources types.Spec
	// OffendingResourceKinds is a slice containing each ResourceKind for which there were insufficient
	// HostResources available (and thus that particular ResourceKind contributed to the inability of the node
	// to fulfill the resource request).
	OffendingResourceKinds []ResourceKind
}

// NewInsufficientResourcesError constructs a new InsufficientResourcesError struct and returns a pointer to it.
func NewInsufficientResourcesError(avail types.Spec, req types.Spec, kinds []ResourceKind) InsufficientResourcesError {
	return InsufficientResourcesError{
		AvailableResources:     avail,
		RequestedResources:     req,
		OffendingResourceKinds: kinds,
	}
}

func (e InsufficientResourcesError) Unwrap() error {
	return fmt.Errorf(e.Error())
}

func (e InsufficientResourcesError) Error() string {
	return e.String()
}

func (e InsufficientResourcesError) Is(other error) bool {
	var insufficientResourcesError *InsufficientResourcesError
	return errors.As(other, &insufficientResourcesError)
}

func (e InsufficientResourcesError) String() string {
	return fmt.Sprintf("InsufficientResourcesError[Available=%s,Requested=%s]",
		e.AvailableResources.String(), e.RequestedResources.String())
}
