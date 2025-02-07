package scheduling

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
)

const (
	// NoResource is a sort of default value for ResourceKind.
	NoResource ResourceKind = "N/A"
	CPU        ResourceKind = "CPU"
	GPU        ResourceKind = "GPU"
	VRAM       ResourceKind = "VRAM"
	Memory     ResourceKind = "Memory"
	// UnknownResource is used as a catch-all for which there is conceivably a relevant ResourceKind,
	// but we're not returning it for whatever reason, whereas NoResource means that there is no
	// relevant ResourceKind for the circumstances.
	UnknownResource ResourceKind = "Unknown"

	// IdleResources can overlap with pending HostResources. These are HostResources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources ResourceStatus = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending HostResources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending HostResources on a node is the amount of HostResources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources ResourceStatus = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources ResourceStatus = "committed"

	// SpecResources are the total allocatable HostResources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources ResourceStatus = "spec"

	// UnknownStatus indicates that type ResourceStatus relevant to the circumstances is not known, but
	// in theory one exists.
	UnknownStatus ResourceStatus = "unknown"

	// NegativeResourceQuantity indicates that the inconsistent/invalid resource is
	// inconsistent/invalid because its quantity is negative.
	NegativeResourceQuantity Inconsistency = "negative_quantity"

	// QuantityGreaterThanSpec indicates that the inconsistent/invalid resource
	// is inconsistent/invalid because its quantity is greater than that of the scheduling.Host
	// instances types.Spec quantity.
	QuantityGreaterThanSpec Inconsistency = "quantity_greater_than_spec"

	// IdleSpecUnequal indicates that our IdleResources and SpecResources are unequal despite having no kernel
	// replicas scheduled locally on the node. (When the ndoe is empty, all our HostResources should be idle.)
	IdleSpecUnequal Inconsistency = "idle_and_spec_resources_unequal"

	// IdleCommittedSumDoesNotEqualSpec indicates that the sum of IdleResources and CommittedResources
	// does not equal SpecResources.
	IdleCommittedSumDoesNotEqualSpec Inconsistency = "idle_committed_sum_does_not_equal_spec"

	// PendingNonzero indicates that our PendingResources are non-zero despite having no replicas scheduled locally.
	PendingNonzero Inconsistency = "pending_nonzero"
)

// ResourceKind can be one of CPU, GPU, or Memory
type ResourceKind string

func (k ResourceKind) String() string {
	return string(k)
}

// ResourceStatus differentiates between idle, pending, committed, and spec HostResources.
type ResourceStatus string

func (t ResourceStatus) String() string {
	return string(t)
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
	if e.AvailableResources != nil && e.RequestedResources != nil {
		return fmt.Sprintf("InsufficientResourcesError[available=%s,Requested=%s]",
			e.AvailableResources.String(), e.RequestedResources.String())
	}

	return fmt.Sprintf("InsufficientResourcesError[OffendingKinds=%v]", e.OffendingResourceKinds)
}

// Inconsistency defines the various ways in which HostResources can be in an inconsistent or illegal state.
// Examples include a resource being negative, a resource quantity being larger than the total available HostResources
// of that kind on the node, and so on.
type Inconsistency string

func (i Inconsistency) String() string {
	return string(i)
}

// InconsistentResourcesError is a custom error type used to indicate that some resource quantity within
// the AllocationManager is in an inconsistent or invalid/illegal state.
//
// A InconsistentResourcesError contains the information to describe exactly what is wrong, in terms of which
// quantity or quantities or involved, what the nature of the inconsistency or illegal state is, etc.
type InconsistentResourcesError struct {
	// ResourceKind indicates which kind of resource is in an inconsistent or invalid state.
	ResourceKind ResourceKind

	// ResourceStatus indicates which status of resource (idle, pending, or committed) is in an inconsistent or invalid state.
	ResourceStatus ResourceStatus

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
func NewInconsistentResourcesError(kind ResourceKind, inconsistency Inconsistency,
	status ResourceStatus, quantity decimal.Decimal) *InconsistentResourcesError {

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
	status ResourceStatus, quantity decimal.Decimal, referenceQuantity decimal.Decimal) *InconsistentResourcesError {

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
