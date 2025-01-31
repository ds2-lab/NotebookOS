package scheduling

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/scusemua/distributed-notebook/common/types"
)

const (
	// NoResource is a sort of default value for ResourceKind.
	NoResource ResourceKind = "N/A"
	CPU        ResourceKind = "CPU"
	GPU        ResourceKind = "GPU"
	VRAM       ResourceKind = "VRAM"
	Memory     ResourceKind = "Memory"
)

// ResourceKind can be one of CPU, GPU, or Memory
type ResourceKind string

func (k ResourceKind) String() string {
	return string(k)
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
		return fmt.Sprintf("InsufficientResourcesError[Available=%s,Requested=%s]",
			e.AvailableResources.String(), e.RequestedResources.String())
	}

	return fmt.Sprintf("InsufficientResourcesError[OffendingKinds=%v]", e.OffendingResourceKinds)
}
