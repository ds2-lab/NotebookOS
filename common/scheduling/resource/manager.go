package resource

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"log"
	"sync"
)

const (
	// IdleResources can overlap with pending HostResources. These are HostResources that are not actively bound
	// to any containers/replicas. They are available for use by a locally-running container/replica.
	IdleResources Status = "idle"

	// PendingResources are "subscribed to" by a locally-running container/replica; however, they are not
	// bound to that container/replica, and thus are available for use by any of the locally-running replicas.
	//
	// Pending HostResources indicate the presence of locally-running replicas that are not actively training.
	// The sum of all pending HostResources on a node is the amount of HostResources that would be required if all
	// locally-scheduled replicas began training at the same time.
	PendingResources Status = "pending"

	// CommittedResources are actively bound/committed to a particular, locally-running container.
	// As such, they are unavailable for use by any other locally-running replicas.
	CommittedResources Status = "committed"

	// SpecResources are the total allocatable HostResources available on the Host.
	// SpecResources are a static, fixed quantity. They do not change in response to resource (de)allocations.
	SpecResources Status = "spec"

	// NoResource is a sort of default value for Kind.
	NoResource Kind = "N/A"
	CPU        Kind = "CPU"
	GPU        Kind = "GPU"
	VRAM       Kind = "VRAM"
	Memory     Kind = "Memory"

	// NegativeResourceQuantity indicates that the inconsistent/invalid resource is
	// inconsistent/invalid because its quantity is negative.
	NegativeResourceQuantity Inconsistency = "negative_quantity"

	// ResourceQuantityGreaterThanSpec indicates that the inconsistent/invalid resource
	// is inconsistent/invalid because its quantity is greater than that of the scheduling.Host
	// instances types.Spec quantity.
	ResourceQuantityGreaterThanSpec Inconsistency = "quantity_greater_than_spec"

	// IdleSpecUnequal indicates that our IdleResources and SpecResources are unequal despite having no kernel
	// replicas scheduled locally on the node. (When the ndoe is empty, all our HostResources should be idle.)
	IdleSpecUnequal Inconsistency = "idle_and_spec_resources_unequal"

	// PendingNonzero indicates that our PendingResources are non-zero despite having no replicas scheduled locally.
	PendingNonzero Inconsistency = "pending_nonzero"
)

var (
	// ErrInsufficientMemory indicates that there was insufficient memory HostResources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientMemory = errors.New("insufficient memory HostResources available")

	// ErrInsufficientCPUs indicates that there was insufficient CPU HostResources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientCPUs = errors.New("insufficient CPU HostResources available")

	// ErrInsufficientGPUs indicates that there was insufficient GPU HostResources available to validate/support/serve
	// the given resource request/types.Spec.
	//
	// Deprecated: use InsufficientResourcesError instead.
	ErrInsufficientGPUs = errors.New("insufficient GPU HostResources available")

	// ErrInvalidSnapshot is a general error message indicating that the application of a snapshot has failed.
	ErrInvalidSnapshot = errors.New("the specified snapshot could not be applied")

	// ErrIncompatibleResourceStatus is a specific reason for why the application of a snapshot may fail.
	// If the source and target Status values do not match, then the snapshot will be rejected.
	ErrIncompatibleResourceStatus = errors.New("source and target Status values are not the same")
)

// InsufficientResourcesError is a custom error type that is used to indicate that HostResources could not be
// allocated because there are insufficient HostResources available for one or more HostResources (CPU, GPU, or RAM).
type InsufficientResourcesError struct {
	// AvailableResources are the HostResources that were available on the node at the time that the
	// failed allocation was attempted.
	AvailableResources types.Spec
	// RequestedResources are the HostResources that were requested, and that could not be fulfilled in their entirety.
	RequestedResources types.Spec
	// OffendingResourceKinds is a slice containing each Kind for which there were insufficient
	// HostResources available (and thus that particular Kind contributed to the inability of the node
	// to fulfill the resource request).
	OffendingResourceKinds []Kind
}

// NewInsufficientResourcesError constructs a new InsufficientResourcesError struct and returns a pointer to it.
func NewInsufficientResourcesError(avail types.Spec, req types.Spec, kinds []Kind) *InsufficientResourcesError {
	return &InsufficientResourcesError{
		AvailableResources:     avail,
		RequestedResources:     req,
		OffendingResourceKinds: kinds,
	}
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

// Kind can be one of CPU, GPU, or Memory
type Kind string

// Inconsistency defines the various ways in which HostResources can be in an inconsistent or illegal state.
// Examples include a resource being negative, a resource quantity being larger than the total available HostResources
// of that kind on the node, and so on.
type Inconsistency string

// Manager is a wrapper around several HostResources structs, each of which corresponds to idle, pending,
// committed, or spec HostResources.
type Manager struct {
	mu sync.Mutex

	// lastAppliedSnapshotId is the ID of the last snapshot that was applied to this Manager.
	lastAppliedSnapshotId int32

	idleResources      *HostResources
	pendingResources   *HostResources
	committedResources *HostResources
	specResources      *HostResources
}

// NewManager creates a new Manager struct from the given types.Spec and returns
// a pointer to it (the new Manager struct).
//
// The given types.Spec is used to initialize the spec and idle resource quantities of the new Manager struct.
func NewManager(spec types.Spec) *Manager {
	resourceSpec := types.ToDecimalSpec(spec)

	return &Manager{
		// ManagerSnapshot IDs begin at 0, so -1 will always be less than the first snapshot to be applied.
		lastAppliedSnapshotId: -1,
		idleResources: &HostResources{
			resourceStatus: IdleResources,
			millicpus:      resourceSpec.Millicpus.Copy(),
			memoryMB:       resourceSpec.MemoryMb.Copy(),
			gpus:           resourceSpec.GPUs.Copy(),
			vramGB:         resourceSpec.VRam.Copy(),
		},
		pendingResources: &HostResources{
			resourceStatus: PendingResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
			vramGB:         decimal.Zero.Copy(),
		},
		committedResources: &HostResources{
			resourceStatus: CommittedResources,
			millicpus:      decimal.Zero.Copy(),
			memoryMB:       decimal.Zero.Copy(),
			gpus:           decimal.Zero.Copy(),
			vramGB:         decimal.Zero.Copy(),
		},
		specResources: &HostResources{
			resourceStatus: SpecResources,
			millicpus:      resourceSpec.Millicpus.Copy(),
			memoryMB:       resourceSpec.MemoryMb.Copy(),
			gpus:           resourceSpec.GPUs.Copy(),
			vramGB:         resourceSpec.VRam.Copy(),
		},
	}
}

// ApplySnapshotToResourceWrapper atomically overwrites the target resourceWrapper's resource quantities with
// the resource quantities encoded by the given HostResourceSnapshot instance.
//
// ApplySnapshotToResourceWrapper returns nil on success.
//
// If the given HostResourceSnapshot's SnapshotId is less than the resourceWrapper's lastAppliedSnapshotId,
// then an error will be returned.
func ApplySnapshotToResourceWrapper[T types.ArbitraryResourceSnapshot](r *Manager, snapshot types.HostResourceSnapshot[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure that the snapshot being applied is not old. If it is old, then we'll reject it.
	if r.lastAppliedSnapshotId > snapshot.GetSnapshotId() {
		return fmt.Errorf("%w: %w (last applied ID=%d, given ID=%d)",
			ErrInvalidSnapshot, scheduling.ErrOldSnapshot, r.lastAppliedSnapshotId, snapshot.GetSnapshotId())
	}

	var err error
	if err = ApplySnapshotToResources(r.idleResources, snapshot.GetIdleResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.pendingResources, snapshot.GetPendingResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.committedResources, snapshot.GetCommittedResources()); err != nil {
		return err
	}

	if err = ApplySnapshotToResources(r.specResources, snapshot.GetSpecResources()); err != nil {
		return err
	}

	return nil
}

// String returns a string representation of the Manager that is suitable for logging.
func (r *Manager) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Sprintf("Manager{%s, %s, %s, %s}",
		r.idleResources.String(), r.pendingResources.String(), r.committedResources.String(), r.specResources.String())
}

// IdleResources returns a ComputeResourceState that is responsible for encoding the current idle HostResources
// of the target Manager.
func (r *Manager) IdleResources() *HostResources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources
}

// PendingResources returns a ComputeResourceState that is responsible for encoding the current pending HostResources
// of the target Manager.
func (r *Manager) PendingResources() *HostResources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources
}

// CommittedResources returns a ComputeResourceState that is responsible for encoding the current committed HostResources
// of the target Manager.
func (r *Manager) CommittedResources() *HostResources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources
}

// SpecResources returns a ComputeResourceState that is responsible for encoding the current spec HostResources
// of the target Manager.
func (r *Manager) SpecResources() *HostResources {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources
}

// idleResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the current idle HostResources
// of the target Manager.
func (r *Manager) idleResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources.ResourceSnapshot(snapshotId)
}

// pendingResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the current pending HostResources
// of the target Manager.
func (r *Manager) pendingResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources.ResourceSnapshot(snapshotId)
}

// committedResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the current committed HostResources
// of the target Manager.
func (r *Manager) committedResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources.ResourceSnapshot(snapshotId)
}

// specResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the current spec HostResources
// of the target Manager.
func (r *Manager) specResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources.ResourceSnapshot(snapshotId)
}

// IdleProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current idle HostResources
// of the target Manager.
func (r *Manager) IdleProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.idleResources.ProtoSnapshot(snapshotId)
}

// PendingProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current pending HostResources
// of the target Manager.
func (r *Manager) PendingProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pendingResources.ProtoSnapshot(snapshotId)
}

// CommittedProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current committed HostResources
// of the target Manager.
func (r *Manager) CommittedProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.committedResources.ProtoSnapshot(snapshotId)
}

// SpecProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the current spec HostResources
// of the target Manager.
func (r *Manager) SpecProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.specResources.ProtoSnapshot(snapshotId)
}

// ComputeResourceSnapshot returns a pointer to a ComputeResourceSnapshot created for the specified "status" of HostResources
// (i.e., "idle", "pending", "committed", or "spec").
func (r *Manager) ResourceSnapshot(status Status, snapshotId int32) *ComputeResourceSnapshot {
	switch status {
	case IdleResources:
		{
			return r.idleResourcesSnapshot(snapshotId)
		}
	case PendingResources:
		{
			return r.pendingResourcesSnapshot(snapshotId)
		}
	case CommittedResources:
		{
			return r.committedResourcesSnapshot(snapshotId)
		}
	case SpecResources:
		{
			return r.specResourcesSnapshot(snapshotId)
		}
	default:
		{
			log.Fatalf("Unknown or unexpected Status specified: \"%s\"", status)
			return nil
		}
	}
}
