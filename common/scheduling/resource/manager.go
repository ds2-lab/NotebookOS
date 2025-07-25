package resource

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/proto"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/scheduling/transaction"
	"github.com/scusemua/distributed-notebook/common/types"
	"log"
	"sync"
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
	// If the source and target ResourceStatus values do not match, then the snapshot will be rejected.
	ErrIncompatibleResourceStatus = errors.New("source and target ResourceStatus values are not the same")
)

type Transaction func(state *transaction.State)

// Manager is a wrapper around several HostResources structs, each of which corresponds to idle, pending,
// committed, or spec HostResources.
type Manager struct {
	idleResources      *HostResources
	pendingResources   *HostResources
	committedResources *HostResources
	specResources      *HostResources
	mu                 sync.Mutex

	// lastAppliedSnapshotId is the ID of the last snapshot that was applied to this Manager.
	lastAppliedSnapshotId int32
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
		idleResources:         NewHostResources(resourceSpec, resourceSpec, scheduling.IdleResources),
		pendingResources:      NewHostResources(types.ZeroDecimalSpec, nil, scheduling.PendingResources),
		committedResources:    NewHostResources(types.ZeroDecimalSpec, resourceSpec, scheduling.CommittedResources),
		specResources:         NewHostResources(resourceSpec, resourceSpec, scheduling.SpecResources),
	}
}

// unsafeGetTransactionState returns a *transaction.State to use as input to a Transaction.
//
// unsafeGetTransactionState is NOT thread-safe and must be called while the Manager's mutex is already acquired.
func (m *Manager) unsafeGetTransactionState() *transaction.State {
	idleResources := m.idleResources.toTransactionResources(true)
	pendingResources := m.pendingResources.toTransactionResources(true)
	committedResources := m.committedResources.toTransactionResources(true)
	specResources := m.specResources.toTransactionResources(false)

	return transaction.NewState(idleResources, pendingResources, committedResources, specResources)
}

// GetTransactionState returns a *transaction.State to use as input to a Transaction.
func (m *Manager) GetTransactionState() *transaction.State {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.unsafeGetTransactionState()
}

// unsafeCommitTransaction commits the final working resource counts from the Transaction
// to the resource counts of the Manager.
func (m *Manager) unsafeCommitTransaction(t scheduling.TransactionState) {
	m.idleResources.SetTo(t.IdleResources().Working())
	m.pendingResources.SetTo(t.PendingResources().Working())
	m.committedResources.SetTo(t.CommittedResources().Working())
}

// GetTransactionData returns the data required to perform a transaction.CoordinatedTransaction.
func (m *Manager) GetTransactionData() (scheduling.TransactionState, scheduling.CommitTransactionResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	initialState := m.unsafeGetTransactionState()
	commit := func(state scheduling.TransactionState) {
		m.unsafeCommitTransaction(state)
	}

	return initialState, commit
}

// RunTransaction atomically executes the specified Transaction.
//
// If the Transaction completes without any errors, then the final working resource counts from the Transaction
// will be applied to the resource counts of the Manager.
//
// If there are any errors, then the Transaction is discarded entirely.
func (m *Manager) RunTransaction(operation scheduling.TransactionOperation) error {
	if operation == nil {
		return transaction.ErrNilTransactionOperation
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx := transaction.New(operation, m.unsafeGetTransactionState())
	state, err := tx.Run()
	if err != nil {
		return err
	}

	m.unsafeCommitTransaction(state)

	return nil
}

func (m *Manager) GetResourceCountsAsString() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return fmt.Sprintf("IDLE [%s], PENDING [%s], COMMITTED [%s]", m.idleResources.GetResourceCountsAsString(),
		m.pendingResources.GetResourceCountsAsString(), m.committedResources.GetResourceCountsAsString())
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
func (m *Manager) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return fmt.Sprintf("TransactionResources{%s, %s, %s, %s}",
		m.idleResources.String(), m.pendingResources.String(), m.committedResources.String(), m.specResources.String())
}

// IdleResources returns a ComputeResourceState that is responsible for encoding the working idle HostResources
// of the target Manager.
func (m *Manager) IdleResources() *HostResources {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.idleResources
}

// PendingResources returns a ComputeResourceState that is responsible for encoding the working pending HostResources
// of the target Manager.
func (m *Manager) PendingResources() *HostResources {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.pendingResources
}

// PendingResourcesSpec returns a *types.DecimalSpec that is created from the underlying ComputeResourceState.
// The underlying ComputeResourceState is responsible for encoding the working pending HostResources of the target
// Manager.
func (m *Manager) PendingResourcesSpec() *types.DecimalSpec {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.pendingResources.ToDecimalSpec()
}

// CommittedResourcesSpec returns a *types.DecimalSpec that is created from the underlying ComputeResourceState.
// The underlying ComputeResourceState responsible for encoding the working committed HostResources of the target
// Manager.
func (m *Manager) CommittedResourcesSpec() *types.DecimalSpec {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.committedResources.ToDecimalSpec()
}

// CommittedResources returns a ComputeResourceState that is responsible for encoding the working committed HostResources
// of the target Manager.
func (m *Manager) CommittedResources() *HostResources {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.committedResources
}

// SpecResources returns a ComputeResourceState that is responsible for encoding the working spec HostResources
// of the target Manager.
func (m *Manager) SpecResources() *HostResources {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.specResources
}

// idleResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the working idle HostResources
// of the target Manager.
func (m *Manager) idleResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.idleResources.ResourceSnapshot(snapshotId)
}

// pendingResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the working pending HostResources
// of the target Manager.
func (m *Manager) pendingResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.pendingResources.ResourceSnapshot(snapshotId)
}

// committedResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the working committed HostResources
// of the target Manager.
func (m *Manager) committedResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.committedResources.ResourceSnapshot(snapshotId)
}

// specResourcesSnapshot returns a *ComputeResourceSnapshot struct capturing the working spec HostResources
// of the target Manager.
func (m *Manager) specResourcesSnapshot(snapshotId int32) *ComputeResourceSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.specResources.ResourceSnapshot(snapshotId)
}

// IdleProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the working idle HostResources
// of the target Manager.
func (m *Manager) IdleProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.idleResources.ProtoSnapshot(snapshotId)
}

// PendingProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the working pending HostResources
// of the target Manager.
func (m *Manager) PendingProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.pendingResources.ProtoSnapshot(snapshotId)
}

// CommittedProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the working committed HostResources
// of the target Manager.
func (m *Manager) CommittedProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.committedResources.ProtoSnapshot(snapshotId)
}

// SpecProtoResourcesSnapshot returns a *proto.ResourcesSnapshot struct capturing the working spec HostResources
// of the target Manager.
func (m *Manager) SpecProtoResourcesSnapshot(snapshotId int32) *proto.ResourcesSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.specResources.ProtoSnapshot(snapshotId)
}

// ResourceSnapshot returns a pointer to a ComputeResourceSnapshot created for the specified "status" of HostResources
// (i.e., "idle", "pending", "committed", or "spec").
func (m *Manager) ResourceSnapshot(status scheduling.ResourceStatus, snapshotId int32) *ComputeResourceSnapshot {
	switch status {
	case scheduling.IdleResources:
		{
			return m.idleResourcesSnapshot(snapshotId)
		}
	case scheduling.PendingResources:
		{
			return m.pendingResourcesSnapshot(snapshotId)
		}
	case scheduling.CommittedResources:
		{
			return m.committedResourcesSnapshot(snapshotId)
		}
	case scheduling.SpecResources:
		{
			return m.specResourcesSnapshot(snapshotId)
		}
	default:
		{
			log.Fatalf("Unknown or unexpected ResourceStatus specified: \"%s\"", status)
			return nil
		}
	}
}
