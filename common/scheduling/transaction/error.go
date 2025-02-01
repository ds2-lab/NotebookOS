package transaction

import (
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/scheduling"
)

var (
	ErrNilTransactionOperation       = errors.New("could not run operation because operation was nil")
	ErrNilParticipantMutex           = errors.New("could not run operation because participant's mutex was nil")
	ErrNilInitialState               = errors.New("could not run operation because initial state was nil")
	ErrNilInitialStateFunction       = errors.New("could not run operation because initial state function was nil")
	ErrImmutableResourceModification = errors.New("operation failed because an attempt to modify an immutable resource was made")
	ErrNegativeResourceCount         = errors.New("negative resource count")
)

type ErrTransactionFailed struct {
	Reason            error
	OffendingKinds    []scheduling.ResourceKind
	OffendingStatuses []scheduling.ResourceStatus
}

func NewErrTransactionFailed(reason error, offendingKinds []scheduling.ResourceKind,
	offendingStatuses []scheduling.ResourceStatus) ErrTransactionFailed {

	err := ErrTransactionFailed{
		Reason:            reason,
		OffendingKinds:    offendingKinds,
		OffendingStatuses: offendingStatuses,
	}

	return err
}

func (err ErrTransactionFailed) Error() string {
	return fmt.Sprintf("the transaction failed offending kind(s)=%v, offending status(es)=%v, and reason: %v",
		err.OffendingKinds, err.OffendingStatuses, err.Reason)
}

func (err ErrTransactionFailed) String() string {
	return err.Error()
}
