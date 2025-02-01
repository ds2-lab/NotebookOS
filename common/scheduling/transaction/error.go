package transaction

import (
	"errors"
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
	Reason          error
	OffendingKind   scheduling.ResourceKind
	OffendingStatus scheduling.ResourceStatus
}

func NewErrTransactionFailed(reason error, offendingKind scheduling.ResourceKind,
	offendingStatus scheduling.ResourceStatus) ErrTransactionFailed {

	err := ErrTransactionFailed{
		Reason:          reason,
		OffendingKind:   offendingKind,
		OffendingStatus: offendingStatus,
	}

	return err
}

func (err ErrTransactionFailed) Error() string {
	return ""
}

func (err ErrTransactionFailed) String() string {
	return err.Error()
}
