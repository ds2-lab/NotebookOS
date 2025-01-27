package transaction

import (
	"errors"
)

var (
	ErrNilTransactionOperation       = errors.New("could not run operation because operation was nil")
	ErrNilParticipantMutex           = errors.New("could not run operation because participant's mutex was nil")
	ErrNilInitialState               = errors.New("could not run operation because initial state was nil")
	ErrNilInitialStateFunction       = errors.New("could not run operation because initial state function was nil")
	ErrImmutableResourceModification = errors.New("operation failed because an attempt to modify an immutable resource was made")
	ErrTransactionFailed             = errors.New("transaction failed")
	ErrNegativeResourceCount         = errors.New("negative resource count")
)
