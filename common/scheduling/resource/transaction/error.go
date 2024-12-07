package transaction

import (
	"errors"
)

var (
	ErrNilTransactionOperation       = errors.New("could not run operation because operation was nil")
	ErrNilInitialState               = errors.New("could not run operation because initial state was nil")
	ErrImmutableResourceModification = errors.New("operation failed because an attempt to modify an immutable resource was made")
)
