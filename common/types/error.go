package types

import (
	"errors"
	"fmt"
)

var (
	ErrIncompatibleDeploymentMode = errors.New("current deployment mode is incompatible with the requested action")
	ErrStopPropagation            = fmt.Errorf("stop propagation")
)
