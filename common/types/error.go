package types

import "errors"

var (
	ErrIncompatibleDeploymentMode = errors.New("current deployment mode is incompatible with the requested action")
)
