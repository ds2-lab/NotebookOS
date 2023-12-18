package client

import (
	"fmt"

	"github.com/zhangjyr/distributed-notebook/common/jupyter/types"
)

var (
	ErrHandlerNotImplemented = fmt.Errorf("handler not implemented")
	ErrIOPubNotStarted       = fmt.Errorf("IOPub not started")
	ErrStopPropagation       = types.ErrStopPropagation
)
