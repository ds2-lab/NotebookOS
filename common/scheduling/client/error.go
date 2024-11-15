package client

import (
	"fmt"
)

var (
	ErrHandlerNotImplemented = fmt.Errorf("handler not implemented")
	ErrIOPubNotStarted       = fmt.Errorf("IOPub not started")
)
