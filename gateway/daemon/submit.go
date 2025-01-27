package daemon

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"sync"
)

// ExecutionSubmitter ensures that "execute_request" (and "yield_request") messages are sent one-at-a-time.
type ExecutionSubmitter struct {
	mu  sync.Mutex
	log logger.Logger
}

func NewExecutionSubmitter() *ExecutionSubmitter {
	submitter := &ExecutionSubmitter{}

	config.InitLogger(&submitter.log, submitter)

	return submitter
}
