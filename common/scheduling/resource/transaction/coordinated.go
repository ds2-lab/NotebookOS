package transaction

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/scusemua/distributed-notebook/common/types"
	"time"
)

type CoordinatedRunner struct {
	transaction func(m *State)
	state       *State
	resultChan  chan interface{}

	log logger.Logger
}

func NewCoordinatedRunner(transaction func(m *State), state *State, resultChan chan interface{}, name string) *CoordinatedRunner {
	runner := &CoordinatedRunner{
		transaction: transaction,
		state:       state,
		resultChan:  resultChan,
	}

	if name != "" {
		config.InitLogger(&runner.log, name)
	} else {
		config.InitLogger(&runner.log, runner)
	}

	return runner
}

func (r *CoordinatedRunner) State() *State {
	return r.state
}

func (r *CoordinatedRunner) RunTransaction() {
	st := time.Now()

	defer func() {
		if err := recover(); err != nil {
			r.log.Warn("Transaction failed after %v: %v", time.Since(st), err)
			r.resultChan <- err
		} else {
			r.log.Debug("Transaction succeeded. Time elapsed: %v.", time.Since(st))
			r.resultChan <- struct{}{}
		}
	}()

	if r.transaction == nil {
		return
	}

	r.transaction(r.state)

	zeroSpec := types.NewDecimalSpec(0, 0, 0, 0)

	// Clamp committed resources between 0 and the Host's spec resources.
	r.state.IdleResources().Sanitize(zeroSpec, r.state.SpecResources().Initial())

	// There isn't really an upper limit here (not one that we can compute right now).
	r.state.PendingResources().Sanitize(zeroSpec, nil)

	// Clamp committed resources between 0 and the Host's spec resources.
	r.state.CommittedResources().Sanitize(zeroSpec, r.state.SpecResources().Initial())

	if err := r.state.Validate(); err != nil {
		panic(err)
	}
}
