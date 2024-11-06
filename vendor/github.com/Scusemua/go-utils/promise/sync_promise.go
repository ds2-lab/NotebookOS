package promise

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type SyncPromise struct {
	AbstractPromise

	cond   *sync.Cond
	mu     sync.Mutex
	timers []*time.Timer
}

func ResolvedSync(rets ...interface{}) *SyncPromise {
	promise := NewSyncPromiseWithOptions(nil)
	promise.Resolve(rets...)
	return promise
}

func NewSyncPromise() *SyncPromise {
	return NewSyncPromiseWithOptions(nil)
}

func NewSyncPromiseWithOptions(opts interface{}) *SyncPromise {
	promise := &SyncPromise{}
	promise.cond = sync.NewCond(&promise.mu)
	promise.timers = make([]*time.Timer, 0, 2)
	promise.AbstractPromise.ResetWithOptions(opts)
	promise.SetProvider(promise)
	return promise
}

func (p *SyncPromise) Reset() {
	p.ResetWithOptions(nil)
}

func (p *SyncPromise) ResetWithOptions(opts interface{}) {
	p.Resolve(nil, ErrReset)
	runtime.Gosched()
	p.AbstractPromise.ResetWithOptions(opts)
}

func (p *SyncPromise) Resolve(rets ...interface{}) (Promise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.IsResolved() {
		return p, ErrResolved
	}

	p.ResolveRets(rets...)
	p.cond.Broadcast()
	return p, nil
}

func (p *SyncPromise) Timeout(timeouts ...time.Duration) error {
	ch, err := p.TimeoutC(timeouts...)
	if err == ErrResolved {
		return nil
	} else if err != nil {
		return err
	}

	<-ch
	if p.IsResolved() {
		return nil
	} else {
		return ErrTimeout
	}
}

// PromiseProvider
func (p *SyncPromise) Wait() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for atomic.LoadInt64(&p.resolved) == PromiseInit {
		p.cond.Wait()
	}

	for _, timer := range p.timers {
		timer.Stop()
	}
	p.timers = p.timers[:0]
}

func (p *SyncPromise) Lock() {
	p.mu.Lock()
}

func (p *SyncPromise) Unlock() {
	p.mu.Unlock()
}

func (p *SyncPromise) OnCreateTimerLocked(timer *time.Timer) {
	p.timers = append(p.timers, timer)
}
