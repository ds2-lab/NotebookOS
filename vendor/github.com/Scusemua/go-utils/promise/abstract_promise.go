package promise

import (
	"sync/atomic"
	"time"
)

type PromiseProvider interface {
	Lock()
	Unlock()
	Wait()
	OnCreateTimerLocked(timer *time.Timer)
}

type AbstractPromise struct {
	resolved int64
	provider PromiseProvider

	options interface{}
	val     interface{}
	err     error

	deadline    time.Time
	willTimeout bool
}

func (p *AbstractPromise) SetTimeout(timeout time.Duration) {
	p.deadline = time.Now().Add(timeout)
	p.willTimeout = true
}

func (p *AbstractPromise) Deadline() (time.Time, bool) {
	return p.deadline, p.willTimeout
}

func (p *AbstractPromise) Reset() {
	p.ResetWithOptions(nil)
}

func (p *AbstractPromise) ResetWithOptions(opts interface{}) {
	atomic.StoreInt64(&p.resolved, PromiseInit)
	p.options = opts
	p.val = nil
	p.err = nil
}

func (p *AbstractPromise) Close() {

}

func (p *AbstractPromise) IsResolved() bool {
	return atomic.LoadInt64(&p.resolved) != PromiseInit
}

func (p *AbstractPromise) ResolveRets(rets ...interface{}) bool {
	switch len(rets) {
	case 0:
		break
	case 1:
		p.val = rets[0]
	default:
		p.val = rets[0]
		if rets[1] == nil {
			p.err = nil
		} else {
			p.err = rets[1].(error)
		}
	}
	atomic.StoreInt64(&p.resolved, time.Now().UnixNano())
	return true
}

func (p *AbstractPromise) ResolvedAt() time.Time {
	ts := atomic.LoadInt64(&p.resolved)
	if ts == PromiseInit {
		return time.Time{}
	} else {
		return time.Unix(0, ts)
	}
}

func (p *AbstractPromise) Options() interface{} {
	return p.options
}

func (p *AbstractPromise) Value() interface{} {
	p.provider.Wait()
	return p.val
}

func (p *AbstractPromise) Result() (interface{}, error) {
	p.provider.Wait()
	return p.val, p.err
}

func (p *AbstractPromise) Error() error {
	p.provider.Wait()
	return p.err
}

func (p *AbstractPromise) TimeoutC(timeouts ...time.Duration) (<-chan time.Time, error) {
	timeout := time.Duration(0)
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}

	p.provider.Lock()

	if p.IsResolved() {
		p.provider.Unlock()
		return nil, ErrResolved
	}

	if timeout == 0 && !p.willTimeout {
		p.provider.Unlock()
		return nil, ErrTimeoutNoSet
	} else if timeout == 0 {
		// Can < 0
		timeout = time.Until(p.deadline)
	}

	if timeout <= 0 {
		p.provider.Unlock()
		return nil, ErrTimeout
	}
	timer := time.NewTimer(timeout)
	p.provider.OnCreateTimerLocked(timer)

	p.provider.Unlock()

	return timer.C, nil
}

func (p *AbstractPromise) OnCreateTimerLocked(*time.Timer) {
	// Default implementation of PromiseProvider
}

func (p *AbstractPromise) SetProvider(provider PromiseProvider) {
	p.provider = provider
}
