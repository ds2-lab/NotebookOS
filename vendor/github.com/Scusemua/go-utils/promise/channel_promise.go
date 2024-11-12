package promise

import (
	"runtime"
	"sync"
	"time"
)

type ChannelPromise struct {
	AbstractPromise

	cond chan struct{}
	mu   sync.Mutex
}

func ResolvedChannel(rets ...interface{}) *ChannelPromise {
	promise := NewChannelPromiseWithOptions(nil)
	promise.Resolve(rets...)
	return promise
}

func NewChannelPromise() *ChannelPromise {
	return NewChannelPromiseWithOptions(nil)
}

func NewChannelPromiseWithOptions(opts interface{}) *ChannelPromise {
	promise := &ChannelPromise{}
	promise.resetWithOptions(opts)
	promise.SetProvider(promise)
	return promise
}

func (p *ChannelPromise) Reset() {
	p.ResetWithOptions(nil)
}

func (p *ChannelPromise) ResetWithOptions(opts interface{}) {
	p.Resolve(nil, ErrReset)
	runtime.Gosched()
	p.resetWithOptions(opts)
}

func (p *ChannelPromise) Resolve(rets ...interface{}) (Promise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.cond:
		return p, ErrResolved
	default:
		p.AbstractPromise.ResolveRets(rets...)
		close(p.cond)
	}
	return p, nil
}

func (p *ChannelPromise) Timeout(timeouts ...time.Duration) error {
	ch, err := p.TimeoutC(timeouts...)
	if err == ErrResolved {
		return nil
	} else if err != nil {
		return err
	}

	select {
	case <-ch:
		return ErrTimeout
	case <-p.cond:
		return nil
	}
}

// PromiseProvider
func (p *ChannelPromise) Wait() {
	<-p.cond
}

func (p *ChannelPromise) Lock() {
	p.mu.Lock()
}

func (p *ChannelPromise) Unlock() {
	p.mu.Unlock()
}

func (p *ChannelPromise) resetWithOptions(opts interface{}) {
	p.AbstractPromise.ResetWithOptions(opts)
	p.cond = make(chan struct{})
}
