package sync

import (
	"context"
	"sync"
)

// CappedPool is a pool that will pool a limited number of values
type CappedPool[V any] struct {
	New    func() V
	Closer func(V)

	capacity int
	pooled   chan V

	mu     sync.RWMutex
	closed bool
}

func NewCappedPool[V any](cap int) *CappedPool[V] {
	return (&CappedPool[V]{}).init(cap)
}

func InitCappedPool[V any](p *CappedPool[V], cap int) *CappedPool[V] {
	return p.init(cap)
}

func (p *CappedPool[V]) init(cap int) *CappedPool[V] {
	p.capacity = cap
	p.pooled = make(chan V, p.capacity)
	return p
}

func (p *CappedPool[V]) Get(ctx context.Context) (v V, err error) {
	var ok bool
	select {
	case v, ok = <-p.pooled:
		if !ok {
			return v, ErrPoolClosed
		}
		return v, nil
	default:
		if p.New == nil {
			return v, nil
		} else {
			return p.New(), nil
		}
	}
}

func (p *CappedPool[V]) Put(v V) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		p.releaseLocked(v)
		return ErrPoolClosed
	}

	select {
	case p.pooled <- v:
		return nil
	default:
		// Cap reached. Simply drop it.
		p.releaseLocked(v)
		return ErrPoolFull
	}
}

func (p *CappedPool[V]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closeLocked()
}

func (p *CappedPool[V]) defaultCloser(V) {
}

func (p *CappedPool[V]) releaseLocked(v V) {
	if p.Closer != nil {
		p.Closer(v)
	}
}

func (p *CappedPool[V]) closeLocked() {
	p.closed = true

	// close to ensure range call ends.
	close(p.pooled)

	closer := p.Closer
	if closer == nil {
		closer = p.defaultCloser
	}
	for pooled := range p.pooled {
		closer(pooled)
	}
}
