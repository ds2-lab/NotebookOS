package sync

import (
	"context"
	"sync"
	"time"
)

type ConcurrencyPool[V any] struct {
	*CappedPool[V]

	// The minimum duration the Get can be blocked. The default is one second.
	BlockInterval time.Duration

	allocated int
	cond      *sync.Cond
	ticker    *time.Timer
}

func NewConcurrencyPool[V any](cap int) *ConcurrencyPool[V] {
	return InitConcurrencyPool(&CappedPool[V]{}, cap)
}

func InitConcurrencyPool[V any](p *CappedPool[V], cap int) *ConcurrencyPool[V] {
	return (&ConcurrencyPool[V]{
		CappedPool: InitCappedPool(p, cap),
	}).init(cap)
}

func (p *ConcurrencyPool[V]) init(cap int) *ConcurrencyPool[V] {
	p.allocated = 0
	p.cond = sync.NewCond(&p.mu)
	if p.BlockInterval != 0 {
		p.BlockInterval = time.Second
	}
	p.ticker = time.NewTimer(p.BlockInterval)
	go p.patrol()
	return p
}

func (p *ConcurrencyPool[V]) Get(ctx context.Context) (v V, err error) {
	// Check if context is canceled before attempting to get a value.
	// This ensures priority is given to the canceled case.
	select {
	case <-ctx.Done():
		return v, ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check again to ensure not canceled.
	select {
	case <-ctx.Done():
		return v, ctx.Err()
	default:
	}

	var ok bool
	for {
		select {
		case v, ok = <-p.pooled:
			if !ok {
				return v, ErrPoolClosed
			}
			return v, nil
		case <-ctx.Done():
			return v, ctx.Err()
		default:
			if p.allocated < p.capacity {
				p.allocated++
				if p.New == nil {
					return v, nil
				} else {
					return p.New(), nil
				}
			}

			// Wait for a value to be released.
			p.cond.Wait()
		}
	}
}

func (p *ConcurrencyPool[V]) Release(v V) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.allocated--
	p.releaseLocked(v)
}

func (p *ConcurrencyPool[V]) Put(v V) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		p.releaseLocked(v)
		return ErrPoolClosed
	}

	select {
	case p.pooled <- v:
		p.cond.Signal()
		return nil
	default:
		// Unlikely, just in case.
		p.allocated--
		p.releaseLocked(v)
		return ErrPoolFull
	}
}

func (p *ConcurrencyPool[V]) patrol() {
	for {
		<-p.ticker.C

		// Give blocked Get a chance to timeout.
		p.mu.Lock()

		if p.closed {
			p.mu.Unlock()
			return
		}

		p.cond.Broadcast() // Wake up all blocked Get calls.
		p.mu.Unlock()

		p.ticker.Reset(p.BlockInterval)
	}
}
