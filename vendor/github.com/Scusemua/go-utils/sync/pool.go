package sync

import (
	"context"
	"errors"
)

var (
	ErrPoolClosed = errors.New("pool is closed")
	ErrPoolFull   = errors.New("pool is full")
)

// Pool defines a pool that supports eviction awareness and get value by specified filters.
type Pool[V any] interface {
	// Get returns a value qualifies specified filter from the pool.
	Get(context.Context) (V, error)

	// Put puts a function back to the pool.
	Put(V) error

	// Close closes the pool.
	Close()
}

// WaitPool defines a pool that has limited capacity and will wait for a value to be returned if the pool is full.
type WaitPool[V any] interface {
	Pool[V]

	// Release releases the quota occupied by a pooling value.
	Release(V)
}
