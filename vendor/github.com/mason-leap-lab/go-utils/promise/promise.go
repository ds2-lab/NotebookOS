package promise

import (
	"context"
	"errors"
	"time"

	"github.com/mason-leap-lab/go-utils/sync"
)

const (
	PromiseInit = int64(0)
)

var (
	ErrResolved       = errors.New("resolved already")
	ErrTimeoutNoSet   = errors.New("timeout not set")
	ErrTimeout        = errors.New("timeout")
	ErrNotImplemented = errors.New("not implemented")
	ErrReset          = errors.New("promise reset")

	// OnNewPromise offers a default implementation to instancize a new Promise for the PromisePool.
	OnNewPromise = func() Promise {
		return NewWaitGroupPromise()
	}

	// PromisePool offer a default pool to reused the Promises on calling NewPromise.
	// The default implmentation offers no pooling, call InitPool to setup a pool.
	PromisePool sync.Pool[Promise] = &sync.NilPool[Promise]{New: OnNewPromise}
)

type Promise interface {
	// Reset Reset promise
	Reset()

	// ResetWithOptions Reset promise will options
	ResetWithOptions(interface{})

	// Close Close the promise
	Close()

	// IsResolved If the promise is resolved
	IsResolved() bool

	// Get the time the promise last resolved. time.Time{} if the promise is unresolved.
	ResolvedAt() time.Time

	// Resolve Resolve the promise with value or (value, error)
	Resolve(...interface{}) (Promise, error)

	// Options Get options
	Options() interface{}

	// Value Get resolved value
	Value() interface{}

	// Result Helper function to get (value, error)
	Result() (interface{}, error)

	// Error Get last error on resolving
	Error() error

	// SetTimeout Set how long the promise should timeout.
	SetTimeout(time.Duration)

	// Deadline returns the deadline if timeout is set.
	Deadline() (time.Time, bool)

	// Timeout returns ErrTimeout if timeout, or ErrTimeoutNoSet if the timeout not set.
	Timeout(timeout ...time.Duration) error

	// TimeoutC returns a channel that is closed when the promise timeout.
	TimeoutC(timeout ...time.Duration) (<-chan time.Time, error)
}

// InitPool intialize the pool with specified capacity.
func InitPool(cap int) {
	PromisePool = sync.InitCappedPool(&sync.CappedPool[Promise]{New: OnNewPromise}, cap)
}

// Resolved returns a resolved promise.
func Resolved(rets ...interface{}) (p Promise) {
	p, _ = NewPromise().Resolve(rets...)
	return
}

// NewPromise returns a new promise provided by the pool.
func NewPromise() (p Promise) {
	p, _ = PromisePool.Get(context.TODO())
	p.Reset()
	return
}

// NewPromise returns a new promise with options provided by the pool.
func NewPromiseWithOptions(opts interface{}) Promise {
	p := NewPromise()
	p.ResetWithOptions(opts)
	return p
}

// Recycle returns the promise to the pool.
func Recycle(p Promise) {
	PromisePool.Put(p)
}
