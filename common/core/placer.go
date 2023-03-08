package core

import (
	"errors"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
	"github.com/zhangjyr/distributed-notebook/common/types"
)

var (
	ErrExceedsMaxGPU = errors.New("exceeds max gpus settings per host")
	ErrNotSupported  = errors.New("not supported")
)

// Placer defines the interface for a placer that is responsible for:
// 1. Finding hosts that can satisfy the spec.
// 2. Placing a replica on a host.
type Placer interface {
	// FindHosts returns a list of hosts that can satisfy the spec.
	// The number of hosts returned is determined by the placer.
	FindHosts(types.Spec) []Host

	// Place atomically places a replica on a host.
	Place(host Host, sess MetaSession) (*gateway.KernelConnectionInfo, error)

	// Reclaim atomically reclaims a replica from a host.
	Reclaim(host Host, sess MetaSession) error
}

type PlacerStats interface {
	IdleGPUs() types.StatFloat64Field
	IdleHosts() types.StatInt32Field
}
