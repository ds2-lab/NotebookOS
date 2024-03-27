package core

import (
	"fmt"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
)

type HostMetaKey string

type HostStats interface {
}

type HostMeta interface {
	Value(key interface{}) interface{}
}

// Host defines the interface for a host scheduler that is responsible for:
// 1. Provisioning host-local jupyter kernels.
// 2. Providing statictics of the host for cluster indexing.
type Host interface {
	gateway.LocalGatewayClient
	fmt.Stringer

	// ID returns the host id.
	ID() string

	// Returns the name of the Kubernetes host that the node is running on.
	NodeName() string

	// Addr returns the host address.
	Addr() string

	// Restore restores the host connection.
	Restore(Host) error

	// Stats returns the statistics of the host.
	Stats() HostStats

	// SetMeta sets the meta data of the host.
	SetMeta(key HostMetaKey, value interface{})

	// GetMeta return the meta data of the host.
	GetMeta(key HostMetaKey) interface{}
}
