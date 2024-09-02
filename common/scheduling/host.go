package scheduling

import (
	"fmt"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
)

type HostMetaKey string

type HostStats interface {
	// Priority returns the host's "priority", which is the benefit gained or lost in terms of GPU time per migration.
	Priority() float64

	InteractivePriority() float64

	PreemptionPriority() float64

	// SchedulingOutPriority returns the host's "scheduling-out priority", or SOP, which is defined as the time of the
	// last rescheduling operation plus the frequency of training tasks multiplied by the interactive priority of the
	// potential training task plus the sum of the preemption priorities of the preemptible tasks.
	SchedulingOutPriority() float64

	// SchedulingInPriority returns the host's "scheduling-in priority", or SIP, which is defined as a * the interactive
	// priority of a given task + b * the sum of the preemption priorities of the preemptible tasks
	SchedulingInPriority() float64
}

type HostMeta interface {
	Value(key interface{}) interface{}
}

// Host defines the interface for a host scheduler that is responsible for:
// 1. Provisioning host-local jupyter kernels.
// 2. Providing statistics of the host for cluster indexing.
type Host interface {
	gateway.LocalGatewayClient
	fmt.Stringer

	// ID returns the host id.
	ID() string

	// NodeName returns the name of the Kubernetes host that the node is running on.
	NodeName() string

	// Addr returns the host address.
	Addr() string

	// Restore restores the host connection.
	Restore(Host) error

	// Stats returns the statistics of the host.
	Stats() HostStats

	// SetMeta sets the metadata of the host.
	SetMeta(key HostMetaKey, value interface{})

	// GetMeta return the metadata of the host.
	GetMeta(key HostMetaKey) interface{}
}
