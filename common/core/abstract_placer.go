package core

import (
	"context"

	"github.com/zhangjyr/distributed-notebook/common/gateway"
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host Host, sess MetaSession) (*gateway.KernelConnectionInfo, error) {
	return host.StartKernelReplica(context.Background(), sess.(*gateway.KernelReplicaSpec))
}

// Reclaim atomically reclaims a replica from a host.
func (placer *AbstractPlacer) Reclaim(host Host, sess MetaSession) error {
	_, err := host.StopKernel(context.Background(), &gateway.KernelId{Id: sess.ID()})
	return err
}
