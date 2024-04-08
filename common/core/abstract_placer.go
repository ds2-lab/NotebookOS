package core

import (
	"context"
	"time"

	"github.com/mason-leap-lab/go-utils/logger"
	"github.com/zhangjyr/distributed-notebook/common/gateway"
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
	log logger.Logger
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host Host, sess MetaSession) (*gateway.KernelConnectionInfo, error) {
	return host.StartKernelReplica(context.Background(), sess.(*gateway.KernelReplicaSpec))
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host Host, sess MetaSession, noop bool) error {
	if noop {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %v.", sess.ID(), host)
	_, err := host.StopKernel(ctx, &gateway.KernelId{Id: sess.ID()})

	return err
}
