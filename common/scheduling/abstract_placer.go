package scheduling

import (
	"context"
	"github.com/zhangjyr/distributed-notebook/common/proto"
	"time"
	"errors"

	"github.com/mason-leap-lab/go-utils/logger"
)

var (
	ErrNilHost = errors.New("host is nil when attempting to place kernel")
)

// AbstractPlacer implements basic place/reclaim functionality.
// AbstractPlacer should not be used directly. Instead, embed it in your placer implementation.
type AbstractPlacer struct {
	log logger.Logger
}

// Place atomically places a replica on a host.
func (placer *AbstractPlacer) Place(host *Host, in *proto.KernelReplicaSpec) (*proto.KernelConnectionInfo, error) {
	if host == nil {
		placer.log.Debug("Host cannot be nil when placing a kernel replica...")
		return nil, ErrNilHost
	}

	return host.StartKernelReplica(context.Background(), in)
}

// Reclaim atomically reclaims a replica from a host.
// If noop is specified, it is the caller's responsibility to stop the replica.
func (placer *AbstractPlacer) Reclaim(host *Host, sess *Session, noop bool) error {
	if noop {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	placer.log.Debug("Calling StopKernel on kernel %s running on host %v.", sess.ID(), host)
	_, err := host.StopKernel(ctx, &proto.KernelId{Id: sess.ID()})

	return err
}
