package scheduling

import (
	"context"
	"github.com/zhangjyr/distributed-notebook/common/proto"

	"google.golang.org/grpc"
)

type ClusterScheduler interface {
	// MigrateKernelReplica selects a qualified host and adds a kernel replica to the replica set.
	// Unlike StartKernelReplica, a new replica is added to the replica set and a training task may
	// need to start immediately after replica started, e.g., preempting a training task.
	MigrateKernelReplica(ctx context.Context, in *proto.KernelId, opts ...grpc.CallOption) (*proto.ReplicaId, error)
}
