package smr

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type LogStorage interface {
	Save(raftpb.HardState, []raftpb.Entry) error

	SaveSnapshot(walpb.Snapshot)

	ReleaseLockTo(uint64) error

	Close() error
}

type LogSnapshotter interface {
	SaveSnap(raftpb.Snapshot) error

	Load() (*raftpb.Snapshot, error)

	LoadNewestAvailable([]walpb.Snapshot) (*raftpb.Snapshot, error)
}
